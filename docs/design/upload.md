# Upload

An upload writes one object to S3 from a caller's source: bytes in memory, a file, or a custom
stream that produces parts over time. S3 offers two ways to write it — a single `PutObject`, or a
multipart upload that sends up to 10,000 parts independently and assembles them on completion — and
the right one depends on the object's size and on whether that size is known before reading. The
upload is a state machine that picks the path, partitions the source into parts, keeps every part's
bytes alive through its retries, and reports the committed object back to the caller.

It has four parts: **path selection** (single request or multipart, decided once from the source's
size bounds), **source partitioning** (a part reader over memory, a file, or a custom stream, with a
part size that fits S3's part limit), the **multipart state machine** (lazily generated part work,
balanced accounting, and a single elected completion), and the **request mapping** (how the caller's
input becomes `PutObject`, `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload`
requests). Memory staging for upload bodies is specified in the [memory design](./memory.md#upload-staging-and-retry);
the retry loop that wraps each send is specified in the [retry design](./retry.md).

---

## Requirements

### One surface for every source

A caller uploads with the same builder whether the data is a `Bytes`, a file path, or a stream whose
total length is unknown until it ends. The source type must not change the API, only how the
transfer manager reads it.

### Use the cheapest request sequence that fits

A small object costs one round trip as a `PutObject`; a multipart upload costs at least three
(`CreateMultipartUpload`, one or more `UploadPart`, `CompleteMultipartUpload`). A large object
gains from multipart: parts send concurrently and a failed part is retried alone. The path is chosen
from the source's declared size against a threshold, and a source whose size has no upper bound
cannot fit a single request, so it must use multipart.

### Fit S3's part limits

A multipart upload holds at most 10,000 parts, numbered 1 to 10,000. When the source declares an
upper bound, the part size must grow so the object fits; when it does not, the limit must be
enforced as parts are produced. A custom stream's part numbers must name distinct positions in the
object.

### Hold the source to its declaration

A source declares bounds on its total size, and a caller may declare an exact `content_length`. The
bytes actually produced must satisfy both. A stream that ends early, or runs past its upper bound,
fails the upload rather than storing an object of the wrong size; S3 independently checks a declared
exact size on completion.

### Bounded memory, replayable bodies

Memory held by an upload must be bounded by the part size and concurrency, not by the object size.
Every byte sent must stay available, unchanged, until every attempt that sends it completes, so a
retry re-sends identical data. A forward-only source cannot be re-read, so its parts must be
retained rather than re-produced.

### Integrity by default

When the S3 client is configured to send checksums, the upload sends them without being asked: one
for a single request, one per part for multipart. A caller who already knows a checksum can supply
it instead of having it calculated.

### Cooperate with the scheduler

An upload must produce work lazily, one part per poll, so memory and in-flight work are bounded by
the scheduler rather than by the upload. A custom stream that cannot produce its next part must not
hold an execution slot while it waits, and must resume exactly where it stopped when it is woken.

### Leave nothing open on cancellation

A cancelled or failed multipart upload has parts stored and billed until it is completed or aborted.
A caller must be able to abort it, and choose to retain the parts instead.

---

## Architecture

The pieces: `UploadInput` and its builder are the caller's request; `Upload::orchestrate` resolves
defaults and enqueues an `UploadTransfer`; `UploadState` is the state machine; `PartReader` turns
the source into numbered `PartData`; `PartTransferState` holds multipart accounting and parked
source reads; `PartPlan` decides when dispatch is finished; the `convert` module maps input fields
onto each S3 request; `UploadHandle` joins or aborts.

### Entry and defaults

`Client::upload()` returns a fluent builder over `UploadInput`, whose fields mirror `PutObject`'s
plus three of the transfer manager's own: `body`, `checksum_strategy`, and
`failed_multipart_upload_policy`. `initiate()` builds the input and calls `Upload::orchestrate`,
which:

1. sets the default `ChecksumStrategy` (CRC64NVME, full object) when the caller set none and the S3
   client's `request_checksum_calculation` is `WhenSupported`;
2. takes the body out of the input and computes the `BucketType` from the bucket name;
3. creates the `TransferContext` (linked to a parent when the upload is a child of `upload_objects`);
4. constructs the `UploadTransfer`, which validates the size declarations, and enqueues it.

Validation happens before enqueue: a size hint whose lower bound exceeds its upper bound, or a
negative `content_length` or one outside the body's bounds, fails `initiate()` with `InputInvalid`.
A valid `content_length` narrows the size hint to exactly that length.

### Choosing the path

The decision is made once, on the first poll, from the size hint and the source type:

```rust
let use_mpu = match size_hint.upper() {
    None => true,
    Some(upper) => stream.is_mpu_only() || upper >= mpu_threshold_bytes,
};
```

| Source | Size hint | Path |
|---|---|---|
| `Bytes` | exact | `PutObject` below the threshold, multipart at or above |
| File | exact | `PutObject` below the threshold, multipart at or above |
| `PartStream` (including `TokioIo`) | any | multipart |
| any | no upper bound | multipart |

The threshold is `Config::multipart_threshold`, 16 MiB under `PartSize::Auto`. A custom stream
always uses multipart because it is forward-only and may carry caller-supplied per-part checksums;
its body cannot be presented as one rewindable `PutObject` body.

### State machine

```text
                         poll_work (first)
                               │
               ┌───────────────┴────────────────┐
           use_mpu=false                    use_mpu=true
               │                                │
               ▼                                ▼
       PutObjectInFlight          PendingInit{init_in_flight}
               │                                │ CreateMultipartUpload
               │                                ▼
               │                          Transferring ◄──┐ UploadPart per poll,
               │                                │         │ parked reads resumed
               │                                ├─────────┘
               │                                │ dispatch closed, all parts retired
               │                                ▼
               │                           Completing
               │                                │ CompleteMultipartUpload dispatched
               │                                ▼
               │                         CompleteInFlight
               │                                │
               └──────────────► Done ◄──────────┘
```

Every state that waits on one in-flight request — `PendingInit` with `init_in_flight`,
`PutObjectInFlight`, `CompleteInFlight` — returns `PollWork::Pending` with `set_pending`, and the
request's completion mutates state under the lock and then calls `try_wake`. That is the scheduler's
[edge-triggered wake protocol](./scheduler.md#edge-triggered-wake). `Transferring` is the only state
that returns more than one ready work item.

The multipart upload ID lives in the state from `Transferring` through `CompleteInFlight` and leaves
only when `CompleteMultipartUpload` succeeds, so an abort can always find an upload that might still
be open (`open_upload_id`).

### Single-request path

`execute_put_object` converts the source into a native, retryable `SdkBody`:

- `Bytes` uses `SdkBody::from(Bytes)`, whose rebuild is a reference-counted clone.
- A file is opened once, then wrapped in `SdkBody::retryable` over `DirectFileBody` (managed threads)
  or `OffloadedFileBody` (shared runtime). Each attempt starts a new positioned cursor over the same
  open file and reads 1 MiB pooled chunks; the object is never staged whole.

The body stays a native `SdkBody` rather than a wrapper so the SDK keeps its in-memory checksum path:
a wrapper would force aws-chunked trailer encoding (see
[checksum framing](./memory.md#checksum-framing)). The request is built from the input by
`copy_fields_to_put_object_request`, sent inside the outer retry loop, and the response becomes the
`UploadOutput`. The transfer then moves to `Done` and signals its handle.

### Multipart path

**Create.** `CreateMultipartUpload` carries the input's object-level fields and, when a checksum
strategy is set, its algorithm and checksum type. Its response seeds the output builder. The part
size is then fixed for the life of the upload:

```text
part_size = max(configured_part_size, upper.div_ceil(10_000))   if the size hint has an upper bound
part_size = configured_part_size                                 otherwise
```

The configured size is `Config::part_size`, 8 MiB under `PartSize::Auto`. The plan is fixed with it:

| `PartPlan` | Source | Dispatch closes when |
|---|---|---|
| `Known { total_parts, declared_object_size }` | `Bytes` or file, exact size | `parts_dispatched == total_parts` |
| `UntilEof { size_hint }` | `PartStream` | the stream returns end-of-stream |

**Transfer.** Each poll in `Transferring` returns one `UploadPart` work item: a parked read that has
been notified, if any; otherwise a fresh read, unless the source is blocked or dispatch is closed.
Execution reads one part from the `PartReader` and sends it:

- `Bytes` parts are zero-copy slices of the caller's buffer.
- File parts claim a disjoint range under a short lock, admit a part-sized envelope from the buffer
  pool, and read it with a positioned read, so concurrent parts read concurrently.
- `PartStream` parts come from the caller's `poll_part`. A gate (`PartStreamGate`) hands the stream
  to exactly one future at a time, because the stream is a single mutable value.

Before the part is sent, its bytes are counted against the plan's upper bound, a stream's part count
against 10,000, and its part number against `1..=10,000`. The send wraps `UploadPart` in the retry
loop over a retryable body (`part_body::sdk_body`): contiguous parts use the native `Bytes` body,
segmented pooled parts an exact-length body that replays the same immutable segments. A response
without an ETag fails the part, because `CompleteMultipartUpload` identifies parts by ETag.

**Complete.** When dispatch is closed and every dispatched part has retired, exactly one caller moves
the state to `Completing` (`try_begin_completing`), and the next poll dispatches
`CompleteMultipartUpload`. It sorts the completed parts, rejects a repeated part number, and sends
`MpuObjectSize`: the declared exact size if the source declared one, otherwise the bytes actually
read. A declared exact size is independent of the part accounting, so S3 detects an assembled object
that differs from what the source declared.

**Empty source.** S3 requires at least one part. A multipart source that produced no parts, when its
lower bound is zero, gets one synthesized zero-byte part after every read has drained, so the state
can tell "no parts" from "an explicit empty part".

### Blocked custom sources

A `PartStream` may return `Poll::Pending` because its data has not arrived. The upload must not hold
an execution slot while it waits, and must not drop the pending future: the future owns the stream
and any progress already made.

```text
execute_upload_part
   │ poll NextPartFuture with a PartReadWake (wraps the scheduler waker)
   ├── Ready(part) ─► send UploadPart
   └── Pending ─► park_read(future, wake) in PartTransferState
                  return WorkOutcome::Yielded          ◄── slot released
                  ...
       source calls waker ─► PartReadWake.notified = true ─► scheduler.wake(id)
       poll_work ─► schedule_part() returns the notified read first ─► resumed execution
```

`PartReadWake` latches a notification that races with parking: a source can call its waker just
before it returns `Pending`, before the future is in transfer state, and `requeue_if_notified`
re-delivers it after parking. While a read is parked the gate reports the source unavailable, so
later polls return `Pending` with `SourceUnavailable` instead of starting reads that would only
block behind it. This is the scheduler's
[execution-time yield](./scheduler.md#execution-time-yield).

### Memory

Upload acquires memory at execution time, not before dispatch (see
[backpressure](./scheduler.md#backpressure)):

| Source | Single request | Multipart part |
|---|---|---|
| `Bytes` | caller-owned, outside pool accounting | caller-owned slice, outside pool accounting |
| File | 1 MiB pooled chunks, re-read per attempt | one part-sized pooled envelope per part |
| `PartStream` | — | whatever the stream acquires through `StreamContext::part_buffer` |

A part's pooled memory is held by its immutable `SegmentedBytes` until the request body and every
retry clone drop; the send drops the retry body before recording completion, so the final part's
memory is released before `CompleteMultipartUpload` can signal `join()`. Concurrency bounds how many
parts hold memory at once. The full ownership model is in
[upload staging and retry](./memory.md#upload-staging-and-retry).

### Checksums

`ChecksumStrategy` carries an algorithm, a checksum type for multipart (`FullObject` or `Composite`),
and an optional caller-supplied full-object value.

- **Single request.** A supplied full-object value is set as the algorithm's checksum header;
  otherwise `checksum_algorithm` asks the SDK to calculate it over the body.
- **Multipart.** `CreateMultipartUpload` and `CompleteMultipartUpload` carry the algorithm and
  type. Each part sends a `PartData::with_checksum` value if the stream supplied one, otherwise the
  SDK calculates one. `CompleteMultipartUpload` carries a full-object value only if the caller
  supplied one, up front or from `PartStream::full_object_checksum`.

The transfer manager does not calculate a full-object checksum for multipart itself. S3 checks every
checksum it receives against the bytes it received; only a value calculated from the caller's own
source lets S3 check the assembled object against that source.

With `request_checksum_calculation = WhenRequired` and no strategy, no checksum is sent; S3 still
stores a CRC64NVME full-object checksum it calculates.

### Retry and failure bounds

`PutObject` and `UploadPart` are each wrapped in the outer `retry` loop with
`classify_upload_part_retry`: a transient transport failure or a throttle is re-issued, and any other
service error, including a `412` from a caller's `If-Match`, is terminal. `CreateMultipartUpload` and
`CompleteMultipartUpload` rely on the SDK's retry alone. All sends use the per-bucket retry
partition; body sends add the tightened stalled-stream protection (`upload_override`). Uploads carry
no latency deadline and do not hedge, because timing a whole part-send is size-blind. A stall while
the body is sent is bounded by stalled-stream protection; a response that never arrives after the
body is fully sent is not bounded. See the [retry design](./retry.md).

Any failure records the error on the context, emits the terminal report, and signals the handle.

### Request mapping

The `convert` module copies the input onto each request with one setter per field, so adding an
input field is one line per request that carries it:

| Request | Carries |
|---|---|
| `PutObject` | every object field, `If-Match` / `If-None-Match`, checksum |
| `CreateMultipartUpload` | every object field, checksum algorithm and type |
| `UploadPart` | SSE-C, request payer, expected bucket owner, part checksum |
| `CompleteMultipartUpload` | `If-Match` / `If-None-Match`, SSE-C, checksum type and full-object value |
| `AbortMultipartUpload` | request payer, expected bucket owner |

`content_length` is not copied: the SDK takes it from the body, and the transfer manager uses it
only to validate the source.

### Cancellation and abort

Dropping the `UploadHandle` marks the transfer cancelled, purges its queued work, and returns
immediately; in-flight work stops at its next await point, and an open multipart upload is left in
place. `abort()` cancels, waits until no work for the transfer is executing, then sends
`AbortMultipartUpload` if an upload ID is still open and `failed_multipart_upload_policy` is
`AbortUpload` (the default); `Retain` keeps the parts.

`failed_multipart_upload_policy` is read only by `abort()`. `join()` on a failed upload returns the
error without sending `AbortMultipartUpload` and consumes the handle, so a failed multipart upload
stays open until a lifecycle rule removes it.

A `CreateMultipartUpload` dropped mid-flight can leave an upload whose ID was never returned. A
`CompleteMultipartUpload` dropped mid-flight leaves the ID in state, so `abort()` aborts it, though
S3 may already have created the object. `abort()` on an upload that already completed returns an
empty `AbortedUpload` and does not delete the object.

### Bucket type

`BucketType` (`Standard` or `Express`, from the `--x-s3` name suffix) is computed and stored on the
transfer but not yet used by upload. S3 Express One Zone differs in ways that matter here — consecutive
part numbers are required, MD5 is not supported, ETags are opaque — and each is currently left to S3
to enforce.

---

## Correctness Invariants

### Part accounting balances exactly once

**Invariant.** Every scheduled part work item is retired exactly once: by a completed `UploadPart`,
by a read that ended without a part, or by a retraction of a dispatch that could not start.

**What it rules out.** A leaked in-flight count that keeps completion from ever starting, or a
double decrement that starts completion while a part is still sending.

**Mechanism.** `PartTransferState` changes `parts_dispatched` and `parts_in_flight` only together
and under the state lock: `schedule_part` and `maybe_start_empty_object_part` increment;
`retract_scheduled_part`, `finish_read_without_part`, and `complete_part` decrement, with checked
arithmetic that panics on underflow. A parked read stays counted in flight. `into_completion`
asserts that bytes read equal bytes uploaded.

### Completion is elected once

**Invariant.** Exactly one caller moves `Transferring` to `Completing`.

**What it rules out.** Two `CompleteMultipartUpload` requests when source end-of-stream and the last
part's completion race on different threads.

**Mechanism.** `try_begin_completing` checks `is_complete` and replaces the state under the lock; the
second caller sees `Completing` and does nothing.

### The stored object matches the source's declaration

**Invariant.** An upload succeeds only if the bytes produced lie within the declared bounds, and a
declared exact size is what S3 checks on completion.

**What it rules out.** Storing a truncated or over-long object from a stream that ended early or ran
long, or from a part the transfer manager dropped or duplicated.

**Mechanism.** `observe_part` checks the running total against the upper bound before each send;
`mpu_object_size` checks both bounds at completion and sends the declared exact size as
`MpuObjectSize`, independent of the part accounting.

### A retry re-sends identical bytes

**Invariant.** Every attempt of a request sends the same bytes.

**What it rules out.** A retry that re-reads a changed or advanced source and stores different data
under the same part number.

**Mechanism.** Every body is retryable: in-memory parts clone immutable `Bytes` or `SegmentedBytes`;
a file `PutObject` re-reads the same range of the same open file. A `PartStream` part is immutable
once produced and is never re-requested from the stream.

### A parked source read is never lost or recreated

**Invariant.** A custom-stream read that returned `Pending` is resumed, not recreated, and its wake
is never lost.

**What it rules out.** Losing partial progress held by the dropped future, or a transfer that waits
forever because its source woke before the read was parked.

**Mechanism.** The exact `NextPartFuture` is parked in `PartTransferState`; `PartReadWake` latches a
racing notification and `requeue_if_notified` re-delivers it; `schedule_part` returns notified reads
before starting new ones.

### An open multipart upload is always reachable

**Invariant.** While a multipart upload might still be open on S3, its ID is in the transfer state.

**What it rules out.** An `abort()` that cannot find an upload ID, leaving parts stored and billed.

**Mechanism.** The ID moves from `Transferring` to `Completing` to `CompleteInFlight` and is dropped
only on `Done`, after `CompleteMultipartUpload` succeeded. The one exception is a
`CreateMultipartUpload` dropped mid-flight: S3 may have created an upload whose ID was never
returned, and no `abort()` can reach it.

---

## Open Decisions

**Apply `failed_multipart_upload_policy` on failure.** The policy is read only by `abort()`, so a
multipart upload that fails through `join()` stays open, while the policy's documentation describes
aborting on any part failure. Either `join()` applies the policy before returning the error, or the
documentation is narrowed to `abort()`.

**Bound the response wait.** A response that never arrives after the body is sent is not bounded.
Bounding it needs a response-first-byte timeout measured from send completion, a signal the SDK does
not expose, or an operation timeout on body sends that must scale with body size.

**Report a completed upload from `abort()`.** `abort()` on an upload that already completed returns
an empty `AbortedUpload`. Either it reports that the object exists, or
the handle documents that it does not.

