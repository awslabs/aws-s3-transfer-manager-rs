# Upload

An upload writes one object to S3 from a caller's source: bytes in memory, a file, or a custom
stream that produces parts over time. S3 offers a single `PutObject` or a multipart upload that
sends up to 10,000 parts independently and assembles them on completion; the right one depends on
the source type, the object's size, and whether the size is known before reading. The upload is a
state machine that picks the path, partitions the source into parts, keeps every part's bytes alive
through retries, and reports the committed object to the caller. An S3 Express One Zone directory
bucket adds append mode: a serial chain of `PutObject` requests, each adding bytes to the end of one
object.

It has five parts: **path selection** (decided once from the source type and its size bounds),
**source partitioning** (a part reader over memory, a file, or a custom stream, with a part size
that fits S3's part limit), the **multipart state machine** (lazily generated part work, balanced
accounting, one elected completion), the **request mapping** (how the caller's input becomes
`PutObject`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and
`AbortMultipartUpload` requests), and [**append mode**](#append) (a serial chain of
offset-conditioned `PutObject` requests that grows one object). Memory staging is specified in the
[memory design](./memory.md#upload-staging-and-retry), the retry loop in the [retry
design](./retry.md).

---

## Requirements

### One surface for every source

One builder serves `Bytes`, a file path, and a stream of unknown length. The source type changes how
the transfer manager reads, not the API.

### Use the cheapest request sequence that fits

A small object costs one `PutObject` round trip. Multipart costs at least three
(`CreateMultipartUpload`, one or more `UploadPart`, `CompleteMultipartUpload`) but sends parts
concurrently and retries a failed part alone. The path follows the source type and declared size
against a threshold; a custom stream, or a source with no upper bound, must use multipart.

### Fit S3's part limits

A multipart upload holds at most 10,000 parts, numbered 1 to 10,000. With an upper bound the part
size grows to fit; without one the limit is enforced as parts are produced. A custom stream's part
numbers must name distinct positions.

### Hold the source to its declaration

A source declares bounds on its total size, and a caller may declare an exact `content_length`.
Bytes produced must satisfy both: a stream that ends early or runs past its upper bound fails the
upload, and S3 checks a declared exact size against the stored parts on completion.

### Bounded memory, replayable bodies

Memory the transfer manager allocates comes from the shared buffer pool and is bounded by its
capacity, not the object size. A caller's `Bytes` source is held whole, and a pooled part larger
than the pool fails with `ExceedsCapacity`. Every byte sent stays available, unchanged, until every
attempt completes. A forward-only source's parts are retained. A file `PutObject` re-reads the file
per attempt; attempts match only while the file is unchanged.

### Integrity by default

With checksums enabled on the S3 client, the upload sends one checksum per single request or per
part without being asked. A caller may supply a full-object value, and per-part values from a
`PartStream`.

### Cooperate with the scheduler

Work is produced lazily, one part per poll. A
custom stream that cannot produce its next part holds no execution slot and resumes exactly where it
stopped when woken.

### Abort on request

A cancelled or failed multipart upload keeps its parts stored and billed until completed or
aborted. `abort()` aborts it unless the caller chose to retain the parts.

---

## Architecture

The pieces: `UploadInput` and its builder are the caller's request; `Upload::orchestrate` resolves
defaults and enqueues an `UploadTransfer`; `UploadState` is the state machine; `PartReader` turns
the source into numbered `PartData`; `PartTransferState` holds multipart accounting and parked
reads; `PartPlan` decides when dispatch is finished; the `convert` module maps input fields onto
each S3 request; `UploadHandle` joins or aborts.

### Entry and defaults

`Client::upload()` returns a fluent builder over `UploadInput`, a subset of `PutObject`'s fields:
`body` is an `InputStream`, `checksum_strategy` replaces the checksum fields, and
`failed_multipart_upload_policy` is the transfer manager's own. `initiate()` fails with a build
error if `bucket`, `key` or `body` is unset (an empty object needs an explicit empty body), builds
the input, and calls `Upload::orchestrate`, which:

1. sets the default `ChecksumStrategy` (CRC64NVME, full object) when the caller set none and the S3
   client's `request_checksum_calculation` is `WhenSupported`;
2. takes the body out of the input and computes the `BucketType` from the bucket name;
3. creates the `TransferContext`, which `Upload::orchestrate_child` (used by `upload_objects`) links
   to the parent;
4. constructs the `UploadTransfer`, which validates the size declarations, and enqueues it.

Validation precedes enqueue: a lower bound above the upper bound, or a negative `content_length` or
one outside the body's bounds, fails `initiate()` with `InputInvalid`. A valid `content_length`
narrows the size hint to exactly that length.

### Choosing the path

The decision is made once, on the first poll, from the size hint and the source type. An upload with
`append` set skips it and always appends (see [Append](#append)):

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
| any, with `append` set | any | serial appends; never multipart |

The threshold is `Config::multipart_threshold`, 16 MiB under `PartSize::Auto`. A custom stream
always uses multipart.

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

A state waiting on one in-flight request (`PendingInit` with `init_in_flight`, `PutObjectInFlight`,
`CompleteInFlight`) returns `PollWork::Pending` with `set_pending`. `CreateMultipartUpload`'s
completion moves the state under the lock, then calls `try_wake`, the scheduler's
[edge-triggered wake protocol](./scheduler.md#edge-triggered-wake). `PutObject` and
`CompleteMultipartUpload` instead set `Done` on success and signal the handle; the scheduler retires
the terminal transfer once its work drains. `Transferring` is the only state that returns more than
one ready work item, and it returns `Pending` when the source is unavailable, dispatch is closed, or
no part is ready.

`Done` means the object is committed. A failure or cancellation leaves `UploadState` unchanged, and
`poll_work` returns `PollWork::Done` once the transfer is no longer active.

From `Transferring` until `CompleteMultipartUpload` succeeds, the multipart upload ID is in the
state (`open_upload_id`). Between a successful `CreateMultipartUpload` and `Transferring`, only the
executing work item holds it.

### Single-request path

`execute_put_object` converts the source into a native, retryable `SdkBody`:

- `Bytes` uses `SdkBody::from(Bytes)`, whose rebuild is a reference-counted clone.
- A file source is a range `(path, offset, length)`; the length is trusted, and a file that ends
  early fails with `UnexpectedEof`. The file is opened once when the upload starts and wrapped in
  `SdkBody::retryable` over `DirectFileBody` (managed threads) or `OffloadedFileBody` (shared
  runtime). Each attempt starts a new positioned cursor over the same open file and reads 1 MiB
  pooled chunks; the object is never staged whole. The direct body holds one chunk, the offloaded
  body up to four queued chunks plus the one being read. Pool admission for a chunk waits inside the
  body's `poll_frame`.

A `Bytes` body stays a native `SdkBody` and carries the checksum in a header. A file body carries it
in a trailer (see [checksum framing](./memory.md#checksum-framing)). The request is built by
`copy_fields_to_put_object_request` and sent inside the outer retry loop; the response becomes the
`UploadOutput`, the transfer moves to `Done`, and its handle is signalled.

### Multipart path

**Create.** `CreateMultipartUpload` carries the object-level fields and, when a checksum strategy is
set, its algorithm and type. The response seeds the output builder. The part size is then fixed for
the upload:

```text
part_size = max(configured_part_size, upper.div_ceil(10_000))   if the size hint has an upper bound
part_size = configured_part_size                                 otherwise
```

The configured size is `Config::part_size`, 8 MiB under `PartSize::Auto`. The plan is fixed with it:

| `PartPlan` | Source | Dispatch closes when |
|---|---|---|
| `Known { total_parts, declared_object_size }` | `Bytes` or file, exact size | `parts_dispatched >= total_parts` |
| `UntilEof { size_hint }` | `PartStream` | the stream returns end-of-stream |

**Transfer.** Each poll in `Transferring` returns one `UploadPart` work item: a notified parked read
if any; otherwise a fresh read, unless the source is blocked or dispatch is closed; or the
synthesized empty-object part (below). Execution reads one part from the `PartReader` and sends it:

- `Bytes` parts are zero-copy slices of the caller's buffer.
- File parts claim a disjoint range under a short lock, admit a part-sized envelope from the buffer
  pool, and read it with a positioned read; parts read concurrently. The read runs
  inline on managed threads (`direct_io`) and through `spawn_blocking` otherwise.
- `PartStream` parts come from the caller's `poll_part`. `PartStreamGate` hands the stream, a single
  mutable value, to exactly one future at a time.

Before a part is sent, its bytes are counted against the plan's upper bound, a stream's part count
against 10,000, and its part number against `1..=10,000`. `UploadPart` is sent through the retry
loop over a retryable body (`part_body::sdk_body`): contiguous parts use the native `Bytes` body,
segmented pooled parts an exact-length body that replays the same immutable segments. A response
with a missing or empty ETag fails the upload without a retry.

**Complete.** When dispatch is closed, no part read, upload or parked read is outstanding, and no
empty-object part is owed, exactly one caller moves the state to `Completing`
(`try_begin_completing`), and that poll or the next dispatches `CompleteMultipartUpload`. It sorts
the completed parts, rejects a repeated part number, checks the bytes read against both size bounds,
and sends `MpuObjectSize`: the exact hint if its bounds are equal, otherwise the bytes read. A
source that ends short of or past an exact declaration fails before `CompleteMultipartUpload`;
`MpuObjectSize` is a second check by S3 against the stored parts.

**Empty source.** S3 requires at least one part. A multipart source that produced no parts, with a
zero lower bound, gets one synthesized zero-byte part after every read has drained.

### Blocked part reads

A part read returns `Poll::Pending` when a custom stream's data has not arrived, its pool memory is
not yet admitted, or a file read is offloaded. The upload holds no execution slot while it waits and
must not drop the pending future: it holds the read's registered wake and, for a file read, the read
in progress. Every such read is parked; only a `PartStream` also blocks further reads, through its
gate. A fresh read that finds the gate taken after being scheduled retracts its dispatch and yields.

```text
execute_upload_part
   │ poll NextPartFuture with a PartReadWake (wraps the scheduler waker)
   ├── Ready(part) ─► send UploadPart
   └── Pending ─► park_read(PendingPartRead) in PartTransferState
                  return WorkOutcome::Yielded          ◄── slot released
                  ...
       source calls waker ─► PartReadWake.notified = true ─► scheduler.wake(id)
       poll_work ─► schedule_part() returns the notified read first ─► resumed execution
```

`PartReadWake` latches a notification that races with parking: a source may call its waker just
before returning `Pending`, before the future is in transfer state, and `requeue_if_notified`
re-delivers it after parking. While a read is parked the gate reports the source unavailable, so
later polls return `Pending` with `SourceUnavailable`. This is the scheduler's [execution-time
yield](./scheduler.md#execution-time-yield).

### Memory

Upload acquires memory at execution time, not before dispatch (see
[backpressure](./scheduler.md#backpressure)):

| Source | Single request | Multipart part |
|---|---|---|
| `Bytes` | caller-owned, outside pool accounting | caller-owned slice, outside pool accounting |
| File | 1 MiB pooled chunks, re-read per attempt | one part-sized pooled envelope per part, replayed from memory on retry |
| `PartStream` | — | pooled envelopes from `StreamContext::part_buffer`, or caller-owned `Bytes` outside pool accounting |

A part's pooled memory is held by its immutable owners (`SegmentedBytes`, or the owner-backed
`Bytes` of a single-segment part) until the request body and every retry clone drop. The send drops
the retry body before recording completion. Concurrency bounds executing parts; a parked read keeps
its partial buffer after yielding its slot, and the pool budget bounds the total. A `PartStream`
uses the pool only if it calls `part_buffer`; `TokioIo` acquires one part-size envelope and keeps it
across `Pending`. Only `UploadPart` reads are parked; a `PutObject` file body waits inside the HTTP
body. The full ownership model is in [upload staging and
retry](./memory.md#upload-staging-and-retry).

### Checksums

`ChecksumStrategy` carries an algorithm, a checksum type for multipart (`FullObject` or
`Composite`), and an optional caller-supplied full-object value.

- **Single request.** A supplied full-object value is set as the algorithm's checksum header;
  otherwise `checksum_algorithm` asks the SDK to calculate it over the body.
- **Multipart.** `CreateMultipartUpload` carries the algorithm and type; `CompleteMultipartUpload`
  carries the type. Each part sends a `PartData::with_checksum` value if the stream supplied one,
  otherwise the SDK calculates one. `CompleteMultipartUpload` carries a full-object value only for
  type `FullObject`, and only if the caller supplied one, up front or from
  `PartStream::full_object_checksum`.

The transfer manager does not calculate a full-object checksum for multipart.

With `request_checksum_calculation = WhenRequired` and no strategy, the transfer manager sends no
checksum and ignores `PartData::with_checksum` values; S3 still stores a CRC64NVME full-object
checksum it calculates.

### Retry and failure bounds

`PutObject` and `UploadPart` go through the outer `retry` loop with `classify_upload_part_retry`,
which re-issues a transient transport failure or a throttle. Any other error, including a `412` from
a caller's `If-Match` on `PutObject`, is not re-issued by the loop; the SDK's own retry still
applies. The loop makes at most three attempts and matches throttles by error code, not HTTP status.
`CreateMultipartUpload`, `CompleteMultipartUpload` and `AbortMultipartUpload` rely on the SDK's
retry alone; if the abort fails, `abort()` returns the error and the upload stays open.

All sends use the per-bucket retry partition; body sends add the tightened stalled-stream protection
(`upload_override`). Uploads carry no latency deadline and do not hedge. Stalled-stream protection
bounds a stall while the body is sent; a response that never arrives after the body is fully sent is
bounded only by a timeout on the caller's S3 client. See the [retry design](./retry.md).

Input validation fails `initiate()` before the transfer exists. A failure after the transfer is
enqueued records the error on the context, emits the terminal report, and signals the handle. It
stops new work only; in-flight sibling requests run until `join()` or `abort()` cancels the
transfer.

### Request mapping

The `convert` module copies the input onto each request with one setter per field:

| Request | Carries |
|---|---|
| `PutObject` | every object field, `If-Match` / `If-None-Match`, checksum; in append mode, the write offset and `If-Match` from the append state |
| `CreateMultipartUpload` | every object field except `Content-MD5` and `If-Match` / `If-None-Match`, checksum algorithm and type |
| `UploadPart` | SSE-C, request payer, expected bucket owner, part checksum |
| `CompleteMultipartUpload` | `If-Match` / `If-None-Match`, SSE-C, request payer, expected bucket owner, `MpuObjectSize`, checksum type and full-object value |
| `AbortMultipartUpload` | request payer, expected bucket owner |

The input's `content_length` is not copied: `PutObject` takes the length from the body and
`UploadPart` from the part. A declared value becomes the exact size bound and, as the bounds are
then equal, the transfer's reported total.

### Cancellation and abort

Dropping an active `UploadHandle` marks the transfer cancelled, purges its queued work, and returns
immediately; in-flight work stops at its next await point, and an open multipart upload is left in
place. Dropping a handle whose transfer already failed or completed does nothing. `abort()` cancels,
waits until no work for the transfer is executing, then sends `AbortMultipartUpload` if an upload ID
is still open and `failed_multipart_upload_policy` is `AbortUpload` (the default); `Retain` keeps
the parts.

`failed_multipart_upload_policy` is read only by `abort()`. `join()` on a failed upload cancels
in-flight work, waits for idle, and returns the error without sending `AbortMultipartUpload`; it
consumes the handle. A failed multipart upload stays open until a lifecycle rule removes it. Child
uploads of `upload_objects` are joined, never aborted; a failed or cancelled child stays open too.
Under `Retain`, `abort()` returns an empty `AbortedUpload`, not the upload ID.

A `CreateMultipartUpload` dropped mid-flight can leave an upload whose ID was never returned, and a
part-reader build failure or panic after `CreateMultipartUpload` succeeded fails the transfer before
the ID reaches the state; either upload stays open until a lifecycle rule removes it. A
`CompleteMultipartUpload` dropped mid-flight leaves the ID in state; `abort()` aborts it, and S3 may
already have created the object. `abort()` on an upload that already completed returns an empty
`AbortedUpload` and does not delete the object.

### Bucket type

`BucketType` (`Standard` or `Express`, from the `--x-s3` name suffix) is computed and stored on the
transfer; upload reads it only to [gate append mode](#gating). S3 enforces the rules specific to
directory buckets, such as consecutive part numbers, when the upload completes.

---

## Append

Append mode sends a serial chain of `PutObject` requests with a write offset to one object in an S3
Express One Zone directory bucket. Each request carries the size and ETag the previous response
returned.

### API

```rust
client.upload()
    .bucket("logs--usw2-az1--x-s3")
    .key("app.log")
    .body(InputStream::from(chunk))
    .append(AppendPosition::End)          // or ::Start, or ::After(prev.append_state())
    .initiate()?
    .join().await?;                       // UploadOutput { size, e_tag, append_state(), .. }
```

`UploadInput` gains `append: Option<AppendPosition>`:

| `AppendPosition` | Meaning | Requests before the first append |
|---|---|---|
| `Start` | Create the object, or append to an empty one at offset 0 | none |
| `End` | Append to the object as it is; if the object does not exist yet, it behaves as `Start` | `HeadObject` with `ChecksumMode=ENABLED` |
| `After(AppendState)` | Continue from a previous upload's output | none |

`AppendState` is opaque: size, ETag, and checksum algorithm. `UploadOutput::append_state()` returns
it on success and the error carries it on failure. `UploadOutput::size` carries
`PutObjectOutput::size`; in append mode its ETag, size, and checksum fields are those of the last
append's response.

`build()` rejects, with `InputInvalid`, `append` combined with `if_match`, `if_none_match`, or a
`full_object_checksum` value.

### Gating

A bucket whose name lacks the `--x-s3` suffix is `BucketType::Standard` and fails `initiate()` with
`AppendError::NotAppendable` before any request. A 501 `NotImplemented` response maps to the same
error.

### State machine and scheduling

Append mode replaces the path decision with its own branch of the [state machine](#state-machine):

```text
PendingInit ─► Discovering (End only: HeadObject)
                    │
                    ▼
               Appending ◄──────────────┐  read one chunk; source Pending → Yielded + park_read
                    │ chunk ready       │
                    ▼                   │
              AppendInFlight ───────────┘  response: next offset = size, next If-Match = ETag
                    │ source exhausted
                    ▼
                  Done
```

- `AppendInFlight` returns `PollWork::Pending` with `set_pending`; the append's completion writes
  the next `AppendState` under the state lock, then calls `try_wake`, as `PutObjectInFlight` does.
- Each append costs one IO unit in CFS. An upload waiting on its source or its in-flight append
  leaves the ready set and rejoins at the root floor when woken ([fair
  scheduling](./scheduler.md#fair-scheduling)). N append-mode uploads hold at most N execution slots
  under the global concurrency target; priority is `handle.scheduling().set_priority`.
- A blocked custom source parks as in [blocked part reads](#blocked-part-reads).
- Appends do not hedge.
- A download of an object being appended pins the ETag, so an append during the read fails the read.

### Chunking and memory

| Source | One append carries | Memory |
|---|---|---|
| `Bytes` | `min(remaining, 5 GiB)`, a zero-copy slice | caller-owned, outside pool accounting |
| File | `min(remaining, 5 GiB)`, a file body over that range | 1 MiB pooled chunks, as for `PutObject` |
| `PartStream` | one `PartData` | what the stream acquires, bounded by the part size |

File bodies take an offset and length, so the range body is `DirectFileBody` / `OffloadedFileBody`.
A `PartStream` part is held, immutable, until its append completes, and the next is not read until
then, so an upload holds at most one part. Part numbers are ignored. If the response's `size` is not
`offset + len`, the upload fails with `IntegrityError`.

### Checksum

The algorithm is, in order: the one in `AppendState`; the one `HeadObject` reports (`End`); the
caller's `ChecksumStrategy`; else CRC64NVME. A strategy naming a different algorithm from the
discovered one fails with `InputInvalid` before any append. Each append sets `checksum_algorithm`,
so the SDK calculates a checksum over that append's body; a `PartData::with_checksum` value is sent
as that append's checksum.

### Retry and reconcile

- The append send disables the SDK's inner retry through its per-request config override, the
  mechanism `upload_override` uses. The outer [retry loop](./retry.md) is the only retrier.
- The classifier is `classify_upload_part_retry`: transient transport failures and throttles retry.
  Each reconciles before the re-send.
- **Reconcile** is a `HeadObject`. `size == offset` with the ETag unchanged: re-send under the same
  condition. `size == offset + len`: a ranged `GetObject` of `[offset, offset + len)` compared with
  the part held for retry; equal means committed, so adopt the new ETag and continue. Not equal, or
  any other size, is `AppendError::OffsetMismatch`.

### Part ceiling

`TooManyParts` maps to `AppendError::PartLimitReached`, carrying the last committed `AppendState`.
The upload does not count parts.

### Append errors

`ErrorKind::AppendError(AppendError)`, shaped like `ErrorKind::IntegrityError`.

| `AppendError` | From | Carries |
|---|---|---|
| `NotAppendable` | `BucketType::Standard`; 501 `NotImplemented` | none |
| `OffsetMismatch` | 400 `InvalidWriteOffset`; 412 `PreconditionFailed`; 409 `ConditionalRequestConflict`; 404 `NoSuchKey` on `After`; a failed reconcile | expected offset; observed size if known |
| `PartLimitReached` | 400 `TooManyParts` | none |

Every `AppendError`, and any failure after at least one append committed, carries the last committed
`AppendState`. A failure leaves the committed prefix in place. `abort()` stops further appends.

### Open Decisions

**Concurrent writers to one key.** A second writer fails with `OffsetMismatch`. Either that is the
only mechanism, or append-mode uploads to one bucket and key within a client are also serialized.

**An append in flight at cancel, or with no response.** Either the outcome is reported as unknown
with the last committed `AppendState`, and the caller reconciles with `End`; or `abort()` waits for
the in-flight append, and a response-wait timeout bounds the wait.

**Many small appends.** Each append spends one of 10,000 parts. Either the limit is only surfaced,
or small writes within a flush window are coalesced into one append up to the part size, or appends
below a minimum size are rejected.

**Fields S3 rejects on append.** Either `build()` rejects a field set in append mode that S3 rejects
on an existing object, or every field is forwarded and the S3 error is surfaced.

---

## Correctness Invariants

### Part accounting balances exactly once

**Invariant.** Before completion, every scheduled part read or upload is retired exactly once: by a
completed `UploadPart`, by a read that ended without a part, or by a retraction of a dispatch that
could not start. A parked read stays counted until its resumed execution retires it. A failure or
cancellation ends the transfer without retiring outstanding work.

**What it rules out.** A leaked in-flight count that keeps completion from ever starting, or a
double decrement that starts completion while a part is still sending.

**Mechanism.** `PartTransferState` changes its counters only under the state lock: `schedule_part`
and `maybe_start_empty_object_part` increment `parts_dispatched` and `parts_in_flight`;
`retract_scheduled_part` decrements both; `finish_read_without_part` and `complete_part` decrement
`parts_in_flight` only. All use checked arithmetic that panics on underflow, and the scheduler fails
a transfer whose `poll_work` or `execute` panics. `into_completion` asserts that bytes read equal
bytes uploaded.

### Completion is elected once

**Invariant.** Exactly one caller moves `Transferring` to `Completing`.

**What it rules out.** Two `CompleteMultipartUpload` requests when source end-of-stream and the last
part's completion race on different threads.

**Mechanism.** `try_begin_completing` checks `is_complete` and replaces the state under the lock; a
later caller finds the state no longer `Transferring` and does nothing.

### The stored object matches the source's declaration

**Invariant.** A multipart upload succeeds only if the bytes produced lie within the declared
bounds, and S3 checks `MpuObjectSize` against the stored parts. A file `PutObject` succeeds only if
the file yields its declared length.

**What it rules out.** Storing a truncated or over-long object from a stream that ended early or ran
long, or from a part the transfer manager dropped or duplicated.

**Mechanism.** `observe_part` checks the running total against the upper bound before each send;
`mpu_object_size` checks both bounds at completion and sends `MpuObjectSize`: the exact hint if its
bounds are equal, otherwise the bytes read. A file body fails with `UnexpectedEof` if the file ends
before its length.

### A retry re-sends identical bytes

**Invariant.** Every attempt of a request sends the same bytes, provided a file source is not
modified during the upload.

**What it rules out.** A retry that re-reads a changed or advanced source and stores different data
under the same part number.

**Mechanism.** Every body is retryable: parts, including file parts read into pooled memory, clone
immutable `Bytes` or `SegmentedBytes`; a file `PutObject` re-reads the same range of the same open
file. A `PartStream` part is immutable once produced and is never re-requested from the stream.

### A parked part read is never lost or recreated

**Invariant.** A part read that returned `Pending` is resumed, not recreated, and its wake is never
lost.

**What it rules out.** Losing the registered wake or a file read in progress held by a dropped
future, or a transfer that waits forever because its source woke before the read was parked.

**Mechanism.** The exact `NextPartFuture` is parked in `PartTransferState`; `PartReadWake` latches a
racing notification and `requeue_if_notified` re-delivers it; `schedule_part` returns notified reads
before starting new ones.

### An open multipart upload is always reachable

**Invariant.** From `Transferring` until `CompleteMultipartUpload` succeeds, the multipart upload ID
is in the transfer state.

**What it rules out.** An `abort()` that cannot find an upload ID, leaving parts stored and billed.

**Mechanism.** The ID moves from `Transferring` to `Completing` to `CompleteInFlight` and is dropped
only on `Done`, after `CompleteMultipartUpload` succeeded. Two uploads never reach the state, and no
`abort()` can reach them: one created by a `CreateMultipartUpload` dropped mid-flight, and one whose
part-reader build failed after `CreateMultipartUpload` succeeded.

### One append in flight

**Invariant.** An append-mode upload never dispatches an append while another of its appends is in
flight.

**Mechanism.** `AppendInFlight` returns `PollWork::Pending` until the append's completion writes the
next `AppendState` under the state lock and calls `try_wake`.

### Each append is conditioned on the state the previous one produced

**Invariant.** The write offset and `If-Match` of append *n + 1* are the size and ETag from append
*n*'s response, or from a reconcile that proved append *n* committed.

**Mechanism.** Only an append's successful completion and a reconcile that matched the held bytes
write `AppendState`; the next append's request is built from it.

### A re-send is never unconditional

**Invariant.** A retried append carries the same write offset and `If-Match` as the attempt it
repeats, and is sent only after a reconcile found the append not committed.

**Mechanism.** The request builder reads the offset and ETag from the `AppendState` captured before
the first attempt, and the SDK's inner retry is disabled for append sends, so every re-send passes
through the outer loop's reconcile.
