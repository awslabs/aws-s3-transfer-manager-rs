# Retry

A transfer moves data with many concurrent S3 requests, and some fail: a connection resets
mid-body, a burst of parts trips the client's socket buffers, a prefix under heavy fan-out starts
shedding load with throttles. The SDK retries what it can, but its retry finishes before a
download body is read, so a mid-stream body failure is never covered. This layer is an outer retry
loop that wraps each data-plane request, recovering the failures the SDK's inner retry leaves on the
table and pacing recovery so a re-issue does not re-collide with the condition that caused the
failure.

It has four parts: the **loop** (bounded attempts, per-path classifiers, a jittered-backoff
schedule with two bases), **throttle recovery** (a distinct backoff and a per-bucket retry token
bucket that isolates and recovers from throttle storms), **transient-transport classification** (a
chain-walk that recovers a marker the SDK's own classifier misses), and **stalled-stream
protection** (a tightened grace period that re-issues a dead-but-not-errored connection). It also
consumes the hedge signal — a `GuardError::DeadlineExceeded` from
[hedging](./hedging.md) is a retryable arm, and the free-hedge rule that keeps a speculative cancel
from spending a genuine retry lives here.

---

## Requirements

### Recover the body-read gap the SDK cannot see

The SDK's GetObject orchestration — including its own retry — completes when the response head
returns. The body stream is consumed *after* that, by this crate. A failure while draining the body
(a reset, a truncation, a stalled stream) is therefore invisible to the SDK's retry: it has already
declared success. This loop is the *sole* retrier for the body-read path.

### Survive and recover from throttle storms under fan-out

A directory download or a many-part upload issues a high fan-out of requests to one bucket, and S3
responds to overload with throttles (`SlowDown` and friends). Two failure modes have to be handled:
the immediate one (a re-issue must back off enough to let the service recover, not hammer it), and a
subtler one — the SDK's shared retry token bucket drains under the storm and starts returning
throttles *un-retried*, with no recovery path, so the transfer aborts. Recovery must restore a
bounded retry budget over time and isolate one bucket's storm from every other bucket on the client.

### Classify transport failures correctly despite nested wrapping

Whether a failure is a *transient transport* error (safe to re-issue) or a modeled service response
(often not) decides retryability. A nested dispatch inside the runtime can re-wrap a fully-classified
inner connector error inside an outer `ConnectorError::Other(None)` that carries no marker. Both the
SDK's classifier and a naive check inspect only the outer frame and misclassify the error as
terminal. Classification must see through the wrapping.

### Every failure path bounded and terminating

Retries are capped per operation. Backoff is bounded. The hedge is one-shot. No error, and no
combination of a hedge with genuine failures, can make an operation retry unboundedly.

### Re-issue without re-colliding

A re-issue that fires immediately, or in lockstep with every other simultaneously-failed request,
re-collides with the condition that caused the failure — an empty token bucket, an exhausted socket
buffer, an overloaded prefix. Backoff must be jittered to de-correlate a burst, and its base must
match the failure: fast for a transient transport blip, slow for a service shedding load.

---

## Architecture

The pieces: `retry(classify, build)` is the loop; `RetryDecision` is a classifier's verdict;
`Backoff` is the pure jittered schedule; per-path classifiers (`classify_body_retry`,
`classify_discovery_retry`, `classify_upload_part_retry`) map an error to a decision;
`bucket_retry_partition` supplies the per-bucket token bucket; `is_sdk_transient_transport` does the
chain-walk classification; `tightened_ssp` configures stalled-stream protection.

### An outer loop over the SDK's inner retry

The download body path is where the SDK's retry falls short, so it is the clearest illustration. The
SDK's GetObject orchestration — including its own retry — completes when the response head returns;
this crate then consumes the body stream, and the outer loop is the only retrier covering that read:

```text
  ┌──────────────────────────── retry() ───────────────────────────────┐
  │  build(allow_hedge) each attempt ──► a fresh, identical request      │
  │        │                                                             │
  │        ▼                                                             │
  │  ┌──────────────────── one SDK GetObject ────────────────────────┐  │
  │  │  send ─► [SDK inner retry: throttle, transient, quota] ─► head │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │        │ response head returns; SDK retry is DONE                    │
  │        ▼                                                             │
  │  read body stream  ◄── the SDK never retries here; this loop does   │
  │        │                                                             │
  │        ▼                                                             │
  │  classify(err) ─► Retry | RetryThrottle | NoRetry                   │
  │        │                                                             │
  │        ▼  sleep full-jitter backoff (base by decision), then rebuild │
  └──────────────────────────────────────────────────────────────────────┘
```

The upload part-send and discovery paths use the same loop without the body-read stage: there the
loop's value is throttle-storm recovery and transient-transport classification, not a coverage gap.

The loop calls `build(allow_hedge)` for a fresh future each attempt; `build` must produce an
identical request each call so a retry re-sends or re-reads the same data. It re-issues up to
`MAX_ATTEMPTS` (3), deciding each failure via a caller-supplied `classify` and sleeping a
full-jittered backoff keyed on the 0-based retry index before rebuilding.

**Alternative: rely on the SDK's retry alone.** The SDK's retry is capable and covers the send path,
but it structurally cannot cover a download body read (consumed after orchestration completes), and
its shared retry token bucket has no time-based refill, so a drained bucket has no recovery path.
The outer loop exists for exactly the gaps the inner retry leaves.

### Classifiers own the verdict, the loop owns the schedule

A classifier is a pure function of the error returning a `RetryDecision` — `Retry` (fast transient
base), `RetryThrottle` (hard throttle base), or `NoRetry`. It never decides *how long* to wait; the
loop owns the schedule and selects the `Backoff` by the decision. This keeps timing policy in one
place and makes each classifier a small, exhaustively-testable match. Three exist, one per path:

- **`classify_body_retry`** — the download body path. Retries a latency-deadline straggler
  (`DeadlineExceeded`), a transient-transport send failure, a mid-stream body failure (`IOError`:
  reset, truncation, SSP stall), and a throttle. A checksum mismatch (`IntegrityError`) is terminal —
  a corrupt body must never be re-fetched and masked.
- **`classify_discovery_retry`** — the discovery send (GetObject / HeadObject / partNumber GET). No
  body, no deadline. Retries a throttle and a transient transport; everything else, including
  `InvalidRange` and `NotFound`, is terminal.
- **`classify_upload_part_retry`** — the upload part-send. Retries a transient transport and a
  throttle; a modeled non-throttle service error is terminal. Uploads carry no deadline, so the
  `DeadlineExceeded` arm is unreachable but retained for exhaustiveness.

### The backoff schedule

`Backoff` is full-jitter truncated-exponential: `delay = b · min(initial · 2^i, max)` with `b`
uniform in `[0, 1)`. Full jitter — the whole capped value scaled, not a band around the base —
spreads a burst of simultaneous failures across the entire `[0, ceiling]` window, de-correlating
re-issues so they do not re-collide. `Backoff` owns no RNG; the caller supplies the draw, so `delay`
is a pure function of `(retry_index, rand_unit)` and directly testable.

Two bases, one cap:

- **Transient (100 ms base).** Larger than the SDK's own transient base because this is an *outer*
  retry that fires only after the SDK's inner retry exhausted; the re-issue should span the window in
  which in-flight work completes and refills the shared retry quota, rather than collide with the
  still-drained bucket.
- **Throttle (1 s base).** The SDK standard retry mode's throttling base, 10× the transient base — a
  service shedding load is given room to recover before a re-issue. A transient transport blip is safe
  to re-issue fast; a throttle is not.
- **Cap (5 s).** Below the SDK standard mode's 20 s, because a multi-second stall on one part of a
  larger transfer is pathological.

### Consuming the hedge signal

`GuardError::DeadlineExceeded` from a composed `guarded` (see [hedging](./hedging.md)) is not a
failure — nothing errored, a request was speculatively cancelled for being slow. The loop treats it
as a retryable arm: it re-issues, and the terminal case renders it to an `IOError` naming the
deadline the final attempt exceeded. `build` receives `allow_hedge`, which the loop passes `true`
until the first exceedance and `false` after (**hedge-once**), so an operation is speculatively
cancelled at most once and later attempts run untimed.

#### A hedge never consumes a transport retry

The one-time hedge is a **free** re-issue: it does not advance the attempt counter. A chunk that
hedges once and then hits genuine transient-transport failures still gets the full `MAX_ATTEMPTS`
genuine retries — a speculative cancel must not steal a failure-recovery attempt. This is the
free-hedge rule, and it lives here because it is a property of the loop's attempt accounting
(the decision arm of the loop, simplified for exposition):

```rust
let is_hedge = matches!(ge, GuardError::DeadlineExceeded(_));
let free_hedge = is_hedge && !hedged;          // free only for the FIRST exceedance
let backoff = match classify(&ge) {
    RetryDecision::NoRetry => return Err(into_error(ge)),
    _ if !free_hedge && attempt >= MAX_ATTEMPTS => return Err(into_error(ge)),
    RetryDecision::Retry => &transient,
    RetryDecision::RetryThrottle => &throttle,
};
tokio::time::sleep(backoff.delay(attempt - 1, fastrand::f64())).await;
if is_hedge { hedged = true; }
if !free_hedge { attempt += 1; }               // the free hedge does not advance the counter
```

The loop still terminates: at most one exceedance is free, and after it `guarded` runs untimed and
cannot produce another, so the worst case is `MAX_ATTEMPTS + 1` iterations. The case the free-hedge
rule protects: a chunk hedges once, then fails transiently twice, and still succeeds on its third
*genuine* attempt.

**Alternative: charge the hedge an attempt.** Then a chunk that hedged and then hit a real transport
failure would have only two genuine retries instead of three; under a correlated transport burst (a
connect-timeout wave) that thinned budget is the difference between recovery and abort. The free
hedge keeps the full failure-recovery budget available whether or not the operation also sped up its
tail — a speculative cancel and a genuine failure are different currencies and should not draw on the
same account.

### Throttle recovery

Retrying a throttle with the right backoff (above) handles the immediate case. The subtler failure —
the SDK's shared retry token bucket draining under a storm — needs two fixes to the token bucket
itself.

**Per-bucket partitioning.** The SDK shares one retry token bucket per `RetryPartition`, and its
default partition is region-wide (`s3-{region}`). So a throttle storm on one S3 bucket drains the
retry budget for *every other bucket* on the same client. Keying the partition by S3 bucket
(`s3-tm-{bucket}`) isolates them — each bucket gets its own token bucket — matching CRT, which
partitions per S3 host.

```text
  SDK default: one region-wide bucket        TM: one token bucket per S3 bucket
  ┌───────────────────────────────┐          ┌─────────┐ ┌─────────┐ ┌─────────┐
  │  s3-{region} retry budget      │          │ bucket  │ │ bucket  │ │ bucket  │
  │  ┌──────┐ storm on bucket A    │          │   A     │ │   B     │ │   C     │
  │  │ A B C│ drains it for B, C   │          │ storm   │ │ intact  │ │ intact  │
  │  └──────┘ too                  │          │ drains  │ │         │ │         │
  └───────────────────────────────┘          └─────────┘ └─────────┘ └─────────┘
```

**Time-based refill.** The SDK's default bucket has `refill_rate = 0`: retry budget returns *only* as
requests succeed. Under a storm that drains it before any success lands, there is no recovery path.
A low refill (`10 tokens/s`) restores a bounded budget over time. Under a total outage the sustained
retry rate to S3 is `refill_rate / throttling_retry_cost` (the SDK's throttle cost is 5) ≈ 2
retries/s per bucket, independent of concurrency — a trickle-probe of recovery rather than a re-flood
of a service that is shedding load. Capacity stays at the SDK default (500); a larger pool would only
absorb a larger initial burst (more amplification) without addressing recovery.

The partition carries a freshly-allocated token bucket, so it is built once and cached on the Handle
(`bucket_partition_override`) — all operations and retries to a bucket share one live instance.
Building it per-operation would give each request its own bucket and defeat the shared budget.

### Transient-transport classification

Whether an `SdkError` is a transient transport failure decides retryability, but the crate's
flattened `Error` erases the `SdkError` variant, so the classification is captured at conversion.
The subtlety is the nested wrap:

```text
  SdkError::DispatchFailure
    └─ ConnectorError::Other(None)          ◄── outer frame: NO transient marker
         source: ConnectorError::Other(Some(TransientError))   ◄── inner: the real marker
                   source: IncompleteMessage (truncated HTTP response)

  SDK's TransientErrorClassifier: inspects the OUTER frame only ─► "not transient" ─► terminal
  is_sdk_transient_transport:     walks the source chain ─► finds the inner marker ─► transient
```

A nested dispatch in the runtime (a per-thread `http_client_fn`) re-wraps a fully-classified inner
connector error inside an outer `Other(None)`. `is_sdk_transient_transport` walks the error source
chain: it checks the outer `ConnectorError` (is-IO, is-timeout, or an `Other(TransientError)`
marker), then `downcast_ref`s every `ConnectorError` in the source chain for the same, recovering the
inner classification the outer frame hides. It deliberately excludes service responses — a 503
`SlowDown` arrives as a `ServiceError`, not a `DispatchFailure`, so throttling is never misclassified
as transient here (which matters: a throttle wants the throttle backoff, not the fast transient one).

The chain-walk is a workaround: the double-wrap should be corrected at its source in the runtime so a
single frame carries the marker; see [Future Work](#future-work).

### Stalled-stream protection

A connection can go dead without erroring — bytes simply stop arriving. Stalled-stream protection
(SSP) aborts a body whose *throughput* stays at zero for a grace period and re-issues on a fresh
connection. `tightened_ssp` sets the grace to 2 s, below the SDK's 5 s default: it fires only on zero
byte-progress, so a slow-but-progressing stream is never affected, and a tighter grace only shortens
how long a genuinely dead gap wedges the transfer. It bounds a mid-download-body dead connection and
a mid-upload-body stall (the transferring peer stops making progress); it does *not* bound a response
that never arrives after the request body is fully sent — that gap is noted in
[Open Questions](#open-questions).

---

## Correctness Invariants

The loop and its classifiers rest on a handful of invariants. Each is stated with what it rules out
and the mechanism that upholds it.

### Bounded termination

**Invariant.** A single `retry` call runs at most `MAX_ATTEMPTS + 1` iterations and always returns.

**What it rules out.** An operation spinning on retries or hedges — a persistent retryable error, or a
hedge combined with genuine failures, running unboundedly.

**Mechanism.** Genuine failures advance `attempt`, which is capped at `MAX_ATTEMPTS`. The hedge is
free at most once (`free_hedge = is_hedge && !hedged`); after the first exceedance `hedged` is set and
never cleared, so a second exceedance would consume an attempt. The `+1` is the single free hedge.
`NoRetry` returns immediately.

### A hedge never consumes a transport retry

**Invariant.** The first `DeadlineExceeded` re-issues without advancing the attempt counter.

**What it rules out.** A speculative cancel stealing a genuine failure-recovery attempt, so a chunk
that both hedged and then failed transiently aborts with retries to spare.

**Mechanism.** `attempt += 1` is guarded by `!free_hedge`; the first hedge skips it.

### Corrupt data is never re-fetched

**Invariant.** An integrity error (checksum mismatch) is terminal on the body path.

**What it rules out.** A corrupt body being re-fetched and a subsequent clean read masking the fact
that the object, or the stored data, is wrong.

**Mechanism.** `classify_body_retry` maps `ErrorKind::IntegrityError` to `NoRetry`, and the mapping
is covered end-to-end through the full stack.

### Throttle isolation

**Invariant.** A throttle storm on one S3 bucket does not drain another bucket's retry budget.

**What it rules out.** One overloaded prefix taking down retries for every other bucket a client
touches.

**Mechanism.** The retry partition is keyed per S3 bucket (`s3-tm-{bucket}`), each with its own token
bucket, cached once on the Handle so all operations to a bucket share one live instance.

### Transient classification sees through nested wrapping

**Invariant.** A transient marker anywhere in the `ConnectorError` source chain classifies the error
as transient transport.

**What it rules out.** A re-wrapped `Other(None)` outer frame hiding an inner `TransientError` marker,
misclassifying a retryable transport failure as terminal and aborting a recoverable transfer.

**Mechanism.** `is_sdk_transient_transport` checks the outer `ConnectorError` and walks the source
chain, `downcast_ref`-ing every `ConnectorError` for an IO / timeout / `TransientError` classification.

---

## Open Questions

**`is_throttle` is status-blind.** Throttle detection matches on the parsed service error *code*
(`SlowDown`, `Throttling`, …). A throttle that arrives as an HTTP *status* (429 or 503) without a
recognized code — some `HeadObject` 503 responses — is not classified as a throttle here and falls
through to the SDK's inner retry only. Plumbing the raw HTTP status into the flattened `Error` would
let the classifier catch these; it is not wired today.

**The upload response-wait gap.** SSP bounds a stall *during* body transfer, but not a response that
never arrives after the request body is fully sent — the connection is not zero-throughput (the send
completed), it is simply waiting on a response that will not come. Neither SSP nor hedging (which is
download-only today) bounds this. It needs either an upload-side deadline or a response-wait timeout.

---

## Future Work

**Fix the transient double-wrap at its source.** The chain-walk in `is_sdk_transient_transport` is a
workaround for a nested dispatch in the runtime (`http_client_fn`) that re-wraps a classified
connector error inside a bare `Other(None)`. Correcting the wrap so a single frame carries the marker
would let both the SDK's own classifier and this one work off the outer frame, and the chain-walk
could be removed.

**Extend the hedge to uploads.** Retry already covers the upload part-send path; the latency deadline
that would make a slow part-send a hedge candidate does not. See [hedging](./hedging.md#future-work).

**Status-based throttle detection.** Plumb the raw HTTP status (429/503) through the flattened error
so `is_throttle` catches code-less throttles (see Open Questions).
