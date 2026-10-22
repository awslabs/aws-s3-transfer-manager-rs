# Design documents

Each design document states a subsystem's requirements, architecture, and correctness invariants. A change that touches a subsystem must preserve the invariants listed for it. A change that alters an invariant must update the document in the same pull request.

| Document | Covers | Code |
|---|---|---|
| [scheduler.md](scheduler.md) | Work generation, fair scheduling, concurrency control, backpressure, and the contracts every `Transfer` state machine upholds | `scheduler/`, `transfer.rs` |
| [memory.md](memory.md) | The shared buffer pool: admission, accounting, reuse, and reclamation | `runtime/buffer_pool/` |
| [paged-recv-buffer.md](paged-recv-buffer.md) | Download reassembly, the occupancy gate, and read-ahead | `operation/download/recv_buffer.rs`, `operation/download/read_ahead.rs` |
| [retry.md](retry.md) | The outer retry loop, failure classification, and throttle recovery | `retry.rs` |
| [hedging.md](hedging.md) | Speculative re-issue, the adaptive deadline, and the hedge budget | `retry.rs` |
| [upload.md](upload.md) | Single-object upload: path selection, source partitioning, the multipart state machine, request mapping, and append mode | `operation/upload.rs`, `operation/upload/`, `io/part_reader.rs` |

Code paths are relative to `aws-sdk-s3-transfer-manager/src/`.

## Invariant index

This index locates each invariant. The linked section is authoritative.

### Scheduler and transfer state machines

- [State machine contracts](scheduler.md#state-machine-contracts): failed lifecycle, poll-time pending/wake obligation, execution-yield obligation, panic safety
- [Edge-triggered wake](scheduler.md#edge-triggered-wake)
- [Execution-time yield](scheduler.md#execution-time-yield)
- [Single-poll exclusivity](scheduler.md#single-poll-exclusivity)
- [Bounded per-call cost](scheduler.md#bounded-per-call-cost)

### Memory

- [A live carrier is charged and prepared](memory.md#a-live-carrier-is-charged-and-prepared)
- [Admitted work does not wait for another owner](memory.md#admitted-work-does-not-wait-for-another-owner)
- [Admission preserves order and backpressure](memory.md#admission-preserves-order-and-backpressure)
- [Reservation close does not release owned bytes](memory.md#reservation-close-does-not-release-owned-bytes)
- [Published bytes never alias writable memory](memory.md#published-bytes-never-alias-writable-memory)
- [Reclamation cannot overtake ownership](memory.md#reclamation-cannot-overtake-ownership)
- [Release survives shutdown](memory.md#release-survives-shutdown)

### Download delivery

- [Producer/consumer handoff](paged-recv-buffer.md#producerconsumer-handoff)
- [Exclusive producer write, no slot aliasing](paged-recv-buffer.md#exclusive-producer-write-no-slot-aliasing)
- [Segment-reclaim safety](paged-recv-buffer.md#segment-reclaim-safety-the-lock-free-hop)
- [Outstanding block pin](paged-recv-buffer.md#outstanding-block-pin)
- [Drain claim and reclaim handoff](paged-recv-buffer.md#drain-claim-and-reclaim-handoff)
- [Surface exclusion](paged-recv-buffer.md#surface-exclusion)
- [Lost-wake safety at the occupancy gate](paged-recv-buffer.md#lost-wake-safety-at-the-occupancy-gate)
- [Admission-park progress](paged-recv-buffer.md#admission-park-progress)
- [Caller obligation](paged-recv-buffer.md#caller-obligation)

### Retry and hedging

- [Bounded termination](retry.md#bounded-termination)
- [A hedge never consumes a transport retry](retry.md#a-hedge-never-consumes-a-transport-retry-1)
- [Corrupt data is never re-fetched](retry.md#corrupt-data-is-never-re-fetched)
- [Throttle isolation](retry.md#throttle-isolation)
- [Transient classification sees through nested wrapping](retry.md#transient-classification-sees-through-nested-wrapping)
- [Speculation is self-limiting](hedging.md#speculation-is-self-limiting)
- [A hedge is armed at most once per operation](hedging.md#a-hedge-is-armed-at-most-once-per-operation)
- [A timeout never ratchets the deadline tighter](hedging.md#a-timeout-never-ratchets-the-deadline-tighter)
- [A uniformly slow link is never hedged](hedging.md#a-uniformly-slow-link-is-never-hedged)
- [Hedging concurrency safety](hedging.md#concurrency-safety)

### Upload

- [Part accounting balances exactly once](upload.md#part-accounting-balances-exactly-once)
- [Completion is elected once](upload.md#completion-is-elected-once)
- [The stored object matches the source's declaration](upload.md#the-stored-object-matches-the-sources-declaration)
- [A retry re-sends identical bytes](upload.md#a-retry-re-sends-identical-bytes)
- [A parked part read is never lost or recreated](upload.md#a-parked-part-read-is-never-lost-or-recreated)
- [An open multipart upload is always reachable](upload.md#an-open-multipart-upload-is-always-reachable)
- [One append in flight](upload.md#one-append-in-flight)
- [Each append is conditioned on the state the previous one produced](upload.md#each-append-is-conditioned-on-the-state-the-previous-one-produced)
- [A re-send is never unconditional](upload.md#a-re-send-is-never-unconditional)
