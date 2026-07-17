# Paged Receive Buffer

The download delivery path reassembles an object from ranged GETs that complete out of order, and
delivers the reassembled bytes to a consumer — a byte stream read in order, or a file written by
position. The paged receive buffer is the structure that holds in-flight and completed parts between
arrival and delivery; the occupancy gate is the control law that paces issuance into it. This
document covers both: the buffer, the gate, and the read-ahead surface that tunes the gate.

---

## Requirements

### In-order delivery over out-of-order arrival

A download issues many ranged GETs concurrently. They complete in non-deterministic order, from
whichever execution thread finished the read. A stream consumer, however, reads the object front to
back. The buffer must accept a completed part into its position from any producer at any time, and
hand parts to a single stream consumer in strict sequence order, holding an early-arriving later
part until the parts before it arrive.

### Two consumption modes

The delivered bytes go to one of two sinks, and they have different ordering needs:

- **A byte stream** (`ByteStream` to the caller) is read in order. It needs strict in-order delivery,
  one part at a time, each freed as the consumer passes it.
- **A file** is written by position: each part goes to its absolute offset, so a file download does
  not need in-order delivery at all — it drains completed parts to disk as they arrive, out of order,
  and never reads front-to-back.

One buffer serves both. Which mode is in force is fixed per transfer.

### Resident memory bounded independently of issuance and delivery position

Concurrent GETs against a fast network with a slow or blocked consumer will buffer unbounded data if
issuance is not paced. Issuance must be bounded by how much data is *resident* — claimed but not yet
freed — and that bound must be independent of two other quantities it is easily conflated with:

- The **issuance permit** (how many GETs may be outstanding) is a concurrency concern, not a memory
  one.
- The **in-order delivery position** (how far a stream has been read) is meaningful only to the
  stream consumer, and says nothing about resident memory for a file consumer that never advances it.

Resident occupancy, the issuance permit, and the delivery position are three distinct quantities.
Binding issuance to resident occupancy — and to that alone — is the requirement, and it demands a
structure that can track resident occupancy independently of the delivery cursor.

### Issuance paces to consumption, for either consumer

Issuance must run ahead of consumption far enough to keep the pipeline full, and stop when the
consumer falls behind. Critically this has to hold for the file consumer, which frees parts by
draining them to disk and never advances an in-order cursor: pacing keyed on the in-order position
would never observe the file consumer's progress and would stall issuance permanently once the
initial permit was spent.

### Liveness

Every issuance pause must have a wake that resumes it — the consumer freeing occupancy, the memory
budget granting a queued reservation, or a GET completing. A paused transfer that is never woken is a
hung transfer. The pacing gate, the memory budget, and the buffer's producer/consumer handoff each
introduce a pause, and each must be lost-wake-safe. These pauses ride the scheduler's
`Pending`/`wake` lifecycle and inherit its edge-triggered-wake obligation; see
[Edge-triggered wake](./scheduler.md#edge-triggered-wake) in the scheduler design.

---

## Architecture

The pieces: `PagedRecvBuffer` holds the parts; the `OccupancyGate` counts what is resident and gates
issuance; the read-ahead window sets the gate's bound; the `SinkWrite` seam abstracts the file write.
They compose in `poll_work` (issuance) and in the delivery surfaces (consumption).

### PagedRecvBuffer

`PagedRecvBuffer<T>` assigns each payload a sequence number at claim time, accepts completed payloads
into their slots in any order from many producers, and delivers them to a single consumer. It is a
`VecDeque<Arc<Segment<T>>>` under one `Mutex`; each segment is a fixed run of `seg_size` slots
covering a contiguous span of sequences. The deque grows at the tail and reclaims from the front, so
resident memory tracks the gap between the oldest undelivered and newest claimed sequence, not the
total sequence count.

```text
  reclaim ◀── front                        tail ──▶ grow
  ┌─────────┐    ┌─────────┐    ┌─────────┐
  │  seg 0  │─nx▶│  seg 1  │─nx▶│  seg 2  │
  │ x x x x │    │ x x x . │    │ F . _ _ │
  │ 0 1 2 3 │    │ 4 5 6 7 │    │ 8 9     │
  │ base=0  │    │ base=4  │    │ base=8  │
  └─────────┘    └───────▲─┘    └─────────┘
                         │
                    cursor=7   (issued=10)

  x = delivered   F = filled, awaiting delivery
  . = claimed, in flight   _ = not yet claimed   nx = next pointer
```

Seqs 0–6 are delivered (the stream cursor is at 7). Seq 7 is in flight and is the head-of-line gap:
the cursor cannot advance until it fills, even though seq 8 has already arrived and waits behind it.
`seg 0` is fully delivered and eligible for front-reclaim on the next claim.

Segments live behind `Arc`, so they are heap-stable: a `VecDeque` reallocation on growth moves only
the `Arc` pointers, never invalidating a producer's or consumer's reference into a segment. The only
intrusive link is one `next` pointer per segment, by which the stream consumer reaches the successor
segment without taking the lock.

The two hot paths are lock-free. `fill` writes a claimed slot and publishes it with a `Release`
store; `poll_next` reads the cursor slot under an `Acquire` load and hops to the successor segment
via the `next` pointer without taking the lock. Only the issuer paths take the lock — `claim`
(assign a sequence, maybe grow, opportunistically reclaim the front), `take_drain_run` (claim a
filled run for the block surface), and segment reclaim — each a short critical section that never
spans a fill, a delivery, or an I/O. One lock acquisition per claim; none per fill.

**Alternative: a fixed-capacity ring indexed by `seq % capacity`.** A ring gates issuance on
`claimed - consumed < capacity`, which makes one number — the capacity — serve as the issuance
permit, the delivery buffer size, and the memory bound at once; none can move without the others.
And its one `consumed` cursor is the in-order delivery position, the right bound for a stream
consumer but the wrong one for a file consumer that drains out of order and never advances it: gated
on that cursor, a file download larger than the ring can free every buffered part to disk yet never
resume issuance, because `claimed - consumed` stays pinned at capacity. A ring cannot meet the
independence requirement above — it has no place to express resident occupancy apart from the
delivery cursor. Growing and reclaiming a segmented buffer, with occupancy counted separately, is
what provides it.

**Alternative: an out-of-order queue keyed by sequence (RB-tree).** The Linux TCP stack reassembles
out-of-order segments in a red-black tree keyed by sequence, with graduated eviction under memory
pressure (`tcp_prune_queue`). A tree admits arbitrary sparse arrival and arbitrary eviction, which
the kernel needs because a remote peer can send any segment at any time and the socket buffer is a
hard limit. This buffer's arrival pattern is narrower: sequences are claimed densely and
monotonically (the issuer hands out 0, 1, 2, … with no gaps in the *claimed* range), and eviction is
not required because issuance is paced to keep resident memory bounded rather than dropping and
re-requesting data. A dense, monotonic claim range with front reclaim is exactly a paged structure —
a two-level array (segment, slot) with O(1) indexed access and lazy per-segment allocation, the same
shape as a multi-level page table. The tree's per-node ordering cost and pointer overhead buy
flexibility this workload does not use.

### The two surfaces and the single occupancy counter

A buffer instance is consumed through exactly one of two surfaces:

- **Stream** (`RecvBufferConsumer::poll_next`) delivers one payload at a time in strict sequence
  order, emptying each slot as it passes. Its handle is unique and non-cloneable and `poll_next`
  takes `&mut self`, so "single consumer" is a type-level fact: the delivery cursor and current
  segment are fields of the handle, not shared state.
- **Block** (`PagedRecvBuffer::take_drain_run`) hands out a contiguous filled run as an owned
  `SegmentWrite` token for bulk consumption. Runs may be taken and completed out of order and
  concurrently, and one segment may be partitioned into several runs.

The crux is that a single `released` count is incremented by *both* surfaces — once per in-order
stream delivery, and by a run's filled count on each block drain. The occupancy gate reads
`issued - released`, so both a stream consumer that pulls in order and a file consumer that drains
out of order register their progress through the same counter. That is what lets a file download pace
to its drain rate. The in-order delivery cursor (`consumed`) is separate and drives only stream-path
reclaim; the file consumer never touches it.

### The occupancy gate

The buffer above holds parts and reports resident occupancy exactly; what remains is to *pace*
issuance against it. Three pieces cooperate, described over this and the next two subsections. The
**occupancy gate** here is the mechanism: a per-part admission check against a bound. The
**read-ahead window** is that bound — how far issuance may run ahead of consumption — and the public
knob that sets it. The **global memory budget** is a second, independent bound that issuance also
honors, composed with the window. The gate comes first because the window and the budget both feed
into it.

Issuance of a part requires `issued - released < window`. The `OccupancyGate` is a plain accumulator
of two monotonic counts: `issued` (parts claimed for issuance) and `released` (parts whose payload
memory a delivery surface has freed). `issued - released` is resident occupancy in parts — an exact
quantity, recomputed each time, never estimated from a rate and never decaying. `try_issue` admits
and counts a part in one step, or returns "closed" without mutating so the caller parks:

```rust
fn try_issue(&mut self, window: u64) -> bool {
    if self.issued - self.released >= window {
        false          // gate closed; caller parks
    } else {
        self.issued += 1;
        true
    }
}
```

The gate bounds how far issuance runs ahead of consumption and nothing more. The pacing regimes
follow directly:

- **Fast consumer:** `released` keeps pace, occupancy stays low, the gate does not bind, and the
  transfer runs at the concurrency limit.
- **Slow consumer:** occupancy fills to the window, the gate closes, issuance waits. Resident memory
  for this transfer is bounded at `window` parts.
- **Blocked consumer** (a stalled or not-yet-filled head-of-line part): occupancy pins at the window,
  leaving a full window of slack to make progress around the hole rather than collapsing to a single
  outstanding part.

**Alternative: estimate a drain rate and clock issuance to it.** An earlier design paced issuance
against an estimated consumer drain rate — measure how fast the consumer frees parts, issue to match.
Estimation carries error and lag: it must decay stale samples (so a consumer paused between bursts
looks slower than it is), it needs warm-up before the estimate is trustworthy, and it can overshoot
or undershoot the true resident footprint. Counting resident occupancy exactly (`issued - released`)
removes all of that — there is no rate to estimate, no decay constant to tune, no warm-up latch. The
gate reacts to the free-buffer-space subtraction directly, which is both simpler and exactly correct.

### Two composed issuance bounds

The read-ahead window is one of two independent upper bounds on issuance; the other is the global
memory budget — a single process-wide ceiling on resident transfer memory that every transfer
reserves against, so it bounds one transfer's footprint and the aggregate across all of them at
once (the budget's own admission accounting is its concern; see `memory-budget.md`). In `poll_work`
the two compose in a fixed order — window first, budget second — so issuance takes their min:

```text
  try_issue(window)  ──closed──▶  park (consumer will wake us)
        │ open
        ▼
  reserve against budget  ──queued──▶  flush resident run, then park
        │ granted                       (budget grant will wake us)
        ▼
  issue the ranged GET
```

The window is gated first so a window-blocked transfer never holds a slice of the fungible memory
budget it cannot yet use — which would starve other transfers of budget they could use. When the
window admits but the budget queues the reservation, the gate's `issued` bump stays (the parked claim
legitimately occupies its one window seat) and the claimed slot is stashed pending the grant, so a
budget-parked transfer holds exactly the one slot it will fill.

### The drain primitive

The block surface drains through `take_drain_run`, backed by two per-segment cursors:

- `claim_cursor` — leading slots claimed for draining, advanced only under the lock, so a run is
  handed out at most once and concurrent drainers take disjoint runs.
- `drained_count` — slots written to the sink and freed; when it reaches the segment length, every
  claimed slot has been written and the segment is reclaimable.

```text
  one segment, seg_size = 8, drain batch = 3

    slot      0 1 2   3 4 5   6 7
    payload   D D D   F F F   F .
              └run A┘ └run B┘

  claim_cursor  = 6   slots 0–5 claimed for draining (runs A and B)
  drained_count = 3   run A written to the sink and freed; run B claimed and
                      still draining — its slots stay FILLED, only the payload freed
  slot 6 is filled, but the run beyond claim_cursor (slot 6 alone; slot 7 in flight)
  is below the drain batch of 3, so it cannot be claimed yet

  D = drained (payload freed)   F = filled   . = claimed, in flight
```

A *drainable run* is the contiguous `FILLED` slice between `claim_cursor` and the first unfilled slot.
A non-terminal claim takes it once it spans the drain batch, or once it reaches the end of a full
segment (so a sub-batch tail residue still drains); a terminal claim takes whatever contiguous filled
prefix exists. The drain batch is a minimum trigger coupled to the read-ahead window
(`min(seg_size, window)`), so a window smaller than a segment drains in smaller runs instead of never
sealing a segment. Completing a run frees its payloads and advances `drained_count`; dropping the
token without completing runs the same drain, so an aborted consumer still frees its run and cannot
stall reclaim.

### Positioned writes behind a seam

File delivery reads a claimed run's payloads in place and writes them at their absolute object offsets
via `pwritev`, coalescing offset-contiguous payloads into one positioned write. The write target sits
behind a `SinkWrite` trait — `FileSink` for the file path, positioned writes via `pwritev`. The seam
lets the drain orchestration (run coalescing, offset translation) be exercised against an in-memory
capture in tests, and lets an alternative write strategy (O_DIRECT, io_uring) replace the file write
without touching the buffer or the drain logic.

### Read-ahead control surface

`crate::types::ReadAhead { Auto, Parts(n) }` is the public knob for the window, exposed at three
scopes with precedence dynamic > request > client:

- `Config::builder().read_ahead(..)` — client default.
- `download().read_ahead(..)` — per-request override.
- `handle.io_ctl().set_read_ahead(..)` — dynamic, on a running transfer.

`Parts(n)` caps read-ahead at `n` parts of speculation beyond the part the consumer is waiting on, so
`n + 1` are resident and `Parts(0)` is demand paging. `Auto` uses a fixed per-transfer cap
(`DEFAULT_WINDOW_PARTS`), set well above the bandwidth-delay product of a 100 Gbps link so it does not
bind a consumer that keeps pace. The knob resolves to a window in parts in one place,
`window_parts_for`, used both at transfer construction and by the dynamic setter. The window is stored
in an `AtomicU64` so the dynamic setter is a lock-free store the next gate read observes; the value is
otherwise fixed for the transfer. I/O controls live on a `DownloadIoCtl`, separate from scheduling
controls — data movement versus scheduling.

The window is a per-transfer prefetch-depth ceiling, not the memory bound. Resident memory — a
transfer's own and the aggregate across all of them — is bounded by the global memory budget, which
under concurrency typically binds a transfer well before its own window fills. The `Auto` window's
job is only to keep a single uncontended transfer's pipeline full.

---

## Correctness Invariants

The buffer's two lock-free hot paths and its out-of-order block surface rest on a handful of
invariants. Each is stated with what it rules out and the mechanism that upholds it.

### Producer/consumer handoff

**Invariant.** A reader touches a slot's payload only after observing that the producer has published
it.

**What it rules out.** Reading a slot the producer has not finished writing — a data race on the
payload bytes.

**Mechanism.** A slot moves `EMPTY → FILLED` via a `Release` store after the payload write; a reader
observes `FILLED` via an `Acquire` load before reading. The pair forms the happens-before edge:
payload write → `Release` FILLED → `Acquire` FILLED → payload read. This is the only synchronization
the two hot paths need, and it is why they take no lock.

### Exclusive producer write, no slot aliasing

**Invariant.** Each sequence number is claimed once and maps to exactly one slot for that segment's
lifetime; the producer holding the claim is the slot's sole writer.

**What it rules out.** Two producers writing the same slot, or a slot being reused for a second
sequence while the first is still live — either of which would race.

**Mechanism.** The issuer advances `issued` monotonically, so a sequence is claimed once. Segments
are allocated fresh on growth and freed on reclaim, never recycled, so a slot is never the live target
of two sequences — there is no reuse window to race. The quantity the caller must bound is resident
memory, not aliasing (see *Caller obligation*).

### Segment-reclaim safety (the lock-free hop)

**Invariant.** The successor segment a stream consumer hops into is always still resident when it
gets there.

**What it rules out.** The consumer reconstructing an `Arc` from a `next` pointer into a segment that
front-reclaim has already freed — a use-after-free.

**Mechanism.** A front segment is removed (`pop_front`, front-only) once it is fully delivered
(`base + len <= consumed`) or fully block-drained (`drained_count == len`). On the stream path only
the first applies, and only the consumer advances `consumed`; so when the consumer hops, the successor
it is entering has `base == consumed`, which makes `base + len <= consumed` false, and the surfaces
being exclusive keeps `drained_count` at 0. The successor cannot be popped and stays strong-referenced
by the deque, which is what makes the lock-free `Arc` reconstruction from the `next` pointer sound.

### Outstanding block pin

**Invariant.** A segment with a run still being consumed by the block surface stays alive until that
run drains.

**What it rules out.** Front-reclaim freeing a segment out from under an in-flight positioned write.

**Mechanism.** A live `SegmentWrite` token holds an `Arc<Segment<T>>`, so even if front-reclaim pops
the segment from the deque first, the token's strong reference keeps it alive until the write
completes and the token drops.

### Drain claim and reclaim handoff

**Invariant.** A filled run is claimed by at most one drainer, and a reclaimer that frees a segment
has observed all of that segment's payload frees.

**What it rules out.** Two drainers writing the same run, and a segment being reclaimed before its
freed payloads' writes are visible.

**Mechanism.** `claim_cursor` advances only under the deque lock, so concurrent drainers take disjoint
runs. `drained_count` is advanced with `AcqRel` after a run's payloads are freed and read with
`Acquire` by front-reclaim, so a reclaimer observing `drained_count == len` has seen those frees. The
advisory fill probe that raises the drain edge reads `claim_cursor` lock-free and is *not* ordered
against the lock-free fills, so its `filled - claim_cursor` is a saturating subtraction rather than a
bare one — a drainer can transiently be ahead of a producer's not-yet-incremented `filled_count`, and
the per-slot `FILLED` state, not the counters' relative order, gates a safe read.

### Surface exclusion

**Invariant.** An instance is driven through the stream surface or the block surface, never both.

**What it rules out.** The stream `take` emptying a slot while the block surface reads the same slot's
payload in place — a data race, and the reason both paths' `unsafe` is sound.

**Mechanism.** This is a caller obligation, not type-enforced: `PagedRecvBuffer` is `Clone` and
exposes the block surface, so the type system does not prevent driving both. A block-only caller drops
the `RecvBufferConsumer`. The `unsafe` in both paths relies on the caller honoring the exclusion.

### Lost-wake safety at the occupancy gate

**Invariant.** Every gate-closed park has a matching wake from the consumer's next release.

**What it rules out.** The issuer reading a closed gate and parking while, concurrently, the
consumer's release that would reopen it is lost — a permanently stalled transfer with a non-empty
buffer.

**Mechanism.** There is no lock-free path to `released`. The issuer's `try_issue` and the consumer's
`release` both mutate the gate under the transfer's state lock, following the scheduler's mutator
discipline `lock → mutate → unlock → try_wake` (see
[Edge-triggered wake](./scheduler.md#edge-triggered-wake)). Because both sides hold the same lock,
the interleaving that would lose a wake is unrepresentable rather than merely avoided: either the
release is visible to the issuer's gate check (so it does not park), or the issuer parks first and
the release's post-unlock `try_wake` observes the pending flag and fires.

### Budget-park deadlock avoidance

**Invariant.** A transfer that parks on the memory budget must not do so while holding a drainable
resident run it has no path to release.

**What it rules out.** A cross-transfer deadlock: a disk transfer parks on the budget holding a
resident run that is below the drain batch (so no fill-triggered drain frees it) and cannot reach
terminal (its remaining part is the one blocked on the budget). Spread across concurrent disk
transfers, none can release, so none is granted.

**Mechanism.** Before parking on the budget, a transfer holding a drainable resident run emits a
`DrainResident` work item to flush the run (releasing its budget), and parks only when there is
nothing left to flush. The drain runs in `execute` — `poll_work` does no I/O — and its completion
re-polls the transfer, so the freed budget re-grants in FIFO order.

### Caller obligation

The buffer does not bound its own growth: it allocates a segment whenever a claim reaches a sequence
past the current tail. Bounding outstanding (claimed-but-undelivered) sequences is the caller's job —
here, the occupancy gate. Without such a bound, resident memory grows without limit.

---

## Open Questions

**`Auto`-mode resident-size adaptation.** The `Auto` window is a fixed per-transfer part count sized
above a 100 Gbps BDP. It does not adapt to the actual object part size or to how much memory a
transfer is truly resident. A window expressed in bytes, or adapted to observed part size, would bound
small-part transfers more tightly and let large-part transfers keep fewer parts in flight. Today the
global memory budget is what actually binds resident memory under concurrency; the per-transfer window
only keeps a single uncontended transfer's pipeline full.

**Cross-transfer arbitration lives in the budget, not the window.** The window bounds one transfer.
Fair division of a shared memory ceiling across many concurrent transfers is the memory budget's
concern (see `memory-budget.md`), and the two compose as a min in `poll_work`. Whether any policy
belongs at the window layer (e.g. per-transfer windows that shrink as concurrency rises) or all of it
stays in the budget is open.

---

## Future Work

**Alternative sink strategies behind `SinkWrite`.** The seam is in place; O_DIRECT and io_uring write
paths can replace `FileSink`'s `pwritev` without touching the buffer or the drain orchestration.

**Move blocking file operations off the runtime.** The disk drain executes its positioned write inline
on the execute path, and temp-file create/rename happen on the caller's runtime. Moving these onto
managed threads is orthogonal to the buffer and tracked separately.

**Proactive reclaim.** Reclaim today is front-only and opportunistic — a segment is freed on the next
`claim` once it is fully delivered or drained, so the buffer never sheds memory ahead of delivery. A
consumer that seeks or abandons already-buffered parts could free memory sooner by reclaiming parts
the consumer has moved past. This is not a buffer-local change: a proactively-freed part may be needed
again, so the transfer must be able to re-issue it, which means rolling back the state machine's
issuance position rather than advancing it monotonically. The buffer, the occupancy gate, and the
issuer would need to agree on a reclaim-and-re-issue protocol; the current monotonic-issuance model
does not support it.
