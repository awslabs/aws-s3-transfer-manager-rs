# Memory Budget

Concurrent transfers hold data in memory between the network and the consumer: parts fetched
ahead of a slow reader, parts buffered for out-of-order reassembly, parts staged for upload. A
fast network with a slow consumer, multiplied across many transfers, grows that resident memory
without bound. The memory budget is a single process-wide admission cap on how much transfer data
may be resident at once. It grants reservations before a transfer commits memory, parks a transfer
when the cap is reached, and wakes it when memory frees — so the client backpressures instead of
running the host out of memory.

The budget bounds how much resident data *exists*. It does not allocate, pool, or recycle that
data — buffers are allocated normally, and the budget only caps their aggregate. Reducing the
allocator churn behind that resident memory is a separate concern (see [Future Work](#future-work)).

---

## Requirements

### Bound resident transfer memory across the whole client

The cap is global, not per-transfer. One hundred transfers sharing a host draw from one budget, so
the bound holds regardless of how many run concurrently or how the load is distributed among them.
A per-transfer allowance cannot do this: it either sums to more than the host can bear at high
transfer counts, or throttles a single transfer needlessly when it is the only one running.

### Account without knowing part sizes

Transfers use different part sizes — a download slices to its part size, an upload takes the
caller's, a directory transfer mixes many object sizes. The budget must account all of them against
one ceiling without a per-transfer or per-part-size unit, and the accounting error must stay bounded
regardless of the sizes in play.

### Admission must not starve any request

A request that fits must eventually be admitted. Under contention a stream of small requests must
not indefinitely defer a larger one, and no request may be dropped or refused outright — the budget
delays, it does not deny. The wait for any request that could ever fit must be bounded.

### A request larger than the whole budget must still make progress

An upload of an un-sliced object, or any single reservation exceeding the entire capacity, cannot
be satisfied by waiting for free space — the space will never be enough. It must still complete
rather than park forever, without letting an oversized request corrupt the bound for everyone else.

### Release must never depend on scheduling

A transfer frees budget by consuming the data it reserved. That release path must not route through
any resource the transfer is simultaneously contending for — in particular not the scheduler's
dispatch capacity. If freeing memory requires a scheduling turn, and that turn is gated by capacity
the memory-holding transfers occupy, the client deadlocks (see [Correctness
Invariants](#release-is-decoupled-from-scheduling)).

### Size the budget to the machine without configuration

The default must be safe on a 2 GiB container and a 768 GiB box alike, leaving headroom for the OS
page cache and the rest of the process, with no user tuning. An explicit override must be available
for callers who know their environment.

---

## Architecture

The pieces: `MemoryBudget` holds the cap and the wait queue; a `Reservation` is an RAII ticket for
granted chunks whose drop returns them; `reserve` grants or parks and `try_reserve` is its
allocation-free fast path; a parked request holds a `WaitTicket` that yields its `Reservation` once
the budget grants it. Sizing is resolved once at client construction from a `MemoryBudgetConfig`.

### Chunk accounting

The budget counts in fixed-size **chunks**, not bytes. The chunk is a nominal accounting unit
(8 MiB), unrelated to any transfer's part size: a reservation for `n` bytes costs
`ceil(n / chunk)` chunks. A 16 MiB part against an 8 MiB chunk costs two chunks; a 20 MiB part costs
three. Counting in a fixed unit keeps the budget part-size-agnostic — one integer `in_use` count
serves every transfer regardless of its part size — and bounds the accounting error to within one
chunk per reservation for parts at or above the chunk size.

The error bound is one-sided and only tight for large parts. A reservation always rounds *up* to a
whole chunk, so a sub-chunk object over-accounts by up to nearly a full chunk: a 256 KiB object
still costs one 8 MiB chunk. For part-sized downloads and multi-megabyte uploads this is under one
chunk of slack on a many-chunk reservation; for a workload of many small objects it is the dominant
term, and the effective concurrency the budget admits is set by the chunk count, not the objects'
true size (see [Open Questions](#chunk-granularity-over-accounts-for-small-objects)).

**Alternative: byte-exact accounting.** Tracking exact bytes removes the small-object over-accounting
entirely, but makes every reservation a variable-width claim against the cap, which complicates the
fit check and the wait-queue drain and offers no benefit for the part-sized transfers that dominate
throughput workloads. The chunk trades a bounded, mostly-negligible over-count for a single-integer
accounting model.

### Reservation lifecycle

A reservation is acquired before a transfer commits the memory and released when the data is
consumed:

```text
  claim a slot ─► reserve(bytes) ─┬─ Ready(Reservation) ─► fill ─► deliver / drain ─► drop
                                  │                                                    │
                                  └─ Pending(WaitTicket) ─► [parked] ─► granted ───────┘
                                                                                       │
                                              drop returns chunks ◄────────────────────┘
                                                        │
                                                        ▼
                                              drain parked waiters that now fit
```

`Reservation` is an RAII ticket: it holds a chunk count and an `Arc` to the budget, and its `Drop`
returns the chunks and drains the wait queue. The reservation is attached to the buffer slot at
claim time and moves into the delivered payload, so it drops exactly when the data it accounts for
is released — on the stream path when the consumer drops the delivered part, on the disk path when
the drain frees the part's payload after writing it. Release is thus tied to consumption, not to any
explicit accounting call a code path might miss.

`reserve` returns `Ready` with an immediate grant when the queue is empty and the request fits,
otherwise it enqueues a waiter and returns `Pending` with a `WaitTicket`. The caller holding a
`WaitTicket` is woken by the budget's notify (wired to `scheduler.wake`) and re-polls to `take` the
granted `Reservation`. The wake does not carry the memory: the releasing transfer's drain reserves
the chunks in the waiter's name — storing the `Reservation` in its slot — *before* firing the notify,
so the wake is only a nudge to come collect a grant that already exists.

```text
  transfer A releasing                          transfer B parked on the budget
  ────────────────────                          ───────────────────────────────
                                                reserve() → Pending(WaitTicket)
                                                poll_work → Pending   [slot: Waiting]

  drop Reservation
    └─ lock budget; in_use −= n
       drain: B's need fits →
         slot ← Granted(R)     ───────────────▶ [slot: Granted(R)]   (B still parked)
         collect B's notify
       unlock budget
    └─ notify()  = scheduler.wake(B) ─────────▶ woken → poll_work
                                                  ticket.take() → Some(R)
                                                  attach R, issue the request

  grant is stored (slot ← Granted) BEFORE the wake fires, and the wake fires
  AFTER the budget lock is released — so a wake is never lost, and the notify
  path never nests the budget lock inside a slot lock.
```

Because the grant is a real `Reservation` sitting in the slot, a grant that is never collected is not
a leak. If the caller drops its `WaitTicket` without taking — its transfer cancelled or failed after
the wake — the ticket's drop drops the still-slotted `Reservation`, returning its chunks by the same
RAII path as any release and draining the next waiter. A ticket dropped *before* a grant (still
`Waiting`) instead removes its waiter from the queue and re-drains, since vacating a queue position
can expose capacity that arrival order had held back from the waiters behind it.

`try_reserve` is the allocation-free fast path: it grants under exactly the same condition `reserve`
would return `Ready`, but registers no waker and never enqueues, so a caller that can re-drive itself
avoids building a notify closure on the common uncontended path.

### Grant-on-release, in arrival order

When a reservation drops, the freed chunks are handed directly to parked waiters rather than left
for waiters to re-poll and re-contend. The drop path walks the wait queue front-to-back, grants each
waiter that fits, and stores the `Reservation` into the waiter's slot for it to `take` — then wakes
it. Admission is strictly first-come-first-served: the drain grants the front waiter if it fits and
**stops at the first that does not**, never serving a smaller request queued behind a larger one.

```text
  a release frees 2 chunks; drain sweeps the queue front → back

              front ─────────────────────────────▶ back
            ┌───────┐   ┌───────┐   ┌───────┐   ┌───────┐
   grant ◀──│ need 2│   │ need 5│   │ need 1│   │ need 3│
            └───────┘   └───────┘   └───────┘   └───────┘
              fits         STOP        (not reached)
              (2 ≤ 2)    (5 > 2 free)

  drain stops at need-5, though need-1 behind it would fit. serving it
  early would let a stream of small requests defer need-5 forever; holding
  the order bounds need-5's wait, at the cost of idling 2 chunks until it fits.
```

This is what bounds the wait. Once a request is queued, `in_use` only falls until its need is met —
no later arrival can jump it and no smaller request behind it can be served first — so a stream of
small requests can never indefinitely defer a larger one. The cost is head-of-line blocking: a large
request at the front idles free chunks while they accumulate to its need. That arises only under
mixed part sizes; a uniform part size never triggers it.

**Alternative: wake all waiters and let them re-contend (wake-and-retry).** Releasing chunks and waking
every parked waiter to retry `reserve` lets them race for the freed space. Under contention this
starves: a waiter that loses the race re-parks at the back, so a request can be passed over
repeatedly with no bound on its wait. Grant-on-release with FIFO ordering makes the front waiter's
admission monotonic instead of a repeated lottery.

**Alternative: priority-ordered admission.** Serving high-priority reservations first would let an
interactive transfer preempt a background one for memory. Without a reclaim mechanism — the ability
to revoke memory already granted to a lower-priority transfer — priority in the admission queue alone
is a half-measure: it reorders who waits, but a background transfer holding the budget still blocks a
foreground one until it releases on its own. Admission stays FIFO until reclaim exists to make
priority meaningful.

### The forced grant for oversized requests

A request needs `need <= capacity - in_use` to fit — except when `in_use == 0`, where it is granted
unconditionally regardless of `need`. This forced grant is the sole path by which a request larger
than the entire capacity makes progress: it can only ever run when nothing else holds the budget, and
strict FIFO guarantees that point is eventually reached for the front waiter. Downloads slice to part
size and never hit it; it is the backstop for an un-sliced upload or any single object whose
reservation exceeds the whole budget. Without it, such a request would park forever behind a bound it
can never satisfy.

Granting an oversized request drives `in_use` above `capacity` for its lifetime. That is deliberate:
it happens only from an idle budget, one request at a time (the next waiter cannot be
granted until `in_use` returns below capacity), so the overshoot is bounded to a single oversized
reservation and never compounds.

### Sizing policy

The capacity is resolved once at client construction from `MemoryBudgetConfig`, against RAM detected
from the machine (cgroup-aware — the smaller of the cgroup limit and physical RAM, so a container
sees its limit, not the host's):

- **`Auto`** (default) — `SAFE_MEM_FRACTION` (0.25) of detected RAM, rounded to the nearest power of
  two (ties round up) and clamped to `[512 MiB, 32 GiB]`. A conservative default of 2 GiB is used when RAM cannot be
  detected. The quarter-of-RAM fraction leaves the majority for the OS page cache and the rest of the
  process; the clamp keeps a tiny box usable and stops a huge box from reserving more than buffering
  can use.
- **`Fraction(f)`** — an explicit fraction of detected RAM, clamped to `(0.0, 1.0]`, floored at
  512 MiB but not capped: an explicit fraction is taken at the caller's word on a large box.
- **`Limit(bytes)`** — an explicit byte ceiling, bypassing detection, raised to one chunk if it would
  round to zero.

The budget is a backstop, not the operating point. The concurrency controller settles throughput at
line rate well below the cap; the budget binds only when a slow consumer backs parts up in the
[prefetch buffer](./paged-recv-buffer.md). It binds below its absolute cap, but under concurrency it
is typically the tighter of a transfer's two issuance bounds — it binds a transfer before that
transfer's own read-ahead window fills (see [paged receive buffer](./paged-recv-buffer.md)).

RAM is an imprecise proxy for the right ceiling — network bandwidth sets the real
pipeline depth, and bandwidth is uncorrelated with RAM across instance families (an `m5.24xlarge` and
an `m5n.24xlarge` share 384 GiB of RAM but differ ~4× in bandwidth). The clamp keeps the RAM-derived
estimate safe at both ends; bandwidth-aware sizing is a separate refinement (see
[Open Questions](#ram-is-an-imprecise-proxy-for-the-right-ceiling)).

**Alternative: a fixed absolute default, RAM-blind.** A constant ceiling is simple but wrong across the
range of hosts the client runs on: safe on a large box means starving a small one, and safe on a
small box means under-using a large one. Sizing from detected RAM adapts the default to the machine;
the clamp bounds the adaptation at both ends.

---

## Correctness Invariants

The budget is shared across every transfer and mutated from many threads — reservers on their
transfer threads, releasers on completion, the drain on release. Its invariants are stated against
that concurrency.

### Release is decoupled from scheduling

**Invariant.** A transfer can always release the memory it holds without first obtaining a
work-generation (`poll_work`) turn.

**What it rules out.** A deadlock of the form: transfers hold the budget to capacity; freeing their
chunks requires a re-poll; the re-poll is gated by scheduler dispatch capacity that those same
transfers occupy; none can free, so none is dispatched, so none can free. A saturated budget and a
saturated scheduler wedge each other permanently.

**Mechanism.** Release is driven by consumption, not by a work-generation poll. The `Reservation`
rides the payload it accounts for and its `Drop` returns the chunks — on the consumer dropping a
delivered part, or on the disk drain freeing a part's payload after writing it. Terminal completion
and the tail drain run on the execution path (`execute`), not inside the scheduler's `poll_work`, so
a transfer that cannot be polled for new work can still complete and release what it holds. This is
the budget side of the same guarantee the paged receive buffer states from the consumer side, where
a transfer flushes its resident run before parking on the budget rather than pinning memory it
cannot release (see [budget-park deadlock avoidance](./paged-recv-buffer.md#budget-park-deadlock-avoidance)).

### Admission is starvation-free

**Invariant.** Every request with `need <= capacity` is admitted after a bounded wait.

**What it rules out.** A request deferred indefinitely by a stream of later or smaller requests that
keep jumping ahead of it.

**Mechanism.** Strict FIFO with no skip-ahead. A queued request is never bypassed — neither by a
fresh request that would fit (`reserve` and `try_reserve` both refuse to grant while the queue is
non-empty) nor by a smaller queued request behind it (the release drain stops at the first waiter
that does not fit). Once queued, a request sees `in_use` only fall until its need is met.

### An oversized request makes progress without breaking the bound

**Invariant.** A request with `need > capacity` is eventually granted, and granting it never lets a
second reservation compound the overshoot.

**What it rules out.** An un-sliced upload larger than the whole budget parking forever; and,
separately, multiple oversized grants stacking `in_use` arbitrarily far above capacity.

**Mechanism.** The `in_use == 0` forced grant admits an oversized front waiter only from a fully idle
budget. FIFO guarantees the idle point is reached (every prior reservation drains ahead of it), and
because the grant requires `in_use == 0`, the next waiter cannot be granted until the oversized
reservation releases and `in_use` falls — so at most one oversized reservation is outstanding at a
time and the overshoot is bounded to it.

### No lost wake

**Invariant.** A parked request whose need becomes satisfiable is always woken.

**What it rules out.** A release that frees enough chunks but fails to wake the waiter it unblocked,
leaving it parked against a budget that would now admit it.

**Mechanism.** Every mutation of `in_use` or the queue runs the drain, which collects the notify
closures of all waiters it grants; the closures are invoked *after* the budget lock is released —
the `lock → mutate → unlock → notify` discipline the scheduler's edge-triggered wake also follows
(see [Edge-triggered wake](./scheduler.md#edge-triggered-wake); a parked reservation's notify is
wired to `scheduler.wake`, so the budget park rides that same lifecycle). The paths that can unblock
a waiter — a reservation dropping, a waiter cancelling (which can expose free capacity to those
behind it), a capacity increase — all drain and notify. A cancelled or dropped `WaitTicket`
re-drains the queue so cancelling the front waiter does not strand the ones behind it.

### Lock ordering is acyclic

**Invariant.** The budget lock and a waiter's slot lock are never held nested in conflicting orders.

**What it rules out.** A deadlock between the release drain (holds the budget lock, takes a slot lock
to store a grant) and a concurrent `WaitTicket` drop (would hold its slot lock and take the budget
lock to cancel) if the two orderings nested.

**Mechanism.** The drain takes a slot lock only briefly to store a grant and never drops a
`Reservation` while holding it. `WaitTicket::drop` extracts its slot state under the slot lock,
*releases the slot lock*, and only then acts — dropping a granted reservation (which re-locks the
budget) or cancelling a still-waiting entry (which locks the budget). Neither path ever holds a slot
lock while taking the budget lock, so the `budget → slot` and `slot → budget` orders never form a
cycle.

---

## Open Questions

### Chunk granularity over-accounts for small objects

A reservation rounds up to a whole chunk, so an object smaller than the 8 MiB chunk reserves a full
chunk — a 256 KiB object costs 8 MiB of budget, 32× its true footprint. On a workload of many small
objects this sets the concurrency the budget admits by the chunk count rather than the objects' real
size, throttling small-file concurrency below what memory allows. A smaller chunk narrows the
over-accounting at the cost of a longer wait queue and finer-grained drains; byte-exact accounting
removes it at the cost of variable-width claims. The right unit is entangled with the recycling pool
(below), which redefines what "one unit of resident memory" means.

### RAM is an imprecise proxy for the right ceiling

The `Auto` policy sizes from detected RAM, but the quantity that actually bounds a useful budget is
network bandwidth — the pipeline depth that keeps the link full — and bandwidth is uncorrelated with
RAM across instance families. The clamp keeps the RAM estimate from being dangerous at either end,
but a bandwidth-aware or NIC-derived sizing would target the real operating point rather than a proxy
for it.

---

## Future Work

**A recycling buffer pool behind the same reserve API.** The budget caps how much resident memory
*exists*; it does not reduce the allocator churn that resident memory causes. Under high throughput
the per-part allocate/free cycle drives resident set well above the reserved bound and caps sustained
throughput — a measured ceiling that is an allocator cost, not a budget failure (the budget binds
correctly; occupancy pegs at the cap). A bounded recycling pool that reuses buffers instead of
returning them to the allocator addresses that, added behind the same `reserve` interface so the
admission model is unchanged. How the budget and the pool integrate — whether the pool's recycled
units become the budget's accounting unit, and how the two bounds compose — is a holistic design in
its own right, and the natural place to revisit chunk granularity (above).

**Wire the resizable capacity to a caller.** The budget's capacity is resizable at runtime (a grow
drains newly-admissible waiters; a shrink is soft and never revokes granted chunks), but no caller
adjusts it today. A control surface that lowers the budget under external memory pressure, or raises
it when headroom is detected, would use this.

**Reclaim for priority-aware admission.** Admission is FIFO because priority ordering without reclaim
is a half-measure. A mechanism to revoke memory from a lower-priority transfer would let a foreground
transfer preempt a background one for budget, at which point priority-ordered admission becomes
meaningful.
