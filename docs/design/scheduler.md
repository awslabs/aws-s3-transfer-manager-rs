# Scheduler

The scheduler coordinates transfer execution: work generation, prioritization, capacity gating, and
submission. It holds transfers and polls them for work, maintaining control over ordering and
admission until the moment of execution.

---

## Requirements

### Priority Control

Concurrent transfers compete for shared capacity. When priorities change (a background prefetch
becomes an active read, a user cancels one transfer and wants another to speed up), the scheduler
must reorder what it submits next. Work already in-flight completes naturally, but pending work
should reflect the new priority immediately.

Example: a virtual filesystem prefetches file A at background priority, generating GetObjectRange
work items. The user opens file B. File B's transfer is promoted to high priority. The next work
the scheduler generates should favor file B over file A, without waiting for file A's in-flight
requests to drain.

### Adaptive Concurrency

The transfer manager cannot assume dedicated access to the host's network. Other processes, other
TM instances, or other services may share the same NIC. The scheduler must support ramping
concurrency up when throughput improves and backing off when it doesn't. Fixed concurrency must also
be supported for testing and environments where the right value is known.

### Memory Bounding

Concurrent range requests for a download complete in non-deterministic order. Out-of-order data
must be buffered until the consumer catches up. Without bounds, a fast network and slow consumer
leads to unbounded memory growth. The scheduler must support bounding memory from in-flight and
buffered work. When the budget is hit, individual slow consumers should throttle their own
transfers, not starve others.

### Liveness

Every transfer must make progress regardless of concurrent workload and memory limits. Higher
priority transfers should get more resources, but lower priority transfers must not be starved
indefinitely. A system with 100 concurrent transfers and a tight memory budget must not deadlock.

### Throughput Maximization

A single large transfer must be able to saturate available network bandwidth. The scheduler must not
artificially limit per-transfer concurrency. If only one transfer is active, it should get all
available capacity within the defined constraints (memory budget, NIC allocation, concurrency
target).

### Cancellation and Shutdown

Cancellation must remove pending work immediately and let in-flight work drain cooperatively.
Shutdown must wait for all in-flight work to complete before returning. Both must be deterministic:
no leaked tasks, no orphaned resources.

---

## Architecture

### Scheduler-Controlled Submission

The scheduler holds transfers and polls them for work when capacity is available:

```
                    ┌──────────────────────────────────────────────┐
                    │                 Scheduler                    │
                    │                                              │
  enqueue ────────► │  Transfers ──► Ready Set ──► Execution       │
                    │      ▲              │             │          │
  wake ───────────► │      │              │         capacity-      │
                    │      │              ▼          gated         │
  priority change ► │      │        generate_work()     │          │
                    │      │         (CFS ordering)     │          │
                    │      │                            ▼          │
                    │      └──── on_completion() ◄── execute()     │
                    └──────────────────────────────────────────────┘
```

Work generation is event-driven: the scheduler generates work when a transfer arrives, a transfer
is woken, work completes (freeing capacity), or the concurrency target changes. It never polls on
a timer.

`generate_work()` pops the transfer with the lowest virtual runtime from the ready set, calls
`poll_work()`, and dispatches the result: `Ready(work)` submits the work for execution and
re-adds the transfer to the ready set (it may have more work); `Pending` leaves the transfer
out of the ready set until explicitly woken; `Done` removes the transfer entirely. The loop
repeats while capacity is available and the ready set is non-empty.

**Alternative: Eager spawning.** Spawn a task per work item and use cooperative cancellation.
Once a task enters a runtime's run queue (tokio's work-stealing queues, for example, or glommio's
per-core queues), it executes in whatever order the runtime chooses. We cannot reorder, cancel, or
gate already-spawned tasks without cooperative mechanisms that add complexity and latency. CRT takes
the same approach we do: all scheduling runs on a dedicated event loop where the client controls
ordering directly.

### Transfers as State Machines

Each transfer (upload or download) is a state machine that the scheduler polls for work through a
uniform trait:

```rust
pub(crate) trait Transfer: Send + Sync {
    fn ctx(&self) -> &TransferContext;
    fn poll_work(&self) -> PollWork;
    fn execute<'a>(&'a self, work: &'a mut WorkItem) -> BoxFuture<'a, WorkOutcome>;
}
```

`poll_work()` returns `Ready { io, spawned }` (a work item to dispatch, optionally fused with a
child spawn), `Spawned` (a composite enqueued one child, no work item), `Pending`, or `Done`.
`execute(work)` returns `Success`, `Failed`, or `Cancelled`. The scheduler calls `poll_work()` when
it has capacity and the transfer is in the ready set; it never calls `poll_work()` on a transfer
that returned `Pending` until that transfer is explicitly woken.

**Lazy generation.** Transfers produce work on demand, not all upfront. A 10,000-part upload
generates one work item per `poll_work()` call. Memory and in-flight work are naturally bounded
without the scheduler knowing anything about the operation's internals.

**Isolation.** Each transfer owns its internal state (part tracking, sequence windows, completion
counts). The scheduler never holds a lock on transfer internals, and transfers never lock each
other. The scheduler's lock covers only ready set selection and work submission.

**Testability.** Mock state machines can simulate any behavior (fixed work counts, pending/wake
cycles, failures, panics) without S3 calls. Scheduler tests are fast and deterministic.

**Uniformity.** Upload, download, and their multi-object variants all implement the same trait.
The scheduler is agnostic to what a transfer does. It knows about priority, capacity, and the
ready/pending/done lifecycle.

CRT uses the same pattern: each meta request type (auto_ranged_get, auto_ranged_put, copy,
default) implements a vtable with `update()` that returns a request or signals work remaining.

**Composite transfers.** A composite transfer's `poll_work` does not produce a
leaf work item for the execution layer; it recursively calls
`scheduler.enqueue_transfer` to spawn child transfers that each drive their
own state machine. Upload of a directory and download of a prefix are the
current examples. A composite is both a consumer of `poll_work` calls and a
producer of transfers in the same call stack.

A composite `poll_work` spawns **at most one child per call** and returns
`PollWork::Spawned`: the scheduler charges the spawn against the parent's
virtual runtime, re-inserts the parent under its held claim, and re-polls it
while dispatch capacity remains — so child materialization tracks the
concurrency target implicitly rather than a per-call batch. Both leaf and
composite `poll_work` are therefore O(1): one claim, one spawn, one merge.

`PollWork::Spawned` dispatches no work item and consumes no dispatch ticket;
the spawned child accounts for its own ticket when it is later polled to
`Ready`. This is what keeps a composite from starving its own children under a
tight concurrency target — if a spawn consumed a ticket, a composite could fill
every slot with spawns and leave no capacity to poll those children into real
work. Spawn and reap fuse: a `Ready { spawned: true }` poll both dispatches a
child-reaping work item and charges a spawn in one call, so a steady stream of
child completions cannot crowd out refill.

The scheduler groups a composite and all its descendants into a single
scheduling entity at the root level (see "Fair Scheduling" below), so a
composite's fan-out does not affect its share of dispatch relative to peers.

The **max composite children** cap (`DEFAULT_MAX_CONCURRENT_CHILDREN`, default
512) bounds how many children a composite may have materialized at once, shared
by directory upload and download alike. It is a memory backstop, not the
operating point: hierarchical CFS governs the steady-state spawn rate, and the
fair-share rate-limiting keeps the working set well below the cap for typical
workloads. Directory listing is gated separately, on a low-water mark in the
discovered-key buffer, so discovery runs ahead of spawning without being
coupled to the child cap.

### Fair Scheduling

When multiple transfers are ready, the scheduler uses Completely Fair Scheduling (CFS), adapted
from the Linux kernel's process scheduler. The ready set is a two-level hierarchy:

- **Root tree:** groups sorted by `group_vruntime`. Each top-level transfer owns one group.
- **Inner tree:** within each group, members (the top-level transfer itself plus all its
  descendants) are sorted by individual `vruntime`.

`generate_work()` pops the group with the lowest `group_vruntime`, then pops the member with the
lowest individual `vruntime` from that group. This gives each top-level transfer equal scheduling
share at the root regardless of how many children it has spawned - a composite with 10000 children
gets the same root-level share as a single-file upload.

**Group-entity accounting.** When a member is popped from a group, the group's `group_vruntime`
advances by `vruntime_delta_for_priority(member.priority)`. This mirrors Linux CFS group-entity
accounting: when a task in a cgroup runs, the cgroup entity itself runs. The priority-scaled delta
means a higher-priority group advances more slowly and wins more dispatch share at the root.

**Priority scaling.** Virtual runtime advances by `(cost * PRIORITY_SCALE) / priority` per unit of
work. `IO_WORK_COST = 128` is the base cost charged when a transfer dispatches one unit of IO work;
`PRIORITY_SCALE = 256` is a precision-preserving multiplier that prevents integer division from
collapsing to zero at high priorities. A priority-255 transfer accumulates vruntime at rate 128 per
IO work unit; a priority-1 transfer at rate 32768. The ratio gives priority-255 roughly 256x the
scheduling share of priority-1, but priority-1 still makes progress because its vruntime stays low
while it waits. A composite spawning a child is charged `SPAWN_WORK_COST = 64` — half the IO cost —
so spawn cadence enters CFS as a first-class cost rather than being governed by a batch constant
(see "Spawn-cost accounting").

New transfers start at the current minimum virtual runtime across all active groups, preventing
a newly enqueued transfer from monopolizing the scheduler while it "catches up." Groups that
become empty (all members returned Pending or Done) leave the root tree and rejoin at the current
root floor when a member is next inserted.

**Alternative: Round-robin.** CRT uses this: each meta request gets one `update()` call per pass
through a linked list, with no priority weighting. This is simple but provides no mechanism for
priority differentiation.

**Alternative: Strict priority.** Always select the highest-priority transfer. This starves
lower-priority transfers entirely while any higher-priority work exists. A background prefetch
would make zero progress while an active read is running.

### Work Model

Work items carry a kind and opaque data:

```rust
pub(crate) struct WorkItem {
    pub(crate) kind: WorkKind,
    pub(crate) data: Option<Box<dyn WorkData>>,
}
```

The kind (Disk or Network) is the one piece the scheduler sees. The execution layer uses it for
dispatch. The data is opaque: each state machine defines its own payload type and downcasts in
`execute()`. The scheduler never inspects work data.

A logical operation may span multiple I/O steps: read data from disk, then send it over the
network. When a work item completes, the transfer can produce a successor that continues the same
logical operation. The successor may be a different kind (disk to network) or the same kind (a
retry). Successors go directly to execution, bypassing the ready set and CFS ordering. Re-entering
the ready set at each step would mean completed disk reads sit in buffers waiting behind other
transfers for their network turn. Higher latency, more memory held longer, no fairness benefit
since capacity was already consumed at admission. CFS controls admission; successors complete
admitted work without re-competing.

### Concurrency Control

The scheduler delegates concurrency decisions to a controller. The scheduler calls
`controller.on_completion()` after each work item finishes and checks `controller.target()` in
`generate_work()` to decide whether to poll the next transfer.

```rust
pub(crate) trait ConcurrencyController: Send + Sync {
    fn target(&self) -> usize;
    fn on_completion(&self, bytes: u64, duration: Duration);
}
```

A fixed controller returns a constant target. An adaptive controller observes throughput and adjusts
the target, ramping up when throughput improves and backing off when it doesn't. The adaptive
controller needs a minimum concurrency to bootstrap: some work must be in-flight to measure
throughput.

**Alternative: Fixed or derived concurrency.** Set the target from a throughput goal or explicit
configuration. CRT does this, deriving connection count from a throughput target at client creation
(`ceil(throughput_target_gbps / throughput_per_connection_gbps)`). Fixed concurrency works when the
customer knows their hardware, but a single default is not optimal across instance sizes, and it
does not adapt when available bandwidth changes (shared NIC, competing processes, variable load).

The scheduler tracks a single capacity number, not per-kind capacity.

**Alternative: Per-kind capacity.** Separate capacity for disk and network, where the scheduler
asks transfers for a specific kind of work. This pushes scheduling knowledge into the transfer:
the transfer must predict what kind of work it will produce before producing it, which is not
always possible (a transfer may produce disk or network work depending on internal state). When a
transfer cannot produce the requested kind, the scheduler must hold it aside, try others, and
re-insert it later, increasing back-and-forth between scheduler and transfers. It also requires
the scheduler to discover and track the optimal concurrency for disk and network independently,
two dimensions instead of one, where the right ratio depends on part size, disk speed, network
speed, and consumer behavior. Single capacity lets the adaptive controller discover a single
operating point without the scheduler needing to understand the workload shape. Disk and network
have very different throughput characteristics, and a single gate can lead to suboptimal allocation
when one is saturated and the other is idle.

### Backpressure

The scheduler provides a general backpressure mechanism through the ready set: transfers that
cannot acquire resources return `Pending` from `poll_work()`, and the scheduler stops polling them
until they are woken. The mechanism is not specific to any resource. Transfers use it for whatever
gating they need.

**Sequence window.** Each download limits how far ahead of the consumer it generates work. The
window defines the maximum gap between the consumer's read position and the generation head. When
the gap is exhausted, the download returns `Pending`. When the consumer reads data, it advances
the read head and wakes the transfer. The gap scales with concurrency to avoid becoming a
throughput bottleneck at high concurrency while still bounding memory from out-of-order completion.

**Buffer pool.** A transfer polls a `ReserveFuture` for the work item's planned memory envelope before
dispatch. While admission is pending, it retains the candidate work and future and returns
`Pending`. The pool assigns the `Reservation` before waking the transfer; a later poll moves that
reservation into the dispatched work. Reservation close, cancellation, and carrier returns that
reduce admission use reconsider queued admission according to the
[memory design](./memory.md#scheduler-admission-and-dispatch).

When a transfer returns `Pending`, it leaves the ready set. The scheduler does not poll it again
until something calls `scheduler.wake(id)`, which re-inserts the transfer into the ready set and
triggers `generate_work()`. This closes the event-driven cycle: work completes or a resource is
freed, the transfer is woken, and the scheduler polls it for the next piece of work.

In both cases, the scheduler's role is the same: provide the Pending/wake lifecycle. What transfers
gate on is their own concern.

**Wake primitive protocol.** The Pending/wake handshake is edge-triggered, so
lost-wake avoidance relies on the transfer's state mutex serializing the
poller and mutator:

- The poller (inside `poll_work`) holds the state lock while checking the
  gating condition and calling `ctx.set_pending`. The mutator takes the same
  lock to mutate state. Because both sides hold the lock, they cannot
  interleave: either the mutator's change is visible to the poller (so it
  returns Ready), or the poller sets pending first and the mutator's later
  `try_wake` observes it.
- Any code path that mutates gating state follows
  `lock → mutate → unlock → try_wake`. `try_wake` swaps the pending flag and
  calls `scheduler.wake(id)` only if the flag was set. Spurious calls are
  cheap: the swap is a single atomic op.

A second layer guards `generate_work` itself. Work generation runs
**single-runner**: one runner drains generation passes while concurrent wakes
coalesce onto it via a request epoch, rather than each wake starting a competing
pass. Single-runner generation is what makes CFS ordering exact — the
pop-highest-priority / dispatch / re-insert loop is never interleaved across
threads — and it closes the lost-wake window when a runner releases its claim
after a `Pending` return: a wake arriving mid-release bumps the epoch, and the
runner re-checks the epoch before retiring rather than exiting with work left
unqueued. Parallel fill of the submission queue is a deliberate non-goal today;
see [Future Work](#future-work).

### Cancellation

When a transfer is cancelled, its cancellation token is triggered, which sets the terminal flag.
The scheduler checks the terminal flag before polling and removes terminal transfers from the
registry and ready set. Queued work belonging to the cancelled transfer is purged from the
execution queue. In-flight work completes naturally; the execution layer checks the terminal flag
at execution start and cooperatively cancels. `wait_for_idle()` blocks until the transfer's
outstanding count (queued + executing) reaches zero, guaranteeing all resources are released
before returning.

### State Machine Contracts

A Transfer implementation must uphold several contracts:

**Failed lifecycle.** When execution fails, the transfer must record the error and signal
termination before returning the failure outcome. The scheduler relies on the terminal signal to
stop generating work and clean up.

**Pending/wake obligation.** Every `Pending` return from `poll_work()` must have a corresponding
future wake path. If a transfer returns `Pending` and nothing ever wakes it, that transfer is stuck
permanently. This is the correctness obligation of the edge-triggered model: the scheduler scales
with active transfers rather than total transfer count, but every `Pending` must eventually resolve.

**Panic safety.** If `execute()` panics, the scheduler catches it and forces the terminal
transition from outside. Execution continues with the next work item. If `poll_work()` panics,
the scheduler's `generate_work` catches the panic via `catch_unwind`, releases the descriptor's
claim, force-terminates the panicking transfer (cascading to children via `cancel_transfer`), and
continues processing other transfers. Implementations should not rely on panic recovery  - a caught
panic still corrupts the transfer's internal state and forces termination.

### Concurrency and Threading

Scheduler work runs on the caller's thread. `on_completion` and `wake` drive
`generate_work` synchronously, typically from a managed execution thread. This
is a deliberate choice: the hot path has no channel hop, scheduler state is
touched with warm caches, and many execution threads can drive the scheduler
in parallel across different transfers. The choice is safe as long as the
scheduler's per-call cost stays within the constraints below.

**`poll_work` is synchronous and short.** No `.await`, no blocking I/O, no
unbounded loops. A long `poll_work` pins the caller's runtime and prevents it
from polling its own async tasks (including the in-flight SDK requests that
the scheduler depends on to make progress). Cost is O(1) per call for leaf
transfers; composite transfers bound their fan-out explicitly.

**Single-poll exclusivity.** At most one thread is inside `poll_work(desc)`
for any given descriptor at a time. Without it, burst completions converge
on lock contention on the transfer's state mutex, starving the runtimes
those threads host. A claim flag on the descriptor enforces the invariant:
the descriptor enters the ready set under a claim and stays claimed across
re-insertions in the `Ready` path until `poll_work` returns `Pending` or
`Done`. Concurrent wakes that try to re-insert while the descriptor is
claimed are no-ops, preventing duplicate ready-set entries that would
re-open the window.

**Ready set uniqueness.** The ready set contains at most one entry per
transfer id. Duplicates re-open the single-poll window and, under bursty
completions, produce lock-contention storms on the transfer's internal state.

**Edge-triggered wake primitive.** When a transfer returns `Pending`,
`generate_work` must release its claim so a future wake can re-queue it. Any
wake that arrives between the claim release and a possible re-insert would
be lost without additional coordination. A `wake_requested` flag on the
descriptor is set unconditionally by `wake`. `generate_work`'s release path
clears the claim, then reads `wake_requested`; if it was set, it re-inserts
the descriptor itself. The resulting release-and-recheck pattern is race-free
against concurrent wakes.

**Managed runtime interaction.** Managed execution threads each host a
current-thread tokio runtime. A current-thread runtime yields only at `.await`
points  - sync code on a thread blocks the tokio loop entirely. This is the
complement of the synchronous-scheduler choice: short scheduler calls coexist
with the runtime's task polling; long ones starve it.

### Cost Model

Every scheduler operation has a cost that is paid on the caller's thread. The
contracts on `poll_work` and fan-out exist because the scheduler sits in the
hot path of every execution thread.

**`poll_work` is O(1) per call.** The leaf case (upload, download) touches
transfer state under a short critical section and returns immediately.
Composite transfers spawn at most one child per call — a single claim,
orchestrate, and enqueue — so they are O(1) too; spawn throughput across a
generation pass comes from the scheduler re-polling the parent while capacity
remains, not from batching inside one poll.

**`enqueue_transfer` is O(1) but not free.** Inserts into the ready set,
writes to the transfers map, and conditionally drives `generate_work`. A
composite calls it once per `poll_work`, so the per-poll cost is one enqueue;
the aggregate over a pass is bounded by the concurrency target that gates how
many times the parent is re-polled.

**`on_completion` is O(1) + one `generate_work` pass.** The generate_work
pass pops at most the number of descriptors that fit under the current
concurrency target, each paying a `poll_work` call. Total cost of one
`on_completion` is therefore `O(target × poll_work_cost)`.

**What the cost model rules out.** A `poll_work` implementation that
iterates over an unbounded collection (pending entries, completed children,
retry queues) violates the O(1) contract and can pin the caller's runtime
long enough to starve it. Reviewing any `while`/`loop` in a `poll_work`
implementation against this contract is cheap insurance.

### Execution Layer

The scheduler generates work and tracks what is in-flight. How work actually runs is the execution
layer's concern:

- Disk I/O via `spawn_blocking`, io_uring, or direct I/O
- Network I/O via SDK client calls, multi-NIC routing
- Batching via io_uring submission batching, vectored I/O for sequential reads
- Optimized pipelines that bypass the two-phase model (e.g., socket-ready-driven streaming)

The scheduler is execution-layer-agnostic. It does not know whether work runs on tokio, glommio,
or a custom runtime. It submits work, receives completions, and tracks capacity.

For maximum throughput, the execution layer should own its threads rather than relying on a general
purpose runtime's thread pool. Managed threads with per-thread current-thread runtimes enable thread
affinity, NUMA-local buffer pools, and predictable execution ordering. The scheduler does not need
to change to support this: it generates work and receives completions regardless of how the
execution layer is structured.

### Scheduling Overhead

The scheduler only polls transfers in the ready set. A transfer enters the ready set when
enqueued, woken, or when `poll_work()` returns `Ready` (it may have more). It leaves when
`poll_work()` returns `Pending` or `Done`. This means scheduling cost scales with active transfers
and throughput, not with total transfer count. 1000 idle transfers and 1 active transfer cost the
same as 1 transfer.

At 55 Gb/s with 128 concurrency on a c6in.16xlarge, scheduling overhead is not a measured
bottleneck. If contention becomes measurable at higher throughput, the architecture supports
sharding without changing the Transfer trait or execution model.

---

## Open Questions

**Bandwidth sharing and safe defaults.** The capacity gate works regardless of how the concurrency
target is determined, but the policy for that target (how aggressively to claim bandwidth on a
shared NIC) is a significant design question. A proactive baseline/burst/settle model may be safer
than purely reactive slow-start. This is a controller policy question, not a scheduler architecture
question.

**Concurrency controller API.** A simple `target() -> usize` may suffice, or the controller may
need richer interaction: probe windows, hold-off periods, per-transfer signals. Shaped by
implementation.

**Worker pool sizing.** Adaptive concurrency may require growing or shrinking the execution pool,
not just changing the capacity gate. Whether this means adding workers or adjusting the gate is TBD.

**Memory pressure cancellation.** Proactive cancellation of background transfers under memory
pressure needs design. The seam exists (scheduler has `cancel_transfer`), but the trigger mechanism
(buffer pool signals to scheduler) is not designed.

---

## Correctness Invariants

The scheduler and transfer state machines coordinate through a handful of
invariants that the surrounding code must uphold. This section states each
one, explains what it rules out, and describes the mechanism that enforces
it. The two design choices that gave the scheduler its shape - running
scheduler work on the execution thread, and hierarchical CFS grouping for
composite transfers - are recorded at the end of the section.

### Edge-triggered wake

**Invariant.** Every `PollWork::Pending` creates an obligation: some later
mutator of the gating state must cause a matching wake to be delivered.
Without that wake the transfer stalls forever.

**What it rules out.** A mutator that finishes its update, calls
`try_wake`, and observes the poller has not yet marked itself pending will
treat the wake as unnecessary. If the poller then marks itself pending and
returns `Pending`, the wake is already gone  - lost. Two orderings have to
be defended: inside the transfer's state machine (the poller/mutator
handshake) and inside the scheduler (the moment `generate_work` transitions
a descriptor back to idle).

**Mechanism.** On the state-machine side, the poller's condition check and
`TransferContext::set_pending` call both happen under the transfer's state
lock. Every mutator follows `lock → mutate → unlock → try_wake`. Because
both sides hold the same lock, they are serialized: either the mutator's
change is visible to the poller's check (so it returns Ready and never sets
pending), or the poller sets pending first and the mutator's post-unlock
`try_wake` observes it and fires.

On the scheduler side, the descriptor carries a `wake_requested` flag that
`wake` sets unconditionally, whether or not its `ready_set.insert` succeeds.
`generate_work`'s post-`poll_work` handling releases the descriptor's claim
and then reads `wake_requested`; if it is set, `generate_work` re-inserts
the descriptor itself. A wake that arrives during the release window is
never lost.

### Single-poll exclusivity

**Invariant.** At most one thread is inside `poll_work(desc)` for a given
descriptor at a time. State-machine invariants (single-owner mutation,
ordered phase transitions) are written under this assumption.

**What it rules out.** A ready set keyed on `(vruntime, id)` pairs without
deduplication admits duplicate entries for the same transfer. Under burst
completions many execution threads can pop different entries for the same
parent, enter `poll_work` concurrently, and contend on the transfer's
state mutex. Lock contention starves the threads' tokio runtimes, so the
async tasks they host cannot make progress and no further completions
arrive to resolve the contention.

**Mechanism.** A claim flag on the descriptor is CAS-swapped to true by
`ReadySet::insert`. A failed CAS indicates the descriptor is already
queued or being polled, and the insert becomes a no-op. The claim is
asserted on first insert and held continuously across the `Ready` path's
pop / `poll_work` / reinsert cycles, released only when `poll_work` returns
`Pending` or `Done`. Callers that need to re-insert after `PollWork::Ready`
use `ReadySet::reinsert_under_claim`, which bypasses the CAS because the
caller still owns the claim.

### Bounded per-call cost

**Invariant.** `poll_work` is O(1) per call with a bounded critical
section. A composite transfer's `poll_work` spawns at most one child per
call, so it is O(1) as well — there is no per-call batch to bound.

**What it rules out.** A loop inside `poll_work` whose termination depends
on exhausting an input queue (pending directory entries, retry buffer,
completed-child list) can run for far longer than the scheduler's
implicit cost model assumes. During that call the caller's thread is
executing scheduler code and not polling its runtime; other transfers
cannot be polled. At high concurrency this converges on runtime
starvation.

**Mechanism.** A composite spawns at most one child per `poll_work` call and
returns immediately (`Spawned`), so there is no spawn loop to bound — per-call
cost is structurally O(1). The max-children cap (default 512) is a backstop on
total materialized children, not a per-call bound. Any `while`/`loop` inside a
`poll_work` implementation is a place to audit against this invariant.

### Design choice: scheduler work on the execution thread

`on_completion` and `wake` drive `generate_work` synchronously on the
caller's thread. This is the hot-path choice: no channel hop, warm caches,
parallel scheduler drive across execution threads. It depends on the
invariants above holding  - the scheduler stays off the critical path only
as long as `poll_work` is short, single-polling is enforced, and fan-out
is bounded.

A dedicated scheduler thread that owns scheduling state and drains
completion events from a channel is the known alternative. It would
structurally eliminate runtime starvation at the cost of per-completion
latency, cache locality, and single-thread serialization of scheduler
work. The current model is sufficient at today's throughput targets and
cheaper in the common case; the dedicated-thread design is available as a
fallback if the invariants above become harder to uphold.

### Design choice: hierarchical CFS for composite transfers

Child transfers enqueued by a composite's `poll_work` are placed into the
parent's group in the ready set. The group competes as a single entity at
the root tree, regardless of how many children it contains. This is the
cgroup-style model from Linux CFS: a task group's aggregate share is
determined by the group entity's weight, not by the number of tasks inside.

A composite with 10000 children and a single-file upload each get one
group at the root. Both groups advance `group_vruntime` at the same rate
(assuming equal priority), so each gets roughly 50% of dispatch share.
Within the composite's group, children compete against each other and
against the composite itself via individual vruntime.

**Spawn-cost accounting.** When a composite spawns a child, both the parent's
individual vruntime and its group's `group_vruntime` advance by
`vruntime_delta_for_cost(SPAWN_WORK_COST, priority)` — the priority-scaled
formula at half the IO cost. Charging the group makes a composite's spawn
cadence visible to cross-group fairness: a spawn-heavy composite accrues group
vruntime faster and self-limits its dispatch share against peers. The reduced
individual charge (half the IO cost) keeps the parent scheduling-competitive
with its own children, so it wins enough poll turns to keep the pipeline
refilled rather than spawning in bursts and stalling.

**Alternative considered: flat peer model.** The prior design treated
children as independent peers at the root level. A composite with N
children claimed N+1 scheduling shares against a single transfer's one.
This was correct in the sense that the composite genuinely had N units of
work, but it violated the user's mental model: two `upload_objects` calls
with different file counts should get equal throughput, not throughput
proportional to file count. The hierarchical model matches user
expectations and the Linux cgroup analogy.

---

## Future Work

**Request consolidation.** Merging overlapping byte range requests. Transfer-level concern.

**Placement and NUMA awareness.** Execution layer concern. The scheduler generates work; the
execution layer decides which thread/core/NIC runs it based on affinity hints in the work payload.

**Batching.** Time-based, count-based, or opportunistic batching for io_uring submissions and
vectored I/O. Depends on workload characteristics.

**Parallel submission-queue fill.** Generation is single-runner today, which makes priority ordering
exact at the cost of serializing the pop / dispatch / re-insert loop. At very high dispatch rates
that serialization is a potential throughput ceiling. Parallel fill — multiple runners generating
concurrently, trading some ordering precision for throughput — is the known evolution; the
single-runner epoch gate is the seam that would change to support it.
