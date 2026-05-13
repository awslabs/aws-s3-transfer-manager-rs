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

`poll_work()` returns `Ready(work)`, `Pending`, or `Done`. `execute(work)` returns `Success`,
`Failed`, or `Cancelled`. The scheduler calls `poll_work()` when it has capacity and the transfer
is in the ready set; it never calls `poll_work()` on a transfer that returned `Pending` until that
transfer is explicitly woken.

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

A leaf `poll_work` is O(1); a composite `poll_work` is `O(batch × enqueue_cost)` 
where the batch is the number of children it spawns per call. The batch must 
be bounded explicitly by the composite — there is no scheduler-side fallback 
that will catch an unbounded fan-out. The `pipeline_depth` knob on composite
transfers is the materialization bound: how many children may be simultaneously live,
regardless of the scheduler's overall concurrency target. Memory cost and
`poll_work` duration both scale with this bound, so the right value trades
throughput against memory and per-call latency.

Fairness between a composite and unrelated top-level transfers falls out of
CFS naturally. Each child is enqueued with `vruntime = min_vruntime` at
insert time — the same policy Linux applies to newly forked tasks — and
competes as a peer. A composite with N live children effectively claims
`N+1` scheduling shares against another independent transfer's one; this is
correct because the composite genuinely has N units of work to do. See
"Invariants and Violations" for the considered alternative (hierarchical
group accounting) and why we do not use it today.

### Fair Scheduling

When multiple transfers are ready, the scheduler uses Completely Fair Scheduling (CFS), adapted
from the Linux kernel's process scheduler. Each transfer accumulates virtual runtime as it
generates work. The ready set is ordered by virtual runtime, so `generate_work()` always selects
the transfer that has received the least scheduling share.

Priority acts as a weight on virtual runtime accumulation. When a transfer generates a work item,
its virtual runtime increases by `base_cost / priority`. Higher priority means slower accumulation,
so the transfer generates more work before yielding. A priority-255 transfer gets roughly 256x the
scheduling share of a priority-1 transfer, but the priority-1 transfer still makes progress because
its virtual runtime stays low while it waits.

New transfers start at the current minimum virtual runtime across all active transfers, preventing
a newly enqueued transfer from monopolizing the scheduler while it "catches up."

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

**Buffer pool.** Transfers acquire buffer tickets before generating work. When the memory budget is
exhausted, transfers return `Pending`. When buffers are released, blocked transfers are woken.

When a transfer returns `Pending`, it leaves the ready set. The scheduler does not poll it again
until something calls `scheduler.wake(id)`, which re-inserts the transfer into the ready set and
triggers `generate_work()`. This closes the event-driven cycle: work completes or a resource is
freed, the transfer is woken, and the scheduler polls it for the next piece of work.

In both cases, the scheduler's role is the same: provide the Pending/wake lifecycle. What transfers
gate on is their own concern.

**Wake primitive protocol.** The Pending/wake handshake is edge-triggered, so
lost-wake avoidance is split between the poller and the mutator:

- The poller (inside `poll_work`) marks the transfer context pending
  (`ctx.set_pending`) **before** checking the gating condition. If the
  condition becomes satisfied in the window between the mark and the check,
  the next mutator's wake will still fire.
- Any code path that mutates gating state follows
  `lock → mutate → unlock → try_wake`. `try_wake` swaps the pending flag and
  calls `scheduler.wake(id)` only if the flag was set. Spurious calls are
  cheap: the swap is a single atomic op.

A second layer at the scheduler protects `generate_work` itself against wakes
that arrive while it is releasing a descriptor's claim after a `Pending`
return. See "Concurrency and Threading" below.

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
continues processing other transfers. Implementations should not rely on panic recovery — a caught
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
for any given descriptor at a time. The ready set's insert path is CAS-gated
on a claim flag carried by the descriptor. The claim remains asserted from the
moment the scheduler decides to poll through `pop` and `poll_work` until
`generate_work` has finished handling the outcome (`Ready`, `Pending`, or
`Done`). The Transfer trait's `&self` API forces interior-mutable state
behind a mutex, so single-poll exclusivity isn't required for correctness —
its purpose is performance, keeping that mutex effectively uncontended in
steady state. Without it, burst completions converge on lock contention
that pins managed-thread runtimes (see "Invariants and Violations").

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
points — sync code on a thread blocks the tokio loop entirely. This is the
complement of the synchronous-scheduler choice: short scheduler calls coexist
with the runtime's task polling; long ones starve it.

### Cost Model

Every scheduler operation has a cost that is paid on the caller's thread. The
contracts on `poll_work` and fan-out exist because the scheduler sits in the
hot path of every execution thread.

**`poll_work` is O(1) per call.** The leaf case (upload, download) touches
transfer state under a short critical section and returns immediately.
Composite transfers pay `O(batch × enqueue_cost)` when they spawn children;
the batch size must be bounded by the composite (e.g. a per-call bound).

**`enqueue_transfer` is O(1) but not free.** Inserts into the ready set,
writes to the transfers map, and conditionally drives `generate_work`. At
typical fan-out rates the cost is a few microseconds; at `pipeline_depth`
scale (hundreds per call, inside a composite's `poll_work`) the aggregate
cost is still bounded but non-trivial.

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
it. The two design choices that gave the scheduler its shape — running
scheduler work on the execution thread, and flat CFS ordering across
parent and child transfers — are recorded at the end of the section.

### Edge-triggered wake

**Invariant.** Every `PollWork::Pending` creates an obligation: some later
mutator of the gating state must cause a matching wake to be delivered.
Without that wake the transfer stalls forever.

**What it rules out.** A mutator that finishes its update, calls
`try_wake`, and observes the poller has not yet marked itself pending will
treat the wake as unnecessary. If the poller then marks itself pending and
returns `Pending`, the wake is already gone — lost. Two orderings have to
be defended: inside the transfer's state machine (the poller/mutator
handshake) and inside the scheduler (the moment `generate_work` transitions
a descriptor back to idle).

**Mechanism.** On the state-machine side, `TransferContext::set_pending` is
called by the poller *before* it evaluates the gating condition, and every
mutator follows `lock → mutate → unlock → try_wake`. `try_wake` only calls
`scheduler.wake(id)` if the pending flag is set, and either the poller's
flag-set-before-check or the mutator's post-unlock wake is guaranteed to
be observed.

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
queued or being polled, and the insert becomes a no-op. The claim stays
asserted from insert through `pop` through `poll_work`, and is released
only by `generate_work` after handling the outcome. Callers that need to
re-insert after `PollWork::Ready` use `ReadySet::reinsert_under_claim`,
which bypasses the CAS because the caller still owns the claim.

### Bounded per-call cost

**Invariant.** `poll_work` is O(1) per call with a bounded critical
section. A composite transfer whose `poll_work` recursively calls
`enqueue_transfer` pays `O(batch × enqueue_cost)` and must bound `batch`
explicitly.

**What it rules out.** A loop inside `poll_work` whose termination depends
on exhausting an input queue (pending directory entries, retry buffer,
completed-child list) can run for far longer than the scheduler's
implicit cost model assumes. During that call the caller's thread is
executing scheduler code and not polling its runtime; other transfers
cannot be polled. At high concurrency this converges on runtime
starvation.

**Mechanism.** Composite transfers carry an explicit materialization bound
(`pipeline_depth`) and their draining loops must include the batch being
built in the same expression that determines whether to continue. Any
`while`/`loop` inside a `poll_work` implementation is a place to audit
against this invariant.

### Design choice: scheduler work on the execution thread

`on_completion` and `wake` drive `generate_work` synchronously on the
caller's thread. This is the hot-path choice: no channel hop, warm caches,
parallel scheduler drive across execution threads. It depends on the
invariants above holding — the scheduler stays off the critical path only
as long as `poll_work` is short, single-polling is enforced, and fan-out
is bounded.

A dedicated scheduler thread that owns scheduling state and drains
completion events from a channel is the known alternative. It would
structurally eliminate runtime starvation at the cost of per-completion
latency, cache locality, and single-thread serialization of scheduler
work. The current model is sufficient at today's throughput targets and
cheaper in the common case; the dedicated-thread design is available as a
fallback if the invariants above become harder to uphold.

### Design choice: flat CFS ordering for composite transfers

Child transfers enqueued by a composite's `poll_work` are initialized with
`vruntime = min_vruntime` — the policy Linux CFS applies to newly forked
tasks. Parent and children then compete as peers in the CFS ordering, not
as a hierarchical group. A composite with N live children effectively
claims `N+1` scheduling shares against another independent transfer's
one. This is correct because the composite genuinely has N units of work
to perform; the fairness property is self-regulating as long as child
materialization is bounded.

A hierarchical alternative — treating a composite and its children as a
single group with a bounded aggregate share, cgroup-style — was
considered. It would matter if users needed strict fairness budgets
across independent composites, which is not a current use case. The flat
peer model is simpler, has a familiar kernel analogue, and keeps the
scheduler ignorant of composite boundaries.

---

## Future Work

**Hedging.** Speculative retry of slow requests. Two options: scheduler duplicates the work item
(both compete, loser cancelled), or execution layer races two HTTP requests within a single work
item. Needs design when we get there.

**Request consolidation.** Merging overlapping byte range requests. Transfer-level concern.

**Placement and NUMA awareness.** Execution layer concern. The scheduler generates work; the
execution layer decides which thread/core/NIC runs it based on affinity hints in the work payload.

**Batching.** Time-based, count-based, or opportunistic batching for io_uring submissions and
vectored I/O. Depends on workload characteristics.
