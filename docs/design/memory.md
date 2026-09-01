# Memory

Concurrent downloads retain ranges fetched ahead of a slow consumer and ranges awaiting ordered
delivery. Uploads retain retryable parts assembled from forward-only input. Independent
per-transfer limits either constrain a lone transfer or multiply beyond host memory under
concurrency. Allocating and freeing each part also leaves allocator and page-management work on the
data path.

One memory pool defines one admission, accounting, and physical-storage domain. A transfer manager
can construct the pool or use a caller-provided handle shared with another component. Managed work
reserves planned demand before dispatch and acquires writable buffers while executing. Reservation
controls admission; acquisition selects physical memory. The baseline HTTP path copies decoded
payload from transport-owned `Bytes` into reserved pooled storage. A future transport integration
may acquire unreserved storage when it cannot carry a reservation. Private protocol
scratch and clients without the pool remain outside its accounting.

## Requirements

### Bound admission and account overage

Configured capacity is the normal ceiling for managed admission. Uploads, downloads, retry bodies,
completed output awaiting consumption, and pooled response frames share that ceiling rather than
independent direction, connection, or transfer limits.

Configured capacity does not preallocate memory and is not a hard limit on process RSS or mapped
address space.

Pooled ownership acquired without an open envelope, or retained after its envelope closes, may put
admission above configured capacity. That ownership remains accounted and delays later managed work
until its owners release it.

### Share one accounting domain across components

An explicitly supplied pool can place a transfer manager and another component in one admission,
ownership, preparation, and metrics domain. Their reservations and carrier charges share one
configured capacity.

A component that requires bounded admission reserves before acquisition. Unreserved acquisition
does not become bounded merely because its caller shares the pool.

FIFO order and idle-only eligibility are pool-wide. One component's head request may block later
requests from every component; the shared domain provides no per-component latency isolation.

Transfer-manager shutdown releases only that manager's state. It does not close a pool retained by
another caller.

### Resolve memory contention before dispatch

A work item's planned memory demand is admitted before it occupies dispatch capacity. Otherwise
several work items can hold execution resources and partial state while each waits for memory held
behind the work it prevents from running.

Successful admission removes waits on other buffer owners from the execution path. An admitted
operation does not suspend until another operation returns a buffer. Mapping, placement,
registration, and operating-system allocation can still fail; admission guarantees progress
against pool contention rather than infallible physical allocation.

### Support acquisition when a reservation cannot be carried

An integration boundary that cannot present a reservation may use unreserved acquisition. The pool
accounts that ownership before exposing writable memory. Configured capacity alone does not make the
acquisition wait or fail; failure requires an inability to obtain physical storage.

An independently driven unreserved issuer requires its own backpressure. The pool cannot bound work
that continues outside managed admission.

### Keep memory ownership with the bytes

Download data may be cloned, sliced, moved to another thread, or retained after the transfer
operation completes. Upload data from a forward-only source remains replayable through SDK and
transfer-manager retry windows. Operation completion is therefore not a valid proxy for payload
lifetime.

When one pooled allocation backs several immutable views, it remains accounted and unavailable for
reuse until its final view drops. After the operation finishes acquiring memory, unused planned
demand leaves accounting while each allocation that remains owned continues to count once,
regardless of its number of views. Mutable access and published immutable views never overlap.
Physical memory becomes reusable before its capacity is credited to later admission.

Pooled upload memory preserves exact content length and replays the same immutable bytes across
attempts.

### Preserve admission order and oversized progress

Admission pressure delays managed work instead of rejecting it. FIFO order prevents a stream of
small requests from indefinitely deferring an earlier large request.

When no active planned demand remains, the FIFO head may be admitted even if its envelope exceeds
configured capacity or retained ownership would otherwise block it. This idle-only exception admits
at most one managed request at a time. Progress assumes earlier reservations close and physical
preparation succeeds.

Cancellation removes delayed demand or closes future direct-acquisition authority. Memory already
owned remains accounted until final return.

### Release without scheduler or runtime participation

The final payload owner may drop on any thread and outlive the transfer manager handle. It retains
the state required to release physical memory and accounting without a scheduler turn, an async
runtime, or a weak-state upgrade.

A return or cancellation that makes delayed work eligible reconsiders that work directly.
Reconsideration may run on the thread dropping the final owner. Registered reservation wakers run
only after pool locks are released.

### Reuse storage without retaining the peak working set

Steady traffic reuses buffers across operations instead of allocating and freeing one part-sized
object for every I/O. Free buffers can serve uploads, downloads, retries, or response frames rather
than reserving fixed portions of configured capacity for each path.

The pool can add capacity as demand grows. After a period with no managed work, it reclaims wholly
free capacity with hysteresis without reclaiming memory required by live owners or admitted demand.
Reclamation operates only on free memory; acquisition and return correctness do not depend on it
running.

### Support runtime-selected payload shapes

Object, upload-part, download-range, and transport-frame sizes are selected at runtime and need not
match allocator geometry. Admission and ownership therefore cannot depend on fixed part sizes,
contiguous allocation, or one size class per logical payload. Scatter buffers are a valid result;
contiguous ranges are an optimization.

### Provide page-granular pooled memory

Each carrier begins at a page boundary, occupies a whole number of pages, and shares no page with
another carrier. Placement, protection, reclamation, and registration cannot affect a page owned by
another live carrier.

Page granularity does not make every payload view suitable for direct I/O. Linux `O_DIRECT`
requirements can vary by filesystem and device and may require per-file `STATX_DIOALIGN` checks
([open(2)][open2], [statx(2)][statx2]). Payload that starts inside a buffer, short intermediate
writes, and a final unaligned tail require aligned assembly or a buffered fallback.

### Scale buffer reuse across cores and pool size

Covered acquisition within the optimistic scan budget and coverage-restoring return have bounded
cost independent of pool size and avoid global serialization.

Publishing an uncovered charge, preparing storage, serialized registry-wide fallback, and returning
an uncovered charge may enter admission serialization. These slow paths preserve exhaustive reuse
and admission liveness. Contention alone cannot cause repeated growth without progress.

### Contain resource failures

After pool construction, a recoverable resource failure is contained to the reservation,
acquisition, or reclamation attempt that encountered it. Unrelated operations and the pool remain
usable. A failed operation exposes no buffer whose ownership or accessibility is uncertain. A
detected ownership or stable address violation is fail-stop.

### Provide safe defaults without mandatory tuning

The default configured capacity derives from memory available to the process, including container
limits, while leaving headroom for the rest of the process and operating system. Callers can
override the capacity, but correctness and basic progress do not depend on tuning buffer geometry,
scan effort, reclamation timing, or topology policy.

### Report admission and physical pressure

Callers can inspect configured capacity, admitted demand, outstanding ownership, prepared capacity,
and queued reservations without depending on the internal ledger or bitmap representation. The
memory metrics form one coherent admission sample. Diagnostic events identify allocation failure,
degraded reclamation, and repeated serialized fallback without adding carrier-return work to the
common path.

## Architecture

The first three subsections establish [pool topology](#pool-structure), the
[ownership spine](#handles-and-ownership), and the
[reservation and buffer flow](#reservation-and-buffer-flow). The remaining subsections define
[admission and accounting](#admission-and-accounting), [physical storage](#physical-storage), and
[byte ownership and delivery](#ownership-and-delivery).

### Pool structure

`BufferPool` is the public handle to admission and physical storage behind a private `PoolInner`:

```text
BufferPool
`-- Arc<PoolInner>
    +-- Mutex<AdmissionState>
    |   `-- admission policy, prepared capacity, FIFO waiters
    +-- CoverageState
    |   `-- available coverage and uncovered charges
    +-- Arena
    |   `-- physical storage, carrier claiming, reuse, reclamation
    `-- MaintenanceCoordinator
        `-- idle deadlines, cleanup retry, thread lifecycle
```

`AdmissionState` decides whether planned demand may be dispatched. `CoverageState` accounts
per-carrier acquisition and return. `Arena` owns mappings and carrier state but does not grant
admission. A reservation grant prepares capacity for the admitted total.

All capacity and accounting quantities in this design count carriers. A carrier is one fixed-size,
page-granular unit of physical ownership. The arena may group carriers into larger mapping and
reclamation units, but those groups do not change the accounting unit.

The core handle declarations are:

```rust
/// A cloneable handle to one admission, accounting, and storage domain.
///
/// Every clone shares the same configured capacity, reservations, ownership
/// charges, prepared storage, wait queue, and observability state.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    admission: Mutex<AdmissionState>,
    coverage: CoverageState,
    arena: Arena,
    maintenance: MaintenanceCoordinator,
}

/// Non-cloneable direct-acquisition authority for one admitted envelope.
///
/// Closing or dropping the reservation revokes acquisition of new carriers.
/// Buffers and carriers already acquired through it retain the private state
/// needed for growth checks and final return.
pub struct Reservation {
    state: Option<Arc<ReservationState>>,
}

/// A cancellation-safe request for one reservation.
///
/// Dropping the future cancels a queued request or retires an assigned
/// reservation that has not been observed.
#[must_use = "a reservation request does nothing unless polled or awaited"]
pub struct ReserveFuture {
    state: ReserveFutureState,
}
```

`BufferPool` and `Reservation` are `Send + Sync`. `ReserveFuture` is `Send` and may move between
executors while pending; it updates its registered waker on the next poll. Neither `Reservation` nor
`ReserveFuture` is `Clone`.

Concurrent calls to `BufferPool::acquire(&reservation, ..)` are supported. The packed reservation
owner state linearizes their direct-acquisition-authority debits. `close_acquisition(self)` requires
exclusive ownership of the public handle, so safe code cannot race close with an outstanding shared
borrow.

`PooledBufMut` and `SegmentedBytes` are public data-plane types. `PooledBufMut` owns exclusive
writable carrier ranges. `SegmentedBytes` presents immutable bytes retained by pooled or foreign
owners. Their complete layouts appear under [Ownership and delivery](#ownership-and-delivery).

A reservation names demand rather than physical carriers. Granting one does not remove carriers
from the arena or bind them to a block. Every prepared carrier remains available to any acquisition
in the single compatibility domain.

### Handles and ownership

Reservations, mutable buffers, and immutable views retain the state required to return physical
memory and accounting. In ownership diagrams, an arrow points from a holder to the state it retains:

```text
BufferPool --------------------------+
ReservationState --------------------+
CarrierGuard ------------------------+
ReserveFuture -----------------------+--> Arc<PoolInner>

Reservation --> Arc<ReservationState>

CarrierGuard
    +-- aggregate charge --> Arc<PoolInner>
    `-- optional reservation state --> Arc<ReservationState>

PooledBufMut
    +-- GrowthAuthority
    |     +-- Reserved --> Arc<ReservationState> --> Arc<PoolInner>
    |     `-- Unreserved -------------------------> Arc<PoolInner>
    `-- writable ranges --> 0..N Arc<CarrierGuard>
pooled Bytes --------------> Arc<CarrierGuard>
SegmentedBytes
    +-- pooled holds -------> 0..N Arc<CarrierGuard>
    `-- foreign holds ------> 0..N foreign owners
```

`PoolInner` owns queued waiter state but not granted reservations or carrier guards. A blocked
`ReserveFuture` retains the pool and its waiter slot. A grant removes that waiter from the admission
queue before storing the `Reservation` in the slot. Every carrier guard retains `PoolInner`; no weak
upgrade, scheduler turn, or runtime task is required for final return.

One carrier guard represents one physical carrier and one accounting charge. Several immutable
views may share that guard without duplicating either. A reserved mutable buffer and its directly
acquired carrier guards retain private reservation state. Return before close restores
direct-acquisition authority for retry. Close prevents later buffer growth and returns from
reopening direct-acquisition authority.
After close, the private state drops after the final reserved buffer, direct carrier, and in-flight
debit.

### Reservation and buffer flow

`poll_work()` polls a stored `ReserveFuture` before returning dispatchable work:

```text
poll_work()
    |
 poll ReserveFuture
    |
    +-- Ready(Reservation)
    |       |
    |       `-- PollWork::Ready { io, .. } -> dispatch
    |
    +-- Pending
    |       |
    |       +-- retain future in transfer state
    |       +-- PollWork::Pending
    |       `-- registered waker -> scheduler.wake(id) -> later poll_work()
    |
    `-- Ready(error) -> no dispatch
```

`BufferPool::reserve` returns a future without performing admission. Its first poll either returns
an immediate `Reservation` or creates one FIFO waiter and stores the supplied waker. Grant or
terminal failure wakes the task, and a later poll consumes the assigned result. Only
`PollWork::Ready` carries work to dispatch; its `IoRequest` work data owns the `Reservation`.

A reservation grant admits one carrier-count envelope for the work item. The envelope is fungible:
it is not divided between direct and unreserved acquisition and does not predict which path will
consume it. `Reservation` additionally limits direct acquisition by that work item.

After dispatch, reserved and unreserved acquisition converge on the same mutable and immutable
buffer types:

```text
dispatched work + Reservation -- acquire(min_bytes) ----------+
caller without reservation -- acquire_unreserved(min_bytes) +--> PooledBufMut
                                                                    |
                                              PooledBufMut::reserve(min_writable)
                                                      reuses tail, then
                                                      acquires shortfall
                                                                    |
                                                                 publish
                                                                    |
                                                                    v
                                                         Bytes / SegmentedBytes
                                                                    |
                                                           final carrier owner drops
                                                                    |
                                                                    v
                                                   physical return, then accounting return
```

When no buffer may need another reserved carrier,
`Reservation::close_acquisition` retires its planned demand.

Both initial acquisition paths account requested capacity before exposing writable memory.
Reserved acquisition also consumes direct-acquisition authority from its `Reservation`. A buffer
may later extend the same acquisition stream. Growth uses its existing writable tail before
accounting and acquiring the carrier-rounded shortfall. Ownership beyond admitted demand remains
accounted until return and delays later admission; later grants do not absorb it. Initial
acquisition and growth either satisfy the complete request or return an error and never wait for
another carrier owner to return memory.

`PooledBufMut` holds exclusive mutable authority over unpublished ranges. Publication transfers
initialized ranges to `Bytes` or `SegmentedBytes` without changing the carrier charge. Immutable
views can outlive both the work item and the public pool handle.

`close_acquisition` consumes the public `Reservation`, revokes acquisition of new carriers through
that reservation state, and retires planned demand while preserving the charges held by mutable or
immutable owners. Existing buffers may continue writing, publishing, freezing, and dropping.
Growth that fits in an existing writable tail remains valid; growth requiring another carrier
returns `ReservationClosed`. Final carrier return first makes physical storage reusable and then
releases its accounting charge; the resulting capacity may make the FIFO head eligible for
admission.

### Admission and accounting

#### Accounting model

Admission tracks demand that may be acquired separately from ownership that may outlive it. The
model uses the following terms:

- An **envelope** is the carrier count granted to one reservation.
- A **charge** accounts one carrier from aggregate debit until rollback or final owner return.
- **Available coverage** is the part of all open envelopes not occupied by charges.
- An **uncovered charge** is a charge not backed by an open envelope.
- **Admission used** is the complete open envelopes plus all uncovered charges.

Available coverage is aggregate. It is not assigned to a reservation or divided into direct,
upload, download, or transport portions. Direct and unreserved acquisition consume the same
coverage.

Assume one reservation has an envelope of four carriers:

| Action                 | Open envelope                | Charges outside an open envelope | `admission_used` |
| ---------------------- | ---------------------------- | -------------------------------: | ---------------: |
| Grant four             | four available               |                                0 |                4 |
| Acquire two directly   | two charged, two available   |                                0 |                4 |
| Acquire one unreserved | three charged, one available |                                0 |                4 |
| Close the reservation  | none                         |                                3 |                3 |
| Return all carriers    | none                         |                                0 |                0 |

A covered acquisition changes available coverage into a charge without changing
`admission_used`. The unreserved acquisition in the example consumes available coverage just like
the direct acquisition; its API path does not make the charge uncovered.

**Overage case.** After the third row, one unit of available coverage remains. A separate request
for two more carriers, whether unreserved or direct when direct-acquisition authority permits, would
consume that unit and add one uncovered charge. `admission_used` would rise from four to five. A
later grant would add its complete envelope without removing that charge. Otherwise successive
grants could absorb the same outstanding ownership without any carrier returning.

Close removes the one unused unit from admission. The three carrier owners remain, so their charges
move outside the closed envelope. No carrier moves and no memory is allocated during this
reclassification. Prepared capacity remains available after the final return and is not shown.

The accounting fields name the states in that lifecycle:

- **Active planned demand** (`active_planned_demand`) is the sum of all open reservation envelopes.
- **Available coverage** (`available_coverage`) is the unused portion of those envelopes.
  Acquisition converts coverage into an ownership charge without changing admission.
- **Uncovered charges** (`uncovered_charges`) account live or in-flight ownership outside open
  envelope coverage.
- **Outstanding charges** (`outstanding_charges`) count all covered and uncovered ownership,
  including acquisition debits awaiting transfer to physical carriers.

Admission and physical preparation gate on:

```text
admission_used =
    active_planned_demand
  + uncovered_charges
```

The other accounting quantities are derived:

```text
covered_charges =
    active_planned_demand
  - available_coverage

outstanding_charges =
    covered_charges
  + uncovered_charges

admission_overage =
    max(0, admission_used - configured_capacity)
```

`admission_overage` is an operational measurement, not a separate charge.

The pool keeps policy, physical state, aggregate accounting, and direct-acquisition authority
separate:

| Scope                        | Quantities                                                                                | Question                                                       |
| ---------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Admission policy             | `configured_capacity`                                                                     | May another reservation be granted normally?                   |
| Physical storage             | `prepared_capacity`, `physical_live`                                                      | How much storage is ready, and how much is owned?              |
| Aggregate accounting         | `active_planned_demand`, `available_coverage`, `uncovered_charges`, `outstanding_charges` | How much demand and ownership pressure is charged to the pool? |
| Direct-acquisition authority | `envelope`, `direct_outstanding`                                                          | May this reservation acquire another carrier directly?         |

Configured capacity is not a hard physical-memory limit. Prepared capacity is not ownership.
Aggregate accounting does not bind a charge to a particular envelope. Direct-acquisition authority
limits one reservation but does not partition available coverage.

#### State and invariants

All internal quantities use a carrier-count type:

```rust
#[repr(transparent)]
struct CarrierCount(usize);
```

Byte requests convert to `CarrierCount` at the acquisition boundary. `usize` supports indexing and
geometry without making the semantic type depend on the packed accounting representation.

Aggregate accounting has serialized admission state and lock-free charge state:

```rust
struct PoolInner {
    admission: Mutex<AdmissionState>,
    coverage: CoverageState,
    arena: Arena,
}

struct AdmissionState {
    ledger: AdmissionLedger,
    waiters: VecDeque<Waiter>,
    parked_reservations_total: u64,
}

struct AdmissionGuard<'a> {
    inner: MutexGuard<'a, AdmissionState>,
}

struct AdmissionLedger {
    configured_capacity: CarrierCount,
    prepared_capacity: CarrierCount,
    active_planned_demand: CarrierCount,
}

struct CoverageState {
    // Low u32: available coverage. High u32: uncovered charges.
    packed: AtomicU64,
}
```

`AdmissionState` serializes grant policy, physical preparation, FIFO state, planned-demand changes,
and acquisition transitions that add uncovered charges. `parked_reservations_total` increments
saturating once when a reservation request first enters the FIFO. `CoverageState` linearizes
per-carrier debit and return. A debit fully covered by available coverage and a return that
only restores coverage complete with one compare-and-exchange loop. The containing acquisition
enters admission serialization to publish and prepare a shortfall, or before serialized fallback
after an optimistic physical miss.

`AdmissionGuard` is the private proof that one acquisition owns admission serialization. Code that
changes prepared capacity or enters serialized physical fallback receives this guard and does not
lock admission internally.

The serialized return transition and its no-lost-wake ordering are defined under
[Close and return](#close-and-return).

Available coverage and uncovered charges share one atomic value because acquisition, close, and
return must observe and change them as one state. With separate atomics, a return can observe no
uncovered charge and decide to restore coverage while a concurrent close withdraws the envelope and
reclassifies its occupied portion. The return would then restore coverage after active planned
demand reached zero. One packed compare-and-exchange instead observes either side of close and
retries against the complete new state.

The packed representation uses checked `u32` lanes. Grant and acquisition reject a transition that
would violate:

```text
active_planned_demand + uncovered_charges <= u32::MAX
```

With a 4 KiB carrier, the bound represents approximately 16 TiB. A larger carrier increases the
representable byte capacity proportionally. The public and internal count type remains `usize`; the
check occurs before encoding a packed transition.

Available coverage never exceeds the demand that created it:

```text
available_coverage <= active_planned_demand
```

The sum of direct outstanding charges across open reservations also cannot overlap available
coverage:

```text
available_coverage + open_direct_outstanding <= active_planned_demand
```

Grant adds equal planned demand and available coverage. Acquisition publishes its aggregate charge
before adding direct outstanding authority, so that direct addition is already reflected by lower
coverage or a new uncovered charge. Close uses the closing reservation's direct outstanding count
and the remaining active demand to distinguish coverage it may withdraw. Return lowers direct
outstanding before restoring coverage. Unreserved acquisition only lowers coverage and does not
affect the direct term.

A carrier is never live before it is charged:

```text
physical_live <= outstanding_charges <= admission_used
```

Acquisition installs its complete charge before claiming physical storage. Final return makes the
carrier physically reusable before releasing its charge. Non-negative available coverage gives the
second inequality.

Completed transitions and reclamation decisions retain enough prepared capacity to cover admission:

```text
prepared_capacity >= admission_used
```

This inequality is the **admission floor**. A grant prepares before publishing its accounting
transition. An uncovered acquisition installs its charge and prepares while admission remains
serialized; the floor may be transiently false inside that critical section, but preparation
completes before writable memory is exposed or admission is unlocked. Failure rolls back without
exposing writable memory. Close and return cannot increase the floor.

**Alternative: Static envelope partitions.** Rejected: a grant could divide its envelope into
direct and unreserved portions, but reservation time cannot predict which path will acquire the
memory. A fixed split can strand one portion while another path creates avoidable uncovered charges.

#### Reservation admission

The public reservation surface accepts bytes and keeps carrier counts internal:

```rust
impl BufferPool {
    pub fn try_reserve(
        &self,
        bytes: usize,
    ) -> Result<Option<Reservation>, ReserveError>;

    pub fn reserve(&self, bytes: usize) -> ReserveFuture;
}

impl Future for ReserveFuture {
    type Output = Result<Reservation, ReserveError>;
}

#[non_exhaustive]
pub enum ReserveError {
    InvalidSize,
    PhysicalPreparationFailed,
    MetadataAllocationFailed,
    CapacityOverflow,
}
```

`try_reserve` attempts one immediate grant. It returns `Ok(None)` when an older waiter exists or the
request is not immediately eligible. It may allocate reservation state and prepare physical
capacity when it grants. A zero-byte request returns `ReserveError::InvalidSize`.
`PhysicalPreparationFailed` reports virtual-storage preparation. `MetadataAllocationFailed` reports
failure to reserve queue, registry, bitmap, claim, or ownership metadata. Both leave admission and
ownership state unchanged.

`reserve` creates a `ReserveFuture` without entering admission. The future's first poll converts the
request to a carrier count and performs one admission operation:

```text
first poll
    |
    +-- eligible -> prepare -> Ready(Reservation)
    |
    +-- ineligible -> enqueue with current Waker -> Pending
    |
    `-- zero, count overflow, or preparation failed -> Ready(error)
```

The immediate path allocates no waiter and stores no waker. The queued path rechecks eligibility
while admission is serialized before linking the waiter, so capacity released before registration
is not lost. Public methods reject zero before converting bytes to internal carrier counts.

Internally, reservation admission operates on carrier counts:

```rust
fn try_reserve_count(
    &self,
    envelope: CarrierCount,
) -> Result<Option<Reservation>, ReserveError>;
```

A fresh request cannot bypass an existing waiter. With an empty FIFO, or when examining its head,
an envelope is eligible for a normal grant when:

```text
admission_used + envelope <= configured_capacity
```

An idle-only grant is also eligible when:

```text
active_planned_demand == 0
```

Idle here means that no reservation retains direct-acquisition authority; carriers may remain
owned. The idle-only rule permits one envelope larger than configured capacity and one envelope
blocked only by uncovered charges. Admission cannot force independently driven owners to return.
Granting the envelope makes `active_planned_demand` nonzero and disables another idle-only grant
until it closes.

Strict FIFO provides the liveness argument. While an ineligible oversized request remains at the
head, no fresh request or later waiter can be granted. Earlier reservations therefore close without
replacement, and `active_planned_demand` eventually reaches zero. The head then satisfies the
idle-only predicate. Unreserved acquisition may continue during this interval, but it does not add
active planned demand and cannot prevent the idle-only grant. Permitting bypass would remove this
progress guarantee.

Before publishing a grant, admission prepares physical capacity for the post-grant
`admission_used`. Preparation does not assign carriers to the reservation. Success adds the complete
envelope to `active_planned_demand` and `available_coverage`; `uncovered_charges` is unchanged.
Failure changes none of those quantities.

The private reservation state records its complete envelope and carrier ownership:

```rust
struct ReservationState {
    pool: Arc<PoolInner>,
    envelope: CarrierCount,

    // CLOSED plus direct carrier guards and in-flight direct debits.
    owner_state: AtomicU64,
}
```

`Reservation` is not `Clone`. Its local `direct_outstanding` count cannot exceed `envelope`.
Direct-acquisition authority limits one work item. Available coverage accounts both acquisition
paths and does not assign physical carriers to reservations.

#### Wait queue

The FIFO and waiter slot are private:

```rust
struct Waiter {
    envelope: CarrierCount,
    slot: Arc<WaitSlot>,
}

struct WaitSlot {
    state: Mutex<WaitState>,
}

enum WaitState {
    Queued { waker: Waker },
    Granted(Reservation),
    Failed(ReserveError),
    Taken,
}

enum ReserveFutureState {
    New {
        pool: BufferPool,
        bytes: usize,
    },
    Queued {
        pool: BufferPool,
        slot: Arc<WaitSlot>,
    },
    Complete,
}
```

One slot has the following lifecycle:

```text
Queued
  +-- grant ----------------------> Granted(Reservation)
  |                                  +-- poll ------> Taken + caller owns grant
  |                                  `-- drop ------> Taken + grant is retired
  |
  +-- preparation failure --------> Failed(ReserveError)
  |                                  `-- poll/drop -> Taken
  |
  `-- cancellation ---------------> Taken
```

FIFO drain examines requests in arrival order while holding admission serialization. It locks each
head's `WaitSlot` before preparing or accounting a grant. A stale `Taken` head is unlinked and drain
continues. An ineligible `Queued` head remains linked and stops the pass.

For an eligible `Queued` head, drain holds both locks through preparation, accounting, and
replacement with `Granted`. Preparation failure installs `Failed` without publishing admission and
continues draining. Success and failure both unlink the head before releasing its slot.

Preparation is required only when prepared supply is below the post-grant admission floor and is
amortized across the carriers in each prepared block. Holding the private slot adds no pool-wide
serialization beyond the admission lock already required for preparation. Poll or cancellation of
that same future may wait for preparation to finish.

The terminal slot state is installed before its waker runs. Wakers run after releasing admission
serialization and may schedule an immediate re-poll. Polling a queued future updates the registered
waker when `Waker::will_wake` reports a different task. A notified future therefore consumes its
assigned reservation rather than racing another caller for released capacity.

Drain may invoke a waker synchronously on any thread that releases admission, including a thread
dropping the final payload owner. No pool lock is held. Synchronous pool reentry is valid; a
blocking waker delays only the invoking operation and cannot violate pool correctness.

Dropping a queued future cancels its request and drains again if cancellation exposed a new head.
Dropping a future after grant but before observation drops the stored reservation and retires its
envelope. Grant racing cancellation produces either one caller-owned reservation or one retired
reservation.

The slot lock decides a grant-versus-cancellation race:

```text
cancellation wins                         grant wins
-----------------                         ----------
cancel locks slot first                   drain holds admission and locks slot first
Queued -> Taken                           prepare and account while holding both locks
release slot                              Queued -> Granted
drain unlinks stale head and continues    unlink head, then release locks and wake
no reservation exists                     poll returns it, or drop retires it
```

Cancellation can mark the slot `Taken` before drain locks it even when drain already holds
admission. Drain then removes the stale queue link and continues without preparing or creating a
reservation. If drain locks the slot first, cancellation waits until `Granted` or `Failed` is
published. It then takes and retires a grant or consumes the failure. No interleaving creates two
reservations or loses an admitted envelope.

Admission takes the admission mutex before a waiter-slot mutex. Future drop removes the slot state
under its mutex, releases that mutex, and only then enters admission for cancellation. This order
prevents lock inversion.

Reservation close and front cancellation reconsider the FIFO within their serialized transitions.
A return that removes uncovered charges enters admission serialization after its packed accounting
transition. FIFO drain collects wakers while serialized and invokes them after unlocking.

#### Mutable buffer acquisition and growth

Initial acquisition creates one mutable allocation stream with a minimum writable byte capacity.
The buffer can extend that stream without moving initialized bytes:

```rust
impl BufferPool {
    pub fn acquire(
        &self,
        reservation: &Reservation,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError>;

    pub fn acquire_unreserved(
        &self,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError>;
}

impl PooledBufMut {
    pub fn reserve(&mut self, min_writable: usize) -> Result<(), AcquireError>;
}

#[non_exhaustive]
pub enum AcquireError {
    InvalidSize,
    ForeignReservation,
    ReservationClosed,
    ReservationCapacityExceeded,
    CapacityOverflow,
    PhysicalAllocationFailed,
    MetadataAllocationFailed,
}
```

`BufferPool::reserve` admits a planned memory envelope and returns a reservation.
`PooledBufMut::reserve` extends one existing mutable allocation stream. The shared verb follows the
`BytesMut` convention at the buffer level; the receiver type distinguishes admission from writable
growth.

The pool rejects zero with `AcquireError::InvalidSize`, then converts `min_bytes` to a nonzero
carrier count once. The conversion and acquisition either provide at least that capacity or return
an error. `PooledBufMut::reserve(min_writable)` guarantees that `remaining_mut()` is at least
`min_writable` on success. Zero is a no-op. It uses the buffer's current writable tail before
converting only the shortfall to a carrier count.

`InvalidSize` means the requested byte count is zero. `ForeignReservation` means the reservation
belongs to another pool. `ReservationClosed` means a reserved buffer needs new carriers after its
reservation has closed.
`ReservationCapacityExceeded` means carrier rounding would exceed the reservation's remaining
direct-acquisition authority. `CapacityOverflow` means the request cannot be represented by pool
geometry or packed accounting. `PhysicalAllocationFailed` means physical storage could not be
prepared. `MetadataAllocationFailed` means ownership metadata could not reserve required capacity.
These errors expose no partial initial buffer. Failed buffer growth preserves its initialized
bytes, publication state, and prior writable capacity.

Reserved growth never switches to unreserved acquisition or expands its envelope.
`ReservationCapacityExceeded` leaves the existing buffer unchanged. Its caller may retain the
existing initialized data when the surrounding protocol permits a partial value; a complete part,
range, or other indivisible value fails when its buffer cannot grow to the required size.

Initial direct acquisition and reserved buffer growth publish the complete aggregate charge before
debiting direct-acquisition authority. Unreserved buffers retain only pool authority and cannot
switch to reserved growth; reserved buffers cannot switch to unreserved growth. Both modes debit
aggregate accounting before asking the arena for physical storage:

1. Consume as much `available_coverage` as remains.
2. Add any shortfall to `uncovered_charges` while admission is serialized.
3. Prepare physical capacity for the resulting `admission_used`.
4. Release admission after any required preparation.
5. For reserved acquisition, atomically debit direct-acquisition authority. If close or capacity
   exhaustion rejects the debit, roll back the aggregate charge.
6. Perform optimistic physical claim.
7. On an optimistic miss, acquire a fresh `AdmissionGuard` before serialized arena fallback.
8. Transfer one aggregate charge and optional direct provenance to each `CarrierGuard`.

Reserved acquisition may first load the direct state to reject authority that is already closed or
exhausted before aggregate preparation. That precheck does not reserve authority; the atomic debit
after aggregate publication remains authoritative.

An acquisition fully covered by available coverage performs the aggregate debit through
`CoverageState` without the admission mutex. A shortfall enters admission serialization before
publishing the uncovered charge, prepares through the resulting admission floor, and then releases
admission before optimistic bitmap work.

Publishing the charge before unlock is load-bearing. The charge remains in `admission_used`, so trim
cannot reduce aggregate prepared capacity below a floor that includes the in-flight acquisition.
The claim-trim gate protects any particular block selected concurrently. Deferring charge
publication until after claim would remove the aggregate protection.

Aggregate preparation failure changes no direct-acquisition authority. If the later direct debit is
rejected, the acquisition reverses its aggregate charge before returning the direct-authority
error. A later physical failure rolls back owned bits, retires direct authority, and then reverses
the aggregate charge. If that reversal removes an uncovered charge, it uses the same
admission-drain escalation as [final return](#close-and-return). No writable memory is exposed
before the complete acquisition commits.

`AcquisitionDebit` owns charges awaiting transfer to carrier guards:

```rust
struct AcquisitionDebit {
    pool: Arc<PoolInner>,
    direct: Option<Arc<ReservationState>>,
    untransferred: CarrierCount,
}
```

Each successful physical claim moves one unit out of `AcquisitionDebit`. If a later claim fails,
completed guards return their carriers and charges, and dropping the debit restores every
untransferred charge. Initial acquisition therefore returns the complete requested capacity or an
error. Buffer growth appends the complete carrier-rounded shortfall or leaves the buffer unchanged;
partial physical success is not exposed.

Acquisition does not wait for an existing carrier owner to return. A reusable-storage miss enters
the arena's serialized recheck and growth path. Physical allocation may fail after accounting
succeeds; rollback leaves the reservation and pool usable.

#### Close and return

Closing consumes the public handle and revokes retained direct-acquisition authority. Dropping an
open reservation performs the same transition:

```rust
impl Reservation {
    pub fn close_acquisition(self);
}

impl Drop for Reservation {
    fn drop(&mut self);
}
```

`acquire` is synchronous and borrows the non-cloneable public `Reservation`, so consuming that
handle makes another initial acquire structurally impossible. Existing reserved buffers retain
private `Arc<ReservationState>` values and may race growth with close. The aggregate charge is
published before the packed reservation owner state linearizes that race:

```text
growth debit wins  -> growth may complete; close prevents later growth
close wins         -> growth rolls back its aggregate charge and returns ReservationClosed
```

A growth debit can win this race and then fail physical allocation after close. Its rollback retires
that in-flight debit but leaves the reservation state closed. A buffer operation satisfied entirely
by its existing writable tail does not debit direct-acquisition authority and remains valid after
close.

Coverage remains aggregate rather than partitioned by reservation. The packed close transition
returns the closing reservation's direct outstanding count `D`. At most `E - D` units of its
envelope are nominally unused at that point. Close removes those available units while preserving
coverage attributable to remaining active demand. A direct return may lower `D` and restore
coverage after the close snapshot; availability above remaining active demand is removed as part of
the closing envelope. If unreserved acquisition consumed some nominally unused coverage, close
reclassifies that deficit instead. [Appendix A](#reservation-close) gives the complete transition.

Close and aggregate return may execute concurrently. If return linearizes first, it restores
coverage for close to withdraw. If close linearizes first, close records uncovered charges for
return to remove. Packing coverage and uncovered charges makes both orders produce the same final
accounting state.

Final `CarrierGuard` drop performs these operations in order:

1. Return the physical carrier to the arena.
2. Decrement the originating reservation's outstanding direct-carrier count when direct provenance
   exists.
3. Remove one uncovered charge, or restore one unit of available coverage when no uncovered charge
   remains.

Physical return precedes accounting release. Reversing the order can admit a waiter that finds no
reusable carrier and grows the arena while the responsible carrier is still unavailable.

A return first applies one packed accounting transition. If it restores only available coverage,
the return is complete without admission serialization. If it removes an uncovered charge, the
return then enters admission serialization and drains the FIFO. A reservation request that acquired
the mutex before the accounting transition is reconsidered by that drain; one that acquires it
afterward observes the reduced `admission_used` during its own eligibility check.

FIFO drain runs on the thread performing the final carrier return. It may wait for admission
serialization, process multiple queue heads, prepare blocks for eligible grants, and invoke
registered wakers after releasing all pool locks. The path has no latency bound. This design assumes
that a return which must prepare additional storage is uncommon; it does not assume that returns
removing uncovered charges are uncommon.

An uncovered-charge return may wait behind a serialized acquisition performing a registry-wide
fallback scan. Return latency in that regime can therefore depend on registry size; the
pool-size-independent guarantee applies to coverage-restoring return.

A direct return restores direct-acquisition authority while the reservation remains open. Return
and growth rollback cannot reopen a closed reservation.

**Obligations.**

Obligation IDs use `A` for admission and accounting, `P` for physical storage, `O` for ownership
and delivery, `I` for integration, and `C` for configuration and operations.

- **A1: Accounting bounds.** Available coverage does not exceed active planned demand, and each
  physical carrier owner has one aggregate charge.
- **A2: Charge before storage.** Acquisition installs its complete charge before physical claim.
  A shortfall reaches the resulting admission floor before admission is released.
- **A3: Atomic acquisition.** Initial acquisition and growth expose the complete requested
  carrier-rounded capacity or preserve the prior state. Rollback restores every untransferred
  aggregate and direct-acquisition debit.
- **A4: Direct-acquisition authority.** One mutable buffer retains one acquisition mode.
  Concurrent debits consume direct-acquisition authority exactly once, and close racing growth
  either authorizes the complete debit or rejects it.
- **A5: Close reclassification.** Close removes the complete envelope without removing surviving
  owner charges. A later grant does not reclassify those charges.
- **A6: Return ordering.** Physical return precedes charge release. A return or post-unlock
  rollback that removes an uncovered charge reconsiders the FIFO.
- **A7: FIFO transfer.** Fresh requests do not bypass waiters. Each waiter receives one terminal
  result, made visible before its registered waker runs after admission unlock.
- **A8: Admission lifetime.** Dropping one pool handle does not invalidate reservations, waiters,
  carrier owners, or other handles.

### Physical storage

#### Block geometry and lifetime

The arena prepares, claims, returns, and reclaims carriers. Admission determines how many carriers
may be acquired; the arena selects their addresses.

The physical hierarchy is:

- A **carrier** is the fixed-size, page-granular unit of acquisition, ownership, and accounting.
- A **block** is the preparation and reclamation unit. It contains a fixed number of carriers.
- A **block slot** represents one block. Its virtual range, geometry, and valid bitmap masks remain
  stable for the pool's lifetime.
- A **block incarnation** is one claimable activation of a slot. It owns an occupancy bitmap and an
  `Active`, `Draining`, or `Dead` state.

```text
Arena
+-- BlockSlot 0
|   +-- VirtualRange: [carrier 0][carrier 1][carrier 2][carrier 3]
|   `-- current -----> BlockIncarnation A { state, in_use bitmap }
|
+-- BlockSlot 1
|   +-- VirtualRange: [carrier 0][carrier 1][carrier 2][carrier 3]
|   `-- current -----> BlockIncarnation B { state, in_use bitmap }
|
`-- ...

carrier_address = block_base + carrier_index * carrier_size
block_bytes     = carrier_count * carrier_size
```

Only one incarnation is current for a slot. Revival allocates a new incarnation and bitmap; it never
clears and reuses a bitmap from an earlier activation. An old claim attempt may retain incarnation A
after the slot publishes incarnation B, but the two attempts mutate different bitmap objects.

The block base and carrier size are page multiples, so every carrier begins on a page boundary and
shares no page with another carrier. `final_word_mask` clears padding bits in a partial final bitmap
word; padding bits never name carriers and are never claimable. Complete interior words use
`u64::MAX` without separate stored entries.

**Alternative: Multiple carrier size classes.** Rejected: multiple classes can reduce internal tail
waste but partition reusable capacity and add size-class admission and ownership. One page-multiple
carrier size makes every free carrier suitable for every request. It retains internal tail waste and
per-carrier ownership cost. A request may span blocks; contiguous runs are an optimization.

The pool geometry is private:

```rust
struct PoolGeometry {
    page_size: NonZeroUsize,
    block_size: NonZeroUsize,
    carrier_size: NonZeroUsize,
    carriers_per_block: NonZeroUsize,
    bitmap_words: NonZeroUsize,
    final_word_mask: u64,
}
```

Construction accepts block size in bytes, validates page and carrier alignment and whole-carrier
block size, then derives carrier count, bitmap size, and the final-word mask. Configured capacity
need not be a whole number of blocks; whole-block preparation may exceed configured capacity
without changing admission.

The stable per-block state is:

```rust
/// An exclusively owned, page-aligned virtual address range.
///
/// The address and length remain stable until drop. Mapping-state transitions
/// change accessibility or backing without changing ownership of the range.
struct VirtualRange {
    base: NonNull<MaybeUninit<u8>>,
    len: usize,
}

// SAFETY: the range is exclusively reserved for this object, and sharing the
// wrapper exposes no reference or unsynchronized access to its storage.
unsafe impl Send for VirtualRange {}
unsafe impl Sync for VirtualRange {}

struct BlockSlot {
    id: u32,
    range: VirtualRange,
    geometry: PoolGeometry,

    // Serializes target-specific protection, discard, and revival.
    mapping: Mutex<MappingState>,
    current: ArcSwapOption<BlockIncarnation>,
}

struct BlockIncarnation {
    state: AtomicIncarnationState, // Active | Draining | Dead
    in_use: Box<[AtomicU64]>,      // one bit per carrier
}
```

`VirtualRange` is the provenance root for every carrier pointer in the slot. Private checked
methods derive addresses from `base` and reject offsets or lengths outside the range. Claim and
mapping synchronization determine when the resulting pointer may be dereferenced; sharing
`VirtualRange` alone grants no access. `BlockSlot` is `Send + Sync` by composition and requires no
unsafe trait implementation of its own. Final `BlockSlot` destruction releases the virtual
reservation after no registry, carrier guard, or byte owner can retain the slot.

The bitmap is the source of truth for physical ownership. A set valid bit has exactly one
`ProvisionalBits` or `CarrierAllocation` owner. A clear valid bit is available to claim. Search
hints and rotating origins select where to look; they never own or hide capacity.

#### Preparation and growth

Physical quantities describe different states:

- **Stable virtual range** is exclusively owned by a block slot and remains reserved while
  inaccessible.
- **Committed backing** is physical or commit-accounted memory where the operating system exposes
  that distinction.
- **Prepared capacity** has completed the mapping, protection, and pool-wide setup required for
  acquisition.
- **Physical live** counts carriers unavailable for reuse because a provisional claim, mutable
  buffer, or immutable byte owner retains them.

Configured capacity is an admission policy. It does not imply that the same amount is mapped,
committed, prepared, or resident.

The arena's registry and serialized growth state are:

```rust
struct Arena {
    carrier_size: usize,
    registry: BlockRegistry,
    // Serializes slot preparation, registry rebuild, and exhaustive fallback.
    state: Mutex<ArenaState>,
}

struct ArenaState {
    slots: Vec<Arc<BlockSlot>>,
}

struct BlockRegistry {
    generation: ArcSwap<RegistryGeneration>,
}

struct RegistryGeneration {
    // Stable scan order for physical acquisition.
    slots_in_claim_order: Box<[Arc<BlockSlot>]>,
    // Non-overlapping virtual ranges sorted by start address.
    address_ranges: Box<[AddressRange]>,
}

struct AddressRange {
    // Exposed addresses used only for comparison; end is exclusive.
    start: usize,
    end: usize,
    // Index into RegistryGeneration::slots_in_claim_order.
    slot_index: usize,
}
```

`BlockRegistry` publishes claim order and address classification as one immutable generation.
Optimistic claim scans load `slots_in_claim_order` without taking `ArenaState`. Address
classification uses binary search over `address_ranges` and accepts a match only when its complete
checked range lies within one slot. The integer bounds supply metadata only; carrier pointers are
always derived from `VirtualRange`. Classification does not widen the range-limited provenance of
an incoming immutable view.

Generation construction rejects address overflow, overlap, or an invalid slot index. Growth rebuilds
and publishes both arrays while holding `ArenaState`. Return goes directly to the slot recorded by
the carrier allocation and does not scan the registry.

Any operation that may change `prepared_capacity` acquires `AdmissionState` before `ArenaState`.
Reservation preparation already holds admission serialization when it enters the arena. An
acquisition miss enters admission serialization before taking `ArenaState`; no path holds
`ArenaState` while acquiring `AdmissionState`.

All `BlockSlot` values belong to one compatibility domain and are fungible. A slot does not record
whether its block was prepared below or above configured capacity. Admission state already records
configured capacity and total prepared capacity; reclamation derives excess prepared capacity from
those aggregate quantities.

Preparation operates on whole blocks. Under admission serialization it:

1. Selects a reserved slot or reserves a new stable virtual range.
2. Makes the complete range writable and performs required pool-wide setup.
3. Allocates a fresh incarnation and initializes its bitmap.
4. Adds the block's carrier count to `prepared_capacity`.
5. Publishes the incarnation as `Active`.

Publication follows the prepared-capacity update. A claimant that observes `Active` therefore sees
complete geometry, writable storage, and capacity included in the admission floor. Failure before
publication leaves no claimable incarnation and does not add the block to prepared capacity.

Block rounding may prepare spare carriers without admitting additional demand. Those carriers
remain reusable. Before claiming them, an acquisition consumes available coverage and records any
remainder as uncovered charges.

The single compatibility domain lets exhaustive fallback find enough free capacity whenever the
accounting and bitmap invariants hold.

Reservation grant prepares through the complete post-grant admission floor before publishing the
grant. Existing prepared capacity satisfies part or all of that floor; every newly required block
completes target-specific preparation before it is counted.

On a commit-accounting target, preparation consumes commit capacity for every newly required block.
A grant, including an idle-only oversized grant, can therefore fail before work is dispatched. On a
target where making a range writable does not establish resident backing, a successful grant does
not prove later residency. The exact target operations are defined by the
[platform contract](#platform-contract).

Preparation may run synchronously. Mapping, placement, registration, or operating-system commit can
fail and return an acquisition or reservation error. No progress guarantee places a time bound on
those operations.

#### Carrier claiming and fallback

One arena acquisition requests a complete carrier batch. It returns every requested carrier or an
error; no partial batch is exposed.

The common path performs a bounded amount of optimistic bitmap work from a rotating origin:

1. Load one immutable registry generation.
2. Inspect at most the configured number of bitmap words across `Active` blocks.
3. Keep every bit won from each atomic bitmap operation.
4. Confirm that each touched block remains `Active` after the bitmap mutation.
5. Return the batch when enough carriers have been won.

The scan bound limits common-path work, not usable capacity. A miss can be false because free
carriers may exist outside the inspected locations or may have been returned concurrently.

After an optimistic miss, acquisition retains its provisional bits. For a shortfall path, the
published charge and prepared-capacity floor remain in force. Both covered and shortfall paths
acquire a fresh `AdmissionGuard` at this point, then call the same serialized fallback:

1. Acquire `ArenaState`.
2. Exhaustively scan every `Active` block and retain successful provisional claims.
3. If the batch remains incomplete, establish a writable block and fresh incarnation privately.
4. Set the fallback claimant's required bits in the unpublished incarnation.
5. Add the whole block to `prepared_capacity`.
6. Publish the incarnation and its remaining free carriers as `Active`.
7. Repeat preparation until the batch is complete or preparation fails.

Preclaiming the missing carriers before publication prevents lock-free claimants from consuming the
new capacity and forcing the serialized claimant to grow repeatedly. The fallback therefore
returns the complete batch or a physical allocation error. It does not wait for an existing owner
to return a carrier. The guarantee does not bound mutex scheduling or operating-system allocation
latency.

Concurrent optimistic claimants may retain partial batches while waiting to enter fallback. Those
bits remain charged and safe, but fragmentation can make a serialized claimant prepare a block that
would not have been needed if the acquisitions ran serially. This is an efficiency tail rather than
the repeated-growth failure prevented by private grow-and-claim. Operationally, it can appear as
extra block preparation followed by reclamation.

A fully covered accounting debit avoids admission serialization when optimistic physical
acquisition completes its batch. A shortfall pays admission serialization for accounting and
preparation, but not for its optimistic physical claim.

Bitmap acquisition uses OR-and-keep-won-bits:

```text
candidate = free valid bits selected from one word
previous  = in_use.fetch_or(candidate)
won       = candidate & !previous
```

Bits found set in `previous` belong to other owners and are never cleared by this attempt. Batch
rollback clears only `won`. Return clears exactly the bit carried by one allocation.

The ownership stages are linear:

```rust
struct WonWord {
    word_index: usize,
    // Bits changed from clear to set by one bitmap operation.
    mask: u64,
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, PartialEq)]
struct IncarnationIdentity(NonZeroUsize);

struct ProvisionalBits {
    slot: Arc<BlockSlot>,
    incarnation_identity: IncarnationIdentity,
    // Every remaining bit is cleared on rollback.
    won: Vec<WonWord>,
}

struct CarrierAllocation {
    slot: Arc<BlockSlot>,
    index: u32,
    incarnation_identity: IncarnationIdentity,
}

struct ClaimBatch {
    required: CarrierCount,
    claimed: CarrierCount,
    blocks: Vec<ProvisionalBits>,
}

struct PendingAcquisition {
    guards: Vec<Arc<CarrierGuard>>,
    claim: Option<ClaimBatch>,
    debit: Option<AcquisitionDebit>,
}

struct AcquiredRuns {
    runs: SmallVec<[CarrierRun; 4]>,
    capacity: usize,
}

impl Arena {
    fn complete_claim_serialized(
        &self,
        admission: &mut AdmissionGuard<'_>,
        pending: &mut PendingAcquisition,
    ) -> Result<(), AcquireError>;
}

impl PendingAcquisition {
    fn finish(self) -> Result<AcquiredRuns, AcquireError>;
}

impl Drop for PendingAcquisition {
    fn drop(&mut self) {
        self.guards.clear();
        drop(self.claim.take());
        drop(self.debit.take());
    }
}
```

`IncarnationIdentity` is the nonzero allocation address of the protected `BlockIncarnation`,
captured only as an opaque comparison token. It is never converted back into a pointer. A valid
post-gate owner keeps its incarnation current through its live bit, so allocator address reuse
cannot occur during a valid comparison. Identity remains diagnostic: bitmap ownership and the
claim-trim gate authorize rollback, return, and pointer construction.

A `CarrierAllocation` stores the block slot and carrier index rather than a derived pointer. Its
ownership bit prevents the block mapping from being deactivated while the allocation is live. Byte
access derives a temporary pointer from the slot's validated `VirtualRange` and carrier index.

`PendingAcquisition::finish` succeeds only when `claimed == required`. It consumes the complete
batch and matching accounting debit, converts every provisional bit to one `CarrierAllocation`, and
transfers each allocation and charge into `AcquiredRuns`. Initial acquisition constructs a
`PooledBufMut` from those runs and its growth authority. Buffer growth appends the complete result
only after `finish` succeeds. An incomplete pending acquisition is dropped and cannot initialize or
extend a buffer.

Dropping `ProvisionalBits` rolls back every bit it still owns. Conversion removes a bit from its
provisional mask only after the corresponding `CarrierAllocation` owns it. Unwinding during
conversion therefore returns completed allocations and leaves unconverted bits for provisional
rollback.

The accounting debit is installed before physical acquisition. `PendingAcquisition::drop` returns
completed guards and the remaining physical batch before releasing the debit. During `finish`, each
allocation and one charge are consumed into an `Arc<CarrierGuard>` retained by the pending owner.
Only the complete guard set is arranged into carrier runs and returned as `AcquiredRuns`. Capacity
prepared before a failed acquisition remains `Active` and reusable.

`complete_claim_serialized` requires admission serialization on entry and never acquires it. Its
caller scopes `AdmissionGuard` to the fallback call and releases it before
`PendingAcquisition::finish` or drop:

```text
acquire AdmissionGuard
  -> complete_claim_serialized
release AdmissionGuard
  -> success: finish PendingAcquisition
  -> failure: drop PendingAcquisition
```

The published charge and provisional bits keep the pending acquisition valid after admission is
released. Failure returns provisional and completed physical ownership before reversing the charge
through the final-return escalation. No `PendingAcquisition` drop can reenter admission while its
caller still holds `AdmissionGuard`.

The common scan cost is independent of total pool size. Serialized fallback inspects the complete
registry while holding admission and arena serialization. Its scan work is bounded by registry size,
not by the optimistic scan budget. A larger optimistic budget spends more lock-free work but reduces
entry into a fallback that blocks grants, closes, uncovered-charge returns, and FIFO drains needing
admission serialization. Carrier return performs one bitmap update and does not take the arena
growth lock.

#### Block trim

A trim removes a free whole block from prepared capacity, makes its stable virtual range
inaccessible, and discards its backing while retaining the block slot for later revival. All valid
carrier bits must be clear.

Trim separates the bounded admission transition from protection and discard work:

1. Under admission serialization, verify that removing the block preserves the admission floor.
2. Publish `Draining` on the current `Active` incarnation.
3. Scan that incarnation's bitmap.
4. If a bit is set, restore `Active`; `prepared_capacity` has not changed.
5. If all bits are clear, subtract the block from `prepared_capacity`.
6. Return a single-owner cleanup token and release admission serialization.
7. Make the complete range inaccessible and discard backing outside admission serialization.
8. Publish `Dead` and clear `current`.

Deferring the prepared decrement until the confirming scan succeeds makes trim abandonment
invisible to admission. The scan is bounded by one block's bitmap. Protection changes, device
unregistration, and backing discard do not run while admission is serialized.

The cleanup token owns the obligation to finish deactivation or leave the block nonclaimable.
Dropping an unconsumed token performs the same cleanup. Cleanup marks the token consumed before
calling failure-prone target operations so unwinding cannot issue a second cleanup attempt.

#### Claim-trim gate

Claim and trim use a store-then-load gate on one incarnation:

```text
claim                                      trim
-----                                      ----
protect current incarnation               hold admission serialization
set candidate bits (SeqCst)                publish Draining (SeqCst)
read incarnation state (SeqCst)            read bitmap (SeqCst)

state is Active -> keep won bits           bit observed -> restore Active
state is not Active -> roll back            all free -> confirm trim
```

This is a [Dekker-style gate][dekker], not the complete Dekker mutual-exclusion algorithm. Each side
publishes its intent before reading the other side's state. Claim and trim cannot both miss those
publications under the sequentially consistent order:

```text
claim sees Active  -> trim must observe the set bit and abandon
trim sees all-free -> claim must observe Draining and roll back
```

Acquire/release ordering alone permits both loads to miss the preceding store. The four gate
accesses use sequential consistency, or an equivalent full store-load fence, because the forbidden
execution is a store-buffering cycle.

A claim protects `BlockSlot::current` with an ArcSwap guard before its first bitmap mutation and
through the state read. The guard protects the incarnation metadata from reclamation. It does not
pin committed pages and does not become a reference retained by a successful carrier.

Rollback before and after the state gate use different lifetime proofs:

- **Gate-failure rollback** clears won bits through the original incarnation guard. `current` may
  already name another incarnation.
- **Post-gate rollback** reacquires `current`. The gate-passed live bit proves trim cannot have
  removed or replaced that incarnation.

Final carrier return follows the post-gate proof. It protects `current`, verifies the
comparison-only incarnation identity, and clears the owned bit. `Active` and `Draining` are both
valid during return: clearing the final bit may allow a concurrent trim to confirm all-free. An
absent current incarnation, identity mismatch, `Dead` current incarnation, or missing owned bit is
fail-stop. The release path never clears a bit after an invariant check fails.

`IncarnationIdentity` diagnoses a violated invariant; it does not authorize a bitmap write or
physical access. The live bit prevents replacement. Integer generation is unnecessary for safety
and, if retained as a hint optimization, does not participate in rollback, return, or pointer
construction.

A carrier address is resolved only after its bit passes the `Active` gate. A failed claim never
forms a pointer into the slot. Once the gate passes, the live bit forces trim to abandon and keeps
the stable mapping prepared until final return.

Fresh incarnation metadata closes the stale-rollback race:

```text
claim A protects incarnation A but has not set a candidate bit
trim publishes Draining on A and confirms bitmap A is free
trim deactivates A and removes it from current
revival publishes incarnation B with fresh bitmap B
claim B wins carrier N in bitmap B
claim A then sets carrier N in bitmap A, observes not-Active, and clears bitmap A

bitmap B remains set; claim A never resolves an address
```

ArcSwap keeps incarnation A's metadata alive until its guard is released. A distinct bitmap keeps
A's rollback from changing B.

**Alternative: Reset one bitmap on revival.** Rejected: the stale rollback can clear a bit owned by
the new incarnation after revival resets and reuses the bitmap.

An operation holds at most one ArcSwap guard and drops it after one block gate. Multi-block batches
retain provisional bits rather than guards. This bounds reader-protection use independently of
batch size.

#### Mapping and revival

Mapping state records whether the stable virtual range is accessible:

```rust
enum MappingState {
    Reserved { reclaim_pending: bool },
    Prepared,
    ActivationRecoveryPending,
    DeactivationRecoveryPending,
}
```

Incarnation state controls claims. Mapping state controls access to the stable virtual range.
`MappingState::Prepared` records a writable, fully prepared mapping; it does not by itself include
the block in `prepared_capacity`. The recovery variants distinguish a failed preparation with no
published incarnation from a failed deactivation that retains a `Draining` incarnation.

```text
incarnation state

None
  |
  | prepare mapping; add prepared capacity; publish
  v
Active
  |
  | publish Draining
  v
Draining ---------------- bit observed ----------------> Active
  |
  | all-free: remove prepared capacity
  | deactivation succeeds; attempt backing discard
  v
Dead ------------------- clear current ----------------> None
```

```text
mapping state

Reserved
  | whole-range RW succeeds
  v
Prepared
  | whole-range NONE succeeds
  v
Reserved

Reserved  -- whole-range RW fails ----> ActivationRecoveryPending
Prepared  -- whole-range NONE fails --> DeactivationRecoveryPending

ActivationRecoveryPending   -- whole-range NONE succeeds --> Reserved
ActivationRecoveryPending   -- whole-range RW succeeds ----> Prepared
DeactivationRecoveryPending -- whole-range NONE succeeds --> Reserved
DeactivationRecoveryPending -- whole-range RW succeeds ----> Prepared
```

The state machines couple through ordered capacity and publication transitions:

- Preparation or revival establishes `MappingState::Prepared`, adds the block to
  `prepared_capacity`, and then publishes an `Active` incarnation.
- Trim publishes `Draining` before its confirming scan. A set bit restores `Active` without changing
  mapping state or prepared capacity.
- All-free confirmation removes the block from `prepared_capacity` while the mapping remains
  `Prepared` and the incarnation remains `Draining`. Cleanup then makes the range inaccessible,
  attempts discard, publishes `Dead`, and clears `current`.
- Failed preparation enters `ActivationRecoveryPending` with `current` empty. Failed deactivation
  enters `DeactivationRecoveryPending` with the existing incarnation `Draining`. Both states remain
  outside prepared capacity and are nonclaimable.

A successful whole-range inaccessible transition from either recovery state continues the inactive
path. A successful whole-range writable transition restores mapping state first. Admission then adds
prepared capacity before either publishing a fresh incarnation or restoring a `Draining`
incarnation to `Active`. No failed protection call authorizes a mapping, capacity, or incarnation
transition.

Mapping failures preserve the admission floor. Failed initial preparation or revival does not add
prepared capacity or publish `Active`. Deactivation begins only after the post-removal floor check
and prepared-capacity subtraction; a later protection failure leaves that block nonclaimable and
does not subtract it again. Recovery adds the block back to prepared capacity before publishing or
restoring `Active`.

After successful deactivation, backing discard runs only while the slot remains `Reserved`.
Discard failure leaves the range inaccessible, records `reclaim_pending`, completes the incarnation
transition to `Dead`, and clears `current`. A maintenance pass can retry discard while the slot
remains inactive. Prepare serializes with retry and clears pending reclaim before publishing a
fresh incarnation, so delayed discard cannot reclaim revived pages.

Revival preserves the virtual address:

1. Serialize against trim, recovery, and discard retry.
2. Make the complete reserved range writable and perform required preparation.
3. Allocate a fresh incarnation and bitmap.
4. Add the block to prepared capacity.
5. Publish the fresh incarnation as `Active`.

The default mapping backend keeps each slot's virtual range reserved until pool destruction. The
[platform contract](#platform-contract) defines its target operations and qualification rules.
Every result preserves exclusive ownership of the same address. Deactivation success makes access
fault. Deactivation failure enters `DeactivationRecoveryPending` without assuming that prior
protection remains. Discard success makes backing absent or reclaimable; discard failure may retain
resident pages but cannot make the block claimable.

**Alternative: Allocator-owned blocks.** A page-aligned allocation provides the same common-path
carrier claim, return, and reuse while retained. The standard allocator API does not provide a
per-block operation that revokes access and makes backing reclaimable while preserving ownership of
the allocation. Retaining idle blocks therefore leaves resident-set reduction to allocator policy.
Deallocating them gives up the address and makes physical release behavior allocator-dependent. This
is a viable simpler design when reuse is primary and reclamation is best effort. The selected design
instead gives idle trim an explicit page-level operation and failure contract.

**Alternative: Release and replace virtual ranges.** A pool using operating-system mappings could
release a trimmed range and allocate a new range on later growth. This returns virtual address space
with the backing but makes trim a topology change. Old claim attempts and registry generations must
retire before the address can be reused, or every stale lookup must detect replacement. Revival also
requires a new slot or registry publication. The selected backend can make a block inaccessible and
discard its backing or make it reclaimable without waiting for metadata readers to quiesce. Those
readers may still observe the retired incarnation, but cannot construct or dereference a carrier
pointer. The cost is one retained virtual reservation per peak block slot and target-specific
mapping and recovery state.

Correctness does not assume that a failed protection call leaves the prior protection unchanged.

Prepared bytes are logically uninitialized regardless of initial or recommitted contents. Physical
acquisition exposes `MaybeUninit<u8>`. Only initialized prefixes may later become safe references,
immutable views, or outgoing I/O sources.

Registered, wired, or completion-owned mappings are trim-ineligible until their capability owner
completes the required teardown. A backend that can lose exclusive address ownership on an error
does not satisfy the block-slot contract.

#### Reclamation policy

Reclamation is armed when the transfer-manager scheduler becomes globally idle. The signal schedules
an eligibility-checked trim attempt; it does not assert that every carrier or another component
sharing the pool is idle. An activity epoch identifies the scheduler idle interval, and new managed
work invalidates the pending deadline. Transfer-local idle does not arm reclamation.

When the deadline expires, the pool captures one retention target for that idle epoch:

```text
configured_block_ceiling =
    round_up_to_whole_blocks(configured_capacity)

idle_retention_target =
    round_down_to_whole_blocks(configured_block_ceiling * 25%)
```

Pools whose configured ceiling spans fewer than four blocks retain no warm block after the idle
deadline. This preserves reclamation for small pools instead of letting one block consume the
complete retention allowance.

Prepared capacity above `configured_block_ceiling` is aggregate excess. It is not attached to
particular blocks. Any free block is eligible while both conditions remain true:

```text
prepared_capacity_after >= admission_used

prepared_capacity_after >= idle_retention_target
```

The target is not recomputed as blocks are reclaimed or cleanup retries run. Prepared capacity below
the target remains cached; reclamation never prepares capacity to reach it. Capacity above
`configured_block_ceiling` is reclaimed before `idle_retention_target` can be reached. Live
carriers, provisional bits, and whole-block geometry can leave more capacity than the target.

When no eligible block is free, the coordinator keeps the idle reclaim request pending and scans
again after a bounded delay. New managed work cancels that retry by invalidating the idle epoch.
Carrier return does not have to notify the coordinator because the next scan observes blocks that
have become free. Reclaiming one block never prevents growth, claim, or trim of unrelated blocks.

The timeout provides cache hysteresis and is not a safety mechanism. Its duration, retry cadence,
carrier size, block size, optimistic scan budget, and rotating-origin policy are measurement
choices.

**Obligations.**

- **P1: Stable geometry.** Every carrier is page-aligned, page-multiple, wholly contained in one
  stable virtual range, and represented by one valid bitmap bit. Padding bits never become
  carriers.
- **P2: Physical ownership.** A set valid bit has one `ProvisionalBits` or `CarrierAllocation`
  owner. Pointer construction follows a successful `Active` gate and stays within the owning
  `VirtualRange`.
- **P3: Activation publication.** Initial preparation or revival adds prepared capacity before
  publishing a fresh `Active` incarnation. Protection recovery adds prepared capacity before
  restoring its retained `Draining` incarnation to `Active` and does not reset that bitmap.
- **P4: Claim-trim exclusion.** Claim protects its incarnation through the state gate. Claim and
  trim cannot both miss the other's publication; gate-failure rollback uses the protected
  incarnation, and a gate-passed bit prevents replacement until return.
- **P5: Batch transfer.** Provisional conversion transfers each won bit once. Incomplete
  acquisition publishes no buffer and returns all physical ownership before accounting rollback.
- **P6: Serialized fallback.** Fallback receives admission before arena state, never reacquires
  admission internally, exhausts prepared capacity before growth, and reserves the requested batch
  before publishing new free capacity.
- **P7: Block deactivation.** Trim removes prepared capacity only after all-free confirmation.
  Failed protection leaves the block nonclaimable, and discard retry cannot affect a revived block.
- **P8: Reclamation bounds.** Reclamation preserves the admission floor and the captured
  `idle_retention_target`.
- **P9: Fail-stop ownership.** Missing current state, identity mismatch, unexpected lifecycle
  state, or a missing owned bit aborts before allocator mutation continues.

### Ownership and delivery

Physical ownership follows carriers. Byte presentation follows initialized ranges. A carrier run
groups adjacent writable carriers for I/O, while a segment groups contiguous immutable bytes for
reading. Neither changes the carrier-level accounting or return unit.

```text
PooledBufMut --> WritableCarrier --+
                                   |
Bytes --------> PooledWindow ------+--> Arc<CarrierGuard>
                                   |
SegmentedBytes -> Hold::Pooled ----+
```

`WritableCarrier`, `CarrierRun`, `PooledWindow`, `Segment`, `OwnedRange`, and `Hold` are private.
`PooledBufMut` is the public mutable acquisition result. `SegmentedBytes` is the public immutable
output type.

#### Carrier ownership

One completed carrier acquisition creates one `CarrierGuard`:

```rust
struct CarrierGuard {
    // State required to validate return and release the aggregate charge.
    pool: Arc<PoolInner>,
    // Single-owner physical-return capability, taken once by final drop.
    allocation: Option<CarrierAllocation>,
    // Originating reservation state for a direct acquisition.
    direct: Option<Arc<ReservationState>>,
}

// SAFETY: the allocation names a stable slot, shared state is synchronized,
// and the guard exposes no pointer access.
unsafe impl Send for CarrierGuard {}
unsafe impl Sync for CarrierGuard {}
```

The guard owns one carrier and one aggregate charge. `direct` records optional
reservation provenance; it does not assign available coverage to that reservation. Byte containers
do not retain the public `Reservation` handle. Reserved mutable buffers and direct carrier guards
retain its private state, so close may race buffer growth and may precede final carrier return.

`CarrierGuard` is allocated behind `Arc` but is not itself cloneable. Every owner of a range within
the carrier clones the same `Arc<CarrierGuard>`. Final `Arc` drop performs the
[close-and-return sequence](#close-and-return) exactly once. Direct provenance restores
direct-acquisition authority only while the reservation remains open. After close, return retires
the local owner count without reopening acquisition.

Before clearing the bitmap bit, return verifies that the slot still contains the recorded
incarnation, that its lifecycle state permits return, and that the recorded bit remains set. The
live bit prevents trim from replacing that incarnation in a valid execution. A missing
incarnation, identity or lifecycle mismatch, or clear bit indicates double return, stale ownership,
allocator corruption, or a broken trim/revival invariant. The non-returning fail-stop handler
aborts without clearing an unexpected bit or releasing accounting.

The strong `Arc<PoolInner>` keeps the slot registry, admission state, and final-return path alive
after public pool handles or reservations have dropped. One escaped immutable view therefore
retains the pool as well as its carrier. The carrier's live bitmap bit prevents trim of its block.

The guard is per carrier, never per run or segment. A run-level guard would make one surviving byte
pin every carrier in the run.

**Alternative: Store a holder count beside each bitmap bit.** Rejected: trim would still use the
bitmap, so the count would add a second liveness fact that must agree with it. `Arc<CarrierGuard>`
keeps liveness in the owner graph and allocates refcount state only for checked-out carriers.

#### Mutable buffers

`PooledBufMut` is a growable mutable allocation stream over one or more carrier runs. Growth appends
carriers and never reallocates or moves initialized bytes. Each buffer is one carrier-rounding
domain; independent buffers cannot consume each other's writable tails.

```text
PooledBufMut
  +-- CarrierRun A: block A [carrier 1][carrier 2][carrier 3]
  `-- CarrierRun B: block B [carrier 7]
```

Adjacency is opportunistic. Acquisition returns the requested carrier count even when the result
requires several runs. `CarrierRun` has no independent ownership or accounting.

```rust
pub struct PooledBufMut {
    // Selects reserved or unreserved acquisition for every later growth.
    growth: GrowthAuthority,
    // Preserves logical byte order; entries after the fourth spill to heap.
    runs: SmallVec<[CarrierRun; 4]>,
    // Next byte at which mutable initialization can continue.
    write_cursor: BufferCursor,
    // First initialized byte awaiting publication.
    publish_cursor: BufferCursor,
    // Initialized bytes between the publication and write cursors.
    initialized: usize,
    // Initialized unpublished bytes plus uninitialized writable capacity.
    retained_capacity: usize,
}

enum GrowthAuthority {
    Reserved(Arc<ReservationState>),
    Unreserved(Arc<PoolInner>),
}

struct CarrierRun {
    // Stable slot containing every carrier in this run.
    slot_id: u32,
    // Index of the run's first carrier within the slot.
    first_carrier: u32,
    // Per-carrier ownership in ascending address order.
    carriers: Vec<WritableCarrier>,
}

struct WritableCarrier {
    // None after publication transfers the carrier's final mutable hold.
    guard: Option<Arc<CarrierGuard>>,
    // Unique mutable authority over the carrier's unpublished suffix.
    writable: ExclusiveRange,
    // Initialized prefix within `writable`.
    initialized: usize,
}

struct ExclusiveRange {
    ptr: NonNull<MaybeUninit<u8>>,
    len: usize,
}

struct BufferCursor {
    run: usize,
    carrier: usize,
}
```

`SmallVec` is an inline-storage optimization. Its capacity does not limit acquisition or
fragmentation and may change without changing the ownership contract.

Carrier entries remain positionally stable while the buffer lives. Publishing a carrier's complete
remaining range takes its guard and leaves an empty metadata entry; the buffer then retains no
ownership or charge for that carrier, and cursor normalization skips the entry. Stable entries keep
publication from shifting the write cursor while later growth appends carriers.

`ExclusiveRange` is linear and private. It cannot be cloned. Its consuming split operations produce
disjoint ranges, so publication can remove an initialized prefix while retaining exclusive mutable
authority over the suffix.

Every acquired byte is logically uninitialized, including bytes returned from newly prepared or
zero-filled pages. Initialization advances from the write cursor and publication advances from the
publish cursor:

```rust
impl PooledBufMut {
    pub fn capacity(&self) -> usize;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn initialized_chunk(&self) -> &[u8];
    /// Ensures at least `min_writable` uninitialized writable bytes remain.
    pub fn reserve(&mut self, min_writable: usize) -> Result<(), AcquireError>;
    pub fn publish_prefix(&mut self, count: usize) -> Bytes;
    pub fn freeze(self) -> SegmentedBytes;
}

unsafe impl BufMut for PooledBufMut {
    fn remaining_mut(&self) -> usize;
    fn chunk_mut(&mut self) -> &mut UninitSlice;
    unsafe fn advance_mut(&mut self, count: usize);
}

// SAFETY: moving transfers every unique range capability. Carrier and growth
// authority remain live through synchronized shared state.
unsafe impl Send for PooledBufMut {}
```

`capacity` reports `len() + remaining_mut()`: all capacity still retained by the mutable buffer.
Growth increases it and publication removes the published byte count from it. `len` reports
initialized but unpublished bytes, and `remaining_mut` reports exclusive uninitialized writable
capacity.

`initialized_chunk` returns the contiguous initialized prefix beginning at the publication cursor.
It is empty exactly when `len() == 0`; when `len()` is nonzero, cursor normalization makes it
nonempty. Its length can be smaller than `len()` because publication does not cross a carrier
boundary:

```text
8 KiB carriers, 13 KiB initialized

carrier A: 8 KiB initialized
carrier B: 5 KiB initialized + 3 KiB writable

len()                     = 13 KiB
initialized_chunk().len() = 8 KiB
```

`reserve(min_writable)` guarantees `remaining_mut() >= min_writable` on success. It returns
immediately when the existing tail satisfies the request. Otherwise it computes:

```text
shortfall = min_writable - remaining_mut()
new_carriers = ceil(shortfall / carrier_size)
```

The buffer then acquires and appends the complete carrier-rounded shortfall through its fixed
`GrowthAuthority`. Reserved buffers retain private reservation state; unreserved buffers retain
the pool. A buffer never changes modes. Failure leaves its runs, cursors, initialized bytes, and
existing writable tail unchanged.

Closing a reservation revokes new-carrier acquisition but does not invalidate existing buffers.
After close, `reserve` succeeds when the current tail already satisfies `min_writable` and returns
`AcquireError::ReservationClosed` when more carriers are required.

Carrier rounding is therefore per buffer rather than per call to `reserve`. With 4 KiB carriers, a
single buffer filled in five 10 KiB increments acquires thirteen carriers for 50 KiB:

```text
one growable buffer                 ceil(50 KiB / 4 KiB) = 13 carriers
five independent 10 KiB buffers    5 * ceil(10 KiB / 4 KiB) = 15 carriers
```

The second shape remains valid but consumes more direct-acquisition authority. Callers incrementally
building one independently delivered value retain one buffer and grow it. Separate values with
independent I/O, publication, retry, or ownership lifetimes use separate buffers. A reservation
for 50 KiB provides thirteen carriers and therefore cannot satisfy that five-buffer shape,
which requires fifteen, even though the requested byte lengths sum to 50 KiB. Callers that require
independent buffers should size the envelope from their rounded buffer shapes or use
`BufferPool::carrier_size()` to align acquisitions and fills, except for each final extent.

The write and publication cursors normalize past exhausted carriers and runs before observing their
current ranges. The [`bytes::BufMut`][bytes-buf-mut] implementation therefore returns a nonempty
`chunk_mut` exactly when `remaining_mut` is nonzero, as required by the
[`BufMut::chunk_mut` contract][bytes-chunk-mut]. Advancing exactly to a carrier boundary makes the
next call observe the next writable carrier rather than an empty suffix.

`advance_mut(count)` asserts that the first `count` bytes exposed through writable ranges were
initialized. The caller must not advance beyond the ranges supplied by the preceding mutable view
or completed vectored operation. A crate-private vectored adapter exposes raw pointer-length pairs,
not `&mut [u8]`, and distributes the completed byte count across carriers in order.

`PooledBufMut` does not implement `Buf`. Consuming initialized bytes with `Buf::advance` would be
ambiguous with transferring them into immutable ownership. Publication is explicit through
`publish_prefix`.

Moving `PooledBufMut` between threads transfers its exclusive authority. It is `Send` when
`CarrierGuard`, `PoolInner`, and `ReservationState` are `Send + Sync`, but it is not `Sync` and does
not expose shared mutation.

Dropping `PooledBufMut` drops its growth authority and every remaining `WritableCarrier`. A carrier
without a published view then returns immediately. `freeze` ends growth, consumes the mutable
buffer, and transfers only initialized unpublished ranges into `SegmentedBytes`. It drops each
mutable hold with no initialized output; the carrier returns unless an earlier published view still
shares its guard. An initialized prefix in a partially used carrier retains that carrier and
discards its unused suffix.

#### Immutable `Bytes` publication

`publish_prefix` normalizes the publication cursor to the first carrier with initialized
unpublished bytes, then returns one contiguous `Bytes` from that carrier. It does not cross a
carrier boundary. The operation consumes mutable authority over the prefix and retains mutable
authority over the disjoint suffix. Its precondition is
`0 < count && count <= initialized_chunk().len()`; violation panics.

```text
before publication

+--------------------+---------------------------+
| initialized prefix | exclusive writable suffix |
+--------------------+---------------------------+
                 \             /
                  \           /
               Arc<CarrierGuard>

after publication

Bytes -> PooledWindow ---------+
                               +--> Arc<CarrierGuard>
exclusive writable suffix -----+
```

The range-limited owner contains no mutable capability:

```rust
struct PooledWindow {
    guard: Arc<CarrierGuard>,
    ptr: NonNull<u8>,
    len: usize,
}

impl AsRef<[u8]> for PooledWindow {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: construction consumed mutable authority over this initialized
        // range, and the guard keeps its carrier live.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

// SAFETY: the pointer names immutable initialized storage retained by the guard.
unsafe impl Send for PooledWindow {}
```

Construction verifies that the range is initialized, lies within the guard's carrier, and does not
overlap the retained `ExclusiveRange`. The guard's live bit prevents trim, and its strong pool
reference keeps the stable virtual range and return machinery alive.

`Bytes::from_owner` moves `PooledWindow` into the `Bytes` owner and drops it only after every clone
of that `Bytes` has dropped ([`from_owner`][bytes-from-owner]). A nonempty `Bytes::slice` clones the
same underlying handle before narrowing its pointer and length ([`slice`][bytes-slice]). The owner
vtable increments one owner refcount on clone and drops the boxed owner after the final decrement
([owner vtable][bytes-owner-vtable]).

Separate calls to `publish_prefix` create separate `PooledWindow` owners. Each owner holds a clone
of the same carrier guard:

```text
one carrier

+----------+----------+----------+-------------------------+
| Bytes A  | Bytes B  | Bytes C  | writable suffix         |
+----+-----+----+-----+----+-----+------------+------------+
     |          |          |                  |
 PooledWindow PooledWindow PooledWindow WritableCarrier
     |          |          |                  |
     +----------+----------+------------------+
                       |
                Arc<CarrierGuard>
```

One published `Bytes` pins at most one carrier. One carrier may be pinned by several non-overlapping
published views, their clones and slices, and one disjoint writable suffix. The carrier returns only
after all of them release the shared guard. A surviving short view can therefore retain at most one
carrier of otherwise unused capacity.

Converting an owner-backed `Bytes` into `BytesMut` performs a deep copy
([`from_owner` contract][bytes-from-owner]). A consumer cannot recover mutable access to pool
storage through the immutable handle.

#### `SegmentedBytes`

`SegmentedBytes` presents immutable data without requiring physically adjacent carriers. A
`Segment` is one contiguous initialized presentation range. An `OwnedRange` records which owner
retains each subrange within that segment.

```rust
#[derive(Clone)]
pub struct SegmentedBytes {
    // Presentation ranges in logical byte order.
    segments: VecDeque<Segment>,
    // Consumed bytes within the front segment.
    front_offset: usize,
    // Bytes remaining from this cursor.
    remaining: usize,
}

#[derive(Clone)]
struct Segment {
    // Concrete slot proving common pointer provenance before coalescing.
    slot: Option<Arc<BlockSlot>>,
    // Start and length of one contiguous initialized presentation range.
    ptr: NonNull<u8>,
    len: usize,
    // Ordered, complete ownership coverage for the presentation range.
    owners: VecDeque<OwnedRange>,
}

#[derive(Clone)]
struct OwnedRange {
    // Exclusive segment-relative end of this owner.
    end: usize,
    // Keeps bytes through `end` immutable and live.
    hold: Hold,
}

#[derive(Clone)]
enum Hold {
    Pooled(Arc<CarrierGuard>),
    View(Bytes),
}

struct SegmentedBytesBuilder {
    // Optional pool used to recover canonical pointers for opaque views.
    pool: Option<Arc<PoolInner>>,
    // Presentation ranges assembled so far.
    segments: VecDeque<Segment>,
    // Sum of the assembled presentation lengths.
    remaining: usize,
}

impl SegmentedBytesBuilder {
    fn push_segmented(&mut self, buffer: SegmentedBytes);
    fn push_view(&mut self, view: Bytes);
    fn finish(self) -> SegmentedBytes;
}
```

`Hold::Pooled` retains storage frozen directly from `PooledBufMut`. `Hold::View` retains an existing
immutable producer view, whether pool-backed or foreign. `push_segmented` consumes the input's
remaining segments and their existing holds; it does not reconstruct ownership from pointers.
`PooledBufMut::freeze` constructs pooled segments directly with stable slot identity and pointers
derived from the slot's `VirtualRange`. Owner boundaries need not be presentation boundaries for
those direct pooled ranges:

```text
one Segment, using 64 KiB carriers for illustration: contiguous presentation [0..192 KiB]

+--------------------+--------------------+--------------------+
|      0..64 KiB     |    64..128 KiB    |   128..192 KiB    |
+---------+----------+---------+----------+---------+----------+
          |                    |                    |
       owner A              owner B              owner C
```

Construction extends the previous segment only when both ranges identify the same concrete
`BlockSlot` by `Arc::ptr_eq`, the previous range ends at the next range's pointer, and both pointers
retain provenance derived from that slot's stable `VirtualRange`. A pool-local numeric slot index is
not sufficient identity. The owners keep every byte in the merged range live.

An opaque `Bytes` remains `Hold::View(Bytes)` and never authorizes physical or accounting return.
When the builder has the pool that may have produced the view, integer address classification can
locate a complete range within one concrete `BlockSlot`. Classification does not widen the
range-limited provenance of `Bytes::as_ptr()`. Instead, the builder derives a new checked pointer
from that slot's `VirtualRange` provenance root while the original `Bytes` remains the initialized
immutable owner. Pool-produced byte owners keep their carrier bits live, so trim cannot deactivate
the range while any classified view remains.

Classified views may coalesce with adjacent classified or direct pooled ranges from the same
concrete slot. A foreign view, a view from another pool, or a range crossing a slot boundary remains
an independent segment. Construction never creates another `CarrierGuard` or clears a bitmap bit
from an address.

**Alternative: Recover carrier ownership from incoming views.** Rejected: an unseen clone or slice
may still retain the original owner. Returning the carrier from pointer metadata would make live
immutable memory available for mutation.

Every unconsumed byte is initialized, immutable, and retained by an owner. The front segment's base
pointer may precede the read cursor after `advance` releases a consumed prefix; no method
dereferences that prefix. Moving or sharing the container does not move the remaining backing or
permit mutation:

```rust
// SAFETY: methods form slices only over unconsumed immutable initialized
// ranges retained by owners, and no such range overlaps mutable authority.
unsafe impl Send for SegmentedBytes {}
unsafe impl Sync for SegmentedBytes {}
```

#### Reading and contiguous conversion

The [`bytes::Buf`][bytes-buf] implementation is the primary interface for reading
`SegmentedBytes` without making the backing contiguous:

```rust
impl Buf for SegmentedBytes {
    fn remaining(&self) -> usize;
    fn chunk(&self) -> &[u8];
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize;
    fn advance(&mut self, count: usize);
}

impl SegmentedBytes {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn into_contiguous(self) -> Bytes;
}

impl From<Bytes> for SegmentedBytes {
    fn from(bytes: Bytes) -> Self;
}
```

`chunk` returns the remaining portion of the front segment. When `remaining` is nonzero, cursor
normalization ensures that `chunk` is nonempty. `chunks_vectored` writes at most `dst.len()`
entries, returns the number written, and emits at most one `IoSlice` per remaining segment. Segment
boundaries, not carrier owners or producer frames, determine those entries.

`advance` crosses segment and owner boundaries in order. It drops each `OwnedRange` when this
cursor passes that owner's final byte. Crossing a carrier boundary can therefore return that
carrier before the complete `SegmentedBytes` is consumed. Several owner ranges may share one
carrier guard; dropping one range returns the carrier only when no other range or clone retains it.
`advance` panics if `count` exceeds `remaining`.

Cloning `SegmentedBytes` clones its current cursor state and remaining holds. Each clone advances
independently. Advancing one clone cannot release backing still reachable through another. `len`
and `is_empty` report the state of that clone's cursor.

`into_contiguous` consumes the remaining data:

| Remaining shape  | Result                                  | Pool effect                                             |
| ---------------- | --------------------------------------- | ------------------------------------------------------- |
| Empty            | Empty `Bytes`                           | Remaining holds are released                            |
| One segment      | Owner-backed `Bytes` without copying    | Source carriers remain charged until result drop        |
| Several segments | One heap allocation and an ordered copy | Each source hold is released after its bytes are copied |

The single-segment case moves the remaining owner ranges into a `ContiguousOwner` and constructs
`Bytes::from_owner` over its adjusted pointer and length. The multi-segment case copies through the
`Buf` implementation into one `BytesMut`. Each source hold remains live through its copy and is
released only after the copy cursor passes its final byte. The result is then frozen. Copied bytes
are ordinary heap memory outside pool accounting.

```rust
struct ContiguousOwner {
    ptr: NonNull<u8>,
    len: usize,
    owners: VecDeque<OwnedRange>,
}

impl AsRef<[u8]> for ContiguousOwner {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the owners retain every byte in this immutable initialized range.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

// SAFETY: the pointer names immutable initialized storage retained by owners.
unsafe impl Send for ContiguousOwner {}
```

The public contract does not expose segment iterators. Borrowed reads use `Buf`; callers that need
one independently owned contiguous value use `into_contiguous`. `From<Bytes>` constructs a
pool-independent one-segment value and retains the supplied `Bytes` as its owner.

**Obligations.**

- **O1: Carrier guard.** One carrier acquisition creates one guard and one charge. The guard covers
  one carrier and returns it only after every writable and immutable owner releases the shared hold.
- **O2: Mutable authority.** Every writable range has one non-cloneable authority. Reserved and
  unreserved growth modes cannot be exchanged, and close cannot invalidate retained ranges.
- **O3: Growth.** A mutable buffer consumes its retained tail before acquiring the complete
  carrier-rounded shortfall. Failure preserves all existing bytes, cursors, and capacity.
- **O4: Initialization cursors.** `advance_mut` stays within completed writes. `BufMut` and
  `initialized_chunk` normalize across carriers and do not report an empty current range while the
  corresponding byte count is nonzero.
- **O5: Publication.** Publication and freeze expose initialized bytes only. Publishing a prefix
  consumes its mutable authority, leaves a disjoint suffix, and keeps every pointer within its live
  carrier.
- **O6: Immutable ownership.** Clones, slices, and separate windows share carrier charges without
  duplicating them. Freeze returns wholly unused carriers and retains partial carriers only through
  initialized ranges.
- **O7: Segment ownership.** Ordered holds cover every unconsumed byte. Coalescing requires direct
  pooled ranges or classified opaque views with equal concrete slot identity, slot-rooted
  provenance, and pointer adjacency. Unclassified views remain separate segments and no view
  authorizes physical or accounting return.
- **O8: Read cursors.** `Buf::advance` releases only crossed holds, `chunks_vectored` respects
  destination capacity, and clones retain independent cursors and owners.
- **O9: Contiguous conversion and unsafe access.** Zero-copy conversion retains source charges.
  Copying retains each source through its final read. Slice construction and `Send` or `Sync`
  implementations rely on initialized immutable ranges, stable addresses, disjoint mutable
  authority, and live owners.

## Integration

Transfer-manager work that can present a `Reservation` uses reserved acquisition. Upload staging and
the default download path both follow this rule. Hyper returns foreign `Bytes`; the transfer manager
copies decoded payload into reserved pooled storage before retaining or delivering it. The copy
transiently holds transport memory outside pool accounting but requires no transport modification.

An integration boundary that cannot carry a reservation may use unreserved acquisition. It accounts
writable storage before use and publishes through the same `Bytes` and `SegmentedBytes` ownership
types. The baseline Hyper integration does not use this path.

### Scheduler admission and dispatch

Each work item reserves its memory envelope from `poll_work()` before becoming dispatchable. The
transfer stores a `ReserveFuture` with the candidate work while admission is pending. A ready future
moves the `Reservation` into the `IoRequest`; a pending future remains in transfer state and causes
`PollWork::Pending`.

`PollWork` and `IoRequest` are transfer-manager scheduler types, not pool types. The memory contract
requires only that the ready variant's `io` work data owns the granted `Reservation`.

`poll_work()` has no `Context` parameter. A crate-private scheduler adapter supplies one:

```rust
pub(crate) struct SchedulerWake {
    scheduler: Scheduler,
    transfer_id: TransferId,
}

impl Wake for SchedulerWake {
    fn wake(self: Arc<Self>) {
        self.scheduler.wake(self.transfer_id);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.scheduler.wake(self.transfer_id);
    }
}
```

The transfer polls its stored future with a `Waker` built from `SchedulerWake`. The scheduler
records every wake request even while the transfer descriptor is claimed for `poll_work()`. If
the future is granted after registering its waker but before `poll_work()` returns `Pending`, the
scheduler observes that mark when it releases the claim and reinserts the transfer. No notification
is lost in the poll-to-pending interval.

Reservation or preparation failure produces no `IoRequest`. Cancellation removes queued admission
by dropping the stored future, or consumes an already granted reservation before dispatch.
Cancellation after dispatch stops producers, closes direct-acquisition authority, and drops
unpublished mutable buffers. Immutable bytes already published retain their carriers and charges.

Execution code uses `pool.acquire(&reservation, min_bytes)`. The download collector uses the same
reservation when copying foreign transport bytes into pooled staging. Scheduler admission remains
independent of the transport's request and connection lifecycle.

The reservation envelope includes the carrier-rounded shape of every independent buffer that can be
live at once. `ReservationCapacityExceeded` while completing a part or range indicates an
envelope-sizing or buffer-usage mismatch. The transfer manager fails that unit rather than
expanding its reservation, switching to unreserved acquisition, or treating an incomplete value as
complete.

### Upload staging and retry

An upload that stages part data in the pool uses one `PooledBufMut` for each concurrently staged
part. A known part length may be acquired in one request. An incremental source retains the same
buffer and calls `reserve` before each fill, so the part's writable tail remains available across
reads. A source read advances initialization only by its completed byte count; short completion,
error, or cancellation leaves the remaining range uninitialized and unpublishable. The completed
part freezes into `SegmentedBytes`:

```text
Reservation
    |
    v
pool.acquire(part capacity)
    |
    v
PooledBufMut
    |
    +-- PooledBufMut::reserve(min_writable) --> source read --+
    |                                                        |
    +<-------------------------------------------------------+
    |
    `-- freeze --> SegmentedBytes
                         |
            +------------+------------+
            |                         |
       attempt 1 cursor           retry cursor
            |                         |
            +------ same holds -------+
```

Parallel parts use separate buffers because their fill, retry, publication, and ownership lifetimes
are independent. A final partial carrier belongs to that part and may remain retained by its final
immutable segment.

Each SDK attempt receives a fresh body and cursor over the same immutable holds. The retained
`SegmentedBytes` remains live through SDK retries and any transfer-manager retry checkpoint that can
replay the part. `SdkBody::retryable` rebuilds a body for each clone attempt, and
`SdkBody::try_clone` succeeds only when such a rebuild operation exists
([`SdkBody` retry support][sdk-body-retry]).

Retry requires either a source that can be read again or immutable bytes retained through the retry
window. An addressable file or range source can rebuild an attempt by rereading its input. A
forward-only source retains the staged `SegmentedBytes` or supplies another replay layer.
Caller-owned `Bytes` and other already-resident upload memory remain outside pool accounting unless
copied into pooled storage.

The body reports its exact remaining length through `size_hint`; segmentation does not make the
length unknown. The SDK body adapter forwards each `Bytes` frame and its size bounds without
requiring one contiguous value ([`SdkBody` HTTP body adapter][sdk-body-http]).

Segmentation does not require a gather copy for checksum calculation, signing, or aws-chunked
framing. The body adapters consume frames in order. A streaming checksum body updates the checksum
per data frame and emits the value in trailers ([checksum body][sdk-checksum-body]). A segmented
streaming `SdkBody` therefore selects a different wire shape from a single in-memory `Bytes` when
SDK-owned checksum calculation is enabled: the checksum is carried in an aws-chunked trailer rather
than an HTTP header ([S3 checksum selection][s3-checksum-selection],
[aws-chunked selection][s3-chunked-selection]). The upload path may preserve header placement by
calculating the checksum while filling and setting the header before transmit. Both paths retain a
segmented body; neither requires a gather copy.

The upload calls `Reservation::close_acquisition` or drops the reservation after no staging buffer
may require another carrier. Replaying an existing `SegmentedBytes` does not require open
direct-acquisition authority. Closing does not release carriers retained by the retry body. A
staging buffer that survives close may consume its existing writable tail but receives
`ReservationClosed` if it attempts to grow.

### Download receive and delivery

A ranged GET reserves its planned payload range before dispatch. The reservation is not passed
through the SDK to Hyper. The collector copies foreign response frames into one reserved mutable
stream:

```text
poll_work()
  -> poll BufferPool::reserve(range envelope)
  -> execute(GET)
       |
       v
Hyper Frame<Bytes>
       |
       v
pool.acquire(&reservation, min_bytes)
       |
       v
copy initialized bytes -> publish or freeze -> SegmentedBytes
```

Each response has an independent `PooledBufMut`. The collector reuses its writable tail across
frames and calls `reserve` only when the next copy does not fit. Concurrent ranges and retry
attempts use separate buffers because their transport and terminal lifetimes are independent.
Foreign input is released after its bytes have been copied.

The reservation closes after the response reaches a terminal state and no transfer retry or reserved
acquisition can begin. Download output may outlive the reservation, work item, HTTP client, public
pool handle, or transfer manager. Each pooled byte owner retains its `CarrierGuard` and `PoolInner`
until final drop.

Error and control bodies have no guessed pre-dispatch allowance. They remain foreign unless a
managed path deliberately copies them into reserved staging.

Every `Bytes` or `SegmentedBytes` clone extends the lifetime of its carrier charges. After
reservation close, retained owners can keep uncovered charges live and delay grants anywhere in the
shared FIFO. The pool cannot distinguish retry-required retention from an incidental clone.
Cancellation and terminal paths release every transfer-manager-owned clone not retained by an
active retry, delivery, I/O, or caller lifetime. A cache that intentionally retains pooled bytes
remains charged and pairs retention with its own admission and eviction policy.

#### Disk writes

Once decoded payload is in pooled carriers, the download sink writes the same storage without
another staging copy. It consumes `SegmentedBytes` through `Buf`. Vectored writes call
`chunks_vectored` and advance the cursor by the completed byte count, including on short writes:

```text
SegmentedBytes
  -> chunks_vectored
  -> submit [IoSlice A, IoSlice B, ...]
  -> complete N bytes
  -> advance(N)
  -> release every owner crossed by the new cursor
```

The buffer owner and all I/O metadata remain live until synchronous return or asynchronous terminal
completion. Cancelling completion-based I/O does not release owners until cancellation or the
original operation produces that completion.

Submission size is independent of carrier size. The I/O path may coalesce already available,
file-offset-contiguous segments into a larger submission, but it does not retain carriers waiting
for a preferred byte count. Buffered vectored writes accept any published segment layout.

Direct I/O adds address, length, and file-offset constraints. Page-aligned carrier bases alone do
not satisfy them. The sink determines the required alignment for the selected file and device
([`open(2)`][open2], [`statx(2)`][statx2]). Aligned dense runs may be submitted directly. Other
ranges use aligned assembly or buffered I/O, including a final unaligned tail. Direct and buffered
writes never cover overlapping file pages.

### Hyper transport boundary

Hyper exposes response payload as `Bytes` and does not currently provide a supported interface for
supplying reusable receive buffers ([Hyper buffer-pool discussion][hyper-buffer-pool]). The baseline
therefore copies decoded payload into reserved pooled storage. Transport-owned memory remains
outside pool accounting until that copy completes.

Zero-copy receive requires an upstream allocation and ownership seam before payload publication,
including H2 stream lifecycle and any TLS or completion-owned input. That transport design is future
work. The pool's unreserved acquisition and immutable-publication contracts provide the memory-side
primitives without defining the transport API.

**Obligations.**

- **I1: Scheduler handoff.** `poll_work()` dispatches only a granted, prepared reservation. A
  terminal future result is visible before wake, and a wake racing `Pending` schedules another poll.
- **I2: Cancellation ownership.** Cancellation closes direct-acquisition authority and releases
  transfer-manager-owned mutable buffers and clones unless their lifetime was transferred to active
  retry, delivery, I/O, or caller ownership. Published bytes remain valid.
- **I3: Staging and replay.** Reserved staging charges copied bytes before exposure. One
  incrementally filled upload part reuses one buffer, and replay retains every byte through its
  final consuming attempt while preserving exact length and order.
- **I4: Download ownership.** Reserved collection reuses one mutable stream per response, publishes
  only copied bytes, and releases foreign input after copy completion.
- **I5: Disk completion.** Submitted byte owners remain live through terminal completion. Short
  writes release exactly the completed prefix, and direct and buffered writes do not overlap file
  pages.

## Configuration and operations

Configured capacity controls admission. It does not preallocate the pool, limit mapped address
space, or bound process RSS. Carrier geometry controls internal storage. Reclamation timing controls
how long free prepared storage remains cached. Only configured capacity is a public resource policy.

### Capacity configuration

The public capacity policy has three forms:

```rust
#[non_exhaustive]
pub enum MemoryBudgetConfig {
    Auto,
    Fraction(f64),
    Limit(usize),
}

#[non_exhaustive]
pub enum BufferPoolBuildError {
    InvalidCapacity,
    MemoryDetectionUnavailable,
    CapacityOverflow,
    UnsupportedPageGeometry,
}

pub struct BufferPoolBuilder {
    memory_budget: MemoryBudgetConfig,
}

impl BufferPool {
    pub fn builder() -> BufferPoolBuilder;
    /// Returns the fixed writable allocation unit used by this pool.
    pub fn carrier_size(&self) -> usize;
    pub fn metrics(&self) -> MemoryMetrics;
}

impl BufferPoolBuilder {
    pub fn memory_budget(self, budget: MemoryBudgetConfig) -> Self;
    pub fn build(self) -> Result<BufferPool, BufferPoolBuildError>;
}
```

`MemoryBudgetConfig` is the only public automatic, fractional, or explicit byte policy. The
fallible pool builder is its only resolution boundary; pooled storage does not introduce a second
capacity enum or preserve a second clamping path. A transfer-manager client either constructs its
default pool with `Auto` or accepts an already validated explicit pool.

Pool construction resolves the policy after selecting carrier geometry. Effective memory is the
smaller of physical memory and a process or container memory limit where the platform exposes one.
Linux detection includes the effective cgroup hierarchy. Detection is a construction-time snapshot;
later changes to an external memory limit do not silently resize the pool.

`Auto` resolves as:

```text
effective_memory = min(physical_memory, process_memory_limit)

detected:
    auto_capacity = min(effective_memory / 4, 32 GiB)

not detected:
    auto_capacity = 2 GiB
```

The result is rounded down to a whole carrier count. A 2 GiB process therefore receives a 512 MiB
configured capacity. A 768 GiB process receives the 32 GiB ceiling. The pool prepares neither value
at construction.

`Fraction(f)` requires detected effective memory and finite `0 < f <= 1`. It resolves to
`effective_memory * f`, rounded down to whole carriers, without the `Auto` ceiling. `Limit(bytes)`
bypasses detection and rounds down to whole carriers. Construction rejects an invalid fraction, a
resolved value smaller than one carrier, arithmetic overflow, or a carrier count that cannot fit
the packed accounting representation. `MemoryDetectionUnavailable` applies only to `Fraction`;
`Auto` uses its documented fallback.

An invalid fraction or a resolved value below one carrier returns
`BufferPoolBuildError::InvalidCapacity`. Arithmetic overflow or a carrier count outside the packed
representation returns `CapacityOverflow`. `MemoryDetectionUnavailable` is unreachable for
`Limit` and `Auto`.

The packed coverage state stores each component in `u32`. With a 4 KiB carrier,
`u32::MAX` carriers represent almost 16 TiB. Configuration and every envelope conversion check this
limit before changing admission state. Byte conversion for public reporting is also checked.

Configured capacity is immutable for the lifetime of a pool. A caller that requires another policy
constructs another pool and directs new work to it. Existing reservations and byte owners remain
with their original pool until release.

**Alternative: Apply a fixed minimum capacity.** Rejected: a 512 MiB minimum consumes all detected
memory in a 512 MiB container and contradicts the required process headroom. Idle-only admission
already permits progress for an envelope larger than configured capacity.

The pool starts without prepared blocks. Construction establishes policy, geometry, accounting, and
maintenance state; admission or acquisition prepares the first block. `carrier_size()` exposes the
rounding unit for callers that align independent buffers or I/O fills. Other geometry is internal.

### Transfer-manager configuration

The transfer manager has one memory setting:

```rust
#[non_exhaustive]
pub enum MemoryConfig {
    Auto,
    Explicit(BufferPool),
}

impl config::Builder {
    pub fn memory(self, config: MemoryConfig) -> Self;
}

impl Client {
    pub fn metrics(&self) -> ClientMetrics;
}

impl ClientMetrics {
    pub fn memory(&self) -> &MemoryMetrics;
}
```

`MemoryConfig::Auto` is the default. It constructs a pool with `MemoryBudgetConfig::Auto`.
`MemoryConfig::Explicit` installs the supplied handle in the client. The caller can retain another
clone for a cache or another component:

```rust
let pool = BufferPool::builder()
    .memory_budget(MemoryBudgetConfig::Limit(8 * 1024 * 1024 * 1024))
    .build()?;

let config = Config::builder()
    .memory(MemoryConfig::Explicit(pool.clone()))
    // S3 client configuration omitted.
    .build();
let transfer_manager = Client::new(config);
```

Another component enters the same bounded admission domain through reservation:

```rust
let reservation = pool.reserve(requested_bytes).await?;
let buffer = pool.acquire(&reservation, writable_bytes)?;
```

There is no separate transfer-manager memory-budget setting and no pool-global close operation.
Every clone refers to the same admission, accounting, storage, maintenance, and metrics state.
`Client::metrics().memory()` exposes read-only metrics for an automatically constructed pool without
exposing a cloneable pool handle. An explicitly supplied pool exposes the same `MemoryMetrics`
contract through `BufferPool::metrics()`.

### Maintenance coordinator

Reclamation and cleanup recovery run on a lazy pool-owned thread rather than an async runtime
worker:

```rust
struct MaintenanceCoordinator {
    configured_capacity: CarrierCount,
    block_capacity: CarrierCount,
    control: Arc<MaintenanceControl>,
    diagnostics: Arc<MaintenanceDiagnostics>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct MaintenanceControl {
    state: Mutex<MaintenanceState>,
    wake: Condvar,
}

struct MaintenanceState {
    activity_epoch: u64,
    idle_deadline: Option<IdleDeadline>,
    reclaim: Option<ReclaimRequest>,
    cleanup: Option<CleanupRequest>,
    next_cleanup_generation: u64,
    stopping: bool,
    disabled: bool,
}

struct IdleDeadline {
    epoch: u64,
    expires_at: Instant,
}
```

The scheduler's global-idle transition and mapping cleanup failures signal the coordinator.
[Reclamation policy](#reclamation-policy) defines idle epochs, deadlines, retry, and trim
eligibility. [Mapping and revival](#mapping-and-revival) defines protection and discard recovery.
The coordinator executes those operations without changing their safety conditions.

The first deadline or cleanup request starts the thread. The worker owns
`Arc<MaintenanceControl>` and `Weak<PoolInner>`. It waits without retaining the pool. When work is
due, it releases the control mutex, upgrades the weak pool reference for one pass, and drops that
strong reference before waiting again. The control block contains no admission, arena, slot, or
mapping state.

The coordinator copies immutable configured and block capacity at construction, so worker startup
does not enter admission. Its worker mutex serializes concurrent first signals and lets final
destruction take the join handle exactly once. Diagnostics are atomic observations and do not
participate in policy or lifecycle decisions.

After releasing the temporary strong reference, the worker accesses no pool state. A thread-creation
failure disables maintenance and reports degraded reclamation. Admission, acquisition, and owner
return remain valid; free prepared capacity may remain resident until pool destruction.

Idle timeout and retry cadence are measured constants, not public controls.

### Shutdown and destruction

Transfer-manager shutdown drops its queued reservation futures, undispatched reservations,
in-flight mutable buffers, and pool handle through their normal cancellation paths. Existing
immutable byte owners remain valid and continue to return physical storage and accounting.

A shared pool remains operational while another caller owns a handle. Its FIFO, unreserved
acquisition, maintenance, and metrics are independent of transfer-manager shutdown.

`BufferPool`, `ReserveFuture`, `ReservationState`, and carrier guards retain `Arc<PoolInner>`. The
maintenance thread retains only `Weak<PoolInner>` while waiting. Final pool destruction therefore
begins only after no handle, waiter, reservation, mutable buffer, or immutable byte owner can access
pool storage.

Final destruction marks maintenance stopping and wakes the worker. A thread other than the worker
joins it before releasing pool state. If the worker's temporary upgrade is the final
`Arc<PoolInner>`, destruction runs on that worker and drops the join handle without joining itself.
The worker has completed its final pool access before releasing that reference. Destruction then
cancels maintenance work and releases each slot's stable virtual range without a scheduler or async
runtime.

### Failure containment

Recoverable resource failures remain local:

| Failure                                           | Result                                                                                |
| ------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Invalid capacity, geometry, or counter range      | Pool construction fails before publishing a handle                                    |
| Virtual-range reservation                         | That growth or acquisition attempt fails                                              |
| Ownership metadata reservation                    | That growth or acquisition attempt fails without exposing partial ownership            |
| Reservation preparation                           | That request fails; reusable capacity prepared before the failure remains available   |
| Reserved or unreserved acquisition                | The complete debit rolls back; no partial writable buffer escapes                     |
| Whole-range protection                            | The block enters its nonclaimable recovery state outside prepared capacity            |
| Backing discard                                   | The inactive block records `reclaim_pending`; other blocks remain usable              |
| Maintenance-thread creation                       | Maintenance is disabled; ordinary paths continue and affected blocks stay unavailable |
| Ownership, identity, or address-reservation check | The non-returning fail-stop handler aborts without continuing allocator mutation      |

The pool returns no unaccounted fallback memory after acquisition failure. Temporary growth remains
pool-owned and charged before writable access. A caller may allocate outside the pool, but that
memory is outside the pool's bound.

Anonymous mapping and protection success do not guarantee that every later first-touch fault will
succeed under operating-system overcommit or container pressure. Automatic sizing leaves
headroom, but the pool cannot convert a process-level out-of-memory kill into `AcquireError`.

### Observability

The public metrics surface reports stable operational concepts rather than ledger fields or block
lifecycle details:

```rust
#[derive(Debug, Clone)]
pub struct ClientMetrics {
    // Private representation.
}

#[derive(Debug, Clone)]
pub struct MemoryMetrics {
    // Private representation.
}

impl MemoryMetrics {
    /// Normal admission ceiling after policy and carrier rounding.
    pub fn configured_capacity_bytes(&self) -> u64;

    /// Active planned demand plus charges outside active demand.
    pub fn admission_used_bytes(&self) -> u64;

    /// Complete envelopes whose direct-acquisition authority remains open.
    pub fn active_planned_demand_bytes(&self) -> u64;

    /// Aggregate charges held by carrier owners or in-flight acquisition debits.
    pub fn charged_capacity_bytes(&self) -> u64;

    /// Admission use above the configured ceiling.
    pub fn admission_overage_bytes(&self) -> u64;

    /// Capacity whose mapping, placement, and registration are complete.
    ///
    /// Preparation rounds its current floor up to whole blocks and may exceed
    /// that floor by less than one block. Idle-only admission overage may also
    /// place prepared capacity above the normal configured ceiling.
    pub fn prepared_capacity_bytes(&self) -> u64;

    /// Reservation requests retained in FIFO order.
    pub fn queued_reservations(&self) -> usize;

    /// Cumulative reservation requests that entered the FIFO.
    pub fn parked_reservations_total(&self) -> u64;
}
```

The representation and fields remain private. Adding a signal adds a getter; callers cannot
construct or destructure metrics values. The API does not expose `available_coverage`,
`uncovered_charges`, bitmap population, block states, or scan hints. Those are implementation
details rather than durable user contracts.

`BufferPool::metrics()` takes admission serialization and loads the packed coverage state once.
Configured capacity, planned demand, charged capacity, overage, prepared capacity, and queue state
therefore form one coherent admission sample. It does not scan live bitmaps or add a carrier-return
counter to the common path. Values are carrier-rounded bytes and exclude foreign `Bytes`, protocol
scratch, copied contiguous output, and other process memory.

Prepared capacity follows whole-block geometry and the current admission floor. Block rounding may
raise it by less than one block beyond that floor. It can exceed configured capacity either through
that rounding or because an idle-only grant raised admission above the normal ceiling.

`parked_reservations_total` is copied from the admission state in the same sample. It increments
saturating exactly once when a request first enters the FIFO and never decrements on grant or
cancellation. It is monotonic for the lifetime of the pool and does not count an immediate grant.

`Client::metrics().memory()` returns the same `MemoryMetrics` sample. When a caller supplies a pool
shared with another client or component, those values describe the complete shared memory domain,
not only activity attributable to that client. `ClientMetrics` can add scheduling, connection,
retry, or runtime groups without replacing the client metrics entry point. Those groups may be
sampled independently and do not promise one atomic sample across subsystems.

A crate-private diagnostic snapshot includes raw accounting fields, bitmap population, block
lifecycle counts, pending cleanup, and scan-path counters for tests and tracing. It is not a public
compatibility surface.

Operational signals are organized by the failure they diagnose:

| Symptom                                     | Signals                                                                   | Interpretation                                                              |
| ------------------------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Managed work remains queued                 | configured, admission used, queue depth, parked reservations total        | Admission is binding or an older FIFO request is ineligible                 |
| Admission remains above configured capacity | admission overage, planned demand, charged capacity                       | Idle-only admission or ownership outside active planned demand remains live |
| Memory remains prepared after global idle   | prepared capacity, charged capacity, reclaim retries                      | `idle_retention_target`, live owners, or cleanup prevents reclamation       |
| Reuse repeatedly enters serialized fallback | serialized-acquisition count, prepared capacity, geometry                 | Optimistic scan work or placement is missing reusable capacity              |
| Capacity repeatedly grows and reclaims      | blocks prepared, blocks reclaimed, idle epochs                            | Idle timeout or retention target is causing cache churn                     |
| A block remains unavailable                 | protection-pending and reclaim-pending capacity, retry and failure counts | Platform cleanup is degraded but isolated to named blocks                   |
| Allocation or preparation fails             | operation error, requested capacity, prepared capacity, platform error    | The caller encountered a physical resource failure                          |

Normal reservation transitions are `trace`. Per-carrier successful acquisition and return
require no event. Reservation parking and serialized fallback are `trace` with monotonic diagnostic
counters. Block preparation, idle deadline changes, and successful reclamation are `debug`.
Allocation, protection, discard, maintenance-thread, and fail-stop precursor events are `warn` or
higher. Rare failure records include the operation, requested capacity, block identity where
applicable, platform error, and resulting pool state.

Metrics are pull-only, and event records are emitted by existing paths. The pool starts no
reporting task and maintains no rate window. Diagnostic counters saturate at `u64::MAX` and never
participate in control decisions.

**Obligations.**

- **C1: Construction.** Capacity and geometry validation complete before a pool handle is
  published.
- **C2: Maintenance isolation.** A stale idle epoch cannot reclaim. Maintenance waits without a
  strong pool reference, holds no control lock while entering pool or platform operations, and
  cannot invalidate ordinary operation when disabled.
- **C3: Shutdown scope.** Transfer-manager shutdown cancels only that manager's
  direct-acquisition authority. Dropping one pool handle cannot stop shared maintenance or
  invalidate another handle or byte owner. Final destruction never joins the maintenance worker
  from that worker.
- **C4: Metrics contract.** Public memory metrics keep their documented meaning independently of
  ledger representation. Parked-reservation metrics count each FIFO entry once and do not
  participate in admission.
- **C5: Failure reporting.** Rare resource failures and degraded reclamation produce an operation
  result or diagnostic signal.

## Correctness invariants

The local obligations in Architecture, Integration, and Configuration and operations constrain
individual mechanisms. The following properties constrain their composition across admission,
physical storage, ownership, integration, and shutdown.

### A live carrier is charged and prepared

At each public-operation completion and each admission or reclamation decision:

```text
physical_live
    <= outstanding_charges
    <= admission_used
    <= prepared_capacity
```

[Mutable buffer acquisition and growth](#mutable-buffer-acquisition-and-growth) installs the
complete aggregate debit before claiming storage.
Preparation reaches the post-transition `admission_used` before a grant completes, an uncovered
acquisition exposes writable memory, or admission serialization is released. Final owner return
clears the physical bit before releasing its charge. [Block trim](#block-trim) preserves the
admission floor before removing prepared capacity.

This prevents writable memory from escaping accounting, admission from relying on inaccessible
storage, and accounting release from admitting work before the corresponding carrier is reusable.
Configured capacity is absent from the chain because idle-only admission and uncovered acquisition
may exceed it.

### Admitted work does not wait for another owner

A grant prepares capacity through its resulting `admission_used`. Reserved and unreserved
acquisition follow the bounded search and serialized fallback defined under
[Carrier claiming and fallback](#carrier-claiming-and-fallback). They return the complete requested
carrier batch or an
error and do not wait for another carrier owner to return memory.

This prevents admitted work from occupying execution capacity while waiting for memory held by work
that cannot run. The guarantee excludes mutex scheduling and operating-system mapping, placement,
registration, or commit latency. Failure to obtain physical storage remains an explicit operation
error.

### Admission preserves order and idle progress

A queued reservation request cannot be bypassed by a fresh request or a later waiter. Capacity
released for the FIFO is transferred directly into the head waiter's terminal result before its
waker runs. The waiter lifecycle and slot-lock interleaving under
[Wait queue](#wait-queue) establish that cancellation produces either one caller-owned grant or one
retired grant, never both.

When no reservation retains active planned demand, the FIFO head may receive an idle-only grant
even when its envelope exceeds configured capacity or uncovered charges prevent a normal grant.
Every admitted envelope is nonzero, so that grant makes active planned demand nonzero and a second
idle-only grant cannot compound the overshoot. Strict no-bypass ordering ensures active planned
demand can fall to zero while an oversized head waits. Progress requires earlier reservations to
close and physical preparation to succeed; FIFO order does not impose a time bound on either.

### Reservation close does not release owned bytes

[Close and return](#close-and-return) consumes the public reservation handle, revokes acquisition of
new carriers through its private state, and removes its complete envelope from active planned
demand. Existing mutable buffers retain initialized bytes and writable tails. A tail-only reserve
remains valid; a reserve that needs another carrier fails unless its debit linearized before close.

Charges held by mutable buffers, immutable views, in-flight I/O, and in-flight acquisition debits
remain accounted. Close reclassifies the occupied portion of the envelope as
`uncovered_charges`, which remain until the charges return. A later grant adds a new envelope
without absorbing them.

This separates planned work lifetime from byte lifetime. Transfer completion, retry completion, and
reservation close cannot invalidate data retained by a caller, transport, or I/O operation.

### Published bytes never alias writable memory

Each carrier has one non-cloneable mutable authority.
[Immutable Bytes publication](#immutable-bytes-publication) consumes authority over an initialized
prefix and
may retain authority only over a disjoint suffix. Immutable views, their clones, and segmented
containers retain the carrier guard but cannot recover mutable access to pool storage.

Only initialized ranges become `Bytes`, `&[u8]`, `IoSlice`, or outgoing I/O sources. Every pointer
used by such a range remains within one stable block range and is retained by an owner that prevents
carrier return. This prevents mutable aliasing, uninitialized reads, and use after reclamation
across every delivery form.

### Reclamation cannot overtake ownership

A valid set bit has exactly one `ProvisionalBits` or `CarrierAllocation` owner. The
[claim-trim gate](#claim-trim-gate)
ensures that a claim either keeps its bit while trim abandons or rolls back through its protected
incarnation while trim proceeds. Revival creates a fresh bitmap, so stale rollback cannot mutate a
new activation.

Trim removes only an all-free block, preserves the admission floor, makes the complete stable range
inaccessible, and retains exclusive address ownership. Protection or discard failure leaves the
affected block nonclaimable; it does not weaken the ownership of another block or carrier.

### Release survives shutdown

[Shutdown and destruction](#shutdown-and-destruction) drops only manager-owned futures,
reservations, buffers, and pool handles. It does not revoke another handle's reservation or pooled
byte ownership. Carrier guards and byte owners retain `PoolInner` and complete rollback or final
return after every transfer-manager handle has dropped. Maintenance and admission remain available
while another public handle or waiter exists.

Recoverable allocation and cleanup failures expose no partially owned writable buffer. A detected
ownership, incarnation, or stable-address violation enters the non-returning fail-stop path before
clearing an unexpected bit or releasing its charge.

## Open Questions

### Carrier and block geometry

Carrier and block sizes are measurement choices. Carrier size must be a multiple of the runtime page
size, and block size must be a whole number of carriers. Smaller carriers reduce small-object and
tail waste but increase bitmap operations, ownership transitions, scatter width, and carrier
returns.

The initial implementation uses a 16 KiB carrier before runtime page-size alignment and a 128 MiB
target block. Pools configured below 128 MiB use their complete carrier-rounded capacity as one
block. Block size is configured in bytes; carrier count and bitmap width are derived geometry, not
policy inputs.

Block size controls mapping amortization, all-free scan length, reclaim granularity, and how much
capacity one long-lived carrier can keep prepared. Geometry selection must account for small
objects, large multipart transfers, mixed upload and download traffic, intended core counts, and
aggregate rates on target hardware.

These choices do not change the stable-slot, fresh-incarnation, batched-fallback, or ownership
contracts.

### Scan and reclamation constants

The optimistic scan budget, idle timeout, and cleanup retry cadence are selected by measurement.
The initial scan target covers 32 MiB at any carrier size, rounded up to a complete bitmap word. At
the default geometry an 8 MiB part therefore occupies one quarter of the scan window rather than
requiring every observed bit to be free. The scan budget trades lock-free work against entry into
serialized fallback. Because fallback holds admission serialization during its registry-wide scan,
the budget also controls exposure to registry-sized admission stalls. Registry size bounds fallback
scan work. The idle timeout trades retained RSS against repeated preparation across traffic bursts.
Retry cadence trades cleanup latency against repeated platform calls while a block remains
unavailable.

Measurements must include mixed object sizes, burst and sustained traffic, idle gaps, allocation
failure, high core counts, and aggregate rates on target hardware. These constants do not change
the exhaustive serialized fallback, idle-epoch invalidation, or fail-closed cleanup contracts.

The baseline reclaims after scheduler-global idle. Sustained low demand that never reaches global
idle may therefore retain peak prepared capacity. A later policy may arm reclamation when
reservation demand remains below its recent peak without changing the worker, admission floor, or
trim gate. That trigger belongs at scheduler or reservation frequency rather than on every carrier
return.

## Future Work

### Asynchronous preparation

Eager page placement, memory locking, and device registration can make synchronous preparation too
expensive for reservation or acquisition. A preparer may move those operations to a dedicated
actor while retaining the same transition: a grant is published only after compatible capacity
reaches `prepared_capacity`.

Acquisition still cannot wait for another carrier owner. Any wait introduced by asynchronous
preparation must depend only on the preparer's progress and must terminate with prepared capacity
or an explicit preparation error.

### Registered and topology-specific storage

A future block compatibility key could identify fixed-buffer registration, provided-buffer
ownership, NUMA placement, or another hard storage domain. Registration and completion ownership
are hard constraints; NUMA locality remains a preference unless an integration requires otherwise.

A hard domain can have free capacity while the FIFO head requires another domain. Adding such
domains requires an explicit admission policy: preserve one global FIFO and permit incompatible
capacity to idle, or partition admission and define fairness across partitions. Static direction
quotas remain unnecessary.

### Pool-backed transport receive

A future transport receive-buffer interface may acquire unreserved pooled storage before publishing
payload bytes. A richer contract may also carry direct-acquisition authority after request or stream
attribution, allowing attributable payload to use `acquire` instead of `acquire_unreserved`.
Protocol scratch, connection state, and reads selected before attribution remain outside that
authority.

The accounting model does not depend on either extension. Both acquisition paths consume aggregate
coverage, and no transport integration can recover accounting ownership from an already published
byte view.

### TLS and completion-owned receive

A completion-native network path selects or transfers buffer ownership before reporting received
bytes. TLS may retain and mutate encrypted input before publishing disjoint plaintext. Supporting
that path requires an owned-input contract through TLS and a pre-received-data or completion-owned
HTTP transport seam.

Stable carrier addresses, initialized-range publication, and future compatibility keys preserve the
required ownership model. Readiness-oriented receive allocation and completion-owned receive require
distinct transport ownership contracts.

### External pressure

An operating-system or container pressure signal may arm reclamation before scheduler-global idle.
Such a signal changes when reclamation is requested, not block eligibility, the admission floor,
the idle retention target, or claim-trim safety. A pressure producer must remain optional because
portable process-level pressure notification is not available on every target.

### Shared-domain fairness and attribution

A shared pool has one FIFO, configured capacity, and metrics domain. It provides no per-component
latency isolation or attribution. Adding quotas, weighted fairness, or participant metrics requires
component identity and policy inside that shared domain; independent accounting over one arena would
not preserve a combined memory bound.

## Appendix A: Admission transitions

This appendix is the state-transition reference for admission and aggregate accounting. All
quantities count carriers:

```text
admission_used =
    active_planned_demand
  + uncovered_charges

outstanding_charges =
    active_planned_demand
  - available_coverage
  + uncovered_charges
```

`available_coverage` and `uncovered_charges` are one packed atomic state. Transitions that change
active planned demand, add uncovered charges, change prepared capacity, or modify the FIFO hold
admission serialization. Every debit and return linearizes on the packed state. A fully covered
debit and a coverage-only return need no further serialization. A return that removes uncovered
charges enters admission serialization after its packed transition to reconsider the FIFO.

### Reservation transitions

| Transition          | Preconditions                                                            | State change                                                                                           | Follow-up                                      |
| ------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------- |
| Normal grant        | FIFO empty or request is head; `admission_used + envelope <= configured` | Prepare through the resulting admission floor; add `envelope` to planned demand and available coverage | Publish one open `Reservation`                 |
| Idle-only grant     | FIFO empty or request is head; `active_planned_demand == 0`              | Same as normal grant                                                                                   | Publish one open `Reservation`                 |
| Queue               | Request is not immediately eligible                                      | Append one `Queued` waiter; accounting is unchanged                                                    | Retain waker and cancellation state            |
| Grant queued head   | Head slot is `Queued` and request is eligible                            | Hold slot through preparation and grant; replace `Queued` with `Granted`                               | Unlink; wake after admission unlock            |
| Preparation failure | Head is selected but post-grant preparation fails                        | Planned demand and coverage are unchanged; capacity prepared before failure remains prepared           | Store `Failed`, continue FIFO drain, then wake |
| Cancel queued       | Wait slot remains `Queued`                                               | Remove the waiter; accounting is unchanged                                                             | Reconsider the new head                        |
| Cancel granted      | Wait slot contains an untaken `Reservation`                              | Consume and close that reservation exactly once                                                        | Reconsider the FIFO                            |

A grant of envelope `E` applies:

```text
prepare_to(admission_used + E)

active_planned_demand += E
available_coverage += E
```

Existing `uncovered_charges` do not change.

### Acquisition transitions

For an acquisition of `N` carriers:

```text
covered = min(N, available_coverage)

available_coverage -= covered
uncovered_charges += N - covered
```

A direct initial acquisition or reserved-buffer growth first publishes the aggregate debit, then
atomically reserves `N` units of direct-acquisition authority. It rejects the request when
`direct_outstanding + N > envelope` or the owner state is `CLOSED`, and rolls back the aggregate
debit before returning that error. Consuming the public `Reservation` prevents another initial
acquisition after close. Existing buffers retain private state, so close races growth through the
packed reservation owner state: either the direct debit precedes close, with its aggregate charge
already published, or growth observes `CLOSED` and reverses that charge. An unreserved acquisition
has no direct-acquisition-authority transition. If `N - covered` is nonzero, the acquisition
publishes the complete charge and prepares to the new `admission_used` while holding admission,
then releases admission before the direct debit and optimistic physical claim. The published charge
keeps the prepared-capacity floor in force across that unlocked window. Both covered and shortfall
acquisitions acquire a fresh guard only if optimistic physical claim misses.

An advisory direct-state load may reject an already closed or exhausted reservation before the
aggregate debit. It does not authorize acquisition and cannot replace the post-aggregate atomic
debit.

Mutable-buffer growth computes `N` from only the shortfall below the requested writable capacity:

```text
shortfall = min_writable.saturating_sub(remaining_mut)
N = ceil(shortfall / carrier_size)
```

`N == 0` changes no accounting and remains valid after direct-acquisition authority closes. For
`N > 0`, the complete acquired run set is appended atomically or the buffer remains unchanged.

The aggregate debit owns all `N` charges until each charge moves into one carrier guard. Physical
claim is all-or-error at the API boundary. Failure returns completed physical carriers, rolls back
provisional bits, releases every untransferred charge, and restores direct-acquisition authority
before returning an error.

Aggregate rollback and final return use the same accounting transition for count `N`:

```text
repaid = min(N, uncovered_charges)

uncovered_charges -= repaid
available_coverage += N - repaid
```

The transition requires `N <= outstanding_charges`. Acquisition debit ownership and one charge per
`CarrierGuard` establish that precondition. Physical carriers are returned before a final-return
transition. A direct return also decrements `direct_outstanding`; it restores direct-acquisition
authority only while the reservation remains open.

A final return with `repaid > 0` enters admission serialization after the packed transition and
reconsiders the FIFO. After admission has been released for physical claim, acquisition rollback
follows the same rule after returning its physical bits: if its packed inverse removes an uncovered
charge, it enters admission serialization and drains the FIFO. Preparation failure reverses its
charge while the initial guard remains held and requires no separate drain. Serialized fallback
never acquires or reacquires admission internally; after an optimistic miss, its caller supplies one
held guard before entering arena state.

### Reservation close

Closing an envelope `E` consumes its public direct-acquisition authority exactly once:

```text
D = direct_outstanding observed by the close transition
potentially_unused = E - D
remaining_active = active_planned_demand - E

nominally_unused = min(potentially_unused, available_coverage)
required_for_active = available_coverage.saturating_sub(remaining_active)
unused_removed = max(nominally_unused, required_for_active)
reclassified = E - unused_removed

active_planned_demand = remaining_active
available_coverage -= unused_removed
uncovered_charges += reclassified
```

Without a concurrent direct return, `reclassified` is at least `D`. If return lowers direct
outstanding after close observes `D` and restores aggregate coverage before the close CAS,
`required_for_active` removes that newly unused coverage. Close creates no owner and removes no
charge. It cannot withdraw available coverage needed by another open reservation's direct
authority. Close racing final return produces the same state in every interleaving because each
operation changes available coverage and uncovered charges as one packed transition.

### Prepared-capacity transitions

| Transition              | Preconditions                                                       | Ordering                                                                                              |
| ----------------------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Prepare or revive block | Stable range retained; target preparation succeeds                  | Establish writable mapping, add block to `prepared_capacity`, then publish fresh `Active` incarnation |
| Failed preparation      | Any target operation fails                                          | Publish no `Active`; enter `ActivationRecoveryPending`; retain earlier successful preparation         |
| Abandon trim            | Claim-trim gate observes any valid set bit                          | Restore `Active`; leave `prepared_capacity` unchanged                                                 |
| Confirm trim            | Gate confirms all valid bits clear and admission floor is preserved | Subtract block from `prepared_capacity`, then perform physical cleanup                                |
| Failed deactivation     | Whole-range inaccessible transition fails                           | Keep block nonclaimable in `DeactivationRecoveryPending` outside prepared capacity                    |
| Failed discard          | Range is inaccessible but backing discard fails                     | Keep block inactive with `reclaim_pending`; prepared capacity is unchanged                            |

The admission floor is checked against the post-removal count:

```text
prepared_capacity_after >= admission_used
```

Failed preparation never adds capacity. Failed deactivation occurs after this checked subtraction
and does not subtract the block again. Recovery adds capacity before making the block claimable, so
all mapping-failure transitions preserve the admission floor.

## Appendix B: Memory ordering

The following orderings are minimum requirements. A stronger ordering is correct but adds no new
invariant. Mutex and ArcSwap operations use their synchronization contracts rather than duplicate
atomics around protected data.

### Lock order

No path holds a later lock while acquiring an earlier lock:

```text
AdmissionState
    +-- WaitSlot
    +-- ArenaState
    `-- BlockSlot mapping/lifecycle

ArenaState
    `-- BlockSlot mapping/lifecycle
```

Serialized fallback receives a held `AdmissionGuard`, then acquires `ArenaState`, and holds both
through exhaustive scan and growth. Covered and shortfall paths acquire that guard after their
optimistic physical miss. Fallback never acquires admission internally and never acquires
`AdmissionState` while holding `ArenaState`. Its caller releases `AdmissionGuard` before finishing
or dropping `PendingAcquisition`.

`ReserveFuture` cancellation releases `WaitSlot` before entering `AdmissionState`. Carrier return
clears physical ownership and releases any block protection before entering admission accounting.
Maintenance releases `MaintenanceState` before entering admission, arena, block, or platform
operations. Registered wakers run with none of these locks held.

### Claim-trim ordering

The gate requires one sequentially consistent order over its store-load pairs:

| Side  | First access                                | Second access                              |
| ----- | ------------------------------------------- | ------------------------------------------ |
| Claim | Set candidate bits with `fetch_or(SeqCst)`  | Load incarnation state with `SeqCst`       |
| Trim  | Change `Active` to `Draining` with `SeqCst` | Load every valid bitmap word with `SeqCst` |

Acquire/release ordering is insufficient because each load may otherwise observe the state before
the other side's store. A full `SeqCst` fence between each store and load is equivalent when the
implementation preserves one global order over the four accesses.

Gate-failure rollback clears only bits won in the protected old incarnation. Post-gate rollback and
final return clear only recorded live bits in the current incarnation. Those clears require
`Release` or stronger ordering; they do not form another store-load gate. A bitmap scan used only
as a search hint may use `Relaxed`; ownership is decided by the gated `fetch_or`.

### Publication and accounting

| State                                 | Operation                                     | Minimum ordering or synchronization                      |
| ------------------------------------- | --------------------------------------------- | -------------------------------------------------------- |
| Packed coverage and uncovered charges | Load or failed CAS retry                      | `Acquire`                                                |
| Packed coverage and uncovered charges | Successful debit, grant, close, or return CAS | `AcqRel`                                                 |
| Reservation owner state               | Direct debit, close, rollback, or return      | One packed `AcqRel` read-modify-write                    |
| Block registry                        | Publish or load immutable registry generation | ArcSwap publication and guard contract                   |
| Current block incarnation             | Publish, replace, or protect incarnation      | ArcSwap publication and guard contract                   |
| Wait result                           | Store terminal result and consume it          | `WaitSlot` mutex                                         |
| Admission ledger and FIFO             | Every read and write                          | `AdmissionState` mutex                                   |
| Mapping and cleanup state             | Every transition                              | Block mapping/lifecycle mutex                            |
| Maintenance epoch and deadlines       | Every read and write                          | `MaintenanceState` mutex and condition-variable protocol |

The packed accounting CAS that releases a charge is ordered after physical bitmap return. A grant
or metrics sample that observes the accounting release therefore cannot precede the physical return
that made the carrier reusable.

Reserved acquisition publishes its packed aggregate debit before its reservation-local direct
debit. Close reads the direct count from the same atomic transition that sets `CLOSED`; a direct
debit that loses this race reverses its already-published aggregate charge. A successful direct
debit therefore names a published charge until its corresponding return retires it.

Prepared-capacity accounting precedes `Active` publication. Deactivation follows `Draining`,
all-free confirmation, and prepared-capacity removal. Immutable byte publication requires no
additional allocator fence: consuming exclusive mutable authority and transferring initialized
ranges into synchronized owners establishes the Rust ownership boundary.

## Appendix C: Safety and platform obligations

Every unsafe operation must name the owner that keeps its address valid, the range that is
initialized, and the synchronization that excludes mutation or reclamation.

### Unsafe surface

| Operation                               | Required precondition                                                                                                     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Reserve a virtual block range           | `VirtualRange` exclusively owns the complete page-aligned range until final slot destruction                              |
| Compute a carrier address               | Checked arithmetic stays within its `VirtualRange`; the claim passed the `Active` gate                                    |
| Expose a writable range                 | One linear `ExclusiveRange` owns the complete range; no immutable view overlaps it                                        |
| Advance initialized length              | A completed write initialized every advanced byte and did not exceed the exposed writable ranges                          |
| Construct `PooledWindow` or `Bytes`     | The range is initialized, immutable, within one live carrier, and retained by its carrier guard                           |
| Construct `ContiguousOwner`             | Ordered owners cover every byte in the contiguous range for the complete owner lifetime                                   |
| Form `&[u8]`, `IoSlice`, or I/O source  | The referenced range is initialized and retained for the complete borrow or asynchronous operation                        |
| Implement `Send` or `Sync`              | Moving or sharing the type cannot duplicate mutable authority; every shared pointer names immutable synchronized storage  |
| Coalesce adjacent presentation ranges   | Both pointers derive from the same stable slot, are adjacent, and retained owners cover the merged range                 |
| Clear bits after gate failure           | The protected incarnation and bitmap remain allocated outside the block's inaccessible `VirtualRange`                     |
| Make a block inaccessible or discard it | The claim-trim gate confirmed all-free, prepared accounting no longer includes it, and the slot retains address ownership |

`VirtualRange` is the only unsafe `Send + Sync` implementation in the pool ownership spine.
`BlockSlot`, `Arena`, and `PoolInner` inherit those traits by composition. Its private raw pointer
cannot create a reference without the checked range, mapping-state, and claim-gate preconditions in
the table.

Incarnation state and occupancy bitmaps are separately allocated metadata, not storage inside
`VirtualRange`. Gate-failure rollback may therefore clear the protected old bitmap after trim has
made the block range inaccessible.

Pointer identity is diagnostic. It never authorizes a bitmap write, pointer construction, or
return. A failed identity or ownership check aborts before allocator mutation continues.

`MaybeUninit<u8>` remains the storage type after initial preparation and revival. Platform zero-fill
does not establish Rust initialization. Only completed writes and explicit initialized-length
transitions permit safe references or immutable publication.

### Owner lifetime

One carrier guard owns one physical bit and one charge. Every writable suffix, published
`PooledWindow`, contiguous owner, segmented hold, and asynchronous I/O submission retains that
guard directly or through another owner. The final guard drop is the only path that clears the bit
and releases the charge.

`Bytes` clones and slices retain their original owner through the `bytes` owner vtable. They do not
create another carrier guard or accounting charge. Incoming views remain opaque owners; address
lookup can establish presentation adjacency but cannot recover physical-return authority.

### Platform contract

| Target  | Stable reservation                         | Prepare                                    | Deactivate                   | Discard                 |
| ------- | ------------------------------------------ | ------------------------------------------ | ---------------------------- | ----------------------- |
| Linux   | Anonymous private `PROT_NONE` mapping      | Whole-range `mprotect(READ \| WRITE)`      | Whole-range `mprotect(NONE)` | `MADV_DONTNEED`         |
| macOS   | Anonymous private `PROT_NONE` mapping      | Whole-range `mprotect(READ \| WRITE)`      | Whole-range `mprotect(NONE)` | `MADV_FREE`             |
| Windows | `VirtualAlloc(MEM_RESERVE, PAGE_NOACCESS)` | `VirtualAlloc(MEM_COMMIT, PAGE_READWRITE)` | `VirtualFree(MEM_DECOMMIT)`  | Same decommit operation |

All targets must preserve exclusive ownership of the virtual range after every successful or failed
operation. Failed initial preparation or commit enters `ActivationRecoveryPending`; failed
deactivation enters `DeactivationRecoveryPending`. No prior protection is assumed. Recovery
requires a later successful whole-range transition before the block can reenter prepared capacity.

Windows preparation consumes system commit capacity for each newly required whole block. Starting
without prepared capacity, a grant commits enough blocks to cover its complete post-grant admission
floor and may fail before the grant is published. Linux and macOS writable protection does not
provide the same commitment to later residency.

Discard runs only after the range is inaccessible. Failure may retain backing but cannot make the
block claimable. Retry and preparation serialize so a delayed discard cannot affect a revived
block. Registered, wired, mixed-policy, or completion-owned blocks remain trim-ineligible until
their capability owner completes teardown.

The Linux operations follow [mmap(2)][mmap2], [mprotect(2)][mprotect2], and
[madvise(2)][madvise2]. Windows reservation, commit, and decommit follow
[VirtualAlloc][virtual-alloc] and [VirtualFree][virtual-free].

Qualification for each supported target includes runtime page and allocation geometry, stable
address ownership, successful prepare/deactivate/revive, commit or overcommit failure, discard
failure, and process destruction with escaped owners already absent. RSS or working-set reduction
is an operational observation, not proof of address ownership or inaccessibility.

## Appendix D: Verification

Verification targets the mechanism that establishes each property. Model checking covers bounded
concurrent state machines. Miri covers raw-pointer and initialization rules. Property and
failure-injection tests cover arithmetic, ownership, rollback, and platform result handling.
The `Obligation` column maps each property to the named contract in the corresponding mechanism
section; one property may discharge several contracts.

### Admission verification

| Obligation | Property                                                                          | Evidence                                               | Negative control                                                  |
| ---------- | --------------------------------------------------------------------------------- | ------------------------------------------------------ | ----------------------------------------------------------------- |
| A1, A5     | Debit, close, and return preserve aggregate charges in every order                | Loom over packed state and reservation close           | Split coverage and uncovered charges into independent atomics     |
| A5         | A grant never absorbs an existing uncovered charge                                | Transition property test with repeated grant and close | Recompute uncovered charges from the new envelope                 |
| A7         | Grant, cancellation, and poll produce one terminal waiter result                  | Loom over FIFO and wait slot                           | Release the slot during preparation and publish after `Taken`     |
| A7         | Waker reentry observes the terminal result without lock nesting                   | Loom with waker reentry                                | Invoke the waker before publication or while admission is locked  |
| A7         | Idle-only admission grants at most one request at a time                          | State-machine property test                            | Gate idle escape on configured headroom instead of planned demand |
| A6         | Uncovered-charge return cannot strand an eligible waiter                          | Loom over packed return, enqueue, and FIFO drain       | Skip admission drain after repaying an uncovered charge           |
| A2         | Published shortfall preserves the floor during unlocked claim                     | Composed Loom over debit, trim, claim, and rollback    | Unlock before charge publication or floor preparation             |
| A3, A6     | Post-unlock shortfall rollback cannot strand an eligible waiter                   | Loom over rollback, enqueue, and FIFO drain            | Skip admission drain after rollback repays an uncovered charge    |
| A6         | Physical return precedes a newly eligible waiter's acquisition                    | Composed pool-level Loom model                         | Release accounting before clearing the physical bit               |
| A3         | Partial acquisition failure restores every debit and direct-acquisition authority | Failure injection after each claim and conversion      | Drop the debit before provisional and completed carriers          |
| A4         | Close racing buffer growth has one complete outcome                               | Loom over authority, debit, rollback, and close        | Split `CLOSED` from the direct-authority debit                    |
| A4         | Concurrent acquisitions consume direct-acquisition authority exactly once         | Loom over packed reservation owner state               | Load and store authority without compare-and-exchange             |
| A4, A5     | Close preserves another open reservation's coverage and every surviving charge     | Multi-reservation regression and composed Loom         | Derive unused coverage only from the global available lane        |
| A1         | Count conversion and packed lanes never overflow                                  | Boundary property tests over byte and carrier counts   | Remove one checked conversion                                     |

### Physical-storage verification

| Obligation | Property                                                                  | Evidence                                                  | Negative control                                               |
| ---------- | ------------------------------------------------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------- |
| P4         | Claim and trim cannot both pass their gate                                | Loom over one claim and one trim                          | Weaken either store-load pair to acquire/release               |
| P3, P4     | Stale rollback cannot clear a revived carrier                             | Loom over claim, trim, revival, and new claim             | Reset and reuse one bitmap across activations                  |
| P3, P4, P7 | Protection recovery preserves ownership in the retained incarnation       | Loom over claim, failed deactivation, recovery, and claim | Reset the retained bitmap during protection recovery           |
| P4         | Gate-failure rollback writes only through the protected incarnation       | Loom with removal of `current` before rollback            | Reacquire `current` for stale rollback                         |
| P2, P5     | Post-gate batch rollback and final return clear exactly owned bits        | Loom plus multiword property tests                        | Clear the candidate mask instead of won bits                   |
| P1         | Padding bits never become carriers or affect all-free                     | Property tests around every final-word width              | Ignore `final_word_mask` during claim or trim                  |
| P1, P2     | Pool ownership spine is `Send + Sync` through `VirtualRange`              | Compile-time trait assertions and unsafe-code inspection  | Store an unwrapped `NonNull` directly in `BlockSlot`           |
| P1, P2     | Address lookup classifies only complete in-range views                    | Boundary property tests over sorted block ranges          | Classify by start address without checking the end             |
| P6         | Fallback lock scope excludes pending finish and rollback                  | Deterministic miss-path tests plus lock assertions        | Finish or drop pending while admission remains held            |
| P4         | Gate-failure rollback remains valid after range deactivation              | Loom over claim, trim, and delayed rollback               | Allocate incarnation metadata inside `VirtualRange`            |
| P6         | Serialized fallback exhausts prepared capacity before growth              | Deterministic fragmented-registry tests                   | Grow after an optimistic miss without exhaustive recheck       |
| P6         | Fresh growth reserves the fallback claimant's carriers before publication | Concurrent fallback and fast-claim test                   | Publish the free incarnation before preclaiming the batch      |
| P7         | Protection and discard failure leave the block nonclaimable               | Failure injection at each mapping transition              | Restore `Active` from a failed platform call                   |
| P3, P7     | Mapping failures preserve the admission floor                             | Failure injection across prepare, trim, and recovery      | Count failed preparation or subtract failed deactivation twice |
| P8         | Idle retention does not decay within one idle epoch                       | Maintenance state-machine test over repeated scans        | Derive each retry target from current prepared capacity        |
| P9         | Corrupt ownership state aborts before allocator mutation continues        | Fail-stop injection at each ownership check               | Clear a bit or release accounting after a failed check         |
| A8, C3     | Shared-pool shutdown preserves other handles and final return             | Composed Loom with queue, reservation, and `Bytes`        | Drop pool state when manager-owned handles disappear           |

### Ownership verification

| Obligation | Property                                                              | Evidence                                               | Negative control                                        |
| ---------- | --------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------- |
| O4, O5     | Only initialized bytes become safe references or immutable owners     | Miri over partial writes, publication, and freeze      | Freeze acquired capacity instead of initialized length  |
| O2, O5     | Mutable publication leaves a disjoint writable suffix                 | Miri over repeated `publish_prefix` and writes         | Retain mutable authority over the published prefix      |
| O3         | Incremental growth uses the existing tail before rounding a shortfall | Property tests over carrier geometry and fill sizes    | Round every `reserve` request independently             |
| O3         | Failed growth preserves bytes, cursors, and writable capacity         | Failure injection and Miri at every growth commit step | Append runs before the complete acquisition commits     |
| O1, O6     | One carrier returns once after its final view, slice, and suffix drop | Guard-level Loom and arbitrary drop-order tests        | Create one return guard per view or per run             |
| O7         | Segment owners cover every unconsumed byte                            | Property tests over builder input and cursor movement  | Merge unknown or nonadjacent ranges                     |
| O7, P2     | Classified views use concrete slot identity and slot-rooted provenance | Strict-provenance Miri over same-slot, cross-carrier, foreign, and cross-pool views | Extend `Bytes::as_ptr()` or classify by pool-local slot index |
| O8         | `Buf` never reports an empty chunk while bytes remain                 | Generic-consumer property test                         | Leave the cursor on an exhausted segment boundary       |
| O4         | Mutable and publication cursors normalize across carriers             | Generic `BufMut` and repeated-publication tests        | Leave either cursor on an exhausted carrier boundary    |
| O4, O5     | Publication never crosses `initialized_chunk`                         | Miri and boundary tests over every carrier split       | Bound publication by total initialized length           |
| O8         | Vectored reads respect destination capacity                           | Property tests over destination lengths and segments   | Write one entry for every remaining segment             |
| O6, O8     | Advancing one clone cannot release another clone's backing            | Clone and partial-consumption property test            | Share cursor state without sharing owner holds          |
| O9         | Contiguous conversion preserves bytes and ownership semantics         | Shape and byte-equivalence property tests              | Release owners before constructing the zero-copy result |
| O9         | Copied sources remain owned through their final read                  | Miri over multi-segment contiguous conversion          | Release a source hold before copying its bytes          |
| O8, I5     | Partial vectored I/O advances exactly the completed prefix            | Miri and property tests over every completion boundary | Advance by submitted rather than completed length       |

### Integration and operations verification

| Obligation | Property                                                               | Evidence                                                                  | Negative control                                           |
| ---------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------- |
| I1         | `poll_work()` dispatches only a granted, prepared reservation          | Scheduler integration test with parking and cancellation                  | Dispatch while the reservation future is pending           |
| I1         | Wake racing `poll_work()` pending cannot strand a granted reservation  | Scheduler state-machine test across registration and claim release        | Ignore a wake recorded while the transfer is claimed       |
| I2         | Cancellation releases manager holds without invalidating escaped bytes | Integration tests across queued, dispatched, and published states         | Release published bytes or retain unpublished buffers      |
| I3         | Upload retry retains exact bytes through the final consuming attempt   | Retry tests with partial body polling and source failure                  | Release staged bytes before the last retry                 |
| I3         | Upload parts reuse one mutable stream across source reads              | Multipart tests with carrier-misaligned read completions                  | Allocate one buffer for every source read                  |
| I3, I4     | Reserved staging preserves bytes and releases foreign input after copy | Download tests with frame and carrier boundary mismatch                   | Publish before copy completion or retain foreign input     |
| I4, I5     | Download and disk completion retain owners through final access        | Integration tests with partial reads, writes, and cancellation            | Drop byte owners immediately after submission              |
| I4         | A download response reuses its suffix across response frames           | Collector tests spanning carrier and publication boundaries               | Acquire a new buffer for every response frame              |
| C2         | A stale idle epoch cannot reclaim after new managed work               | Maintenance state-machine test with deadline races                        | Accept an expired epoch after activity                     |
| C2         | Cleanup retry cannot race preparation or revived access                | Concurrency test with injected protection and discard failures            | Run discard without the slot mapping lock                  |
| C1         | Capacity detection honors process and container limits                 | Platform tests for physical memory, cgroup limits, and explicit overrides | Ignore the effective process or container limit            |
| P3         | Commit-accounting targets prepare through the floor before grant       | Native grant test with injected commit exhaustion                         | Publish a grant before whole-block commit                  |
| P7         | Supported mapping backends preserve the platform contract              | Native probe matrix for Linux, macOS, and Windows                         | Treat a failed protection or discard call as success       |
| C2, C5     | Maintenance failure leaves ordinary operation valid and is reported    | Thread-start and cleanup-failure injection                                | Disable maintenance without recording degraded reclamation |
| C3         | Manager shutdown preserves externally owned pool state                 | Integration test with shared pool, reservation, and escaped bytes         | Close the shared pool during manager shutdown              |
| C2, C3     | Worker wait retains no pool owner; final upgrade may own the pool      | Deterministic wait and final-owner lifetime tests                         | Wait with pool `Arc` or join the worker from itself        |
| C4         | Public memory metrics report one coherent admission sample             | Concurrent metrics-sampling property test                                 | Load accounting fields in independent lock intervals       |
| C4         | Parked-reservation total counts each FIFO entry once                   | Immediate, queued, cancelled, and granted reservation tests               | Increment on every pending poll                            |
| C5         | Resource failures produce an operation result or diagnostic signal     | Failure injection at each recoverable resource boundary                   | Suppress both the operation error and diagnostic event     |

Benchmarks select carrier size, block size, optimistic scan work, idle timing, and retry cadence.
They cover small-object waste, scatter width, allocation churn, packed-state contention,
fragmented reuse, idle burst recovery, high core counts, and aggregate rates on target hardware.
Benchmark results tune constants; they do not weaken ownership, accounting, fallback, or
reclamation invariants.

[hyper-buffer-pool]: https://github.com/hyperium/hyper/discussions/4139
[dekker]: https://en.wikipedia.org/wiki/Dekker%27s_algorithm
[bytes-buf]: https://docs.rs/bytes/1.12.1/bytes/trait.Buf.html
[bytes-buf-mut]: https://docs.rs/bytes/1.12.1/bytes/trait.BufMut.html
[bytes-chunk-mut]: https://docs.rs/bytes/1.12.1/src/bytes/buf/buf_mut.rs.html#165-171
[bytes-from-owner]: https://github.com/tokio-rs/bytes/blob/76c0fbb54ed4336caf9d2311658a2f4a5627c21d/src/bytes.rs#L247-L289
[bytes-owner-vtable]: https://github.com/tokio-rs/bytes/blob/76c0fbb54ed4336caf9d2311658a2f4a5627c21d/src/bytes.rs#L1104-L1164
[bytes-slice]: https://github.com/tokio-rs/bytes/blob/76c0fbb54ed4336caf9d2311658a2f4a5627c21d/src/bytes.rs#L351-L385
[madvise2]: https://man7.org/linux/man-pages/man2/madvise.2.html
[mmap2]: https://man7.org/linux/man-pages/man2/mmap.2.html
[mprotect2]: https://man7.org/linux/man-pages/man2/mprotect.2.html
[open2]: https://man7.org/linux/man-pages/man2/open.2.html
[statx2]: https://man7.org/linux/man-pages/man2/statx.2.html
[virtual-alloc]: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-virtualalloc
[virtual-free]: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-virtualfree
[sdk-body-retry]: https://github.com/smithy-lang/smithy-rs/blob/f10257612a93616cae942a5c7a44747a8a9a505a/rust-runtime/aws-smithy-types/src/body.rs#L108-L125
[sdk-body-http]: https://github.com/smithy-lang/smithy-rs/blob/f10257612a93616cae942a5c7a44747a8a9a505a/rust-runtime/aws-smithy-types/src/body/http_body_1_x.rs#L17-L71
[sdk-checksum-body]: https://github.com/smithy-lang/smithy-rs/blob/f10257612a93616cae942a5c7a44747a8a9a505a/rust-runtime/aws-smithy-checksums/src/body/calculate.rs#L76-L123
[s3-checksum-selection]: https://github.com/awslabs/aws-sdk-rust/blob/df3f0a472d192594823536e3574edc0b4ae0bb91/sdk/s3/src/http_request_checksum.rs#L249-L283
[s3-chunked-selection]: https://github.com/awslabs/aws-sdk-rust/blob/df3f0a472d192594823536e3574edc0b4ae0bb91/sdk/s3/src/aws_chunked.rs#L198-L206
