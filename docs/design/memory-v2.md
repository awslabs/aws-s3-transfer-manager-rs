# Memory

Concurrent downloads retain ranges fetched ahead of a slow consumer and ranges awaiting ordered
delivery. Uploads retain retryable parts assembled from forward-only input. Independent
per-transfer limits either constrain a lone transfer or multiply beyond host memory under
concurrency. Allocating and freeing each part also leaves allocator and page-management work on the
data path.

One memory pool belongs to one transfer manager. Managed work reserves planned demand before
dispatch and acquires writable buffers while executing. Reservation controls admission; acquisition
selects physical memory. Traffic through an HTTP client configured with the pool shares the same
accounting even when a transport read carries no reservation. Private protocol scratch and clients
without the pool remain outside its accounting.

## Requirements

### Bound admission and account overage

Configured capacity is the ceiling applied when admitting managed work. Concurrent uploads,
downloads, retry bodies, completed output awaiting consumption, and pooled transport reads share one
accounting domain rather than independent direction, connection, or transfer limits.

Configured capacity is not a hard limit on process RSS or mapped address space. It neither requires
the pool to allocate that amount of memory nor establishes a floor that the pool retains.

An acquisition without a reservation may take pooled ownership above configured capacity. A request
admitted while managed work is otherwise idle may do the same. In either case, the memory enters
accounting before use and displaces later managed work until its owners release it.

Unreserved acquisition by manager-owned integrations originates from work the manager dispatched.
Withholding new admission stops additional work from adding to the overage, and owner release drains
it. A shared pool-enabled client or another independently driven integration can add traffic outside
manager dispatch and therefore requires an external bound for configured capacity to remain
meaningful.

### Resolve memory contention before dispatch

A work item's planned memory demand is admitted before it occupies dispatch capacity. Otherwise
several work items can hold execution resources and partial state while each waits for memory held
behind the work it prevents from running.

Successful admission removes waits on other buffer owners from the execution path. An admitted
operation does not suspend until another operation returns a buffer. Mapping, placement,
registration, and operating-system allocation can still fail; admission guarantees progress
against pool contention rather than infallible physical allocation.

### Support acquisition when a reservation cannot be carried

An internal integration boundary that cannot present a reservation may use unreserved acquisition.
Transport receive allocation is the motivating case: the transport can request writable memory
without transfer or reservation identity.

Managed transfer code that can present reservation authority does not use this path. Exhausting
configured capacity does not make an unreserved acquisition wait for another buffer owner or fail by
itself. The acquisition may fail when compatible physical memory cannot be obtained. Its memory
enters pool accounting before writable access is exposed to the caller.

Unreserved accounting is independent of protocol, frame boundaries, and request identity.

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

Admission pressure delays managed work instead of rejecting it. Without admission order, a stream of
small requests can consume each release and indefinitely defer an earlier large request. Delayed
work is therefore admitted in arrival order once preceding demand retires.

A request larger than configured capacity, or blocked only by memory held outside active managed
demand, runs once no other managed demand remains. Idle-only admission applies to one managed
request at a time under either condition. Progress assumes that earlier owners eventually return
memory and required physical allocation succeeds.

Cancelling delayed work removes its demand from admission. Cancellation after admission releases
future acquisition authority; memory already handed to an owner remains accounted until its final
owner returns.

### Release without scheduler or runtime participation

The final payload owner may drop on any thread and may outlive the transfer manager handle. Its
return path releases both physical memory and the associated accounting without a scheduler turn,
an async runtime, or a fallible upgrade of weak state. Live owners retain the state required to
complete their return.

Requiring a scheduler turn for release can wedge admission and scheduling: memory-owning work can
occupy the dispatch capacity needed to run the release that would unblock it.

When a return or cancellation makes delayed work eligible, that work is reconsidered without waiting
for unrelated scheduler or admission activity.

### Reuse storage without retaining the peak working set

Steady traffic reuses buffers across operations instead of allocating and freeing one part-sized
object for every I/O. Free buffers can serve uploads, downloads, retries, or transport reads rather
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

Each pool allocation unit begins at a page boundary, occupies a whole number of pages, and shares no
page with another allocation unit. This lets placement, protection, reclamation, and registration
operate without affecting a page owned by another live allocation.

Page granularity does not make every payload view suitable for direct I/O. Linux `O_DIRECT`
requirements can vary by filesystem and device and may require per-file `STATX_DIOALIGN` checks
([open(2)][open2], [statx(2)][statx2]). Payload that starts inside a buffer, short intermediate
writes, and a final unaligned tail require aligned assembly or a buffered fallback.

### Preserve placement opportunities without static partitions

NUMA locality, NIC affinity, and completion ownership can influence where storage should come from.
Locality is a preference: remote memory remains correct, and correctness cannot require thread
pinning. Registration or completion ownership may instead be a hard compatibility constraint that
excludes otherwise free storage.

Soft placement preferences do not partition capacity or make otherwise compatible memory
unavailable. Planned demand can express coarse affinity and hard compatibility without binding a
reservation to a physical buffer.

### Scale buffer reuse across cores and pool size

Buffer acquisition and return occur for every payload unit moved through the pool. Their frequency
is much higher than admission, growth, or reclamation. Common reuse and return have bounded cost
independent of total pool size and do not serialize all participating cores.

Contention alone cannot cause an acquisition to grow repeatedly without making progress.
Operating-system allocation latency remains outside this guarantee.

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

## Architecture

### Pool structure

`BufferPool` is the top-level pool API. It owns admission and physical storage behind a shared
`PoolInner`:

```text
BufferPool
`-- Arc<PoolInner>
    +-- Mutex<AdmissionState>
    |   `-- admission policy, prepared capacity, FIFO waiters, shutdown
    +-- CoverageState
    |   `-- available coverage and uncovered charges
    `-- Arena
        `-- physical storage, carrier acquisition, reuse, reclamation
```

`AdmissionState` decides whether planned demand may be dispatched. `CoverageState` accounts
acquisition and return at carrier frequency. `Arena` owns mappings and carrier state but does not
grant admission. A reservation grant prepares capacity for the admitted total.

All capacity and accounting quantities below count carriers. A carrier is one fixed-size,
page-granular unit of physical ownership. The arena may group carriers into larger mapping and
reclamation units, but those groups do not change the accounting unit.

The core types have the following contracts:

| Type               | Contract                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------- |
| `BufferPool`       | Cloneable handle to one admission and physical-storage domain                                     |
| `PoolInner`        | Shared state retained by pool handles, reservation state, waiter handles, and carrier guards      |
| `AdmissionState`   | Serialized admission policy, prepared capacity, queue, cancellation, and shutdown state           |
| `CoverageState`    | Atomic aggregate coverage and uncovered-charge state                                              |
| `Arena`            | Physical preparation, acquisition, reuse, placement, and reclamation                              |
| `Reservation`      | Linear authority over one admitted envelope; close consumes it and ends direct acquisition        |
| `ReservationState` | Private state retained by carriers acquired through a reservation                                 |
| `WaitTicket`       | Cancellable handle to one queued reservation request and its eventual result                      |
| `Carrier`          | The fixed-size, page-granular allocation unit; also the physical ownership and accounting unit    |
| `CarrierGuard`     | Final-return owner for one acquired carrier, its aggregate charge, and optional reservation state |
| `PooledBufMut`     | Fixed-capacity mutable buffer over one or more acquired carriers                                  |
| `SegmentedBytes`   | Immutable public byte container over one or more contiguous presentation segments                 |

A reservation names demand rather than physical carriers. Granting one does not remove carriers
from the arena or bind them to a block. Carriers that satisfy the same hard requirements remain
available to any eligible acquisition.

### Handles and ownership

Reservations, mutable buffers, and immutable views retain the state required to return physical
memory and accounting:

```text
BufferPool --------------------------+
Pool-enabled transport provider -----+
ReservationState --------------------+
CarrierGuard ------------------------+
WaitTicket --------------------------+--> Arc<PoolInner>

Reservation --> Arc<ReservationState>

CarrierGuard
    +-- aggregate charge --> Arc<PoolInner>
    `-- optional reservation state --> Arc<ReservationState>

PooledBufMut ------> 1..N Arc<CarrierGuard>
Bytes ------------->      Arc<CarrierGuard>
SegmentedBytes ----> 1..N Arc<CarrierGuard>
```

`PoolInner` owns queued waiter state but not granted reservations or carrier guards. A grant removes
its waiter from the admission queue before publishing the `Reservation` to the waiting caller.
Every carrier guard retains `PoolInner`; no weak upgrade, scheduler turn, or runtime task is required
for final return.

One carrier guard represents one physical carrier and one accounting charge. Several immutable views
may share that guard without duplicating either. Direct acquisition also retains private reservation
state. Return before close restores direct authority for retry. Close prevents later returns from
reopening acquisition. The private state drops after the final direct carrier returns.

### Reservation and buffer flow

`poll_work()` completes reservation admission before returning dispatchable work:

```text
poll_work()
    |
 reserve(envelope)
    |
    +-- grant: Reservation
    |       |
    |       `-- PollWork::Ready { io, .. } -> dispatch
    |
    +-- queued: WaitTicket
    |       |
    |       `-- PollWork::Pending
    |               |
    |         grant or error -> scheduler.wake(id)
    |               |
    |         later poll_work()
    |               +-- Reservation -> PollWork::Ready { io, .. } -> dispatch
    |               `-- error or cancellation -> no dispatch
    |
    `-- error -> no dispatch
```

`reserve` returns an immediate `Reservation`, a `WaitTicket` that owns a queued request, or an error.
A queued transfer returns `PollWork::Pending`. Grant or terminal failure wakes the transfer, and a
later `poll_work()` observes the ticket result. Only `PollWork::Ready` carries work to dispatch; its
`IoRequest` work data owns the `Reservation`.

A reservation grant admits one carrier-count envelope for the work item. The envelope is fungible:
it is not divided between direct and unreserved acquisition and does not predict which path will
consume it. `Reservation` additionally limits direct acquisition by that work item.

After dispatch, reserved and unreserved acquisition converge on the same mutable and immutable
buffer types:

```text
dispatched work + Reservation -- acquire(request) ----+
provider without reservation -- acquire_unreserved ---+--> PooledBufMut
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

When reserved acquisition is complete:

```text
Reservation -- close_acquisition --> retire planned demand
```

Both acquisition paths account requested capacity before exposing writable memory. Reserved
acquisition also consumes authority from its `Reservation`. Ownership beyond admitted demand
remains accounted until return and delays later admission; later grants do not absorb it. Both paths
either satisfy the complete request or return an error and never wait for another carrier owner to
return memory.

`PooledBufMut` holds exclusive mutable authority over unpublished ranges. Publication transfers
initialized ranges to `Bytes` or `SegmentedBytes` without changing the carrier charge. Immutable
views can outlive both the work item and the public pool handle.

`close_acquisition` consumes the `Reservation`, ends future direct acquisition, and retires planned
demand while preserving the charges held by mutable or immutable owners. Final carrier return first
makes physical storage reusable and then releases its accounting charge; the resulting capacity may
make the FIFO head eligible for admission.

### Admission and accounting

#### Accounting model

Admission tracks demand that may be acquired separately from ownership that may outlive it. The
model uses the following terms:

- An **envelope** is the carrier count granted to one reservation.
- A **charge** accounts one carrier from aggregate debit until rollback or final owner return.
- **Available coverage** is the part of all open envelopes not occupied by charges.
- An **uncovered charge** is a charge not backed by an open envelope.
- **Admission used** is the complete open envelopes plus all uncovered charges.

Envelope coverage is aggregate. It is not assigned to a reservation or divided into direct, upload,
download, or transport portions. Direct and unreserved acquisition consume the same coverage.

Assume one reservation has an envelope of four carriers:

| Action                 | Open envelope                | Charges outside an open envelope | `admission_used` |
| ---------------------- | ---------------------------- | -------------------------------: | ---------------: |
| Grant four             | four available               |                                0 |                4 |
| Acquire two directly   | two charged, two available   |                                0 |                4 |
| Acquire one unreserved | three charged, one available |                                0 |                4 |
| Close the reservation  | none                         |                                3 |                3 |
| Return all carriers    | none                         |                                0 |                0 |

A covered acquisition changes available envelope capacity into a charge without changing
`admission_used`. The unreserved acquisition in the example consumes aggregate coverage just like
the direct acquisition; its API path does not make the charge uncovered.

Close removes the one unused unit from admission. The three carrier owners remain, so their charges
move outside the closed envelope. No carrier moves and no memory is allocated during this
reclassification. Prepared capacity remains available after the final return and is not shown.

At this point, one unit of aggregate coverage remains. A request for two more carriers, whether
unreserved or direct when reservation-local authority permits, would consume that unit and add one
uncovered charge. `admission_used` would rise from four to five. A later grant would add its complete
envelope without removing that charge. Otherwise successive grants could absorb the same
outstanding ownership without any carrier returning.

The accounting fields name the states in that lifecycle:

- **Active planned demand** (`active_planned_demand`) is the sum of all open envelopes.
- **Available coverage** (`available_coverage`) is the unoccupied part of active planned demand.
- **Uncovered charges** (`uncovered_charges`) are charges outside active planned demand.
- **Outstanding charges** (`outstanding_charges`) include carrier charges and acquisition debits
  awaiting transfer to physical carriers.

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

The pool keeps policy, physical state, aggregate accounting, and reservation-local authority
separate:

| Scope                 | Quantities                                                                                | Question                                                       |
| --------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Admission policy      | `configured_capacity`                                                                     | May another reservation be granted normally?                   |
| Physical storage      | `prepared_capacity`, `physical_live`                                                      | How much storage is ready, and how much is owned?              |
| Aggregate accounting  | `active_planned_demand`, `available_coverage`, `uncovered_charges`, `outstanding_charges` | How much demand and ownership pressure is charged to the pool? |
| Reservation authority | `envelope`, `direct_outstanding`                                                          | May this reservation acquire another carrier directly?         |

Configured capacity is not a hard physical-memory limit. Prepared capacity is not ownership.
Aggregate accounting does not bind a charge to a particular envelope. Reservation-local authority
limits one direct caller but does not partition aggregate coverage.

#### State and invariants

All internal quantities use a carrier-count type:

```rust
#[repr(transparent)]
struct CarrierCount(usize);
```

Byte requests convert to `CarrierCount` at the acquisition boundary. `usize` supports indexing and
geometry without making the semantic type depend on the packed accounting representation.

Aggregate accounting has serialized state and carrier-frequency state:

```rust
struct PoolInner {
    admission: Mutex<AdmissionState>,
    coverage: CoverageState,
    arena: Arena,
}

struct AdmissionState {
    ledger: AdmissionLedger,
    waiters: VecDeque<Waiter>,
    closed: bool,
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

`AdmissionState` serializes grant policy, physical preparation, FIFO state, shutdown, and
transitions that change uncovered charges. `CoverageState` sits outside the mutex so an acquisition
fully covered by available coverage and a return that only restores coverage can complete with one
compare-and-exchange loop.

Available coverage and uncovered charges share one atomic value because acquisition, close, and
return must observe and change them as one state. Separate atomics permit close to miss an
acquisition between its coverage debit and uncovered-charge publication. They also permit return to
restore coverage after close has withdrawn the corresponding envelope.

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

Grant adds equal planned demand and available coverage. Acquisition only lowers coverage. Close
lowers planned demand and removes no more coverage than its envelope. Return restores coverage only
after uncovered charges have been removed.

A carrier is never live before it is charged:

```text
physical_live <= outstanding_charges <= admission_used
```

Acquisition installs its complete charge before claiming physical storage. Final return makes the
carrier physically reusable before releasing its charge. Non-negative available coverage gives the
second inequality.

Completed transitions retain enough prepared capacity to cover admission:

```text
prepared_capacity >= admission_used
```

Grant and uncovered acquisition prepare to the new `admission_used` before publishing their
transition. Failure rolls back without exposing writable memory. Close and return cannot increase
the required preparation.

**Alternative: Static envelope partitions.** A grant could divide its envelope into direct and
unreserved portions. Reservation time cannot predict which path will acquire the memory. A fixed
split can strand one portion while another path creates avoidable uncovered charges.

#### Reservation admission

`try_reserve` attempts an immediate grant without constructing a notification callback. Inability
to grant immediately is normal control flow:

```rust
fn try_reserve(
    &self,
    envelope: CarrierCount,
) -> Result<Option<Reservation>, ReserveError>;

fn reserve(
    &self,
    envelope: CarrierCount,
    notify: NotifyFn,
) -> Result<ReserveOutcome, ReserveError>;

enum ReserveOutcome {
    Ready(Reservation),
    Pending(WaitTicket),
}

enum ReserveError {
    Closed,
    PhysicalPreparationFailed,
    CapacityOverflow,
}
```

`try_reserve` returns `Ok(None)` when an older waiter exists or the request is not currently
eligible. It may still allocate reservation state and prepare physical capacity when it grants.
`reserve` rechecks while admission is serialized before enqueuing, so capacity released between the
two calls is not lost.

A fresh request cannot bypass an existing waiter. With an empty FIFO, or when examining its head,
an envelope is eligible for a normal grant when:

```text
admission_used + envelope <= configured_capacity
```

An idle-only grant is also eligible when:

```text
active_planned_demand == 0
```

Idle here means that no reservation retains acquisition authority; carriers may remain owned. The
idle-only rule permits one envelope larger than configured capacity and one envelope blocked only
by uncovered charges. Admission cannot force independently driven owners to return. Granting the
envelope makes `active_planned_demand` nonzero and disables another idle-only grant until it closes.

Before publishing a grant, admission prepares physical capacity for the post-grant
`admission_used`. Preparation does not assign carriers to the reservation. Success adds the complete
envelope to `active_planned_demand` and `available_coverage`; `uncovered_charges` is unchanged.
Failure changes none of those quantities.

`Reservation` is non-cloneable direct-acquisition authority:

```rust
struct Reservation {
    state: Option<Arc<ReservationState>>,
}

struct ReservationState {
    pool: Arc<PoolInner>,
    envelope: CarrierCount,

    // CLOSED plus direct carrier guards and in-flight direct debits.
    owner_state: AtomicU64,
}
```

`Reservation` is not `Clone`. Its local `direct_outstanding` count cannot exceed `envelope`.
Reservation-local authority limits one work item. Aggregate coverage accounts both acquisition
paths and does not assign physical carriers to reservations.

#### Wait queue

The FIFO stores the result and notification separately:

```rust
type NotifyFn = Arc<dyn Fn() + Send + Sync + 'static>;

struct Waiter {
    envelope: CarrierCount,
    slot: Arc<WaitSlot>,
    notify: NotifyFn,
}

struct WaitSlot {
    state: Mutex<WaitState>,
}

enum WaitState {
    Queued,
    Granted(Reservation),
    Failed(ReserveError),
    Taken,
}

struct WaitTicket {
    slot: Arc<WaitSlot>,
    pool: Arc<PoolInner>,
}
```

FIFO drain examines requests in arrival order and stops at the first ineligible request. An eligible
head is removed only after preparation succeeds or fails. Success installs an already-charged
`Reservation` in its slot. Preparation failure installs `Failed` and continues draining; a failed
head cannot permanently block the queue.

The terminal slot state is installed before its callback runs. Callbacks run after releasing
admission serialization and may re-enter the pool. `WaitTicket::take` reports pending while the slot
remains `Queued` and consumes exactly one terminal result afterward. A notified caller therefore
receives its assigned reservation rather than racing other callers for released capacity.

Dropping a queued ticket cancels its request and drains again if cancellation exposed a new head.
Dropping an untaken grant drops the stored reservation and retires its envelope. Grant racing
cancellation produces either one caller-owned reservation or one retired reservation.

Admission takes the admission mutex before a waiter-slot mutex. Ticket drop removes the slot state
under its mutex, releases that mutex, and only then enters admission for cancellation. This order
prevents lock inversion.

Closing admission rejects new reservation requests and installs `Failed(Closed)` in every queued
slot. It does not invalidate granted reservations, unreserved acquisition, or carrier return.

Reservation close, a return that removes uncovered charges, front cancellation, and configured
capacity increase reconsider the FIFO. Each transition collects callbacks while serialized and
invokes them after unlocking.

#### Acquisition

Both acquisition paths request a minimum writable byte capacity:

```rust
fn acquire(
    &self,
    reservation: &Reservation,
    min_bytes: NonZeroUsize,
) -> Result<PooledBufMut, AcquireError>;

fn acquire_unreserved(
    &self,
    min_bytes: NonZeroUsize,
) -> Result<PooledBufMut, AcquireError>;
```

The pool converts `min_bytes` to a carrier count once. The conversion and acquisition either
provide at least that capacity or return an error. Independent request rounding against one
reservation remains an open question below.

Acquisition fails if direct authority is invalid or insufficient, the carrier count cannot be
represented, or physical storage cannot be obtained. These conditions do not expose partial
writable capacity.

Direct acquisition first debits reservation-local authority. Both paths then debit the complete
carrier count from aggregate accounting before asking the arena for physical storage:

1. Consume as much `available_coverage` as remains.
2. Add any shortfall to `uncovered_charges` while admission is serialized.
3. Prepare physical capacity for the resulting `admission_used`.
4. Claim the complete carrier count from the arena.
5. Transfer one aggregate charge and optional direct provenance to each `CarrierGuard`.

An acquisition fully covered by available coverage performs the aggregate debit through
`CoverageState` without the admission mutex. A shortfall enters admission serialization before
publishing the uncovered charge. Preparation failure applies the inverse transition and restores
direct authority before returning an error. No writable memory is exposed during that interval.

`AcquisitionDebit` owns charges awaiting transfer to carrier guards:

```rust
struct AcquisitionDebit {
    pool: Arc<PoolInner>,
    direct: Option<Arc<ReservationState>>,
    uncommitted: CarrierCount,
}
```

Each successful physical claim moves one unit out of `AcquisitionDebit`. If a later claim fails,
completed guards return their carriers and charges, and dropping the debit restores every
uncommitted charge. Acquisition therefore returns the complete requested capacity or an error;
partial physical success is not exposed.

Acquisition does not wait for an existing carrier owner to return. A reusable-storage miss enters
the arena's serialized recheck and growth path. Physical allocation may fail after accounting
succeeds; rollback leaves the reservation and pool usable.

#### Close and return

Closing consumes direct-acquisition authority:

```rust
fn Reservation::close_acquisition(self);
```

`acquire` is synchronous and borrows the non-cloneable `Reservation`. Consuming the handle for close
makes acquire-versus-close and partial-rollback-versus-close races unrepresentable through the safe
API. Dropping an open reservation performs the same close transition.

Coverage is aggregate rather than attributed to individual reservations. Closing envelope `E`
therefore removes `E` units from aggregate planned demand. It removes available units first. If
fewer than `E` units remain available, the rest are occupied by charges that must remain accounted
after close:

```text
unused_removed = min(E, available_coverage)
occupied_reclassified = E - unused_removed

active_planned_demand -= E
available_coverage -= unused_removed
uncovered_charges += occupied_reclassified
```

For the four-carrier example, one unit remains available and three are occupied. Close removes the
one unused unit and reclassifies the three occupied units as uncovered charges. No new charge is
created. Direct carrier guards retain `ReservationState` only until their local owner counts have
returned.

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
reusable carrier and grows the arena while the responsible carrier is still unavailable. Removing
an uncovered charge lowers `admission_used` and reconsiders the FIFO. Restoring coverage does not
change admission eligibility.

A direct return restores local acquisition authority while the reservation remains open. Return
cannot reopen a closed reservation.

**Obligations.**

- Available coverage never exceeds active planned demand.
- Every physical carrier owner has exactly one aggregate charge.
- Acquisition installs the complete charge before exposing writable memory.
- Partial acquisition failure restores every uncommitted aggregate and reservation-local debit.
- Closing a reservation removes its complete envelope without removing surviving owner charges.
- New grants do not reclassify existing uncovered charges.
- Physical return precedes aggregate charge release.
- A waiter receives its assigned terminal result exactly once.
- Fresh reservation requests do not bypass an existing waiter.
- Notification callbacks run only after the terminal result is visible and admission is unlocked.
- Closing admission leaves existing reservations, carrier owners, and their return paths valid.

### Physical storage

#### Block geometry and lifetime

The arena prepares, claims, returns, and reclaims carriers. Admission determines how many carriers
may be acquired; the arena selects their addresses.

The physical hierarchy is:

- A **carrier** is the fixed-size, page-granular unit of acquisition, ownership, and accounting.
- A **block** is the preparation and reclamation unit. It contains a fixed number of carriers.
- A **block slot** represents one block. Its virtual range, geometry, valid bitmap masks, and
  compatibility metadata remain stable for the pool's lifetime.
- A **block incarnation** is one claimable activation of a slot. It owns an occupancy bitmap and an
  `Active`, `Draining`, or `Dead` state.

```text
Arena
 `-- registry snapshot
      `-- Arc<BlockSlot>
           +-- stable virtual range
           +-- carrier geometry and valid bitmap masks
           +-- mapping state
           `-- current: ArcSwapOption<BlockIncarnation>
                    |
                    +-- at t1 --> BlockIncarnation A { state, bitmap A }
                    |
                    `-- at t2 --> BlockIncarnation B { state, bitmap B }

slot lifetime:        |---------------------------------------------|
incarnation A:             |----------|
incarnation B:                            |------------------|
```

Only one incarnation is current. Revival allocates a new incarnation and bitmap; it never clears
and reuses a bitmap from an earlier activation. An old claim attempt may retain incarnation A after
the slot publishes incarnation B, but the two attempts mutate different bitmap objects.

Each block contains a whole number of equal-size carriers:

```text
block base
    |
    v
+-----------+-----------+-----------+-----------+
| carrier 0 | carrier 1 | carrier 2 | carrier 3 |
+-----------+-----------+-----------+-----------+

carrier_address = block_base + carrier_index * carrier_size
block_bytes     = carrier_count * carrier_size
```

The block base and carrier size are page multiples, so every carrier begins on a page boundary and
shares no page with another carrier. A final partial bitmap word is masked by immutable
`valid_masks`; padding bits never name carriers and are never claimable.

One carrier size removes size-class fragmentation and makes every free carrier in the same
compatibility domain suitable for every byte-capacity request. It retains internal tail waste and
per-carrier ownership cost. Contiguous runs within one block are an optimization. A request can
complete from carriers in different blocks.

The stable per-block state is:

```rust
struct BlockSlot {
    id: u32,
    base: NonNull<MaybeUninit<u8>>,
    len: usize,
    carrier_size: usize,
    carrier_count: CarrierCount,
    valid_masks: Box<[u64]>,

    // Serializes target-specific protection, discard, and revival.
    mapping: Mutex<MappingState>,
    current: ArcSwapOption<BlockIncarnation>,
}

struct BlockIncarnation {
    state: AtomicU8,             // Active | Draining | Dead
    in_use: Box<[AtomicU64]>,    // one bit per carrier
}
```

The bitmap is the source of truth for physical ownership. A set valid bit has exactly one
provisional or committed owner. A clear valid bit is available to claim. Search hints and rotating
origins select where to look; they never own or hide capacity.

#### Preparation and growth

Physical quantities describe different states:

- **Reserved address space** is exclusively owned by a block slot but inaccessible.
- **Committed backing** is physical or commit-accounted memory where the operating system exposes
  that distinction.
- **Prepared capacity** has completed the mapping, protection, placement, and registration required
  for acquisition.
- **Physical live** counts carriers retained by provisional, mutable, or immutable owners.

Configured capacity is an admission policy. It does not imply that the same amount is mapped,
committed, prepared, or resident.

The arena's registry and serialized growth state are:

```rust
struct Arena {
    carrier_size: usize,
    registry: BlockRegistry,
    state: Mutex<ArenaState>,
}

struct ArenaState {
    slots: Vec<Arc<BlockSlot>>,
}

struct BlockRegistry {
    snapshot: ArcSwap<Vec<Arc<BlockSlot>>>,
}
```

`BlockRegistry` publishes immutable snapshots of the slots. Optimistic claim scans load a snapshot
without taking `ArenaState`. Growth, registry publication, and serialized fallback claiming use
`ArenaState`. Return goes directly to the slot recorded by the carrier allocation and does not scan
the registry.

Slots are fungible. A slot does not record whether its block was prepared below or above configured
capacity. Admission state already records configured capacity and total prepared capacity;
reclamation derives excess prepared capacity from those aggregate quantities.

Preparation operates on whole blocks. Under admission serialization it:

1. Selects a compatible reserved slot or reserves a new stable virtual range.
2. Makes the complete range writable and performs required placement or registration.
3. Allocates a fresh incarnation and initializes its bitmap.
4. Adds the block's carrier count to `prepared_capacity`.
5. Publishes the incarnation as `Active`.

Publication follows the prepared-capacity update. A claimant that observes `Active` therefore sees
complete geometry, writable storage, and capacity included in the admission floor. Failure before
publication leaves no claimable incarnation and does not add the block to prepared capacity.

Block rounding may prepare spare carriers without admitting additional demand. Those carriers
remain reusable. Before claiming them, an acquisition consumes available coverage and records any
remainder as uncovered charges.

The scalar prepared count does not establish that a particular hard compatibility domain can
satisfy an acquisition. The baseline has one compatibility domain, so exhaustive fallback must find
enough free capacity when the accounting and bitmap invariants hold. A preference for contiguity or
locality does not authorize growth while compatible capacity remains available. Hard compatibility
can make the scalar sufficient while no eligible carrier exists.

Block metadata reserves a hard compatibility key for registration or completion ownership. NUMA
locality remains a soft preference used to order eligible blocks. Hard compatibility can require
additional preparation; it does not create static admission quotas.

Preparation may run synchronously. Mapping, placement, registration, or operating-system commit can
fail and return an acquisition or reservation error. No progress guarantee places a time bound on
those operations.

#### Carrier acquisition

One arena acquisition requests a complete carrier batch. It returns every requested carrier or an
error; no partial batch is exposed.

The common path performs a bounded amount of optimistic bitmap work from a rotating origin:

1. Load one immutable registry snapshot.
2. Inspect at most the configured number of bitmap words across eligible `Active` blocks.
3. Keep every bit won from each atomic bitmap operation.
4. Confirm that each touched block remains `Active` after the bitmap mutation.
5. Return the batch when enough carriers have been won.

The scan bound limits common-path work, not usable capacity. A miss can be false because compatible
free carriers may exist outside the inspected locations or may have been returned concurrently.

After an optimistic miss, acquisition enters the serialized fallback:

1. Exhaustively scan every compatible `Active` block.
2. Retain successful provisional claims across blocks.
3. If the batch remains incomplete, prepare a compatible block privately.
4. Set the fallback claimant's required bits in the fresh incarnation before publishing it.
5. Add the whole block to prepared capacity and publish its remaining carriers as `Active`.
6. Repeat preparation until the batch is complete or preparation fails.

Preclaiming the missing carriers before publication prevents lock-free claimants from consuming the
new capacity and forcing the serialized claimant to grow repeatedly. The fallback therefore
returns the complete compatible batch or a physical preparation or compatibility error. It does not
wait for an existing owner to return a carrier. The guarantee does not bound mutex scheduling or
operating-system allocation latency.

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
struct ProvisionalBits {
    slot: Arc<BlockSlot>,
    incarnation_identity: IncarnationIdentity,
    won: Vec<WonWord>,
}

struct CarrierAllocation {
    slot: Arc<BlockSlot>,
    index: u32,
    incarnation_identity: IncarnationIdentity,
    ptr: NonNull<MaybeUninit<u8>>,
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

impl PendingAcquisition {
    fn finish(self) -> Result<PooledBufMut, AcquireError>;
}

impl Drop for PendingAcquisition {
    fn drop(&mut self) {
        self.guards.clear();
        drop(self.claim.take());
        drop(self.debit.take());
    }
}
```

`PendingAcquisition::finish` succeeds only when `claimed == required`. It consumes the complete
batch and matching accounting debit, converts every provisional bit to one `CarrierAllocation`, and
transfers each allocation and charge into the returned `PooledBufMut`. An incomplete pending
acquisition is dropped and cannot publish a buffer.

Dropping `ProvisionalBits` rolls back every bit it still owns. Conversion removes a bit from its
provisional mask only after the corresponding `CarrierAllocation` owns it. Unwinding during
conversion therefore returns completed allocations and leaves unconverted bits for provisional
rollback.

The accounting debit is installed before physical acquisition. `PendingAcquisition::drop` returns
completed guards and the remaining physical batch before releasing the debit. During `finish`, each
allocation and one charge are consumed into an `Arc<CarrierGuard>` retained by the pending owner.
Only the complete guard set is arranged into carrier runs and moved into `PooledBufMut`. Capacity
prepared before a failed acquisition remains `Active` and reusable.

The common scan cost is independent of total pool size. Serialized fallback is allowed to inspect
the complete compatible registry because it runs only after a bounded optimistic miss. Carrier
return performs one bitmap update and does not take the arena growth lock.

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
6. Return a linear cleanup token and release admission serialization.
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
    ProtectionPending,
}
```

Incarnation state controls claims. Mapping state controls access to the stable virtual range.
`MappingState::Prepared` records a writable, fully prepared mapping; it does not by itself include
the block in `prepared_capacity`.

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

Reserved  -- whole-range RW fails ----> ProtectionPending
Prepared  -- whole-range NONE fails --> ProtectionPending

ProtectionPending -- whole-range NONE succeeds --> Reserved
ProtectionPending -- whole-range RW succeeds ----> Prepared
```

The state machines couple through ordered capacity and publication transitions:

- Preparation or revival establishes `MappingState::Prepared`, adds the block to
  `prepared_capacity`, and then publishes an `Active` incarnation.
- Trim publishes `Draining` before its confirming scan. A set bit restores `Active` without changing
  mapping state or prepared capacity.
- All-free confirmation removes the block from `prepared_capacity` while the mapping remains
  `Prepared` and the incarnation remains `Draining`. Cleanup then makes the range inaccessible,
  attempts discard, publishes `Dead`, and clears `current`.
- A failed protection call enters `ProtectionPending` and leaves the block outside prepared
  capacity. Initial preparation failure leaves `current` empty; deactivation failure leaves the
  existing incarnation `Draining`. Neither state is claimable.

A successful whole-range inaccessible transition from `ProtectionPending` continues the inactive
path. A successful whole-range writable transition restores mapping state first. Admission then
adds prepared capacity before either publishing a fresh incarnation or restoring a `Draining`
incarnation to `Active`. No failed protection call authorizes a mapping, capacity, or incarnation
transition.

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

The baseline mapping backend keeps each slot's virtual range reserved until pool destruction:

| Target  | Reserve                                    | Prepare                                    | Deactivate                  | Discard         |
| ------- | ------------------------------------------ | ------------------------------------------ | --------------------------- | --------------- |
| Linux   | anonymous private `PROT_NONE` mapping      | `mprotect(PROT_READ \| PROT_WRITE)`        | `mprotect(PROT_NONE)`       | `MADV_DONTNEED` |
| macOS   | anonymous private `PROT_NONE` mapping      | `mprotect(PROT_READ \| PROT_WRITE)`        | `mprotect(PROT_NONE)`       | `MADV_FREE`     |
| Windows | `VirtualAlloc(MEM_RESERVE, PAGE_NOACCESS)` | `VirtualAlloc(MEM_COMMIT, PAGE_READWRITE)` | `VirtualFree(MEM_DECOMMIT)` | same operation  |

The backend contract requires exclusive ownership of the same address after every result.
Deactivation success makes access fault. Deactivation failure enters `ProtectionPending` without
assuming that prior protection remains. Discard success makes backing absent or reclaimable;
discard failure may retain resident pages but cannot make the block claimable.

The Linux operations follow [mmap(2)][mmap2], [mprotect(2)][mprotect2], and
[madvise(2)][madvise2]. Windows reservation, commit, and decommit follow
[VirtualAlloc][virtual-alloc] and [VirtualFree][virtual-free].

Correctness does not assume that a failed protection call leaves the prior protection unchanged.

Prepared bytes are logically uninitialized regardless of initial or recommitted contents. Physical
acquisition exposes `MaybeUninit<u8>`. Only initialized prefixes may later become safe references,
immutable views, or outgoing I/O sources.

Registered, wired, or completion-owned mappings are trim-ineligible until their capability owner
completes the required teardown. A backend that can lose exclusive address ownership on an error
does not satisfy the block-slot contract.

#### Reclamation policy

Reclamation is armed when the transfer manager becomes globally idle. An activity epoch identifies
the idle interval; new managed work invalidates the pending deadline. Transfer-local idle does not
arm reclamation.

When the deadline expires, the pool snapshots configured and prepared capacity and computes:

```text
configured_block_ceiling =
    round_up_to_whole_blocks(configured_capacity)

retention_basis =
    min(prepared_capacity, configured_block_ceiling)

idle_retention_target =
    round_up_to_whole_blocks(retention_basis * 25%)
```

Prepared capacity above `configured_block_ceiling` is aggregate excess. It is not attached to
particular blocks. Any free block is eligible while both conditions remain true:

```text
prepared_capacity_after >= admission_used

min(prepared_capacity_after, configured_block_ceiling)
    >= idle_retention_target
```

The second condition permits reclamation above `configured_block_ceiling` without reducing the
retained baseline. Below that ceiling, it preserves the idle retention target. Reclamation never
prepares capacity to meet the target. Live carriers, provisional bits, hard compatibility, and
whole-block geometry can leave more capacity than the target.

When no eligible block is free, the coordinator keeps the idle reclaim request pending and scans
again after a bounded delay. New managed work cancels that retry by invalidating the idle epoch.
Carrier return does not have to notify the coordinator because the next scan observes blocks that
have become free. Reclaiming one block never prevents growth, claim, or trim of unrelated blocks.

The timeout provides cache hysteresis and is not a safety mechanism. Its duration, retry cadence,
carrier size, block size, optimistic scan budget, and rotating-origin policy are measurement
choices.

**Obligations.**

- Every carrier is page-aligned, page-multiple, and wholly contained in one stable block range.
- Padding bits outside `valid_masks` are never claimed or counted live.
- A valid set bit has exactly one linear provisional or committed owner.
- Revival publishes `Active` only after preparation and prepared-capacity accounting complete.
- Every activation uses a fresh bitmap; no reachable bitmap is reset for reuse.
- A claim protects one incarnation from before its first bitmap mutation through its state gate.
- Claim and trim cannot both complete while missing the other's publication.
- Gate-failure rollback writes only through its original incarnation guard.
- A gate-passed bit prevents incarnation replacement until rollback or final return clears it.
- Pointer construction follows a successful `Active` gate and never occurs for a rejected claim.
- Provisional conversion transfers every bit once, and an incomplete acquisition publishes no
  buffer.
- Serialized fallback exhaustively scans compatible prepared capacity before growth.
- Fresh growth reserves the fallback batch before publishing remaining capacity.
- Partial batch failure returns every physical bit before its accounting debit rolls back.
- Trim lowers prepared capacity only after all-free confirmation and before physical cleanup.
- Failed protection leaves the block nonclaimable and outside prepared capacity.
- Discard retry cannot race preparation or reclaim a revived block.
- Reclamation preserves both the admission floor and the idle retention target.
- Missing current state, identity mismatch, unexpected lifecycle state, or missing owned bits is
  fail-stop.

### Ownership and delivery

Physical ownership follows carriers. Byte presentation follows initialized ranges. A carrier run
groups adjacent writable carriers for I/O, while a segment groups contiguous immutable bytes for
reading. Neither changes the carrier-level accounting or return unit.

```text
one acquired carrier and one charge
                  |
                  v
            CarrierGuard
             /    |    \
            /     |     \
 WritableCarrier  |   Hold::Pooled
  writable suffix |         |
                  |         v
                  |   SegmentedBytes
                  v
           PooledWindow
                  |
                  v
                Bytes
```

`WritableCarrier`, `CarrierRun`, `PooledWindow`, `Segment`, `OwnedRange`, and `Hold` are private.
`PooledBufMut` is the mutable acquisition result. `SegmentedBytes` is the public immutable output
type (replacing `AggregatedBytes`).

#### Carrier ownership

One completed carrier checkout creates one `CarrierGuard`:

```rust
struct CarrierGuard {
    // State required to validate return and release the aggregate charge.
    pool: Arc<PoolInner>,
    // Linear physical-return capability, taken exactly once by final drop.
    allocation: Option<CarrierAllocation>,
    // Originating reservation state for a direct checkout.
    direct: Option<Arc<ReservationState>>,
}

// SAFETY: the allocation names a stable slot, shared state is synchronized,
// and the guard exposes no pointer access.
unsafe impl Send for CarrierGuard {}
unsafe impl Sync for CarrierGuard {}
```

The guard owns one checked-out carrier and one aggregate charge. `direct` records optional
reservation provenance; it does not assign aggregate coverage to that reservation. The public
`Reservation` is not retained by byte containers and may close while its carrier guards remain
live.

`CarrierGuard` is allocated behind `Arc` but is not itself cloneable. Every owner of a range within
the carrier clones the same `Arc<CarrierGuard>`. Final `Arc` drop runs `CarrierGuard::drop` once:

1. Take the `CarrierAllocation` and return its bitmap bit to the arena.
2. Decrement the originating reservation's outstanding direct-carrier count when `direct` is
   present.
3. Return one aggregate charge.

The second step restores direct acquisition authority only while the reservation remains open.
After close, it retires the local owner count without reopening acquisition.

Before clearing the bitmap bit, return verifies that the slot still contains the recorded
incarnation, that its lifecycle state permits return, and that the recorded bit remains set. The
live bit prevents trim from replacing that incarnation in a valid execution. A missing
incarnation, identity or lifecycle mismatch, or clear bit indicates double return, stale ownership,
allocator corruption, or a broken trim/revival invariant. The non-returning fail-stop handler
aborts without clearing an unexpected bit or releasing accounting.

The physical return completes before the accounting return.

The strong `Arc<PoolInner>` keeps the slot registry, admission state, and final-return path alive
after public pool handles or reservations have dropped. One escaped immutable view therefore
retains the pool as well as its carrier. The carrier's live bitmap bit prevents trim of its block.

The guard is per carrier, never per run or segment. A run-level guard would make one surviving byte
pin every carrier in the run.

**Alternative: Store a holder count beside each bitmap bit.** Rejected: trim would still use the
bitmap, so the count would add a second liveness fact that must agree with it. `Arc<CarrierGuard>`
keeps liveness in the owner graph and allocates refcount state only for checked-out carriers.

#### Mutable buffers

`PooledBufMut` is a fixed-capacity mutable buffer over one or more carrier runs. It never reallocates
or moves initialized bytes.

```text
PooledBufMut
  +-- CarrierRun A: block A [carrier 1][carrier 2][carrier 3]
  `-- CarrierRun B: block B [carrier 7]
```

Adjacency is opportunistic. Acquisition returns the requested carrier count even when the result
requires several runs. `CarrierRun` has no independent ownership or accounting.

```rust
pub(crate) struct PooledBufMut {
    // Preserves logical byte order; entries after the fourth spill to heap.
    runs: SmallVec<[CarrierRun; 4]>,
    // Next byte at which mutable initialization can continue.
    write_cursor: BufferCursor,
    // First initialized byte awaiting publication.
    publish_cursor: BufferCursor,
    // Initialized bytes between the publication and write cursors.
    initialized: usize,
    // Fixed writable capacity granted by acquisition.
    acquired_capacity: usize,
}

struct CarrierRun {
    // Stable slot containing every carrier in this run.
    slot_id: u32,
    // Index of the run's first carrier within the slot.
    first_carrier: u32,
    // Start of the contiguous writable run.
    ptr: NonNull<MaybeUninit<u8>>,
    // Total byte capacity of the run.
    capacity: usize,
    // Per-carrier ownership in ascending address order.
    carriers: VecDeque<WritableCarrier>,
}

struct WritableCarrier {
    guard: Arc<CarrierGuard>,
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
    offset: usize,
}
```

`SmallVec` keeps up to four runs inside `PooledBufMut`; a more fragmented acquisition spills
additional runs to heap storage. Four is an inline-capacity preference, not an acquisition or
fragmentation limit. The inline capacity may change without changing the ownership or accounting
contracts.

`ExclusiveRange` is linear and private. It cannot be cloned. Its consuming split operations produce
disjoint ranges, so publication can remove an initialized prefix while retaining exclusive mutable
authority over the suffix.

Every acquired byte is logically uninitialized, including bytes returned from newly committed or
zero-filled pages. Initialization advances from the write cursor and publication advances from the
publish cursor:

```rust
impl PooledBufMut {
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn publish_prefix(&mut self, count: usize) -> Bytes;
    fn freeze(self) -> SegmentedBytes;
}

unsafe impl BufMut for PooledBufMut {
    fn remaining_mut(&self) -> usize;
    fn chunk_mut(&mut self) -> &mut UninitSlice;
    unsafe fn advance_mut(&mut self, count: usize);
}

// SAFETY: moving the buffer transfers its unique range capabilities; every
// referenced carrier remains live through its guard.
unsafe impl Send for PooledBufMut {}
```

`capacity` reports the capacity acquired for the buffer; it never grows. `len` reports initialized
but unpublished bytes, and `remaining_mut` reports exclusive writable capacity. `chunk_mut`
returns the current carrier's contiguous uninitialized suffix.

`advance_mut(count)` asserts that the first `count` bytes exposed through writable ranges were
initialized. The caller must not advance beyond the ranges supplied by the preceding mutable view
or completed vectored operation. A crate-private vectored adapter exposes raw pointer-length pairs,
not `&mut [u8]`, and distributes the completed byte count across carriers in order.

Moving `PooledBufMut` between threads transfers its exclusive authority. It is `Send` when
`CarrierGuard` is `Send`, but it is not `Sync` and does not expose shared mutation.

Dropping `PooledBufMut` drops every remaining `WritableCarrier`. A carrier without a published view
then returns immediately. `freeze` consumes the mutable buffer and transfers only initialized
unpublished ranges into `SegmentedBytes`. It drops each mutable hold with no initialized output;
the carrier returns unless an earlier published view still shares its guard. An initialized prefix
in a partially used carrier retains that carrier and discards its unused suffix.

#### Immutable publication

`publish_prefix` returns one contiguous `Bytes` from initialized data in the current carrier. It
does not cross a carrier boundary. The operation consumes mutable authority over the prefix and
retains mutable authority over the disjoint suffix. It panics if `count` is zero, exceeds the
initialized unpublished prefix, or crosses the current carrier boundary.

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

#### Segmented delivery

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
    // Stable slot identity used only to validate presentation adjacency.
    slot_id: Option<u32>,
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

struct SegmentedBytesBuilder<'a> {
    // Resolves incoming views against stable slot ranges during construction.
    arena: &'a Arena,
    // Presentation ranges assembled so far.
    segments: VecDeque<Segment>,
    // Sum of the assembled presentation lengths.
    remaining: usize,
}

impl<'a> SegmentedBytesBuilder<'a> {
    fn push_segmented(&mut self, buffer: SegmentedBytes);
    fn push_view(&mut self, view: Bytes);
    fn finish(self) -> SegmentedBytes;
}
```

`Hold::Pooled` retains storage frozen directly from `PooledBufMut`. `Hold::View` retains an existing
immutable producer view, whether pool-backed or foreign. `push_segmented` consumes the input's
remaining segments and their existing holds; it does not reconstruct ownership from pointers.
`PooledBufMut::freeze` constructs pooled segments directly. The private builder resolves the
complete range of `push_view` against the arena's stable slot ranges; the finished value retains no
builder borrow. Owner boundaries need not be presentation boundaries:

```text
one Segment, assuming 64 KiB carriers: contiguous presentation [0..192 KiB]

+--------------------+--------------------+--------------------+
|      0..64 KiB     |    64..128 KiB    |   128..192 KiB    |
+---------+----------+---------+----------+---------+----------+
          |                    |                    |
       owner A              owner B              owner C
```

Construction extends the previous segment only when both ranges have the same known `slot_id` and
the previous range ends at the next range's pointer. Stable block-slot address ranges make this
test unambiguous, and the owners keep every byte in the merged range live. A foreign view has
`slot_id: None` and starts a new segment even if its address happens to be adjacent.

Address lookup on an incoming `Bytes` supplies presentation metadata only. It cannot prove that the
view is the final owner or recover the checkout's accounting provenance. Pool-backed incoming views
therefore remain `Hold::View(Bytes)`; construction never creates another `CarrierGuard` or clears a
bitmap bit from an address.

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
```

`chunk` returns the remaining portion of the front segment. When `remaining` is nonzero, cursor
normalization ensures that `chunk` is nonempty. `chunks_vectored` emits one `IoSlice` per remaining
segment, not per carrier owner or producer frame.

`advance` crosses segment and owner boundaries in order. It drops each `OwnedRange` when this
cursor passes that owner's final byte. Crossing a carrier boundary can therefore return that
carrier before the complete `SegmentedBytes` is consumed. Several owner ranges may share one
carrier guard; dropping one range returns the carrier only when no other range or clone retains it.
`advance` panics if `count` exceeds `remaining`.

Cloning `SegmentedBytes` clones its current cursor state and remaining holds. Each clone advances
independently. Advancing one clone cannot release backing still reachable through another. `len`
and `is_empty` report the state of that clone's cursor.

`into_contiguous` consumes the remaining data:

| Remaining shape  | Result                                  | Pool effect                                         |
| ---------------- | --------------------------------------- | --------------------------------------------------- |
| Empty            | Empty `Bytes`                           | Remaining holds are released                        |
| One segment      | Owner-backed `Bytes` without copying    | Source carriers remain charged until result drop    |
| Several segments | One heap allocation and an ordered copy | This container releases source holds during copying |

The single-segment case moves the remaining owner ranges into a `ContiguousOwner` and constructs
`Bytes::from_owner` over its adjusted pointer and length. The multi-segment case copies through the
`Buf` implementation into one `BytesMut` and freezes it. Copied bytes are ordinary heap memory
outside pool accounting.

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
one independently owned contiguous value use `into_contiguous`.

**Obligations.**

- One carrier checkout creates exactly one `CarrierGuard` and one aggregate charge.
- Final guard drop returns the physical carrier before releasing reservation-local and aggregate
  accounting.
- A carrier guard covers one carrier, never a run or segment.
- Every writable range has one non-cloneable mutable authority.
- `advance_mut` never marks bytes initialized beyond a completed writable range.
- Publication and freeze expose initialized bytes only.
- Publishing a prefix consumes mutable authority over that range and leaves a disjoint suffix.
- Every `PooledWindow` pointer remains within the live carrier retained by its guard.
- `Bytes` clones, slices, and separate windows do not duplicate the carrier charge.
- A carrier returns only after every immutable owner and writable suffix releases its guard.
- Freeze returns wholly unused carriers and retains partially used carriers only through initialized
  ranges.
- Every unconsumed segment range is covered completely by its ordered holds; consumed prefixes are
  never dereferenced.
- Segment coalescing requires a known equal block slot and pointer adjacency.
- Incoming views never authorize bitmap return or accounting recovery.
- `Buf::advance` releases crossed holds and never leaves an empty current chunk with bytes remaining.
- Cloned segmented buffers retain independent cursors and backing ownership.
- Zero-copy contiguous conversion retains source charges; copying releases source carriers and
  moves the result outside pool accounting.
- Unsafe `Send`, `Sync`, and slice construction rely on immutable initialized ranges, stable
  addresses, disjoint mutable authority, and live owner holds.

## Integration

The pool crosses subsystem boundaries in two forms. Transfer-manager code that can present a
`Reservation` uses reserved acquisition. Hyper's receive-buffer contract does not carry that
authority through the SDK, client, connection, and protocol-decoder layers, so its provider uses
unreserved acquisition. Both paths produce the ownership types defined above, so final byte owners
return physical storage and accounting without scheduler participation.

```text
transfer-managed path                         Hyper receive path

poll_work()                                   pool-enabled client
  -> reserve envelope                           -> acquire_unreserved
  -> dispatch Reservation                       -> publish Bytes
  -> acquire(&reservation, min_bytes)            -> collect views
              \                                  /
               +-------> SegmentedBytes <--------+
                             |
                       final owner drops
                             |
                  carrier return, then charge return
```

### Scheduler admission and dispatch

Each transfer reserves its memory envelope from `poll_work()` before returning dispatchable work.
An immediate grant moves the `Reservation` into the `IoRequest` work data. A queued request retains
its `WaitTicket`, returns `PollWork::Pending`, and observes the latched grant or error on a later
poll after the transfer is woken.

```text
poll_work()
  |
  +-- Reservation -----------------> PollWork::Ready(IoRequest)
  |                                         |
  |                                      dispatch
  |
  +-- WaitTicket ------------------> PollWork::Pending
  |                                         |
  |                               grant or failure wakes transfer
  |                                         |
  `---------------------------------- later poll_work()
```

Reservation or preparation failure produces no `IoRequest`. Cancellation removes queued
admission, or consumes an already granted reservation before dispatch. Cancellation after dispatch
stops new acquisition, drops unpublished mutable buffers, and then closes reservation authority.
Immutable bytes already published retain their own carriers and charges.

Execution code holding the work item uses `pool.acquire(&reservation, min_bytes)`. The HTTP
provider is not given the reservation, a derived ticket, or a per-request allocation allowance.
This separation keeps scheduler admission independent of the transport's request and connection
lifecycle.

### Upload staging and retry

An upload that stages part data in the pool acquires `PooledBufMut` through its reservation, fills
the mutable ranges, and freezes the initialized bytes into `SegmentedBytes`:

```text
Reservation
    |
    v
pool.acquire
    |
    v
PooledBufMut -- disk/source fill --> freeze --> SegmentedBytes
                                                   |
                                      +------------+------------+
                                      |                         |
                                 attempt 1 cursor           retry cursor
                                      |                         |
                                      +------ same holds -------+
```

Each SDK attempt receives a fresh body and cursor over the same immutable holds. The retained
`SegmentedBytes` remains live through SDK retries and any transfer-manager retry checkpoint that
can replay the part. `SdkBody::retryable` rebuilds a body for each clone attempt, and
`SdkBody::try_clone` succeeds only when such a rebuild operation exists
([`SdkBody` retry support][sdk-body-retry]).

Retry requires either a source that can be read again or immutable bytes retained through the retry
window. An addressable file or range source can rebuild an attempt by rereading its input. A
forward-only source cannot; it retains the staged `SegmentedBytes` or supplies another replay layer.
Caller-owned `Bytes` and other already-resident upload memory remain outside pool accounting unless
copied into pooled storage.

The body reports its exact remaining length through `size_hint`; segmentation does not make the
length unknown. The SDK body adapter forwards each `Bytes` frame and its size bounds without
requiring one contiguous value ([`SdkBody` HTTP body adapter][sdk-body-http]).

Segmentation does not require a gather copy for checksum calculation, signing, or aws-chunked
framing. The body adapters consume frames in order. A streaming checksum body updates the checksum
per data frame and emits the value in trailers
([checksum body][sdk-checksum-body]). A segmented streaming `SdkBody` therefore selects a different
wire shape from a single in-memory `Bytes` when SDK-owned checksum calculation is enabled: the
checksum is carried in an aws-chunked trailer rather than an HTTP header
([S3 checksum selection][s3-checksum-selection], [aws-chunked selection][s3-chunked-selection]).
The upload path may preserve header placement by calculating the checksum while filling and setting
the header before transmit. Both paths retain a segmented body; neither requires a gather copy.

The upload calls `Reservation::close_acquisition` or drops the reservation after no later source
rebuild or staging operation can perform another reserved acquisition. Replaying an existing
`SegmentedBytes` does not require open acquisition authority. Closing does not release carriers
retained by the retry body.

### Download receive and delivery

A ranged GET reserves its planned payload range before dispatch. The reservation accounts the
planned demand, but it is not passed through the SDK to the HTTP client:

```text
poll_work()                         HTTP client
  -> reserve range                    -> provider.acquire_unreserved
  -> dispatch GET                     -> Frame<Bytes>
          |                                  |
          +---------- collect ---------------+
                         |
                  SegmentedBytes
                         |
              deliver or write to disk
```

The download collector pushes each body `Bytes` into `SegmentedBytes`. Pool-backed frames retain
their existing owner through `Hold::View`; foreign frames retain their own `Bytes` owner.
Address-range lookup may coalesce adjacent presentation ranges, but it never reconstructs a
`CarrierGuard`, transfers a bitmap bit, or recovers reservation provenance from a pointer.

The reservation closes after the response reaches a terminal state and no transfer retry or direct
acquisition can begin. Download output may outlive the reservation, work item, HTTP client, public
pool handle, or transfer manager. Each pooled `Bytes` owner retains its `CarrierGuard` and
`PoolInner` until final drop, so this escape does not require the output type to retain the
reservation.

Error and control bodies have no guessed pre-dispatch allowance. A pool-enabled transport acquires
their storage through the same unreserved path and accounts any demand beyond available coverage.
An unsupported or custom transport may return ordinary foreign `Bytes`; `SegmentedBytes` can carry
those views, but their allocation and release remain outside pool accounting.

### Disk I/O

Upload reads initialize `PooledBufMut`. A read advances initialization only by the byte count
reported complete. Error, cancellation, or short completion leaves the remaining range
uninitialized and unpublishable.

Publishing decoded HTTP payload directly from pooled carriers lets the download sink write the same
storage without an intermediate transport-to-disk staging copy. Download writes consume
`SegmentedBytes` through `Buf`. Vectored writes call `chunks_vectored` and advance the cursor by the
completed byte count, including on short writes:

```text
SegmentedBytes
  -> chunks_vectored
  -> submit [IoSlice A, IoSlice B, ...]
  -> complete N bytes
  -> advance(N)
  -> release every owner crossed by the new cursor
```

The buffer owner and all I/O metadata remain live until synchronous return or asynchronous
completion. Cancellation of completion-based I/O does not release owners until the cancellation or
original operation has produced its terminal completion.

Submission size is independent of carrier size. The I/O path may coalesce already available,
file-offset-contiguous segments into a larger submission, but it does not retain carriers waiting
for a preferred byte count. Buffered vectored writes accept any published segment layout.

Direct I/O adds constraints on the address, length, and file offset of each submitted range.
Page-aligned carrier bases alone do not satisfy those constraints. The sink determines the required
alignment for the selected file and device and classifies each output range before its first write
([`open(2)`][open2], [`statx(2)`][statx2]):

- An aligned dense run may be submitted directly without copying.
- A range that cannot preserve address and file-offset phase uses aligned assembly or buffered I/O.
- A final sub-block tail may use a disjoint buffered write after all direct ranges.

Direct and buffered writes never cover overlapping file pages. The chosen route remains fixed for
the affected range, so page-cache and direct-I/O ownership cannot diverge for the same bytes.

### Hyper transport boundary

Any pool-enabled HTTP transport requires a writable-storage provider that retains the pool while it
can acquire or return storage. The concrete integration targets Hyper. Client construction installs
a provider shared at client or connection scope with a strong `Arc<PoolInner>`. The provider is
request-blind: each request for writable storage calls `acquire_unreserved`, and the provider does
not accept a `Reservation` or transfer-manager type.

Passing reservation authority through request extensions is not part of this contract. Such an
extension could attribute some HTTP/1 receive storage, but it does not provide authority when H2
selects frame storage before stream lookup. A richer upstream contract may add reserved attribution;
the unreserved path remains required wherever storage is selected before that attribution exists.

Using a dedicated transfer-manager client keeps planned demand and request-blind receive demand in
one operational domain. If the same provider-backed client serves independently driven traffic,
that traffic may create unreserved charges and delay managed admission. Configured capacity cannot
bound an open set of callers that continues to issue requests independently of transfer-manager
backpressure.

The memory-side receive contract is:

```text
acquire owned writable storage
    |
initialize a prefix
    |
publish Bytes over that prefix
    |
retain only the disjoint writable suffix
    |
publish again, release the suffix, or terminate
    |
final owner returns the carrier
```

A successful short read retains the writable suffix rather than acquiring another carrier for the
next read. Carrier demand then follows payload capacity instead of socket-read count. A transport
poll that returns `Pending` before initializing any byte may release the untouched buffer; the
registered I/O waker, not buffer ownership, preserves read progress.

The pool's unreserved acquisition is synchronous: it reuses storage, grows prepared capacity, or
returns an allocation error without waiting for another carrier owner. This does not require the
upstream provider API to be synchronous. A poll-based API can return the pool result immediately
and retain the ability to support other providers whose supply is asynchronous.

The Hyper HTTP/1 adapter keeps headers, chunk framing, and trailers in bounded protocol scratch and
publishes decoded payload from body carriers. Bytes read beyond the final header delimiter may
require one bounded copy into the first body carrier. This copy does not require gathering the
remaining body.

HTTP/2 requires a lower receive seam than wrapping Hyper's `Incoming` or `h2::RecvStream`. The H2
codec receives a complete frame in `BytesMut`, then freezes DATA payload before stream lookup and
before Hyper polls it ([frame decode][h2-data-decode], [DATA freeze][h2-data-freeze],
[frame dispatch][h2-frame-dispatch]). Hyper receives the resulting `Bytes` only when it polls the
stream body ([Hyper H2 body][hyper-h2-body]). Direct provider ownership must therefore enter H2
before DATA freeze.

The H2 adapter privately maps Hyper's generic provider to stream-aware receive state. A decoded DATA
frame keeps its writable suffix provisional while `Connection` submits the frame to `Streams`.
Acceptance into the stream's receive queue commits the suffix as reusable stream storage. Rejected,
discarded, or erroneous DATA drops the provisional suffix before control returns or the error
propagates. END_STREAM does not commit a suffix.

A suffix committed by an earlier frame can remain after the application stops receiving.
`RecvStream` drop, local reset, receive-side closure, and stream-store removal therefore record an
idempotent receive-interest closure in `Streams` and wake `Connection`. `Connection` drains those
lifecycle notifications and asks the codec to discard committed writable capacity for the stream
([H2 stream lookup][h2-stream-lookup]). Published payload `Bytes` remain valid through their owners.
H2 flow-control credit is released according to protocol consumption and may precede final `Bytes`
drop ([Hyper flow-control release][hyper-h2-body]). Carrier return remains tied to byte ownership
and is independent of flow-control release.

Completion-native network receive has a different ownership direction: the transport or kernel
selects storage before reporting received bytes. TLS may also place ciphertext below the HTTP
decoder. Such a path requires a pre-received-data or completion-owned transport seam and is not an
implementation of this writable-body provider contract.

**Obligations.**

- `poll_work()` never returns dispatchable work before reservation grant and physical preparation.
- A queued grant or failure is visible to the next transfer poll before its notification runs.
- Cancellation cannot leave direct acquisition authority reachable after reservation close.
- Upload replay retains every immutable source byte through the final attempt that may read it.
- Every upload body reports an exact remaining length and preserves byte order across segments.
- Download collection retains incoming owners without reconstructing physical or accounting
  ownership from addresses.
- Reservation close does not release or invalidate published upload or download bytes.
- Disk completion retains every submitted byte owner until the operation can no longer access it.
- Short writes advance and release exactly the completed prefix.
- Direct and buffered disk writes never target overlapping file pages.
- A provider retains `PoolInner` for every interval in which it may acquire or return storage.
- Transport publication exposes initialized bytes only and preserves disjoint mutable authority
  over any retained suffix.
- Every terminal HTTP path releases unpublished writable storage and leaves published `Bytes` to
  their owners.
- H2 provider state is installed before provider-owned DATA storage is allocated or frozen.
- H2 commits a DATA frame's provisional writable suffix only after `Streams` accepts the payload;
  rejection, discard, error, and END_STREAM drop it.
- H2 receive-interest closure discards committed writable capacity without invalidating published
  payload bytes or coupling flow-control credit to carrier return.

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
```

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
the packed accounting representation.

The packed coverage state stores each component in `u32`. With a minimum 4 KiB carrier,
`u32::MAX` carriers represent almost 16 TiB. Configuration and every envelope conversion check this
limit before changing admission state. Byte conversion for public reporting is also checked.

A configured-capacity update uses the same validation:

- An increase changes the normal admission ceiling and reconsiders the FIFO. Each resulting grant
  still prepares its post-grant requirement before publication.
- A decrease is soft. It revokes no reservation or carrier and admits no ordinary work until
  `admission_used` falls within the new ceiling.
- Reclamation uses the new configured block ceiling on its next valid idle epoch. A decrease does
  not synchronously trim on the caller's thread.

**Alternative: Apply a fixed minimum capacity.** Rejected: a 512 MiB minimum consumes all detected
memory in a 512 MiB container and contradicts the required process headroom. Idle-only admission
already permits progress for an envelope larger than configured capacity.

### Geometry and startup

One pool uses one carrier class. Geometry is internal:

```rust
struct PoolGeometry {
    page_size: NonZeroUsize,
    carrier_size: NonZeroUsize,
    carriers_per_block: NonZeroUsize,
}
```

Construction validates that carrier size is a multiple of runtime page size, block size is a
checked whole-carrier multiple, bitmap and valid-mask lengths represent every carrier exactly, and
the resulting address and byte calculations cannot overflow. Configured capacity need not be a
whole number of blocks. Whole-block preparation can therefore make `prepared_capacity` exceed
configured capacity without changing admission.

Carrier size, carriers per block, optimistic scan effort, and reclaim intervals are implementation
parameters selected by measurement. They are not public tuning requirements. Their values are
reported at pool construction so an operational record identifies the geometry in use.

The pool starts without prepared blocks. Construction establishes policy, geometry, accounting,
and maintenance state; growth reserves stable virtual ranges and prepares backing when admission or
acquisition first requires capacity. Initial prewarming and external memory-pressure monitoring are
not part of the baseline.

### Maintenance coordinator

Reclamation and cleanup recovery run on a lazy pool-owned thread rather than an async runtime
worker:

```rust
struct MaintenanceCoordinator {
    state: Mutex<MaintenanceState>,
    wake: Condvar,
    thread: OnceLock<JoinHandle<()>>,
}

struct MaintenanceState {
    activity_epoch: u64,
    idle_deadline: Option<IdleDeadline>,
    reclaim_requested: bool,
    cleanup_requested: bool,
    stopping: bool,
    disabled: bool,
}

struct IdleDeadline {
    epoch: u64,
    expires_at: Instant,
}
```

The scheduler's global-idle transition arms a deadline for the current activity epoch. New managed
work increments the epoch and invalidates that deadline. A stale deadline cannot initiate
reclamation. The timeout supplies cache hysteresis; it does not participate in allocator safety.

At a valid deadline, the coordinator applies the target and eligibility rules under
[Reclamation policy](#reclamation-policy). If live carriers or compatibility prevent the target,
the coordinator keeps reclaim intent and retries after a bounded delay while the epoch remains
current. Carrier return does not signal the coordinator. A later scan observes newly free blocks.

Cleanup retry is independent of global idle. A failed whole-range protection operation leaves its
block in `ProtectionPending`; a failed discard records `reclaim_pending` on an inactive block. The
coordinator retries those block-local operations without making either block claimable. Preparation
serializes with retry, so delayed cleanup cannot affect a revived block.

The first deadline or cleanup request starts the thread. The thread owns a `Weak<PoolInner>` and
does not keep the pool alive. It releases the coordinator mutex before entering admission, scanning
blocks, changing protection, or discarding backing. A thread-creation failure marks maintenance
disabled and reports degraded reclamation. Admission, acquisition, and owner return remain valid;
free prepared capacity may remain resident until pool destruction.

Exact idle timeout and retry cadence are measurement parameters. They are constants rather than
public controls.

### Shutdown and destruction

Closing the transfer manager closes admission before stopping maintenance:

```text
close admission
  -> reject new reservations
  -> fail queued waiters and publish their terminal results
  -> invoke notifications after releasing admission serialization
  -> stop and join the maintenance thread
  -> drop manager-owned pool and provider handles
```

Admission close does not revoke a granted reservation. In-flight work may continue reserved
acquisition until it closes or drops that reservation. A provider that still owns
`Arc<PoolInner>` may complete unreserved receive work until the provider shuts down. Existing
carrier guards remain valid and continue to return physical storage and accounting.

Maintenance shutdown cancels idle deadlines and cleanup retries. It does not need to trim prepared
capacity or recover every block before exit. Blocks outside prepared capacity remain unavailable;
the final pool destructor releases their stable virtual ranges with the rest of the arena.

`ReservationState`, transport providers, and carrier guards retain `Arc<PoolInner>`. The maintenance
thread retains only `Weak<PoolInner>`. `PoolInner` destruction can therefore begin only after no
reservation, provider, mutable buffer, or immutable byte owner can access pool storage. Final
destruction releases every slot's reserved virtual range and requires no scheduler or runtime
participation.

### Failure containment

Recoverable resource failures remain local:

| Failure                                             | Result                                                                                   |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Invalid capacity, geometry, or counter range        | Pool construction or capacity update fails before publishing new state                   |
| Virtual-range reservation                           | That growth or acquisition attempt fails                                                 |
| Reservation preparation                             | That request fails; reusable capacity prepared before the failure remains available      |
| Reserved or unreserved acquisition                  | The complete debit rolls back; no partial writable buffer escapes                        |
| Capability-specific preparation                     | That incompatible acquisition fails; unrelated domains remain usable                     |
| Whole-range protection                              | The affected block enters `ProtectionPending` outside prepared capacity                  |
| Backing discard                                     | The inactive block records `reclaim_pending`; other blocks remain usable                 |
| Maintenance-thread creation                         | Maintenance is disabled; ordinary paths continue and affected blocks stay unavailable    |
| Ownership, identity, or address-reservation check   | The non-returning fail-stop handler aborts without continuing allocator mutation         |

The pool does not allocate unaccounted fallback memory after a pool acquisition fails. Temporary
growth remains pool-owned and charged before writable access. A transport can fall back to its
ordinary allocator only when its provider contract explicitly permits leaving pool accounting.

Anonymous mapping and protection success do not guarantee that every later first-touch fault will
succeed under operating-system overcommit or container pressure. Automatic sizing leaves
headroom, but the pool cannot convert a process-level out-of-memory kill into `AcquireError`.

### Observability

The public snapshot reports stable operational concepts rather than ledger fields or block
lifecycle details:

```rust
#[derive(Debug, Clone)]
pub struct MemoryPoolSnapshot {
    // Private representation.
}

impl MemoryPoolSnapshot {
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
    pub fn prepared_capacity_bytes(&self) -> u64;

    /// Reservation requests retained in FIFO order.
    pub fn queued_reservations(&self) -> usize;

    /// Cumulative reservation requests that entered the FIFO.
    pub fn total_parked(&self) -> u64;
}
```

The representation and fields remain private. Adding a signal adds a getter; callers cannot
construct or destructure snapshots. The API does not expose `available_coverage`,
`uncovered_charges`, bitmap population, block states, or scan hints. Those are implementation
details rather than durable user contracts.

Snapshot collection takes admission serialization and loads the packed coverage state once.
Configured capacity, planned demand, charged capacity, overage, prepared capacity, and queue state
therefore form one coherent admission sample. It does not scan live bitmaps or add a carrier-return
counter to the common path. Values are carrier-rounded bytes and exclude foreign `Bytes`, protocol
scratch, copied contiguous output, and other process memory.

A crate-private diagnostic snapshot includes raw accounting fields, bitmap population, block
lifecycle counts, pending cleanup, and scan-path counters for tests and tracing. It is not a public
compatibility surface.

Operational signals are organized by the failure they diagnose:

| Symptom                                      | Signals                                                                    | Interpretation                                                               |
| -------------------------------------------- | -------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Managed work remains queued                  | configured, admission used, queue depth, total parked                      | Admission is binding or an older FIFO request is ineligible                  |
| Admission remains above configured capacity  | admission overage, planned demand, charged capacity                        | Idle-only admission or ownership outside active planned demand remains live  |
| Memory remains prepared after global idle    | prepared capacity, charged capacity, reclaim retries                       | Retention floor, live owners, compatibility, or cleanup prevents reclamation |
| Reuse repeatedly enters serialized fallback  | serialized-acquisition count, prepared capacity, geometry                  | Optimistic scan work or placement is missing reusable capacity               |
| Capacity repeatedly grows and reclaims       | blocks prepared, blocks reclaimed, idle epochs                             | Idle timeout or retention target is causing cache churn                      |
| A block remains unavailable                  | protection-pending and reclaim-pending capacity, retry and failure counts  | Platform cleanup is degraded but isolated to named blocks                    |
| Allocation or preparation fails              | operation error, requested capacity, prepared capacity, platform error     | The caller encountered a resource or compatibility failure                   |

Normal reservation transitions are `trace`. Carrier-frequency successful acquisition and return
require no event. Reservation parking and serialized fallback are `trace` with monotonic diagnostic
counters. Block preparation, idle deadline changes, and successful reclamation are `debug`.
Allocation, protection, discard, maintenance-thread, and fail-stop precursor events are `warn` or
higher. Rare failure records include the operation, requested capacity, block identity where
applicable, platform error, and resulting pool state.

Snapshots are pull-only, and event records are emitted by existing paths. The pool starts no
reporting task and maintains no rate window. Diagnostic counters saturate at `u64::MAX` and never
participate in control decisions.

**Obligations.**

- Configuration validation completes before publishing capacity or geometry.
- Capacity decrease revokes no reservation, debit, carrier, or byte owner.
- A stale idle epoch cannot initiate or continue policy reclamation.
- Maintenance never holds its state mutex while entering pool or platform operations.
- Maintenance ownership cannot keep `PoolInner` alive.
- Maintenance failure cannot invalidate admission, acquisition, or final-owner return.
- Admission close publishes every queued terminal result before notification.
- Shutdown preserves all granted authority and owner return paths until their final drop.
- Public snapshot values retain their documented meaning independently of ledger representation.
- Rare resource failures and degraded reclamation produce an operation result or diagnostic signal.

## Correctness invariants

The local obligations above constrain individual mechanisms. The following properties constrain
their composition across admission, physical storage, ownership, integration, and shutdown.

### A live carrier is charged and prepared

At each public-operation completion and each admission or reclamation decision:

```text
physical_live
    <= outstanding_charges
    <= admission_used
    <= prepared_capacity
```

Acquisition installs the complete aggregate debit before claiming storage. Preparation reaches the
post-transition `admission_used` before a grant completes, an uncovered acquisition exposes
writable memory, or admission serialization is released. Final owner return clears the physical
bit before releasing its charge. Trim preserves the admission floor before removing prepared
capacity.

This prevents writable memory from escaping accounting, admission from relying on inaccessible
storage, and accounting release from admitting work before the corresponding carrier is reusable.
Configured capacity is absent from the chain because idle-only admission and uncovered acquisition
may exceed it.

### Admitted work does not wait for another owner

A grant prepares capacity through its resulting `admission_used`. Reserved and unreserved
acquisition first search compatible reusable storage, then serialize an exhaustive recheck and
compatible growth. They return the complete requested carrier batch or an error. They do not wait
for another carrier owner to return memory.

This prevents admitted work from occupying execution capacity while waiting for memory held by work
that cannot run. The guarantee excludes mutex scheduling and operating-system mapping, placement,
registration, or commit latency. Failure to obtain compatible physical storage remains an explicit
operation error.

### Admission preserves order and idle progress

A queued reservation request cannot be bypassed by a fresh request or a later waiter. Capacity
released for the FIFO is transferred directly into the head waiter's terminal result before its
notification runs. Cancellation produces either one caller-owned grant or one retired grant, never
both.

When no reservation retains active planned demand, the FIFO head may receive an idle-only grant
even when its envelope exceeds configured capacity or uncovered charges prevent a normal grant.
That grant makes active planned demand nonzero, so a second idle-only grant cannot compound the
overshoot. Progress requires earlier owners to return and physical preparation to succeed; FIFO
order does not impose a time bound on either.

### Reservation close does not release owned bytes

Closing a reservation consumes future direct-acquisition authority and removes its complete
envelope from active planned demand. Charges held by mutable buffers, immutable views, in-flight
I/O, and in-flight acquisition debits remain accounted. Close reclassifies the occupied portion of
the envelope as `uncovered_charges`, which remain until the charges return. A later grant adds a new
envelope without absorbing them.

This separates planned work lifetime from byte lifetime. Transfer completion, retry completion, and
reservation close cannot invalidate data retained by a caller, transport, or I/O operation.

### Published bytes never alias writable memory

Each carrier has one non-cloneable mutable authority. Publication consumes authority over an
initialized prefix and may retain authority only over a disjoint suffix. Immutable views, their
clones, and segmented containers retain the carrier guard but cannot recover mutable access to pool
storage.

Only initialized ranges become `Bytes`, `&[u8]`, `IoSlice`, or outgoing I/O sources. Every pointer
used by such a range remains within one stable block range and is retained by an owner that prevents
carrier return. This prevents mutable aliasing, uninitialized reads, and use after reclamation
across every delivery form.

### Reclamation cannot overtake ownership

A valid set bit has exactly one provisional or committed physical owner. The claim-trim gate
ensures that a claim either keeps its bit while trim abandons or rolls back through its protected
incarnation while trim proceeds. Revival creates a fresh bitmap, so stale rollback cannot mutate a
new activation.

Trim removes only an all-free block, preserves the admission floor, makes the complete stable range
inaccessible, and retains exclusive address ownership. Protection or discard failure leaves the
affected block nonclaimable; it does not weaken the ownership of another block or carrier.

### Release survives shutdown

Closing admission fails queued work but does not revoke granted reservations, provider access,
carrier guards, or byte owners. Those objects retain `PoolInner` and can complete acquisition,
rollback, and final return after manager-owned handles and maintenance have stopped. Final
destruction begins only after no such owner remains.

Recoverable allocation and cleanup failures expose no partially owned writable buffer. A detected
ownership, incarnation, or stable-address violation enters the non-returning fail-stop path before
clearing an unexpected bit or releasing its charge.

## Open Questions

### Carrier and block geometry

Carrier size and carriers per block are measurement choices. Carrier size must be a multiple of the
runtime page size. Smaller carriers reduce small-object and tail waste but increase bitmap
operations, ownership transitions, scatter width, and carrier returns.

Block size controls mapping amortization, all-free scan length, reclaim granularity, and how much
capacity one long-lived carrier can keep prepared. Geometry selection must account for small
objects, large multipart transfers, mixed upload and download traffic, intended core counts, and
aggregate multi-NIC rates beyond 600 Gbps.

These choices do not change the stable-slot, fresh-incarnation, batched-fallback, or ownership
contracts.

### Scan and reclamation constants

The optimistic scan budget, idle timeout, and cleanup retry cadence are selected by measurement.
The scan budget trades lock-free work against entry into serialized fallback. The idle timeout
trades retained RSS against repeated preparation across traffic bursts. Retry cadence trades
cleanup latency against repeated platform calls while a block remains unavailable.

Measurements must include mixed object sizes, burst and sustained traffic, idle gaps, allocation
failure, high core counts, and aggregate network rates above 600 Gbps. These constants do not change
the exhaustive serialized fallback, idle-epoch invalidation, or fail-closed cleanup contracts.

### Carrier rounding across incremental acquisition

Admission and carrier charges use whole carriers. Repeated independent acquisitions can therefore
consume more direct authority than rounding the complete logical extent once. With eight-byte
carriers, a 100-byte extent requires thirteen carriers, while five independent
`acquire(min_bytes = 20)` calls require fifteen. Under the direct-authority limit above, the fifth
call fails with `ReservationCapacityExceeded`; it does not automatically create an uncovered
charge.

An acquired carrier's unused suffix remains available only through the `PooledBufMut` that owns its
exclusive mutable range. A later independent pool acquisition cannot use that suffix. Publishing
bytes from a partially filled carrier can therefore pin unused capacity until the carrier's final
view drops.

The acquisition contract must select one of these directions:

- Reservation planning sums the carrier-rounded size of every independently acquired buffer.
- Direct acquisition beyond the planned carrier envelope succeeds with an accounted uncovered
  charge.
- Mutable buffers retain and extend writable tails so incremental fills incur carrier rounding once
  per buffer rather than once per acquisition call.

The first two directions preserve the fixed-capacity `PooledBufMut` contract stated under
Architecture. Retaining and extending writable tails would require changing that contract and the
acquisition API. None of the directions requires per-byte carrier charges or recovery of carrier
ownership from immutable views.

## Future Work

### Asynchronous preparation

Eager page placement, memory locking, and device registration can make synchronous preparation too
expensive for reservation or acquisition. A preparer may move those operations to a dedicated
actor while retaining the same transition: accounting authority is published only after compatible
capacity reaches `prepared_capacity`.

Acquisition still cannot wait for another carrier owner. Any wait introduced by asynchronous
preparation must depend only on the preparer's progress and must terminate with prepared capacity
or an explicit preparation error.

### Registered and topology-specific storage

The block compatibility key can identify fixed-buffer registration, provided-buffer ownership,
NUMA placement, or another hard storage domain. Registration and completion ownership are hard
constraints; NUMA locality remains a preference unless an integration requires otherwise.

A hard domain can have free capacity while the FIFO head requires another domain. Adding such
domains requires an explicit admission policy: preserve one global FIFO and permit incompatible
capacity to idle, or partition admission and define fairness across partitions. Static direction
quotas remain unnecessary.

### Reservation-aware transport acquisition

A transport contract may carry optional reservation authority to its writable-buffer provider.
Payload acquisition attributable to one transfer can then use `acquire` rather than
`acquire_unreserved`. Protocol scratch, connection-level state, reads selected before attribution,
and integrations without that contract continue to require unreserved acquisition.

The accounting model does not depend on this extension. Both paths consume the same aggregate
coverage, and transport attribution cannot recover ownership from an already published byte view.

### TLS and completion-owned receive

A completion-native network path selects or transfers buffer ownership before reporting received
bytes. TLS may retain and mutate encrypted input before publishing disjoint plaintext. Supporting
that path requires an owned-input contract through TLS and a pre-received-data or completion-owned
HTTP transport seam.

Stable carrier addresses, initialized-range publication, and hard compatibility keys preserve the
required ownership model. The readiness-oriented writable provider remains a separate transport
mode; completion ownership does not fit its borrowed writable-tail contract.

### External pressure

An operating-system or container pressure signal may arm reclamation before scheduler-global idle.
Such a signal changes when reclamation is requested, not block eligibility, the admission floor,
the idle retention target, or claim-trim safety. A pressure producer must remain optional because
portable process-level pressure notification is not available on every target.

### Shared admission domains

Several transfer managers or independently driven clients may share one pool only when they also
share admission policy and uncovered-charge bounds. A shared arena without shared admission lets
one consumer create ownership that another consumer cannot control or predict.

The stable owner and return model permits a wider domain, but configuration, shutdown, fairness,
and operational attribution require a domain-level contract rather than independent manager
policies over one arena.

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
active planned demand, uncovered charges, prepared capacity, the FIFO, or shutdown state hold
admission serialization. A fully covered debit and a coverage-only return use the packed atomic
without that lock.

### Reservation transitions

| Transition          | Preconditions                                                                 | State change                                                                                                  | Follow-up                                        |
| ------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| Normal grant        | FIFO empty or request is head; `admission_used + envelope <= configured`      | Prepare to the post-grant floor; add `envelope` to planned demand and available coverage                      | Publish one open `Reservation`                   |
| Idle-only grant     | FIFO empty or request is head; `active_planned_demand == 0`                   | Same as normal grant                                                                                          | Publish one open `Reservation`                   |
| Queue               | Request is not immediately eligible                                           | Append one `Queued` waiter; accounting is unchanged                                                           | Retain notification and cancellation state       |
| Grant queued head   | Head becomes eligible                                                         | Apply normal or idle-only grant, then replace `Queued` with `Granted`                                         | Notify after admission unlock                    |
| Preparation failure | Head is selected but post-grant preparation fails                             | Planned demand and aggregate coverage are unchanged; capacity prepared before failure remains prepared        | Store `Failed`, continue FIFO drain, then notify |
| Cancel queued       | Wait slot remains `Queued`                                                    | Remove the waiter; accounting is unchanged                                                                    | Reconsider the new head                          |
| Cancel granted      | Wait slot contains an untaken `Reservation`                                   | Consume and close that reservation exactly once                                                               | Reconsider the FIFO                              |
| Close admission     | Pool is open                                                                  | Mark closed and replace every queued slot with `Failed(Closed)`; live accounting is unchanged                 | Notify after admission unlock                    |

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

A direct acquisition first reserves `N` units of reservation-local authority and rejects the
request when the reservation is closed or `direct_outstanding + N > envelope`. An unreserved
acquisition has no local-authority transition. If `N - covered` is nonzero, admission prepares to
the new `admission_used` before physical acquisition can publish writable memory.

The aggregate debit owns all `N` charges until each charge moves into one carrier guard. Physical
claim is all-or-error at the API boundary. Failure returns completed physical carriers, rolls back
provisional bits, releases every uncommitted charge, and restores direct authority before returning
an error.

Aggregate rollback and final return use the same accounting transition for count `N`:

```text
repaid = min(N, uncovered_charges)

uncovered_charges -= repaid
available_coverage += N - repaid
```

The physical carriers are returned before this transition. A direct return also decrements
`direct_outstanding`; it restores local authority only while the reservation remains open.

### Reservation close

Closing an envelope `E` consumes its public authority exactly once:

```text
unused_removed = min(E, available_coverage)
occupied_reclassified = E - unused_removed

active_planned_demand -= E
available_coverage -= unused_removed
uncovered_charges += occupied_reclassified
```

Close creates no owner and removes no charge. It withdraws unused planned demand and leaves
occupied demand represented as uncovered charges. Close racing final return produces the same
state in either linearization order because each operation changes available coverage and uncovered
charges as one packed transition.

### Prepared-capacity transitions

| Transition                 | Preconditions                                                       | Ordering                                                                                              |
| -------------------------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Prepare or revive block    | Stable range retained; target preparation succeeds                  | Establish writable mapping, add block to `prepared_capacity`, then publish fresh `Active` incarnation |
| Failed preparation         | Any target operation fails                                          | Publish no `Active` incarnation; retain earlier successful preparation                                |
| Abandon trim               | Claim-trim gate observes any valid set bit                          | Restore `Active`; leave `prepared_capacity` unchanged                                                 |
| Confirm trim               | Gate confirms all valid bits clear and admission floor is preserved | Subtract block from `prepared_capacity`, then perform physical cleanup                                |
| Failed deactivation        | Whole-range inaccessible transition fails                           | Keep block nonclaimable in `ProtectionPending` outside prepared capacity                              |
| Failed discard             | Range is inaccessible but backing discard fails                     | Keep block inactive with `reclaim_pending`; prepared capacity is unchanged                            |

The admission floor is checked against the post-removal count:

```text
prepared_capacity_after >= admission_used
```

## Appendix B: Memory ordering

The orderings below are minimum requirements. A stronger ordering is correct but adds no new
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

Ticket cancellation releases `WaitSlot` before entering `AdmissionState`. Carrier return clears
physical ownership and releases any block protection before entering admission accounting.
Maintenance releases `MaintenanceState` before entering admission, arena, block, or platform
operations. Notification callbacks run with none of these locks held.

### Claim-trim gate

The gate requires one sequentially consistent order over its store-load pairs:

| Side  | First access                                      | Second access                                  |
| ----- | ------------------------------------------------- | ---------------------------------------------- |
| Claim | Set candidate bits with `fetch_or(SeqCst)`        | Load incarnation state with `SeqCst`           |
| Trim  | Change `Active` to `Draining` with `SeqCst`       | Load every valid bitmap word with `SeqCst`     |

Acquire/release ordering is insufficient because each load may otherwise observe the state before
the other side's store. A full `SeqCst` fence between each store and load is equivalent when the
implementation preserves one global order over the four accesses.

Gate-failure rollback clears only bits won in the protected old incarnation. Post-gate rollback and
final return clear only recorded live bits in the current incarnation. Those clears require
`Release` or stronger ordering; they do not form another store-load gate. A bitmap scan used only
as a search hint may use `Relaxed`; ownership is decided by the gated `fetch_or`.

### Publication and accounting

| State                                  | Operation                                      | Minimum ordering or synchronization                                       |
| -------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------- |
| Packed coverage and uncovered charges  | Load or failed CAS retry                       | `Acquire`                                                                 |
| Packed coverage and uncovered charges  | Successful debit, grant, close, or return CAS  | `AcqRel`                                                                  |
| Reservation owner state                | Direct debit, close, rollback, or return       | One packed `AcqRel` read-modify-write                                     |
| Block registry                         | Publish or load immutable registry snapshot    | ArcSwap publication and guard contract                                    |
| Current block incarnation              | Publish, replace, or protect incarnation       | ArcSwap publication and guard contract                                    |
| Wait result                            | Store terminal result and consume it           | `WaitSlot` mutex                                                          |
| Admission ledger and FIFO              | Every read and write                           | `AdmissionState` mutex                                                    |
| Mapping and cleanup state              | Every transition                               | Block mapping/lifecycle mutex                                             |
| Maintenance epoch and deadlines        | Every read and write                           | `MaintenanceState` mutex and condition-variable protocol                  |

The packed accounting CAS that releases a charge is ordered after physical bitmap return. A grant
or snapshot that observes the accounting release therefore cannot precede the physical return that
made the carrier reusable.

Prepared-capacity accounting precedes `Active` publication. Deactivation follows `Draining`,
all-free confirmation, and prepared-capacity removal. Immutable byte publication requires no
additional allocator fence: consuming exclusive mutable authority and transferring initialized
ranges into synchronized owners establishes the Rust ownership boundary.

## Appendix C: Safety and platform obligations

Every unsafe operation must name the owner that keeps its address valid, the range that is
initialized, and the synchronization that excludes mutation or reclamation.

### Unsafe surface

| Operation                               | Required precondition                                                                                                      |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Reserve a virtual block range           | The complete page-aligned range becomes exclusively owned by one slot until final pool destruction                         |
| Compute a carrier address               | Checked index and size arithmetic remain within the stable range; the claim passed the `Active` gate                       |
| Expose a writable range                 | One linear `ExclusiveRange` owns the complete range; no immutable view overlaps it                                         |
| Advance initialized length              | A completed write initialized every advanced byte and did not exceed the exposed writable ranges                           |
| Construct `PooledWindow` or `Bytes`     | The range is initialized, immutable, within one live carrier, and retained by its carrier guard                            |
| Construct `ContiguousOwner`             | Ordered owners cover every byte in the contiguous range for the complete owner lifetime                                    |
| Form `&[u8]`, `IoSlice`, or I/O source  | The referenced range is initialized and retained for the complete borrow or asynchronous operation                         |
| Implement `Send` or `Sync`              | Moving or sharing the type cannot duplicate mutable authority; every shared pointer names immutable synchronized storage   |
| Coalesce adjacent presentation ranges   | Both ranges have the same known stable slot and pointer adjacency; retained owners cover the merged range                  |
| Make a block inaccessible or discard it | The claim-trim gate confirmed all-free, prepared accounting no longer includes it, and the slot retains address ownership  |

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

| Target  | Stable reservation                         | Prepare                                      | Deactivate                   | Discard                 |
| ------- | ------------------------------------------ | -------------------------------------------- | ---------------------------- | ----------------------- |
| Linux   | Anonymous private `PROT_NONE` mapping      | Whole-range `mprotect(READ \| WRITE)`        | Whole-range `mprotect(NONE)` | `MADV_DONTNEED`         |
| macOS   | Anonymous private `PROT_NONE` mapping      | Whole-range `mprotect(READ \| WRITE)`        | Whole-range `mprotect(NONE)` | `MADV_FREE`             |
| Windows | `VirtualAlloc(MEM_RESERVE, PAGE_NOACCESS)` | `VirtualAlloc(MEM_COMMIT, PAGE_READWRITE)`   | `VirtualFree(MEM_DECOMMIT)`  | Same decommit operation |

All targets must preserve exclusive ownership of the virtual range after every successful or
failed operation. A failed protection or commit operation enters `ProtectionPending`; no prior
protection is assumed. Recovery requires a later successful whole-range transition before the block
can reenter prepared capacity.

Discard runs only after the range is inaccessible. Failure may retain backing but cannot make the
block claimable. Retry and preparation serialize so a delayed discard cannot affect a revived
block. Registered, wired, mixed-policy, or completion-owned blocks remain trim-ineligible until
their capability owner completes teardown.

Qualification for each supported target includes runtime page and allocation geometry, stable
address ownership, successful prepare/deactivate/revive, commit or overcommit failure, discard
failure, and process destruction with escaped owners already absent. RSS or working-set reduction
is an operational observation, not proof of address ownership or inaccessibility.

## Appendix D: Verification

Verification targets the mechanism that establishes each property. Model checking covers bounded
concurrent state machines. Miri covers raw-pointer and initialization rules. Property and
failure-injection tests cover arithmetic, ownership, rollback, and platform result handling.

### Admission and accounting

| Property                                                                 | Evidence                                                | Negative control                                                      |
| ------------------------------------------------------------------------ | ------------------------------------------------------- | --------------------------------------------------------------------- |
| Debit, close, and return preserve aggregate charges in every order       | Loom over the packed state and reservation close        | Split coverage and uncovered charges into independent atomics         |
| A grant never absorbs an existing uncovered charge                       | Transition property test with repeated grant and close  | Recompute uncovered charges from the new envelope                     |
| Grant, cancellation, and take produce one terminal waiter result         | Loom over FIFO and wait slot                            | Remove direct grant transfer and wake callers to race                 |
| Callback reentry observes the terminal result without lock nesting       | Loom with callback reentry                              | Invoke the callback before publication or while admission is locked   |
| Idle-only admission grants at most one request at a time                 | State-machine property test                             | Gate idle escape on configured headroom instead of planned demand     |
| Physical return precedes a newly eligible waiter's acquisition           | Composed pool-level Loom model                          | Release accounting before clearing the physical bit                   |
| Partial acquisition failure restores every debit and direct authority    | Failure injection after each claim and conversion step  | Drop the aggregate debit before provisional and completed carriers    |
| Count conversion and packed lanes never overflow                         | Boundary property tests over byte and carrier counts    | Remove one checked conversion                                         |

### Physical storage

| Property                                                                  | Evidence                                                 | Negative control                                                      |
| ------------------------------------------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------- |
| Claim and trim cannot both pass their gate                                | Loom over one claim and one trim                         | Weaken either store-load pair to acquire/release                      |
| Stale rollback cannot clear a revived carrier                             | Loom over claim, trim, revival, and new claim            | Reset and reuse one bitmap across activations                         |
| Gate-failure rollback writes only through the protected incarnation       | Loom with removal of `current` before rollback           | Reacquire `current` for stale rollback                                |
| Post-gate batch rollback and final return clear exactly owned bits        | Loom plus multiword property tests                       | Clear the candidate mask instead of won bits                          |
| Padding bits never become carriers or affect all-free                     | Property tests around every final-word width             | Omit `valid_masks` from claim or trim                                 |
| Serialized fallback exhausts compatible capacity before growth            | Deterministic fragmented-registry tests                  | Grow after a bounded optimistic miss without exhaustive recheck       |
| Fresh growth reserves the fallback claimant's carriers before publication | Concurrent fallback and fast-claim test                  | Publish the free incarnation before preclaiming the batch             |
| Protection and discard failure leave the block nonclaimable               | Failure injection at each mapping transition             | Restore `Active` from a failed platform call                          |
| Shutdown with escaped owners preserves final return                       | Composed Loom model with queue, reservation, and `Bytes` | Drop pool state when manager-owned handles disappear                  |

### Ownership and delivery

| Property                                                                 | Evidence                                                | Negative control                                                      |
| ------------------------------------------------------------------------ | ------------------------------------------------------- | --------------------------------------------------------------------- |
| Only initialized bytes become safe references or immutable owners        | Miri over partial writes, publication, and freeze       | Freeze acquired capacity instead of initialized length                |
| Mutable publication leaves a disjoint writable suffix                    | Miri over repeated `publish_prefix` and writes          | Retain mutable authority over the published prefix                    |
| One carrier returns once after its final view, slice, and suffix drop    | Guard-level Loom and arbitrary drop-order tests         | Create one return guard per view or per run                           |
| Segment owners cover every unconsumed byte                               | Property tests over builder input and cursor movement   | Merge unknown or nonadjacent ranges                                   |
| `Buf` never reports an empty chunk while bytes remain                    | Generic-consumer property test                          | Leave the cursor on an exhausted segment boundary                     |
| Advancing one clone cannot release another clone's backing               | Clone and partial-consumption property test             | Share cursor state without sharing owner holds                        |
| Contiguous conversion preserves bytes and ownership semantics            | Shape and byte-equivalence property tests               | Release source owners before constructing the zero-copy result        |
| Partial vectored I/O advances exactly the completed prefix               | Miri and property tests over every completion boundary  | Advance by submitted rather than completed length                     |

### Integration and operations

| Property                                                                 | Evidence                                                                  |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `poll_work()` dispatches only a granted, prepared reservation            | Scheduler integration test with parking and cancellation                  |
| Upload retry retains exact bytes through the final consuming attempt     | Retry tests with partial body polling and source failure                  |
| Download and disk completion retain owners through final access          | Integration tests with partial reads, writes, and cancellation            |
| Every terminal HTTP path releases unpublished capacity                   | H1 and H2 lifecycle tests for EOF, reset, rejection, and body drop        |
| A stale idle epoch cannot reclaim after new managed work                 | Maintenance state-machine test with deadline races                        |
| Cleanup retry cannot race preparation or revived access                  | Concurrency test with injected protection and discard failures            |
| Capacity detection honors process and container limits                   | Platform tests for physical memory, cgroup limits, and explicit overrides |
| Supported mapping backends preserve the platform contract                | Native probe matrix for Linux, macOS, and Windows                         |
| Public snapshots report one coherent admission sample                    | Concurrent snapshot property test                                         |

Benchmarks select carrier size, block size, optimistic scan work, idle timing, and retry cadence.
They cover small-object waste, scatter width, allocation churn, packed-state contention,
fragmented reuse, idle burst recovery, high core counts, and aggregate rates above 600 Gbps.
Benchmark results tune constants; they do not weaken ownership, accounting, fallback, or
reclamation invariants.

[dekker]: https://en.wikipedia.org/wiki/Dekker%27s_algorithm
[bytes-buf]: https://docs.rs/bytes/latest/bytes/trait.Buf.html
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
[h2-data-decode]: https://github.com/hyperium/h2/blob/46bfd629f8167adbb542dfcaf5378690dada997a/src/codec/framed_read.rs#L119-L145
[h2-data-freeze]: https://github.com/hyperium/h2/blob/46bfd629f8167adbb542dfcaf5378690dada997a/src/codec/framed_read.rs#L235-L244
[h2-frame-dispatch]: https://github.com/hyperium/h2/blob/46bfd629f8167adbb542dfcaf5378690dada997a/src/proto/connection.rs#L318-L365
[h2-stream-lookup]: https://github.com/hyperium/h2/blob/46bfd629f8167adbb542dfcaf5378690dada997a/src/proto/streams/streams.rs#L544-L609
[hyper-h2-body]: https://github.com/hyperium/hyper/blob/116a9dfec5e38bd77993dac9b61520a29d002321/src/body/incoming.rs#L235-L249
