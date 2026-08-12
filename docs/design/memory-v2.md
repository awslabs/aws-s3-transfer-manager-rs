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

A request larger than configured capacity, or blocked only by unreserved overage, runs once no other
managed demand remains. Idle-only admission applies to one managed request at a time under either
condition. Progress assumes that earlier owners eventually return memory and required physical
allocation succeeds.

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
    +-- AdmissionState
    |   `-- admission accounting, FIFO waiters, shutdown
    `-- Arena
        `-- physical storage, carrier acquisition, reuse, reclamation
```

`AdmissionState` decides whether planned demand may be dispatched and accounts capacity until its
final owner returns it. `Arena` owns mappings and carrier state but does not grant admission.
A reservation grant prepares capacity for the admitted total.

The core types have the following contracts:

| Type | Contract |
|---|---|
| `BufferPool` | Cloneable handle to one admission and physical-storage domain |
| `PoolInner` | Shared state retained by pool handles, reservation state, waiter handles, and carrier guards |
| `AdmissionState` | Admission, accounting, queue, cancellation, and shutdown state |
| `Arena` | Physical preparation, acquisition, reuse, placement, and reclamation |
| `Reservation` | Linear authority over one admitted envelope; close consumes it and ends direct acquisition |
| `ReservationState` | Private state retained by carriers acquired through a reservation |
| `WaitTicket` | Cancellable handle to one queued reservation request and its eventual result |
| `Carrier` | The fixed-size, page-granular allocation unit; also the physical ownership and accounting unit |
| `CarrierGuard` | Final-return owner for one acquired carrier, its aggregate charge, and optional reservation state |
| `PooledBufMut` | Fixed-capacity mutable buffer over one or more acquired carriers |
| `SegmentedBytes` | Immutable public byte container over one or more contiguous presentation segments |

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

[open2]: https://man7.org/linux/man-pages/man2/open.2.html
[statx2]: https://man7.org/linux/man-pages/man2/statx.2.html
