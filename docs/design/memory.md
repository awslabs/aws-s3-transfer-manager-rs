# Memory

Concurrent transfers hold data in memory between the network and the consumer: parts fetched ahead of
a slow reader, parts buffered for out-of-order reassembly, parts staged for upload. A fast network
with a slow consumer, multiplied across many transfers, grows that resident memory without bound, and
allocating each buffer fresh and freeing it per part drives the resident set well above the data that
is live at any instant. The memory subsystem bounds both. It admits work against a single process-wide
cap, granting a reservation before a transfer commits memory and parking the transfer when the cap is
reached, and it serves the buffers that work fills from a fixed arena, reusing them rather than
returning them to the system allocator.

A transfer acquires memory in two steps at two times. It reserves an envelope when the scheduler
generates its work, before dispatch. It claims physical buffers from the arena later, while the work
executes. The reservation is the admission decision, the claim is its physical realization, and the
two are separate because the scheduler requires that an admitted work item never block mid-execution
waiting for memory. The availability question is settled before dispatch, so the physical draw that
follows cannot fail for want of capacity.

---

## Requirements

### Bound resident data-plane memory across the whole client

A single process-wide cap bounds the data-plane memory a client holds, independent of how many
transfers run or how load is distributed among them. The cap is global, not per-transfer. A fixed
per-transfer allowance is wrong at both ends of the concurrency range: large enough to let one
transfer saturate the network, it exceeds host memory once many transfers run; small enough to be safe
at high transfer counts, it throttles a lone transfer to a fraction of the memory available to it.

The cap bounds admitted work and retained capacity, not the resident set at an instant. Some payload
memory is claimed by code that can neither present a reservation nor be made to wait, so the resident
set can exceed the cap transiently. Every claim is accounted, and the excess is charged against later
admission until it is repaid, which is what keeps the client inside its budget without a hard ceiling.

A hard ceiling is neither achievable nor what the problem calls for. A client does not own all the
memory its transport uses: the HTTP client may be shared with a credentials provider, with other SDK
clients, or with requests the caller makes directly, and memory those paths take is not reserved and
does not appear as transfer work. Nor is a ceiling what motivates the subsystem. The cost being removed
is per-part allocation and first-touch, which caps sustained throughput and inflates the resident set
well above live data, and removing it must not require the caller to tune or replace the allocator.

The subsystem does not compute an overage limit in advance. That number is the product of connection
count, per-connection transport limits, and a concurrency target, all of which other subsystems own and
vary at runtime. What bounds it instead is cause: overage arises from requests the client dispatched for
admitted work, it is repaid as consumers drop the data, and withholding admission stops new work from
adding to it. Where the transport is shared, other users' reads are charged the same way and displace
the client's own work rather than escaping the cap (see [unreserved claims and
debt](#unreserved-claims-and-debt)).

What the cap is *sized for* is data-plane payload — the bytes of the objects in flight. A transport also
needs per-connection protocol memory for headers, chunk framing, and TLS records, and the cap does not
budget for it. That is a statement about sizing, not about which bytes the arena may hold: where a
transport reads headers through the same buffer source it reads payload through, those bytes occupy an
arena carrier and are accounted like any other claim the subsystem cannot attribute. Scratch the
transport allocates for itself never reaches the arena and is neither accounted nor bounded here (see
[what the buffer source can carry](#what-the-buffer-source-can-carry)).

### Admitted work runs to completion without stalling on memory

A work item admitted to run obtains the buffers it needs without blocking mid-execution on memory that
never arrives. Admission and physical acquisition are distinct steps, and clearing admission must imply
the buffers will be there: a work item that reserved successfully does not then stall inside execution
waiting for a claim. This rules out the deadlock in which a work item holds partial state while waiting
for memory that is itself gated behind the work it blocks. The guarantee attaches to claims made
against a held reservation; claims that cannot present one are covered by [serve claims that cannot be
gated](#serve-claims-that-cannot-be-gated) instead.

### Serve claims that cannot be gated

Not all payload memory is claimed by transfer code. On the download path the response body is read by
the transport, which asks for a buffer and has no reservation to present. Such a claim is served
unconditionally — it can be neither refused nor deferred — and fails only when the operating system
cannot supply memory at all. What it takes is accounted, which the subsystem gets for free by being
the one that hands the memory out, and that accounting is what reaches admission.

### Account for transfers without knowing their part sizes

A client moves objects from a few bytes to terabytes, up and down, concurrently. A large object moves
as a sequence of parts, a small object is a single short buffer, a directory transfer mixes many sizes,
and a transfer's part size is determined at runtime rather than fixed. Admission accounts all of them
against one cap without requiring part sizes to be known in advance.

### Admission delays but never denies, and never starves

A request that fits is eventually admitted. Under contention a stream of small requests does not
indefinitely defer a larger one, and no request is refused outright: admission delays, it does not
deny. The wait for any request that could ever fit is bounded. A single request larger than the entire
cap still makes progress rather than parking forever, and does not corrupt the bound for other
transfers while it runs.

### Release does not depend on scheduling

A transfer frees the memory it holds by consuming the data. The reservation is released as the data it
accounts for is delivered or written, on the execution path, not through a work-generation turn. The
release path does not route through any resource the transfer is simultaneously contending for, in
particular the scheduler's dispatch capacity. If freeing memory required a scheduling turn gated by the
capacity the memory-holding transfers occupy, admission and scheduling would wedge each other.

### Reuse buffers across operations

A steady-state transfer allocates almost nothing. Buffers are served from a fixed arena and returned
to it for reuse, so resident memory tracks live data rather than allocator state, and a transfer does
not pay allocation and first-touch on every part. A buffer's lifetime is decoupled from the system
allocator: the arena is mapped once and reused across operations.

### Buffers are page-aligned and page-multiple in size

Every buffer starts on a page boundary and is a whole number of pages. Direct disk I/O (`O_DIRECT`)
requires the buffer address, file offset, and transfer length to meet the underlying device's
alignment: historically the filesystem block size, since Linux 2.6.0 the device logical block size,
queryable per file via `statx(2)` `STATX_DIOALIGN` ([open(2)][open2], [statx(2)][statx2]). A
page-multiple buffer over-satisfies that on any real device, since a page is a multiple of the logical
block size, so the same buffers serve buffered and direct I/O without a bounce buffer. What this buys is
alignment of the buffers the subsystem hands out; a consumer that publishes payload at an offset inside one
can still produce a segment direct I/O rejects, which is a term of the delivery contract rather than a
property of the arena. Page alignment
also satisfies io_uring fixed-buffer registration and makes NUMA first-touch placement page-granular.
The arena honors it once rather than each buffer paying for it.

### Place buffers local to the hardware that fills them, without depending on it

On a multi-socket, multi-NIC host the NUMA node a buffer's pages sit on affects the bandwidth of the
DMA that fills it, so the subsystem places buffers local to the filling hardware and keeps them there
across reuse. This is an optimization, not a correctness condition. Locality requires the runtime to
pin threads, which it does not do by default, and remote access remains correct. Absent pinning the
subsystem routes through a shared path and functions correctly, only less locally optimized.

### Size to the machine without configuration

The default cap is safe on a 2 GiB container and a 768 GiB host alike, leaving headroom for the OS page
cache and the rest of the process, with no tuning. It is derived from detected memory, cgroup-aware so
a container sees its limit rather than the host's, and an explicit override is available for callers
who know their environment.

[open2]: https://man7.org/linux/man-pages/man2/open.2.html
[statx2]: https://man7.org/linux/man-pages/man2/statx.2.html

---

## Architecture

### The arena: blocks, carriers, segments

Three nouns describe the arena, smallest to largest.

A **carrier** is the unit the subsystem hands out: a fixed-size, page-aligned buffer, the smallest thing
a claim can draw. Every carrier is the same size. A small object occupies one carrier; a larger buffer is
filled and delivered as several. There are no size classes. Memory is obtained from the operating system
in blocks and handed to callers in carriers, so the two units are distinct and both matter: capacity is
mapped and reclaimed a block at a time, and claimed and returned a carrier at a time.

A **block** is a large, page-aligned region mapped from the OS in one `mmap` and divided into carriers
of the uniform size. A carrier is a window into its block — identified by the block and its index
within it — not a separate allocation; its address is `block.base + index * carrier_size`, page-aligned
because both terms are. Each block owns a **bitmap**, one bit per carrier, and that bitmap is the single
source of truth for which carriers are free. Claiming a carrier flips its bit to in-use; returning one
flips it back. The block is mapped once and reused across countless carrier cycles, so the cost of the
`mmap`, of first-touching its pages, and of any later device registration is paid per block and
amortized over every carrier it yields. The **arena** is the whole collection of blocks under one cap;
there is one arena per client.

A **segment** is a contiguous run of one or more carriers within a single block. Because carriers in a
block are physically adjacent, a run of consecutive free carriers *is* a contiguous region of memory —
one address and length, so one `IoSlice` and one deliverable `Bytes`. A buffer for a whole unit of work
(a download range, an upload part) is one or more segments, which need not be contiguous with each
other: they may sit anywhere in any block. The handle over that set of segments is the **segment
buffer** (see [the segment buffer](#the-segment-buffer)), the arena's analogue of `BytesMut`/`Bytes`.

```text
  Arena  (one per client; owns the cap and the blocks)
  │
  ├── Block 0   ── one mmap(), page-aligned, the grow/trim unit ──────────────┐
  │     base ─► ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐      │
  │             │ carr │ carr │ carr │ carr │ carr │ carr │ carr │ carr │  ... │
  │             └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘      │
  │     bitmap:    1      0      0      0      1      1      1      0    …      │
  │             (1 = in use, 0 = free; the single source of truth)            │
  │                       └──── carriers 1,2,3 free & adjacent ────┘          │
  │                              = one contiguous 3-carrier SEGMENT            │
  └────────────────────────────────────────────────────────────────────────────┘
  ├── Block 1   ...
  └── Block N

  carrier address = block.base + index * carrier_size   (page-aligned)
  a segment       = a run of adjacent free carriers in ONE block (1 IoSlice, 1 Bytes)
  a work buffer   = 1+ segments, NOT necessarily adjacent (any carriers, any blocks)
```

One carrier size is the load-bearing choice. Its consequence is that every free carrier satisfies every
request: no request needs a shape that free memory cannot fit, so there is no external fragmentation and
no free-list arrangement that leaves capacity unusable. Admission reduces to counting carriers. The
machinery a multi-size arena would need does not exist — no subdividing a large region into small slots,
no per-class free tracking, no migrating capacity between classes as the traffic mix shifts, no
relabeling a region from one class to another while claims are in flight.

The cost this trades for is internal, and the design accounts for it rather than claiming it away. A
small object occupies a whole carrier, so a 64 KiB object in a larger carrier holds a fraction of its
payload, and a part that is not a carrier multiple wastes up to nearly one carrier in its tail.
Per-buffer overhead grows with the number of carriers: a large object is many carriers, each returned
on its own drop, so a very large transfer is many carrier operations rather than one. And a block is
reclaimed only when all its carriers are free, so a single long-lived carrier holds its block resident
(see [reclaiming capacity](#preparing-and-reclaiming-capacity)). What one carrier size removes is
external fragmentation and the concurrency of cross-class relabeling; what remains is internal waste and
carrier count, both a function of the carrier size, which is why that size is chosen by measurement (see
[carrier size](#carrier-size)).

A carrier is named by its block and index, carrying the generation its block slot held when the name
was minted, so a name that outlives its block's mapping is detected rather than followed (see
[unmapping a block safely](#unmapping-a-block-safely)):

```rust
struct CarrierId { block: u32, index: u32, generation: u64 }

struct Block {
    base: AtomicPtr<u8>,      // page-aligned mapping; null when the block is empty
    carrier_size: usize,      // uniform within the arena
    inuse: Box<[AtomicU64]>,  // one bit per carrier, 1 = in use; the single source of truth
    status: AtomicU64,        // low bits: Empty | Active | Draining; rest: generation.
                              // generation occupies the high bits so its carry cannot
                              // reach the state bits. One word, so a claim reads the
                              // pair in one load (see unmapping a block safely)
    // ... NUMA node
}

// carrier address = block.base + index * carrier_size  (page-aligned, since both are)
```

**Alternative: two size classes, a large part class and a small receive class in one arena.** A large
class serves upload parts and disk reads as single contiguous buffers; a small class serves
socket-sized receive reads. Rejected: sharing one arena between two sizes reintroduces the machinery one
size avoids. The arena must subdivide large regions into small slots and track which region is which
class, migrate capacity between classes as the up-and-down mix shifts, and relabel a region from one
class to the other while claims are in flight, and it strands capacity when a sparse scatter of
long-lived small buffers pins large regions at low occupancy, worst on a high-concurrency small-object
workload. The one thing the large class buys, a physically contiguous buffer for disk I/O, is instead
obtained opportunistically as a segment over uniform carriers (see
[contiguous runs](#contiguous-runs-and-disk-io)) without the class machinery.

**Alternative: a general allocator over a byte-granular arena.** Buffers of any exact size drawn from
anywhere in a block removes internal waste but reintroduces external fragmentation, which then needs
coalescing or compaction to recover. Compaction is not available here: a live carrier's address is held
by the consumer inside a delivered buffer, or by the kernel for a registered or in-flight I/O, so it
cannot be moved. Uniform carriers avoid the fragmentation rather than paying to recover from it.

### Reserve and claim

A transfer **reserves** an envelope of carriers at scheduling time, when the [scheduler](./scheduler.md)
generates its work in `poll_work`, before the work is dispatched. It **claims** physical carriers at
execution time, while the work runs. Reserve is the admission decision and touches only the global cap:
it grants an envelope if the cap has room and parks the transfer in arrival order if it does not. Claim
is the physical draw against a granted envelope and touches the arena.

The split is imposed by the scheduler's work model. Work runs to completion inside `execute`, and a
work item that blocks there waiting for admission holds a dispatch slot that could advance another
transfer. The blocked work may also be what must run to free the memory it waits on, so admission and
scheduling deadlock. Settling admission in `poll_work`, before a dispatch slot is committed, keeps
`execute` from blocking on the cap: an admitted work item holds an envelope, and claiming against it
never re-consults admission. The scheduler requires `poll_work` to be synchronous and O(1) (see
[scheduler cost model](./scheduler.md#cost-model)), so reserve does no I/O; it is a bounded check against
the cap.

What claim draws against is the reservation, not a fresh availability decision: the claim spends
authority the grant already established, and it never re-consults admission. Backing normally exists
when it runs, because reserve raises `prepared` to cover every live envelope and keeps a block of
headroom ahead of demand.

It can nonetheless come up short, and the reason is that admission can only gate what has not been
granted yet. Unreserved claims cannot be refused, so the memory they consume is discovered after
envelopes are already live, and suppressing admission cannot retract a reservation a transfer is already
holding. Growth is the only instrument that restores backing for an envelope already promised, and the
shortfall is discovered where the carrier is drawn. So a claim that finds nothing free maps a block to
back what it is owed rather than failing or waiting on admission (see [preparing and reclaiming
capacity](#preparing-and-reclaiming-capacity)); the physical layer absorbs the gap that admission cannot
reach. Because that path maps memory, a claim reports an allocation failure to its caller — the only way
a claim does not return carriers is the operating system declining to back them.

A claim yields a **segment buffer** — a handle over the carriers drawn (see
[the segment buffer](#the-segment-buffer)). A download reads one carrier per socket read and claims one
at a time; an upload knows its whole part up front and claims all its carriers at once, contiguous where
the arena can supply it. Claim reaches the arena from two kinds of caller, and the API differs
accordingly.

The **transfer itself** holds its reservation and claims against it directly, presenting the
reservation and drawing carriers up to the granted envelope. A transfer cannot claim more than it
reserved: the reservation is the authority the claim spends, checked in hand. This is the explicit,
self-accounting path, used wherever the transfer owns the fill, as an upload does when it stages a part
from disk.

An **injected buffer source** claims where the transfer does not own the fill. When a response body is
read by a lower I/O layer rather than by transfer code, that layer needs carriers but has no reservation
to present: the interface it exposes takes a buffer request, not a transfer's reservation handle. It
claims through a second entry point that asks for a carrier and receives one. This is an **unreserved
claim**. It cannot be refused and it cannot be parked, so the arena serves it and records what it took
(see [unreserved claims and debt](#unreserved-claims-and-debt)).

The two entry points draw from the same arena against the same cap and differ in one respect: whether
the caller can present a reservation. With one, the envelope is enforced at the claim. Without one, the
claim is served first and reconciled against the envelope afterward.

**Alternative: one acquire at execute time.** Collapsing reserve and claim into a single
acquire-while-running call is simpler, but it puts the availability question inside `execute`, where a
work item that cannot get memory blocks while holding a dispatch slot. That is the deadlock the split
prevents. Reserve-before-dispatch keeps the blocking decision in `poll_work`, where a transfer that
cannot be admitted parks without holding a slot.

**Alternative: check the reservation on the unreserved path too.** Enforcing the envelope at every
claim would catch an over-claim where it happens rather than after the fact. It is not available: the
claim arrives with no way to name the reservation it belongs to (see [what the buffer source can
carry](#what-the-buffer-source-can-carry)), so there is nothing to check it against. Were the identity
available, the check would still have to be weighed against putting a shared atomic on the busiest path
in the system.

### What the buffer source can carry

On the download path the transfer does not read its own response body; the transport does. The
transport can be supplied with a buffer source, so received bytes land in arena memory rather than in
allocations the transport makes itself — that much is established. What is not established is any way
to tie a given read to the reservation of the transfer whose response it belongs to. The source is
called with a buffer request, at a point where the transport knows the size it wants and little else;
the per-request identity that would name a reservation is not available there. The transport may also
read headers into the same buffer it reads payload into, so a claim is not necessarily payload at all.

This is the constraint that shapes the two claim paths. What is possible inside the transport is
designed separately; this subsystem needs only the consequence: one path claims with a reservation in
hand, and one claims unreserved, its consumption discovered rather than authorized.

It also fixes what the cap does and does not reach, which is a scope statement rather than an open
question. The dividing line is not payload versus protocol but *served from the arena* versus not.
Anything the arena hands out is accounted, whether it turns out to hold object bytes or response
headers, because the subsystem is the one handing it out and cannot be told which it is. Memory the
transport allocates for itself — TLS record buffers, HPACK tables, connection scratch — is invisible
here and is bounded, if at all, by the transport. The honest total-memory statement is therefore that
arena capacity is bounded by the cap, not that one number bounds every byte the client holds. The cap is
sized against payload because payload is what scales with concurrency and part size; header bytes riding
the same buffer source are small, and they are charged rather than excluded, so they consume cap rather
than escaping it.

### Unreserved claims and debt

A reservation is granted before the work is dispatched, and part of the memory that work goes on to use
is claimed by the transport rather than by the transfer. The reservation therefore states two things:
how many carriers the envelope covers, and how many of them the holder will claim itself. The remainder
is **unreserved coverage** — the part of the envelope set aside for claims the transfer will not make in
its own name. A ranged download claims nothing directly and sets its whole envelope as coverage. An
upload staging a part from disk claims all of it directly and sets none.

Coverage is accounting, not escrow. No carriers are held aside; nothing is handed to the transport. It
records how much unreserved consumption the client has already been charged for.

Because coverage is carved out of the envelope rather than added to it, the holder's own claims are
capped at what remains: a reservation may claim `envelope − coverage` carriers in its own name. This is
what keeps the cap honest — a download that declared its whole envelope as coverage claims nothing
directly, so the transport's reads and the transfer's claims cannot both draw the full envelope (see
[preparing and reclaiming capacity](#preparing-and-reclaiming-capacity)).

The subsystem cannot tell which reservation an unreserved claim belongs to, and does not need to. Every
question it answers is about a total compared against the cap, and totals do not require identity. What
coverage buys is that the memory was *already admitted*: a claim landing within outstanding coverage
draws on carriers a grant has paid for, so it moves nothing in the admission sum. Only consumption past
all outstanding coverage is memory no grant anticipated.

The same reasoning covers the case where the buffer source is shared with code outside this client. A
foreign claim spends coverage it never declared, because the subsystem cannot distinguish it. Coverage
then drains faster than the client's own transports would drain it, debt accrues sooner, and admission
tightens: the client runs with less rather than the cap being exceeded on its behalf. No separate
mechanism handles this, which is why a shared source is accounted for rather than merely tolerated.

An unreserved claim is served whether or not coverage remains. When coverage is available the claim is
already paid for. When it is exhausted the carrier is still served — refusing it breaks a connection
mid-response and parking it can stall the read loop that would free the memory — and the excess is
recorded as **unreserved debt**. Debt is the memory the client holds that no reservation covers. It
enters the admission sum, so it displaces work the client could otherwise start, and it leaves the sum
only when the carriers behind it are returned.

Debt does not fall when a reservation is granted. A grant adds coverage for claims made after it, and
that coverage does not apply to consumption that was already live. Without this rule the debt would be
recomputed against each new grant's coverage and would settle at nothing: suppose eight carriers of
coverage, ten claimed, then the reservation closes with the consumer still holding all ten, leaving ten
in debt. Admitting a second transfer of the same shape brings eight carriers of coverage with it, and
subtracting them from the ten live carriers puts the debt at two — before the new transfer has claimed
anything. Its own claims then land on top of the same ten carriers, and the next grant subtracts the
same eight again. The client would keep admitting work against memory it has already committed. Holding
the debt fixed until the carriers come back keeps each grant paying only for what follows it.

A closing reservation converts what it cannot account for. Its coverage is withdrawn, and any
unreserved consumption still live and no longer covered becomes debt at that moment. The exact
conversion, and the full set of quantities admission tracks, is in [Appendix D](#appendix-d-the-admission-ledger).

Coverage is an allowance that recycles rather than one a transfer spends once. An envelope bounds what a
transfer holds at any instant, not what it moves over its lifetime, and a transport reads many frames
through the same carriers: a download whose coverage were consumed permanently by the first pass would
spend the rest of the transfer in debt by construction. So a returned carrier credits coverage back,
after any debt is repaid.

Repaying debt first is what keeps debt from persisting. The subsystem cannot attribute a return any more
than it can attribute a claim, so it cannot tell a covered return from an uncovered one. Of the orderings
available to it, repaying debt first is the one that guarantees debt falls as carriers come back;
crediting coverage first would leave debt outstanding while a steady working set recycles through the
same carriers, and admission would stay suppressed with nothing to release it.

Backpressure does not act at the claim that overruns coverage. That claim has already happened by the
time the arena sees it, and the caller cannot wait. It acts at the next reserve: debt sits in the
admission sum, so the next transfer to ask for an envelope finds less room, and if the debt is large
enough it parks. The subsystem pushes back where it decides — at admission — rather than where the
overage appeared.

### Claiming and returning a carrier

A claim finds a free carrier and marks it in use; a return marks it free. Both run on the hot path — a
claim per read on download and per part on upload, a return as each carrier's data is consumed — and the
workload has no thread affinity to exploit. A carrier is filled on the I/O thread that ran the read and
returned on whatever thread the consumer happened to drop the delivered bytes on, and across a client
running many mixed uploads and downloads at once, neither thread owns the carrier or the block. The
structure that fits an ownerless, no-affinity workload is the bitmap itself: any thread returns any
carrier and any thread claims any carrier with no ownership handshake.

The bitmap is one atomic word per sixty-four carriers, and it is the single source of truth. A set bit
means the carrier is in use; a clear bit means it is free. A **claim** takes a free carrier by setting
its bit with an atomic OR, then inspecting the word the OR returned: if the bit was already set, another
thread took the carrier first and the claim moves on to the next free bit. A **return** clears the
carrier's bit with an atomic AND. Because different carriers live in different words, returns from many
threads distribute across the bitmap rather than serializing on one structure — there is no shared
free-list head to contend. Neither operation carries a remembered value that a concurrent free-and-reuse
could invalidate — the OR and the AND each act on a single bit unconditionally and report the prior state
— so the claim/return path has no ABA hazard on the carrier bit.

An upload claiming many carriers at once sets a whole word's worth of free bits in one atomic OR against
a mask, keeping whichever bits it actually won (the bits that were clear before the OR) and moving to the
next word for the rest — so a part of many carriers is a handful of atomics, not one per carrier, and
without any all-or-nothing compare-and-swap that a concurrent return could livelock. Because a set bit is
in-use, "all carriers free" is the whole word reading zero, which is the test trim uses.

Each worker thread keeps a **shard**: a small thread-local list of recently-seen free carrier IDs, used
only to *start* a claim's search near carriers likely still free, never as owned capacity. A claim
consults its shard first and takes a hinted carrier by the same atomic OR; if the hint is stale — the
carrier was taken since, or its block was reclaimed, or reclaimed and since revived — the claim discards it
and scans the bitmap. The shard is written only by the scan: on a miss, the claim scans a word, takes one free bit, and
stashes the word's other free bits as hints for its next claims. Because the shard holds only hints and
the bitmap records every free carrier, a carrier freed anywhere is discoverable by a scan regardless of
which thread last touched it — shard contents are never capacity the cap cannot see. Returns go straight
to the bitmap, never to a shard, so no carrier is ever parked in thread-local storage where another
thread's claim cannot reach it. The shard belongs to the worker thread, not the transfer: a claim is a
synchronous operation that does not yield mid-way, so it always consults the shard of whatever worker is
running it, and a transfer that migrates between workers simply starts from a different shard. The shard
accelerates finding a free carrier; it never holds one. This is what separates it from a general
allocator's thread cache, which caches freed blocks for the freeing thread to reallocate — a carrier's
consumer is not its next claimant, so a return-side cache would strand capacity on threads that never
claim.

```text
  claim:                                       return:
    consult shard for a near-free hint           clear the carrier's bit (atomic AND)
    take a free bit (atomic OR),                → distributes across bitmap words,
      inspect prior word to confirm it was free    no shared free-list head
    lost the race → next free bit
    shard empty → scan the bitmap,
      stash the word's other free bits as hints
    bounded misses → serialized claim (below)
```

The ordering each atomic requires is stated at the site that issues it; Appendix A collects them.

```rust
impl Arena {
    /// Claims one carrier. The `Err` case is an `mmap` the operating system
    /// declined; the subsystem itself never refuses a claim and never waits on
    /// admission (see "reserved implies claimable").
    fn claim(&self, shard: &mut Shard) -> Result<Carrier, AllocError> {
        for _ in 0..CLAIM_ATTEMPTS {
            // Start near a carrier recently seen free. A hint names a block, an
            // index, and the generation that block held when the hint was minted.
            // It is a starting point, never owned capacity.
            while let Some(id) = shard.pop_hint() {
                if let Some(c) = self.try_take(id) {
                    return Ok(c);
                }
                // Stale: the carrier was taken since, or its block was reclaimed,
                // or reclaimed and since revived. `try_take` left the bitmap as it
                // found it, so there is nothing to undo here.
            }

            // No usable hint: scan the bitmap, the single source of truth for what
            // is free, stashing the scanned word's other free bits as hints.
            match self.scan_and_take(shard) {
                Scan::Took(c) => return Ok(c),
                // Free bits were found but every take lost its race. A free carrier
                // still exists by the accounting, so scan again.
                Scan::LostRace => continue,
                // Nothing free anywhere: consumption that could not be refused has
                // overtaken `prepared`. Map a block to back what is already owed,
                // rather than failing or waiting on admission.
                Scan::NoneFree => return self.grow_and_take(),
            }
        }

        // Bounded losses: serialize under the per-node lock, where no take is lost.
        // This resolves contention for capacity already guaranteed to exist; it
        // never maps memory and never waits for any.
        self.claim_serialized(shard)
    }

    /// The gate against trim. Sets the carrier's bit, then reads the block's status,
    /// and keeps the carrier only on `active` with a matching generation (see
    /// "unmapping a block safely" for why no weaker pair of orderings works).
    fn try_take(&self, id: CarrierId) -> Option<Carrier> {
        // The block record is never freed and never moves, so indexing it is always
        // safe; only the region its `base` points at comes and goes.
        let block = &self.blocks[id.block as usize];
        let (word, bit) = (id.index as usize / 64, 1u64 << (id.index % 64));

        // Take the bit. SeqCst: this store is the first half of the gate, and the
        // load below must not be reordered before it. The returned prior word is
        // what says the carrier was ours to take -- no remembered value is compared,
        // so there is no ABA hazard on the bit.
        if block.inuse[word].fetch_or(bit, Ordering::SeqCst) & bit != 0 {
            return None; // another thread holds it; the bitmap is unchanged by us
        }

        // The second half of the gate. One load reads state and generation together,
        // so the pair cannot be observed inconsistently. SeqCst for the same reason.
        //
        // The test is affirmative -- in service AND the same incarnation this name
        // refers to. Rejecting only `draining` would let `empty` through, since a
        // block has three states and a name can outlive a mapping entirely.
        if block.status.load(Ordering::SeqCst) != Status::active(id.generation) {
            block.inuse[word].fetch_and(!bit, Ordering::SeqCst); // only our own bit
            return None;
        }

        // Only now is an address formed. The set bit forbids unmap, so `base` cannot
        // be cleared while this carrier is held, and a claim that backed out above
        // never reached this line. Relaxed suffices: the mapping was published
        // before the status that admitted us, and the acquire half of that SeqCst
        // load orders this read after it.
        let base = block.base.load(Ordering::Relaxed);
        Some(Carrier { id, ptr: unsafe { base.add(id.index as usize * block.carrier_size) } })
    }

    /// The batch form: an upload claims a whole part at once. One OR sets every free
    /// bit it wants in a word and it keeps whichever it won -- not an all-or-nothing
    /// compare-and-swap, which a concurrent return to the same word would livelock.
    fn try_take_word(&self, id: CarrierId, word: usize, want: u32) -> Option<u64> {
        let block = &self.blocks[id.block as usize];

        let mask = lowest_set_bits(!block.inuse[word].load(Ordering::Relaxed), want);
        if mask == 0 {
            return None;
        }

        let won = mask & !block.inuse[word].fetch_or(mask, Ordering::SeqCst);
        if won == 0 {
            return None;
        }

        if block.status.load(Ordering::SeqCst) != Status::active(id.generation) {
            block.inuse[word].fetch_and(!won, Ordering::SeqCst); // only the bits we won
            return None;
        }
        Some(won)
    }
}

impl Status {
    /// `status` packs generation and state in one word; `active(g)` is the only
    /// value a claim accepts. Generation occupies the high bits, so its carry
    /// cannot reach the state bits.
    fn active(generation: u64) -> u64 {
        generation << STATE_BITS | ACTIVE
    }
}
```

A batch claim backs out by clearing the bits it won, not the mask it attempted. Bits the OR found already
set belong to another claim, and clearing them would free a carrier that thread is about to fill.

A claim succeeds on two separate conditions. Whether a free carrier *exists* is accounting: a claim
happens only within a granted envelope the claimant has not spent, and the arena holds `prepared >=
reserved + debt` (the counters are defined under [preparing and reclaiming
capacity](#preparing-and-reclaiming-capacity)), so a carrier is free — or, in the case that sum has been
overtaken by consumption that could not be refused, the claim maps one. Whether this claim *gets one
promptly* is contention: the claim scans for any free bit and takes it, and if it loses that race, a free
carrier still exists by the accounting, so it scans on and takes a different one. Contention decides
which claim gets which carrier; it never leaves an admitted claim without one. The claim path is
lock-free, and because a free carrier is guaranteed to exist or be obtainable, the system always makes
progress.

The residual is the usual lock-free one: a single thread could, under adversarial scheduling, keep
losing the race and starve while the system progresses. To bound that, a claim that fails a fixed number
of attempts takes a serialized path, claiming under a per-node lock where no atomic can be lost. This is
a fairness backstop, not a capacity wait: the carrier it claims was already guaranteed to exist, so the
lock removes contention rather than waiting for memory to appear. The serialized path never maps memory
or grows the arena, which is admission's concern settled at reserve.

**Obligations** (discharged in [Appendix C](#appendix-c-verification)).
- A claim keeps a carrier only when its bit was clear before the take and its block reads `active` with a
  matching generation.
- A rejected take leaves the bitmap exactly as it found it, and a batch rejection clears only the bits it
  won.
- An address is formed only after the gate passes — the pointer is computed in one place, after the status
  check.
- The scan finds any free carrier regardless of which thread returned it, so a claim within a granted
  envelope never fails for lack of a carrier that exists.
- A claim terminates: bounded losses fall through to the serialized path, which cannot lose a take.

**Alternative: a per-thread cache that owns its carriers.** A thread cache that takes ownership of a
batch of carriers — marking them in use until the local cache drains, as a general-purpose allocator's
thread cache does — removes an atomic from the common claim. Rejected: it relies on an affinity this
workload lacks. A general allocator's freeing thread is usually a future allocating thread, so a carrier
cached on free is reused cheaply by the same thread; here the thread that drops a delivered buffer is a
consumer that will not claim again, so a carrier cached on its return sits unreachable to the I/O threads
that need it, breaking the guarantee that a reserved carrier is claimable. Recovering it needs
cross-thread stealing or flushing, reintroducing the shared traffic the cache was meant to avoid.
Holding the bitmap as truth and the shard as a hint keeps every free carrier discoverable, at the cost of
one atomic per claim.

**Alternative: a per-block atomic free-list for returns.** Give each block a lock-free stack of returned
carriers, pushed on return and drained on claim, as a sharded-free-list allocator does. Rejected for the
same missing affinity: that design is cheap because a single owner drains each block's list, making it a
clean multiple-producer/single-consumer stack; with fungible blocks and no owner, the drain is
multiple-consumer and the stack reintroduces the ABA the bitmap avoids, and it serializes returns to a
block on one list head where the bitmap's per-word atomics distribute them. The bitmap gets both the
distribution and the adjacency information (for contiguous claims) that a free-list cannot.

### The reservation and its lifetime

A reservation is an RAII value. `reserve` returns one when it grants an envelope; its drop returns the
envelope to the cap and wakes the next parked transfer. Nothing releases a reservation explicitly.
Release is the drop, and the drop happens when the data the reservation accounts for is gone, so the
reservation is held for exactly as long as the memory it covers is resident.

```rust
struct Reservation {
    budget: Arc<MemoryBudget>, // the shared admission quantities + wait queue
    carriers: u32,             // the granted envelope
    coverage: u32,             // the part of it set aside for unreserved claims
    live_here: AtomicU32,      // carriers claimed in this reservation's own name
}

impl Reservation {
    /// How many more carriers the holder may claim itself. Coverage is carved
    /// OUT of the envelope, so it is subtracted here: a download that declared
    /// its whole envelope as coverage claims nothing directly, and an upload
    /// that declared none claims all of it. Without the subtraction a download
    /// could claim its full envelope while the transport spends its coverage as
    /// well, holding twice what admission counted once.
    fn remaining(&self) -> u32 {
        self.carriers - self.coverage - self.live_here.load(Ordering::Relaxed)
    }

    /// The explicit path: the envelope is enforced here, at the claim, because
    /// the caller has the reservation in hand. Exceeding it is a caller bug, not
    /// a capacity condition -- admission already granted this memory.
    fn claim(&self, arena: &Arena, n: u32) -> Result<SegmentBuffer, AllocError> {
        assert!(n <= self.remaining(), "claim exceeds the granted envelope");
        let buf = arena.claim_n(n)?;
        self.live_here.fetch_add(n, Ordering::Relaxed);
        Ok(buf)
    }
}

impl Arena {
    /// The unreserved path has no reservation to present and so no envelope to
    /// check against (see "what the buffer source can carry"). It is served first
    /// and reconciled afterward: draw the carrier, charge it against outstanding
    /// coverage, and record whatever coverage cannot absorb as debt.
    fn claim_unreserved(&self) -> Result<Carrier, AllocError> { /* ... */ }
}

impl Drop for Reservation {
    /// The sole release path. Runs on whatever thread drops the last delivered
    /// frame, so it must not depend on a work-generation turn.
    fn drop(&mut self) {
        let mut ledger = self.budget.lock();

        // Withdraw this reservation's coverage, and convert what that leaves
        // uncovered into debt: the part of `coverage` that `avail` cannot
        // absorb is consumption that is still live and has just lost its cover.
        let uncovered = self.coverage.saturating_sub(ledger.avail);
        ledger.debt += uncovered;
        ledger.avail = ledger.avail.saturating_sub(self.coverage);
        ledger.declared -= self.coverage;

        // Return the envelope.
        ledger.reserved -= self.carriers;

        // Hand the freed capacity to waiters that fit, front to back, stopping
        // at the first that does not. The grant is recorded here, under the
        // lock; the wakes fire after it is released, so a wake is never lost and
        // the wake path never nests locks.
        let wakes = ledger.drain_queue();
        drop(ledger);
        for w in wakes {
            w.wake();
        }
    }
}
```

Debt only ever rises here. A grant adds coverage for claims that follow it and cannot retroactively cover
consumption that is already live, which is why the conversion is additive rather than a recomputation
against the current coverage (see [unreserved claims and debt](#unreserved-claims-and-debt)). The full set
of quantities and every transition among them is in [Appendix D](#appendix-d-the-admission-ledger).

The reservation covers a whole unit of work, a download range or an upload part, not an individual
carrier. It is acquired at `poll_work`, carried on the work item into `execute`, and moved onto the
delivered segment buffer, where it rides until the data is consumed. The carriers that hold the data are
part of that buffer, so they and the reservation are released together as it is consumed: the carriers
return to the arena as each segment's bytes drop, and the reservation returns to the cap when the last
of them is gone. Admission and the arena are distinct accounting layers, but on this path their release
coincides.

Admission accounts at the granularity of the work it admitted, the whole part, not the individual
carrier. It does not readmit new work against a part that is still resident even if that part's data is
partially consumed, because the reservation is held until the whole result is released. Releasing the
reservation carrier by carrier as a part drains would let admission readmit sooner but would make it
account at a finer granularity than it admitted; whether that is worth it is a question only a very
large part with a slow consumer raises (see
[Open Questions](#reservation-granularity-on-very-large-parts)).

A download range shows the full path:

```text
  poll_work              execute                       delivery            drop
  ─────────              ───────                       ────────            ────
  reserve(range)         read the range body:          segment buffer      each segment's Bytes
    -> Reservation         claim carrier, fill  ┐       delivered as        drops -> its carriers
    held on work item      claim carrier, fill  ├─ into  Bytes frames,      return to the arena
                           claim carrier, fill  ┘  a     one per segment    -> when the LAST frame
                           ...                     segment                     drops, Reservation
                           Reservation moves ──────► buffer                     drops -> envelope
                           onto the buffer                                       back to cap, waiter woken
```

The reservation is single-owner up to delivery: acquired once, moved along on the work item, never
reference-counted for concurrent owners. At delivery it is shared across the segment buffer's frames,
because a work buffer of several segments is delivered as several `Bytes` and a slow consumer may hold
some after others have dropped. The reservation is held behind a shared count so that it returns to the
cap only when the *last* frame drops — not the first — keeping the whole part's envelope reserved for
exactly as long as any of its data is resident. This is not multiple independent owners racing to
release: there is one reservation, released once, when the last byte it accounts for is gone. The
individual carriers, by contrast, return to the arena eagerly as each frame drops (see
[the segment buffer](#the-segment-buffer)), so physical memory frees ahead of the admission envelope.

**Obligations** (discharged in [Appendix C](#appendix-c-verification)).
- The holder's own claims are capped at `envelope − coverage`, which is what makes `live_reserved <=
  reserved` hold and therefore what makes the cap bound real footprint. No mechanism enforces it other than
  this subtraction.
- A reservation's drop returns the envelope exactly once, and debt only rises at a close.
- A grant never reduces standing debt.
- The wake fires after the budget's lock is released, and the grant is recorded before the wake.

Retry does not re-reserve. A range-GET that fails mid-body and retries re-reads the body into fresh
carriers, but the reservation is held across attempts, on the work item, not dropped and re-taken. Only
the partial data of the failed attempt is discarded; its carriers return to the arena and the
reservation stays. If retries are exhausted and the work fails without producing a segment buffer, the
reservation drops with the work item, returning the envelope. The reservation's lifetime is the work's,
not an attempt's.

### The segment buffer

A claim yields a **segment buffer**: the handle over the carriers drawn for one unit of work. It comes
in a writable form and an immutable form, mirroring `BytesMut` and `Bytes`, and it is the only view of
arena memory a filler or a consumer ever holds. It is not a `BytesMut`. A `BytesMut` grows by
reallocating and moving its bytes, which would relocate a carrier out from under an in-flight I/O or a
device registration; a segment buffer is a fixed set of windows into the arena that never moves.

The writable form is what a filler writes into: an upload reading a part from disk, or an injected
source writing received bytes. It exposes its carriers as writable byte slices — one per segment, so a
vectored read (`preadv`) fills a multi-segment part in one call, and a single-segment fill (a socket
read) just writes the one slice. It carries 1 to _k_ segments; on the common contiguous claim it is a
single segment, and only a fragmented claim holds several. Sealing it (`freeze`) turns it into the
immutable form without copying — it forgets its own carrier ownership and hands it to the immutable
buffer, exactly as `BytesMut::freeze` yields a `Bytes` over the same allocation.

The immutable form is a sequence of segments delivered downstream. What that sequence has to look like is
fixed by the consumers it must satisfy without a copy — the SDK body type carrying `Bytes`, and disk
writes and generic byte sinks taking `bytes::Buf` — and getting arena memory into those shapes is the
subject of [Ecosystem](#ecosystem).

Two properties of the immutable form hold whatever the caller-facing type turns out to be. No route out of
it loses track of a carrier: every route either clones a `Bytes`, extending the carrier's life while keeping
it counted, or copies out of it, ending that life at a point the arena observes. None yields a bare pointer,
so a consumer cannot escape the accounting. And capacity is held from claim until the last delivered view
drops, so a slow consumer converts throughput into resident memory — which is why retention appears in the
requirements as something the cap must tolerate rather than something the arena can prevent. The shape of
the type the caller actually receives is open (see [the shape of the delivery
type](#the-shape-of-the-delivery-type)).

### Preparing and reclaiming capacity

Five quantities separate what was configured from what is backed, granted, and in use:

```text
  configured  the cap the sizing policy or an override set        (a target)
  prepared    carriers backed by mapped arena
  reserved    carriers granted to live reservations
  live        carriers currently claimed against the arena bitmap
  debt        live unreserved consumption no live reservation covers
```

`live` decomposes by how the carrier was obtained, and only one part is bounded by a reservation:

```text
  live = live_reserved + live_unreserved
  live_reserved <= reserved      each reservation's own claims fit its envelope
  live_unreserved                bounded by the transport, not by this subsystem
```

Admission gates on `reserved + debt`. Reserve grants an envelope if `reserved + debt + envelope <=
configured` and parks the transfer in arrival order if it does not, so debt displaces new work exactly
as a live reservation does.

That sum also bounds what the client actually holds, which is what makes the cap meaningful:

```text
  footprint = live_reserved + covered_live + debt
            <= reserved + debt                     coverage is part of the envelope
            <= configured                          admission gates this sum
```

The first step is the load-bearing one, and it holds only because coverage is carved *out* of the
envelope rather than added beside it. A reservation's own claims are capped at `envelope − coverage`,
not at `envelope`: a ranged download that sets its whole envelope as coverage may claim nothing in its
own name, and an upload that sets none may claim all of it. Without that subtraction a download could
claim its full envelope directly and have its coverage spent by the transport as well, holding twice
what admission counted once.

The physical layer has a separate obligation. Every carrier a live reservation may still claim must be
backed, and the carriers behind debt are backed too, so:

```text
  prepared >= reserved + debt
```

is what the physical layer works to restore. It is a target rather than an invariant: debt arises from
claims that cannot be refused, which happen after envelopes are already granted, and no admission
decision can retract a reservation that is already live. So the same sum has two ceilings — admission
holds it under `configured`, and the arena keeps `prepared` at or above it — and growth is what closes
the gap when the second one opens.

Reserve raises `prepared` toward that target ahead of demand. It maps when the headroom of
prepared-but-unreserved capacity falls below a low-water mark, so most reserves are a bounded check and
only an occasional reserve maps. What growth costs, and therefore whether it belongs on this path at
all, depends on what preparing a block entails.

Where growth runs depends on what preparing a block costs. When preparation is an anonymous `mmap`, it
runs inline in reserve. `mmap` does not commit physical memory: it reserves a range of address space,
and the kernel commits a physical page only on first write to it, as a page fault. Those faults land
during `execute`, on the I/O thread filling the carrier, where a page fault is an ordinary microsecond
event rather than a scheduling hazard, and only pages actually written are ever committed, so resident
memory tracks data filled rather than address space reserved. The `mmap` itself is fast and rarely
fails, so reserve can grow inline: it returns a `Result`, granting on success and surfacing an
allocation failure to the transfer that asked rather than parking it against capacity that will never
arrive. Returning the failure to the caller is the useful behavior, since a transfer can react to being
unable to get memory. Reserve's common case stays an O(1) check and its rare case a single `mmap`, both
within what `poll_work` tolerates, since neither blocks on I/O nor `.await`s.

Growth moves off the reserve path when preparation becomes expensive enough that doing it inline would
stall the reserving thread. That happens when a block's pages must be faulted eagerly rather than lazily
— for example to place them on a chosen NUMA node instead of wherever `execute` first writes them — or
when a block must be registered with a device, as io_uring fixed buffers or `mlock` require, which is
slower still and can hit a limit. In that regime a background **preparer** maps, touches, and registers
blocks ahead of demand off the hot path, and reserve returns to a pure check against `prepared` that
parks when the preparer has not caught up. The quantity model does not change with who moves
`prepared`; only the actor does (see [Future Work](#background-capacity-preparer)).

Growth also runs where debt is incurred, and there it is not bounded by `configured`. An unreserved
claim that finds no free carrier maps a block to serve it, because the alternative is refusing memory to
a read that cannot be refused. This is the only path that raises `prepared` above `configured`, and the
excess is the overage the cap tolerates by design (see [bound resident data-plane
memory](#bound-resident-data-plane-memory-across-the-whole-client)). Carriers obtained this way are ordinary carriers in
ordinary blocks: the arena has no separate allocation class for them, and nothing about a block records
whether it was mapped inside or outside the budget. That uniformity is what makes the excess
self-liquidating — a returned overage carrier is immediately reusable by the next claim, so reuse
absorbs it without any reclamation step, and trim removes the mapping only if it stays idle.

Reclaiming capacity — trim — lowers `prepared` by unmapping an idle block, and it is the reverse of
growth in two layers, an admission layer and a physical layer, matching the two things a block is.

The admission layer moves `prepared`, and it is where trim must not race reserve. Before a block can be
taken out of service, trim checks that removing it leaves enough prepared capacity to cover every
outstanding reservation and the debt: `prepared - block_carriers >= reserved + debt`. If that fails, the
block's capacity is still needed and trim does not touch it. Including debt in the floor is what stops
trim from reclaiming the very blocks that were mapped to keep already-granted envelopes claimable, which
is precisely the situation while debt is outstanding. If it holds, trim lowers `prepared` by the block's
carrier count and marks the block draining, and it does both under the cap's serialization so that no concurrent
reserve can grant an envelope against the capacity being removed. This floor check is what preserves
[reserved implies claimable](#reserved-implies-claimable) across a trim: the capacity a reservation
counts on is guaranteed to remain in some *other* block. A separate steady-state floor keeps `prepared`
from dropping below a working headroom, so operation between bursts does not repeatedly map and unmap the
same block, and hysteresis holds a block draining-eligible only after it has been idle past a threshold
rather than the instant it empties.

That same floor is how the overage retires. Blocks mapped above `configured` to serve unreserved claims
are held only while `reserved + debt` needs them, so as debt falls the floor drops and those blocks
become eligible in turn. Nothing distinguishes them from the rest — they are reclaimed by the ordinary
trim path, which is why the excess needs no separate accounting to disappear. Reclamation is
block-granular, so a single carrier of debt can hold a whole block above the cap until it is repaid,
which is one reason to keep the block size small relative to the cap.

The physical layer unmaps, and it is where trim must not race a claim. A block is eligible only when all
of its carriers are free — one live carrier pins its block, since a block cannot be partially unmapped
without disturbing the live carrier's neighbours — and taking it out of service safely against concurrent
claims is the subject of [unmapping a block safely](#unmapping-a-block-safely).

What is open is the *trigger*, because reclamation is driven by idleness, which is the absence
of the activity that would trigger it. A pool holding peak resident memory between bursts is calling
neither reserve nor claim, so no hot-path event fires to notice the blocks have gone idle. The trigger
must therefore be independent of activity — a timer, a periodic sweep, or a memory-pressure signal —
which is what makes trim the operation that most wants a background actor even while growth stays inline.
Which trigger is left open (see [Open Questions](#what-triggers-trim)).

**Alternative: grow only in a background preparer from the start.** Preparing all capacity off the hot
path keeps reserve a pure check even when preparation is cheap. Rejected for the first version, where
preparation is a single fast `mmap`: a background actor is machinery whose cost is not justified while
growth is cheap, and an inline `reserve() -> Result` surfaces allocation failure directly to the
transfer instead of leaving it parked against capacity a background actor silently failed to prepare.
The background preparer earns its place when preparation becomes expensive, above, not before.

### Parking and granting

When a reserve does not fit, it parks. Parked reserves are a FIFO queue, and the queue is what makes
admission both starvation-free and free of the retry storms a wake-and-race scheme produces. When a
reservation drops and returns its envelope, the freed carriers are handed directly to the waiters that
fit, front to back, rather than left for waiters to wake and re-contend for.

The drain walks the queue from the front and grants each waiter whose envelope fits the freed capacity,
and it **stops at the first waiter that does not fit** rather than skipping ahead to a smaller one
behind it. Stopping is what bounds the wait: a large reserve at the front is never passed over by a
stream of smaller reserves behind it, so once a reserve is queued, capacity only accumulates toward it
until it is granted. The cost is head-of-line idling — a large front waiter holds freed carriers idle
while they accumulate to its envelope — which arises only under mixed sizes; a uniform size never
triggers it.

```text
  a release frees capacity; the drain sweeps the queue front to back

        front ───────────────────────────────► back
      ┌───────┐   ┌───────┐   ┌───────┐   ┌───────┐
      │ fits  │   │ too   │   │ would │   │  ...  │
      │ grant │   │ big   │   │ fit   │   │       │
      └───────┘   └───────┘   └───────┘   └───────┘
       granted      STOP      not reached

  the drain stops at the first that does not fit, though a smaller reserve
  behind it would fit; serving it early would let small reserves defer the
  larger one without bound. Holding order bounds the larger reserve's wait.
```

A woken waiter collects a reservation that already exists; it does not re-run reserve. The drain moves
the freed envelope into the waiter's slot as a real reservation and then wakes it, so the woken transfer
takes a grant that has been set aside for it rather than racing other threads for capacity that may be
gone by the time it runs. The grant is recorded before the wake fires, and the wake fires after the cap
lock is released, so a wake is never lost and the wake path never nests locks. A transfer that is cancelled after its grant is recorded drops that grant like any
reservation, returning the envelope and draining the next waiter; a transfer cancelled while still
waiting removes itself from the queue, which can expose capacity that arrival order had held behind it,
so that removal also drains.

### Requests larger than the whole cap

A reserve whose envelope exceeds the entire cap cannot be satisfied by waiting, since the capacity to
fit it will never exist. The admission gate `reserved + debt + envelope <= configured` can never pass
for it, so it would park forever. The escape is a single exception: when `reserved == 0`, meaning no
other reservation holds any capacity, the front waiter is granted unconditionally, ignoring the gate.

The condition is `reserved == 0` and not the fully drained `reserved == 0 && debt == 0`, and the reason
is that the exception is only worth anything if the condition it waits on is one admission can force.
`reserved` is entirely this subsystem's: every envelope in it was granted here, FIFO drains the queue
ahead of the oversized waiter, and reserve grants nothing new while the queue is non-empty, so
`reserved` reaches zero. `debt` is not. It falls only as the carriers behind it are returned, and where
the buffer source is shared with code outside the client those returns are not the client's to make.
Waiting for `debt == 0` would reinstate the indefinite park the exception exists to remove, and would do
it precisely in the configuration where debt can stand indefinitely.

So the overshoot is `envelope + debt` above the cap rather than `envelope` alone, and the two terms are
independent: neither compounds within its own class, and they add rather than multiply. At most one
oversized reservation is outstanding, because requiring `reserved == 0` means no further envelope is
granted until this one releases. Debt is bounded by cause, and while the oversized reservation holds the
cap no new work is admitted to add to it.

The forced grant has a physical consequence as well as an accounting one. Backing every carrier the
grant makes claimable means `prepared` must reach an oversized `reserved`, so this is the second path
that maps above `configured` — the first being an unreserved claim that finds nothing free. Reserve
grows to serve it, and the excess retires the same way any other does: when the reservation drops, the
trim floor falls below the blocks that were mapped for it and they become eligible on the ordinary path.

A download never hits this: it slices to a part size well under the cap. It is the backstop for an
upload of a single object whose part does not fit the cap, or any single reservation larger than the
whole. Without it, such a request would park forever behind a bound it can never meet. This is distinct
from the case where an envelope exceeds `configured` but the transfer can choose a smaller part: there
the transfer reduces its part size to something reservable, and the forced grant is only for a
reservation that is intrinsically larger than the cap and cannot be subdivided.

**Alternative: wake all waiters and let them re-contend.** Freeing capacity and waking every waiter to
re-run reserve lets them race for it. Under contention this starves: a waiter that loses re-parks at
the back, and can be passed over without bound. Granting in queue order makes the front waiter's
admission monotonic rather than a repeated lottery.

**Alternative: priority-ordered admission.** Granting higher-priority reserves first would let an
interactive transfer take memory ahead of a background one. Without a way to revoke memory already
granted to a lower-priority transfer, priority in the queue alone is a half-measure: it reorders who
waits, but a background transfer already holding capacity still blocks a foreground one until it
releases on its own. Admission stays FIFO until reclaim exists to make priority meaningful.

### Contiguous runs and disk I/O

A work buffer's carriers do not need to be contiguous in memory: it is delivered as a sequence of
segments, each consumed in order, and vectored I/O carries a scattered set in a single call. Because
every carrier is page-aligned and a whole number of pages, a scattered set already satisfies the kernel's
`O_DIRECT` vectored-alignment rule — which requires each segment's address *and* length to be a multiple
of the device's logical block size, a rule a page-multiple carrier meets on any device — so a `preadv` or
`pwritev` over disjoint carriers is one legal syscall regardless of where they sit. Contiguity is not
required for correctness or to keep the syscall count down. This covers whole carriers; a segment published
at an offset *within* one is a separate problem, taken up in [payload alignment within a
carrier](#payload-alignment-within-a-carrier).

Where contiguity pays is narrower, and the arena serves it opportunistically. Adjacent carriers in a
block are one contiguous region, so a claim that finds a run of free carriers hands back one segment
covering all of them instead of several. This shortens the vectored I/O's descriptor list, and it is the
form a future io_uring fixed-buffer read or write wants, where one registered buffer maps one contiguous
file range. The reservation guarantees a *count* of carriers, never their adjacency, so a claim returns
the full count grouped into as few segments as the free bitmap currently allows — one segment when a run
is available, several when the free carriers are scattered — and never returns short. The claimability
argument and the bound are unaffected by whether a run is found; only the segment count of the result
changes. A claim never withholds count to hold out for contiguity: a short-but-contiguous result would
weaken the count guarantee exactly when the arena is fragmented, and vectored I/O handles several
segments anyway.

On the upload side, where a whole part is claimed at once, a run is often available and the claim captures
it. On the download side runs form differently, at delivery rather than claim: as frames are pushed into
the buffer, a frame that abuts the back segment — same block, and its pointer meeting the segment's end —
extends that segment instead of starting a new one, so consecutive carriers coalesce into one segment even
though they were claimed one socket read at a time. The merge tests only block identity and pointer
adjacency, never a carrier index, so it applies to arena-claimed carriers and to transport-published views
alike; foreign memory (`block: None`) never abuts and stays its own segment. How many segments a delivered
part carries then tracks the run length the arena handed out, not the frame count, which is what makes the
`pwritev` descriptor count track carriers rather than framing. So contiguity is an optimization the arena
produces when it can — at claim on upload, at delivery on download — and never depends on.

The tail of a file is the one place `O_DIRECT` alignment is not free. A read whose length is rounded up
past end-of-file is legal — the kernel returns a short count and the carrier's valid prefix is used — so
the upload read path needs no special handling. A write of a final sub-block tail is *not* legal under
`O_DIRECT`, which rejects a length that is not block-aligned. Rather than pad the write and correct the
file size afterward, the design writes the aligned whole-block portions with `O_DIRECT` and the final
sub-block tail with one ordinary buffered write. Because the file is preallocated to its exact size, that
buffered write extends nothing and needs no truncation to fix up, and because the tail region is disjoint
from every direct-written region and written last, the buffered and direct writes never touch the same
pages. Whether the buffered tail is issued through a second descriptor or by clearing `O_DIRECT` on the
one descriptor is an implementation detail; the model is a direct body with a buffered tail.

### Payload alignment within a carrier

The alignment argument above is about *carriers*: a carrier's base is page-aligned and its length is a page
multiple, so a vectored direct write over whole carriers is legal. It does not extend to what a transport
publishes out of one. A buffer source hands over writable capacity; the transport decides where within that
capacity payload lands, and it may publish a view beginning partway in. Three short frames read into one
carrier and published separately yield views at offsets 0, 5 KiB, and 12 KiB. Only the first is
page-aligned. Handed to `pwritev` under `O_DIRECT`, the other two are rejected, because the kernel's
constraint is on each iovec's address and length, not on the buffer they were carved from.

Carrier alignment is therefore necessary and not sufficient, and the offset is the transport's to choose,
not the arena's.

**The offset propagates; it does not stay local.** Write `c` for the offset within a carrier at which payload
begins. A payload byte at index `p` in the body sits at address `carrier_base + ((p + c) mod carrier_size)`,
and its file offset is `part_base + p` for a page-aligned `part_base`. The kernel constrains both, so a direct
write starting at `p` requires

```
p     ≡ 0 (mod page)        file offset
p + c ≡ 0 (mod page)        buffer address
```

which together require `c ≡ 0 (mod page)`. With 4 KiB pages, 64 KiB carriers, and payload beginning 3000 bytes
into the first carrier:

| carrier | address    | payload index | file offset | address | length | file offset |
|---------|------------|---------------|-------------|---------|--------|-------------|
| first   | `A + 3000` | 0             | 0           | no      | no     | yes         |
| second  | `B + 0`    | 62536         | 62536       | yes     | yes    | no          |
| third   | `C + 0`    | 128072        | 128072      | yes     | yes    | no          |

Every carrier after the first is page-aligned in address and length, and every one of them fails on file
offset by the same residue, because a page-multiple carrier size preserves it. A misaligned start is not a
misaligned prefix followed by an aligned body: it is a constant phase shift applied to the whole part.

Nothing downstream absorbs that phase. Writing the prefix buffered up to the next page boundary advances `p`
to 65536, which satisfies the file-offset term, but the resumption point is then at address `B + 3000` —
misaligned by the same 3000. Both terms move together as the write pointer advances, so their difference stays
`c`; no starting point satisfies both unless `c` is itself page-aligned. Copying only the first carrier's
payload to offset zero of a fresh carrier fails for the same reason: the second carrier still begins at payload
index 62536. Repair is all-or-nothing per part, because the file's aligned cut points are fixed by the wire and
an extent spanning a carrier boundary is not contiguous in memory.

**So `c ≡ 0 (mod page)` is a requirement on the buffer source**, and the only one that matters. Stated as a
term of the contract: framing does not share a carrier with payload, and payload is written densely from
offset zero. A source that meets it makes every interior carrier writable directly on all three terms, leaving
the body's final carrier short in length alone — the tail case the write path already handles with one buffered
write. A source that does not meet it makes no part of that body directly writable at any offset.

Whether a source can meet it is decidable from the response headers, before any body byte lands, since it
turns on the framing the response declares. A transfer coding that interleaves framing with payload cannot
meet it at all: chunk size lines and CRLFs re-establish `c` at every chunk boundary at arbitrary values, so
there is no single condition to satisfy. Keeping that framing out of carriers is not a matter of asking,
either — `hyper` reads chunk framing one byte at a time ([`decode.rs:252`][hyper-chunk-byte]) through the same
interface that yields payload ([`decode.rs:486`][hyper-chunk-body]), so a buffer source that is unaware of
protocol state hands framing bytes a carrier along with everything else. Direct I/O is therefore a property of
a response's framing, classified when its headers are parsed, not a property of a transfer or of the sink.

Nor is the requirement free for framing that *can* meet it. HTTP/1 serves header parsing and body delivery from
one buffer: headers are parsed in place ([`io.rs:169`][hyper-parse]) and payload leaves as a subslice at
whatever offset parsing stopped at ([`io.rs:352`][hyper-read-mem]). HTTP/2 is structurally easier, in that a
frame's payload length is known before its payload arrives, but not free either: a frame header is consumed
from the same buffer as the payload behind it ([`framed_read.rs:229`][h2-frame-advance]), and nine bytes is
never a page multiple. In both cases `c = 0` requires the transport to recognize that framing is complete and
begin payload in a fresh buffer. What makes that a bounded ask rather than a rewrite is read-ahead: bytes of
body already sitting in the framing buffer can be copied into the fresh carrier's first bytes, after which
every subsequent read appends densely. The cost is one copy of a read's worth of bytes per response, against a
copy of every byte of every part.

**Two routes reach an aligned write, and they are policy rather than architecture.** The requirement above is
what a zero-copy download path needs. A copy at delivery reaches the same aligned write without it, by
claiming a carrier when a body chunk arrives and copying into it from offset zero — which makes `c = 0` hold
by construction, for any framing, without asking the transport for anything.

| | payload first lands in | copies | direct-I/O eligible |
|---|---|---|---|
| carrier, `c = 0` | arena | none | framing that can meet the requirement |
| carrier, copy at delivery | arena | one per chunk | any framing |
| transport's own buffer, copy at delivery | transport allocation | one per chunk | any framing |

The rows differ in two independent ways, which is why this is not a single either/or. Whether payload lands in
a carrier decides *accounting*: a transport reading into arena memory has its receive buffers inside the cap
and visible to the same instrumentation as everything else, whether or not a copy follows. Whether `c = 0`
holds decides *copies*. A design can take the first without the second, and the middle row is exactly that:
one cap and universal direct I/O, at one copy per chunk.

Copying is cheaper than it appears from the copy count, which is why the middle row is a real position rather
than a concession. A delivery-time copy is synchronous with the read, so the source buffer returns immediately
and the transport's working set is sized by concurrency rather than by bytes in flight. What direct I/O buys in
that route is unchanged — no page-cache pollution, no double-buffered resident set, no writeback stalls — and
those are the reasons for wanting it at high throughput. The copy elimination is a further win on top, not the
whole of it.

The design therefore treats the route as internal policy, selected per response from its declared framing, and
not as a user-facing choice: the condition turns on framing, filesystem, and cache pressure, none of which a
caller can see at call time. A caller expresses intent to use direct I/O; the policy decides how to honor it.
The routes also compose in the order they become available, since each is complete on its own — reading into
carriers is an accounting win that stands whether or not the copy is eliminated later.

**Coalescing published views.** Under short reads a carrier can arrive as several `Bytes` that together cover
it, and the sink has to recognize that to write it directly — three 5 KiB views are three unaligned writes,
while the 15 KiB they span may be part of an aligned run. A `Bytes` does not say which carrier it came from,
or that it came from the arena at all; its vtable is opaque. What the sink can test is that one view's
pointer plus its length equals the next view's pointer and their file offsets are likewise contiguous, which
identifies a run to merge into a single `iovec` without knowing anything about carriers. Adjacency also
fails correctly: framing bytes left between two payload views break pointer contiguity, and those views
must not be merged. Because merging produces one descriptor spanning both views, the run must lie within a
single mapping, which pointer adjacency alone does not establish — two independent mappings can abut. The
arena resolves that from the pointer, since a carrier's address is its block's base plus an index times the
carrier size, so the block table maps an address back to the carrier containing it. Whether the sink merges
by that resolution or receives something that already carries carrier identity is part of the delivery
type's shape (see [open questions](#the-shape-of-the-delivery-type)).

[hyper-parse]: https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h1/io.rs#L169
[hyper-read-mem]: https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h1/io.rs#L352
[hyper-chunk-byte]: https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h1/decode.rs#L252
[hyper-chunk-body]: https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h1/decode.rs#L486
[h2-frame-advance]: https://github.com/hyperium/h2/blob/v0.4.12/src/codec/framed_read.rs#L229

**Alternative: reach alignment by always copying, and latch the whole transfer off direct I/O on first
misalignment.** This is the CRT's model, and its receive path cannot do otherwise: transport buffers come from
a pool whose data area shares an allocation with the message header
([`message_pool.c:132`][crt-msg-wrapper], [`:168`][crt-msg-buffer]), placing payload at a fixed nonzero offset
in an unaligned allocation, so `c = 0` is unreachable there by layout. The S3 layer `memcpy`s each body chunk
into a page-aligned pool buffer claimed with length zero ([`s3_meta_request.c:1702`][crt-body-append],
[`s3_default_buffer_pool.c:782`][crt-ticket-claim], [`:258`][crt-aligned-alloc]) and gates the remaining terms
at init, refusing direct I/O unless the part size and base position are page multiples
([`s3_meta_request.c:406`][crt-partsize-gate]). Alignment then holds by construction and the only misalignment
left is the file's own tail, so a whole-request latch ([`:2199`][crt-fallback]) costs nothing — it fires once,
at the end. Rejected as *the* model, rather than as one route, because it forecloses the zero-copy path: the
requirement is satisfiable for some framing here, the routes are selectable per response, and a latch would
let one short read cost a transfer its direct I/O for the remainder. The always-copy route is kept as the
policy for framing that cannot meet the requirement.

[crt-msg-wrapper]: https://github.com/awslabs/aws-c-io/blob/v0.27.5/source/message_pool.c#L132
[crt-msg-buffer]: https://github.com/awslabs/aws-c-io/blob/v0.27.5/source/message_pool.c#L168
[crt-fallback]: https://github.com/awslabs/aws-c-s3/blob/v0.13.3/source/s3_meta_request.c#L2199
[crt-partsize-gate]: https://github.com/awslabs/aws-c-s3/blob/v0.13.3/source/s3_meta_request.c#L406
[crt-body-append]: https://github.com/awslabs/aws-c-s3/blob/v0.13.3/source/s3_meta_request.c#L1702
[crt-ticket-claim]: https://github.com/awslabs/aws-c-s3/blob/v0.13.3/source/s3_default_buffer_pool.c#L782
[crt-aligned-alloc]: https://github.com/awslabs/aws-c-s3/blob/v0.13.3/source/s3_default_buffer_pool.c#L258

### Unmapping a block safely

Unmapping is where trim meets the hot claim path, and two things can be freed with different lifetimes.
The **mapped region** — a block's carrier bytes — is what `munmap` releases. The **block record** — its
base pointer, bitmap, state, and generation — is a small control structure. Separating the two is what
makes the concurrency tractable.

The block record is never freed. A block occupies a stable position in the arena's block collection for
the arena's life, and its record and bitmap outlive any single mapping. Because the record never moves or
frees, a thread that holds a reference to a block — or resolves one from a carrier's block index — can
always read its status and bitmap safely; only the *region* it points at may come and go, and that is what
the gate below protects.

```text
   ┌─────────┐
   │  empty  │  no mapping, no claims; the record and bitmap still exist
   └────┬────┘
        │  revive: map a region, initialize the bitmap all-free,
        │          then publish (active, generation + 1) in one store
        ▼
   ┌─────────┐
   │ active  │  mapped, carriers claimable
   └────┬────┘
        │  drain: publish (draining, same generation)
        │         only for a block already observed idle and all-free
        ▼
   ┌──────────┐
   │ draining │  mapped, no new claims
   └────┬─────┘
        │
        ├── confirming read: all carriers still free
        │      └─► munmap the region, publish empty ─────────────► empty
        │
        └── confirming read: a carrier was taken
               └─► abandon: revert to active, restore `prepared`,
                   reclaim a different block later ──────────────► active
```

Trim unmaps the *region* and returns the block to **empty**; growth later revives an empty block by
mapping a fresh region into it. Revival initializes the region and bitmap *before* publishing `active`, so
a claim that sees `active` sees a fully initialized block, never a half-mapped one.

A block's state and its generation live in one word. A claim needs both — the state to know the block is in
service, the generation to know it is the same incarnation the claim's name refers to — and reading them
as one load makes the pair consistent by construction rather than by argument. Revival advances the
generation and sets the state in the same store.

The dangerous interval between trim and a claim is narrow. Once a claim has flipped a carrier's bit to
in-use, the block has a live carrier, and trim's all-free requirement forbids unmapping it — so the
address handed out stays valid for the whole life of the segment buffer with no further protection. The
only race is the instant *before* the bit is taken: a claim has selected a still-free carrier in a block
that trim is simultaneously deciding is all-free and about to unmap. Closing that window is a mutual
signal between the two:

- Trim publishes **draining**, then reads the bitmap to confirm all carriers free.
- A claim sets its carrier's bit, then reads the block's status; it keeps the carrier only if the state is
  **active** *and* the generation matches the one its name refers to. Anything else — draining, empty, or a
  generation that moved — and it clears the bit and claims elsewhere.

Each side writes its flag and then reads the other's, so neither can complete while blind to the other:

```text
     trim                                    claim
     ────                                    ─────
  T1 publish draining                     C1 set the carrier's bit
        │                                       │
  T2 read the bitmap                      C2 read the block's status
        │                                       │
        ├─ all free   → unmap                   ├─ active & generation matches → keep
        └─ bit taken  → abandon                 └─ anything else → clear the bit, retry

  Both sides completing wrongly requires each load to miss the other's store:

      T2 missed C1  ⇒  T2 before C1
      C2 missed T1  ⇒  C2 before T1

  with T1 before T2 and C1 before C2 fixed by program order, this closes a cycle —
  T1 → T2 → C1 → C2 → T1 — and no single total order over the four accesses admits it.
```

The requirement is therefore a total order over those four accesses: sequential consistency on them, or an
explicit full fence between each side's store and its load. Acquire/release is insufficient, because the
hazard is precisely the store-load reordering it permits — with it, both loads may miss both stores, the
cycle opens, and trim unmaps a region a claim is about to write into. This is the interleaving the design
is most likely to lose in review, since the orderings look stronger than the surrounding code needs.

The generation is checked here for a reason beyond validating a shard hint. Between a claim reading a
block's status and setting its bit, the block could be drained, unmapped, and revived — a new mapping in
the same record, reading `active` again. The claim's bit would have landed on the previous incarnation's
bitmap, which revival zeroed, so the claim would hold nothing while believing it holds a carrier, and a
later claim could hand out the same carrier. Revival advances the generation, so a mismatch means
"different incarnation" and the claim backs out. State and generation are read together, after the set, on
every claim path including the shard-hint fast path: state answers whether the block is in service, and
generation answers whether it is the same block the name meant. Neither question subsumes the other.

This is the subsystem's sharpest ordering argument and is stated as a [correctness
invariant](#unmapping-never-races-a-live-carrier).

A carrier's address is its block's base plus an index times the carrier size, so resolving a name to an
address reads the block's base pointer — which trim clears at unmap. That read belongs *after* the gate, not
before. A claim that passes the gate holds a set bit, and a set bit forbids unmap, so the base it then reads
cannot be cleared while it holds that carrier. A claim that backs out never resolves an address at all, so
the pointer it would have computed is never formed. The base pointer therefore needs no ordering of its own;
it inherits the gate's. This is also why a shard hint carries a block index, a carrier index, and a
generation, but never an address: an address cached beside a hint would have been resolved before the gate,
and a hint outlives the incarnation it names.

A claim that loses this race — sets a bit, finds the block draining, and backs out — is guaranteed to find
capacity elsewhere, because trim only began draining after the [floor
check](#preparing-and-reclaiming-capacity) confirmed the remaining blocks cover every reservation. Trim,
for its part, does not wait for that claim: seeing the bitmap not-all-free at its confirming read, it
**abandons** this block — reverts the draining mark and its `prepared` reduction, leaving the block in
service — and reclaims a different one later. A block that reached draining was idle by policy, so a claim
arriving mid-drain is rare; abandoning is simpler than holding a wait, and it needs no in-flight-claim
counter.

Because a block only unmaps when all carriers are free, there is never a *live* carrier name to invalidate
at unmap; the names the generation guards are stale shard hints, discarded on the mismatch exactly as a
stale-but-taken hint is.

Trim touches the hot path only through the one word a claim already reads, and the `munmap` itself runs off
the hot path, on whatever actor the trigger drives.

**Obligations** (discharged in [Appendix C](#appendix-c-verification)).
- The gate admits no cycle over its four accesses, so a total order over them is required.
- Revival publishes `active` only after its region is mapped and its bitmap initialized.
- A carrier's address is resolved only after the gate passes, and a claim that backs out resolves none.
- A claim keeps a carrier only on `active` and a matching generation. Draining and empty are distinct
  rejections, and neither relies on the generation check to catch it.
- The generation is assumed not to wrap within a hint's lifetime. A false match requires the number of
  revivals between stashing a hint and checking it to be exactly a multiple of the generation field's range;
  every other value mismatches. The consequence if it did occur is aliased writes into one carrier, so the
  field is sized to make the assumption hold by margin rather than by argument.

### Sizing the cap

The cap is resolved once at construction, against memory detected from the machine. Detection is
cgroup-aware: it takes the smaller of the cgroup limit and physical memory, so a container sees its own
limit rather than the host's. Three policies:

- **Auto** (default): a safe fraction of detected memory, rounded to a power of two and clamped to a
  floor and ceiling. The fraction leaves the majority of memory for the OS page cache and the rest of
  the process; the clamp keeps a small container usable and stops a very large host from reserving more
  than the pipeline can use. A conservative fixed default applies when memory cannot be detected.
- **Fraction(f)**: an explicit fraction of detected memory, floored but not capped, taken at the
  caller's word on a large host.
- **Limit(bytes)**: an explicit ceiling, bypassing detection.

The cap is a backstop, not the operating point. The concurrency controller settles throughput at line
rate well below it, and it binds only when a slow consumer backs data up. Detected memory is an
imprecise proxy for the right ceiling, since the quantity that actually bounds a useful cap is network
bandwidth, which is uncorrelated with memory across instance families; the clamp keeps the estimate safe
at both ends, and bandwidth-aware sizing is a later refinement (see
[Open Questions](#memory-is-an-imprecise-proxy-for-the-cap)).

**Alternative: a fixed absolute default.** A constant ceiling is simple but wrong across the range of
hosts the client runs on: safe on a large host starves a small one, and safe on a small host under-uses
a large one. Sizing from detected memory adapts the default; the clamp bounds the adaptation.

### Observability

Two surfaces: a pull-only snapshot for programmatic use, and `tracing` at a named target for diagnosis.
Neither is a metrics facade and neither runs a reporting task — the snapshot is computed when asked, and
log records are emitted by the paths that already run.

**The snapshot.** One method, taking the budget's lock once, converting the ledger's carrier counts to
bytes. The four fields the shipped type already has keep their meaning, so this extends rather than
replaces it; `#[non_exhaustive]` is what makes the extension additive.

```rust
/// Snapshot of the memory subsystem's admission and capacity state.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct MemoryBudgetSnapshot {
    /// The resolved ceiling — what the configured policy produced on this machine.
    pub capacity_bytes: u64,
    /// Capacity mapped and ready to serve claims. Rises with growth, falls with trim,
    /// bounded by `capacity_bytes`.
    pub prepared_bytes: u64,
    /// Bytes granted to live reservations. An admission level, not a residency figure: a
    /// reservation is charged its whole envelope from grant, before any of it is claimed.
    pub reserved_bytes: u64,
    /// Bytes in claimed carriers. The subsystem's actual footprint, and the only field here
    /// that tracks residency.
    pub live_bytes: u64,
    /// Live consumption drawn without a reservation — what a buffer source spent. Covered by
    /// declared coverage where a transfer declared any, and by `debt_bytes` where it did not.
    pub unreserved_bytes: u64,
    /// The part of unreserved consumption no coverage absorbed. Repaid by returns.
    pub debt_bytes: u64,
    /// Reservations parked waiting for a grant. Zero means admission is not binding now.
    pub waiters: usize,
    /// Cumulative parks, maps, unmaps, and entries to the serialized claim path. Monotonic;
    /// callers difference them to get rates.
    pub counters: MemoryBudgetCounters,
}
```

`capacity_bytes`, `prepared_bytes`, `reserved_bytes`, `debt_bytes`, and `waiters` are ledger quantities read
as they stand (see [Appendix D](#appendix-d-the-admission-ledger)); `unreserved_bytes` is the derived
`(declared − avail) + debt`. `live_bytes` is not a ledger field and is deliberately not maintained as one: a
global live counter would add a second contended atomic to a claim path built to touch one word (see
[claiming and returning a carrier](#claiming-and-returning-a-carrier)). It is computed on demand instead, by
population count over the in-use bitmaps — one pass over `carriers / 64` words, on a cold path, against a
hot path that stays at one atomic.

`reserved_bytes` and `live_bytes` differ by granted-but-unclaimed envelope, which is normal and large on the
download path because coverage is envelope a transfer never claims itself. Neither bounds the process's
resident set: the cap bounds admitted work and prepared capacity, and a flattening copy taken by a consumer
is memory the ledger has no term for (see [the shape of the delivery
type](#the-shape-of-the-delivery-type)).

Monotonic counters rather than rates or windows, because a caller sampling twice derives the rate and the
subsystem then keeps no window state: `parks`, `blocks_mapped`, `blocks_unmapped`, `serialized_claims`. A
map count that climbs alongside an unmap count is growth and trim thrashing the same capacity; a
`serialized_claims` count that climbs is the fairness backstop firing under real contention rather than
sitting idle.

**Two derived quantities that the ledger cannot yield.** Park duration and retention age are durations, not
levels, so each needs state added deliberately.

Park duration is recorded where the grant already happens: the waiter carries its enqueue instant, and the
grant computes the elapsed time under the lock it is already holding. It feeds an exponentially weighted
mean and a high-water mark, not a histogram — it is a diagnostic, never a control input, and the crate's
existing latency machinery is the precedent for an EWMA over an accumulated distribution.

Retention age is the age of the oldest live *reservation*, not of the oldest carrier. A per-carrier
timestamp would cost a second cache line written on every claim and megabytes of metadata at cap scale, to
answer a question the reservation already answers: a consumer holding delivered data holds the reservation
that accounts for it, so the reservation's grant instant is the retention clock. Consumption with no
reservation behind it has no such clock, and standing debt is its signal instead.

**What is logged, and at which level.** Every record sets the scheduling target explicitly, so admission and
capacity events filter with the scheduler's, which is the concern they share. The level rule is the one the
shipped budget already applies: steady-state per-item events are `trace`, edges a human cares about are
`debug`.

| event | level | why |
|---|---|---|
| reserve granted on the fast path; grant to a parked waiter; reservation released | `trace` | per unit of work, and unremarkable |
| a reserve parks | `debug` | a park is a backpressure event, not steady state — with `need`, `reserved`, `configured`, and queue depth |
| a block is mapped or unmapped | `debug` | capacity changed; once per block, and the pair is what shows thrashing |
| an unreserved claim raises debt | `trace` | routine on the transport path; standing debt is the gauge that matters, not the event |
| a claim falls through to the serialized path | `trace` | correct behaviour under contention; the counter is the signal |
| a map the operating system declines | `warn` | the residual failure, returned to the caller |
| the resolved cap, and a cap raised to the one-block floor | `debug` | emitted once, so the effective configuration is reconstructable from logs |

Standing `debt_bytes` and the oldest reservation age are the two figures that carry information no single
level does. Debt is the only place unreserved consumption becomes visible, so a debt that does not fall is
retention the arena cannot attribute — including retention inside a buffer source shared with code outside
this client. Oldest-reservation age separates a slow consumer from a leaked one: a bounded and an unbounded
working set are indistinguishable at any single instant, and differ only in whether the oldest age stops
growing.

---

## Ecosystem

Everything above concerns memory the subsystem owns; what follows is the boundary where that memory leaves
it. Arena carriers have to arrive at consumers as `bytes::Bytes` and `bytes::Buf`, because that is what the
SDK body type and every byte sink in the ecosystem accept. The arena is not free to choose its delivery
representation, and the constraints that representation imposes reach back into the design: they decide the
granularity at which carriers return, and they set a floor on what delivery costs in allocations.

### Delivering arena memory as `Bytes`

`Bytes` is not a choice. The `http_body::Body` trait is generic in what a body yields: `type Data: Buf`
admits any buffer type ([`http-body` `lib.rs:38`][http-body-trait]). The SDK closes that generic. `impl Body
for SdkBody` fixes `type Data = Bytes` ([`http_body_1_x.rs:49`][sdk-body-impl]), and every constructor
accepting a caller-supplied body requires `Body<Data = Bytes>` as a compile-time bound
([`:19`][sdk-body-impl]). There is no variant, feature flag, or escape hatch admitting another buffer type.
So payload must be expressible as `Bytes` where it crosses into the SDK, whatever representation the
subsystem uses internally, and `Bytes::from_owner` is the only public construction that does it without
copying — which makes its per-owner allocation a floor on delivery cost rather than an implementation detail
that could be optimized away.

A `Bytes` is a pointer, a length, a data pointer, and a vtable ([`bytes.rs:279`][bytes-from-owner]). The
vtable is what makes it possible to hand out arena memory at all: a `Bytes` need not own a heap
allocation, only something that keeps its pointer valid. `Bytes::from_owner` is the public constructor for
that case, taking any `T: AsRef<[u8]> + Send + 'static`, boxing it, and calling `as_ref()` exactly once to
capture the pointer and length ([`bytes.rs:254`][bytes-from-owner]).

The owner is a carrier handle: a small value holding the segment's pointer, its length, its carrier ID,
and a reference to the arena. `from_owner` boxes that handle, which moves it, but `as_ref()` returns a
slice built from the *pointer inside* it, which addresses the mmap'd block rather than the handle's own
storage. The handle's location is therefore irrelevant to the validity of the bytes. What keeps them valid
is the arena reference the handle holds: while the handle lives the block stays mapped, because a live
carrier's bit forbids unmap (see [unmapping never races a live
carrier](#unmapping-never-races-a-live-carrier)), and the handle lives as long as the `Bytes` does.

Return is the owner's `Drop`, but the carrier is not the owner. A single carrier is published as several
independent `Bytes` — a transport reads three short frames into one carrier and hands each up as its own
view — and each of those is a distinct `from_owner` with its own boxed refcount, because the transport
mints them as it reads, not by slicing one owner it holds over the whole carrier. Nothing may hold a `Bytes`
over a carrier while later frames are still being written into its tail: `from_owner` calls `as_ref()` once
and captures a slice over the *whole* owner, so a live view over the unwritten tail would alias the writer's
`&mut`. The owner is therefore per view, and the carrier it names must return exactly once no matter how
many owners name it.

A shared **carrier guard** supplies that. The owner each view carries holds an `Arc<CarrierGuard>`; the
guard's `Drop` returns the one carrier to the arena, and an `Arc` runs it only when its last clone drops.
Every view over a carrier holds a clone of the same guard, so the carrier returns when the last view over
it drops — and not before — regardless of how many `from_owner` boxes exist:

```text
  one 64 KiB carrier, three published frames

  ┌──────────┬──────────┬──────────┬────────────────────────┐
  │  5 KiB   │  7 KiB   │  9 KiB   │   43 KiB unwritten     │
  └────┬─────┴────┬─────┴────┬─────┴────────────────────────┘
       │          │          │
    Bytes A    Bytes B    Bytes C     each its own from_owner, ptr/len over its frame
       │          │          │
     guard      guard      guard      an Arc<CarrierGuard> clone apiece
       └──────────┴──────────┘
                  │
          one CarrierGuard  ──  its Drop returns  ──  one arena carrier

  the carrier returns when A, B, and C are all dropped;
  a surviving 5 KiB view pins the whole 64 KiB carrier.
```

The carrier, not the view, is therefore the unit of return, and a single surviving view holds its whole
carrier. The slack one live carrier can pin is strictly less than the carrier size, which bounds the
fragmentation but also ties it to the carrier size: a larger carrier strands more behind a short retained
view, which is why the size is chosen by measurement against the observed distribution of delivered chunk
sizes (see [open questions](#carrier-size)). The guard is per carrier, never per run: an `Arc` over a run of
carriers would pin every carrier in the run until the last byte of it cleared, so a guard covering more than
one carrier would trade the arena's carrier-exact return for the same coarse stranding a large carrier
causes.

Placing the count in the guard rather than beside the bitmap is deliberate. The alternative — a per-carrier
holder count in the block record, incremented on publish and decremented on return — would make carrier
liveness two facts that must agree: the bit the [claim path](#claiming-and-returning-a-carrier) and the
[unmap gate](#unmapping-never-races-a-live-carrier) both run on, and a count beside it. The guard keeps
liveness a property of the views themselves, so the bitmap stays the single source of truth and the return
path stays one atomic on it (see [Appendix A](#appendix-a-memory-ordering)).

**Alternative: a per-carrier holder count in the block record.** Give each carrier a `u32` count next to its
bit, so `Clone` and multi-view return read the count rather than an `Arc`. Rejected on two grounds. It is
purely additive — the gate and trim still decide on the bit, so the count replaces nothing and only adds a
second liveness fact to keep consistent, falsifying "the bitmap is the single source of truth" wherever that
claim is load-bearing. And it is not free: a `u32` per carrier is thirty-two times the bitmap's own
footprint and scales inversely with a carrier size that is itself a [measurement output](#carrier-size),
block revival must initialize it, and the return path grows from one atomic to a decrement, a fence, and the
bit clear. The guard pays one small allocation per carrier that is *in flight* instead of a standing cost
across *every* carrier the arena can hold.

Delivery costs one heap allocation per published view — the `from_owner` box — plus one `CarrierGuard`
allocation per carrier and a refcount bump per view sharing it. On the fill path a view is published per
read, which is the granularity at which the transport already allocates, so the boxes replace allocations
rather than adding them. There is no public way to construct a `Bytes` with a custom vtable, so `from_owner`
is the only route and it always boxes. A zero-allocation delivery — where the carrier handle *is* the
`Bytes` backing, with its refcount in the arena — needs a public custom-vtable constructor
([tokio-rs/bytes#437][bytes437]). This is a constraint on how well the boundary can be made to perform, not
a dependency: the design works today, and the allocation is the price.

The design leans on the `bytes` crate for exactly one guarantee: that the owner's `Drop` runs when the last
clone of a `Bytes` drops, which is documented behavior of `from_owner` ([`bytes.rs:248`][bytes-from-owner]),
not an internal detail. The carrier count is the arena's own — the guard's `Arc` — so nothing about carrier
return depends on how `bytes` implements its refcount. What `bytes` decides is only when one view's owner
drops; the guard decides when the carrier returns.

The vtable also fixes what a consumer can do with delivered memory. An arena-backed `Bytes` reports itself
as never uniquely owned ([`bytes.rs:1140`][bytes-from-owner]), so `try_into_mut` deep-copies instead of
handing back a mutable view. A consumer cannot obtain write access to a carrier the arena still accounts
for, which means the immutability of a delivered segment is enforced by the ecosystem rather than by
convention.

### The transport as a buffer source

On the download path the injected buffer source is the HTTP transport's receive path — concretely `hyper`
and, for HTTP/2, the `h2` crate beneath it. This is the one part of the design whose mechanism lives in
code this design does not own: `hyper` allocates its own receive buffers, and no released version exposes
an interface for supplying them. The arena reaches the download path through an extension to that
transport, and the upload path, where the transfer reads its own data, depends on nothing external.

The contract below is therefore stated in terms of what the arena requires from a buffer source, not as an
API. Which protocol states may read into a provided buffer, how a session attaches to a request, and what
h2 requires beyond `hyper` are transport-side questions specified separately. This subsystem depends on four
properties:

- **The transport writes into memory it did not allocate.** It is handed a buffer with writable capacity
  and reads socket bytes into it directly. A copy at this boundary would defeat the purpose: the
  transport's own allocation would still exist and still scale with concurrency.
- **The transport publishes a prefix as `Bytes`, backed by the arena's owner.** This is what makes return
  automatic. The published views ride the refcount described above, so the carrier comes back without the
  transport knowing anything about the arena.
- **A read cannot name a reservation.** The buffer source is called at a point where the transport knows
  the size it wants; the per-request identity that would name a reservation is not available there. This
  is why the unreserved claim path exists, and it is a property of where the call sits rather than a
  limitation to be negotiated away.
- **The transport decides where payload lands within the buffer.** It may read headers or framing into the
  same buffer, and it may retain a partially filled buffer's tail across reads. Payload therefore does not
  necessarily begin at the start of a carrier, which is the alignment problem taken up in [payload
  alignment within a carrier](#payload-alignment-within-a-carrier).

The first three follow from the shape of any buffer-supplying interface: a source that did not satisfy them
would not remove the transport's allocation. The fourth is a term the arena has to ask for explicitly,
because the straightforward implementation publishes bytes as soon as it reads them — and it is the one term
a buffer source may be unable to satisfy fully, which is why the disk sink does not assume it.

### `Buf` and `BufMut` over a discontiguous buffer

Neither trait requires contiguous storage, which is what lets a multi-segment buffer be delivered without
gathering it. `Buf` is a cursor: `remaining`, `chunk` returning the contiguous run at the current
position, `advance` moving the cursor, and `chunks_vectored` filling one `IoSlice` per disjoint region
([`buf_impl.rs:148`, `:181`, `:212`, `:255`][bytes-buf]). The immutable form is a queue of segments and a
front cursor; each segment holds its bytes' backing in one of two forms, split by who owns the memory:

```rust
struct Segment {
    ptr: *const u8,
    len: usize,
    hold: Hold,
}

enum Hold {
    // Carriers this buffer claimed and fills or writes itself. One guard per
    // carrier, so return is carrier-exact and a clone is one Arc bump per carrier.
    Carriers { block: u32, guards: VecDeque<Arc<CarrierGuard>> },
    // Views a producer published over memory the arena did not claim — the SDK-fed
    // download path, where the transport allocated. Their own refcounts hold the
    // memory. `block: None` marks foreign memory, which is what makes it unmergeable.
    Views { views: VecDeque<Bytes>, block: Option<u32> },
}
```

`chunk`, `advance`, and `chunks_vectored` walk the segment queue and the front offset; a single `advance`
may cross several segments, and it releases each backing the cursor moves past — a guard from the
`Carriers` arm, a `Bytes` from the `Views` arm — so a slow consumer's resident memory falls as it reads
rather than all at once when the buffer drops. Release granularity is the carrier on both arms: a guard
returns its one carrier when it is popped and its last clone is gone, and a `Bytes` frees whatever is
beneath it when the last view over that carrier drops. The `chunk` contract forces one detail — it must
return an empty slice if and only if `remaining` is zero ([`buf_impl.rs:173`][bytes-buf]) — so the cursor
skips a segment the instant it is exhausted rather than resting at its length, where a generic consumer
would read the empty `chunk` as end-of-buffer and truncate with bytes still queued. `chunks_vectored` fills
one `IoSlice` per remaining segment, so the whole buffer feeds one `pwritev`; the `chunk`-then-`advance`
idiom then moves segment by segment, and a discontiguous buffer is consumed correctly by code that has no
idea it is discontiguous.

The two arms are the concrete form of the [buffer-source](#the-transport-as-a-buffer-source) distinction.
When the arena supplies the buffer — upload reads, the aws-chunked copy path, and download once the
transport exposes a receive-buffer seam — the filler writes into claimed carriers and the segment holds
their guards. When the transport supplies it — today's default download over `hyper`/`h2`, which allocates
its own receive buffers — the frames arrive as `Bytes` the arena never claimed, held as `Views` with no
guard and `block: None`. A single delivered buffer can hold both kinds of segment, so the cursor and its
release path are written against `Hold`, not against a single provenance.

`Clone` on the delivery type falls out of the two arms without a holder count. A `Views` segment clones by
cloning its `Bytes`, which bump the same per-carrier refcounts; a `Carriers` segment clones its
`Arc<CarrierGuard>`s, one bump per carrier. A carrier returns when every clone's cursor has passed it and
every clone is dropped — the guard's last `Arc` reference — so incremental release survives cloning rather
than being defeated by it. This is what lets the delivered type stay `Clone`, matching what the SDK body
types already offer, at the cost of one `Arc` bump per carrier per clone.

`chunks_vectored` is what keeps discontiguity cheap rather than merely tolerable: a multi-segment part
feeds one `pwritev` over the whole set, so a scattered claim costs the same syscall count as a contiguous
one (see [contiguous runs and disk I/O](#contiguous-runs-and-disk-io)). It is also what the claim path's
scatter tolerance rests on — a claim can take carriers wherever they are free because nothing downstream
needs them adjacent.

The writable form implements `BufMut` for fillers that speak it, and the trait's shape matters here for a
different reason: `chunk_mut` returns an `&mut UninitSlice` and `advance_mut` is `unsafe`, with the caller
promising the bytes it skipped have been initialized ([`buf_mut.rs:107`, `:179`][bytes-buf-mut]). That
matches a carrier drawn from a freshly mapped block, whose pages are zero-filled by the kernel but whose
contents are not meaningful as data: the arena hands out uninitialized capacity, and the writer's
`advance_mut` is what asserts the prefix has become data. The transition to the immutable form seals that
boundary — the frozen buffer's length is what was advanced, never what was claimed.

**Alternative: gather segments into one contiguous buffer at delivery.** Copying a multi-segment part into
a single allocation before handing it downstream would let every consumer treat it as one slice and remove
the cursor logic entirely. Rejected: the copy is exactly the per-part cost the arena exists to remove, and
it would be paid on every delivery rather than saved once. The traits already express discontiguity, the
kernel already accepts vectored I/O over it, and a consumer that requires contiguity can ask for it
explicitly.

[bytes-from-owner]: https://github.com/tokio-rs/bytes/blob/v1.12.1/src/bytes.rs#L254
[bytes-buf]: https://github.com/tokio-rs/bytes/blob/v1.12.1/src/buf/buf_impl.rs#L148
[bytes-buf-mut]: https://github.com/tokio-rs/bytes/blob/v1.12.1/src/buf/buf_mut.rs#L107
[bytes437]: https://github.com/tokio-rs/bytes/issues/437
[segmented-buf]: https://docs.rs/bytes-utils/0.1.4/src/bytes_utils/segmented.rs.html#117
[http-body-trait]: https://github.com/hyperium/http-body/blob/v1.1.0/http-body/src/lib.rs#L38
[sdk-body-impl]: https://github.com/smithy-lang/smithy-rs/blob/release-2026-07-23/rust-runtime/aws-smithy-types/src/body/http_body_1_x.rs#L49

---

## Correctness Invariants

The subsystem is touched concurrently from many threads: transfers reserving on their own threads,
execution threads claiming and filling, arbitrary consumer threads returning carriers to the bitmap, a
releaser draining the wait queue, and a trigger unmapping idle blocks. The invariants below are stated
against that concurrency. Each names what must hold, what it rules out, and the mechanism that enforces
it. Several rest on memory-ordering arguments — most sharply the [unmap
gate](#unmapping-never-races-a-live-carrier) — and those carry a standing obligation to be verified by
model checking, not by inspection.

### Reserved implies claimable

**Invariant.** A transfer holding a reservation for `n` carriers can claim `n` carriers without the
claim being refused by the subsystem and without waiting on admission. The claim fails only if the
operating system cannot back the memory.

**What it rules out.** A transfer admitted to run, then stalling mid-`execute` against the cap because a
carrier it was promised is not physically there, which would wedge memory against scheduling.

**Mechanism.** Two things have to hold: that the carriers are accounted for, and that they are backed.

Accounting is what the envelope buys. A reservation's own claims are capped at `envelope − coverage`, so
`live_reserved <= reserved` at all times, and admission never grants past `configured` (see [preparing
and reclaiming capacity](#preparing-and-reclaiming-capacity)). Because carriers are uniform, a free
carrier is a claimable carrier — there is no shape a claim can need that a free carrier fails to satisfy
— and a claim tolerates scatter, taking its carriers from wherever they are free rather than requiring a
contiguous run, so counting free carriers is enough to know the claim can be served. The bitmap records
every free carrier, so a claim finds one by scanning even if it was freed on another thread, and shard
hints never hide capacity from that scan.

Backing is what the arena works to maintain, at `prepared >= reserved + debt`. Reserve raises `prepared`
to cover every live envelope plus headroom, and trim's floor holds the same sum, so the capacity a
reservation counts on survives reclamation. Unreserved consumption can still overtake it between a grant
and a claim, since that consumption cannot be refused and arrives after the grant. When it does, the
claim maps a block rather than waiting, restoring the target at the point the shortfall is found.
Mapping is the residual failure: an `mmap` that the operating system declines returns the failure to the
caller, which is a different outcome from waiting on the cap and cannot deadlock against it.

Where preparation is too expensive to run inline — eagerly faulted pages, device-registered blocks — the
claim waits for the background preparer instead of mapping. That wait is not a cap wait, and the
distinction is what makes it acceptable: waiting on admission is circular, because the capacity the claim
waits for is freed by live work and the waiting transfer is part of that work, while the preparer's
progress depends on the operating system rather than on the stalled claim and so terminates whether or
not other transfers release anything. A claim stalls on the I/O thread inside `execute`, where it costs
that transfer's latency and holds its reservation longer, rather than in `poll_work`, where it would
block the scheduler.

### The bound moves only at reserve and release

**Invariant.** The accounted total `reserved` changes only when reserve grants and when a reservation
drops. Claiming, returning, and consulting or populating a shard never change it.

**What it rules out.** The hot per-carrier return path taking the admission lock, and the accounted
total drifting from reality as carriers cycle.

**Mechanism.** A carrier's claim and return move only a bitmap bit, never `reserved`. Reserve moves
`reserved` up, the reservation's drop moves it down, each once per unit of work. Reservation lifetime
and carrier lifetime are separate: carriers cycle within a held reservation, and the reservation
accounts for the whole envelope regardless of how its carriers move. `prepared`, the other accounted
total, moves only at grow and trim, the latter behind the floor check that keeps `prepared >= reserved +
debt`. `debt` is the exception that proves the rule: it moves on an unreserved claim and on the return
that repays it, because that consumption is not covered by any envelope and admission has no other point
at which to learn of it.

### Release does not require a scheduling turn

**Invariant.** A transfer can release the memory it holds without first obtaining a work-generation
turn.

**What it rules out.** A deadlock in which transfers hold the cap full, freeing requires a re-poll, the
re-poll is gated by dispatch capacity those transfers occupy, and none can free.

**Mechanism.** Release is the reservation's drop, driven by the consumer dropping the delivered segment
buffer's frames or the disk drain freeing a written part, on the execution path, not through `poll_work`.
A transfer that cannot be polled for new work can still complete and release what it holds. This carries
the memory budget's release-decoupling guarantee (see [scheduler](./scheduler.md#backpressure)).

### Admission is starvation-free

**Invariant.** A queued request is never bypassed. Once queued, it is granted before any request behind
it, and the capacity it waits on only accumulates toward it.

**What it rules out.** A request deferred indefinitely by later or smaller requests jumping ahead of it.

**Mechanism.** Strict FIFO with no skip-ahead: a queued request is bypassed neither by a fresh request
(reserve does not grant while the queue is non-empty) nor by a smaller queued request behind it (the
release drain stops at the first that does not fit). Once queued, a request sees capacity only
accumulate toward it.

**What it does not claim.** Not a bounded wait. Admission gates on `reserved + debt`, and while the two
components are equally displacing, they are not equally recoverable: `reserved` falls as this client's
own reservations drop, but debt falls only as the carriers behind it are returned, by whoever holds them.
Where the buffer source serves only this client, those holders are its own consumers and the wait
terminates for the same reason release does not depend on scheduling. Where the source is shared, a
foreign holder can keep debt standing, and no ordering property of the queue makes that wait finite.
Starvation-freedom is a statement about order, which this subsystem controls; liveness additionally
requires returns, which it does not. A dedicated transport is the configuration in which the two
coincide.

### An oversized request makes progress without breaking the bound

**Invariant.** A request larger than the whole cap is eventually granted, and granting it never lets a
second reservation compound the overshoot.

**What it rules out.** A reservation that cannot be subdivided parking forever against a bound it can
never meet, and the escape that admits it becoming a way for several oversized reservations to stack.

**Mechanism.** The `reserved == 0` exception grants an oversized front waiter when no other reservation
holds capacity. FIFO reaches that point: reserve grants nothing while the queue is non-empty, and every
earlier reservation drains ahead of this waiter. Requiring `reserved == 0` means no further envelope is
granted until the oversized one releases, so at most one is outstanding. The overshoot is bounded to that
one reservation plus whatever debt stands at the time — the exception deliberately does not wait for
`debt == 0`, which is not a condition admission can force (see [requests larger than the whole
cap](#requests-larger-than-the-whole-cap)).

### A claim makes progress under contention

**Invariant.** A claim within a held reservation completes in bounded time, not starved by other
threads winning the race for free carriers.

**What it rules out.** A livelock in which the subsystem makes progress but one claim repeatedly loses
the race for a free bit and never advances.

**Mechanism.** A free carrier exists by the reserved-implies-claimable accounting, so a claim that loses
the atomic take on a bit scans on and takes another. After a bounded number of failed attempts it takes a
serialized path under a per-node lock where no take is lost. The serialized path resolves contention for
capacity already guaranteed; it never maps or grows the arena.

### Unmapping never races a live carrier

**Invariant.** A block's region is never unmapped while a carrier in it is live or a claim into it is in
flight, and a stale carrier ID naming an unmapped-and-revived block never reads the wrong memory.

**Mechanism.** The block record is never freed, so its status and bitmap are always safe to read; only its
mapped region comes and goes. Trim publishes draining, then reads all-free; a claim sets a bit, then reads
the block's status, keeping the carrier only if the state is active and the generation matches. The two are
a store-then-load on each side, requiring sequential consistency (or a full fence) — acquire/release is
insufficient, since the hazard is precisely the store-load reordering it permits. The all-free requirement
means a live carrier's bit forbids unmap, so a handed-out address is valid for its buffer's life, and
resolving that address after the gate is what ties it to the bit. Requiring `active` rather than rejecting
only draining is what catches a name into an already-unmapped block; the generation additionally catches a
name into a *revived* one, whose state reads active again. On a lost race trim **abandons** the block
(reverts the draining mark) rather than waiting for the claim, so no in-flight-claim counter is needed. See
the [obligations](#unmapping-a-block-safely) on that section for what must be model-checked, including the
negative check that weakening the orderings is caught.

### The subsystem functions without a pinned runtime

**Invariant.** Correctness does not depend on thread pinning; without it the subsystem still bounds
memory and serves every claim.

**Mechanism.** A shard is only a claim-side hint. Absent pinning it is simply less populated with
locally-relevant carriers, and claims fall back to scanning the bitmap, which records every free carrier
regardless of which thread returned it — returns always go straight to the bitmap, never to a shard.
NUMA-local placement is an optimization that pays off once the runtime pins threads; the cap, the
reservation model, and claimability hold regardless.

---

## Open Questions

### Carrier size

The carrier size is chosen by measurement. It trades small-object internal waste (a small object holds a
whole carrier) against carrier count and per-carrier overhead on large transfers (a smaller carrier makes
a large part more carriers). The transport's own read sizing and the observed distribution of delivered
chunk sizes bound the useful range; the value is a benchmark output, and the geometry is parameterized on
it so the number is not baked into the design.

### Reservation granularity on very large parts

A reservation is held for a whole part until the data is consumed, so a very large part with a slow
consumer holds its whole envelope reserved while it drains, even as its early carriers return to the
arena. Releasing the envelope carrier by carrier as the part drains would let admission readmit sooner,
at the cost of accounting at a finer granularity than admission admitted. Whether this matters is a
question only a very large part with a slow consumer raises; it is left for measurement.

### The shape of the delivery type

The delivery mechanism is settled — a queue of segments over the two `Hold` arms, releasing carriers at the
consumer's cursor and cloning without a holder count (see [`Buf` and `BufMut` over a discontiguous
buffer](#buf-and-bufmut-over-a-discontiguous-buffer)). What remains open is the caller-facing surface: which
type carries it, and how one caller-facing hazard is handled.

Delivered data currently reaches the caller as `AggregatedBytes`, which wraps a `SegmentedBuf<Bytes>` and
implements `Buf` over the sequence of frames. The mechanism above fits either behind it — reshaping
`AggregatedBytes` around the segment queue — or behind a new type; that is a public-API question, not a
mechanism one, and it turns on how much of the existing surface must be preserved. `AggregatedBytes` is
`Clone`, which the two-arm design supports, so cloning is not the deciding factor. The type reached this
crate by being copied from the SDK runtime, and it was shaped when delivered buffers were transport
allocations the client had no stake in after handing them over. The arena changes what it is for: delivered
segments are now capped capacity, and holding them suppresses admission — which the current shape does not
express.

The sharpest expression of that is `into_bytes`, which the type offers as a convenience. It flattens the
sequence into one contiguous `Bytes`, copying whenever there is more than one segment — the underlying
`copy_to_bytes` slices without copying only when the whole request is satisfied from the front segment
([`segmented.rs:117`][segmented-buf]) — and the carriers it reads from are released as it consumes them:

```text
  a caller holds 8 MiB of delivered data for a long time

  hold the segments                     flatten with into_bytes
  ─────────────────                     ───────────────────────
  8 MiB of arena carriers pinned        one 8 MiB copy, then carriers returned
  counted in the cap                    heap memory, outside the cap
  suppresses admission of new work      admission unaffected
  no copy                               one copy of the whole part
```

Flattening converts capped capacity into uncapped heap, so it relieves admission pressure without reducing
the process's resident set. A caller archiving many parts in memory keeps the pipeline moving by paying for
copies; one that consumes and drops promptly should hold the segments and copy nothing. The arena cannot
make that choice for the caller, and the caller cannot make it well without knowing that segments are capped
and copies are not — so whether the answer is documentation, a distinct method, or a type that names the
tradeoff is the remaining open question. It is bounded by what the SDK body types accept at the boundary
(see [delivering arena memory as `Bytes`](#delivering-arena-memory-as-bytes)).

### Which alignment route wins, and what protocol scratch costs

Both routes in [payload alignment within a carrier](#payload-alignment-within-a-carrier) reach an aligned
write, and which one to prefer is unmeasured. The zero-copy route removes a copy of every byte but is
available only for framing that can start payload at offset zero, and only once a transport can. The copy
route is universal and costs one copy per body chunk, against a synchronous copy whose source buffer returns
immediately. Deciding it needs measurements of both copies: the one the copy route adds, next to the direct
write it enables, and the one the zero-copy route removes, against a saturated link.

The related quantity is what protocol scratch costs when the transport reads into carriers. Framing buffers
are small and churn per read; part-sized carriers are long-lived and retained until a write completes. Serving
both from one arena gives a single cap and one place to observe, at the cost of mixing a high-churn size class
into the block table. A separate arena isolates the churn and lets the two be sized independently, at the cost
of two caps that cannot lend to each other. The deciding number is how large a transport's working set is at
target concurrency, including the TLS layer beneath it.

A body written partly buffered and partly direct also mixes the two modes on one file, which `open(2)` warns
costs throughput even where coherency is handled. That applies to the tail write every part already needs, so
it is unavoidable rather than a consequence of a route; its cost is likewise unmeasured.

### What triggers trim

Reclaiming idle blocks needs a trigger independent of activity, since a quiescent pool fires no hot-path
event. The candidates are a timer, a periodic sweep, or a memory-pressure signal. The choice turns on
whether a long-lived client holding peak resident memory between bursts is a case worth paying a standing
actor for; the trim policy (all-carriers-free, hysteresis, floor) holds regardless of the trigger.

### Memory is an imprecise proxy for the cap

Auto sizing derives the cap from detected memory, but the quantity that actually bounds a useful cap is
network bandwidth, which is uncorrelated with memory across instance families. The clamp keeps the
estimate safe at both ends; a bandwidth-aware or NIC-derived sizing would target the operating point
rather than a proxy for it.

---

## Future Work

### Background capacity preparer

When preparing a block becomes expensive — eager first-touch to place pages on a NUMA node, or
registration for io_uring fixed buffers or `mlock` — growth moves off the reserve path to a background
preparer that maps, touches, and registers ahead of demand. The accounting is unchanged;
only the actor that moves `prepared` differs. The same actor is the natural home for trim.

### io_uring and registered buffers

Registered fixed buffers (`READ_FIXED`/`WRITE_FIXED`) for disk and provided-buffer rings for the network
turn the arena into kernel-visible fixed memory. The uniform page-aligned carrier geometry is designed
not to foreclose this: a block registers as a stable region once. A registered buffer is usable only by
the ring it is registered to, so it forms a separate compatibility partition, and the reserved-implies-
claimable argument then holds per partition rather than globally, with a reservation eligible for a
partition needing a placement decision. The first version runs one ordinary partition; the backend and
its completion-ownership model are deferred.

### Threading a reservation to the injected buffer source

The injected buffer source draws carriers without presenting a reservation because its interface cannot
carry one. If that interface can be extended to carry a reservation through to the draw, the draws a
transfer makes for its own payload are enforced at the claim rather than reconciled afterward, which
narrows what coverage and debt have to absorb. It does not remove them. A reservation can only be sized to
payload, since that is the only quantity the transfer knows at admission; the source's own protocol memory
— headers and framing, and the TLS layer beneath if it draws from the arena at all — is not attributable
to any one transfer's envelope and stays outside it. Whether a source can carry a reservation at all is an
integration question bound up with the transport's protocol memory, above.

### Sharing one arena across consumers

A single arena serving more than one consumer under the same cap, rather than each running its own
against the same memory, is not needed for the first version and not designed for. The arena and
reservation shape do not foreclose it.

### A stable NUMA identity

NUMA-local placement pays off only once the runtime pins threads or binds block memory explicitly, which
it does not do today. The arena is structured to place per node so the locality is available once pinning
lands; the placement machinery and its interaction with the kernel's automatic NUMA balancing are future
work.

---

## Appendix A: Memory ordering

Every shared field and the ordering each of its operations requires. This is the complete set: `SeqCst`
appears in exactly the four places below and nowhere else in the subsystem.

**The unmap gate.** Four accesses on two fields, two per side, argued in full under [unmapping a block
safely](#unmapping-a-block-safely). Each side stores then loads, and neither store may sink below its load;
`SeqCst` on all four is what forbids the interleaving in which both sides proceed.

| side | store | load |
|---|---|---|
| claim | `inuse` `fetch_or` of the carrier's bit — `SeqCst` | `status` — `SeqCst`, one load reading state and generation together |
| trim | `status` ← `draining` — `SeqCst` | `inuse` read confirming all-free — `SeqCst` |

Release/acquire is insufficient on either side: the hazard *is* the store-load reordering that release
permits. A `fence(SeqCst)` between each side's store and its load is equivalent and equally correct — what
the argument needs is a single total order over these four accesses, which either form supplies. The
accesses carry the ordering rather than a free-standing fence so it travels with the operation. A mix that
fences one side and relaxes the other is not correct.

**Everything else.** Weak by intent — the claim path is cheap because the gate is the only fenced traffic on
it.

| field | operation | ordering | why not weaker |
|---|---|---|---|
| `inuse` word | claim backing out: `fetch_and` of its own bits | `SeqCst` | Trim's confirming read must not observe the block as all-free while this claim still believes it holds the bit. Part of the gate's argument, on the path a rejected claim takes. |
| `inuse` word | return: `fetch_and` of the carrier's bit | `Release` | The carrier's contents were written before the return; a later claimant of the same bit must see them. Nothing on the return path reads a second location, so there is no store-load pair to fence. |
| `inuse` word | claim: read while scanning for a free bit | `Relaxed` | A scan is a hint. A stale word costs an attempt; the `fetch_or` is what decides, and it carries the ordering. |
| `status` | revival: store `(active, generation + 1)` | `Release` | Publishes the mapped region and the initialized bitmap. A claim that observes `active` through the gate's acquiring load therefore sees a fully initialized block. |
| `status` | trim: store `empty` after `munmap` | `Release` | Ordered after the unmap; a subsequent revival of the same record must not appear to precede it. |
| `base` | claim: read to resolve an address | `Relaxed` | Read only after the gate passes, and the set bit forbids unmap, so it cannot be cleared while the carrier is held. It inherits the gate's ordering; a claim that backs out never reads it. |
| `base` | revival: publish, and trim: clear | `Relaxed` | Both are ordered by the `status` store that follows them, which is the only thing a claim consults before reading `base`. |
| `live_here` | claim and return within a reservation | `Relaxed` | Single reservation, and its value gates only the holder's own claims against an envelope admission already granted. Nothing else reads it. |
| `configured`, `prepared`, `reserved`, `declared`, `avail`, `debt`, the wait queue | every operation | not atomic; the budget's lock | These quantities are only meaningful as a set. Making each atomic would let a reserve read a torn combination and grant against a sum that never existed. The lock is affordable because admission moves once per unit of work, not once per carrier. |

---

## Appendix B: Safety and soundness

The subsystem hands raw pointers into an `mmap`'d region to consumers it never sees again, freezes writable
memory into immutable buffers without copying, and unmaps regions while other threads claim from the same
block.

**The `unsafe` surface, in full.** Seven sites. Everything else in the subsystem is safe code: the bitmap, the
budget's quantities, the wait queue, and the shard are atomics and a lock, not unchecked memory access.

| `unsafe` site | obligation | what upholds it |
|---|---|---|
| forming a carrier's address as `base + index * carrier_size` | the mapping is live and the arithmetic stays within it | The address is formed only after the gate passes, and the set bit forbids unmap. The index is bounded by the bitmap's length, derived from the block's carrier count at revival. |
| `Bytes::from_owner` over a carrier | the pointer stays valid for the whole life of that `Bytes`, including clones on threads the arena never sees | The owner is boxed and therefore moved, but `as_ref()` builds its slice from the pointer *inside* the box, which addresses the mapping — so where the handle lives is irrelevant. Validity is held by the `Arc<CarrierGuard>` the owner carries: the carrier's bit stays set until the last guard clone drops, and a set bit forbids unmap. Several views over one carrier hold clones of one guard, so the bit clears exactly once, when the last of them is gone. |
| publishing a view's slice from within a filling carrier | the published range is initialized and disjoint from the writer's `&mut` tail | A view is published only over bytes the filler has already written, and the writer holds `&mut` only over the unwritten suffix; the two ranges never overlap, and no view is taken over the whole carrier while the tail is still being written. |
| `advance_mut` on the writable form | the bytes skipped have been initialized | The arena hands out uninitialized capacity, and the filler's `advance_mut` is the assertion that a prefix became data. The frozen length is what was advanced, never what was claimed, so no uninitialized byte is delivered. |
| `Send` on a carrier handle and on the segment buffer | ownership can move between threads | A carrier handle owns a disjoint window and a reference to a record that never moves or frees. Moving it moves the right to write those bytes; no thread-local state is involved. A shard, which *is* thread-local, is never part of a handle. |
| `Sync` on a block record | concurrent access from many threads is defined | Every field a thread other than the arena's grower touches is atomic, and the non-atomic fields (`carrier_size`, the bitmap's length) are written once at revival and published by the `status` store. |
| `munmap` of a block's region | no live carrier and no in-flight claim into it | The all-free requirement, the gate, and trim's abandon-on-lost-race. This is the subsystem's sharpest obligation, argued in full under [unmapping a block safely](#unmapping-a-block-safely). |

**Safety properties that hold without `unsafe`.** These are load-bearing but enforced by the type system, the
ecosystem, or the structure — not asserted by the author.

| property | what upholds it |
|---|---|
| two threads writing different carriers in one block do not race | Carriers are disjoint, non-overlapping windows, and a claimed bit is held by exactly one claimant. The mapping itself is written by no one. |
| the freeze does not alias the writable form | The freeze consumes it. The `&mut [u8]` it exposed cannot outlive it, so no mutable reference exists once the immutable form is constructed. |
| a consumer cannot regain write access to a carrier the arena still accounts for | Enforced by the ecosystem rather than by convention: `owned_is_unique` returns false for a custom owner, so `try_into_mut` on an arena-backed `Bytes` deep-copies rather than yielding the carrier ([`bytes.rs:1140`][bytes-owned-unique]). A consumer that wants a mutable buffer gets its own allocation. |
| a stale carrier name is never followed into freed control state | The block record is never freed and never moves. A stale name is detected by the generation, checked in the same load as the state. |

[bytes-owned-unique]: https://github.com/tokio-rs/bytes/blob/v1.12.1/src/bytes.rs#L1140

---

## Appendix C: Verification

An index over the obligations stated beside each mechanism, grouped by the mechanism that incurs them. Each
carries what discharges it and the negative check that must fail if the mechanism is weakened — the negative
checks are the operative half, since each names an ordering or a term that looks removable and is not.

Two obligations are not reachable by a model checker and say so in place: return-on-last-drop depends on
refcount state internal to the `bytes` crate, and the freeze wants Miri rather than a state-space search.
Only the gate and the ledger justify exhaustive checking at all; both are small state machines over a handful
of quantities. Everything else is a property test, an assertion, or inspection.

**[Unmapping a block safely](#unmapping-a-block-safely)** — one claim against one trim, all interleavings.

| obligation | discharged by | negative check |
|---|---|---|
| the gate admits no cycle over its four accesses | exhaustive model check | weakening either gate access to acquire/release must be caught |
| a claim keeps a carrier only on `active` and a matching generation, with draining and empty rejected independently | model check with a stale name into each of the three states | rejecting only `draining` must fail on the `empty` case |
| revival publishes `active` only over an initialized bitmap | model check: no claim observes `active` over an uninitialized block | publishing `active` before initialization must be caught |
| an address is formed only after the gate passes | inspection of the single site that computes it | resolving `base` before the status load must be caught by a trim that clears it mid-claim |
| the generation does not wrap within a hint's lifetime | inspection; the field is sized so the assumption holds by margin | a narrowed generation field must produce a false match under a revival-heavy trace |

**[Claiming and returning a carrier](#claiming-and-returning-a-carrier)**

| obligation | discharged by | negative check |
|---|---|---|
| a rejected take leaves the bitmap as it found it; a batch rejection clears only bits it won | model check: no thread's bit is cleared by another thread's claim | clearing the attempted mask rather than the won bits must be caught |
| a claim within a granted envelope always finds a carrier | property test over interleaved claims, returns, growth, and trim | a shard that retains carriers instead of hinting must strand capacity and fail it |
| a claim terminates under contention | model check for the absence of a livelock cycle; the serialized path cannot lose a take | an all-or-nothing compare-and-swap in the batch claim must livelock against a concurrent return |

**[The reservation and its lifetime](#the-reservation-and-its-lifetime)**

| obligation | discharged by | negative check |
|---|---|---|
| the holder's own claims are capped at `envelope − coverage` | direct check: a download declaring its whole envelope as coverage has a remaining budget of zero | dropping the `coverage` term must let an all-coverage download claim its full envelope and break the bound |
| a reservation's drop returns its envelope exactly once; the wake follows the unlock | model check for a lost wake and for lock nesting | waking under the lock must be caught by the nesting check |

**[The admission ledger](#appendix-d-the-admission-ledger)** — all four over the same randomized
grant/claim/return/close traces.

| obligation | discharged by | negative check |
|---|---|---|
| `live_unreserved = (declared − avail) + debt` holds through every transition | property test over the traces | any transition that moves one term without the other must be caught |
| `footprint <= reserved + debt <= configured` at every step | the same traces | a coverage term added beside the envelope rather than carved out of it must break the first inequality |
| a grant never reduces standing debt | trace: close a reservation with consumption still live, then grant another | recomputing debt against current coverage at close must admit work against memory a previous transfer still holds |
| the `avail` credit never exceeds `declared` | assertion in the return path, not a clamp | the assertion firing means the ledger arithmetic is wrong, not that a clamp is needed |

**Delivery** — [as `Bytes`](#delivering-arena-memory-as-bytes) and [as
`Buf`/`BufMut`](#buf-and-bufmut-over-a-discontiguous-buffer).

| obligation | discharged by | negative check |
|---|---|---|
| a carrier returns exactly once, when the last view over it drops | model check over the guard's `Arc` and its `Drop` — the count is the arena's own state, unlike the per-view `bytes` refcount, which only governs when one owner drops and is **not model-checkable** | a second guard over the same carrier, or a guard covering a run, must return early and be caught |
| the cursor releases each backing as it passes, not all at buffer drop | property test over partial consumption: resident carriers fall as the consumer advances | holding all guards until drop must be caught by a resident-set assertion mid-consumption |
| the merge extends a segment only on block identity and pointer adjacency | property test: a foreign view (`block: None`) or a non-abutting frame starts a new segment | merging across a block boundary or a gap must produce an `IoSlice` spanning unowned memory and be caught |
| the `Buf` cursor never reports end-of-buffer with bytes remaining | property test: a generic consumer reading through `chunk`/`advance` receives every byte | leaving the cursor resting on a segment boundary must truncate and be caught |
| the frozen length is what was advanced, not what was claimed | property test over partial fills; **Miri** over the freeze path | freezing the claimed length must expose uninitialized bytes under Miri |

---

## Appendix D: The admission ledger

Admission is a small state machine over six quantities, all moved under one lock. The body sections argue what
each is for; this states the exact transitions.

```rust
struct MemoryBudget {
    ledger: Mutex<Ledger>,
    waiters: Mutex<VecDeque<Waiter>>, // arrival order; see "parking and granting"
}

/// The six quantities, meaningful only as a set — hence one lock, not six atomics.
struct Ledger {
    configured: u32, // the cap
    prepared: u32,   // carriers backed by mapped arena
    reserved: u32,   // carriers granted to live reservations
    declared: u32,   // total coverage declared by live reservations
    avail: u32,      // declared coverage still unconsumed
    debt: u32,       // live unreserved consumption no reservation covers
}
```

`live_unreserved` is deliberately not a field. It is `(declared − avail) + debt`, and that identity is the
ledger's central invariant: every transition below moves terms so as to preserve it, and a transition that
moved one term without the other would be a bug the identity catches.

**Grant.** A reserve for an envelope of `e` carriers, declaring coverage `d <= e`, is admitted when

```text
  reserved + debt + e <= configured
```

and otherwise parks in arrival order. On admission:

```text
  reserved += e
  declared += d
  avail    += d
```

Coverage is declared, not escrowed: no carrier is set aside and nothing is handed to the transport. Adding
`d` to both `declared` and `avail` leaves `declared − avail` unchanged, so the grant does not alter live
unreserved consumption — correctly, since nothing has been claimed yet.

**Unreserved claim** of `n` carriers, which cannot be refused:

```text
  covered = min(n, avail)
  avail  -= covered
  debt   += n - covered
```

The claim draws on outstanding coverage first, because coverage is memory a grant already paid for; only
what exceeds it is new consumption the admission sum has not seen. The claim is served either way.

**Return** of `n` carriers:

```text
  repay  = min(n, debt)
  debt  -= repay
  avail += n - repay        // and `avail <= declared` must hold
```

Debt is repaid before coverage is credited. The subsystem cannot attribute a return any more than it can
attribute a claim, and among the orderings available to something that cannot tell a covered return from an
uncovered one, this is the one under which debt falls as carriers come back. Crediting coverage first would
leave debt standing while a steady working set recycled through the same carriers, with nothing to release
admission.

`avail <= declared` after the credit is an assertion rather than a clamp, and it holds by arithmetic: a
return cannot exceed live consumption, so `n <= (declared − avail) + debt`, and after repaying the credit is
`n − debt <= declared − avail`. A `min` here would silently absorb a real accounting bug.

**Close**, when a reservation for `e` with coverage `d` drops:

```text
  debt     += d.saturating_sub(avail)   // coverage that is spent and still live
  avail     = avail.saturating_sub(d)
  declared -= d
  reserved -= e
```

Withdrawing `d` from `declared` removes cover from consumption that is still live, and what just lost its
cover is exactly the part of `d` that `avail` cannot absorb. This is the one transition that must be
additive rather than a recomputation. A derived form — `debt = max(0, live_unreserved − declared)` — settles
at nothing under repeated grants: close a reservation with eight carriers live and uncovered, admit a second
transfer of the same shape, and its fresh coverage of eight cancels the debt before it has claimed anything,
so the client admits work against memory the first transfer's consumer still holds. Debt has to be real
state that only a return lowers.

Every quantity moves only at these four transitions, all under the same lock. Claims and returns of carriers
move the bitmap and nothing here, except for the two above that involve unreserved consumption, which
admission has no other point at which to learn of. Each preserves `live_unreserved = (declared − avail) +
debt`: a grant adds `d` to both terms of the difference, an unreserved claim splits `n` between `avail` and
`debt` at the coverage boundary, a return moves `n` back the other way, and a close moves out of `declared`
exactly what `avail` cannot absorb.

The footprint bound follows:

```text
  footprint = live_reserved + covered_live + debt
            <= reserved + debt          coverage is carved out of the envelope
            <= configured               the grant condition gates this sum
```

The first inequality is the one everything else rests on, and it holds only because a reservation's own
claims are capped at `envelope − coverage` rather than at `envelope` (see [the reservation and its
lifetime](#the-reservation-and-its-lifetime)). Nothing else in the ledger enforces it.

The second inequality is what the two documented overage paths cross, deliberately: an unreserved claim that
finds nothing free, and the forced grant for a request larger than the whole cap. Both raise `prepared` above
`configured`, both are accounted, and both retire through the ordinary trim floor as `reserved + debt` falls.
