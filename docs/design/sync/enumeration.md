# Key-ordered enumeration

The first layer of sync: producing the two entry streams that the comparison reads. This describes
what it covers, the decisions behind it and what each would cost to change, and how the pieces fit.

---

## Where this sits

```
        local root                      S3 root
            │                              │
       ┌────▼─────┐                   ┌────▼─────┐             ┐
       │  FsWalk  │                   │  S3Walk  │             │
       └────┬─────┘                   └────┬─────┘             │
            │ FsEntry                      │ Object            │
       ┌────▼─────┐                   ┌────▼─────┐             │  this layer
       │ KeyFilter│                   │ KeyFilter│             │
       └────┬─────┘                   └────┬─────┘             │
            │                              │                   │
            │   Entry { key, meta, source }                    │
            │   StreamError { walk, unkeyable name, listing }  │
            └──────────────┬───────────────┘                   ┘
                           │
                           │   two streams, one key order
                           │
                    ┌──────▼───────┐
                    │  comparison  │    which keys differ, and how
                    └──────┬───────┘
                           │   transfer / delete / skip, per key
                    ┌──────▼───────┐
                    │  execution   │    scheduling, retries, deletes
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  reporting   │    events, counts, failures
                    └──────────────┘
```

The bracketed part is this layer, and it ends where the two streams meet: producing them belongs
here, joining them belongs to the comparison. Nothing here decides what to do about a key, and
nothing here transfers or deletes anything.

## 1. Scope, and why it is this

This layer builds one thing:

> a stream of `(relative key, metadata)` pairs, in the order S3 returns keys, whose ending says
> whether it finished cleanly or gave up, applying marker exclusion, filtering and paging as it
> goes.

A stream, because a run may cover millions of keys and building a list first fixes memory at the
entry count. Keyed, because the key is what makes an entry on one side the counterpart of one on
the other. In S3's order, because the comparison decides a key is missing from a side when that
side moves past where the key would sort, so both sides must agree on "past". With an ending that
says which of the two happened, because "no more entries" and "I could not see the rest" license
opposite actions, and collapsing them is how a sync deletes files it failed to look at. Marker
exclusion, filtering and paging inside, because a consumer that had to remember to do them would
eventually forget.

This layer answers the requirement categories that describe enumeration — **FR-Root**, **FR-Enum**
and **FR-Filter**, which [`functional-spec.md`](functional-spec.md) defines. Comparison,
execution, failure handling, reporting, dry runs and the cost and rate requirements belong to
other layers, though seven of their requirements are served here. **FR-Cmp-6** wants the
comparison handed the entries themselves, so `Entry` carries the walker's own item; the interface
is the comparison's. **FR-Cmp-8** wants a plan that does not depend on interleaving, and this
layer supplies the ordering half. **FR-Fail-7** wants a name a transfer cannot move to hold back a
delete, and **FR-Fail-9** wants a device, FIFO or socket never to fail a run; both follow from
those arriving as entries rather than as failures. **NFR-Tput-1** and **NFR-Lat-1** are throughput
and latency bounds a merge-shaped traversal has to respect, and **NFR-Mem-1** is the memory bound
D11's structure answers.

---

## 2. Design decisions

Most of these are what the comparison will build on, so revisiting one later means touching more
than this layer. D11 and D12 are the cheap ones, and are here so that a reader knows they were
considered.

**D1. Both sides emit one total key order.** A comparison reads absence from position: every
delete is licensed by "the source moved past this key". Weaken the order guarantee and every
decision downstream becomes unsound.

**D2. A relative key is a UTF-8 `String`.** A local name that is not valid UTF-8 is skipped and
named. An S3 object key is Unicode encoded as UTF-8, so such a name has no key it could take.
Converting it lossily would collapse two distinct names onto one key, and sync would treat two
files as one. The key is the join, so changing its type later changes `Entry`, the stream trait,
the filters, and every reason a caller sees.

**D3. An unreadable timestamp is `None`.** `last_modified_secs` is an `Option`. Putting the epoch
there would hand the comparison a number nobody observed, with nothing marking it as invented.
`None` cannot collide with a real time, needs no second field to stay in step with, and will not
compile until a consumer handles it. The epoch appears only where a number is reported to a
caller.

**D4. Neither side is ever held whole.** Entries are pulled one at a time, so nothing accumulates
a side before the comparison sees it. Building a list first is the obvious implementation and it
fixes memory at the entry count. Undoing it later means rewriting every consumer.

**D5. A directory that cannot be read is its own error kind.** Distinct from an entry-level
failure, because the consumer needs the difference to suppress the right deletes. Collapse them
and an unreadable directory looks like one skipped file, which is how objects under a directory
nobody could read get deleted.

**D6. A thing the walk found is an entry, whatever kind of thing it is.** FR-Fail-9 says a device,
FIFO or socket must never fail a run, and FR-Fail-7 says its key must hold back a delete. Both
follow from the entry existing: it arrives at its own key carrying `FileType::Socket`, and a
comparison that reads absence from position sees the key occupied. Reporting it instead put a
per-key fact on a channel with no keys in it.

So errors divide in two, which `is_fatal` answers and only a walk can:

```
   ends the walk   nothing is left to do       SourceUnreadable, NotADirectory, Service
   one entry       should have been readable    Io, PermissionDenied, DirectoryUnreadable,
                                                BrokenSymlink, SymlinkCycle
```

A cycle sits with the second group. It stops a descent, so a subtree goes unenumerated, and no
single key stands for a subtree — the same reason D5 keeps an unreadable directory distinct.

A link pointing at nothing is an error while a socket is an entry, which is easy to get backwards.
FR-Enum-3 sorts by whether the walk *should have been able to read it*: a dangling link is a read
that failed, and the walk would have produced an entry had the target been there.

**D7. Yielding these entries is opt-in, because the walkers already have callers.** `upload_objects`
records every walk error that does not end the walk as a failure, and aborts the whole transfer on
one under its strictest policy. A walk that yielded sockets unbidden would hand it names it
would try to upload.

Two ways out of that: teach `upload_objects` to recognise them, or let the caller ask. FR-Enum-3 is
a sync requirement, so the second matches who wants the information. `upload_objects` sees exactly
what it saw before, and its code is untouched.

Naming the kind still belongs to the walk. A consumer has to tell a socket from a file it could not
read, and only the walk knows which it found.

**D8. The filter is upstream of every way out, not just of emitting.** An excluded name must
produce nothing at all, so every place that yields an entry or reports a failure consults the filter
first — the same check the failure paths already made. Miss it at one site and an excluded socket
comes out anyway.

**D9. A name that cannot be keyed is never filtered out.** Key derivation returns `None` for a
name that is not valid UTF-8, and the local predicate keeps such an entry. Excluding it there
would drop it silently, and a name nobody hears about reads as a name that is free. So filters can
only exclude names that have keys.

**D10. Filters run inside enumeration, on both sides, before metadata is read.** One ordered rule
list matched on the relative key (FR-Filter-1, FR-Filter-2), applied to both sides (FR-Filter-3),
independent of arrival order and holding neither side whole (FR-Filter-4), with anchoring
expressed in the API (FR-Filter-6). Both sides, or an excluded local file makes its object look
orphaned and delete mode removes it. Before metadata, or an excluded entry has already warned,
which FR-Filter-5 forbids.

---

### Reversible, recorded anyway

**D11. Every level of the descent path keeps its children.** Peak memory is therefore the depth of
the path times the width of each directory on it, where a breadth-first walk down a chain holds one
level. That is the price of knowing what comes next without re-reading a directory.

Two alternatives were built and measured on another machine, so the comparison below is by ratio.
Keeping only a resume point per level, and reading a directory again on the way back up, brought
peak to within a third of breadth-first's; it costs one re-read of a directory per subdirectory
the directory holds, which measured 2× slower on these trees and is quadratic in the number of
subdirectories. Capping the total children held and releasing the shallowest levels first roughly
halved peak, with the common shapes back at parity on speed, and still thrashes when two
consecutive levels are both wide, since eviction spares only the deepest level.

Both dismissed. The memory saved is bounded in practice — `PATH_MAX` caps how deep a path can go,
and a single wide directory already costs as much, since sorting one requires holding it — while
re-reading introduces a directory that can change between visits.

Whether either slowdown would show is a separate question, and nothing measures it: the two sides
advance in step, so what matters is which one a run waits on, and no benchmark compares a walk
against a listing. If this ever needs revisiting, spending walk time to save memory is the trade
that fits, and that comparison is what would decide it.

Switching later is an internal change, since the traversal's state is private to the module. It
stays that way on one condition: the comparison must treat an unreadable directory as *unknown
from this key onward under that prefix*, not as *the whole prefix is unknown*. A re-read that
fails has already emitted part of the directory, so a partial range is the honest report. A
comparison written for whole subtrees would have to change too.

**D12. `KeyStream` is implemented directly on `FsWalk` and `S3Walk`.** There is no
`LocalKeyStream(FsWalk)` wrapper. One type per side, one builder per side, and nowhere for
configuration to drift between a walker and something holding it.

The cost is visible as `S3Walk::prefix()`. An impl adds no storage to the type it is written for,
so anything `next_entry` needs has to be readable off the walker — here, the prefix, so it can be
stripped from each key to leave the relative key. A wrapper would have kept that accessor private.

What would force a wrapper: a stream needing state the walker has no business holding. Both
walkers and the trait are crate-private, so adding one later touches construction sites inside
this crate and nothing outside.

**D13. The stream reports its own failures, not the walk's.** `next_entry` yields a `StreamError`,
which is a walk failure, a name that cannot be keyed, or a listed object missing a field a
comparison needs. Only the first comes from a walk.

Sharing `WalkErrorKind` for all three meant borrowing kinds for failures a walk never has, and
inheriting what they imply. An object with no size became `Service`, which is fatal — so one
malformed object in a page stopped a whole sync, when the honest cost is that one key cannot be
compared. A name that cannot be keyed had the same problem in reverse: it read as a walk failure
when the walk had read the name perfectly.

Separating them lets the consumer ask the question it actually has, which is what a failure cost.
A walk failure may have hidden a subtree; the other two cost exactly one key, and the keys around
them still arrive.

## 3. How the pieces fit

### The problem

Sync decides, for every name, whether to copy it, delete it, or leave it. That needs two lists
lined up — what is on disk, what is in the bucket — and neither list can be held in memory,
because either could run to millions of entries.

**Key order** is what makes that possible. If both lists arrive sorted the same way, sync reads
them a little at a time, like merging two sorted decks: look at the top card of each, advance
whichever sorts earlier.

### The one picture

```
     local          bucket
     ─────          ──────
   → a.txt          a.txt      ← same key: compare size and time
     img/logo.png → img/old.png

     "logo" sorts before "old", and the bucket is already past it,
     so img/logo.png exists only on the left.
```

That is the whole comparison. Nothing announced that `img/logo.png` was absent from the bucket;
the bucket side moved past where it would have been. **Absence is read from position.** Everything
in this layer exists to make that inference sound.

### Five types

**`FsWalk`** (existing) walks a local directory tree. **`S3Walk`** (existing) calls
`ListObjectsV2` repeatedly, following the continuation token. Internally they share nothing.
Externally they now promise one thing: entries come out in S3's order.

**`Entry`** (new) is the shape they agree on — the key, the metadata worth comparing, and the
original item (`FsEntry` or `Object`). That third field looks redundant and is not: some
comparison modes need fields only the original carries, like a checksum or an ETag. Handing over a
summary closes that door.

**`KeyFilter`** (new) is the gate, and it matches on the key. **`WalkError`** (existing, one
variant added) carries everything the stream could not turn into an entry.

`SortOrder`, `FsEntry`, `FileType` and `WalkErrorKind` are public, so a caller can come to depend on
them. `FileType` and `WalkErrorKind` are `#[non_exhaustive]`, so a kind added later breaks nobody.
Everything under `io::key` is crate-private, which is what leaves `Entry`, `EntryMeta` and
`KeyStream` free to change shape.

### The sort trick

S3 sorts keys by raw bytes. Making a local walk match is less obvious than it sounds.

A directory `a` produces keys like `a/c`. A file `a.txt` produces `a.txt`. Compare the bytes:

```
        a / c              a . t x t
          ▲                  ▲
         0x2F               0x2E        →  '.' is smaller, so a.txt comes first
```

Now sort the *names* the walk sees — `a` and `a.txt`. Plain string sorting puts `a` first, because
it is a prefix. That is the opposite of S3, and the merge misaligns from there on.

The fix is one byte:

```
        sort  a/  against  a.txt        →  matches S3
        sort  a   against  a.txt        →  does not
```

A name inside a directory can never contain `/`, so that appended byte settles every case.

One consequence: this makes the walk depth-first. A subtree has to be read to the end before any
sibling that sorts after it can be emitted.

### Why the filter matches keys

One rule set has to apply to both sides. `css/app.css` is the same key whether it came from
`./site/css/app.css` or `s3://example/live/css/app.css`, so matching on the key keeps the two
sides in agreement.

Matching on paths would let a rule exclude the local file and keep the object. The object would
then look like it had no counterpart, and delete mode would remove it. Matching on keys keeps an
excluded name absent from both piles, so nothing compares it and nothing deletes it.

### What could not become an entry

A stream yields entries. It also has to report what it could not turn into an entry, because
silence is dangerous: a name missing from the pile reads as "nothing is there", and the object at
the matching key gets deleted.

Two levels, because the consumer treats them differently:

```
   ends the walk   → the walk never got going; there is no pile at all
   one entry       → this key is unknown; the failure policy decides whether to continue
```

A FIFO, a socket, a device file and a symlink the walk was told not to follow all arrive as entries,
each carrying the `FileType` that says which it is, and a walk yields them only when asked, through
`include_special_files`, so the operations that already use the walkers see nothing new. Sync copies
none of them as things stand — the symlink would need a setting changed, the rest can never be
copied at all. But *something occupies that name*, and that
is exactly what has to stop the object at the matching key from being deleted. That is the
difference between a skip and a silent omission.

A timestamp the platform cannot represent is a different shape of problem. The file is fine and
transferable; only one field of its metadata is missing. So it stays an ordinary entry and carries
`None` for its time, and the layer that reports to the caller turns that `None` into the report
FR-Enum-5 asks for. A failure that stands in for an entry and a fact that travels inside one are
separate mechanisms.

Errors surface at the position of the directory they came from, before any key inside it. A
directory-unreadable error at `img/` arrives where `img/` would have, so the comparison learns
that everything under `img/` is unknown before it could conclude that `img/old.png` has no
counterpart. An error about one entry inside a directory arrives at that directory's position too,
which is earlier than where the entry itself sorts. Early is safe, since the consumer only needs
to know before it decides; late would not be.

### One rule about where this runs

`next_entry` is `async`. Calling it may wait on the filesystem or the network.

The comparison will be consulted from `poll_work`, which the scheduler calls in a tight loop
across every in-flight transfer. That method is not `async` and must return quickly, because
anything slow there stalls every other transfer in the process.

So the comparison cannot call `next_entry`:

```
   dispatched work                 poll_work
   ──────────────                  ─────────
   may await                       may not await
   next_entry() → queue    ──→     reads the queue, consults the comparison
```

The queues are the seam between "this may block" and "this may not". Wiring `next_entry` into
`poll_work` would work in a test and starve the scheduler in production.

Which raises the obvious question: what about a comparison that *cannot* answer quickly? Checksum
mode (FR-Cmp-9) has to fetch a checksum the listing does not carry, and property mode (FR-Cmp-11)
has to request the properties. Neither can produce an answer in a synchronous fast path.

FR-Cmp-6 settles it. The interface has to accept "not yet, ask me again" from the start, even
though every mode in the first release answers immediately. A mode that needs to fetch something
says so from `poll_work`, the fetching happens where awaiting is allowed, and the answer is
applied later — in key order, so FR-Cmp-8 still holds.

Enumeration owes that arrangement one thing, and it is why `Entry` carries the original item.
FR-Cmp-6 requires the comparison be given the entries themselves, because a checksum or ETag check
needs listing fields that only the original carries. Hand over `(key, size, time)` alone and a
deferring mode has nothing to defer on.

