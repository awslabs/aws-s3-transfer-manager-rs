# Key-ordered enumeration

The first layer of sync: producing the two entry streams that the comparison reads. This describes
what it covers, the decisions behind it, and what those decisions cost.

---

## Where this sits

```
        local root                      S3 root
            │                              │
       ┌────▼─────┐                   ┌────▼─────┐             ┐
       │  FsWalk  │                   │  S3Walk  │             │
       └────┬─────┘                   └────┬─────┘             │
            │ DirEntry                     │ Object            │
       ┌────▼─────┐                   ┌────▼─────┐             │  this layer
       │ KeyFilter│                   │ KeyFilter│             │
       └────┬─────┘                   └────┬─────┘             │
            │                              │                   │
            │   Entry { key, meta, source }                    │
            │   WalkError { path, kind }                       │
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

The specification holds 81 requirements in twelve categories. The table below covers the three
that describe this layer — **FR-Root**, **FR-Enum** and **FR-Filter** — all 28 of them, in spec
order.

The other nine categories are not handled here: **FR-Cmp** comparison, **FR-Exec** execution,
**FR-Fail** failure handling, **FR-Obs** reporting, **FR-Dry** dry runs, **NFR-Cost** request
cost, **NFR-Tput**, **NFR-Lat** and **NFR-Mem**. Six requirements from them are exceptions, named
after the table.

```
   covered        satisfied here
   partly         the part belonging here is settled; the row names who owns the rest
   inherited      already true of the walkers, which upload_objects and download_objects
                  rely on; the obligation here is to leave them working
   kept open      unbuilt, and a choice here could make it harder — the behavior may still
                  be undecided, or the opt-in may invert something this layer decided
   later phase    unbuilt, and it lands somewhere other than this layer — listed so its
                  absence raises no question
```

| Requirement | Status | Where it stands |
|---|---|---|
| FR-Root-1 Upload, Download and Copy | partly | `FsWalk` and `S3Walk` both implement `KeyStream`, so a direction is a choice of two streams; `Copy` pairs two `S3Walk`s and is backfill work |
| FR-Root-2 a root names a place, never one entry | later phase | root parsing and validation |
| FR-Root-3 relative key derivation | covered | `io/key.rs` |
| FR-Root-4 a trailing separator changes nothing | covered | `strip_key_prefix`, pinned by test |
| FR-Root-5 any bucket identifier the API accepts | later phase | root parsing and validation |
| FR-Root-6 two S3 endpoints for `Copy` | later phase | root parsing and validation |
| FR-Root-7 reject a bad configuration first | later phase | root parsing and validation |
| FR-Root-8 a key resolving outside the root | kept open | settled elsewhere; nothing here forecloses it |
| FR-Enum-1 list both roots through | covered | only filters reduce what gets listed |
| FR-Enum-2 folder markers invisible | covered | dropped in the stream, so no filter can bring them back, pinned by test |
| FR-Enum-3 skip one entry, and say which kind | covered | `Severity`, four warning kinds, reported on request |
| FR-Enum-4 symlinks off by default | inherited | `follow_symlinks` unchanged, beyond skipping unused cycle state |
| FR-Enum-5 a time the platform cannot represent | partly | `last_modified_secs` is an `Option`; never-skip is the comparison's |
| FR-Enum-6 keys compared byte for byte | covered | no folding, no normalizing, pinned by test |
| FR-Enum-7 start before listing finishes | partly | an entry arrives before the second listing page is fetched, pinned by test; nothing starts transfers yet |
| FR-Enum-8 files added or deleted mid-run | inherited | unchanged |
| FR-Enum-9 a symlink loop ends the descent | covered | `SymlinkCycle`; the ancestor-handle check survives depth-first |
| FR-Enum-10 markers become local directories | kept open | one check in `key_and_meta` is where the opt-in has to reach |
| FR-Enum-11 configurable page size | inherited | unchanged |
| FR-Enum-12 an unreadable directory is a range | covered | `DirectoryUnreadable`, positioned in key order |
| FR-Enum-13 listing sends no delimiter | inherited | unchanged |
| FR-Filter-1 ordered include and exclude rules | covered | `io/key_filter.rs` |
| FR-Filter-2 matched on the whole path, `*` crosses `/` | covered | `fnmatch` semantics reproduced |
| FR-Filter-3 the rule set applies to both sides | covered | one `KeyFilter`, read by `local_predicate` and `s3_predicate` |
| FR-Filter-4 order-independent, holds neither side | covered | matched per entry as it arrives, with the same answers in any order, pinned by test |
| FR-Filter-5 an excluded entry stays silent | covered | consulted before metadata, and before any report |
| FR-Filter-6 anchored or matched anywhere | covered | the API says which; the pattern text keeps its `fnmatch` meaning |
| FR-Filter-7 delete what the filters excluded | kept open | `s3_predicate` is the only place to invert |

Six requirements from other categories are served here. **FR-Cmp-6** wants the comparison handed
the entries themselves, so `Entry` carries the walker's own item; the interface is the
comparison's. **FR-Cmp-8** wants a plan that does not depend on interleaving, and this layer
supplies the ordering half. **FR-Fail-9** wants one failure policy, and severity is what makes its
own exception expressible. **NFR-Tput-1**, **NFR-Lat-1** and **NFR-Mem-1** are measured in §5 —
the first two covered, the third for the one term of its bound that has code today.

---

## 2. What exists, and what this adds

`io::walk` already exists, because `upload_objects` and `download_objects` use it. Everything
under `io::key*` is new.

```
   existing, extended here                   new here
   ───────────────────────                   ────────
   FsWalk      + key_order, path_filter,     Entry<T>, EntryMeta   what both sides emit
               report_untransferable
   S3Walk      + prefix()                    KeyStream             the trait they both implement
   DirEntry    one path plus a shared root   KeyFilter, Rule       include and exclude rules
   WalkError   + severity(), four kinds      Severity              ends the run, failure, warning
                                             derive_object_key     path or key → relative key
                                             strip_key_prefix
```

Five of these are public, which is the part a caller can come to depend on: `FsWalk::key_order`,
the switch that turns on S3 ordering; `Severity`; `WalkErrorKind::severity()` and
`WalkError::severity()`; and four new `WalkErrorKind` variants — `SpecialFile`,
`SymlinkNotFollowed`, `NonUtf8Name`, `DirectoryUnreadable`. `WalkErrorKind` was already
`#[non_exhaustive]`, so adding variants breaks nobody, and `Severity` is `#[non_exhaustive]` for
the same reason.

Everything in `io::key*` is crate-private, so `Entry`, `EntryMeta` and `KeyStream` can still
change shape without breaking a caller.

## 3. Design decisions

D1 to D10 are what the comparison will build on, so revisiting one later means touching more than
this layer. D11 and D12 are cheaper to change, and are here so that a reader knows they were
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

**D6. Errors classify into three levels: ends the run, entry failure, entry warning.** A warning
must never become a failure under any policy. FR-Fail-9 requires one failure policy for the whole
run and then names the exception this level exists for: a device, FIFO or socket "is a warning
under either policy". Adding the third level later means reclassifying kinds that callers already
match on.

```
   ends the run   nothing is left to do       SourceUnreadable, NotADirectory, Service
   entry failure  should have been readable   Io, PermissionDenied, DirectoryUnreadable,
                                              BrokenSymlink
   entry warning  no entry describes it       SpecialFile, SymlinkNotFollowed, NonUtf8Name,
                                              SymlinkCycle
```

A link pointing at nothing sits with the failures, which is easy to get backwards. FR-Enum-3 sorts
by whether the entry *should have been readable*: "missing, deleted mid-run, unreadable, or a
symlink pointing at nothing" are one group, and a device, FIFO or socket the other. A dangling
link is a read that failed; the walk would have produced an entry had the target been there.

**D7. Reporting a warning is opt-in, because the walkers already have callers.** `upload_objects`
records every walk error that does not end the run as a failure, and aborts the whole transfer on
one under its strictest policy. A walk that began reporting sockets unbidden would therefore have
failed uploads with nothing wrong with them.

Two ways out of that: teach `upload_objects` to recognise a warning, or let the caller ask for the
reports. FR-Enum-3 is a sync requirement, so the second matches who wants the information.
`upload_objects` sees exactly what it saw before, and its code is untouched.

Severity still belongs to the walk. The consumer that asked for the reports has to tell a socket
from a file it could not read, and only the walk knows which it found.

**D8. The filter is upstream of reporting, not just of emitting.** An excluded entry must produce
no warning, so every place that reports one consults the filter first — the same check the failure
paths already made. Miss it at one site and an excluded socket warns anyway.

**D9. A name that cannot be keyed is never filtered out.** Key derivation returns `None` for a
name that is not valid UTF-8, and the local predicate keeps such an entry. Excluding it there
would drop it silently, which is the outcome the warning exists to prevent. So filters can only
exclude names that have keys.

**D10. Filters run inside enumeration, on both sides, before metadata is read.** One ordered rule
list matched on the relative key (FR-Filter-1, FR-Filter-2), applied to both sides (FR-Filter-3),
independent of arrival order and holding neither side whole (FR-Filter-4), with anchoring
expressed in the API (FR-Filter-6). Both sides, or an excluded local file makes its object look
orphaned and delete mode removes it. Before metadata, or an excluded entry has already warned,
which FR-Filter-5 forbids.

---

### Reversible, recorded anyway

**D11. Every level of the descent path keeps its children.** Peak memory is therefore the depth of
the path times the width of each directory on it — §5 measures 6,941,474 bytes for a hundred
levels of a hundred files, against breadth-first's 159,060.

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

Neither slowdown would show. A local walk reads entries several times faster than `ListObjectsV2`
returns keys, and the two sides advance in step, so the walk already waits on the listing. If this
ever needs revisiting, spending walk time to save memory is the trade that fits.

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

## 4. How the pieces fit

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
original item (`DirEntry` or `Object`). That third field looks redundant and is not: some
comparison modes need fields only the original carries, like a checksum or an ETag. Handing over a
summary closes that door.

**`KeyFilter`** (new) is the gate, and it matches on the key. **`WalkError`** (existing, one
variant added) carries everything the stream could not turn into an entry.

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

Three levels, because the consumer treats them differently:

```
   ends the run   → the walk never got going; there is no pile at all
   entry failure  → this one key is unknown; the failure policy decides whether to continue
   entry warning  → something is at this key and no entry describes it; never a failure
```

The third level covers a FIFO, a socket, a device file, a name that is not valid UTF-8, a symlink
the walk was told not to follow, and a directory reached by a link that loops. A walk reports the
first three only when asked, through `report_untransferable`, so the operations that already use
the walkers see nothing new. Sync copies none of them as things stand — the symlink would need a
setting changed, the rest can never be copied at all. But *something occupies that name*, and that
is exactly what has to stop the object at the matching key from being deleted. That is the
difference between a skip and a silent omission.

A timestamp the platform cannot represent is a different shape of problem. The file is fine and
transferable; only one field of its metadata is missing. So it stays an ordinary entry and carries
`None` for its time, and the layer that reports to the caller turns that `None` into the warning
FR-Enum-5 asks for. Warnings that stand in for an entry and information that travels inside one
are separate mechanisms.

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

---

## 5. Measurements

`benches/walk.rs`, all figures from one host and one build: an m7i.4xlarge running Amazon Linux
2023, x86_64, 6.12 kernel, with fixtures on tmpfs and the machine idle. Every measurement runs
both traversals over the same tree, because breadth-first is the existing traversal and therefore
the only baseline that says which cost key ordering introduced.

**Time to the first entry does not depend on how many entries exist.** The leftmost path is the
same at every size, so a rising curve would mean the walk reads more than it needs to before
emitting anything.

```
   entries      breadth    key_order
      1,000    46.40 µs     46.05 µs
     10,000    46.00 µs     46.40 µs
     50,000    46.61 µs     46.32 µs
```

Fifty times the entries moves nothing: all six medians fall within 1.4% of each other, and no
confidence interval is wider than ±0.5%.

**Ordering costs nothing in throughput.** Key order is faster on all three shapes, by 2.1%, 9.5%
and 3.8%, because breadth-first's sort compares whole paths while key order compares names and
skips the shared prefix on every comparison.

```
   shape                     breadth    key_order
   wide_1dir_10k            17.67 ms     17.29 ms
   deep_100dirs_100each     65.25 ms     59.05 ms
   balanced_f10_d3_10each   55.62 ms     53.53 ms
```

**Peak memory, in bytes, from a tracking allocator in the bench crate.**

```
   shape                          entries      breadth    key_order
   bounded_fanout_1000_per_dir       1,000      603,268      489,124
   bounded_fanout_1000_per_dir      10,000      709,702      492,324
   bounded_fanout_1000_per_dir      50,000      713,910      504,212
   one_wide_directory                1,000      583,127      467,543
   one_wide_directory               10,000    9,147,527    5,999,303
   one_wide_directory               50,000   36,889,607   23,656,127
   chain_depth_25_100_per_dir        2,500       98,250    1,009,202
   chain_depth_50_100_per_dir        5,000      118,450    2,486,626
   chain_depth_100_100_per_dir      10,000      159,060    6,941,474
```

Three shapes, three different answers.

At bounded fanout key order is flat: 3.1% more for fifty times the entries, where breadth-first
grows 18.3%. In one wide directory both grow linearly — 50.6× for key order, 63.3× for
breadth-first — because a directory's children have to be held before they can be sorted. That is
the case NFR-Mem-1 was amended for.

Down a nested chain key order costs an order of magnitude more, and that is the honest price of
the ordering. Depth-first has to keep every directory on the path open to know what comes next, so
peak tracks depth times fanout, while breadth-first walking a chain holds one level: at depth 100
the two are 6,941,474 against 159,060. It is superlinear too, since a path string lengthens as the
walk descends — four times the depth costs 6.9 times the memory.

Two caveats on what any of this covers. NFR-Mem-1 bounds three things — the descent path, one
listing page per side, and transfers in flight — and only the first exists yet. And the largest
tree here is 50,000 entries where the requirement asks about a million; the curves are flat or
linear well before that, but the point itself is untested.
