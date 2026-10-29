# Symlinks and delete mode

This auxiliary design doc to [execution](execution.md) focuses on symlinks in the local tree when
delete mode is on. It decides what a run may delete, and what a download may write, while a link
stands in the tree.

A *link* here is a symbolic link in the local tree. A *hold* on a name makes one side of the merge
answer "unknown" for that name and every key under it. The comparison takes no action on an unknown
side (**FR-Fail-7**), so a hold stops whatever the run would have done under the name. Each case
below names the decision that settles it.

## The cases

### A. An upload past a link the walk does not follow

```
   local source, following off            bucket destination
   ┌───────────────────────────┐          ┌──────────────────┐
   │ photos ──► /mnt/nas       │          │ photos/y.jpg     │
   └───────────────────────────┘          └──────────────────┘

   key            source               destination   decision
   ───            ──────               ───────────   ────────
   photos         the link             absent        skip, nothing to read
   photos/y.jpg   held under photos    present       skip, unknown
```

The walk stops at `photos`, so the source never lists `photos/y.jpg`. Position alone would read the
object as absent, and delete mode would remove it from the bucket. The source holds `photos` (D2),
so `photos/y.jpg` reads as unknown and stays. A dangling link, a loop, and a link whose name the
filter excludes hide keys the same way.

### B. Two links the filter excludes

```
   key order  ────────────────────────────────────────────────────────────►

                photos               photos/y.jpg              videos
   source       excluded link                                  excluded link
   destination                       object
                                     ▲                         ▲
                                     not paired yet            an early drop
                                                               of videos
```

On an upload with following off, the filter excludes `photos` and `videos`, and both are links.
Each excluded link holds the keys under its name. Dropping `videos` from the stream releases every
hold that sorts before `videos`, because the merge treats those keys as passed. But the destination
has not paired `photos/y.jpg` yet, and the object would read as absent. The merge drops an excluded
link only once the other side has no earlier key to pair (D3). So `photos/y.jpg` pairs first, under
the hold, and stays.

### C. A download through a link the walk does not follow

```
   bucket source             local destination, following off
   ┌──────────────────┐      ┌──────────────────────────────┐
   │ photos/a.jpg     │      │ photos ──┐                   │
   └──────────────────┘      └──────────┼───────────────────┘
                                        ▼
                             ┌──────────────────────────────┐
                             │ /mnt/nas, outside the root   │
                             └──────────────────────────────┘

   delete mode   run   the link photos   photos/a.jpg
   ───────────   ───   ───────────────   ────────────
   on            1     removed           held, skipped-unknown
   on            2     gone              downloaded into a real photos/
   off           any   kept              held, skipped-unknown
```

A download of `photos/a.jpg` would land in `/mnt/nas`. The next walk would stop at the link again,
so every run would write the file again. The destination holds `photos` (D2), so no run writes
under the link. With delete mode on, the first run removes the destination-only link, and the
second run downloads into a real directory. The hold works the same way when the filter excludes
the link, and when the tree refuses to remove it.

### D. Two keys name one file

```
   keys the walk yields                         the file on disk
   ────────────────────                         ────────────────
   link/x.txt ──► link ──► real/ ──┐
                                   ├──────────► real/x.txt
   real/x.txt ─────────────────────┘

   bucket keeps   destination only   deleting the destination-only key
   ────────────   ────────────────   ─────────────────────────────────
   real/x.txt     link/x.txt         removes real/x.txt
   link/x.txt     real/x.txt         removes real/x.txt
```

`link` points at `real`, and with following on the walk yields two keys for one file. A delete
removes one directory entry. Whichever key the bucket lacks, deleting it removes the file the other
key keeps. The merge holds the link's own name `link` and its target `real` (D4). The
destination-only key reads as unknown, and the file stays.

### E. A chain of file links

```
   y.txt ──────► z.txt ──────► x.txt
   │             │             │
   kept          a link,       the file,
                 destination   destination
                 only          only
```

The bucket keeps `y.txt`. Deleting `z.txt` would leave `y.txt` dangling, and deleting `x.txt` would
lose the file `y.txt` names. The merge holds every later link on a file link's chain, and the file
at its end (D4). The link `y.txt` stays deletable, because removing a link leaves its target.

### F. A link the walk meets after the merge passed its target

```
   key order  ────────────────────────────────────────────────────────────►

                real/x.txt                          z/link/x.txt
                ▲                                   ▲
                the merge decides                   the walk reads z/, and only then
                the delete here                     meets z/link ──► ../real

   run   what happens                               outcome
   ───   ────────────                               ───────
   1     delete real/x.txt                          Warned, plan incomplete
   2     download z/link/x.txt into real/           Clean
   3     delete real/x.txt again                    Warned, plan incomplete
```

The bucket keeps `z/link/x.txt`. The walk meets a link only when it reads the directory holding the
link, and `z/` sorts after `real/`. So the merge decides the delete of `real/x.txt` before any hold
exists. The run reports an incomplete plan (D6), and the tree alternates between runs. A late file
link settles after one more run. The delete leaves the link dangling, and the next run replaces the
link with the downloaded file (D7).

### G. A link at a kept key's own name

```
   bucket source       local destination, holding one of
   ┌──────────────┐    ┌───────────────────────────────────────────────┐
   │ y.txt        │    │ y.txt ──► missing          a dangling link    │
   └──────────────┘    │ y.txt ──► /run/app.sock    a link to a socket │
                       └───────────────────────────────────────────────┘
```

Neither link has bytes the comparison can read. A dangling link would read as unknown at its own
name, so nothing would download and every run would fail. A followed link to a socket would read as
the socket, and the comparison would skip the key on every run. The download replaces whatever link
stands at a key's own name (D7). The rename lands the file at `y.txt` and leaves the link's target
alone.

### H. Where the run starts

```
   /data/
   ├── sibling/
   │   └── s.txt            above the root, outside the run
   ├── current ──► dest     the root the caller named
   └── dest/
       ├── a.txt
       └── up ──► ..        its target contains the root: a loop
```

The caller names `/data/current`, a link to `/data/dest`. The run starts there with following on or
off, because the follow setting covers the entries inside the root (D8). Following `up` would walk
`/data` again, and an upload would send `up/sibling/s.txt` from outside the root. The walk treats a
link to a directory that contains the root as a loop, and it stops at the link.

## 1. Scope, and why it is this

This design covers the local tree in both directions. A bucket has no links, and a delete there
removes one key, so the bucket side needs nothing here.

The merge pairs one key with one directory entry. A link breaks that pairing in two ways.

**A link the walk does not look behind hides keys.** The keys under the link's name exist on the
other side only, so position alone reads them as absent. An upload would delete them from the
bucket. A download would write them through the link, wherever it points.

**A link the walk follows gives one file two keys.** A delete removes one directory entry. On a
download, the delete through either key removes the file the other key keeps. An upload's
destination is a bucket, so this way reaches downloads only.

The same rules govern the writes a download makes, with delete mode on or off. A write and a delete
on one name have to agree about what the name is. The decisions below meet **FR-Enum-4**,
**FR-Enum-14** and **FR-Enum-15** in [`functional-spec.md`](functional-spec.md). Two keys that
differ only in case (**FR-Fail-8**), and a key that leaves the root through its own path
(**FR-Root-8**), stay outside this design.

## 2. Design decisions

**D1. One follow setting governs everything sync does to the local tree.** Listing, comparing,
writing, and deleting all take one value. When the listing and the writes disagree, a download
writes through a link its listing never looked behind (case C). An upload defaults to not following,
as `upload_objects` does. A download defaults to following, because `download_objects` writes
through a link on a directory above a file and has no setting.

With following on, the walk follows every link it can resolve, and a key names whatever its path
reaches. With following off, a link is an entry at its own name, and sync never reads, writes, or
deletes through it.

**D2. A link the walk does not resolve holds every key under its name.** With following off, every
link qualifies. With following on, a dangling link and a loop qualify. The side holding the link
cannot account for any key under the link's name, so the merge holds the name on that side. On an
upload the source holds, and delete mode removes no destination key under the name (case A). On a
download the destination holds, and the run writes no source key under the name (case C). The run
reports each held key as `skipped-unknown` and marks its plan incomplete.

A held download costs one more run. With delete mode on, the first run removes a destination-only
link, and the second run downloads the keys under its name. The alternative finishes in one run: it
holds each download behind the link's removal and fails the download when the tree refuses. But that
alternative keeps a record of refused names for the rest of the run. It still has to hold the keys
when delete mode is off, or when the filter excludes the link. The hold covers every case with the
mechanism an upload already uses.

Execution's D12 removes a file at `photos` before a download of `photos/a.jpg`. A link the walk
does not resolve never reaches D12, because the hold decides no download under the link.

**D3. The merge drops an excluded link only at the merge position.** A link whose name the filter
excludes makes no pairing, so the merge drops it from the stream. Dropping an entry releases every
hold below its key, because the merge treats those keys as passed. An excluded head can sort ahead
of the merge position while the other side has an earlier key to pair. Dropping the head there
releases holds the earlier key needs (case B). So the merge drops an excluded head only once the
other side has no earlier key to pair. The rule protects every other hold too. An excluded link that
sorts after an unreadable directory leaves that directory's hold in place.

**D4. On a download with following on, a delete never passes through a link or removes what a link
reaches.** A delete removes one directory entry, and two keys can name one file (case D). For each
link the destination walk follows, the merge holds names on the source side. The merge holds a
directory link's own name, so no delete passes through the link. The merge also holds the link's
resolved target when the target sits inside the root. For a file link, the merge holds every later
link on the chain as well (case E). A file link's own name stays deletable, because removing a link
leaves its target.

The holds stand even when the bucket keeps neither key. To delete the target safely, the merge would
need the bucket's answer for the key through the link. That key can sort after the target, so the
merge would have to set the delete aside until the key pairs, and D6 gives that cost. So a download
with following on keeps stale files under a link's target, reports them as `skipped-unknown`, and
ends `Warned`.

**D5. The merge holds at most 1,024 names for followed links.** A link whose target sorts after it
keeps the target's name held until the merge passes the target. In a tree with one such link per
directory, the held names would grow with the tree, and **NFR-Mem-1** rules that out. Past 1,024
names, the source holds every later delete. The bound matches the 1,024 transfers that may wait
before the merge pauses (execution's D11), so the held names cost about what that queue costs.

**D6. A late link makes the plan incomplete.** The walk meets a link only when it reads the
directory holding the link. When that directory sorts after the link's target, the merge has
already decided deletes under the target (case F). Preventing the deletes under the target needs
every link known before the first delete. A local delete batch holds one key, and a waiting batch
pauses the merge (execution's D11). Holding the deletes would stall the merge, and every later
transfer with it, until the walk ends. Setting the deletes aside keeps the merge moving, but memory
then grows with the deletes. A spill file bounds that memory, at the cost of a new kind of state.

So when a name arrives after the merge already reached it, the merge marks the plan incomplete, and
the run ends `Warned`. **FR-Exec-7** names this tree as its one exception. The report fires on every
run over such a tree, including a run that deleted nothing under the target. The merge does not
record whether a pairing became a delete, so it cannot tell the two runs apart. A tree with
`www/current -> ../releases/v3` therefore ends `Warned` on every correct run.

**D7. A download replaces whatever link stands at a key's own name.** The rename that lands a
download replaces a link and leaves its target alone. The same rename covers a dangling link and a
followed link to a socket, FIFO, or device (case G). So the walk reports a dangling link in a
download's destination as an entry at its own name, with no size or time. The walk records no
failure for that entry. Delete mode removes a destination-only dangling link. The hold on the keys
under the link's name stays (D2), because the link may stand for a directory on an unmounted
volume. An upload still reports a dangling link as a failure, because the source has nothing to send
for its key.

**D8. The run starts at the root the caller named.** The follow setting covers the entries inside
the root. The run resolves the root itself as spelled, even when the root is a link, and the
directories above the root stay outside the run (case H). A link to a directory that contains the
root is a loop, and the walk stops at the link. Checking for that loop costs one `open` and `fstat`
for each directory above the root, once per run, and only with following on.

## 3. How the pieces fit

### The problem

The merge reads absence from position. A link makes position mislead in two ways: it hides keys from
one side, and it gives one file two keys. The design answers both with one mechanism the merge
already has.

### A hold under a name

A side keeps its holds in `gap`, a set of `Stretch` values. A stretch under a name covers the name
and every key below it. `Side::missing` answers for a key the side did not produce, and a covered
key reads as unknown. The excerpt below keeps only the hold:

```rust
fn missing(&mut self, key: &str) -> SideState<K::Source> {
    self.gap.release_passed(key);
    if self.gap.covers(key) {
        return SideState::Unknown(KeysLost::UnknownRange);
    }
    SideState::Absent
}
```

The side that holds decides what stops:

```
   hold on            a key under the name reads      so the comparison never
   ───────            ──────────────────────────      ───────────────────────
   the source         unknown on the source side      deletes it
   the destination    unknown on the destination      transfers it
```

The merge releases a stretch once it passes the name. A hold therefore costs memory only while the
merge sits before the end of the name's range.

### Where each hold comes from

```
   link in the local tree    held on      name held                 added in
   ──────────────────────    ───────      ─────────                 ────────
   not followed              its side     its own name              take_source, take_destination
   excluded by the filter    its side     its own name              Walk::next
   dangling, or in a loop    its side     its own name              cost_of
   followed, on a download   the source   own name, target, chain   hold_aliased
```

The first three rows cover a link the walk does not resolve (D2). A link's side is the source on an
upload and the destination on a download. On a download, the walk yields a dangling link as an
entry (D7), so `take_destination` adds that hold. `take_destination` mirrors `take_source`:

```rust
fn take_destination(&mut self) -> Entry<D::Source> {
    let entry = self.dst.take();
    if entry.meta.obstruction == Some(Obstruction::UnfollowedLink) {
        self.dst.gap.add(Stretch {
            under: Some(entry.key.clone()),
        });
    }
    entry
}
```

### The merge position

`Side::take` releases every hold below the key it hands over. The merge calls `take` on an excluded
head only once the other side has no earlier key:

```rust
// The destination still has a key that sorts before this link, so that key pairs first.
let ahead = matches!(
    (&self.src.head, &self.dst.head),
    (Head::Entry(src), Head::Entry(dst)) if dst.key < src.key
);
if !ahead && self.src.head_is(Obstruction::ExcludedLink) {
    let link = self.src.take();
    self.src.gap.add(Stretch { under: Some(link.key) });
    continue;
}
```

The destination's branch mirrors the condition and adds its own stretch.

### Naming what a followed link reaches

`FsWalk` records each link it follows during one `next` call. `LocalDestination` reads the record
after every call, including a call that returns an error, and turns each link into names. A name
comes in one of two kinds, because the merge reaches the two at different points:

```rust
pub(crate) enum AliasedName {
    // Reached once the merge enters the subtree or passes it.
    Directory(String),
    // Reached once the merge pairs the key or a later one.
    File(String),
}
```

`link.txt` sorts after the file `link` and before the directory's subtree `link/`, so the two kinds
need two tests. A link's own name sits under the root as spelled. A target comes from
`canonicalize`, so it sits under the resolved root. On macOS a temporary root reads `/var/...` and
resolves to `/private/var/...`, so each kind of name takes its key under its own root.

`Walk::hold_aliased` adds each name to the source side. A name the merge has already reached marks
the plan incomplete (D6), and the bound applies at the same point (D5):

```rust
if self.src.gap.under.len() < ALIASED_NAMES_HELD {
    self.src.gap.add(stretch);
} else {
    // Past the bound, hold every later key.
    self.src.gap.add(Stretch { under: None });
}
```

### What this design does not decide

Sync has no public entry point yet. The crate-private `WalkerBuilder` defaults to not following in
both directions, so the download default from D1 belongs to the public entry point.

Two keys can reach one local file through a followed link, and both can download. With
`link -> real` and a bucket holding `link/x.txt` and `real/x.txt`, the last write wins.
**FR-Fail-8** governs that case, and FR-Fail-8 has no implementation yet.
