# Symlinks and delete mode

This auxiliary design doc to [execution](execution.md) focuses on symlinks in the local tree when
delete mode is on. It decides what a run may delete, and what a download may write, while a link
stands in the tree.

A *link* here is a symbolic link in the local tree. A *hold* on a name makes one side of the merge
answer "unknown" for the keys it covers. A hold usually covers the name and every key under it. The
comparison takes no action on an unknown side (**FR-Fail-7**), so a hold stops whatever the run
would have done there. Each case below names the decision that settles it.

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

On an upload, the filter excludes `photos` and `videos`, and both are links. Sync never resolves an
excluded link (D2), so each one holds the keys under its name. Dropping `videos` from the stream
releases every hold that sorts before `videos`, because the merge treats those keys as passed. But
the destination has not paired `photos/y.jpg` yet, and the object would read as absent. The merge
drops an excluded link only once the other side has no earlier key to pair (D3). So `photos/y.jpg`
pairs first, under the hold, and stays.

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
second run downloads into a real directory. When the filter excludes the link, delete mode leaves
the link in place, and every run holds the keys under it. When the tree refuses to remove the link,
the hold still keeps every download out of it.

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
key keeps. With delete mode on, the merge holds the link's own name `link` and its target `real`
(D4). The destination-only key reads as unknown, and the file stays. With delete mode off, nothing
can delete the file, so the merge holds nothing.

### E. A chain of file links

```
   y.txt ──────► z.txt ──────► x.txt
   │             │             │
   kept          a link,       the file,
                 destination   destination
                 only          only
```

The bucket keeps `y.txt`, and delete mode is on. Deleting `z.txt` would leave `y.txt` dangling, and
deleting `x.txt` would lose the file `y.txt` names. The merge holds every later link on a file
link's chain, and the file at its end (D4). The link `y.txt` stays deletable, because removing a
link leaves its target.

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

The bucket keeps `z/link/x.txt`, and delete mode is on. The walk meets a link only when it reads the
directory holding the link, and `z/` sorts after `real/`. So the merge decides the delete of
`real/x.txt` before any hold exists. The run reports an incomplete plan (D6). The table shows a
small tree. When many keys sort between `real/` and `z/`, the delete lands before the walk reads
`z/`, and the same run downloads the file again. A late file link settles after one more run. The
delete leaves the link dangling, and the next run replaces the link with the downloaded file (D7).

### G. A link at a kept key's own name

```
   bucket source       local destination
   ┌──────────────┐    ┌───────────────────────────┐
   │ y.txt        │    │ y.txt ──► its target      │
   └──────────────┘    └───────────────────────────┘

   following   the target    default modes        no-overwrite
   ─────────   ──────────    ─────────────        ────────────
   off         anything      replace the link     keep the link
   on          missing       replace the link     replace the link
   on          a file        compare the file     keep the link
   on          a directory   replace the link     keep the link
   on          a socket      skip, obstructed     skip, obstructed
```

With following off, the link is an entry at its own name with no size or time. No comparison can
show that it matches, so the default modes download, and the rename replaces the link. No-overwrite
sees an entry there and keeps it. With following on, the link counts as what it reaches (D7). A
dangling link leaves nothing at `y.txt`, so every mode downloads. A link to a socket reads as the
socket, and every mode skips the key, as for a real socket. A rename never follows the link it
replaces, so no row writes into the link's target.

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
link to a directory that contains the root as a loop, and it stops at the link. If `current`
dangled, the root would exist as a link and be no directory, so the run would end before any work.

### I. A link the merge cannot see

```
   bucket source        local destination, case-insensitive
   ┌──────────────┐     ┌──────────────────────────────┐
   │ photos/a.jpg │     │ Photos ──► /mnt/nas          │
   └──────────────┘     └──────────────────────────────┘

   name           the merge                  the filesystem
   ────           ─────────                  ──────────────
   Photos         a link, held (D2)          a link
   photos/a.jpg   absent, under no hold      inside /mnt/nas, through the link
```

macOS and Windows match names without regard to case by default. The merge holds `Photos` (D2), but
it compares keys byte for byte, so the hold misses `photos/a.jpg`. A download of `photos/a.jpg`
would land in `/mnt/nas`. A link created after the walk read its directory slips past the holds the
same way. So sync checks the path again when it acts (D9).

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
(**FR-Root-8**), stay outside this design. A key whose name matches a link's name only without
regard to case is inside it (case I).

## 2. Design decisions

**D1. One follow setting governs everything sync does to the local tree.** Listing, comparing,
writing, and deleting all take one value. When the listing and the writes disagree, a download
writes through a link its listing never looked behind (case C). An upload defaults to not following,
as `upload_objects` does. A download defaults to following, because `download_objects` writes
through a link on a directory above a file and has no setting.

With following on, the walk follows every link it can resolve, except a link the filter excludes
(D2), and a key names whatever its path reaches. With following off, a link is an entry at its own
name, and sync never reads, writes, or deletes through it.

**D2. A link the walk does not resolve holds every key under its name.** With following off, every
link qualifies. With following on, a dangling link, a link into an endless chain, and a loop
qualify. A link the filter excludes qualifies in both settings. The walk checks the filter before it
resolves a link, so sync never looks behind an excluded link.

The side holding the link cannot account for any key under the link's name, so the merge holds the
name on that side. On an upload the source holds, and delete mode removes no destination key under
the name (case A). On a download the destination holds, and the run writes no source key under the
name (case C). The run reports each held key as `skipped-unknown` and marks its plan incomplete. The
hold rests on what the walk could not see, so it applies with delete mode off too.

An excluded file link protects nothing it points at. Delete mode may remove its target and leave the
link dangling, and the run reports nothing for the link itself.

A held download costs one more run. With delete mode on, the first run removes a destination-only
link, and the second run downloads the keys under its name. The alternative finishes in one run: it
holds each download behind the link's removal and fails the download when the tree refuses. But that
alternative keeps a record of refused names for the rest of the run. It still has to hold the keys
when delete mode is off, or when the filter excludes the link. The hold covers every case with the
mechanism an upload already uses.

Delete mode removes a link as a link. On Windows, Rust's `remove_file` refuses a directory link or a
junction, so the removal uses `remove_dir` there.

Execution's D12 removes a file at `photos` before a download of `photos/a.jpg`. A link the walk
does not resolve never reaches D12, because the hold decides no download under the link.

**D3. The merge drops an excluded link only at the merge position.** A link whose name the filter
excludes makes no pairing, so the merge drops it from the stream. Dropping an entry releases every
hold below its key, because the merge treats those keys as passed. An excluded head can sort ahead
of the merge position while the other side has an earlier key to pair. Dropping the head there
releases holds the earlier key needs (case B). So the merge drops an excluded head only once the
other side has no earlier key to pair. The rule protects every other hold too. An excluded link that
sorts after an unreadable directory leaves that directory's hold in place.

**D4. On a download with following on and delete mode on, a delete never passes through a link or
removes what a link reaches.** A delete removes one directory entry, and two keys can name one file
(case D). For each link the destination walk follows, the merge holds names on the source side. The
merge holds a directory link's own name, so no delete passes through the link. It also holds the
link's resolved target when the target sits inside the root, whatever kind of file the target is.
For a file link, it holds every later link on the chain as well (case E). A file link's own name
stays deletable, because removing a link leaves its target.

The holds stand even when the bucket keeps neither key. To delete the target safely, the merge would
need the bucket's answer for the key through the link. That key can sort after the target, so the
merge would have to set the delete aside until the key pairs, and D6 gives that cost. So with delete
mode on, a download with following on keeps stale files under a link's target, reports them as
`skipped-unknown`, and ends `Warned`.

The holds exist only to stop deletes. With delete mode off, the run names no link targets and holds
nothing. A destination-only key through a link then reports as destination-only, and a run over a
tree like `current -> releases/v3` ends `Clean`.

**D5. The merge holds at most 1,024 names for followed links.** A link whose target sorts after it
keeps the target's name held until the merge passes the target. In a tree with one such link per
directory, the held names would grow with the tree, and **NFR-Mem-1** rules that out. Past 1,024
names, the source holds every later delete. Only names ahead of the merge count. A name the merge
has passed entirely adds no hold, so a burst of links to targets behind the merge cannot trip the
bound. The bound matches the 1,024 transfers that may wait before the merge pauses (execution's
D11), so the held names cost about what that queue costs.

**D6. A late link makes the plan incomplete.** The walk meets a link only when it reads the
directory holding the link. When that directory sorts after the link's target, the merge has
already decided deletes under the target (case F). Preventing the deletes under the target needs
every link known before the first delete. A local delete batch holds one key, and a waiting batch
pauses the merge (execution's D11). Holding the deletes would stall the merge, and every later
transfer with it, until the walk ends. Setting the deletes aside keeps the merge moving, but memory
then grows with the deletes. A spill file bounds that memory, at the cost of a new kind of state.

So with delete mode on, when a name arrives after the merge already reached it, the merge marks the
plan incomplete, and the run ends `Warned`. **FR-Exec-7** names this tree as its one exception. The
report fires on every run over such a tree, including a run that deleted nothing under the target.
The merge does not record whether a pairing became a delete, so it cannot tell the two runs apart. A
tree with `www/current -> ../releases/v3` therefore ends `Warned` on every correct run with delete
mode on. With delete mode off, the merge holds no names (D4), so no name can arrive late.

**D7. What stands at a key's own name counts the way the follow setting says.** With following off,
a link at a key's own name is an entry with no size or time. No comparison can show that it
matches, so the default modes download, and the rename replaces the link. No-overwrite sees an
entry and keeps the link (case G).

With following on, a link at a key's own name counts as what it reaches:

- A dangling link, or a link into an endless chain, reaches nothing. Every mode downloads, and the
  rename replaces the link. Delete mode leaves a destination-only dangling link, because nothing is
  at its name. The walk's error becomes a hold on the keys below the name, and the name itself
  stays free.
- A link to a file compares as that file. When a mode finds a difference, the download's rename
  replaces the link and leaves the target alone.
- A link to a socket, FIFO, or device reads as that file, and every mode skips the key as
  obstructed.
- A link to a directory leaves no entry at its name, because the walk descends into it. The default
  modes download and replace the link, as **FR-Enum-4** says. No-overwrite keeps it. Just before the
  rename, the download checks the final name with links resolved, and it skips the key when the name
  reaches anything.

A dangling link in a download's destination is no failure in either setting. An upload still reports
one as a failure, because the source has nothing to send for its key.

**D8. The run starts at the root the caller named.** The follow setting covers the entries inside
the root. The run resolves the root itself as spelled, even when the root is a link, and the
directories above the root stay outside the run (case H). A root that is a dangling link exists and
is no directory, so the run ends before any work, as **FR-Fail-6** says for such a root. The run
checks the root with `lstat` for that reason. A check that follows the link would read a dangling
root as a directory nobody has created yet.

A link to a directory that contains the root is a loop, and the walk stops at the link. The check
covers the directories above the root as spelled and above the resolved root, because a link can
reach either chain. It identifies each directory by the device and inode that `stat` returns. `stat`
needs only search permission, so a directory the run can enter and cannot list still counts. The
check costs one `stat` for each directory above the root, once per run, and only with following on.

**D9. Sync checks a path again when it acts.** The holds come from the walk, and two things slip
past them (case I). The filesystem can match a key's name to a link's name without regard to case or
Unicode form, and the merge compares bytes. A link can also appear after the walk read its
directory.

So before each local delete, and before each download write with following off, sync calls `lstat`
on each component of the key's path below the root. When a component is a link, sync skips the
action, reports the key as `skipped-unknown`, and marks the plan incomplete. The key's own name
stays out of the check, because delete mode removes a link as a link and a download replaces one. A
download with following on writes through a link above the file (D1), so its writes skip the check.
The download opens its temporary file with `create_new`, so a link planted at that name refuses the
open.

The check costs one `lstat` per path component per action. A window stays between the check and the
system call. Closing the window needs a path lookup that refuses links, such as `openat2` with
`RESOLVE_NO_SYMLINKS` on Linux or `O_NOFOLLOW_ANY` on macOS, and Rust's standard library exposes
neither.

## 3. How the pieces fit

### The problem

The merge reads absence from position. A link makes position mislead in two ways: it hides keys from
one side, and it gives one file two keys. The design answers both with one mechanism the merge
already has, and checks the path again at the moment sync acts.

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
merge sits before the end of the name's range. A dangling link on a download with following on adds
a stretch over only the keys below its name. A download can then still land at the name itself
(D7). Each stretch also records what caused it, so a held key's event can say why (**FR-Obs-1**).

### Where each hold comes from

```
   link                            held on           names held                added in
   ────                            ───────           ──────────                ────────
   not followed, upload            the source        its own name              take_source
   not followed, download          the destination   its own name              take_destination
   excluded by the filter          its side          its own name              Walk::next
   dangling or endless, upload     the source        its own name              cost_of
   dangling or endless, download   the destination   the keys below its name   LocalDestination
   in a loop                       its side          its own name              cost_of
   followed, delete mode on        the source        own name, target, chain   hold_aliased
```

The first six rows cover a link the walk does not resolve (D2). A link's side is the source on an
upload and the destination on a download. The walk checks the path filter before it resolves a
link, in both settings, and yields an excluded link marked `PathFiltered::Excluded`. With following
on, the walk reports a dangling link as an error. On a download, `LocalDestination` turns that
error into a stretch over the keys below the name, and it drops the error. `take_destination`
mirrors `take_source`:

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
need two tests. A link to anything other than a directory names its target as a `File`, so a socket
that a link reaches stays held too. A link's own name sits under the root as spelled. A target comes
from `canonicalize`, so it sits under the resolved root. On macOS a temporary root reads `/var/...`
and resolves to `/private/var/...`, so each kind of name takes its key under its own root. macOS can
also spell one directory as `/Users/...` and as `/System/Volumes/Data/Users/...`. When a target's
resolved path does not start with the resolved root, the merge compares the identity of the target's
ancestors with the root's.

`Walker::downloading` takes the delete mode. With delete mode off, `LocalDestination` names nothing,
so a download with following on and delete mode off resolves no link targets (D4).

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

### Checking a path when sync acts

Execution runs the D9 check before each local delete, and before each download write with following
off. The check walks the key's parent components below the root:

```rust
// The first link among a key's parent components below the root. A delete or a write through that
// link would act on the link's target.
fn link_on_path(root: &Path, key: &str) -> Option<PathBuf> {
    let (parents, _own_name) = key.rsplit_once('/')?;
    let mut at = root.to_path_buf();
    for part in parents.split('/') {
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(meta) if meta.file_type().is_symlink() => return Some(at),
            Ok(_) => {}
            // Nothing stands here yet, so nothing below it can be a link.
            Err(_) => return None,
        }
    }
    None
}
```

`symlink_metadata` goes through the filesystem's own name lookup. On a filesystem that ignores case,
`photos` therefore finds the link `Photos` (case I).

### What this design does not decide

Sync has no public entry point yet. The crate-private `WalkerBuilder` defaults to not following in
both directions, so the download default from D1 belongs to the public entry point.

Two keys can reach one local file through a followed link, and both can download. With
`link -> real` and a bucket holding `link/x.txt` and `real/x.txt`, the last write wins.
**FR-Fail-8** governs that case, and FR-Fail-8 has no implementation yet.

A bind mount or a mount point inside the root gives one file two keys with no link, and `lstat`
cannot tell. D4 holds nothing there. A hard link needs no decision, because a delete removes one
name and leaves the other.

A Windows device name such as `NUL` names a device in every directory, so a key like that leaves the
root through its own name. **FR-Root-8** covers it.
