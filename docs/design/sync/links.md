# Design: links in the local tree

This design implements FR-Enum-4, FR-Enum-14, and FR-Enum-15. Writing those three requirements
also changed FR-Enum-3, FR-Enum-9, FR-Exec-7, FR-Exec-12, FR-Fail-4, FR-Fail-7, FR-Fail-8, and
FR-Root-8. The rules come from one model of the local tree, and most of the code already follows it.

## The model

The merge pairs one key with one directory entry. A link breaks that pairing, and the follow
setting decides how the merge sees the break:

- With following off, a link is an entry at its own name, and the walk never looks behind it. The
  side holding the link cannot account for any key under that name, so the merge holds those keys
  back. The source side already does this for an upload. The destination side starts doing it for
  a download.
- With following on, the walk looks behind each link it can resolve, so two keys can name one file.
  A delete through either key removes the file both keys name, so delete mode never deletes through
  a link or at a followed link's target.

One mechanism carries both cases. A side holds a `Stretch` under a name, `Side::missing` answers
`Unknown(UnknownRange)` for every key under it, and the comparison takes no action on an unknown
key (FR-Fail-7). The source side's stretches stop deletes. The destination side's stretches stop
transfers.

## What HEAD already does

- **An upload with a link the walk did not follow.** `Walk::take_source` adds a source stretch
  under the link's name.
- **An upload with a dangling link or a loop.** `cost_of` turns the walk error into a source
  stretch under the name.
- **A download with a link at a key's own name.** The link carries no size or time, so the
  comparison transfers, and the rename replaces the link.
- **A download delete through a followed link.** `LocalDestination::record_aliases` names the link,
  its target, and its chain. `Walk::hold_aliased` then adds source stretches.
- **A bound on held names.** Past `ALIASED_NAMES_HELD` names, the source holds every later key.
- **A late link.** `hold_aliased` marks the plan incomplete.
- **A removal before a download under it.** `Deletes.clearing` and `Transfers.held` remove a file
  at `photos` before `photos/a.jpg` lands.

## Changes

Each change names the probe or review comment it closes. The probes ran at HEAD `ea5c6fde`, and
their output is in `~/Documents/2026/Q4/tm/sync-links/probes-2026-10-28.md`.

### C1. Drop an excluded link at the merge position

`Walk::next` drops an excluded head with `Side::take`, and `take` releases every stretch and lost
key below the link's name. The other side may not have reached those keys yet. So a second excluded
link released the first link's hold (P1, r4395422459), and one excluded link released the hold of
an unreadable directory before it (P2).

The merge drops an excluded head only when the other side's head does not sort before it. Otherwise
the loop pairs the other side's earlier key first, with the holds still in place. The change touches
both excluded-link branches in `Walk::next`, about 12 lines.

### C2. Hold the keys under a destination link the walk did not resolve

A new `Walk::take_destination` mirrors `take_source`. When the destination head is an unfollowed
link, it adds a destination stretch under the link's name. The destination's excluded-link branch
adds the same stretch before it drops the link. `take_destination_only` and `take_both` call the
new method. About 15 lines.

Every source key under the link then reads unknown on the destination side, and no transfer under
the link reaches `claim_one`. The fix closes three cases:

- A download with delete mode off wrote through the link on every run (P5).
- A download wrote through a link whose name a filter excludes (P6).
- After a refused removal, later downloads wrote through the link (P3, r4396175840). A refused
  removal now has no downloads behind it.

Delete mode still removes a destination-only link, and the next run downloads the keys under its
name. Three tests assert the old behavior and change:

- `a_download_writes_through_a_destination_link_as_download_objects_does`
- `each_removal_on_a_path_runs_before_the_downloads_under_it`
- `an_unfollowed_destination_link_holds_nothing_back`

The `TODO(vnext)` on `take_source` goes away. `Deletes.clearing` stays, because a file at `photos`
still has to go before `photos/a.jpg` lands.

### C3. Report a dangling link as an entry at its own name

`FsWalk::read_dir` reports a dangling link only as a `BrokenSymlink` error. On a download, the
error's stretch covers the link's own name. A kept key at that name therefore never downloads, and
every run fails (P7, r4396285369's file case).

When `include_special_files` is on, the walk also yields the dangling link as a `FileType::Symlink`
entry with no metadata. The key stream maps it to `Obstruction::UnfollowedLink`, so C2 holds the
keys under its name, and the comparison replaces the link at its own name. `LocalDestination`
drops the `BrokenSymlink` error, so a download records no failure. An upload keeps the error, as
FR-Enum-3 asks. About 15 lines in `fs.rs` and `walk.rs`.

`include_special_files` is crate-private, and only sync sets it in production code. So
`upload_objects` sees no change.

### C4. Hold a followed file link's target when a filter excludes the link

`read_dir` records `FollowedLink::File` after the path filter check. An excluded file link
therefore names no target, and delete mode removed the target while the link stayed (review probe
Q4). The record moves above the check, about 2 lines.

### C5. Stop at a link to a directory that contains the root

The walk's cycle check compares a link's target with the directories from the root down. A link
`up -> ..` points above the root, so the walk entered it and walked the root again under `up/root/`
(review probe Q1). The walk seeds the root's ancestor chain with the handles of the directories
above the root, when following is on. A link to any of them then reports a loop at the link.
About 10 lines in `FsWalker::walk`, plus one `open` and `fstat` per directory above the root. This
also changes `upload_objects` with following on, where the same link walked the parent tree.

### C6. Resolve a root that is itself a link

`FsWalker::walk` refuses a root that is a link when following is off (review probe Q6). Sync
resolves an existing root once in `Walker::uploading` and `Walker::downloading`, and the walker
stays the same. About 8 lines.

### C7. Replace a followed link to a special file at a key's own name

With following on, a link to a socket at a kept key's name reads as the socket. The comparison
then skips it as obstructed (review probe Q3). The key stream maps a followed link with a special
target to an obstruction that allows the overwrite. About 8 lines in `stream.rs` and `state.rs`.

## Deferred

- **A download defaults to following.** No caller reaches the crate-private builder's default
  today. The public sync API applies the per-direction default when it lands.
- **A late link's target** (r4396285369, directory case). The fix needs every link known before the
  first delete. Sync would then hold deletes in memory, or spill them to a file the way rsync's
  `--delete-delay` does. FR-Exec-7 now names this case as its exception.
- **Two keys that reach one file** (FR-Fail-8). FR-Fail-8 has no implementation yet, and the link
  case shares its policy.

## Scope

Before C1 to C7, probes P1 to P7 and the three link review comments each end with a wrong outcome.
C1 to C4 change about 45 lines of production code, and each probe and review case now ends the way
the spec says. C5 to C7 close the other cells the spec now decides, at about 26 more lines.
Each change reuses the walk or the merge mechanism above, so none adds a new kind of state. About
10 tests change: 7 new ones, and the 3 above that flip.
