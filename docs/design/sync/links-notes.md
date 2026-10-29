# Working notes: symlinks and delete mode

This file is temporary and lives on the branch `links-design`. The work produces one spec
section for local symlinks with delete mode. A design follows it, and then a scope call decides
whether the work fits PR 202 or moves to a follow-up PR.

## Why the patch cycle

The merge assumes that one key names one directory entry inside the root, and that one entry has
one key. A link breaks that assumption in five ways:

| Break | What goes wrong | Findings |
|---|---|---|
| Alias | a delete through one key removes the file another key keeps | 9c454439, r4396285369 |
| Hide | delete mode reads keys under a link as absent | 0da8a90b, 17ccc0ac, r4395422459 |
| Escape | a write or a delete lands outside the destination | a504ce75, r4396175840 |
| Stand-in | the comparison and the write disagree at a link's own name | 70865883, cde1e3bb |
| Unknown | the walk cannot say whether a subtree exists | r4396285369, file-link runs |

An alias means two keys name one file. A hidden key sits under one name that covers keys nobody
listed. An escape is a key whose path leaves the root. A stand-in is a link at a key's own name.
An unknown comes from a dangling link or a cycle.

## Two views of the local tree

- **View F, following on.** `root/key` names whatever path resolution reaches. Reads, writes, and
  deletes all go through links. Its hazards are aliases and escapes.
- **View L, following off.** A link is an entry at its own name. Sync never reads, writes, or
  deletes through it. Its hazard is the keys under a link: hidden keys on an upload, and an
  obstruction on a download.

A download with following off reads in View L and writes in View F today. It writes through
directory links, the way `download_objects` does. Three defects come from that split:

- With delete mode off, the run downloads the same keys again on every run.
- After a refused removal, later downloads write through the link (r4396175840).
- A destination link whose name a filter excludes probably takes writes too. A probe has not
  confirmed this yet.

## Draft rules for the spec section

- **R1, one view per setting.** The follow setting decides, for every action on the local tree,
  whether sync resolves a link.
- **R2, uploads.** Sync keeps every destination key under a local link that the source walk did
  not resolve, and reports it as skipped-unknown. Four kinds of link qualify: an unfollowed link,
  a link whose name a filter excludes, a dangling link, and a link in a cycle. r4395422459 breaks
  this rule: dropping an excluded link calls `Side::take()`
  away from the merge position.
- **R3, downloads with following on.** A delete removes one directory entry. Delete mode MUST NOT
  remove an entry that a kept key resolves through, or an entry outside the root. Sync reports
  each key it keeps this way as skipped-unknown.
- **R4, late links.** The walk can find a link after the merge decided deletes under its target.
  The spec has to decide this case.
- **R5, downloads with following off.** A key under a link waits while delete mode removes the
  link. When the link stays, sync does not write the key. Four cases leave the link standing:
  - Delete mode is off.
  - The removal fails.
  - A filter excludes the link's name.
  - The source keeps a key at the link's name.
- **R6, a link at a key's own name.** The download replaces the link in both settings, and it
  never writes through it. One case is open: today a dangling link at a kept key's own name reads
  as unknown, and every later run fails.
- **R7, a bound.** Sync bounds the names it holds. Past the bound, sync keeps every later
  destination-only key.

## Open decisions for the user

1. Which default should the follow setting take on a download? Today it is off. On matches
   `download_objects` and the AWS CLI. With off and R1, a default download skips every key under
   a destination link.
2. How should sync handle a late link (R4)? Four options exist:
   - (a) Accept the case and report it, as the code does today.
   - (b) Defer local deletes to the end of the walk. Memory then grows with the deletes.
   - (c) Run a link scan beside the merge, and hold deletes until it finishes. The reviewer
     suggested this. It costs a second traversal.
   - (d) Leave links inside the root unfollowed on a download. With delete mode on, that breaks
     `current -> releases/v3`.
3. May delete mode remove a file outside the root through a link? The recommendation is never.
   The AWS CLI removes such files.
4. Should a download replace a dangling link at a kept key's own name (R6)?

## Next steps

1. Probe HEAD with the three reviewer tests. Then probe four more cases:
   - Run a follow-off download twice with delete mode off.
   - Download into a tree whose link name a filter excludes.
   - Download a kept key whose local name holds a dangling link.
   - Upload a tree with an unreadable directory and one excluded link.
2. Survey precedent with citations: the AWS CLI v2, rsync, rclone, and the Java and Go transfer
   managers.
3. Draft the spec section, and have a subagent review it.
4. Write the design and estimate the scope.
