# Symlinks and delete mode

This auxiliary design doc to [execution](execution.md) focuses on symlinks in the local tree when
delete mode is on. It decides what a run may delete, and what a download may write, while a link
stands in the tree.

## Terms

| Term | Meaning |
|---|---|
| link | A symbolic link in the local folder. It points somewhere else, like a shortcut. |
| upload | The local folder is the source, and the bucket is the destination. |
| download | The bucket is the source, and the local folder is the destination. |
| key | A file's path below the root, such as `photos/a.jpg`. |
| walk | Sync's listing of the local folder, in key order. |
| merge | The step that lines up both sides' keys and decides what to do with each key. |
| following on | The walk looks inside each link. Writes and deletes may pass through it (D1). |
| following off | The walk lists a link as one entry. Nothing passes through it (D1). |
| delete mode | Sync removes each key the destination has and the source lacks. |
| hold | A mark on a name. The merge answers "unknown" for the name and every key under it. |

The comparison does nothing for a key whose side is unknown (**FR-Fail-7**), so a hold keeps sync
from deleting or writing there. Each case below names the decision that settles it.

## The cases

Each case is one way a link could make sync delete or write the wrong file.

| Case | Direction | Following | Delete mode | The risk without the design |
|---|---|---|---|---|
| A | upload | off | on | sync deletes bucket objects |
| B | upload | on or off | on | sync deletes a bucket object |
| C | download | off | on or off | sync writes outside the folder |
| D | download | on | on | sync deletes a file another key needs |
| E | download | on | on | sync deletes a file through a chain of links |
| F | download | on | on | sync deletes and restores a file on alternate runs |
| G | download | on or off | on or off | sync writes into the link's target |
| H | both | on or off | on or off | sync reads or writes above the root |
| I | download | off | on or off | sync writes outside the folder |
| J | upload | on or off | on | sync deletes a bucket object a link stands for |
| K | download | on | on or off | sync replaces a link to a drive that is away |
| L | download | on | on | nobody says whether the filter protects the target |
| M | download | on | on | sync deletes a file a new link names |
| N | download | on | on | a caller cannot tell why sync deleted nothing |

### A. Upload: a link the walk does not look inside

```
   local folder (source)            bucket (destination)
   ┌─────────────────────────┐      ┌──────────────────┐
   │ photos ──► /mnt/nas     │      │ photos/y.jpg     │
   └─────────────────────────┘      └──────────────────┘

   key            local side       bucket side
   ───            ──────────       ───────────
   photos         the link         nothing
   photos/y.jpg   nothing          object
```

**Setup.** This is an upload with following off and delete mode on. The local folder has a link
`photos` that points at `/mnt/nas`, a folder outside the root such as a network drive. The file
`/mnt/nas/y.jpg` exists. The bucket has the object `photos/y.jpg`, an earlier upload of that file.

**What each side yields.** With following off, the walk lists the link `photos` as one entry and
never looks inside it. So the local side yields `photos` and nothing under it. The bucket side
yields `photos/y.jpg`. The bucket has no key `photos`.

**The risk.** The merge reads a key as missing from a side when that side yields nothing there.

| Step | Local head | Bucket head | Without the design |
|---|---|---|---|
| 1 | `photos` | `photos/y.jpg` | pair `photos`: a link has nothing to upload, so skip it |
| 2 | nothing | `photos/y.jpg` | local side answers "missing", so delete the object |

Step 2 deletes `photos/y.jpg` from the bucket, while `/mnt/nas/y.jpg` still exists.

**What sync does.** After the merge pairs the link `photos`, the local side puts a hold on `photos`
(D2).

| Step | Local head | Bucket head | With the design |
|---|---|---|---|
| 1 | `photos` | `photos/y.jpg` | skip `photos`, then hold `photos` |
| 2 | nothing | `photos/y.jpg` | local side answers "unknown", so keep the object |

The run reports `photos/y.jpg` as `skipped-unknown` and ends `Warned`.

**Other settings.** With following on, the walk looks inside `photos` and lists `photos/y.jpg`, so
the merge pairs the two copies normally. Four kinds of link still hide keys with following on: a
link whose target is missing, a link into an endless chain of links, a link that loops back, and a
link the filter excludes. Each one gets the same hold. With delete mode off, step 2 never deletes,
but the hold still applies.

### B. Upload: two links the filter excludes

```
   local folder (source)                 bucket (destination)
   ┌─────────────────────────────┐       ┌──────────────────┐
   │ photos ──► /mnt/nas/photos  │       │ photos/y.jpg     │
   │ videos ──► /mnt/nas/videos  │       └──────────────────┘
   └─────────────────────────────┘

   key            local side       bucket side
   ───            ──────────       ───────────
   photos         marker           nothing
   photos/y.jpg   nothing          object
   videos         marker           nothing
```

**Setup.** This is an upload with delete mode on. The local folder has two links, `photos` and
`videos`. Each one points at a folder on `/mnt/nas`. The filter excludes both links. The bucket has
the object `photos/y.jpg`, a key under `photos`.

**What each side yields.** Sync never looks inside an excluded link (D2). The walk still yields each
excluded link once, as a *marker*: an entry marked `ExcludedLink`, with nothing under it. So the
local side yields the markers `photos` and `videos`. The filter also applies to the bucket listing,
so the bucket yields no key named `photos` or `videos`. Only the bucket side yields `photos/y.jpg`.

**What a marker does.** The merge does two separate things with a marker:

| Action | Acts on | Effect |
|---|---|---|
| hold | the name `photos` | Keys under `photos` answer "unknown" until the merge passes them. |
| skip past | the marker entry | The marker leaves the local list. It never pairs with anything. |

Skipping past an entry moves the merge past its name. The merge then treats every earlier key as
done, so every hold on an earlier name ends. The merge adds the marker's own hold after the skip, so
that hold survives.

**The risk.** Without D3, the merge skips past a marker as soon as the marker reaches the head of
the local list. It does not look at the bucket side first.

| Step | Local head | Bucket head | Without D3 |
|---|---|---|---|
| 1 | `photos` | `photos/y.jpg` | skip past the `photos` marker, then hold `photos` |
| 2 | `videos` | `photos/y.jpg` | skip past the `videos` marker, and the hold on `photos` ends |
| 3 | nothing | `photos/y.jpg` | local side answers "missing", so delete the object |

In step 2, `photos/y.jpg` sorts before `videos`, so skipping past `videos` moves the merge past it
too early.

**What sync does.** Before the merge skips past a marker, it looks at the other side's head. It
skips past the marker only when the other side's head does not sort before the marker (D3).

| Step | Local head | Bucket head | With D3 |
|---|---|---|---|
| 1 | `photos` | `photos/y.jpg` | skip past the `photos` marker, then hold `photos` |
| 2 | `videos` | `photos/y.jpg` | wait, because `photos/y.jpg` sorts first |
| 3 | `videos` | `photos/y.jpg` | pair `photos/y.jpg` as "unknown", and keep it |
| 4 | `videos` | nothing | skip past the `videos` marker, then hold `videos` |

The rule covers every marker, including `photos` in step 1. Step 1 is safe in this case only because
no bucket key sorts before `photos`. A bucket key `a/x.txt` under an unreadable local folder `a/`
would show the same risk at step 1: skipping past `photos` early would end the hold on `a/`. The
same rule protects a download.

### C. Download: a link the walk does not look inside

```
   bucket (source)            local folder (destination)
   ┌──────────────────┐       ┌───────────────────────────┐
   │ photos/a.jpg     │       │ photos ──┐                │
   └──────────────────┘       └──────────┼────────────────┘
                                         ▼
                              ┌───────────────────────────┐
                              │ /mnt/nas, outside folder  │
                              └───────────────────────────┘

   key            bucket side      local side
   ───            ───────────      ──────────
   photos         nothing          the link
   photos/a.jpg   object           nothing
```

**Setup.** This is a download with following off. The bucket has `photos/a.jpg`. The local folder
has a link `photos` that points at `/mnt/nas`, a folder outside the root.

**What each side yields.** The bucket side yields `photos/a.jpg`. With following off, the walk lists
the link `photos` as one entry and never looks inside it. So the local side yields `photos` and
nothing under it.

**The risk.** The walk with following off lists the link and never looks inside it. But a
download writes `photos/a.jpg`
by opening the path `root/photos/a.jpg`. The operating system resolves every directory on a path it
opens, and it follows a link there unless the caller asks it not to. The download code never asks.
So the write goes through `photos` and lands at `/mnt/nas/a.jpg`, even with following off. A
follow setting that governs only the walk leaves this gap. D1 closes it: one follow setting governs
the walk, the writes, and the deletes.

| Delete mode | Run | Without the design |
|---|---|---|
| off | 1 | write `/mnt/nas/a.jpg` through the link |
| off | 2 | the walk again lists nothing under `photos`, so write `/mnt/nas/a.jpg` again |
| on | 1 | remove the link first (execution's D12), then download into a new `photos/` |
| on, removal fails | 1 | write the later downloads under `photos` through the link |

With delete mode off, every run writes through the link again. With delete mode on, the link
usually goes away first. A removal fails when the operating system refuses it. For example, the run
may lack write permission on the folder that holds the link, or that folder may sit on a disk
mounted for reading only. Then the later downloads under the link write through it.

**What sync does.** After the merge pairs the link `photos`, the local side puts a hold on `photos`
(D2). A download under a hold does not start.

| Delete mode | Run | With the design |
|---|---|---|
| off | every run | hold `photos`, and skip `photos/a.jpg` as unknown |
| on | 1 | remove the link, hold `photos`, and skip `photos/a.jpg` as unknown |
| on | 2 | the link is gone, so download `photos/a.jpg` into a new `photos/` |
| on, removal fails | every run | hold `photos`, and skip `photos/a.jpg` as unknown |

With delete mode on, the run removes the link because the bucket has no key `photos`. The hold costs
one more run before `photos/a.jpg` arrives. The design accepts that cost, because one rule then
covers every row. The same hold applies when the filter excludes the link. Delete mode never removes
an excluded link, so the hold stays on every run.

### D. Download: two keys for one file

```
   local folder
   ┌──────────────────────────────┐
   │ link ──► real                │
   │ real/                        │
   │   x.txt                      │
   └──────────────────────────────┘

   keys the walk lists                         the file on disk
   ───────────────────                         ────────────────
   link/x.txt ──► link ──► real/ ──┐
                                   ├─────────► real/x.txt
   real/x.txt ─────────────────────┘
```

**Setup.** This is a download with following on and delete mode on. The local folder has a directory
`real` with the file `x.txt`, and a link `link` that points at `real`. The bucket has `real/x.txt`
and has no key under `link`.

**What each side yields.** With following on, the walk looks inside `link`, so it lists the same
file twice.

| Key | Bucket side | Local side |
|---|---|---|
| `link/x.txt` | nothing | the file `real/x.txt`, reached through `link` |
| `real/x.txt` | object | the file `real/x.txt` |

**The risk.** Delete mode removes a local key the bucket lacks. A delete removes the file at the
key's path, and the operating system follows the link on that path.

| Step | Key | Without the design |
|---|---|---|
| 1 | `link/x.txt` | bucket answers "missing", so delete `root/link/x.txt` |
| 2 | `real/x.txt` | compare the object with the local file |

Step 1 removes `real/x.txt`, because `link/x.txt` is the same file. The bucket still wants that
file. The next run downloads it again, and the run after that deletes it again. Swap the bucket's
key to `link/x.txt`, and the same thing happens: deleting `real/x.txt` removes the file that
`link/x.txt` names.

**What sync does.** The link scan finds `link` and names two keys: the link's own name `link`, and
its target `real` (D4, D6). The merge holds both names on the bucket side. A hold changes only the
answer for a key the bucket lacks: the bucket answers "unknown" there, and not "missing".

| Step | Key | With the design |
|---|---|---|
| 1 | `link/x.txt` | bucket answers "unknown", so keep it |
| 2 | `real/x.txt` | compare the object with the local file |

`link/x.txt` reports `skipped-unknown`, and the run ends `Warned`. The holds also keep every other
local file under `real` that the bucket lacks, so stale files there stay. With delete mode off, sync
deletes nothing, so it names no links and holds nothing.

**A target outside the root.** When `link` points at `/mnt/nas`, outside the root, the scan names
only `link`. The target has no key inside the root, so no name stands for it. The hold on `link`
covers every key under `link`, so sync never deletes inside `/mnt/nas`. Files there that the bucket
lacks stay, and each one reports `skipped-unknown`.

### E. Download: a chain of file links

```
   local folder
   ┌──────────────────────────────┐
   │ x.txt                        │   the real file
   │ y.txt ──► z.txt              │   a link
   │ z.txt ──► x.txt              │   a link
   └──────────────────────────────┘

   y.txt ──────► z.txt ──────► x.txt
```

**Setup.** This is a download with following on and delete mode on. The local folder has the file
`x.txt`. It also has the link `y.txt` that points at `z.txt`, and the link `z.txt` that points at
`x.txt`. The bucket has `y.txt` and has neither `z.txt` nor `x.txt`.

**What each side yields.** With following on, the walk reads each link as the file at the end of
its chain.

| Key | Bucket side | Local side |
|---|---|---|
| `x.txt` | nothing | the file `x.txt` |
| `y.txt` | object | the file `x.txt`, reached through two links |
| `z.txt` | nothing | the file `x.txt`, reached through one link |

**The risk.** Delete mode removes each local key the bucket lacks.

| Step | Key | Without the design |
|---|---|---|
| 1 | `x.txt` | bucket answers "missing", so delete the file `x.txt` |
| 2 | `y.txt` | compare the object with the local file |
| 3 | `z.txt` | bucket answers "missing", so delete the link `z.txt` |

Step 1 deletes the file that `y.txt` stands for. Step 3 removes a link in the middle of the chain.
After the run, `y.txt` points at nothing. The next run then downloads `y.txt` again.

**What sync does.** The link scan finds `y.txt` and names every later link on its chain, `z.txt`,
and the file at its end, `x.txt` (D4, D6). The merge holds both names on the bucket side.

| Step | Key | With the design |
|---|---|---|
| 1 | `x.txt` | bucket answers "unknown", so keep the file |
| 2 | `y.txt` | compare the object with the local file |
| 3 | `z.txt` | bucket answers "unknown", so keep the link |

The design never holds `y.txt` itself. If the bucket lacked `y.txt`, delete mode could remove
`y.txt`. Removing a link leaves its target in place, so that delete loses no file.

### F. Download: the walk finds a link after the merge passed its target

```
   local folder
   ┌──────────────────────────────┐
   │ real/                        │
   │   x.txt                      │
   │ z/                           │
   │   link ──► ../real           │
   └──────────────────────────────┘

   key order ─────────────────────────────────────────────────────►

                real/x.txt                       z/link/x.txt
                ▲                                ▲
                the merge decides here           the walk reads z/ only now,
                                                 and finds z/link ──► ../real
```

**Setup.** This is a download with following on and delete mode on. The local folder has the file
`real/x.txt`, and a link `z/link` that points at `real`. The bucket has `z/link/x.txt` and has no
key under `real`.

**What each side yields.** With following on, the walk lists the same file twice, as in case D.

| Key | Bucket side | Local side |
|---|---|---|
| `real/x.txt` | nothing | the file `real/x.txt` |
| `z/link/x.txt` | object | the file `real/x.txt`, reached through `z/link` |

**The risk.** The walk lists keys in order, and it finds a link only when it reads the directory
that holds the link. `real/` sorts before `z/`. So the merge decides on `real/x.txt` before the walk
reads `z/`, and the walk knows nothing about `z/link` yet. Case D's hold therefore comes too late.

| Run | Without the design | Result |
|---|---|---|
| 1 | delete `real/x.txt` | the file is gone |
| 2 | download `z/link/x.txt` through the link | the file is back |
| 3 | delete `real/x.txt` again | the file is gone |

The runs keep switching between deleting the file and downloading it back.

**What sync does.** A second walk, the *link scan*, lists every link in the local folder (D6). The
scan runs at the same time as the merge, so downloads start at once. Each local delete the merge
decides before the scan ends waits in a list.

| Step | Event | With the design |
|---|---|---|
| 1 | merge decides to delete `real/x.txt` | the delete waits for the scan |
| 2 | scan finds `z/link` | the scan names `z/link` and `real` |
| 3 | scan ends | `real/x.txt` falls under the name `real`, so keep it |
| 4 | merge pairs `z/link/x.txt` | compare the object with the local file |

Every run decides the same thing, keeps `real/x.txt`, and ends `Warned`. A file link
`z/y.txt -> ../a.txt` works the same way: the scan names `a.txt`, and `a.txt` stays. The waiting
list holds at most 1,024 deletes. When it fills, the merge pauses until the scan ends.

### G. Download: a link sits where a file will land

```
   bucket (source)     local folder (destination)
   ┌──────────────┐    ┌───────────────────────────┐
   │ y.txt        │    │ y.txt ──► its target      │
   └──────────────┘    └───────────────────────────┘
```

**Setup.** This is a download in any delete mode. The bucket has `y.txt`. The local folder has a
link named `y.txt`. The link's target can be a file, a directory, a socket, or nothing at all.

**What each side yields.** The bucket side yields `y.txt`. What the local side yields depends on the
follow setting.

| Following | The target | Local side yields at `y.txt` |
|---|---|---|
| off | anything | the link, with no size and no time |
| on | missing | nothing |
| on | a file | the target file's size and time |
| on | a directory | a marker with no size or time, then the keys under `y.txt/` |
| on | a socket | a socket, a kind of file sync cannot write |

**The risk.** A download writes a temporary file, then renames it to `y.txt`. A rename replaces the
directory entry at `y.txt`. When that entry is a link, the rename replaces the link itself and never
touches its target. A write through the link would change the target, and the target may sit
outside the folder. So the design must never write through the link.

**What sync does.** Sync either replaces the link through the rename, or keeps the link and skips
the key (D7). The table shows each choice. "Default modes" means the comparison modes other than
no-overwrite. No-overwrite downloads a key only when nothing sits at its name.

| Following | The target | Default modes | No-overwrite |
|---|---|---|---|
| off | anything | replace the link | keep the link |
| on | missing | replace the link | replace the link |
| on | a file | compare with the target, and replace the link if they differ | keep the link |
| on | a directory | replace the link | keep the link |
| on | a socket | skip the key | skip the key |

Each row has a reason:

- **Following off.** The link has no size or time, so no comparison can show that it matches. The
  default modes download. No-overwrite sees an entry at the name and keeps it.
- **A missing target.** Nothing sits at `y.txt`, so every mode downloads.
- **A file target.** The link counts as that file. When the comparison finds a difference, the
  rename replaces the link, and the target stays as it was.
- **A directory target.** The walk yields the link's own name, with no size or time, and then
  the keys inside it under `y.txt/`. The default modes download, and the rename replaces the
  link. No-overwrite sees an entry at `y.txt` and keeps the link.
- **A socket target.** Sync cannot write a socket, so every mode skips the key, as for a real
  socket.

### H. Both directions: the root itself is a link

```
   /data/
   ├── sibling/
   │   └── s.txt            above the root, outside the run
   ├── current ──► dest     the root the caller named
   └── dest/
       ├── a.txt
       └── up ──► ..        points at /data, above the root: a loop
```

**Setup.** This applies to an upload and a download. The caller names `/data/current` as the root,
and `current` is a link to `/data/dest`. Inside the root, the link `up` points at `/data`, the
directory that holds the root.

**What each side yields.** The local side depends on two things: how sync treats the root, and what
it does with `up`.

| Following | The root `current` | The link `up` |
|---|---|---|
| off | walked as `/data/dest` | one entry, never looked inside |
| on | walked as `/data/dest` | a loop: the walk stops at `up` |

**The risk.** Two things could go wrong without the design.

1. **A root that is a link.** A walk with following off could refuse a root that is a link, and the
   run would not start. But the caller gave that path on purpose, and the follow setting is about
   the entries inside the root.
2. **A link back above the root.** With following on, the walk would follow `up` into `/data`. An
   upload would then send `up/sibling/s.txt`, a file from outside the root. It would also send a
   second copy of `a.txt` as `up/dest/a.txt`.

**What sync does.** Sync opens the root the caller named, even when the root is a link (D8). The
follow setting covers only the entries inside the root. Before the walk follows a link to a
directory, it checks whether that directory contains the root. It compares the target with each
directory above the root. Sync identifies each directory by its device and inode, the numbers the
operating system gives every file. `up` points at `/data`, and `/data` contains the root, so the
walk treats `up` as a loop. It stops there and holds the keys under `up`.

A root that is a link to nothing exists as a link and is not a directory. The run ends before any
work, as **FR-Fail-6** says for a root that is not a directory.

### I. Download: a link the merge cannot match

```
   bucket (source)      local folder (destination), ignores case
   ┌──────────────┐     ┌──────────────────────────────┐
   │ photos/a.jpg │     │ Photos ──► /mnt/nas          │
   └──────────────┘     └──────────────────────────────┘

   key            bucket side      local side
   ───            ───────────      ──────────
   Photos         nothing          the link
   photos/a.jpg   object           nothing
```

**Setup.** This is a download with following off. The local folder is on macOS or Windows, where
the disk treats `photos` and `Photos` as the same name by default. The bucket has `photos/a.jpg`.
The local folder has a link `Photos` that points at `/mnt/nas`.

**What each side yields.** The bucket side yields `photos/a.jpg`. The local side yields the link
`Photos` and nothing under it, as in case C.

**The risk.** The merge compares names byte for byte, and case counts. `Photos` and `photos/a.jpg`
share no prefix byte for byte, so the hold from case C misses.

| Step | Key | Without D9 |
|---|---|---|
| 1 | `Photos` | hold `Photos` |
| 2 | `photos/a.jpg` | not under the hold, so local side answers "missing": download |
| 3 | the write | `photos` resolves to the link `Photos`: the file lands in `/mnt/nas` |

A link someone creates after the walk read its directory slips past every hold the same way.

**What sync does.** Right before sync writes or deletes a local file, it checks each directory on
the file's path for a link (D9). It uses `lstat`, a call that reports what sits at a path without
following a link there. The check asks the disk, and the disk ignores case, so `lstat` on
`root/photos` finds the link `Photos`.

| Step | Key | With D9 |
|---|---|---|
| 1 | `Photos` | hold `Photos` |
| 2 | `photos/a.jpg` | the merge decides to download |
| 3 | the check | `root/photos` is a link, so skip the key as unknown |

With delete mode on, run 1 also removes the link `Photos`, because the bucket has no key `Photos`.
Run 2 then downloads into a real `photos/` directory. With following off, the check runs before each
download write and each upload read. It runs before every local delete in both settings. With
following on, D1 lets a write go through a link above the file on purpose, so the check skips writes
and reads.

### J. Upload: a link sits at a key's own name

```
   local folder (source)            bucket (destination)
   ┌─────────────────────────┐      ┌──────────────────┐
   │ y.txt ──► its target    │      │ y.txt            │
   └─────────────────────────┘      └──────────────────┘
```

**Setup.** This is an upload with delete mode on. The local folder has a link named `y.txt`. The
bucket has the object `y.txt`. The link's target can be a file, a directory, or nothing at all.

**What each side yields.** The bucket side yields `y.txt`. What the local side yields depends on the
follow setting.

| Following | The target | Local side yields at `y.txt` |
|---|---|---|
| off | anything | the link, with nothing to send |
| on | a file | the target file |
| on | missing | an error for `y.txt` |
| on | a directory | a marker the merge skips, then the keys under `y.txt/` |

**The risk.** The AWS CLI with `--no-follow-symlinks` leaves the link out of its listing. The merge
then reads `y.txt` as missing from the local folder, and delete mode removes the object. The link
still stands for that file, so the delete loses a file the caller kept.

**What sync does.** The link counts the way the follow setting says (D7).

| Following | The target | Upload of `y.txt` | Bucket's `y.txt` |
|---|---|---|---|
| off | anything | skip it, `skipped-with-warning` | kept |
| on | a file | send the target's bytes if they differ | replaced if they differ |
| on | missing | fail the key | kept |
| on | a directory | send the keys under `y.txt/` | deleted |

Each row has a reason:

- **Following off.** The link is an entry at `y.txt`, so the local side has something there. Sync
  never deletes a key the source has (**FR-Fail-7**). A link has nothing to send, so the upload
  skips it with a warning.
- **A file target.** The link counts as that file, so the upload compares and sends it like any
  other file.
- **A missing target.** The source has a key it cannot read. The key fails, and the merge answers
  "unknown" for it, so the object stays.
- **A directory target.** The merge skips past the marker at `y.txt`, because the link has
  nothing to send there. The bucket's `y.txt` is destination-only, and delete mode removes it, as
  for a real directory named `y.txt`.

### K. Download: a broken link above a kept key, with following on

```
   bucket (source)            local folder (destination)
   ┌──────────────────┐       ┌───────────────────────────┐
   │ photos/a.jpg     │       │ photos ──┐                │
   └──────────────────┘       └──────────┼────────────────┘
                                         ▼
                              ┌───────────────────────────┐
                              │ /mnt/nas/photos, missing: │
                              │ the drive is not mounted  │
                              └───────────────────────────┘
```

**Setup.** This is a download with following on, in either delete mode. The local folder has a link
`photos` that points at `/mnt/nas/photos`. The network drive `/mnt/nas` is not mounted right now, so
the link points at nothing. The bucket has `photos/a.jpg`.

**What each side yields.** The bucket side yields `photos/a.jpg`. The walk tries to follow `photos`,
finds nothing, and reports an error for it. So the local side yields nothing at `photos` and nothing
under it.

**The risk.** The link most likely stands for a folder on a drive that will come back. Without the
design, two things could go wrong:

| Without the design | Result |
|---|---|
| treat `photos` as empty | write `photos/a.jpg` through a link to nothing |
| remove the link, download into a new `photos/` | the drive returns, and the link is gone |

**What sync does.** The walk's error becomes a hold on the keys below `photos` (D7). Nothing sits at
the name `photos` itself, so delete mode leaves the link alone.

| Following | Run 1 | Run 2 | Settles? |
|---|---|---|---|
| off (case C) | remove the link | download `photos/a.jpg` | yes |
| on | hold `photos/a.jpg` | hold `photos/a.jpg` | no |

With following on, every run reports `photos/a.jpg` as `skipped-unknown` and ends `Warned`. This
class never settles on its own, by design. It settles once the drive mounts again, or once the
caller removes the link. A link into an endless chain and a link that loops back get the same
hold.

### L. Download: an excluded link to a directory

```
   local folder (destination)
   ┌──────────────────────────────┐
   │ photos ──► real              │   excluded by the filter
   │ real/                        │
   │   x.txt                      │
   └──────────────────────────────┘
```

**Setup.** This is a download with following on and delete mode on. The local folder has
`real/x.txt`, and a link `photos` that points at `real`. The filter excludes `photos`. The bucket
has no key under `real`. It may hold `photos/x.txt`, but the filter excludes that key too.

**What each side yields.**

| Key | Bucket side | Local side |
|---|---|---|
| `photos` | nothing | marker (case B) |
| `real/x.txt` | nothing | the file `real/x.txt` |

**The question.** Does the excluded link `photos` protect the files it points at? An excluded link
would protect `real/x.txt` only if sync looked behind it. Sync never looks behind an excluded link
(D2), so the link scan never names `real`.

**What sync does.** The filter wins. It excludes `photos` and leaves `real` in scope, so
`real/x.txt` is a key like any other.

| Key | With the design |
|---|---|
| `photos` | skip past the marker, then hold `photos` |
| `real/x.txt` | bucket answers "missing", so delete it |

The link `photos` stays, and it now points at a directory without `x.txt`. A file link works the
same way. With `y.txt -> a.txt` excluded, delete mode may remove `a.txt`, and `y.txt` then points at
nothing. A caller who wants `real` kept excludes `real` too.

### M. Download: a link created during the run

```
   time ──────────────────────────────────────────────────────────►

      the scan reads z/        the merge decides       someone creates      the walk reads z/
      and finds no link        on real/x.txt           z/link ──► ../real   and follows z/link
```

**Setup.** This is a download with following on and delete mode on. The local folder has
`real/x.txt` and an empty directory `z/`. The bucket has `z/link/x.txt` and has no key under
`real`. While the run is going, another program creates the link `z/link` that points at `real`.

**What each side yields.** The scan reads `z/` before the link exists, so the scan names nothing.
The walk reads `z/` after the link exists, so it lists `z/link/x.txt`.

**The risk.** No hold stands on `real`, and the scan has already ended.

| Step | Event | Result |
|---|---|---|
| 1 | the scan reads `z/` | no link yet, so the scan names nothing |
| 2 | the merge decides on `real/x.txt` | the bucket lacks it, so delete it |
| 3 | another program creates `z/link` | the link now names `real/x.txt` |
| 4 | the walk follows `z/link` | download `z/link/x.txt` through the link |

Step 2 deletes a file the new link names, and step 4 writes it back. A run that reports `Clean`
would hide that the folder changed under it.

**What sync does.** Preventing step 2 would need a fresh scan before each delete. That costs a whole
scan per delete, so sync accepts the window and reports it. The destination walk records each link
it follows. When the walk follows a link the scan never named, sync marks the plan incomplete, and
the run ends `Warned` (D6). The next run's scan finds `z/link`, holds `real`, and keeps
`real/x.txt`. A link removed during the run only leaves an extra hold, and that costs nothing.

### N. Download: a tree with more link names than the bound

```
   local folder (destination)
   ┌──────────────────────────────┐
   │ node_modules/                │   thousands of links, as pnpm builds it
   │   a ──► .pnpm/a@1/...        │
   │   b ──► .pnpm/b@2/...        │
   │ old.txt                      │   the bucket lacks it
   └──────────────────────────────┘
```

**Setup.** This is a download with following on and delete mode on. The local folder holds more
than 1,024 followed links, as a JavaScript project installed with pnpm does. It also holds
`old.txt`, and the bucket lacks that key.

**What each side yields.** The link scan names each link and each target. Past 1,024 names it stops
recording (D5).

**The risk.** Sync cannot tell which local deletes are safe, so it keeps every one. Delete mode then
removes nothing anywhere in the folder, even in folders with no links. A run that reports only
`Warned` and a long list of `skipped-unknown` keys leaves the caller guessing why.

| Step | What happens |
|---|---|
| 1 | the scan passes 1,024 names and stops recording |
| 2 | the scan marks itself past the bound |
| 3 | sync keeps every local delete in the run |
| 4 | each kept key reports `skipped-unknown` |
| 5 | the run ends `Warned`, and it records that it passed the bound |

**What sync does.** The run records that the tree passed the bound, and it logs a warning that names
the bound and two ways out: turn off following links, or exclude the folders that hold many links.
The walk keeps the links it follows while the scan reads, and the same bound applies to them.
When the walk follows more links than the bound, every waiting delete stays, whatever the scan
found. The tree alone decides whether a run passes
the bound, so every run over the tree ends the same way.

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

The rules for a link the walk does not resolve also govern the writes a download makes, with delete
mode on or off (D2). A write and a delete on one name have to agree about what the name is. The
decisions below meet **FR-Enum-4**, **FR-Enum-14** and **FR-Enum-15** in
[`functional-spec.md`](functional-spec.md). Two keys that differ only in case (**FR-Fail-8**), and a
key that leaves the root through its own path (**FR-Root-8**), stay outside this design. A key whose
name matches a link's name only without regard to case is inside it (case I).

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
link qualifies. With following on, a dangling link, a link into an endless chain, a loop, and a link
whose target the walk cannot read qualify. A link the filter excludes qualifies in both settings.
The walk checks the filter before it resolves a link, so sync never looks behind an excluded link.

The side holding the link cannot account for any key under the link's name, so the merge holds the
name on that side. On an upload the source holds, and delete mode removes no destination key under
the name (case A). On a download the destination holds, and the run writes no source key under the
name (case C). The run reports each held key as `skipped-unknown` and marks its plan incomplete. The
hold rests on what the walk could not see, so it applies with delete mode off too.

An excluded link protects nothing it points at (case L). Delete mode may remove an excluded file
link's target and leave the link dangling. It may also remove the files under an excluded directory
link's target. The run reports nothing for the link itself.

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

**D3. The merge skips past an excluded link only at the merge position.** A link whose name the
filter excludes makes no pairing, so the merge skips past it in the stream. Skipping past an entry
releases every hold below its key, because the merge treats those keys as passed. An excluded head
can sort ahead of the merge position while the other side has an earlier key to pair. Skipping past
the head there releases holds the earlier key needs (case B). So the merge skips past an excluded
head only once the other side has no earlier key to pair. The rule protects every other hold too. An
excluded link that sorts after an unreadable directory leaves that directory's hold in place.

**D4. On a download with following on and delete mode on, a delete never passes through a link or
removes what a link reaches.** A delete removes one directory entry, and two keys can name one file
(case D). The link scan names the keys each followed link reaches, and the merge holds the scan's
names on the source side (D6). The
merge holds a directory link's own name, so no delete passes through the link. It also holds the
link's resolved target when the target sits inside the root, whatever kind of file the target is.
For a file link, it holds every later link on the chain as well (case E). A file link's own name
stays deletable, because removing a link leaves its target.

The holds stand even when the bucket keeps neither key. To delete the target safely, the merge would
need the bucket's answer for the key through the link. That key can sort after the target, so the
merge would have to keep each such delete until the key pairs. The number of such deletes grows
with the tree. So with delete mode on, a download with following on keeps stale files under a
link's target, reports them as `skipped-unknown`, and ends `Warned`.

The holds exist only to stop deletes. With delete mode off, the run names no link targets and holds
nothing. A destination-only key through a link then reports as destination-only, and a run over a
tree like `current -> releases/v3` ends `Clean`.

**D5. The link scan keeps at most 1,024 names.** A tree with one link per directory would give the
scan one name per directory. **NFR-Mem-1** rules out memory that grows with the tree. Past 1,024
names, the scan stops recording, and the run holds every local delete. Each held key reports as
`skipped-unknown`, and the run ends `Warned`. The run also records that it passed the bound, and it
logs a warning that says so (case N). The bound counts every name in the tree, wherever the
merge stands. So the tree alone decides whether a run passes the bound, and two runs over one tree
decide the same deletes. The bound matches the 1,024 transfers that may wait before the merge
pauses (execution's D11), so the names cost about what that queue costs.

**D6. Local deletes wait for a scan of the destination's links.** The walk meets a link only when it
reads the directory holding the link. When that directory sorts after the link's target, the merge
decides deletes under the target before the walk knows the link (case F). A delete there removes a
file a kept key names. The next run downloads the file back, and the run after deletes it again.
Preventing that needs every link known before the first delete.

So on a download with following on and delete mode on, a second walk of the destination starts
with the run. This *link scan* reads every directory once and records only links. It applies the
walk's rules: the same filter, the same loop check (D8), and no resolution of an excluded link
(D2). It turns each link into names the way D4 says. The scan skips a directory it cannot read. The
walk cannot read that directory either, so no key reaches a file through a link inside it.

The merge does not wait for the scan. Downloads start as the merge decides them, so the first
transfer starts as promptly as before (**NFR-Lat-1**). Local deletes wait for the scan.
Each delete the merge decides before the scan ends goes into a waiting list. When the scan ends,
the run checks each waiting delete against the scan's names. A delete under a name reads as unknown
and reports as `skipped-unknown`. Every other waiting delete goes out. From then on the merge holds
the scan's names as D4 says, and new deletes go out at once.

The waiting list holds at most 1,024 deletes. When it is full, the merge pauses until the scan
ends. That pause is a fourth watermark beside execution's D11. A download held behind a waiting
removal (execution's D12) waits for the scan too. In those two cases a transfer waits for the scan,
and in no other.

The scan costs one `readdir` for each directory and one `stat` for each link. On Linux and macOS
`readdir` reports each entry's type, so the scan never stats a regular file. The walk calls `stat`
on every file, so the scan costs less than the walk. The `walk_sync_link_scan` group in
`benches/walk.rs` measures both on a balanced tree. There the scan costs about 0.6 of the walk,
and the walk and the scan, run one after the other, cost about 1.6 walks. The benchmark reads a tree
held in the page cache, so it shows how the two reads compare, and not what a cold disk adds. The
scan and the walk run at the same time. A run with fewer than 1,024 deletes waiting never pauses the
merge for the scan.

The tree alone decides each delete, so every run over one tree decides the same deletes. With
delete mode off, nothing deletes, so the run starts no scan.

The scan covers each link that stands when the scan reads its directory. Another program can
create a link later in the run, after a delete under its target went out (case M). Preventing that
would need a fresh scan before each delete. So sync reports the window: the destination walk
records each link it follows, and a followed link the scan never named marks the plan incomplete.
The next run's scan finds the link and holds its names.

A dry run starts the scan too, because the scan changes decisions. A real download with following
on can still write through a link before its walk reads the link's target, and a dry run writes
nothing. So the dry run's plan can differ from the real run's, the exception **FR-Dry-2** allows.

Three alternatives cost more:

- Scanning before the merge starts makes every download wait for a whole local walk. **NFR-Lat-1**
  rules that out.
- Marking the plan incomplete keeps the flip between runs and only reports it.
- A waiting list with no bound lets memory grow with the deletes. A spill file past the bound, as
  rsync's `--delete-delay` keeps, removes the pause. It also adds a new kind of state to the run.

**D7. What stands at a key's own name counts the way the follow setting says.** With following off,
a link at a key's own name is an entry with no size or time. No comparison can show that it
matches, so the default modes download, and the rename replaces the link. No-overwrite sees an
entry and keeps the link (case G).

With following on, a link at a key's own name counts as what it reaches:

- A dangling link, or a link into an endless chain, reaches nothing. Every mode downloads, and the
  rename replaces the link. Delete mode leaves a destination-only dangling link, because nothing is
  at its name. The walk's error becomes a hold on the keys below the name, and the name itself
  stays free. A bucket key below the name therefore waits on every run, until the link resolves or
  the caller removes it (case K).
- A link to a file compares as that file. When a mode finds a difference, the download's rename
  replaces the link and leaves the target alone.
- A link to a socket, FIFO, or device reads as that file, and every mode skips the key as
  obstructed.
- A link to a directory takes its own name, and the walk also descends into it. The walk yields
  the name as an entry marked `LinkToDirectory`, with no size or time. That entry pairs only with
  the same key on the other side. In a download's destination, the default modes download and
  replace the link, as **FR-Enum-4** says. No-overwrite sees an entry there, so it keeps the link
  and reports the key as already at the destination. With no source key at the name, the merge
  skips past the entry, so delete mode never removes the link. In an upload's source, the name has
  nothing to send, so the merge skips past it, and a bucket key at the name is destination-only.
- A loop reaches a directory too. The walk stops at the loop, and only the keys below its name stay
  held, so its own name counts like a link to a directory.
- A link whose target the walk cannot read stays unknown at its own name as well. Its name and the
  keys below it stay held, and the failure follows the run's failure policy.

A dangling link or an endless chain in a download's destination is no failure in either setting. An
upload still reports one as a failure, because the source has nothing to send for its key. On an
upload with following off, a link at a key's own name has nothing to send either. Sync reports the
key as `skipped-with-warning` and keeps the bucket's object (case J).

**D8. The run starts at the root the caller named.** The follow setting covers the entries inside
the root. The run resolves the root itself as spelled, even when the root is a link, and the
directories above the root stay outside the run (case H). A root that is a dangling link exists and
is no directory, so the run ends before any work, as **FR-Fail-6** says for such a root. The run
checks the root with `lstat` for that reason. A check that follows the link would read a dangling
root as a directory nobody has created yet.

A link to a directory that contains the root is a loop, and the walk stops at the link. The check
covers the directories above the root as spelled and above the resolved root, because a link can
reach either chain. The walk keeps the directories on its descent path, and it compares a link's
target against them. So the directories above the root join the path when the walk starts. The walk
opens a directory's handle only when a link to a directory first asks for it, and children share the
handle. A tree without such links opens none. Opening a handle needs read permission. A directory
above the root that the run cannot open matches nothing, and a link to it fails the same way: the
walk cannot read that directory either, so it reports the link's subtree as unreadable.

**D9. Sync checks a path again when it acts.** The holds come from the walk, and two things slip
past them (case I). The filesystem can match a key's name to a link's name without regard to case or
Unicode form, and the merge compares bytes. A link can also appear after the walk read its
directory.

So sync calls `lstat` on each component of the key's path below the root before it acts. It does
that before each local delete, and with following off before each upload read and each download
write. When a component is a link, sync skips the action, reports the key as `skipped-unknown`, and
marks the plan incomplete. The key's own name stays out of the check, because delete mode removes a
link as a link and a download replaces one. With following on, a read or a write passes through a
link above the file by design (D1), so it skips the check. The download opens its temporary file
with `create_new`, so a link planted at that name refuses the open, and the download retries with a
new name.

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
For brevity, the excerpts in this section show `Stretch` with its `under` field only. The full type
also carries the cause, and a form that covers only the keys below a name.

### Where each hold comes from

```
   link                            held on           names held                added in
   ────                            ───────           ──────────                ────────
   not followed, upload            the source        its own name              take_source
   not followed, download          the destination   its own name              take_destination
   excluded by the filter          its side          its own name              Walk::next
   dangling or endless, upload     the source        its own name              cost_of
   dangling or endless, download   the destination   the keys below its name   cost_of         
   in a loop                       its side          the keys below its name   cost_of
   target cannot be read           its side          its own name              cost_of
   followed, delete mode on        the source        own name, target, chain   accept_link_scan
```

Every row but the last covers a link the walk does not resolve (D2). A link's side is the source on
an upload and the destination on a download. The walk checks the path filter before it resolves a
link, in both settings, and yields an excluded link marked `PathFiltered::Excluded`. The key stream
reports that mark as `Obstruction::ExcludedLink`. With following on, the walk reports a dangling
link, an endless chain, and a loop as errors. On a download's destination, `cost_of` turns a
dangling link or an endless chain into a stretch over the keys below the name, and the merge drops
the error. `cost_of` gives a loop that same stretch on both sides. One method on a side holds the
keys under a link the walk did not resolve, on either side:

```rust
fn take_holding(&mut self) -> Entry<K::Source> {
    let entry = self.take();
    if matches!(
        entry.meta.obstruction,
        Some(Obstruction::UnfollowedLink | Obstruction::ExcludedLink)
    ) {
        self.gap.add(Stretch {
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

The link scan is a second `FsWalk` over the destination root, with the walk's follow setting and
filter. It runs as its own work item and yields no entries to the merge. `FsWalk` records each link
it follows during one `next` call. The scan turns each link into names after every call, including a
call that returns an error. A name comes in one of two kinds, because the merge reaches the two at
different points:

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
and resolves to `/private/var/...`, so each kind of name takes its key under its own root. A target
takes its key by removing the resolved root from the front of its resolved path. A target whose
resolved path does not start with the resolved root takes no key. The merge holds no name for it.

A later link on a file link's chain takes its key from its resolved parent directory and the name in
the link's own text.

`Walker::downloading` takes the delete mode. A download with following on and delete mode on
starts a link scan. Every other run starts none, and its walk drops the links it follows, so it
resolves no link targets (D4).

The destination walk follows the same links the scan does, so it names a link only when the scan
missed it. The scan records the path of each link it follows, and the walk looks a link up there
before it names it. Someone created a link the scan missed after the scan read its directory (case
M). Both name a link with one `LinkNamer`, so each link costs its `canonicalize` calls once.

The scan reads with `SortOrder::Native`, because it needs every link and no key order. Many links
can reach one target, so the scan counts each name once before it judges the bound (D5).

### Waiting for the scan

The run takes the scan from the walk when it starts. The scan leaves `State` while a work item reads
it, as the merge does, and each item reads up to `MERGE_BATCH` directories. The item does not yield
between directories, because the batch already bounds it. While the scan reads, `Deletes` holds
every local delete in its `waiting` list, and no batch goes out. The keys still join `clearing`, so
a download behind a waiting removal waits too (execution's D12).

The walk tracks where the run stands with its scan:

```rust
enum Links {
    // No scan: the run deletes nothing through a link, and the walk drops the links it follows.
    NoScan,
    // The scan is still reading. `followed` keeps each link the walk follows now.
    Scanning { namer: LinkNamer, followed: Vec<FollowedLink> },
    // The scan ended. `seen` holds its link paths and `names` its names, both sorted.
    Scanned { namer: LinkNamer, seen: Vec<PathBuf>, names: Vec<String> },
}
```

When the scan ends and the merge is back in `State`, the run hands the scan to the walk. The walk
names the links in `followed` that the scan missed, holds every name on the source side, and returns
the names sorted. The run keeps each waiting delete the returned names cover, and it sends the rest:

```rust
let names = walk.accept_link_scan(scan);
let (kept, sent): (Vec<String>, Vec<String>) = self
    .deletes
    .waiting
    .drain(..)
    .partition(|key| names.as_deref().is_none_or(|names| names_cover(names, key)));
```

`None` means the tree passed the bound. The walk keeps one count of the names it holds for links and
one flag for the bound. Past the bound, the walk holds every delete on the source side, and every
waiting delete stays. The run reports each kept delete as `skipped-unknown` and marks the plan
incomplete. A download held behind a kept key fails, as it does behind a refused removal.

`names_cover` finds the key or a directory above it in the sorted names with a binary search per
path level. After the scan, the walk marks the plan incomplete when it names a missed link whose
name the merge already reached (case M).

### Checking a path when sync acts

The walk records whether the run checks reads and writes, and execution reads that from the walk
when the run starts. A local delete always gets the check. With following off, each upload read and
each download write get it too. The check walks the key's parent components below the root:

```rust
pub(crate) fn link_on_path(root: &Path, key: &str) -> bool {
    let Some((parents, _own_name)) = key.rsplit_once('/') else {
        return false;
    };
    let mut at = root.to_path_buf();
    for part in parents.split('/') {
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(meta) if meta.file_type().is_symlink() => return true,
            Ok(_) => {}
            // Nothing below an absent component can be a link, and an unreadable component fails
            // the action itself.
            Err(_) => return false,
        }
    }
    false
}
```

`symlink_metadata` goes through the filesystem's own name lookup. On a filesystem that ignores case,
`photos` therefore finds the link `Photos` (case I).

Execution checks a transfer in `poll_work` after it claims a slot, just before the spawn. A key with
a link on its path gives the slot back and reports `skipped-unknown`. Execution checks a local
delete in the delete work item, just before the removal. A kept delete reports `skipped-unknown`,
and each download held behind it fails, as behind a refused removal. Both paths mark the plan
incomplete.

### What this design does not decide

The public entry point sets the download default from D1. The crate-private `WalkerBuilder`
defaults to not following in both directions.

Two keys can reach one local file through a followed link, and both can download. With
`link -> real` and a bucket holding `link/x.txt` and `real/x.txt`, the last write wins.
**FR-Fail-8** governs that case.

A bind mount or a mount point inside the root gives one file two keys with no link, and `lstat`
cannot tell. D4 holds nothing there. A hard link needs no decision, because a delete removes one
name and leaves the other.

On Windows a rename cannot replace a directory link, so a download over one fails there.

**FR-Exec-4** adds an opt-in that removes emptied directories. That opt-in needs D4's rule: it must
not remove a directory through a link.

A Windows device name such as `NUL` names a device in every directory, so a key like that leaves the
root through its own name. **FR-Root-8** covers it.

Link naming compares paths as text. Two spellings of one place defeat it in two ways.

- macOS can spell one directory as `/Users/...` and as `/System/Volumes/Data/Users/...`. A target
  resolved to the second spelling under a root resolved to the first takes no key. The merge then
  holds no name for that target.
- A link's own text can spell a later link on its chain differently from the stored name, as `Z.TXT`
  for a stored `z.txt` on a filesystem that ignores case. The hold then sits under `Z.TXT`, and the
  stored key `z.txt` stays deletable.

Comparing the identity of each directory, its device and inode, would close both gaps.
