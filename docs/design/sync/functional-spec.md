# S3 Sync — Functional Specification

## Motivation

The transfer manager can upload a directory and download a prefix, but has no way to make a
destination *match* a source. Callers who need that shell out to
[`aws s3 sync`](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html) or write the comparison
themselves.

## Background: why not just copy `aws s3 sync`

`aws s3 sync` defines what users expect, so parity is the baseline and every requirement names the
`aws s3 sync` behavior it matches or departs from. Citations name a file and symbol under
`awscli/customizations/s3/`.

Separately, the AWS CLI issue tracker records where people want that behavior improved. Each of those
reports is carried by a requirement below, so the transfer manager addresses it from day one.

| Reference behavior | Reported as | Addressed by |
|---|---|---|
| Comparison is size + mtime, so a changed file of unchanged size is skipped and the run reports success | [#8377](https://github.com/aws/aws-cli/issues/8377) (same size, different MD5), [#7011](https://github.com/aws/aws-cli/issues/7011) (one-character CSV fix), [#9074](https://github.com/aws/aws-cli/issues/9074) (35 reactions), [#6750](https://github.com/aws/aws-cli/issues/6750) (87 reactions) | FR-Cmp-9, FR-Cmp-10 |
| The timestamp rule surprises people | [#3273](https://github.com/aws/aws-cli/issues/3273) (80 reactions), [#3415](https://github.com/aws/aws-cli/issues/3415) (21 reactions), [#4460](https://github.com/aws/aws-cli/issues/4460) | FR-Cmp-1, FR-Cmp-3, FR-Cmp-10 |
| Permissions are not preserved between buckets, and no option does it | [#901](https://github.com/aws/aws-cli/issues/901) (66 reactions) | FR-Exec-17 |
| Property drift is undetected — a changed `--acl` or `--cache-control` updates nothing | [#3215](https://github.com/aws/aws-cli/issues/3215) (26 reactions), [#4881](https://github.com/aws/aws-cli/issues/4881) | FR-Cmp-11 |
| One filesystem error ends or silently truncates the run | [#487](https://github.com/aws/aws-cli/issues/487), [#425](https://github.com/aws/aws-cli/issues/425) | FR-Enum-3, FR-Fail-2 |

Being a library also changes requirements on its own terms: the CLI's answers to "what does the caller
see" are stdout lines and an exit code, which must become a returned result and per-entry values the caller
receives.

## Conventions

**Requirement IDs** — `FR-<Category>-<n>` for functional requirements, `NFR-<Category>-<n>` for
non-functional ones, numbered from 1 within each category: roots (`Root`), enumeration (`Enum`), comparison
(`Cmp`), filtering (`Filter`), execution (`Exec`), failures (`Fail`), dry run (`Dry`), observability (`Obs`),
and for NFRs cost, latency, memory and throughput (`Cost`, `Lat`, `Mem`, `Tput`). Numbering stays contiguous
while this is a draft and freezes on approval.

**MUST**, **MUST NOT**, **SHOULD** and **MAY** carry their RFC 2119 meanings.

**Citations** — one per requirement, inline immediately below it. Its tag says where the requirement came
from and how much latitude an implementation has:

| Tag | Where it comes from | Latitude |
|---|---|---|
| `[CLI]` | Read directly from AWS CLI source, under `aws-cli` branch `v2`, `awscli/customizations/s3/`. | Parity. Not ours to redesign; check the code against the cited source. |
| `[DOC]` | Stated in the `aws s3 sync` reference documentation. | Parity, as above. |
| `[DERIVED]` | Stated nowhere, but a necessary consequence of cited CLI code. | Check the derivation. |
| `[ISSUE]` | A public `aws/aws-cli` issue, cited by number with its reaction count. | A departure from `aws s3 sync`. Quietly reverting to its behavior is a regression against a known complaint. |
| `[NEW]` | Absent from the CLI and not raised publicly. | Ours, unratified by anyone outside this document. Each one is a decision to ratify. |

Sources read on 2026-08-25: `aws-cli` `v2` branch (`comparator.py`, `filegenerator.py`, `filters.py`,
`fileformat.py`, `fileinfo.py`, `s3handler.py`, `subscribers.py`, `results.py`, `subcommands.py`,
`utils.py`, `syncstrategy/*.py`) and `s3transfer` `develop` (`download.py`).

Issue survey method: GitHub search API against `aws/aws-cli`, top results by reaction count for
`s3 sync` queries, read on 2026-08-25. **Not exhaustive** — it samples the most-reacted issues, so
low-traffic but valid requests may be missing. Reaction counts are point-in-time.

---

## 1. Definitions

| Term | Meaning |
|---|---|
| **root** | A source or destination location: a local directory, or a bucket plus key prefix. |
| **entry** | One comparable unit under a root: a local file, or an S3 object. |
| **relative key** | An entry's name relative to its own root, with `/` as the separator. The join key between the two sides. An S3 object key is Unicode encoded as UTF-8, so a relative key is too, and a local name that is not valid UTF-8 has no key it could take (FR-Enum-3). |
| **side state** | What enumeration established for a key on one side: **present** (it produced the entry), **absent** (it passed the key's position in key order without producing one), or **unknown** (enumeration failed somewhere covering the key). |
| **pair state** | For a relative key that is present or absent on both sides: `SrcOnly`, `DestOnly`, or `Both`. |
| **action** | The decision for a relative key: `Transfer`, `Delete`, or `Skip`. |
| **direction** | `Upload` (local → S3), `Download` (S3 → local), `Copy` (S3 → S3). |
| **plan** | The complete set of (relative key, action) decisions for a run. |
| **run** | One invocation: enumerate both roots, decide an action per relative key, execute. |

---

## 2. Directions and roots (`FR-Root-*`)

What a run is pointed at, which way bytes move, and which configurations sync refuses.

**FR-Root-1** Sync MUST support `Upload`, `Download`, and `Copy`. Local → local is out of scope.
*`[CLI]` `subcommands.py` → `CommandArchitecture.run`, `cmd_translation = {'locals3': 'upload', 's3s3': 'copy', 's3local': 'download'}`; `SyncCommand.USAGE` lists exactly those three path pairs.*

**FR-Root-2** A root always names a place that holds entries — a local directory, or a bucket plus key
prefix — never one entry. **There is no single-object sync and no non-recursive mode:** every entry beneath
a root takes part, and a caller who wants just one object uses a single-object transfer instead.

This removes a question that would otherwise need answering every run: given `s3://bucket/photos`, is
`photos` an object or a folder full of them? Only the second reading exists.
*`[CLI]` `subcommands.py` → `CommandParameters.__init__`: `if self.cmd in ['sync','mb','rb']: self.parameters['dir_op'] = True`; `SyncCommand.ARG_TABLE` has no `RECURSIVE` entry.*

**FR-Root-3** The relative key MUST be the entry's path with its own root removed and any `\` separators
changed to `/`. A destination path MUST be the destination root joined to that relative key.

So syncing `/data` to `s3://bucket/backup` turns the file `/data/logs/app.log` into the relative key
`logs/app.log` and the object `s3://bucket/backup/logs/app.log`.
*`[CLI]` `utils.py` → `find_dest_path_comp_key` computes the relative key and normalizes separators.*

**FR-Root-4** A trailing separator on a root MUST NOT change behavior: `s3://b/p` and `s3://b/p/` are
the same root, as are `/data` and `/data/`.
*`[CLI]` `subcommands.py` → `CommandParameters._normalize_s3_trailing_slash`; `fileformat.py` → `FileFormat.s3_format` / `local_format` (`dir_op` branch appends the separator).*

**FR-Root-5** An S3 root MUST accept any bucket identifier the S3 API accepts, including access point
and outpost ARNs.
*`[CLI]` `utils.py` → `find_bucket_key` (`_S3_ACCESSPOINT_TO_BUCKET_KEY_REGEX`, `_S3_OUTPOST_TO_BUCKET_KEY_REGEX`). `[DOC]` sync [Example 8](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples) (access point).*

**FR-Root-6** `Copy` is the only direction with two S3 endpoints: an `Upload` reads its source from local
disk and a `Download` writes its destination to local disk, and neither of those sides issues any S3
request. Its two roots MAY sit in different regions and different accounts, each with its own credentials
and endpoint.

So every request an S3-to-S3 run makes has two endpoints it could go to, and it MUST go to the one that owns
the object it acts on: requests that read the source's state go to the source endpoint, requests that touch
the destination go to the destination endpoint.

The case that catches people out is the copy request itself. `CopyObject` and `UploadPartCopy` read the
source, but they are issued **against the destination bucket** and name the source in a `CopySource`
parameter. So the destination credentials must be able to read the source object. Supplying separate source
credentials does not lift that requirement: those credentials are used for listing and for reading source
properties, never for the copy.
*`[DOC]` `--source-region`, [Example 7](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples). `[CLI]` `subcommands.py` → `S3TransferCommand._get_source_and_transfer_clients` builds the pair; `CommandArchitecture.run` gives the forward (source) generator `self._source_client` and the reverse (destination) generator `self._client`. The copy going to the destination follows from `_get_transfer_manager(botocore_transfer_client=transfer_client)` — the transfer manager that issues the copy holds only the destination client. Source property reads follow from `subscribers.py`, where the tag and metadata subscribers read through `fileinfo.source_client` (`self._source_client.get_object_tagging`).*

**FR-Root-7** Sync MUST reject a configuration it cannot work correctly with, before doing any work, and
say why in terms the caller can act on.

A directory bucket is the example that matters: it does not return keys in alphabetical order, and the
comparison depends on both sides arriving in the same order. Sync cannot produce a correct answer there,
so it MUST refuse.
*`[CLI]` `subcommands.py` → `CommandParameters._validate_not_s3_express_bucket_for_sync` ("Cannot use sync command with a directory bucket" — directory buckets do not list lexicographically, which the comparison depends on), plus `_validate_path_args`, `_validate_sse_c_args`. `[ISSUE]` [#8470](https://github.com/aws/aws-cli/issues/8470) — before that validation existed, syncing to a directory bucket silently produced wrong results: "some files that do exist in the source are not recognized".*

**FR-Root-8** A relative key can resolve to a location outside the destination root. Sync MUST account for
that, and the design MUST NOT preclude whatever behavior is settled on for it.
*`[NEW]` — the behavior is an open cross-SDK decision; recorded here so that settling it needs no rework of the design.*

## 3. Enumeration (`FR-Enum-*`)

Producing the list of entries under each root: what counts as an entry, what order they arrive in, and
what happens to the ones that cannot be read.

**FR-Enum-1** A run MUST list both roots all the way through. Only filters (§5) reduce what gets listed.
*`[CLI]` `subcommands.py` → `CommandArchitecture.run`: `'file_generator': [file_generator, rev_generator]`, where `rev_files = FileFormat().format(dest, src, ...)`.*

**FR-Enum-2** A zero-byte S3 object whose key ends in `/` — the marker the console writes so an empty
folder shows up in the browser — MUST be invisible to sync on both sides: never transferred, never
deleted, never treated as a directory.
*`[CLI]` `filegenerator.py` → `FileGenerator.list_objects` yields such keys only when `operation_name == 'delete'`, and sync's reverse generator uses `''`, so markers are filtered on both sides.*

**FR-Enum-3** Walking the local tree MUST skip a single *entry* rather than fail the whole run when that
entry cannot be transferred, cannot be read, or has no key it could take, and the three cases MUST be
told apart:

- **Nothing to transfer** — a device (`/dev/null`), a FIFO, or a socket. No setting makes these
  transferable, and reading one may never finish. A warning under either policy. A symlink sync was told
  not to follow belongs here too (FR-Enum-4).
- **Should have been readable and was not** — missing, deleted mid-run, unreadable, or a symlink pointing
  at nothing: a failure, following the global policy (FR-Fail-9).
- **No key it could take** — a local name that is not valid UTF-8. An S3 object key is Unicode encoded as
  UTF-8, so no key corresponds to such a name. It MUST be reported as `skipped-with-warning`, naming the
  file with its invalid bytes escaped. It MUST NOT be converted lossily to make a key: two names
  differing only in invalid bytes would collapse onto one, and sync would then treat two files as one.

The first two kinds MUST reach the comparison. If sync left them out, anything reading that list would
conclude the name is unused — and delete whatever sits at the matching name on the other side
(FR-Fail-7). A FIFO is never transferred, but the fact that *something occupies that name* is what has to
stop the destination object from being deleted.

The third kind cannot reach it, because occupying a name means holding a key and this entry has none. It
therefore takes no part in the comparison, and sync MUST NOT hold anything back on its behalf. An object
written under a key derived lossily from such a name has a source side that is genuinely absent — no local
file carries that key — so FR-Fail-7's distinction never arises and delete mode (FR-Exec-2) removes it as
it would any other object with no counterpart.

This library writes such objects today: `upload_objects` derives its key with a lossy conversion, so the
two operations run against the same pair are in conflict. Uploading a directory writes an object at the
lossy key, syncing the same directory with delete mode removes it, and sync never replaces it because
enumeration will not produce a key for the file. The local file is never replicated by sync, and
alternating the two operations writes and removes the object indefinitely. That cost is accepted here, and
this requirement is what makes it visible; changing the upload path is out of scope for sync.

A hold was designed before it was rejected. Sync cannot recover which file an object came from, but it can
bound where that file must be: keys under the same parent whose remaining bytes match the valid part of
the name, with a replacement character wherever the invalid bytes were. That set contains the object and
usually little else. It was rejected on cost: the hold protects an object that may correspond to any of
several files or to none, so what it buys is the survival of an arbitrary one, and paying for that means
every caller carrying the machinery to bound and release the set. Paying that to protect an object that
may correspond to any of several files, or to none, costs more than the delete.

This is about one entry at a time. Failing to read an entire directory is a different problem
(FR-Enum-12).
*`[ISSUE]` [#487](https://github.com/aws/aws-cli/issues/487) — "S3 sync will exit when a broken symlink are present" (sic) — and its mirror [#425](https://github.com/aws/aws-cli/issues/425), where a filesystem exception makes the CLI "exit silently, and with a non-error (0) exit status", stopping "prematurely … before all files had been sync'ed up". The requirement is skip-and-warn: not skip-and-stop, and not fail. `[CLI]` the two categories are `filegenerator.py` → `is_special_file` versus `is_readable`; a name the filesystem encoding cannot decode is skipped with a warning naming its raw bytes (`should_ignore_file_with_decoding_warnings` → `FileDecodingError`). That check is locale-dependent, which a UTF-8 validity test is not. `[TM]` directory upload disagrees across implementations, so an object at a lossily-derived key is something sync will meet: this crate's `upload_objects` and the Java v2 transfer manager both build the key with a lossy conversion, the Go transfer manager passes the raw filename bytes through because a Go string need not be valid UTF-8, and boto3 and the JavaScript SDK have no directory upload at all.*

**FR-Enum-4** Following symlinks MUST be a setting, and MUST default to **not** following them. With
following turned on, sync transfers what the link points at, filed under the link's own name rather than
the target's, on every platform.

So a link `current -> releases/v3/` transfers the files inside `releases/v3/` as `current/...`.
*`[NEW]` — **reverses** what the CLI documents (`--follow-symlinks | --no-follow-symlinks`, "the default is to follow symlinks"). Defaulted this way because sync delegates to the transfer manager's directory operations, and defaulting differently from them would surprise anyone composing the two. `[ISSUE]` [#2550](https://github.com/aws/aws-cli/issues/2550) — on Windows, following symlinks uploads 0-byte objects instead of target contents.*

**FR-Enum-5** When a local file's modification time falls outside the range this platform can represent,
sync MUST warn and treat the file as though it were last modified at the UNIX epoch — midnight, 1 January
1970.

The case is a portability one: a timestamp written by a machine with a wider range than the one reading it.
`aws s3 sync` hits it between 64-bit and 32-bit systems.

A modification time before 1970 belongs here only where the platform cannot express it. Where it can, the
time MUST be reported as it stands, as a negative offset from the epoch. Substituting there would leave the
pair failing the time test on every run, so a restored backup would transfer its whole tree every time.

- The substituted time MUST NOT let the file be skipped: whatever the time test is, it MUST come out false
  so the file gets transferred. The epoch is a stand-in for reporting, not evidence that the two sides
  match. On `Upload` it is older than every object in S3, so the FR-Cmp-1 test reads it as "the destination
  is newer" and would never upload a same-size file whose timestamp could not be read.
*`[CLI]` `filegenerator.py` → `_validate_update_time` ("File has an invalid timestamp. Passing epoch time as timestamp.") with `utils.py` → `get_file_stat` returning `None` on `ValueError/OSError/OverflowError`, and `utils.EPOCH_TIME`. The no-skip clause is a deliberate deviation: `aws s3 sync` substitutes the epoch and then compares it like any other timestamp, so on `Upload` a same-size file whose timestamp cannot be read is skipped on every run while the run reports success-with-warning. **Read from the code path, not from an executed test.***

**FR-Enum-6** Relative keys MUST be compared byte for byte. Sync MUST NOT treat two different keys as the
same one by ignoring case (`README` versus `readme`), by normalizing Unicode (the two ways of encoding
`é`), or by decoding percent-escapes (`%20` versus a space).
*`[CLI]` `comparator.py` → `compare_comp_key` compares raw strings; the only lowercasing anywhere is `syncstrategy/caseconflict.py` → `CaseConflictSync`, and it is used solely for conflict detection, never for the join.*

**FR-Enum-7** Transfers MUST be able to start before either side has been listed all the way through.
*`[CLI]` `comparator.py` → `Comparator.call` pulls one item at a time per side via `advance_iterator`; `s3handler.py` → `S3TransferHandler.call` submits as items arrive.*

**FR-Enum-8** Files may be added or deleted while a run is in progress, and sync MUST cope: it MUST NOT
hang, and it MUST NOT produce a wrong plan. A file that disappears between being listed and being read
MUST be reported as skipped-with-warning — it was there a moment ago, and nothing is broken.
*`[CLI]` `filegenerator.py` → `triggers_warning` emits "File does not exist." and skips; `subscribers.py` → `ProvideSizeSubscriber` pins the size captured at enumeration time, so a file that grows mid-run is uploaded at its enumerated length.*

**FR-Enum-9** When following symlinks produces a loop — a link pointing back at a directory above it —
the walk MUST stop instead of descending forever, and the entries it stopped on MUST be reported as
skipped-with-warning.
*`[ISSUE]` [#3961](https://github.com/aws/aws-cli/issues/3961) — "`awscli s3 sync` gets stuck in self-recursive directory loops", with a `subdir/link -> ../subdir/` repro. [#3709](https://github.com/aws/aws-cli/issues/3709) (2 reactions) — a circular link produced `.../symlink/directory3/directory4/symlink/...` repeating, and the run "stopped only when it reached" the path limit. Neither hangs forever, but both burn requests and emit a flood of bogus paths.*

**FR-Enum-10** Sync MUST offer an opt-in that turns zero-byte marker keys into real local directories on
download, so an S3 "folder" containing no objects appears locally. Default behavior remains FR-Enum-2.

For a bucket holding `photos/2018/f.txt` (6 bytes) and `photos/2019/` (0 bytes, the marker the console
writes for an empty folder), a default download produces `photos/2018/f.txt` and nothing else —
`photos/2018/` exists only because a file had to live in it, and `photos/2019/` does not exist at all.
With the opt-in, `photos/2019/` is created empty.
*`[ISSUE]` [#912](https://github.com/aws/aws-cli/issues/912) (53 reactions) — "aws s3 sync does not synchronize s3 folder structure locally", with a repro showing empty directories never appearing.*

**FR-Enum-11** The listing page size MUST be configurable, bounded by the service maximum, and MUST
default to that maximum. Lowering it MUST remain possible at runtime, because oversized pages can time out
on some prefixes, and it costs proportionally more list requests.
*`[DOC]` `--page-size`: "The number of results to return in each response to a list operation. The default value is 1000 (the maximum allowed). Using a lower value may help if an operation times out." `[CLI]` `utils.py` → `BucketLister.list_objects` passes it through as `PaginationConfig.PageSize`.*

**FR-Enum-12** A directory that cannot be read MUST follow the global failure policy, and MUST be
reported as a *range* of keys never looked at — not as one skipped entry.

The distinction exists because the two cost different amounts of knowledge. A skipped file leaves one key
unknown; an unreadable directory leaves every key beneath it unknown, and there may be thousands.

For a local tree `good/a.txt`, `locked/b.txt`, `zzz.txt` where `locked/` cannot be read, and a
destination already holding all three, `aws s3 sync` warns `Skipping file .../locked. File/Directory is
not readable.` and then plans `delete: s3://.../locked/b.txt`. It could not see `locked/b.txt`, so it
concluded the object was orphaned. The run may continue, but every key under `locked/` must be treated as
unknown.

The exposure differs by direction (FR-Fail-7):

- Local tree as **source** (`Upload`) — the source looks emptier than it is, so destination counterparts
  appear orphaned. Continuing is legitimate; deleting them is not.
- Local tree as **destination** (`Download`) — the failure hides local files, so delete mode can only
  under-delete. The exposure moves to transfers: a hidden local file looks absent, and writing over it
  skips the comparison.
*`[CLI]` `filegenerator.py` → `list_files` calls `should_ignore_file` on the directory before `listdir`, so `aws s3 sync` warns and skips rather than raising, continues, and exits 2. Verified by execution: with `locked/` at mode `000` and the destination mirroring the full tree, `aws s3 sync --delete --dryrun` warned about the directory and then planned to delete the object beneath it.*

**FR-Enum-13** Listing MUST send an empty delimiter, so S3 returns every key in one flat, alphabetical
sequence.

Passing `/` instead makes S3 return one folder level at a time, which breaks the single alphabetical order
the comparison depends on and multiplies the number of requests.
*`[CLI]` `utils.py` → `BucketLister.list_objects` sends `Bucket`, `Prefix` and `PaginationConfig` only, with no `Delimiter`. The one `Delimiter: '/'` in the package is `subcommands.py` → `ListCommand._list_all_objects`, which serves `aws s3 ls`.*

## 4. Comparison (`FR-Cmp-*`)

Deciding, for each relative key, whether to transfer it, delete it, or leave it alone.

For a relative key present on both sides, let `delta = dest.last_modified − src.last_modified`.

**FR-Cmp-1 (default mode)** Transfer when the key is on the source side only, or when it is on both sides
and either the sizes differ or the time test below fails. The time test passes — meaning no transfer —
when:
- `Upload` / `Copy`: `delta >= 0`
- `Download`: `delta <= 0`

Spelled out for a key on both sides at the same size, both directions transfer **when the local file is
newer than the object**. For uploading, that is what anyone would want. For downloading it is backwards:
a newer object is exactly what should come down, and that is the case this test skips, while a local file
someone just edited gets overwritten by an older object.

Sync keeps this because `aws s3 sync` does it and parity is the baseline. FR-Cmp-3 and FR-Cmp-10 are the
two ways out of it.
*`[CLI]` `syncstrategy/base.py` → `SizeAndLastModifiedSync.determine_should_sync` = `(not same_size) or (not same_last_modified_time)`, with `BaseSync.compare_time` computing `delta = dest_time - src_time` and skipping when `delta >= 0` for upload/copy, `<= 0` for download. The documentation agrees for the option description but **[Example 3](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples) contradicts it**, describing FR-Cmp-10's behavior instead; only the option description matches the code.*

**FR-Cmp-2 (size-only mode)** Size is the only thing compared, in every direction. Timestamps are
ignored.
*`[CLI]` `syncstrategy/sizeonly.py` → `SizeOnlySync.determine_should_sync = not same_size`. `[DOC]` `--size-only`.*

**FR-Cmp-3 (exact-timestamps mode)** For `Download`, a same-size key is skipped only when the two
timestamps are exactly equal — so a newer object does come down, which is the FR-Cmp-1 problem this mode
exists to solve. `Upload` and `Copy` are unchanged from FR-Cmp-1.
*`[CLI]` `syncstrategy/exacttimestamps.py` → `ExactTimestampsSync.compare_time` (returns `total_seconds(delta) == 0` for `download`, else defers to the base class). `[DOC]` `--exact-timestamps`.*

**FR-Cmp-4 (no-overwrite mode)** A key that already exists on the destination side is never transferred,
whatever its size or timestamp. Only keys missing from the destination are.
*`[CLI]` `syncstrategy/nooverwrite.py` → `NoOverwriteSync.determine_should_sync` returns `False` unconditionally, registered for the `file_at_src_and_dest` slot in `syncstrategy/register.py`; `subcommands.py` → `_get_s3_handler_params` strips `no_overwrite` for sync so no `IfNoneMatch` precondition is sent. `[DOC]` `--no-overwrite`.*

**FR-Cmp-5** Comparison MUST NOT read entry contents and MUST NOT require a per-entry request: size and
last-modified MUST come from listing. The only exceptions are modes whose comparison needs more than a
listing carries — checksum comparison (FR-Cmp-9) and property comparison (FR-Cmp-11) — and those MUST be
opt-in, so the guarantee holds unless the caller asks for something it cannot cover.
*`[CLI]` `filegenerator.py` → `list_objects` uses `utils.BucketLister.list_objects` (`ListObjectsV2` paginator) for recursive operations, and `_inject_extra_information` populates `size`/`last_update` from the list response. `HeadObject` is reached only via `_list_single_object`, which sync never takes (`dir_op` is always true, FR-Root-2).*

**FR-Cmp-6** A caller MUST be able to replace the comparison with their own. Sync hands it a relative key
plus whatever it found on each side — either side may be absent — and takes back a decision. Every built-in
mode MUST be writable through that same interface, so callers are not left with a weaker tool than sync
uses itself.

- **A comparison MUST be able to do work before it answers.** Comparing checksums (FR-Cmp-9) or properties
  (FR-Cmp-11) means reading bytes or making a request first, so the interface MUST accept "not yet, ask me
  again" instead of demanding an immediate decision — including in the first release, where every built-in
  mode does answer immediately. A comparison answering immediately produces answers in key order, because
  the merge hands it keys in that order. A deferred answer carries no such promise, and none is required:
  FR-Cmp-8 is about what a plan contains, not the sequence it is handed over in, and a consumer needing
  order sorts what it collects.
- The comparison MUST be given the **entries themselves**. A checksum or ETag check
  needs listing fields that only the entry carries. Whatever it decides MUST come back with the reason
  attached (FR-Obs-1).
*`[CLI]` `syncstrategy/base.py` → `BaseSync.determine_should_sync` / `register_strategy` / `use_sync_strategy` with the `choosing-s3-sync-strategy` event, consumed by `subcommands.py` → `CommandArchitecture.choose_sync_strategies`. The three slots are `file_at_src_and_dest`, `file_not_at_dest`, `file_not_at_src` (`VALID_SYNC_TYPES`).*

**FR-Cmp-7** Timestamps MUST be compared to the whole second, and each mode MUST state what it does with
the fraction of a second a local filesystem records and S3 does not. Once a file and an object agree, they
MUST keep agreeing: repeated runs over an unchanged pair MUST NOT flip between transferring and skipping.
*`[CLI]` `subscribers.py` → `ProvideLastModifiedTimeSubscriber` writes back `int(time.mktime(last_modified.timetuple()))` — whole seconds — via `utils.set_file_utime`; `syncstrategy/base.py` → `total_seconds` yields float seconds, so `ExactTimestampsSync` demands exact equality. `[DERIVED]` non-oscillation follows only because S3 `LastModified` is second-granular and the write-back matches it.*

**FR-Cmp-8** The same side states and the same options MUST always produce the same plan. Both sides
arrive in key order (FR-Enum-13); beyond that, the plan MUST NOT depend on how the two interleave, or on
the order in which earlier transfers finished.

"The same plan" means the same decision and the same reason for every key. It does not mean the same
sequence: FR-Exec-12 lets transfers happen in any order, so nothing downstream depends on the order
decisions are handed over in, and the destination reaches the same state either way. A consumer that needs
an ordered plan — diffing two dry runs, say — MUST sort what it collects rather than assume the order it
arrived in.
*`[DERIVED]` from `comparator.py` → `Comparator.call`, whose merge join requires both sides "listed in the same order, least to greatest in collation order", and `filegenerator.py` → `list_files` + `normalize_sort`, which emulate S3 byte order locally by suffixing directory names with the path separator before sorting.*

**FR-Cmp-9 (checksum mode)** Sync MUST offer an opt-in mode that compares the checksums both sides have
stored and transfers when they disagree. What it does when one side has no usable checksum MUST be decided
and written down — either compute one, or fall back to FR-Cmp-1 — and the result MUST show which happened.

Four facts about S3 constrain this mode, and MUST be accounted for:

- A listing tells you which checksum *algorithm* an object uses but not the checksum *itself*, so getting
  values costs one request per object.
- An object uploaded in parts has a checksum built from its parts' checksums, which matches another object
  only if the algorithm **and** the part boundaries match. Same bytes, different part size, different
  answer.
- An ETag is only a content hash for objects uploaded in a single part. For a multipart object it cannot be
  compared against a hash of the bytes.
- A local file has no stored checksum at all, so comparing local against S3 means reading every byte on
  disk, every run, unless the results are cached somewhere.

The requests and disk reads this mode costs MUST be documented, and MUST NOT be presented as free.
*`[ISSUE]` [#9074](https://github.com/aws/aws-cli/issues/9074) (35 reactions) — "sync changed files to S3 based on checksum, if present", noting that [#6750](https://github.com/aws/aws-cli/issues/6750) (87 reactions) "only goes half the way: it uploads the checksum as metadata, but doesn't take it into account when computing the sync candidates". Duplicate reports with concrete data loss: [#8377](https://github.com/aws/aws-cli/issues/8377) (same filename, same 1234-byte size, different MD5 — silently skipped) and [#7011](https://github.com/aws/aws-cli/issues/7011) (a one-character typo fix in a CSV, same size, never uploaded).*

**FR-Cmp-10 (transfer whenever the two sides differ)** For `Download`, sync MUST offer a mode that
transfers whenever the sizes differ or the timestamps differ, instead of only when the object is newer
than the file.

This can work for downloads only because FR-Exec-5 puts the object's last-modified time onto the file it
writes. Once a file is up to date, both sides hold the same timestamp, so "they match" is a state that can
actually be reached.

It cannot work for uploads or copies. S3 stamps its own last-modified time on an object when it stores it,
and never carries the source's time across, so the two sides never match. A mode that transfers on any
difference would re-transfer every file on every run, forever.

That leaves a real gap in those two directions: a file edited without changing size, whose timestamp moved
backwards — restoring an old copy from a backup does exactly this. Default mode skips it. Catching it needs
proof that the two sides are identical, and a timestamp cannot supply that proof; a checksum (FR-Cmp-9)
could, or the source's own time stored on the object as metadata. Neither exists yet, which makes this a
known gap.
*`[ISSUE]` [#3273](https://github.com/aws/aws-cli/issues/3273) (80 reactions) — several hundred thousand files, "Both source and destination timestamps are also different but the sync never happens. S3 has the more recent file"; the highest-reaction sync issue found. [#1074](https://github.com/aws/aws-cli/issues/1074) names the cause exactly — "It seems like its just because the `compare_time()` function is wrong" — and was closed by adding `--exact-timestamps` rather than changing the default.*

**FR-Cmp-11 (property comparison)** The comparison MAY also look at properties the caller picks — ACL,
user metadata, cache-control, content-type, storage class — so that an entry whose contents match but whose
properties have drifted apart is still detected and fixed. It MUST be opt-in, and with nobody opting in it
MUST NOT add a single per-entry request (FR-Cmp-5).
*`[ISSUE]` [#3215](https://github.com/aws/aws-cli/issues/3215) (26 reactions) — re-running sync with a different `--acl` leaves the old ACL in place. [#4881](https://github.com/aws/aws-cli/issues/4881) — changing `--cache-control` triggers no update, with the reporter pointing at `syncstrategy/base.py` as the reason. [#901](https://github.com/aws/aws-cli/issues/901) (66 reactions) — permissions not preserved bucket-to-bucket. These are all consequences of FR-Exec-8, which the docs state but users repeatedly find surprising.*

## 5. Filtering (`FR-Filter-*`)

Narrowing which entries take part, on both sides.

**FR-Filter-1** Sync MUST support an ordered list of include and exclude patterns. Every entry starts out
included, the rules are applied in the order given, and the last rule that matches decides.

So excluding `*` and then including `*.jpg` syncs only the JPEGs; writing those two the other way round
syncs nothing, because the exclude now has the last word.
*`[CLI]` `filters.py` → `Filter.call`: "All files begin with the flag set to true. Rules listed at the end will overwrite flags thrown by rules listed before it."*

**FR-Filter-2** A pattern MUST be matched against the entry's whole path counting from the root, and `*`
MUST match across `/` rather than stopping at it.

With a root of `/data`, excluding `*.log` throws out `/data/logs/app.log`: the `*` is allowed to cover
`logs/`, slash included. Anyone expecting `*` to stop at a directory boundary, as a shell would, gets more
excluded than they meant.
*`[CLI]` `filters.py` → `_full_path_patterns` (`os.path.join(rootdir, pattern)`) and `_match_pattern` (`fnmatch.fnmatch(file_path, path_pattern)`, which does not treat `/` specially); roots from `_get_s3_root` (`bucket/prefix`) and `_get_local_root` (absolute path). `[DOC]` "Use of Exclude and Include Filters"; Examples 5–6 (`--exclude "*another/*"` matching at depth).*

**FR-Filter-3** The rule set MUST be applied to both sides: an excluded destination entry MUST NOT be
deleted, and an excluded source entry MUST NOT be transferred.
*`[CLI]` `subcommands.py` → `command_dict['filters'] = [create_filter(...), create_filter(...)]` (one per generator); `filters.py` → `Filter` builds both `patterns` (source root) and `dst_patterns` (destination root) and evaluates both per entry. `[DOC]` `--delete`: "files excluded by filters are excluded from deletion."*

**FR-Filter-4** Filtering MUST work the same regardless of what order entries arrive in, and MUST NOT
require holding either side's full listing in memory.
*`[CLI]` `filters.py` → `Filter.call` is a per-item generator over the listed entries.*

**FR-Filter-5** An entry the filters exclude MUST NOT produce a warning and MUST NOT change the run's
outcome — not even when it is unreadable, or a device or pipe, or has just been deleted. Sync asked to
ignore it, so problems with it are not problems.

This means filtering MUST happen before skip-warnings are produced.

The unit is an entry, which is what FR-Filter-2 matches a pattern against. A directory is not one, and
a bare pattern is an exact match rather than a prefix: `img` excludes the key `img` and leaves
`img/a.txt` alone, where `img/*` is how a subtree is excluded. So excluding a name MUST NOT stop sync
from enumerating what that name contains.

Which means a name whose type sync cannot read is not yet known to be an entry, and MUST be reported
whatever the rules say. It may be a directory holding keys no pattern excluded, and silence there
would take those keys out of the run with nothing said — leaving a delete free to remove their
counterparts. A warning about a name the caller excluded is the price of that, and it is the cheaper
of the two.
*`[ISSUE]` [#1117](https://github.com/aws/aws-cli/issues/1117) — a FIFO excluded by `--exclude '*'` still warns and forces exit code 2. [#3671](https://github.com/aws/aws-cli/issues/3671) — a socket file warned about despite being excluded by an explicit pattern. [#2473](https://github.com/aws/aws-cli/issues/2473) — "file does not exist" for a path inside an excluded tree. [#7072](https://github.com/aws/aws-cli/issues/7072) — "symlinks are evaluated before exclude\include": a symlink to a missing file warns even though its name does not match `--include "package*"`. This inverts the CLI's stage order, where `FileGenerator` emits warnings before `Filter` ever runs.*

**FR-Filter-6** It MUST be possible to say whether a pattern is measured from the root or matched anywhere
in the tree, and every pattern's meaning MUST be documented.

In `aws s3 sync`, `--exclude "bin/*"`, `"/bin/*"` and `"./bin/*"` all fail to exclude a top-level `bin`
directory, and only `"*/bin/*"` works.
*`[ISSUE]` [#1588](https://github.com/aws/aws-cli/issues/1588) (13 reactions) — `--exclude "bin/*"`, `"/bin/*"`, and `"./bin/*"` all fail to exclude `/bin` regardless of cwd; only `"*/bin/*"` works.*

**FR-Filter-7** Sync MUST offer an opt-in that deletes destination entries the filters excluded — the
opposite of FR-Filter-3, and what rsync calls `--delete-excluded`.

The use for it: add an exclusion to a run that previously copied those entries, and the copies already on the
destination stay there forever, because FR-Filter-3 stops any sync from removing them.
*`[ISSUE]` [#4923](https://github.com/aws/aws-cli/issues/4923) (9 reactions) — the reporter's use case is syncing only a recent window while still pruning older objects at the destination.*

## 6. Execution (`FR-Exec-*`)

Carrying out the decisions: transfers, deletes, and the object properties that go with them.

**FR-Exec-1** Every entry sync decides to transfer MUST get exactly what a single-object transfer would
get: the same integrity checks, the same retries, the same splitting of large objects into parts. Sync MUST
NOT quietly offer less than the operation it is built on.
*`[CLI]` `s3handler.py` → `S3TransferHandler` dispatches to `UploadRequestSubmitter` / `DownloadRequestSubmitter` / `CopyRequestSubmitter`, the same submitters `cp` and `mv` use.*

**FR-Exec-2 (delete mode)** When turned on, a key the destination has and the source does not MUST be
removed from the destination — objects deleted from S3, files deleted from disk. Entries the filters
excluded are left alone (FR-Filter-3).
*`[CLI]` `syncstrategy/delete.py` → `DeleteSync` (registered for `file_not_at_src`, sets `dest_file.operation_name = 'delete'`); `s3handler.py` → `DeleteRequestSubmitter` and `LocalDeleteRequestSubmitter`. `[DOC]` `--delete`, [Example 4](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples).*

**FR-Exec-3** Delete mode MUST NOT remove empty local directories; downloads MUST create missing local
directories as needed.
*`[CLI]` `s3handler.py` → `LocalDeleteRequestSubmitter` unlinks files only, with no directory pruning in the delete path; `subscribers.py` → `DirectoryCreatorSubscriber` creates parents before a download.*

**FR-Exec-4** Sync MUST offer an opt-in that removes local directories left empty by delete mode.
Default behavior remains FR-Exec-3.
*`[ISSUE]` [#2685](https://github.com/aws/aws-cli/issues/2685) (58 reactions) — "`s3 sync --delete` not deleting empty folders"; empty directories accumulate indefinitely on repeated syncs.*

**FR-Exec-5** After a download succeeds, the local file's modification time MUST be set to the object's
last-modified time, always.

Without this the file is stamped with the time it was written, which is later than the object it came from.
FR-Cmp-1 then reads that as "the local file is newer" and downloads it again — every run, forever. So this
is a prerequisite for FR-Exec-7.
*`[CLI]` `subscribers.py` → `ProvideLastModifiedTimeSubscriber` → `utils.set_file_utime`; attached in `s3handler.py` → `DownloadRequestSubmitter`. `[DOC]` [Example 3](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples): "the last modified time of the local file is changed to the last modified time of the S3 object."*

**FR-Exec-6** Deletes MAY be sent in batches, but every key's outcome MUST be reported on its own, and a
batch where some keys failed MUST NOT be reported as if all of them did. `DeleteObjects` returns a result
per key precisely because partial success is normal.
*`[NEW]` — the CLI issues one `DeleteObject` per key (`s3handler.py` → `DeleteRequestSubmitter`); `DeleteObjects` batching is our optimization, so the reporting obligation is ours to define.*

**FR-Exec-7 (running it twice changes nothing)** Running the same sync again straight after a successful
one MUST decide to do nothing at all: no transfers, no deletes. This MUST hold in every direction and in
every comparison mode.
*`[DERIVED]` from FR-Cmp-1 plus FR-Exec-5 (the write-back is what makes the download direction stable). `[DOC]` Description: "Recursively copies new and updated files from the source directory to the destination."*

**FR-Exec-8** Properties the caller supplies — user metadata, ACL and grants, encryption settings, storage
class, content type, encoding, language, disposition, cache-control, expiry, website redirect — MUST be
applied only to entries actually transferred. Entries that were skipped keep whatever they already had.

So re-running a sync with a new `--cache-control` changes nothing for files that did not otherwise need
transferring. FR-Cmp-11 is the way to opt out of that.
*`[DOC]` `--metadata`: "In a sync, this means that files which haven't changed won't receive the new metadata." `[CLI]` `utils.py` → `RequestParamsMapper.map_*` is invoked only inside the request submitters, i.e. only for submitted transfers.*

**FR-Exec-9** An S3 object is more than its bytes. It also carries a content type, a cache-control header,
user metadata, tags, and more. When sync copies an object, the caller MUST be able to choose how much of
that comes across, from four levels:

| Level | What comes across |
|---|---|
| none | the bytes only |
| standard properties | `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type`, `Expires`, and user metadata |
| standard properties plus tags (**default**) | the above, plus the object's tags |
| everything preservable | the above, plus object annotations |

A small object is copied with a single `CopyObject` request, and S3 brings the properties and tags across by
itself. No extra requests at any level.

A large object is copied in pieces, one request per piece, and that sequence has no way to carry properties.
Sync has to fetch them from the source and put them on the destination itself: a `HeadObject` for the
standard properties, `GetObjectTagging` and `PutObjectTagging` for the tags, and three more calls for
annotations.

So the very same object costs nothing extra just below the size at which sync switches to copying in pieces,
and three extra requests just above it, at the default level. Sync MUST document that cost for each level,
and one level MUST guarantee no extra requests at all, so that a caller who is counting requests has a safe
choice.
*`[CLI]` the property list is `subscribers.py` → `SetMetadataDirectivePropsSubscriber._ALL_METADATA_DIRECTIVE_PROPERTIES`; `default` adds tags via `SetTagsSubscriber` and `all` adds annotations via `SetAnnotationsSubscriber`. Both gate on `_is_multipart_copy(future)` — `size >= multipart_threshold` — so `aws s3 sync` has the same size-dependent split, and `--copy-props` (`none | metadata-directive | default | all`) is the same four levels. Storage class, encryption, object lock and website-redirect are in no level; ACLs are FR-Exec-17.*

**FR-Exec-10** For `Upload`, content type MUST be guessed from the file name by default, with an
opt-out and an explicit override that wins.
*`[CLI]` `s3handler.py` → `_should_inject_content_type` (`guess_mime_type and not content_type`); `subscribers.py` → `ProvideUploadContentTypeSubscriber`; `utils.py` → `guess_content_type`. `[DOC]` `--no-guess-mime-type`, `--content-type`.*

**FR-Exec-11** Requester-pays MUST be supported on every kind of request sync makes, listing included —
otherwise a run against a requester-pays bucket fails at the first listing, before any transfer is even
considered.
*`[CLI]` `utils.py` → `RequestParamsMapper._set_request_payer_param`, `map_list_objects_v2_params`; `subcommands.py` → `_map_request_payer_params` applies it to both `HeadObject` and `ListObjectsV2` request parameters for both generators. `[DOC]` `--request-payer`.*

**FR-Exec-12** Transfers MAY happen in any order, but within a single run a delete and a transfer for the
same relative key MUST NOT overlap. Otherwise the outcome depends on which finishes last, and the same run
could leave the key either present or absent.
*`[DERIVED]` from `comparator.py` → `Comparator.call`: each `compare_key` resolves to exactly one branch (`equal`, `less_than`, `greater_than`), so a key can never be both transferred and deleted in the same run.*

**FR-Exec-13** A destination file MUST NOT be truncated or removed until its replacement has been
retrieved: a failed download MUST leave previous local content intact.
*`[CLI]` inherited from `s3transfer` → `s3transfer/download.py`: `self._temp_filename = self._osutil.get_temp_filename(fileobj)`, written and then renamed by `IORenameFileTask`, with `remove_file` as the failure cleanup.*

**FR-Exec-14** When an entry's contents match but a property being compared does not (FR-Cmp-11), sync
SHOULD fix just the property rather than sending the whole file again.
*`[ISSUE]` [#3215](https://github.com/aws/aws-cli/issues/3215), [#4881](https://github.com/aws/aws-cli/issues/4881) (as cited in FR-Cmp-11). An ACL or cache-control change on a 5 GB object would otherwise cost 5 GB of transfer.*

**FR-Exec-15** Properties MUST be settable per entry, by handing sync a function that takes the relative
key and returns the properties for it — not only as one fixed set applied to everything.

Without this, a run that needs `Cache-Control: max-age=31536000` on `assets/` and `no-cache` on
`index.html` has to become two runs with complementary filters. That costs two full listings of both sides,
and it makes delete mode hazardous: each run sees the other run's files as destination-only, so the filters
have to exclude them exactly, or one run deletes what the other just wrote.
*`[ISSUE]` [#2045](https://github.com/aws/aws-cli/issues/2045) (15 reactions) — the reporter's alternatives were to reimplement sync, store sidecar `.meta` objects, or upload object-by-object, all rejected as impractical.*

**FR-Exec-16** Content-type guessing MUST use a table this library owns, and MUST be overridable per entry.

`aws s3 sync` defers to the operating system's list — `/etc/mime.types` on Linux, the registry on Windows —
so the same command run from two machines can upload the same file with two different content types.
*`[ISSUE]` [#1249](https://github.com/aws/aws-cli/issues/1249) (25 reactions) — `.woff` is not guessed. The guessing goes through Python's `mimetypes` (`utils.guess_content_type`).*

**FR-Exec-17** For `Copy`, carrying the source's ACL and grants across MUST be an option the caller can
turn on, with the extra requests it costs written down.

A copy replaces the destination's permissions with `private`. So leaving this off narrows who can read the
object, and the result MUST say so.
*`[ISSUE]` [#901](https://github.com/aws/aws-cli/issues/901) (66 reactions) — "s3 sync does not preserve permissions across buckets". No CLI option satisfies it: `--copy-props` never covers ACLs and `--acl` sets one value rather than reproducing the source's. `[DOC]` "the ACL metadata is not preserved and is set to `private` by default".*

**FR-Exec-18** Encryption keys MUST be given separately for each job they do, rather than one key standing
for the whole run:

- a **destination-write key** — the SSE-C key or KMS key for the object being written;
- a **source-read key** — the SSE-C key needed to decrypt the object being read.

For a `Copy` between two SSE-C-encrypted locations these are two different keys and sync MUST accept
both. It MUST also be written down which requests carry which key, consistent with FR-Root-6.
*`[DOC]` `--sse-c-copy-source` / `--sse-c-copy-source-key` are "the algorithm to use when decrypting the source object … must be one that was used when the source object was created", distinct from `--sse-c` / `--sse-c-key` for the destination.*

**FR-Exec-19** The caller MUST be able to select the checksum algorithm used when writing objects, and to
require checksum validation when reading them. This is independent of FR-Cmp-9: FR-Cmp-9 uses a stored
checksum to decide *whether* to transfer, while this requirement governs the integrity of the transfer
itself.
*`[DOC]` `--checksum-algorithm` (`CRC64NVME | CRC32 | CRC32C | SHA1 | SHA256 | …`) and `--checksum-mode` ("To retrieve the checksum, this mode must be enabled. If the object has a checksum, it will be verified"). `[ISSUE]` [#6750](https://github.com/aws/aws-cli/issues/6750) (87 reactions) added these to the high-level commands.*

**FR-Exec-20** The requests sync makes on its own behalf — listing, deleting, and server-side copying —
MUST be retried on throttling and transient transport failure, not only the requests issued by the
per-entry transfers it delegates to.
*`[CLI]` `aws s3 sync` retries these through its SDK layer: `configprovider.py` defaults `retry_mode` to `standard` and `max_attempts` to 3, registered per client on `needs-retry.{service}`, which covers its listing and its deletes. It adds nothing sync-specific.*

**FR-Exec-21** A large server-side copy is assembled from many part-copies, and MUST NOT produce a
destination object built from more than one version of the source.

- Every part MUST copy the version that was there when the copy was planned, and that check MUST travel
  with every **attempt**, not just every part. A retried part is a fresh read of the source, so an
  unchecked retry can mix versions.
- If the source has changed, the copy MUST fail rather than produce a mixed or partial object, and it MUST
  keep failing — retrying cannot make an already-changed source match again.
- The failure MUST say *the source changed*, and MUST be distinguishable from a destination condition that
  was already met.
*`[CLI]` `copies.py` sets `CopySourceIfMatch` on every part from an ETag obtained beforehand, commented "Provide an etag to ensure a stored object is not modified during a multipart copy". The last bullet is a departure: `results.py` → `_on_failure` turns any error whose code is `PreconditionFailed` into a `SkipFileResult` reading "Skipping file ... as it already exists on ...", without checking which precondition failed, so a source that changed mid-copy is reported as a destination that was already up to date.*

**FR-Exec-22** A multipart transfer that fails — an upload or a server-side copy — MUST stop the
multipart upload, so the parts already stored are released. Retaining them MUST require an explicit
opt-in, and a retained multipart upload MUST be reported, because no object listing reveals one.
*`[DOC]` S3 user guide: "you must either complete or stop the multipart upload to stop incurring charges for storage of the uploaded parts", and "After you initiate a multipart upload, there is no expiry". `[DERIVED]` — reporting is required because the leak is invisible to every listing a caller would think to check.*

**FR-Exec-23** A run MUST be cancellable. On cancellation sync MUST stop starting new work, MUST NOT leave a
half-written destination entry looking like a finished one, and MUST report what did finish.
*`[NEW]` — the CLI's sync path has no cancellation surface: `s3handler.py` → `S3TransferHandler.call` submits inside `with self._transfer_manager:` and blocks, and no sync module contains cancel or shutdown handling. Process-level interrupt behavior sits outside the sync code and is not asserted here. FR-Exec-13 supports the "no partial entry" half.*

## 7. Skips, warnings, failures (`FR-Fail-*`)

What happens when an entry, a directory, or a listing cannot be handled, and what the caller is told.

**FR-Fail-1** An object in `GLACIER` or `DEEP_ARCHIVE` with no restored copy MUST be skipped with a
warning on `Download` and `Copy` — its bytes are not retrievable, so trying is a guaranteed failure. An
object whose restore is under way MUST be skipped and reported as such, separately: the bytes arrive when
it finishes, so a later run gets them, and a caller told only that it was archived would think asking
again is pointless. Two separate switches MUST exist: one to attempt the transfer anyway, one to stop
warning about it. `Upload` is unaffected, because writing over an object never reads what is already
there.

Those two classes are the only ones a listing can answer for, and the names are a poor guide. `GLACIER_IR`
reads in real time and never carries a restore status, so treating every Glacier-named class as archived
would skip every one of those objects on every run. `INTELLIGENT_TIERING` reports the same class whether
or not the object currently sits in an archive tier, so a listing cannot tell; sync finds out when a
transfer comes back `InvalidObjectState`.

Storage class comes back in a listing. Restore state only comes back when the listing asks for it, so sync
MUST request `RestoreStatus` on every listing it makes — which keeps the check free of per-entry requests
(FR-Cmp-5). It has to be every one: leave the parameter off and the field is simply missing, which looks
identical to an object that was never restored, so a comparison MUST treat a listing that did not ask as
saying nothing about whether an object is reachable. The parameter is unsupported for directory buckets,
and sync MUST NOT turn it on for listings made on behalf of other operations.
*`[CLI]` `s3handler.py` → `_warn_glacier` (checked in `DownloadRequestSubmitter` and `CopyRequestSubmitter` warning handlers); `fileinfo.py` → `is_glacier_compatible` / `_is_glacier_object` (`GLACIER`, `DEEP_ARCHIVE`). The restore half is a departure: `_is_restored` tests for `ongoing-request="false"` in `Restore`, a HeadObject header, against data that came from a listing, and the CLI never sends `OptionalObjectAttributes`. So the test always fails during a sync and a restored object is skipped with a warning even though its bytes are available. `[DOC]` `--force-glacier-transfer`, `--ignore-glacier-warnings` (including its effect on the exit code); `ListObjectsV2` carries `RestoreStatus` (`IsRestoreInProgress`, `RestoreExpiryDate`) when the request sends `x-amz-optional-object-attributes: RestoreStatus`.*

**FR-Fail-2** Sync MUST offer two failure policies, continue and abort, and MUST default to continue.

There is no default to inherit here: `aws s3 sync` has no aborting mode at all. Continue is the default
because a sync converges — it does part of the work and the next run picks up the rest — so aborting on the
first failed entry throws away progress that the next run has to repeat.
*`[CLI]` `subcommands.py` → `CommandArchitecture.run` computes a return code after the whole run and never aborts; neither rsync, rclone nor the Java transfer manager offers an aborting mode. This default disagrees with `upload_objects` and `download_objects`, which abort (`FailedTransferPolicy` derives `Default` with `#[default] Abort`), so a caller using sync alongside them has two defaults to remember.*

**FR-Fail-3** What each policy obliges:

- **Continue** — the run carries on past the failed entry. Every failure MUST be recorded and readable in
  the result, and the run MUST report completed-with-failures (FR-Fail-5). A failure that only reached a log
  has been lost.
- **Abort** — sync MUST stop starting new work promptly, and MUST report which entries completed before it
  stopped.
*`[NEW]` — the two policies are the same pair the transfer manager's other directory operations expose, so a caller can compose them without learning a second model.*

**FR-Fail-4** The result MUST report how many entries and bytes were transferred, deleted, skipped as
unchanged, skipped with a warning, and skipped as unknown (FR-Fail-7). None of it MUST grow with the number
of entries.

Failures MUST be grouped by cause and common prefix — "247,000 AccessDenied under `logs/2019/`" — which
bounds them by the number of distinct causes.

Per-entry detail, including the outcome of each delete, reaches the caller as events while the run is going
(FR-Obs-1).

`skipped-unknown` MUST be distinguishable from `skipped-unchanged`. They look similar and mean opposite
things: the first says sync could not see what was there and held a delete back, so the destination may
still hold something that should be gone; the second says everything matched and there was nothing to
do.
*`[CLI]` `results.py` → `ResultRecorder` (`files_transferred`, `files_failed`, `files_warned`, `bytes_transferred`, `bytes_failed_to_transfer`, `expected_files_transferred`, `expected_bytes_transferred`) and `CommandResultRecorder`.*

**FR-Fail-5** The result MUST distinguish three overall outcomes: at least one failure; no failures but
at least one warning; clean.
*`[CLI]` `subcommands.py` → `CommandArchitecture.run`: `rc = 1` if `num_tasks_failed > 0`, `rc = 2` if `num_tasks_warned > 0`, else `0`.*

**FR-Fail-6** Failing to list a root at all MUST fail the run, and MUST be reported differently from an
individual entry failing. One entry failing leaves a usable run; a root that cannot be listed means sync
never knew what was there.

A `Download` destination that does not exist yet is not this case, and MUST NOT fail the run. Nothing was
listed there because nothing is there: every key the source holds is missing at the destination, which is
a complete answer rather than an absent one, and the directory is created as entries are written
(FR-Exec-3). A destination root that exists and cannot be read, or exists and is not a directory, still
ends the run — neither can be written into, and neither says the destination is empty.
*`[CLI]` `filegenerator.py` → `list_objects` / `_list_single_object` let `ClientError` propagate out of the generator, failing the command rather than producing an empty side.*

**FR-Fail-7** A key whose side state is unknown on either side MUST get no action, and MUST be reported as
`skipped-unknown`. Two cases follow:

- **Unknown source, present destination** — MUST NOT delete. "Nothing is there" justifies deleting; "I
  could not look" does not.
- **Present source, unknown destination** — MUST NOT transfer. Treating an unknown destination as absent
  skips the comparison, which can overwrite a newer destination entry and defeats no-overwrite mode
  (FR-Cmp-4) outright.

A failure makes a range of keys unknown on the side that failed:

| Failure | Keys it makes unknown |
|---|---|
| an entry that could not be read (FR-Enum-3) | that one key |
| a directory that could not be read (FR-Enum-12) | every key under its prefix, one unbroken run in key order |
| a listing page that could not be fetched | every key from the last one observed to the end of that listing, since pagination cannot resume past a page that never arrived |
| a root that could not be listed (FR-Fail-6) | the whole key space, which is why that failure ends the run: no key has a known state on both sides, so nothing anywhere is licensed |

An entry that cannot be transferred at all — device, FIFO, socket — is **present**. Its delete is held back
because the source has something at that key, and it MUST be reported as `skipped-with-warning`. Only
`skipped-unknown` means the run could not see.
*`[NEW]` — a strengthening. In the CLI a skipped local file is never yielded (`filegenerator.py` → `triggers_warning`), so the comparator sees a destination-only key and deletes it: a local permissions glitch causes remote deletion on a run reporting success-with-warning. The delete has been observed for the directory case: with a local directory at mode `000` and the destination mirroring the full tree, `aws s3 sync --delete --dryrun` warned that the directory was unreadable and then planned to delete the object beneath it. The single-file case runs through the same not-yielded path and was read from the code, not executed. `Upload`-only exposure; no public issue found.*

**FR-Fail-8** For `Download`, two keys that differ only in case — `Report.pdf` and `report.pdf` — land on
the same file on a case-insensitive filesystem, which macOS and Windows use by default. The second download
silently overwrites the first, including when both are in flight at once. The caller MUST choose what
happens: ignore (default), warn, skip, or fail.
*`[DOC]` `--case-conflict` (`ignore | skip | warn | error`, default `ignore`), implemented in `[CLI]` `syncstrategy/caseconflict.py` → `CaseConflictSync` with a lowercased-key `submitted` set plus an existence check.*

**FR-Fail-9** There is one failure policy for the whole run, and it MUST govern every place a failure can
happen: an entry that could not be listed, a directory that could not be read, a listing page that could
not be fetched, an object that could not be transferred or copied, a key that could not be deleted.

One policy, so a caller reasons about failure once. Three things sit outside it:

- An entry that could never be transferred whatever the settings — device, FIFO, socket, a symlink sync
  was told not to follow — is a warning under either policy (FR-Enum-3).
- A local name with no key it could take is a warning under either policy too, for the same reason: no
  setting makes it transferable, and aborting a run over a name sync was never going to send would stop a
  transfer of everything else for nothing (FR-Enum-3).
- A failure that leaves nothing to carry on with — a source root that is unreadable or is not a directory
  (FR-Fail-6) — ends the run under either policy. Not because the policy is overridden, but because there
  is nothing left to continue doing.
*`[DERIVED]` — from FR-Fail-2 and FR-Fail-3: a policy only some failures consult is not a policy, and a per-site decision is how FR-Enum-12 and FR-Fail-7 come to disagree. `[DOC]` rsync draws the same line — a "non-regular file" warning changes neither exit status nor deletion, while an I/O error does both. The exclusion preserves FR-Enum-3, whose provenance is two reports of `aws s3 sync` failing an entire run over one such entry.*

## 8. Dry run (`FR-Dry-*`)

Producing the plan without acting on it.

**FR-Dry-1** A dry run MUST produce the whole plan — every transfer and every delete that would happen —
and MUST NOT issue a single request that changes anything. Listing, filtering and comparing all still
happen; only the acting is withheld.

"The whole plan" means every decision reaches the caller, not that the plan is held anywhere. Decisions MUST
be delivered as they are produced, and when a caller stops reading, sync MUST stop enumerating.

If listing fails part-way — a page that never arrives, a local directory it cannot read — the plan is then
missing decisions (FR-Fail-7), and the result MUST say so.
*`[CLI]` `s3handler.py` → `BaseTransferRequestSubmitter.submit`: `if not self._cli_params.get('dryrun') ... else self._submit_dryrun(fileinfo)`, emitting `results.DryRunResult`. `[DOC]` `--dryrun`.*

**FR-Dry-2** A dry run MUST go through the same comparison code as a real run, so the plan it prints is a
truthful prediction of what a real run would do, given the two sides do not change in between.
*`[CLI]` the dry-run branch sits downstream of the comparator in the same generator chain (`subcommands.py` → `CommandArchitecture.run`), so no separate decision logic exists.*

## 9. Observability (`FR-Obs-*`)

What the caller can see while a run is going, and what the result carries when it finishes.

**FR-Obs-1** For every entry, sync MUST hand the caller a value carrying the direction, the relative key,
the source, the destination, the action taken and the reason for it. Log output does not satisfy this: the
caller MUST be able to read these without parsing text, and MUST get them while the run is still going,
dry runs included.

They MUST carry enough that a caller can print the familiar `upload:` / `download:` / `copy:` / `delete:`
lines from them alone.

An entry with no key it could take (FR-Enum-3) is the one exception to carrying a key, since none exists
to carry. It MUST be identified by its name with the invalid bytes escaped, which is what the warning for
it already names, and everything else on the value is unchanged. Reporting it is what tells a caller a
name was seen and skipped; leaving it out would make the run look like it covered a tree it did not.

Delivery MUST be bounded, so a caller that reads slowly MAY miss events and the run MUST NOT slow down
waiting. The result stays complete either way (FR-Fail-4). A dry run is the exception, where the events are
the output and none may be missed (FR-Dry-1).

- The reason MUST distinguish at least these: not present at the destination, sizes differ, times differ,
  forced, excluded by a filter, in an archival storage class, a case conflict, untransferable (device, FIFO,
  socket), destination-only with delete mode off, `skipped-unknown` along with what caused it, and failed.
*`[ISSUE]` [#4190](https://github.com/aws/aws-cli/issues/4190) (30 reactions) — `aws s3 sync`'s progress output uses carriage returns, unusable in logs, while `--no-progress` suppresses per-object output entirely, leaving no way to log what was transferred. A per-entry event the caller receives solves both. The lines being reproduced come from `results.py` → `ResultPrinter`; the reasons behind them exist only as `LOG.debug` strings in `syncstrategy/*.py`, so exposing them as structured data is new.*

**FR-Obs-2** Sync MUST report overall progress in both entries and bytes, and MUST distinguish "the total
isn't known yet" — listing is still going — from "the total is zero".
*`[CLI]` `results.py` → `ResultPrinter._print_progress` with `ResultRecorder.expected_totals_are_final()` and `_STILL_CALCULATING_TOTALS` (the `~` prefix on expected totals). `[DOC]` `--progress-frequency`, `--no-progress`, `--progress-multiline`, `--quiet`, `--only-show-errors`.*

**FR-Obs-3** Warnings MUST be part of the result the caller gets back, not only lines in a log. A library
cannot assume anyone is reading its logs.
*`[CLI]` `utils.py` → `create_warning` / `WarningResult`, surfaced through `results.py` and reflected in the return code (FR-Fail-5).*

## 10. Non-functional requirements (`NFR-*`)

Behavioral obligations with performance consequences.

**NFR-Cost-1** A no-op sync of N entries per side MUST cost O(N / page size) list requests per side and
zero per-entry requests, unless the caller opted into one of the modes FR-Cmp-5 excepts.
*`[DERIVED]` from FR-Cmp-5 plus `[DOC]` `--page-size` (default and maximum 1000) and `utils.py` → `BucketLister.list_objects` pagination.*

**NFR-Lat-1** The time until the first transfer starts MUST NOT depend on how many entries there are in
total. A run over a million keys MUST begin transferring as promptly as a run over ten.
*`[CLI]` satisfied by the generator chain (FR-Enum-7).*

**NFR-Mem-1** Peak memory MUST be bounded by what a run holds at one moment: the entries of each directory
on the current descent path, one listing page per side, and the transfers in flight. The total number of
entries MUST NOT enter that bound. Measured over trees whose directories each hold a bounded number of
entries, peak usage at a thousand, a hundred thousand and a million entries per side MUST stay flat.

A single directory holding a million entries costs a million entries of memory. Producing key order means
sorting that directory's children, and sorting means holding them.
*`[NEW]`.*

**NFR-Tput-1** Transferring a given set of entries through sync MUST be as fast as transferring that same
set directly. Listing and comparing MUST NOT become a ceiling on how many transfers run at once.
*`[NEW]`.*

**NFR-Tput-2** Deciding one key MUST cost the same whether the run has skipped nothing or skipped a
million entries.
*`[DERIVED]` — a skip records a non-observation that later decisions must consult (FR-Fail-7). Scanning all of them measures 48k decisions/sec at ten thousand skips against 376M at one, which makes the delete decision the run's ceiling instead of the network, reachable from one unreadable subtree. Correctness tests cannot catch this: the degrading scan returns the same answers.*

