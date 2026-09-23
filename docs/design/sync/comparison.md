# Comparison

The second layer of sync: taking the two entry streams enumeration produces and working out, for
each name, whether to transfer it, delete it, or leave it alone. This describes what it covers,
the decisions behind it and what each would cost to change, and how the pieces fit.

Here, a *name* is the path a file sits at relative to the folder being synced —
`photos/2019/a.jpg`, not `/home/me/photos/2019/a.jpg`. S3 calls the same thing a *key*, and the
code does too, so the two words mean one thing throughout.

---

## Where this sits

```
        local root                      S3 root
            │                              │
       ┌────▼─────┐                   ┌────▼─────┐
       │  FsWalk  │                   │  S3Walk  │     enumeration
       └────┬─────┘                   └────┬─────┘
            │   Entry / StreamError        │
            └──────────────┬───────────────┘
                           │                                         ┐
                    ┌──────▼───────┐                                 │
                    │     join     │   one key at a time, in order   │
                    └──────┬───────┘                                 │
                           │   Pairing { key, source, destination }  │  sync::Walk
                    ┌──────▼───────┐                                 │
                    │   Compare    │   a policy a caller can replace │  (this layer)
                    └──────┬───────┘                                 │
                           │   Decision                              ┘
                 ┌─────────┴─────────┐
          ┌──────▼─────┐      ┌──────▼───────┐
          │  dry run   │      │  execution   │   scheduling, retries, deletes
          └────────────┘      └──────┬───────┘
                                     │
                              ┌──────▼───────┐
                              │  reporting   │   events, counts, failures
                              └──────────────┘
```

This layer hands over one decision at a time and never performs an action. Carrying a decision
out — uploading, downloading, deleting — happens beyond it, and the two can overlap: a transfer
for the first name can be under way while the walk is still reaching the last. A dry run is the
same stream with nothing attached to the end, so the two share every line of comparison code.
**FR-Dry-2** asks for exactly that.

## 1. Scope, and why it is this

This layer builds one thing:

> one decision per relative key — transfer, delete or leave alone, carrying the reason it was chosen
> — taken from what each side holds at that key and the settings the run was given, produced in
> key order, by code that cannot wait on I/O.

One decision per key, because a key is what a caller acts on and what a report names. From what
each side holds, because a side has three answers: there is an entry here, there is
nothing here, or sync never managed to look. In key order,
because a plan that depended on which side answered first would come out differently on two runs
over the same buckets. And by code that cannot wait on I/O, because comparing two keys must not
cost a request — a bucket with a million objects would cost a million of them — and a function
with no `await` in it cannot send one.

This layer answers the requirement category that describes comparison — **FR-Cmp**, which
[`functional-spec.md`](functional-spec.md) defines. Two of those are for comparisons that read
bytes or ask the service about a single object: **FR-Cmp-9** compares checksums and **FR-Cmp-11**
compares properties a caller picks, such as storage class or content type. Neither can answer
without I/O, so what this layer offers them is a place to plug in: an answer that says "ask me
again later", which D6 describes.

**FR-Cmp-8** wants the same states and the same settings to produce the same plan on every run.
What serves it is the shape of the interface: a comparison is handed one pairing and takes
`&self`, so the answer follows from its arguments and nothing else. A comparison of somebody's own
may still read a clock or count what it has seen. `&self` hands out no way to mutate, so doing
that means reaching for a `Cell` or an atomic, which is visible in the code and takes a deliberate
act. That is as far as the types go.

Enumeration, execution, failure handling, reporting and dry runs belong to other layers. Several
of their requirements are met here even so. **FR-Fail-7** wants a key no side could account for to
get no action, so a side has three states where two would seem enough (D1). **FR-Enum-12** wants a
directory nobody could read reported as a range of keys never looked at, and turning that into
keys happens here. **FR-Enum-3** wants a name occupied by something sync cannot send — a device, a
socket — to still count as present, so the key on the other side is not read as one the caller
deleted. That is why what blocks a transfer travels with the entry rather than being worked out
later.

**FR-Obs-1** wants one value per key carrying everything a caller needs, which is what settled the
opposite question — what a decision does *not* carry, since the merge already holds the name and
both sides and puts them back together when it reports. **FR-Exec-2** puts the delete decision
here while the deleting happens elsewhere. **FR-Root-7** wants a configuration that cannot work
refused before any work starts, and the reason one exists is here: reading absence from position
holds only while both sides arrive in the same order. A directory bucket does not list keys in
alphabetical order, so it is the configuration that has to be refused. **FR-Exec-5** wants a
download to stamp the file with the object's time, and one of the comparisons below,
`ExactTimestamps`, is only correct once something does. Nothing stamps it today, so on a download
that comparison never reaches an equal pair and sends every key on every run. **FR-Dry-2** wants a
dry run to go through the same comparison as a real run, so this layer stops at a decision. And
**NFR-Mem-1** and **NFR-Tput-2** are the memory and per-key bounds behind two things: a comparison
that keeps nothing at all, and a merge whose own bookkeeping is bounded by the directories it has
open rather than by the number of keys. That second bound holds because the names a merge holds
back are a subset of the children a directory read already collected, and because those names are
kept sorted, so answering for one costs a lookup rather than a scan.

---

## 2. Design decisions

### Decisions the next layers are stuck with

Execution will be built on top of these. Changing one of them later means rewriting whatever was
already using it.

**D1. A side can say "I could not look", and that must never read as "nothing is here".** Asked
what it holds at `photos/a.jpg`, a side answers one of: here it is, there is nothing here, or I
could not look.

The first two come straight from enumeration — either it produced an entry for that name, or it
walked past the name and there was nothing. The third arrives as an error. Something went wrong,
and enumeration says whether it cost this one name or a whole stretch of them; working out which
names that covers is this layer's job (D7).

Here is why the third answer is kept separate. Suppose the bucket holds `photos/2019/a.jpg` and
the local folder has a `photos/2019` directory the process cannot open. If "I could not look" is
folded in with "there is nothing here", the comparison concludes the local file was deleted, and
delete mode removes the object. The file is sitting right there on disk, and the object that
backed it up is gone.

So the answer is a three-way type, and one place reads it: the entry point the interface provides,
written once. It answers all three, and a comparison is only asked about a pair where both sides
produced an entry — so a mode that looks at nothing but file sizes never has to say what it does
when it could not look, because that pair never reaches it. Somebody replacing that entry point
does take on all three, and the compiler tells them so.

**FR-Fail-7** is therefore hard to skip by accident, and a state added later would stop the entry
point compiling until someone placed it. What it costs is paid by the merge rather than by any
comparison: answering "I could not look" at all means turning each failure into the names it cost
first, which is most of the machinery D7 describes.

**D2. This layer sets up both walkers itself, because the order the merge needs is not the
default.** Working out that a name is missing from one side relies entirely on position. Once a
side produces a name that sorts later, the earlier one is never coming. That only holds while both
sides arrive in the same order, and the walkers do not do that by default. Left alone they hand
back entries in whatever order the filesystem gave, stop at the root directory's immediate
contents, and leave out device files and pipes. The last two produce a shorter list that ends
without complaint, and a merge reads a short list as names that are gone, so delete mode removes
their counterparts. Arbitrary order breaks it more thoroughly, because the merge decides a name is
missing by having passed its position: with the order unknown it passes positions whose names have
not arrived yet, and every one of those reads as gone.

Building them here keeps the settings a merge depends on out of a caller's reach, and it is the
only place one set of include and exclude rules can be attached to both sides at once. Without
that last part, a name excluded from one side but not the other looks like a file someone deleted.

Sorting the whole tree costs three things, all following from one fact: a file cannot be handed
over until every subdirectory sorting ahead of it has been read, because one of those subtrees may
hold a key that sorts earlier. So time-to-first-decision grows with the tree, and on a deep one
the walk is nearly finished before anything comes out. That sits against **NFR-Lat-1**, which asks
for the first transfer to start as promptly on a million keys as on ten — the listing side meets
it, and a local walk in this order does not. Claiming a subtree to walk in parallel is unavailable
for the same reason: a subtree handed to another worker cannot know where its keys belong among
the ones still unread. The merge itself would be one step at a time regardless, since pairing two
ordered streams means looking at one key from each. The third cost is memory, and it stays small.
The walk holds the children of every directory on its way down, which
[`enumeration.md`](enumeration.md) prices. The merge adds one entry per side plus whatever names a
failure put at risk (D7), so this layer is a small constant on top of the shape of the tree.

**D3. Being present is not enough to compare or to send, so the entry says which it is.** D1 gave
a side three answers. The first — yes, there is an entry here — splits again, and the three want
opposite actions:

```
   yes, described   size and time were both read                compare them
   yes, unread      a field the side could not read             transfer: nothing proves a match
   yes, blocked     a socket, pipe, device, symlink or archive  nothing to read, so nothing is sent
```

A socket, a named pipe and a device file each occupy a name in a directory with no ordinary
contents behind it — a named pipe is how one program streams data to another, so reading it waits
for whatever is on the other end, possibly forever. The name is real; there is nothing behind it
to send.

The obvious way to record the three kinds is to give a side five answers instead of three,
splitting *yes* apart. Delete mode shows why that was rejected. It asks the source one question —
do you have this name? — and with three answers, one of them means yes. With five, three of them
do, and every piece of code asking has to remember all three. Leave out the blocked one and a
named pipe reads as "nothing here": sync decides the local file was deleted and removes the object
that was backing it up, while the pipe sits in the directory holding the name.

So a side keeps three answers and the extra facts are fields on the entry's metadata: the two
fields a comparison reads are optional (D4), and beside them an obstruction names what stops a
transfer. It is plain data, not a method, because yes-or-no could not say *which* cause it was —
an archived object is worth transferring once a restore finishes, a named pipe never will be.

Which roles a cause blocks belongs to the cause. A socket, named pipe or device blocks both
reading and being written over, since writing to a pipe nobody reads never returns; an archive
blocks only reading, so uploading over an archived object goes ahead normally. So the side the
block sits on matters, and on the destination so does what sync was about to do there. A blocked
**source** keeps the matching object from being deleted (**FR-Enum-3**), because something
occupies that name. A blocked **destination** splits: writing over it is skipped when the block is
one that stops a write, while deleting it goes ahead as normal (**FR-Exec-2**), because removing a
name reads nothing from it and writes nothing to it. Causes a listing does not report stay out — a
retention lock, a legal hold — because finding out would cost a request for every object, which
**FR-Cmp-5** forbids.

**D4. A field nobody managed to read is not a value, so the safe answer is to transfer.** The two
fields a comparison reads are both optional, because either can be missing when a side could not
describe what it found. Handing that absence straight to a comparison goes wrong quietly. In Rust
`None` sorts below every `Some`, so a source file with no timestamp reads as *older* than the
destination, the pair comes out "unchanged", and a file that was never transferred is never
transferred — quietly, every run.

So only two known values can produce a skip. That much no comparison gets to decide for itself,
and the interface settles it once for all of them. What a comparison does with a pair it could not
describe is its own business, so the interface asks. One reading both fields has nothing left to
compare. One reading only the size still has what it needs. And one that refuses to overwrite an
existing name does not care what went unread. The cost is that a side which can never describe an
entry is transferred every run, which is the safe direction and the one the spec already picked.

Size and timestamp are the pair every comparison gets, flattened to the same shape whichever side
produced them. The original entry stays reachable beside them, because **FR-Cmp-11** lets a caller
compare storage class, content type, cache-control, permissions or user metadata, and those live
on the listing's own object.

**D5. The two side types are part of the interface, so a comparison covers the directions it can
serve.** A side is either a local file or a listed object, and the three directions pair them
differently. Downloading puts an object against a file, uploading is the reverse, and copying
between buckets puts two objects together. The pairing already carries that; the question was what
the interface does with it.

It keeps them. Uploading is `Compare<FsEntry, Object>`, downloading is `Compare<Object, FsEntry>`,
copying is `Compare<Object, Object>`. A comparison whose answer depends on direction writes one
implementation per pair, and the compiler then refuses to use it for a direction it does not
cover. The default rule needs this: its timestamp test flips sign between uploading and
downloading, and the exact-timestamps mode changes downloading while leaving the other two alone.

`Compare<FsEntry, FsEntry>` — local to local — is spelled by any comparison with a blanket
implementation. What keeps it out is not the types but the merge, which always builds one local
side against one listing; that is where **FR-Root-1** is enforced.

A comparison that genuinely ignores direction writes a single blanket implementation instead.
Size-only is the example: a size is a size whichever side reported it, so one implementation
serves all three directions. Either way, the built-in comparisons and a caller's own go through
the same interface. **FR-Cmp-6** asks for that.

The alternative was one erased view of a side, with a flag saying local or remote. That makes the
interface simpler and every comparison shorter, at the price of letting a local file be passed
where an object is required — the kind of mistake D3 and D4 exist to prevent.

---

### Room left for things we are not building yet

**D6. An answer can be "ask me again", so checksum comparison can be added later.** Comparing
checksums or object properties means reading bytes or asking S3 about one object, so a comparison
doing that cannot answer on the spot. **FR-Cmp-6** requires the interface to accept "not yet, ask
me again" from the first release, and says so explicitly even though every comparison shipped here
answers immediately. So the answer type carries that case, and somebody's own comparison can use
it today.

Because nothing produces it, whatever ends up driving the comparison should treat one as a bug. It
should skip the name, say why, and mark the plan incomplete: a name that should have been
transferred and was not is the same hole D8 describes. No new reason is needed to say so, because
the skip reason for a pair nothing could describe already covers it. The driver does not live
here, and `Walk` does not name a single one of the comparison's types.

**D7. A failure has to become specific names, or absence cannot be trusted again.**
When something goes wrong, the walk says whether it cost one name or a whole stretch of them.
A failure usually costs at least one, because the walk only reports what it could not account for.
The exception is a filename that could never become a key: nothing was derived from it, so there
is no key on that side for the failure to have hidden. This layer turns the rest into actual
names.

A listing names the object it dropped. A filesystem walk reports an absolute path and no key at
all, so this layer works the key out from the walk root it holds. And a walk failure is raised
while a directory is being read. Reading one collects every child before handing any of them over,
and queues that read's failures alongside them, so the failure comes out at or before the position
of what it cost and never after. The merge writes the name down on arrival and holds it until it
gets there.

So the merge takes a prefix off the failure's own path, remembers it, and forgets it only once it
has passed every key under that prefix. Releasing it at the side's next key instead would settle
nothing: a symlink loop noticed near the top of a directory arrives before entries that sort
earlier but sit deeper in the same subtree, so the next key tells you nothing about what the
failure hid. One walk can hit several such failures, so several prefixes are remembered at once —
drop one and its keys go back to looking falsely absent. Where no prefix can be worked out at all,
the rest of that side is treated as unknown. A listing failure is different: S3 raises it as the
entry is handed over, so it is recorded at the position of the one key it disrupted.

They are kept in name order, because arrival order cost a bug. A walk reports one directory's
failures in whatever order the filesystem listed them, so two unreadable children can arrive as
`z` then `a`; draining from the front leaves `a` unmatched, an unmatched name reads as absent, and
delete mode removes a file sitting on disk whose loss was reported. In name order, deciding one
name costs a lookup (**NFR-Tput-2**).

**D8. Two failures that both cost one name are told apart by whether a name exists to hold.** The
layer below says "one name" for two different things: a file nobody could read, and a filename
that cannot become a key. This layer does opposite things with them. A walk failure carries a
path, so the key can usually be worked out and that one name withheld; where it cannot be, the
whole of that side is treated as unknown. A filename that cannot become a key has nothing to hold
— the comparison never sees a side for it at all. There was an alternative: work out a set of
names the file must be among. **FR-Enum-3** records why it was rejected.

Both leave the plan marked incomplete, because a name went unaccounted for either way, and that is
all they share.

What a failure cost is one type with four cases: a name, a name nobody could work out, a stretch,
and nothing lost at all. A chain of if-conditions was the alternative, and it read as four
unrelated tests — hiding that two of the four arrive from below under the same answer and only
part company here.

**D9. The built-in comparisons get a selector, so a caller can name one without naming a type.**
The interface is how you plug in your own. The selector is the shortcut for people who want one of
ours, picked from a command-line flag or a config value. There are four, each answering the
requirement that asks for it:

```
   SizeAndTime        FR-Cmp-1   transfer when the sizes differ, or the source is newer
   SizeOnly           FR-Cmp-2   transfer when the sizes differ; ignore timestamps
   ExactTimestamps    FR-Cmp-3   on download, leave alone only on an exact time match
   NoOverwrite        FR-Cmp-4   never write over a name the destination already has
```

`NoOverwrite` is the only comparison that refuses to overwrite, so it is the only one that ever
reports `DestinationExists` as its reason. It answers that whether or not the pair could be
described, because it reads neither entry: the destination holds the key, and what went unread
about it changes nothing. `SizeOnly` needs its own rule for an undescribable pair too, and for the
opposite reason — it compares the sizes it did read, because a timestamp nobody could read says
nothing to a mode that ignores timestamps. Without that rule a pair with one unread timestamp
would be sent on every run, over a field the mode never consults.

**FR-Cmp-10** asks for a download that transfers whenever the two sides differ at all, and
`ExactTimestamps` already is that. On a download both requirements come out the same: leave a name
alone only when the sizes match and the times are equal. So there is no fifth comparison: adding
one would give callers two names for one behaviour, in the only direction either applies to. It
cannot be offered for the other two at all, and the comparison records why. S3 stamps its own
timestamp on whatever it stores, so an upload or a copy never reaches an equal pair and would send
every object on every run.

Picking a comparison hands back that comparison, through one method per direction — asking for the
uploading one, the downloading one or the copying one — and the selector implements no comparison
of its own. If it did, it would forward each method by hand, and a method added later would be left
unforwarded — the selector answering with the interface's default while the chosen comparison's
own answer sat unused. Forwarding just the entry point does
not fix it either, since a method called directly still reaches the default. Having no
implementation at all is what removes the default there is to inherit.

**D10. Nothing here is public yet, so the shape is still free to change.** The public shape — how
a run is configured, how the roots are named, the comparison interface itself, what a comparison
is handed — gets settled once, later, when there is enough working code to know what shape it
should be. So the comparison a caller can replace exists today, and how they reach it is decided
elsewhere.

Nothing carries `#[non_exhaustive]` either. That attribute does nothing while a type reaches only
this crate, and it reads as a promise already kept. Publishing means adding it to five types at
once — the decision, its two lists of reasons, the answer type, and the selector. Those are all
things a caller reads rather than writes, so they can be sealed. The comparison interface itself
cannot be, because **FR-Cmp-6** exists so that a caller can write their own. Writing that down
here is cheaper than rediscovering it from five separate comments later.

---

### Cheaper to revisit

**D11. Timestamps compare in whole seconds, so an unchanged pair cannot flip between runs.** S3
stores whole seconds; a local filesystem stores finer. Take a file at 10.6 seconds and an object
stamped 10. Dropping the fraction makes both 10, so they match and the file stays. Rounding makes
the file 11, so it reads as newer and uploads — and uploading does not change the file's own
timestamp, so the next run reads 11 against 10 again and uploads again, for as long as nobody
edits it. **FR-Cmp-7** forbids exactly that. Dropping the fraction is what keeps the answer the
same on every run.

What that accepts is a blind spot. A file edited within the same second it was last transferred,
ending up the same size, compares equal and stays behind. One second is the finest S3 will tell
us, so no comparison based on times alone can see that edit.

This holds as long as the filesystem stores seconds or finer, which agrees with S3 either way. One
storing two-second steps, as FAT does, does not. It shows up on `Download`, where **FR-Exec-5**
writes the object's time onto the file it just fetched: an object stamped at an odd second cannot
be stored there, so the file ends up on the neighbouring even second and the next run reads the
pair as different.

**D12. "Archived" means a storage class and a restore status together, both read from the
listing.** **FR-Fail-1** needs an object that has been restored told apart from one still in the
archive, and asking for both on the listing keeps the check from costing a request per object.

A listing reports a restore status only for an object that has one, so its absence says nothing by
itself — a STANDARD object and a Glacier object nobody ever restored both come back without it. So
the check reads the storage class too and answers from the pair. Only `GLACIER` and `DEEP_ARCHIVE`
can be tested this way, and **FR-Fail-1** records which other classes are excluded and why:

```
   GLACIER or DEEP_ARCHIVE, and:
     no restore status                        archived, cannot be read
     a restore in progress                    not readable yet; a later run will get it
     a finished restore with an expiry date   readable until that date
```

Asking costs a request parameter S3 documents as unsupported for directory buckets. The S3 walk is
shared, and `download_objects` uses it today against whatever bucket its caller names — including
a directory bucket, where the parameter would be refused. Turning it on for everyone would break
those callers. Sync asks for it on its own listings and leaves
the shared default alone.

The rules are applied where the listing is read, and what reaches the comparison is
a single obstruction saying the object cannot be read (D3). A comparison is generic over the two
side types, so reading the storage class directly would mean either carrying the rules through the
interface or repeating the check in every comparison — and one that forgot would dispatch a
transfer that fails.

The answer is a snapshot either way: a restore can expire between the listing and the transfer,
and then execution meets the refusal it would have met with no check at all. What the check buys
is not issuing a doomed transfer for every archived object in a bucket.

---

## 3. How the pieces fit

### The problem

A decision has to come out of one pairing and the run's settings, with no I/O in between. A
comparison that could make a request would cost one per key, so a bucket holding a million objects
would cost a million of them (**FR-Cmp-5**). What makes that awkward is the second constraint: a
side may have failed to account for its key at all, and the answer still has to be one that
destroys nothing.

What follows is how the pieces produce an answer: the join that pairs two streams, the types it
pairs them into, a single run traced through them, what a decision carries, and what this layer
deliberately leaves to someone else.

### The join

Two streams arrive in the same name order. The merge advances whichever side sorts earlier, and
pairs them up when the names match:

```
   source        destination     what the merge concludes
   ──────        ───────────     ────────────────────────
   a.txt         a.txt           both have it          → compare them
   img/logo.png  img/old.png     source sorts earlier  → destination does not have it
   (end)         z.txt           source has run out    → source does not have it
```

"Does not have it" is read off position alone. Once the destination side has reached
`img/old.png`, `img/logo.png` cannot still be coming, because a listing in name order would have
produced it already. That reasoning needs both sides in one order, and the order is S3's: a
general-purpose bucket lists keys as raw bytes, ascending. Nothing can change that, so the local
walk is the side configured to match it, which is what D2 sets up. An error the side survived
turns into "never looked at" for the names it covers, and the pairing carries that through. One
that ends the side ends the run instead: nothing further pairs, so there is no key to report it
against, and the plan says it is partial.

**Every answer comes from one pairing and nothing else.** That is what lets two runs over
unchanged buckets reach the same decisions, and it is the property the rest of this section keeps.

### The types

The join hands over a `Pairing`, and a comparison hands back a `Verdict`. Those are two of the
names in the boxes of the pipeline diagram at the top of this document; here is the whole cast.
The left column already existed in enumeration, the right is new here:

```
   already in io/                    new, in operation/sync/
   ──────────────                    ───────────────────────
   Entry<T>, EntryMeta               SideState<T>            present, absent, unknown
   KeyStream                         Pairing<S, D>           one key, both sides
   StreamError, WalkError            Compare<S, D>           the trait a caller can implement
   keys_lost, KeysLost               Described<'_, T>        a side a mode can compare
                                     Verdict                 decided, or ask me again
                                     Decision                transfer, delete or skip
                                     Transfer, Skip, Delete  what each action carries
                                     TransferReason          why a transfer
                                     SkipReason              why not
                                     SizeAndTime, SizeOnly   the built-in comparisons
                                     ExactTimestamps, NoOverwrite
                                     Mode                    which built-in to use
                                     Walker, …Builder        what a run was configured with
                                     LocalAndBucket, …Builder  one local root, one bucket
                                     Walk                    the run in progress
```

Enumeration gains two things, both on the metadata it already attaches to every entry.
An `Obstruction` says why something cannot be transferred — a device file has no bytes to send, an
archived object cannot be read (D3). And a listing can now be asked whether an archived object has
been restored (D12).

Nothing here records whether the run is an upload, a download, or a copy between buckets. The two
side types say it instead: a local file against an object means an upload, and the reverse means a
download. So the compiler picks the right comparison and no code has to check at runtime (D5).

### One run through

Six keys from an upload with delete mode on. The two middle columns are what the join hands over;
the right is what comes back. Between them they cover every answer this layer can give.

```
   key                 source               destination          decision
   ───                 ──────               ───────────          ────────
   a.txt               present, described   present, described    skip, nothing changed
   b.txt               present, described   present, described    transfer, the sizes differ
   c.txt               present, described   present, its time     transfer, nothing proves
                                              unread                they match
   d.txt               absent               present, described    delete
   photos/2019/x.jpg   unknown, a range     present, described    skip, a side could not look
   backup.fifo         present, a named     absent                skip, nothing to read from it
                         pipe
```

Row by row: `a.txt` and `b.txt` are the ordinary pair, where both sides read cleanly and the mode
decides. `c.txt` shows the second entry point — one timestamp went unread, so no comparison can
show the two match, and the mode is asked again with the raw entries (D4). `d.txt` is on the
destination only, which delete mode removes. `photos/2019/x.jpg` sits under a directory the walk
could not open, so the source is not absent but unaccounted for, and no delete may follow
(**FR-Fail-7**). `backup.fifo` occupies a name with nothing readable behind it, so it is neither
sent nor treated as missing (D3).

The two rows that look alike are the ones worth separating. `d.txt` and `photos/2019/x.jpg` both
have a destination entry and no source entry, and they get opposite treatment: one is a delete,
the other is the delete that must not happen. That distinction is the whole of D1.

### What a decision says

A decision is one of three things, each wrapping a small struct of its own:

```
   Transfer   carries why it is being transferred
   Skip       carries why it is being left alone, and any detail that reason needs
   Delete     carries nothing yet
```

A decision says what should happen and why, and nothing else. The name and both sides stay with
the pairing the merge already holds, and the merge puts them back together when it reports. The
alternative was carrying them on the decision, on the grounds that the decision is what outlives
the call. That would mean a comparison handing back entries it was only lent, a duplicate of the
name and both entries in every answer, and a type too big to pass around freely. The merge has all
three to hand at the moment it reports, so duplicating them buys nothing.

**FR-Obs-1** still gets its one value per name holding the direction, the name, both sides, the
action and the reason, all readable without parsing text. What changed is only where it gets
assembled. The direction was always the merge's to supply, being fixed for a whole run, so putting
it on every name would repeat one answer a million times. The name and the sides are left off for
a different reason: they do vary per name, but the merge is holding them at the moment it reports,
so a comparison handing them back would only be returning what it was lent.

Splitting the reason by action is what stops nonsense being expressible: a transfer cannot claim
it happened because nothing changed, and a skip cannot claim the source was newer. Each action
wraps a struct read through accessors, so a second piece of context can be added later without
changing the shape anyone matches on. `Delete` carries nothing today and will need something,
because an opt-in that removes entries the filters excluded is a second way to arrive there.

A skip holds one private value tying each reason to whatever that reason needs — how much a side
lost, or what was in the way. Built that way so a reason that needs a detail cannot be stated
without one, while a caller grouping names by why they were skipped still gets a flat list of
reasons to match on. Where both sides lost track, the coarser answer is reported. Naming one lost
key tells a caller the keys around it are accounted for, so a caller acting on it holds back that
one name and trusts the rest. If the other side lost a range, that trust is misplaced, and the
range is the answer that does not invite it.

### Why these names

`Walker` holds what a run was configured with and `Walk` is the run in progress, answering `next`
and `is_done`. Enumeration already uses those two names twice over, for its filesystem walk and
its S3 walk, so anyone who has read one side knows where to look on this one. The alternative was
a vocabulary of its own — a join, a plan, a cursor — which reads well on its own and makes
everybody learn it.

Enumeration's third name does not carry across. Its walk contexts each hold one root, so
`FsWalkContext` and `S3WalkContext` say all they need to; a comparison needs two roots at once,
and what those two are differs by direction. `LocalAndBucket` names the pair an upload and a
download share, and a copy will need a second type holding two endpoints, so a neutral
`WalkContext` would have hidden exactly the distinction a caller has to get right.

Those two carry `Fs` and `S3` prefixes because they are siblings in one module. These types live
in `operation/sync/`, where there is one walk and one comparison, so `sync::Walk` already carries
what a `Sync` prefix would. They sit outside `io/` because neither one does any I/O.
`Compare::compare` is a plain function, which is what enforces **FR-Cmp-5** at the definition, and
the walk makes no system calls of its own — it drives two streams that do. Enumeration's walks do
make them, so they stay in `io/`, which also suits them because each has a user outside sync.

Every name here is crate-private today, including the trait a caller is meant to implement. D10
has the reason and what publishing them will cost.

### What this layer does not decide

Three things pass straight through it. The direction is fixed when the run is configured, so no
decision mentions it and the two side types carry it instead (D5). Whether a decision is acted on
belongs to the dry-run switch, which is why the same stream of decisions comes out either way
(**FR-Dry-2**). And the order transfers happen in belongs to execution, which may run them in any
order (**FR-Exec-12**).

That last one bounds what **FR-Cmp-8** asks for. Two runs over the same two sides owe the same
decision and the same reason for every key; they do not owe the same sequence. A comparison
answering on the spot happens to produce them in key order, because that is the order the merge
hands keys over in, but one that defers answers whenever its read finishes, and nothing downstream
cares.

### Where this runs

The scheduler polls for work in a tight loop, and the function handing back the next thing to do
is a plain one. A walk is `async`, so it cannot be advanced from there. `upload_objects` already
solves this: the poll hands out a work item, the scheduler runs it somewhere awaiting is allowed,
and that pass drains a batch of entries into a buffer the next poll reads.

Sync takes the same shape one level up, and building that part belongs to the operation.
`Walk::next()` is async, so it belongs in such a work item, with decisions landing in the buffer
for the poll to dispatch. A dry run needs no scheduler and can drive `Walk::next()` directly. Both
already have what they need from this layer: a `next` that pulls one name at a time, and a plan
that says whether it was complete.

That leaves one thing open. A dry run reads at its own pace and nothing is held up if it stalls,
so making it wait is fine. Reporting must never stall a run, so it cannot wait. Whether one stream
serves both or they end up as two types is settled where delivery is built.

One last note on why the comparison is a plain function. **FR-Cmp-5** says a comparison must not
make a request per entry, and a function with no `await` in it cannot reach for one the ordinary
way. Somebody determined to block anyway still can. A comparison that genuinely needs to fetch
something answers "ask me again" and gets a second call once it has what it needed.
