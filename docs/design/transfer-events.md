# Transfer events

A directory operation moves thousands of objects under one handle. Its metrics snapshot reports how
many entries finished and does not report which ones, so a caller that acts on one object has nothing
to read. Polling also cannot observe an ending: a snapshot taken after a counter moved reports the
counter, not the transition, and no counter names an individual key and its outcome. Callers that
delete a source after its upload commits, render a row per file, or classify failures to decide
whether to retry a run therefore cannot be written against the existing surface.

One event stream reports per-entry lifecycle. An entry is announced when its action is decided and
reported again when that action ends, and the ending names what happened. Quantities stay on a
read-only view that the announcement carries, which a consumer reads on its own schedule against the
same counters the metrics snapshot reads. Registration is per client, per request, or both. Lifecycle
delivery is bounded and lossy with loss counted per consumer; quantities are not delivered and cannot
be lost.

```text
operation                          this design                     consumer
---------                          -----------                     --------
upload / download       --+
                          +--> Lifecycle --> Planned --+--> channel --> recv()
upload_objects            |         |                  |    (bounded,
download_objects        --+         `----> Ended -------+     lossy)
  one entry per object              |
                                    `----> TransferView <------------- read on demand
                                                 ^
                                                 | same counters
                                            metrics()
```

## Requirements

### Report the identity and outcome of each entry

Each entry of an operation is announced once and, when its action is attempted, reported once more
with a terminal outcome. A consumer keyed on identity acts on that report. An aggregate count cannot
serve it, because the action is per object and a count names none.

An entry whose decision attempts nothing has no terminal to report. Its announcement is the complete
record of it.

The operation itself is an entry. For a single-object transfer it is the object. For a directory
operation it is the root, whose endpoints are the source directory and the key prefix rather than a
file and a key, and every object of that operation is reported as its child.

```text
upload_objects, three objects, the second fails under an abort policy

  Planned  root        parent = None
  Planned  a.txt       parent = root
  Planned  b.txt       parent = root
  Planned  c.txt       parent = root
  Ended    a.txt       Succeeded
  Ended    b.txt       Failed(403)
  Ended    c.txt       Cancelled
  Ended    root        Failed(403)

a single-object upload reports one pair, and its parent is None
```

A consumer acting per object skips the parentless entry for a directory operation, where acting on it
would act on the whole tree. It does not skip it unconditionally: for a single-object transfer the
parentless entry is the object.

### Report a terminal only when its effect has committed

A successful terminal states that the object is in S3, or that the file is at the path the caller
named. It does not state that the bytes moved.

Otherwise a consumer that deletes a source when its upload succeeds deletes the only remaining copy
of data whose destination does not yet exist, and a caller that trusts the terminal instead of joining
the operation discards a transfer that had completed.

### Report an ending as the kind of ending it was

A failure is reported as a failure and carries its cause. A cancellation is reported as a
cancellation, including a cancellation caused by a sibling's failure under an abort policy. Neither is
substituted for the other, and neither is inferred from a transfer merely having become inactive.

A cancellation carries no cause. Reporting a failed operation as cancelled therefore reports nothing
at all about why it ended, while the operation's join returns the cause to its caller.

```text
outcome      states                        licenses

Succeeded    the effect committed          delete a source, mark a row done
Failed       it did not, and here is why   retry, or alert, by error kind
Cancelled    it did not, and nobody asked  no retry, no alert, no cleanup
```

### Carry the cause a caller would receive

The error on a failed terminal is the error that operation's join returns. No terminal substitutes a
constructed error for an absent one.

Otherwise a consumer that branches on error kind cannot separate throttling from a missing object or
a permissions failure, and every failure becomes one failure.

### Keep reported quantities correct under delivery loss

A consumer renders from quantities it reads, not from quantities delivered to it. A quantity assembled
from delivered messages is permanently wrong once one message is lost, where a quantity re-read after
a loss is correct on the next read.

Delivery loss is therefore confined to lifecycle. Losing a lifecycle event costs one notification to
one consumer. No loss changes a reported quantity.

### Report the same quantities with or without an observer

Registering a sink does not change what an operation reports about itself. Two runs that differ only
in whether an observer was present report one set of quantities.

Otherwise the settled-entry count depends on who was watching, and it cannot reach the enumerated
total on a run with no consumer, so a progress display shows outstanding work after the operation has
ended.

### Isolate observers from each other and from the operation

Capacity and loss are per consumer. A consumer that stops draining loses its own events and does not
affect another consumer's stream or its loss count.

No consumer can stall a transfer, and no consumer's code runs on a thread a transfer needs.

### Report progress in entries and in bytes

An operation reports a settled-entry count against an enumerated entry total, and transferred bytes
against a byte total. Both totals distinguish "not known yet" from "known to be zero", because a
listing that is still running and a prefix that is empty are different facts to a progress display.

A total is provisional while listing continues and final at the listing-complete edge. A listing that
never ran seals nothing rather than sealing zero, which would claim an arbitrarily large prefix was
empty.

### Attribute a download stall to a cause an operator can act on

A transfer that has stopped moving bytes presents identically regardless of cause, and byte counters
cannot separate a full memory budget from a closed read-ahead window from work that is merely slow. A
view reports the cause of its most recent park so that an operator can distinguish a condition they can
change from one they cannot. Both directions label their park sites, so neither substitutes a cause it
does not have.

This layer does not define the vocabulary of causes. A park is labelled where it happens, in the
operation's own state machine, and a cause reaches a consumer only because the view reads what that
layer recorded. Two consequences follow. Consumer backpressure is not separable from a closed
read-ahead window, because the window closes precisely when a consumer has not drained. A concurrency
limit never appears at all, because a transfer at the concurrency target is not polled and so records
nothing.

## Architecture

The first two subsections establish the [event and view split](#event-and-view-split) and the
[lifecycle obligation](#lifecycle-obligation) that makes reporting exactly-once. The remaining
subsections define the [public surface](#public-surface),
[registration and fan-out](#registration-and-fan-out), and
[publication ordering](#publication-ordering).

### Event and view split

`TransferEventSink` is the registered handle to lifecycle delivery. A per-entry `Lifecycle` holds the
sink and the entry's identity, and a `TransferView` holds a read-only borrow of the entry's counters:

```text
Lifecycle
+-- TransferEventSink
|   `-- one bounded channel per registered consumer, each with its own loss count
+-- entry identity
|   `-- id, parent id, TransferRef, Decision
+-- terminal obligation
|   `-- armed at announcement, discharged once
`-- TransferView
    `-- borrow of the entry's MetricsState, carried on the announcement
```

Lifecycle is pushed and quantities are pulled. An event carries identity, decision, and outcome. It
carries no byte counts. The announcement hands over the view, and the consumer reads the view when it
renders.

```text
                    pushed                              pulled
                    ------                              ------
what travels        identity, decision, outcome         nothing
what is read        nothing                             every counter
on a full channel   dropped, counted per consumer       not applicable
after one loss      that notification is gone           next read is correct
```

The split is what makes lossy delivery admissible. A lost byte delta is a total that stays wrong for
the remainder of a run; a lost lifecycle event costs one notification and is counted. Carrying both on
one channel also places progress in competition with terminals for one capacity, so a burst of byte
updates evicts the event a consumer most needs.

An unbounded channel is the alternative to bounded delivery and converts a slow consumer into
unbounded memory inside the library, which an operation cannot refuse. Reserving a terminal slot per
entry, or parking a full emit for retry, make delivery lossless and convert a slow consumer from a
delay into a hang; both also depend on a parked obligation eventually being retried. Counted loss is a
weaker failure than a stalled operation.

#### Why no emit site can wait for a consumer

Loss is not a property of the event type. It follows from where every emit in this layer happens:

```text
emit site                        holds a state guard   on a scheduler thread   may block
---------                        -------------------   ---------------------   ---------
transfer terminal                yes                   yes                     no
child announcement under a reap  yes                   yes                     no
abandoned-entry sweep            yes                   yes                     no
```

Every one of them runs while an operation's state guard is held, on a thread the scheduler needs, so a
blocking emit stops not only that transfer but every transfer in the client. Bounded and lossy is
therefore the only delivery this layer can offer, rather than the cheaper of two options.

A producer that holds no guard and occupies no scheduler thread has no such constraint. No producer in
this layer is one.

### Lifecycle obligation

An announcement arms the entry's terminal obligation. A terminal claim discharges it, and exactly one
caller succeeds. A claim yields the event as a value rather than sending it, and a claim that is
dropped unsent restores the obligation.

```text
Unarmed
  `-- announce -----------------> Armed
                                   +-- claim ------> Discharged
                                   |                  `-- send -> delivered or counted as lost
                                   +-- claim again -> no value, no second event
                                   `-- claim, then drop the value unsent
                                                    `-- Armed, the next path claims it
```

The obligation is not derived from the transfer's status transition. An entry refused by a failure
policy never receives a transfer context, so it holds an obligation and has no status to derive one
from. A transfer that was never announced reaches a terminal holding no obligation, so a status-driven
emit would report an ending for an entry no consumer had heard of. The status transition also names
the thread that performed it, which is usually the scheduler, and the scheduler holds no sink.

```text
                                obligation    status
entry refused by the policy     yes           none
transfer never announced        no            terminal
announced transfer              yes           terminal
```

#### Deriving an outcome

An outcome is computed from the terminal status together with the stored error, read together:

```text
status      error        outcome
Completed   -            Succeeded
Failed      present      Failed(error)
Cancelled   -            Cancelled
```

No emit site writes an outcome literal. A literal covers more states than it names, because a
cancellation and a failure both leave a transfer inactive, so a site asserting `Cancelled` reports a
failed operation as one the caller stopped.

The stored error is read as a clone rather than taken, so an observer and the operation's joining
caller read one value. No emit site carries a fallback error. With the error present the fallback
branch is unreachable, and an unreachable branch that constructs a plausible error is not detectable
by review in the way a missing match arm is.

### Public surface

`TransferEvent` has two variants. Each holds a private struct read through accessors:

```rust
#[non_exhaustive]
pub enum TransferEvent {
    Planned(Planned),
    Ended(Ended),
}

impl Planned {
    pub fn id(&self) -> TransferId;
    pub fn parent(&self) -> Option<TransferId>;
    pub fn transfer(&self) -> &TransferRef;
    pub fn decision(&self) -> &Decision;
    pub fn view(&self) -> Option<&TransferView>;
}

impl Ended {
    pub fn id(&self) -> TransferId;
    pub fn parent(&self) -> Option<TransferId>;
    pub fn transfer(&self) -> &TransferRef;
    pub fn decision(&self) -> &Decision;
    pub fn outcome(&self) -> &Outcome;
}
```

A `#[non_exhaustive]` struct variant admits an added field and nothing further. A rename, a type
change, and a removal each remain breaking, and every caller's pattern requires `..`. Accessors cost
one method per field and leave the representation free after the first release, which is also what
permits an opaque `TransferId` that can later carry a generation rather than a bare integer that
cannot.

```text
                       add a field   rename   change a type   remove   caller pattern
non_exhaustive enum    yes           no       no              no       requires ..
private struct         yes           yes      yes             yes      accessor call
```

#### Why these names

`Planned` names the decision taken for an entry, including a decision not to transfer it, so it covers
an entry that is announced and never attempted. `Ended` names a terminal without asserting success.

Three pairs are ruled out. `Decided` and `Settled` read as near-homophones aloud, which is what a
reviewer raised. `Started` and `Finished` misdescribes an entry that is announced and never attempted,
so the first name has to mean that an action was chosen rather than that it began. `Decision` and
`Result` collides with `Decision`, which is a payload type on both variants. `Completed` asserts success
and collides with `TransferStatus::Completed`, which under
[Report a terminal only when its effect has committed](#report-a-terminal-only-when-its-effect-has-committed)
is a different fact.

`TransferRef` names both ends of an entry, each an `Endpoint`:

```text
Endpoint
+-- S3 { bucket, key }        either end of a transfer, a copy, or a delete
+-- Local { path }            a file or a directory the caller named
+-- Stream                    a body the caller owns and drains
`-- Unresolved                no address this layer can know
```

`Unresolved` is not a placeholder for an unknown value. A download to a caller-supplied open file is
never told that file's path, so naming one would report an address the transfer manager cannot know.

`Local` carries a path and a path is not required to be valid UTF-8, so a local name with no key it
could take is still reported, at its own name. Escaping such a name for display belongs to whatever
renders it; the bytes are the fact.

`view()` returns `None` when the entry never became a transfer, which is a skip or an entry abandoned
before it started. That is distinct from a live transfer at zero bytes.

`TransferRef`, `Endpoint`, `Decision`, and `Outcome` are `#[non_exhaustive]`. The lifecycle, the
sink's internals, and the emit machinery are crate-private.

### Registration and fan-out

A sink is registered on the client, on a request, or on both. Each registered consumer holds one
bounded channel and one loss count, read through `dropped()`.

```text
Config::builder().events(sink_a)  --+
                                    +--> merge --+--> channel A  cap, dropped()
request.events(sink_a.clone())    --+            |
                                                 |
request.events(sink_b)            ---------------+--> channel B  cap, dropped()

sink_a registered twice is one consumer: one channel, one loss count
```

Registering one sink at both levels yields one consumer, not two. Registration at both levels is the
documented way to observe both scopes, so it is reached by accident as readily as deliberately.
Merging rather than overriding keeps a client-wide observer live when a request registers its own.
Deduplicating on the channel rather than on the sink keeps one consumer from receiving each event
twice, because a sink is `Clone` and two clones are one consumer.

A sink is passed to the call that announces an entry and is not stored beside a transfer. A stored
sink is a live sender, and a consumer's stream ends only when the last sender drops, so a stored sink
would hold a stream open past the operation that created it.

### Publication ordering

A terminal is published after the joining caller is released:

```text
worker thread                          caller thread
-------------                          -------------
transfer reaches terminal
release the joiner  ------------------> join() returns
claim the terminal
send it                                a consumer that drains here
                                       has not seen the last terminals
```

A consumer drains until its stream ends rather than until join returns. The stream ends when the last
sink clone drops.

Awaiting publication inside join is the alternative and places one consumer's channel progress in the
join path of every caller of that operation, so a full channel on one consumer would delay every
caller. The ordering is internal either way; what a caller depends on is the documented contract.

## Integration

Each operation registers a lifecycle at orchestration and discharges it on every path that reaches a
terminal.

### Single-object operations

`download` and `upload` hold one lifecycle. The entry point that selected the destination supplies it,
because only that entry point knows what the destination is:

```text
entry point                destination reported
-----------                --------------------
download().write_to_path   Local { path }
download().write_to_file   Stream          the path is never supplied
download()                 Stream          the caller drains the body
upload()                   S3 { bucket, key }
```

### Directory operations

`upload_objects` and `download_objects` hold a root lifecycle and one child lifecycle per claimed
entry. Children are orchestrated without a sink, because the parent announces its children; a child
holding its own sink would announce every entry twice.

A child's outcome is derived from its join result rather than from its status. A join result and a
status are not the same fact to a consumer deciding whether a file on disk is usable.

An entry refused by the failure policy never receives a transfer context. The parent announces and
finishes it in one step, because the entry cannot announce itself and it is the case a per-entry
consumer most needs reported.

### Commit placement

An operation whose durability step runs outside its state machine moves that step inside it. An upload
already satisfies this, because multipart completion precedes the completed status. A download to a
path does not:

```text
download().write_to_path, today             this design
-------------------------------             -----------
flush bytes to dest.s3tmp.XXXX              flush bytes to dest.s3tmp.XXXX
status = Completed                          rename -> dest
  every reporting path reads this            status = Completed
  and names dest, which does not exist        every reporting path reads this
                                              and names dest, which exists
join() -> rename -> dest
  or the handle is dropped and the
  temporary file is unlinked
```

The rename therefore moves into the transfer's own completion path, ahead of the status, alongside the
final flush. A completion path that runs while holding the state guard emits a commit work item
instead of performing the rename, because the state guard is not a place to perform disk I/O.

Every reporting path reads a status and cannot join. A status that means "flushed" rather than
"committed" therefore makes every one of them report a destination that does not exist.

### Terminal sweeps

A claimed entry occupies one of three places, and each is reached by one sweep:

```text
where the entry is                     reached by
------------------                     ----------
on the pending-entry buffer            the abandoned-entry sweep
in the child-lifecycle map             the orphan drain
claimed, not yet announced             the abandoned-entry sweep, because the
                                       claim captures the sink under the same guard
```

Claiming captures the child sink in the same critical section that removes the entry from the buffer,
which places the third case under the same guard as the first. The capture belongs inside the claim and
not at its call site. The claim borrows the operation state mutably and cannot release the caller's
guard, which makes removal and capture one critical section by construction rather than by two
statements remaining in order.

Reaping removes entries from the child collection before their outcomes are claimed, and the claim
follows an await:

```text
reap batch removed from the child collection
    |
    +-- child joined, outcome claimed            reported with its own outcome
    |
    `-- child not yet joined, operation ends     absent from the collection, so a
            |                                    sweep would read "never existed"
            `-- identities under reap are tracked, so the sweep reads its status
```

The identities under reap are therefore tracked, so a cancellation sweep separates an entry that is
mid-reap from one that never existed and reads its status rather than assuming cancellation.

### Counting

The settled-entry count is incremented where a terminal is claimed, which occurs once per entry, and
outside any branch conditioned on a sink being registered. An entry abandoned before it started is
counted into the enumerated total when listing produces it and is counted as settled when it is
discharged.

```text
entries_settled <= entries_announced <= entry_total

and on every run that ends:

entries_settled == entry_total    when entry_total is Final
```

## Correctness Invariants

The obligations in Architecture and Integration constrain individual mechanisms. The following
properties constrain their composition across announcement, terminal discharge, accounting, and
cancellation, and are what a consumer may rely on.

### Each entry is announced once and reported at most once more

An announcement occurs once per entry. A terminal occurs at most once, and occurs on every path that
ends an operation, including cancellation and a sibling's failure under an abort policy.
[Lifecycle obligation](#lifecycle-obligation) arms the obligation at announcement, and one claim
discharges it. [Terminal sweeps](#terminal-sweeps) reaches every entry that holds one.

This prevents a consumer waiting on a terminal no path emits, and prevents a consumer acting twice on
one entry.

### A successful terminal names an effect that has committed

A successful terminal names a destination that exists. [Commit placement](#commit-placement) orders
the durability step ahead of the status that every reporting path reads.

This prevents a consumer from deleting a source whose destination has not been created, and prevents a
caller that trusts a terminal instead of joining from discarding a completed transfer.

### A terminal names the outcome of its own entry

An entry's outcome describes that entry, including when its terminal is claimed while the operation is
ending for an unrelated reason. [Terminal sweeps](#terminal-sweeps) tracks the identities under reap,
so a sweep does not label an entry by the operation's ending.

This prevents a consumer that cleans up after a cancelled entry from removing complete data.

### An outcome and its cause are read together

An outcome is computed from a status and an error read in one access, and a failure carries the error
its operation's join returns. [Lifecycle obligation](#lifecycle-obligation) removes both the outcome
literal and the fallback error.

This prevents a failed operation reporting as cancelled and therefore reporting no cause, and prevents
a consumer branching on error kind from reading a constructed error.

### A reported quantity describes the run

Every quantity on `TransferView` and on the metrics snapshot has one value for a run whether or not a
sink was registered, and the settled-entry count reaches the enumerated total on every run that ends.
[Counting](#counting) places the increment on the claim and outside any sink-conditioned branch.

This prevents an operation from disagreeing with itself, and prevents a progress display from showing
outstanding work after an operation has ended.

### A consumer is isolated from other consumers and from the operation

Each consumer holds its own channel, capacity, and loss count.
[Registration and fan-out](#registration-and-fan-out) establishes one channel per consumer and
deduplicates a doubly-registered sink. [Publication ordering](#publication-ordering) keeps consumer
progress out of the join path.

This prevents one slow observer from throttling an operation or from appearing to corrupt another
observer's stream.

## Open Questions

**Whether the park-cause vocabulary is public.** The cause a view reports originates in the pending
state an operation records when a poll cannot produce work, which is crate-private and shaped for
aggregate diagnostics: a small set of categories paired with a static reason string, accumulated per
category with its own timings. A consumer-facing accessor needs a type that survives a 1.0 freeze, and
a static string paired with an aggregation category is not obviously that type. Publishing the internal
vocabulary, wrapping it, and reporting only the coarse category are all open, and the choice belongs
with the pending-state layer rather than here. This layer requires only that a view report a cause and
never invent one.

**Channel capacity has no measured floor.** Capacity stands for how far a consumer may fall behind
before it loses events — its render interval multiplied by the operation's entry rate — and the library
knows neither, so it is a required argument with no default, and `NonZeroUsize` excludes the one value
that cannot succeed. What a directory operation needs in order to lose nothing has not been measured;
measuring it requires mixed object sizes, a consumer rendering on a timer against one rendering per
event, and entry rates at target concurrency. A floor would reject a capacity that cannot succeed, at
the cost of rejecting a caller who accepts loss for a smaller allocation, and adding one later widens
the surface and breaks no caller.

## Future Work

**Skip arithmetic.** A skip is announced and never reaches a terminal, so an operation that produces
skips has an enumerated total its settled count cannot reach by terminals alone, and the count would
have to treat an announced skip as discharged at announcement. No operation here produces a skip today
and `Decision` can carry one; the arithmetic is recorded now because a producer arriving later changes
a counter's meaning rather than adding to it.

**Server-side copy.** A copy has an S3 endpoint at both ends, which `Endpoint` admits, and a direction
this layer derives from the two endpoints like any other. Nothing in the event surface changes for it.

**Span export.** The stream is the natural source for one span per entry. Span shape is not designed
here.

**Rates and estimated completion.** The view reports counters and not derivatives. A per-transfer rate
requires a sliding window per transfer, four per transfer, which is 40,000 windows for a 10,000-object
directory. A consumer holding two snapshots computes a rate with one subtraction.

**Backpressure from a consumer to a running transfer.** A consumer that would rather slow a running
transfer than lose an event cannot request that. Providing it requires that a terminal emit no longer
hold the state guard, which is a restructuring of the terminal path rather than a configuration option.
