# Execution

Execution carries out the comparison's decisions. It starts transfers, deletes
objects the comparison selected, and records each key's result.

The public event stream and private diagnostics tell callers about transfer
and delete results. Execution supplies the information both systems need.

## Where this sits

```
       ┌──────────────┐
       │  sync::Walk  │   one Pairing per key, in key order
       └──────┬───────┘
              │   Pairing { what each side holds }
       ┌──────▼───────┐
       │   Compare    │   a plain function, no I/O
       └──────┬───────┘
              │   Decision { Transfer, Delete, Skip }
       ┌──────▼───────┐                                     ┐
       │  poll_work   │   no awaiting: pick the next thing  │
       └──────┬───────┘                                     │
              │                                             │  execution
   ┌──────────┼──────────┬─────────────┐                    │  (this layer)
   ▼          ▼          ▼             ▼                    │
 advance    spawn a    batch a      reap the                │
 a walk     child      delete       finished                ┘
              │          │             │
              ▼          ▼             ▼
              upload or  DeleteObjects announce and end
              download                 each entry
```

Execution is one composite transfer owned by the scheduler. Every child transfer
attaches to that transfer.

A work item is work the scheduler runs where it can await. Dispatching a work
item hands it to the scheduler, and the scheduler charges the parent transfer.
Reaping a child collects a finished child and records what it moved. Reaping
waits for the child, so it is a work item too.

The comparison decides. Execution carries the decision out. The comparison read
the size and timestamp before it handed the key to execution, so execution does
not read either again.

## 1. Scope, and why it is this

Execution runs the comparison's per-key decisions as one scheduled transfer.
The run shares the client concurrency budget. It overlaps comparison, deletes
only keys marked absent, and records one result per key.

**One scheduled transfer.** A sync can issue a million child transfers. The
scheduler sees the sync as one operation. A top-level transfer per key gives
every child its own budget share and lets one sync crowd other operations.

**The run overlaps comparison.** Each small file costs a round trip. A serial
run waits for that round trip before it asks for the next decision. Pending
uploads hide the wait while the walk and listing produce more keys. A large file
hides the same wait inside multipart concurrency.

**Deletes can destroy data.** A failed transfer costs a retry. A wrong delete
costs the object. Execution sends a delete only when the comparison marked the
source key absent.

Execution meets the **FR-Exec** requirements in [`functional-spec.md`](functional-spec.md).
Section 2 names where each **FR-Exec** requirement attaches.

## 2. Design decisions

**D1. Sync is one composite transfer. Every child and work item wakes it.**
The scheduler creates every child with the parent's id. The scheduler budget
therefore applies to the run as a whole. A child reaching its terminal status
signals the parent.

A `Pending` transfer leaves the scheduler's ready set. Finishing a work item
does not poll its parent. Every dispatched work item signals the parent after it
changes state or finishes. A merge advance, a reap, and a delete batch each do
this. A missing signal leaves the run pending with work it cannot observe.

`upload_objects` and `download_objects` follow the same rule. They signal after
they publish work while their walks continue. Sync needs that behavior for both
walks: a merge can produce keys, signal the parent, and keep comparing.

**D2. Sync implements `Transfer`. `poll_work` selects work. `execute` waits.**
The `Transfer` trait is the scheduler's contract with an operation. It has four
methods:

```rust
pub(crate) trait Transfer: Send + Sync + std::fmt::Debug {
    fn ctx(&self) -> &TransferContext;
    fn poll_work(&self) -> PollWork;
    fn execute<'a>(&'a self, work: &'a mut IoRequest)
        -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;
    fn on_terminal(&self) {}
}
```

`poll_work` stays synchronous. `execute` advances either walk, joins a child,
or sends a delete. The poll has four answers:

1. A work item goes to the scheduler.
2. A child enters the scheduler.
3. The transfer waits for a signal.
4. Sync reports that the transfer finished.

A spawn sends no work item, so it does not use a scheduler slot. When the
scheduler later runs child work, the child uses one slot. The scheduler still
charges the parent, so the parent cannot outrun its children. D1's wake rule
applies to the third answer.

Merge advance and child reap are work items because both wait. One bound covers
the merge batch. A merge batch draws an unknown number of entries from either
side, so a per-side bound cannot describe the batch. The bound counts pairings
and limits how long a work item holds an executor slot.

The loop uses source and destination as roles. The caller supplies two
direction-specific parts: the child factory and the comparison rule. Upload and
download differ there. One loop keeps their completion condition the same.

**D3. A run ends after every scheduled item returns.** Reaping moves a
finished child from the child list into a work item. The merge can exhaust both
walks. The reap can empty the child list while it still holds a child's result.
The run stays open until the reap returns.

One run reported thirty-nine of forty files sent. Forty `PutObject` calls had
finished. Every file arrived, but one reap had not recorded its result. A caller
cannot tell that gap from a file that never arrived.

Stopping and finishing are separate facts. Stopping blocks new work. Finishing
says every scheduled item returned. The scheduler never polls a finished run, so
the run marks itself finished last.

**D4. One failure policy governs execution failures. `Continue` is the
default.** **FR-Fail-9** names five sites:

1. The walk cannot read a listed entry.
2. The walk cannot read a root directory.
3. S3 does not return a listing page.
4. A child transfer fails.
5. A destination refuses a delete.

`Continue` records a failure and lets the run carry on. A later run can finish
work that the failed run left behind. `Abort` records the failure and stops new
work.

An unreadable root ends its walk. `Walk::next` then returns `None`, so no stream
remains to continue. **FR-Fail-6** requires that result for a bad root.

A service error part way through a listing ends the run after the retry loop
uses all its attempts. **FR-Fail-7** keeps unread destination keys out of delete
planning. The next run lists those keys again. Recording the unread range as
unknown would keep more progress, but the walk needs a different fatal-error
model before execution can do that.

A per-key delete refusal reaches the run as a named outcome. The same policy
then decides whether the run continues or stops (**FR-Exec-6**).

**D5. A delete batch holds at most 1,000 keys and reports every key.**
`DeleteObjects` accepts 1,000 keys in one request. A batch of 1,000 saves 999
round trips and 999 scheduler dispatches. S3 can remove some keys and refuse
others in the same request, so the run records an outcome for every key
(**FR-Exec-6**).

A pending batch has two endings:

| run ending | pending batch |
| --- | --- |
| The merge finishes normally. | The run sends the batch. |
| A walk fails, the policy aborts, or the caller cancels. | The run drops the batch. |

An early failure leaves a hole in the source stream. A key looks absent only
because the merge passed its position. Sending a pending batch could delete a
file the source still has. The next run lists those keys again and decides them
from a complete view.

A per-key refusal returns with the other keys S3 still refuses. The next request
contains only those keys. S3 can receive a request while the client loses the response. The
run retries because it cannot tell that case from a request that never arrived.
A versioned bucket can receive a second delete marker. An unversioned bucket
still leaves the object absent.

A delete batch uses one scheduler request slot and one round trip. It needs no
separate cost class.

**D6. Sync records every key outcome. Per-entry transfer events and private
diagnostics carry outcomes to callers.** D3 already tracks work that started, work that
returned, and work still outstanding. **FR-Exec-6** needs one outcome
record for each key.

A fatal walk error shows why the record belongs to sync. `upload_objects`
returns the walk error and drops the successes that came before it. Sync keeps
each completed key in its own record, so a later run result does not erase an
earlier success.

The transfer event design needs three settlement rules:

1. A skip settles when sync announces it.
2. A delete has one entry for every key in its batch.
3. A dropped batch settles every entry it already announced.

Per-entry transfer events own the public record. Execution owns the adapter
from the comparison's `Decision` to that record. `TODO(sync)` marks the adapter
at the decision-to-work boundary.

Private diagnostics see three new facts. `DeleteObjects` is a request the
composite sends itself. A pending run waits on one of two sides. A million-key
run uses counters and a bounded failure sample. Sync counts the remaining
failures.

**D7. Requirements attach at three execution boundaries.**

1. **Child creation.** User metadata, ACLs, encryption settings, storage class,
   content type, and the other object properties attach where an entry becomes a
   child transfer. **FR-Exec-8**, **FR-Exec-9**, **FR-Exec-10**, and
   **FR-Exec-15** through **FR-Exec-19** use that boundary.
2. **The local tree.** The download spawner creates the directories it needs
   (**FR-Exec-3**). Pruning empty directories (**FR-Exec-4**) needs a record of
   which directories ended empty. That record exists only after every delete in
   the subtree returns.
3. **Sync-owned requests.** **FR-Exec-20** retries listing and deleting after
   throttling or transient transport failure. Child requests use the retry logic
   in their SDK client. A per-key delete refusal follows D5 because S3 returned
   it inside a successful batch response.

**D8. The download stamps the temporary file before it renames the file.**
`ExactTimestamps` compares the object's time with the file's time. A downloaded
file needs the object's time or the next run sends it again.

The managed download handle owns the temporary path, the destination path, and
the rename. It stamps the temporary file before the rename, so the destination
appears with the object's time.

Stamping is best effort. A timestamp error cannot discard bytes that already
arrived. The managed handle removes the temporary file after a failed download
or a dropped handle, and it preserves the existing destination file.

A filesystem can clamp a far-future timestamp. The download still completes. An
exact-timestamp run can download the file again because the stored time differs
from the object time.

**D9. One child wrapper hides upload and download handles.** An upload join
returns an `UploadOutput`. A download join returns a `DownloadOutput`. Execution
asks both handles two questions: did the child succeed, and how many bytes did
it move.

`TransferMetrics` carries moved bytes on both outputs. The wrapper exposes the
two common facts. Reap code then treats every child the same way.

The wrapper maps the two output types once. The reap path does not need a match
for upload and download at every call site.

**D10. Cancellation stops new work and still answers the caller.** A cancelled
run starts no new children and drops its pending delete batch. The terminal path
still answers the caller after outstanding scheduled work returns.

The scheduler decides whether a child stops that already started. **FR-Exec-23**
limits execution to three actions: stop spawning, drop the pending delete batch,
and answer the caller.

## 3. How the pieces fit

### The problem

The comparison produces one decision at a time. Execution turns each decision into
that work. The scheduler has two constraints: `poll_work` selects work without
waiting, and the run stays open until every scheduled item returns.

### What a decision becomes

The comparison returns three decisions:

1. `Transfer` creates one child transfer and returns `Spawned`.
2. `Delete` adds one key to the pending batch. The batch returns `Ready` when
   it fills or the merge ends.
3. `Skip` records one skipped key and creates no new work.

`DeleteObjects` takes 1,000 keys in one request. `PutObject` and `GetObject`
move one object in each request. Transfers use child concurrency. Deletes use
batching.

`Spawned` creates a child the scheduler polls later. `Ready` carries work that
sync performs. The child uses a scheduler slot when the scheduler polls it. The
delete batch uses a slot when sync dispatches the batch.

`Deferred` is a placeholder for checksum comparison. A checksum comparison can
read bytes or ask S3 for one object, so it may need another turn before it
decides. No shipped comparison returns that verdict. Execution records a
deferred key as skipped and marks the plan incomplete. `Walk` records unread
stream errors. Execution records deferred comparison results. The run reports
both plan holes.

### What a spawned child is

D2 supplies the child factory for each direction. The factory creates an upload
child or a download child and gives it the parent's transfer id. A child terminal
then signals the parent, and cancellation reaches the child through the parent.

The scheduler owns a child after execution enqueues it. The child has its own
scheduling record and uses its own scheduler slot. Execution keeps the D9
wrapper for the child's result and bytes moved.

Each operation owns its work shapes. An upload child can send one object or run
a multipart upload. A download child can discover the object, fetch a range, or
drain bytes already in memory. Execution names the object and starts the child.

### One possible run

Assume `a.txt` and `b.txt` need transfers, `c.txt` compares equal, and `d.txt`
needs deletion. The child cap is two. The table shows states a four-key run can reach:

| Condition | Execution action | Run state |
| --- | --- | --- |
| `a.txt` and `b.txt` need transfer. | Spawn their children. | The child cap is full. |
| `c.txt` compares equal. | Record a skip. | No scheduled work starts. |
| `d.txt` is destination-only. | Add `d.txt` to the batch. | The batch holds `d.txt`. |
| Both children reach a terminal status. | Dispatch their reap. | The reap holds both results. |
| The reap holds both results. | Return `Pending`. | D3 keeps the run open. |
| The merge ends; the batch is ready. | Dispatch `d.txt` delete. | The delete is outstanding. |
| The reap and delete work return. | Report `Done`. | No scheduled work remains. |

### Dry-run

Dry-run is outside this design. A separate design defines how dry-run reports
decisions without starting transfers or deletes.
