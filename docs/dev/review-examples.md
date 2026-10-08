# Review examples

These worked examples apply the questions in [reviewing.md](reviewing.md). Each finding cites the question it answers.

## Sync execution (#202)

The change added sync execution as one 5,600-line `operation/sync/transfer.rs`. A correctness-first review produced about twenty findings and draft line comments. Most of those comments targeted code that the structural review later moved.

The structural pass found the following:

- **Copies (Q1).** `upload_objects` and `download_objects` already held near-identical child-reservation and reaping code. Sync added a third copy, without the RAII guards the other two use.
- **Differences (Q2).** Comparing the two existing copies showed that they handle abort differently. `upload_objects` reports `Done` while children are still live. That made the extraction a design question rather than a silent behavior change.
- **Framework data (Q3).** The first sketch of a shared child trait carried its own byte count. Asking whether `TransferContext` already held that count reduced the trait to `ctx()` and `join()`. It also exposed a defect: sync never recorded child metrics on its own context.
- **Placement (Q5).** The shared child bookkeeping went into the transfer layer, as `transfer/composite.rs`, rather than into `operation/`. It completes the parent/child contract in `transfer.rs`, and depends only on transfer-layer types.
- **Existing helpers (Q1).** A proposed new `download/to_path.rs` duplicated `Download::orchestrate_to_path`, and the temporary-file naming was already written three times. The child path became a sibling function in `download.rs`.
- **Granularity (Q8, Q10).** A proposal of about twelve files was reduced to six that match the sibling operations, plus `test_util.rs`.
- **Names (Q13, Q14).** `report.rs` collided with the caller-facing meaning of "report". `tally.rs` named counters that were already fields of the run's `State`. The module became `state.rs`.
- **Policy (Q11).** A `room(max)` method became `try_reserve() -> Option<Reservation>`, with capacity fixed at construction.

The review was posted as one restructure proposal with this landing order:

1. move-only changes;
2. shared helpers;
3. the extraction, proven on the most-tested operation;
4. behavior changes.

Four design questions were posted inline. Line-level findings were held for the second pass.

## Transfer events (#197)

- **Existing types (Q1, Q15).** A new `StallReason` duplicated the crate-private `PendingReason` and was dropped in its favor. Two total types that differed only in their unit became one `ProvisionalCount<T>`, with aliases for each unit. Separate id and parent fields became `TransferId`.
- **Existing mechanism (Q4).** A new `bytes_streamed` counter was weighed against moving the existing `record_io` call site.
- **Speculative surface (Q12).** A variant-specific accessor was kept off the shared event type.
- **Completion (Q6).** Renaming a finished download into place moved into the state machine's definition of done.
