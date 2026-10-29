# Architecture

This document maps the `aws-sdk-s3-transfer-manager` crate and states the boundaries and conventions that hold across its modules. Each module's documentation covers that module's internals. The [design documents](../design/README.md) cover each subsystem's requirements and invariants. Paths are relative to `aws-sdk-s3-transfer-manager/src/`.

## Codemap

A client operation builds a transfer and enqueues it with the scheduler. The scheduler polls the transfer's state machine for work and submits the work to the runtime, which executes it on managed threads. Payload memory comes from the shared buffer pool.

- **`operation/`** (public): one module per client operation. It holds the builders, the input and output types, the caller's handle, and the operation's `Transfer` implementation.
- **`transfer.rs`** (crate-private): what a transfer is. It holds the `Transfer` trait, `TransferContext` (id, status, metrics, and the pending/wake protocol), and the parent/child contract that composite transfers use.
- **`scheduler/`** (crate-private): work generation, the ready set, fair scheduling, concurrency control, and the gate. Its module documentation states the contracts that every `Transfer` implementation upholds.
- **`runtime/`** (crate-private): managed threads, platform binding, request dispatch, and the buffer pool. `runtime/sync` is the synchronization layer that Loom models.
- **`io/`** (public): input streams, part reading, and the filesystem and S3 walkers.
- **`retry.rs`, `metrics/`, `config/`, `error.rs`**: cross-cutting support.

## Reusable components

`crates/aws-smithy-topology/` provides CPU and NUMA discovery, placement targets,
and explicit CPU binding independently of transfer scheduling. Discovery owns
no threads and does not change affinity. Its crate documentation states the
resource and platform contracts.

`crates/aws-smithy-execution/` owns managed threads and exposes partition handles
for targeted Send task submission. Its topology re-export describes placement;
its runtime owner controls shutdown. Execution handles do not retain ownership.

`crates/aws-smithy-managed-http/` combines native Smithy connection pools with
managed execution. Transport configuration remains on the native pool builder;
the wrapper retains execution handles and native clients without owning runtime
shutdown. It supplies Smithy's HTTP client interface independently of TM.

## Boundaries

- `transfer.rs`, `scheduler/`, and `runtime/` do not depend on `operation/`. Operation-specific behavior reaches them only through the `Transfer` trait.
- Synchronization primitives that Loom must model come from `runtime/sync`, not from `std` directly.
- Every transfer carries a `TransferContext`. Code that needs a transfer's id, status, or byte counts reads them from the context and does not keep its own copy. A composite transfer records each child's metrics on its own context when it reaps the child.
- A transfer is complete when its state machine reaches a terminal state. Finalization belongs to that transition, not to the caller's handle. Examples are renaming a download into place and waiting for destination writes.

## Conventions

- **Operation layout.** `operation/<op>.rs` declares the submodules and re-exports the public types. `operation/<op>/` contains `builders.rs`, `input.rs`, `output.rs`, `handle.rs`, and `transfer.rs`. Single-object operations add `context.rs` for per-transfer pending state. Operation-specific helpers sit beside these files. The layout applies from an operation's first change, before the operation is public.
- **Shared code placement.** Shared code goes in the lowest module that owns its concept:
  - machinery shared by transfer implementations goes in `transfer.rs`, or in submodules under `transfer/`;
  - general filesystem helpers go in `io/fs.rs`;
  - key and path derivation goes in `io/`.
- **Test support.** Test helpers live in a `test_util.rs` or `test_util/` module next to the code they support, as in `scheduler/transfer/test_util.rs`, `operation/download/test_util/`, and `runtime/buffer_pool/test_util/`. Fault injection goes through a trait seam, not through `#[cfg(test)]` variants on production types.
- **Reuse before addition.** Extend an existing type before adding a parallel one. Two types that differ only in a parameter become one generic type with aliases.
- **File size.** A file that mixes concerns is split along them, for example transitions, state, child management, and tests. Splits follow the granularity of sibling modules, not one file per type.
