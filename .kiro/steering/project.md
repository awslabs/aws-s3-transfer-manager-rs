# S3 Transfer Manager - Project Context

## Overview

High-performance S3 Transfer Manager in Rust. The `s3-tm-redux` branch contains a major
architectural redesign with a new event-driven scheduler replacing the previous implementation.

## Repository Structure

```
aws-sdk-s3-transfer-manager/     # Main crate
  src/
    client.rs                    # Client, Handle (owns scheduler + S3 client)
    config.rs                    # Configuration
    operation.rs                 # TransferContext, shared transfer infrastructure
    operation/
      upload/                    # Upload state machine (transfer.rs, handle.rs, context.rs)
      download/                  # Download state machine (transfer.rs, handle.rs, body.rs)
      upload_objects/            # Batch uploads (legacy scheduler, migration pending)
      download_objects/          # Batch downloads (legacy scheduler, migration pending)
    scheduler/
      scheduler.rs               # Core scheduler: CFS fairness, worker pool, work generation
      transfer.rs                # Transfer enum (Upload/Download/Mock dispatch)
      descriptor.rs              # TransferDescriptor: priority, vruntime, work tracking
      ready_set.rs               # ReadySet: SkipMap-based priority queue
      work/
        item.rs                  # WorkItem, WorkData, WorkKind, PollWork, WorkOutcome
        pool.rs                  # WorkerPool: workers pull work from queue
        queue.rs                 # WorkQueue: bounded concurrent queue
    runtime/
      scheduler.rs               # Legacy scheduler (token-bucket, used by *_objects operations)
s3-mock-server/                  # Mock S3 server for testing
```

## Key Patterns

**Scheduler-driven execution**: Transfers produce work lazily via `poll_work()`. Workers pull
work from a shared pool. The scheduler controls admission via CFS fairness and capacity gating.

**State machine transfers**: Upload and Download are state machines that generate `WorkItem`s.
The scheduler is agnostic to what transfers do — it manages priority, capacity, and lifecycle.

**Follow-on work**: A work item can produce a successor (e.g., disk read → network send).
Follow-on work bypasses CFS and goes directly to the worker pool.

**TransferContext**: Shared infrastructure for all transfers — ID, status, cancellation token,
error storage, completion signaling.

## Building and Testing

```bash
cargo test --workspace                    # All tests
cargo test --workspace --all-features     # With all features
cargo clippy --workspace --all-targets    # Lint
cargo fmt --all -- --check                # Format check
```

## Current State (s3-tm-redux branch)

- New scheduler: CFS fairness, priority API, single worker pool
- Upload: PutObject + multipart (CreateMPU → UploadPart → CompleteMPU)
- Download: Discovery → ranged GETs with SeqWindow backpressure
- Legacy scheduler still used by upload_objects/download_objects
