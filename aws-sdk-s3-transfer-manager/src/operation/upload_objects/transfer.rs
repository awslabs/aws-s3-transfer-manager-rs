/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! State machine for plural upload (`upload_objects`).

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::FailedUpload;

/// Work data variants for the UploadObjectsTransfer state machine.
///
/// Currently a single variant; reserved as an enum for future expansion
/// (e.g. explicit drain-signal work, progress-snapshot work).
#[derive(Debug)]
#[allow(dead_code)] // variants wired when state machine is driven
pub(crate) enum UploadObjectsWork {
    /// Pull the next entry from the walker channel and initiate a child
    /// upload transfer for it.
    AdvanceWalker,
}

/// State for the UploadObjectsTransfer state machine.
#[derive(Debug)]
#[allow(dead_code)] // variants driven by poll_work state transitions
pub(crate) enum UploadObjectsState {
    /// Actively pulling entries from the walker and initiating children.
    Enumerating,
    /// Walker exhausted or transfer cancelled; waiting for registered children
    /// to reach terminal state. No new children will be initiated.
    Draining,
    /// All work complete; `poll_work` returns `Done`.
    Complete,
}

/// Metadata tracked per registered child transfer.
///
/// Kept alive by the parent until the child reaches terminal state so
/// cancellation of the parent can cascade to the child, and the child's
/// outcome can be correlated back to the source path / key on failure.
#[derive(Debug)]
#[allow(dead_code)] // constructed by AdvanceWalker work items
pub(crate) struct ChildMeta {
    pub(crate) source_path: std::path::PathBuf,
    pub(crate) key: String,
    /// Holding the handle alive ensures the child continues to run until it
    /// terminates; dropping it signals cancellation to the child.
    pub(crate) handle: crate::operation::upload::UploadHandle,
}

/// Parent state machine for plural upload (`upload_objects`).
///
/// Orchestrates per-entry singular [`crate::operation::upload::UploadTransfer`]
/// children over the scheduler. Each walker entry becomes one child upload
/// transfer, registered and tracked until terminal.
///
/// Cheap to clone; all state behind `Arc`.
#[derive(Clone)]
#[allow(dead_code)] // wired up by the operation orchestrator
pub(crate) struct UploadObjectsTransfer {
    inner: Arc<UploadObjectsTransferInner>,
}

/// Shared interior state for [`UploadObjectsTransfer`].
#[allow(dead_code)] // wired up by the operation orchestrator
pub(crate) struct UploadObjectsTransferInner {
    /// Common transfer lifecycle management (id, handle, status, cancellation).
    ctx: TransferContext,
    /// Immutable request parameters.
    request: Arc<super::UploadObjectsInput>,
    /// State machine phase.
    state: Mutex<UploadObjectsState>,
    /// Maximum concurrent child transfers. Adjustable at runtime.
    pipeline_depth: AtomicUsize,
    /// Currently-registered children (by `TransferId`).
    children: Mutex<HashMap<TransferId, ChildMeta>>,
    /// Per-entry failures accumulated under `FailedTransferPolicy::Continue`.
    failed: Mutex<Vec<FailedUpload>>,
    /// Monotonic counter of successful child uploads.
    successful_uploads: AtomicU64,
    /// Total bytes transferred across all successful children.
    total_bytes_transferred: AtomicU64,
}

#[allow(dead_code)] // wired up by the operation orchestrator
impl UploadObjectsTransfer {
    /// Create a new transfer with the given context and request parameters.
    pub(crate) fn new(ctx: TransferContext, request: super::UploadObjectsInput) -> Self {
        let pipeline_depth = request.pipeline_depth();
        let inner = Arc::new(UploadObjectsTransferInner {
            ctx,
            request: Arc::new(request),
            state: Mutex::new(UploadObjectsState::Enumerating),
            pipeline_depth: AtomicUsize::new(pipeline_depth),
            children: Mutex::new(HashMap::new()),
            failed: Mutex::new(Vec::new()),
            successful_uploads: AtomicU64::new(0),
            total_bytes_transferred: AtomicU64::new(0),
        });
        Self { inner }
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    /// Current pipeline depth (max registered children simultaneously).
    pub(crate) fn pipeline_depth(&self) -> usize {
        self.inner.pipeline_depth.load(Ordering::Acquire)
    }

    /// Adjust pipeline depth at runtime. Takes effect at the next enumeration cycle.
    pub(crate) fn set_pipeline_depth(&self, depth: usize) {
        self.inner.pipeline_depth.store(depth, Ordering::Release);
        // Wake if we were throttled waiting for capacity — harmless otherwise.
        self.inner.ctx.try_wake();
    }

    /// Number of child uploads that completed successfully.
    pub(crate) fn successful_uploads(&self) -> u64 {
        self.inner.successful_uploads.load(Ordering::Acquire)
    }

    /// Cumulative bytes transferred across all successful children.
    pub(crate) fn total_bytes_transferred(&self) -> u64 {
        self.inner.total_bytes_transferred.load(Ordering::Acquire)
    }
}

impl fmt::Debug for UploadObjectsTransfer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadObjectsTransfer")
            .field("id", &self.inner.ctx.id)
            .field("pipeline_depth", &self.pipeline_depth())
            .field("successful_uploads", &self.successful_uploads())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for UploadObjectsTransferInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadObjectsTransferInner")
            .field("ctx", &self.ctx)
            .field(
                "pipeline_depth",
                &self.pipeline_depth.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Transfer for UploadObjectsTransfer {
    fn ctx(&self) -> &TransferContext {
        UploadObjectsTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            *state = UploadObjectsState::Complete;
            self.inner.ctx.signal_terminal();
        }
        PollWork::Done
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            unreachable!("UploadObjectsTransfer::execute called but poll_work never returns Ready")
        })
    }

    fn on_terminal(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::TransferContext;
    use crate::DEFAULT_CONCURRENCY;

    fn create_test_transfer(input: super::super::UploadObjectsInput) -> UploadObjectsTransfer {
        let config = crate::Config::builder()
            .client(aws_smithy_mocks::mock_client!(aws_sdk_s3, []))
            .build();
        let handle = crate::client::Handle::new_for_test(config, DEFAULT_CONCURRENCY);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        UploadObjectsTransfer::new(ctx, input)
    }

    fn default_input() -> super::super::UploadObjectsInput {
        super::super::UploadObjectsInputBuilder::default()
            .bucket("test-bucket")
            .source("/tmp/test-source")
            .build()
            .unwrap()
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_constructor_default_pipeline_depth_is_100() {
        let transfer = create_test_transfer(default_input());
        assert_eq!(transfer.pipeline_depth(), 100);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_constructor_explicit_pipeline_depth() {
        let input = super::super::UploadObjectsInputBuilder::default()
            .bucket("test-bucket")
            .source("/tmp/test-source")
            .pipeline_depth(42)
            .build()
            .unwrap();
        let transfer = create_test_transfer(input);
        assert_eq!(transfer.pipeline_depth(), 42);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_set_pipeline_depth_updates_value() {
        let transfer = create_test_transfer(default_input());
        assert_eq!(transfer.pipeline_depth(), 100);
        transfer.set_pipeline_depth(250);
        assert_eq!(transfer.pipeline_depth(), 250);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_poll_work_returns_done_when_constructed() {
        let transfer = create_test_transfer(default_input());
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_debug_format_does_not_panic() {
        let transfer = create_test_transfer(default_input());
        let _ = format!("{:?}", transfer);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_successful_uploads_and_bytes_start_at_zero() {
        let transfer = create_test_transfer(default_input());
        assert_eq!(transfer.successful_uploads(), 0);
        assert_eq!(transfer.total_bytes_transferred(), 0);
    }
}
