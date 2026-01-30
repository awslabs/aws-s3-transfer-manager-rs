/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::sync::{mpsc, OnceCell};

use crate::error::{self, ErrorKind};
use crate::operation::download::body::Body;
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::output::DownloadOutput;
use crate::operation::download::ChunkOutput;
use crate::operation::download::DownloadContext;
use crate::operation::CompletionReceiver;

/// Response type for a single download object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// The object content
    pub(crate) body: Body,

    /// Object metadata (populated after discovery)
    pub(crate) object_meta: OnceCell<ObjectMetadata>,

    /// Download context
    pub(crate) ctx: DownloadContext,

    /// Completion signal receiver - signals state machine reached terminal state
    pub(crate) completion_rx: Option<CompletionReceiver>,
}

impl DownloadHandle {
    pub(crate) fn new(
        ctx: DownloadContext,
        chunk_rx: mpsc::Receiver<Result<ChunkOutput, error::Error>>,
        completion_rx: CompletionReceiver,
    ) -> Self {
        Self {
            body: Body::new(chunk_rx, ctx.clone()),
            object_meta: OnceCell::new(),
            ctx,
            completion_rx: Some(completion_rx),
        }
    }

    /// Object metadata
    ///
    /// TODO(redux): This should wait for discovery work item to complete
    pub async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        self.object_meta.get().ok_or_else(|| {
            error::from_kind(ErrorKind::ObjectNotDiscoverable)("discovery not yet implemented")
        })
    }

    /// The object content
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Mutable reference to the body
    pub fn body_mut(&mut self) -> &mut Body {
        &mut self.body
    }

    /// Wait for the download to complete and return the result.
    ///
    /// This is the authoritative source for transfer errors. Other methods
    /// (`body.next()`, `object_meta()`) return generic errors; call `join()`
    /// to get the actual error with full context.
    ///
    /// If the body has not been fully consumed, this will cancel the transfer
    /// and wait for outstanding work to complete before returning.
    ///
    /// TODO(redux): Currently, in-flight work is only cancelled when join() is called
    /// after a failure. Consider proactively cancelling on failure so resources are
    /// released without requiring user action.
    pub async fn join(mut self) -> Result<DownloadOutput, error::Error> {
        // Close body - we're done consuming (or never started)
        self.body.close();

        // Wait for transfer state machine to reach terminal state
        if let Some(rx) = self.completion_rx.take() {
            // completion_tx is sent when state machine completes (success/failure)
            // RecvError means sender dropped without signaling - treat as failure
            let _ = rx.await;
        }

        if self.ctx.is_failed() {
            self.ctx.handle.new_scheduler.cancel_transfer(self.ctx.id);
            self.ctx
                .handle
                .new_scheduler
                .wait_for_idle(self.ctx.id)
                .await;
            // take the actual error (only we should do this)
            let err = self
                .ctx
                .take_error()
                .expect("error taken outside of join()");
            return Err(err);
        }

        // Success - build output
        // TODO(redux): populate object_meta from discovery
        Ok(DownloadOutput::new(ObjectMetadata::default()))
    }

    /// Abort the download and cancel any in-progress work.
    ///
    /// When this method returns, all work for this transfer has been
    /// cancelled or completed. No further work will be executed.
    pub async fn abort(mut self) {
        self.ctx.set_cancelled();
        self.body.close();

        // Cancel transfer and purge queued work
        self.ctx.handle.new_scheduler.cancel_transfer(self.ctx.id);

        // Wait for any executing work to complete
        self.ctx
            .handle
            .new_scheduler
            .wait_for_idle(self.ctx.id)
            .await;
    }

    // TODO(redux): should have a way to get at common transfer state/context like id() -> TransferId
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        if self.ctx.is_active() {
            self.ctx.set_cancelled();
            self.ctx.handle.new_scheduler.cancel_transfer(self.ctx.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DownloadHandle;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_handle_properties() {
        is_send::<DownloadHandle>();
        is_sync::<DownloadHandle>();
    }
}
