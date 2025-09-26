use std::sync::Arc;

use crate::{
    error::{self, ErrorKind},
    metrics::aggregators::TransferMetrics,
    operation::download::DownloadContext,
};
use tokio::{
    sync::{oneshot::Receiver, Mutex, OnceCell},
    task::{self},
};

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::operation::download::body::Body;

use super::object_meta::ObjectMetadata;

/// Response type for a single download object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// Object metadata receiver.
    pub(crate) object_meta_rx: Mutex<Option<Receiver<ObjectMetadata>>>,
    /// Object metadata.
    pub(crate) object_meta: OnceCell<ObjectMetadata>,

    /// The object content, in chunks, and the metadata for each chunk
    pub(crate) body: Body,

    /// Discovery task
    pub(crate) discovery: task::JoinHandle<()>,

    /// All child tasks (ranged GetObject) spawned for this download
    pub(crate) tasks: Arc<Mutex<task::JoinSet<()>>>,

    /// Context object for this download
    pub(crate) ctx: DownloadContext,
}

impl DownloadHandle {
    /// Object metadata
    pub async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        let meta = self
            .object_meta
            .get_or_try_init(|| async {
                let mut object_meta_rx = self.object_meta_rx.lock().await;
                let object_meta_rx = object_meta_rx
                    .take()
                    .ok_or("object_meta_rx is already taken")
                    .map_err(error::from_kind(ErrorKind::ObjectNotDiscoverable))?;
                object_meta_rx
                    .await
                    .map_err(error::from_kind(ErrorKind::ObjectNotDiscoverable))
            })
            .await?;

        Ok(meta)
    }

    /// The object content, in chunks, and the metadata for each chunk
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Mutable reference to the body
    pub fn body_mut(&mut self) -> &mut Body {
        &mut self.body
    }

    /// Metrics for this download
    pub fn transfer_metrics(&self) -> Arc<TransferMetrics> {
        self.ctx.metrics().clone()
    }

    /// Abort the download and cancel any in-progress work.
    pub async fn abort(mut self) {
        self.body.close();
        self.discovery.abort();

        let discovery = std::mem::replace(&mut self.discovery, tokio::spawn(async {}));
        let _ = discovery.await;

        // It's safe to grab the lock here because discovery is already complete, and we will never
        // lock tasks again after discovery to spawn more tasks.
        let mut tasks = self.tasks.lock().await;
        tasks.abort_all();
        while (tasks.join_next().await).is_some() {}
    }

    /// Wait for the download to complete
    pub async fn join(mut self) -> Result<(), error::Error> {
        let discovery = std::mem::replace(&mut self.discovery, tokio::spawn(async {}));

        // Wait for discovery to complete
        if let Err(e) = discovery.await {
            return Err(error::from_kind(ErrorKind::RuntimeError)(format!(
                "Discovery task failed: {}",
                e
            )));
        }

        // Wait for all download tasks to complete
        let mut tasks = self.tasks.lock().await;
        let mut has_error = false;
        let mut errors = vec![];

        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                has_error = true;
                errors.push(e);
            }
        }

        if has_error {
            Err(error::from_kind(ErrorKind::RuntimeError)(format!(
                "One or more download tasks failed: {:#?}",
                errors
            )))
        } else {
            Ok(())
        }
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        // If the body is fully processed and we did not detect any errors in any
        // of the individual tasks we record the transfer as successful. Otherwise
        // it is a failure.
        if self.body().is_processed() && !self.ctx.metrics().is_failed() {
            self.ctx.handle.metrics.increment_transfers_successful();
        } else {
            self.ctx.handle.metrics.increment_transfers_failed();
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
