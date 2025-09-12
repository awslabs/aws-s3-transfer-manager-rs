use std::sync::Arc;

use crate::error::{self, ErrorKind};
use tokio::{
    sync::{oneshot::Receiver, Mutex, OnceCell},
    task,
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

    /// Client handle for metrics access
    pub(crate) handle: Arc<crate::client::Handle>,
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

    /// Abort the download and cancel any in-progress work.
    pub async fn abort(mut self) {
        self.body.close();
        self.discovery.abort();
        let _ = self.discovery.await;
        // It's safe to grab the lock here because discovery is already complete, and we will never
        // lock tasks again after discovery to spawn more tasks.
        let mut tasks = self.tasks.lock().await;
        tasks.abort_all();
        while (tasks.join_next().await).is_some() {}
    }

    /// Wait for the download to complete and track completion metrics.
    pub async fn join(self) -> Result<(), error::Error> {
        // Wait for discovery to complete
        if let Err(e) = self.discovery.await {
            self.handle.metrics.increment_transfers_failed();
            return Err(error::from_kind(ErrorKind::RuntimeError)(format!(
                "Discovery task failed: {}",
                e
            )));
        }

        // Wait for all download tasks to complete
        let mut tasks = self.tasks.lock().await;
        let mut has_error = false;

        while let Some(result) = tasks.join_next().await {
            if result.is_err() {
                has_error = true;
            }
        }

        if has_error {
            self.handle.metrics.increment_transfers_failed();
            Err(error::from_kind(ErrorKind::RuntimeError)(
                "One or more download tasks failed",
            ))
        } else {
            self.handle.metrics.increment_transfers_completed();
            Ok(())
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
