/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::sync::Arc;

use crate::error::{self, ErrorKind};
use tokio::{
    sync::{oneshot::Receiver, Mutex, OnceCell},
    task,
};

use crate::operation::download::body::Body;

use super::trailing_meta::TrailingMetadataOnceLock;
use super::{ObjectMetadata, TrailingMetadata};

/// Response type for a single download object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// Object metadata receiver.
    pub(crate) object_meta_receiver: Mutex<Option<Receiver<ObjectMetadata>>>,
    /// Object metadata.
    pub(crate) object_meta: OnceCell<ObjectMetadata>,

    /// Metadata that isn't available until the download completes.
    /// This is similar to object_meta, in that it's transmitted once to the DownloadHandle.
    /// But we can simply use an Arc<OnceLock> (instead of Mutex/Channel/Option/OnceCell combo)
    /// because we never await it.
    pub(crate) trailing_meta: TrailingMetadataOnceLock,

    /// The object content, in chunks, and the metadata for each chunk
    pub(crate) body: Body,

    /// Discovery task
    pub(crate) discovery: task::JoinHandle<()>,

    /// All child tasks (ranged GetObject) spawned for this download
    pub(crate) tasks: Arc<Mutex<task::JoinSet<()>>>,
}

impl DownloadHandle {
    /// Object metadata
    pub async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        let meta = self
            .object_meta
            .get_or_try_init(|| async {
                let mut object_meta_receiver = self.object_meta_receiver.lock().await;
                let object_meta_receiver = object_meta_receiver
                    .take()
                    .ok_or("meta_receiver is already taken")
                    .map_err(error::from_kind(ErrorKind::ObjectNotDiscoverable))?;
                object_meta_receiver
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

    /// Get metadata about the completed download.
    /// This won't return `Some` until all [`body`] chunks have been successfully received.
    ///
    /// [`body`]: method@Self::body
    pub fn trailing_meta(&self) -> Option<&TrailingMetadata> {
        self.trailing_meta.get()
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
