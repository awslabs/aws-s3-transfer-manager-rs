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
    pub(crate) object_meta_receiver: Mutex<Option<Receiver<ObjectMetadata>>>,
    /// Object metadata.
    pub(crate) object_meta: OnceCell<ObjectMetadata>,

    /// The object content
    pub(crate) body: Body,

    /// Discovery task
    pub(crate) discovery: task::JoinHandle<Result<(), error::Error>>,

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

    /// Object content
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Mutable reference to the body
    pub fn body_mut(&mut self) -> &mut Body {
        &mut self.body
    }

    /// Consume the handle and wait for download transfer to complete
    #[tracing::instrument(skip_all, level = "debug", name = "join-download")]
    pub async fn join(mut self) -> Result<(), crate::error::Error> {
        self.body.close();

        self.discovery.await??;
        let mut tasks = self.tasks.lock().await;
        while let Some(join_result) = tasks.join_next().await {
            join_result?;
        }
        Ok(())
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
