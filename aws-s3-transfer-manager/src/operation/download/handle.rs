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
use crate::operation::download::output::DownloadOutput;

use super::object_meta::ObjectMetadata;

/// Response type for a single download object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// Object metadata. TODO: Is there a better way to do this than tokio oncecell?
    pub(crate) object_meta_receiver: Option<Receiver<ObjectMetadata>>,
    pub(crate) object_meta: OnceCell<Result<ObjectMetadata, error::Error>>,

    /// The object content
    pub(crate) body: DownloadOutput,

    /// Discovery task
    pub(crate) discovery: task::JoinHandle<Result<(), error::Error>>,

    /// All child tasks spawned for this download
    pub(crate) tasks: Arc<Mutex<task::JoinSet<()>>>,
    // /// The context used to drive an upload to completion
    // pub(crate) ctx: DownloadContext,
}

impl DownloadHandle {
    /// Object metadata
    pub async fn object_meta(&mut self) -> &Result<ObjectMetadata, error::Error> {
        let meta = self
            .object_meta
            .get_or_init(|| async {
                let meta = self.object_meta_receiver.take().unwrap();
                meta.await
                    .map_err(error::from_kind(ErrorKind::RuntimeError))
            })
            .await;

        meta
    }

    /// Object content
    pub fn body(&self) -> &DownloadOutput {
        &self.body
    }

    /// Mutable reference to the body
    pub fn body_mut(&mut self) -> &mut DownloadOutput {
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
