/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::sync::{mpsc, Mutex, OnceCell};

use crate::error::{self, ErrorKind};
use crate::operation::download::body::Body;
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::ChunkOutput;
use crate::operation::download::DownloadContext;

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

    /// Chunk receiver - kept to pass to body
    pub(crate) chunk_rx: Mutex<Option<mpsc::Receiver<Result<ChunkOutput, error::Error>>>>,
}

impl DownloadHandle {
    pub(crate) fn new(
        ctx: DownloadContext,
        chunk_rx: mpsc::Receiver<Result<ChunkOutput, error::Error>>,
    ) -> Self {
        Self {
            body: Body::new(chunk_rx, ctx.clone()),
            object_meta: OnceCell::new(),
            ctx,
            chunk_rx: Mutex::new(None),
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

    /// Abort the download and cancel any in-progress work.
    pub async fn abort(mut self) {
        self.ctx.cancel();
        self.body.close();
        // TODO(redux): Cancel via scheduler
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
