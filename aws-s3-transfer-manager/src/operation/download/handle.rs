/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::operation::download::body::Body;
use crate::operation::download::object_meta::ObjectMetadata;
use tokio::task;

use super::DownloadContext;

/// Response type for a single download object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// Object metadata
    pub object_meta: ObjectMetadata,

    /// The object content
    pub(crate) body: Body,

    /// All child tasks spawned for this download
    pub(crate) tasks: task::JoinSet<()>,

    /// The context used to drive an upload to completion
    pub(crate) ctx: DownloadContext,
}

impl DownloadHandle {
    /// Object metadata
    pub fn object_meta(&self) -> &ObjectMetadata {
        &self.object_meta
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
    pub async fn join(mut self) -> Result<(), crate::error::Error> {
        self.body.close();
        while let Some(join_result) = self.tasks.join_next().await {
            join_result?;
        }
        Ok(())
    }
}
