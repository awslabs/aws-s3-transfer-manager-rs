/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

/// Shared context used across a single download request
#[derive(Debug, Clone)]
pub(crate) struct DownloadContext {
    /// reference to client handle used to do actual work
    pub(crate) handle: Arc<crate::client::Handle>,
    pub(crate) target_part_size_bytes: u64,
}

impl DownloadContext {
    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }
}
