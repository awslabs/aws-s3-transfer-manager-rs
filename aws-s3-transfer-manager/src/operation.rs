/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

/// Types for single object upload operation
pub mod upload;

/// Types for single object download operation
pub mod download;

/// Types for multiple object download operation
pub mod download_objects;

/// Container for maintaining context required to carry out a single operation/transfer.
///
/// `State` is whatever additional operation specific state is required for the operation.
#[derive(Debug)]
pub(crate) struct TransferContext<State> {
    handle: Arc<crate::client::Handle>,
    state: Arc<State>,
}

impl<State> TransferContext<State> {
    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }
}

impl<State> Clone for TransferContext<State> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            state: self.state.clone(),
        }
    }
}
