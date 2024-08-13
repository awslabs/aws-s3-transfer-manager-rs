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
#[derive(Debug, Clone)]
pub(crate) struct TransferContext<State>
where
    State: Clone,
{
    handle: Arc<crate::client::Handle>,
    state: State,
}

impl<State> TransferContext<State>
where
    State: Clone,
{
    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }
}
