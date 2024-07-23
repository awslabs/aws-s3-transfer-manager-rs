/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

use std::sync::Arc;

/// Request type for uploads to Amazon S3
pub use self::input::{UploadInput, UploadInputBuilder};

mod output;

/// Response type for uploads to Amazon S3
pub use self::output::{UploadOutput, UploadOutputBuilder};

/// Operation builders
pub mod builders;

pub(crate) mod context;
pub(crate) mod handle;
pub use self::handle::UploadHandle;

use crate::error::UploadError;


/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
struct Upload;


impl Upload {
    
    /// Execute a single `Upload` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::upload::UploadInput        
    ) -> Result<UploadHandle, UploadError> {
        unimplemented!()
    }
}

