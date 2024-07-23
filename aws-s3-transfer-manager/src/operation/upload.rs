/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

/// Request type for uploads to Amazon S3
pub use self::input::{UploadInput, UploadInputBuilder};

mod output;

/// Response type for uploads to Amazon S3
pub use self::output::{UploadOutput, UploadOutputBuilder};
