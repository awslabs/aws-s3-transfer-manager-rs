/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Walker types for traversing filesystems and S3 buckets.

/// Error types for walk operations.
pub mod error;
mod fs;
mod s3;

pub use error::{WalkError, WalkErrorKind};
pub use fs::{DirEntry, FsWalk, FsWalkContext, FsWalkContextBuilder, FsWalker, FsWalkerBuilder};
pub(crate) use s3::exclude_s3_folder_markers;
pub use s3::{S3Walk, S3WalkContext, S3WalkContextBuilder, S3Walker, S3WalkerBuilder};
