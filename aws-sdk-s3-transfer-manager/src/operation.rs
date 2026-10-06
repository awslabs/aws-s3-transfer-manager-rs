/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fs::Metadata, path::Path};

use crate::error;

/// Types for single object upload operation
pub mod upload;

/// Types for single object download operation
pub mod download;

/// Types for multiple object download operation
pub mod download_objects;

/// Types for multiple object upload operation
pub mod upload_objects;

// The default delimiter of the S3 object key
pub(crate) const DEFAULT_DELIMITER: &str = "/";

/// Conservative per-transfer backstop on concurrently-materialized child
/// transfers (shared by upload_objects and download_objects).
///
/// The scheduler's hierarchical fair-share drives throughput and the walker
/// provides natural rate-limiting, so this cap primarily bounds working-set
/// memory. 512 covers the highest useful concurrency observed in practice
/// (100 Gbps links sustain ~200-250 concurrent transfers; 600 Gbps multi-NIC
/// configurations stay below 500). Callers that genuinely need more can
/// override via `max_concurrent_downloads` / `max_concurrent_uploads`.
pub(crate) const DEFAULT_MAX_CONCURRENT_CHILDREN: usize = 512;

// Checks if the target path at `path`, with the provided `metadata`, represents a directory.
//
// The caller is responsible for providing the correct `Metadata`. If the `Metadata` is obtained
// via `fs::metadata`, it can only determine whether the path is a file or a directory, but it cannot
// indicate whether the path is a symbolic link. On the other hand, if `Metadata` is obtained through
// `fs::symlink_metadata`, it can identify symbolic links, but calling `is_dir()` on a symlink will
// return false, even if the symlink points to a directory.
pub(crate) fn validate_target_is_dir(metadata: &Metadata, path: &Path) -> Result<(), error::Error> {
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(error::invalid_input(format!(
            "target is not a directory: {path:?}"
        )))
    }
}
