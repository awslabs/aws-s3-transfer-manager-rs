/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3_transfer_manager::{error::Error, operation::download::DownloadHandle};
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::OnceLock;
use uuid::Uuid;

/// Create a directory structure rooted at `recursion_root`, containing files with sizes
/// specified in `files`
///
/// For testing purposes, certain directories (and all files within them) can be made
/// inaccessible by providing `inaccessible_dir_relative_paths`, which should be relative
/// to `recursion_root`.
pub fn create_test_dir(
    recursion_root: Option<&str>,
    files: Vec<(&str, usize)>,
    inaccessible_dir_relative_paths: &[&str],
) -> tempfile::TempDir {
    let temp_dir = match recursion_root {
        Some(root) => tempfile::TempDir::with_prefix(root).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };

    // Create the directory structure and files
    for (path, size) in files {
        let full_path = temp_dir.path().join(path);
        let parent = full_path.parent().unwrap();

        // Create the parent directories if they don't exist
        std::fs::create_dir_all(parent).unwrap();

        // Create the file with the specified size
        let mut file = std::fs::File::create(&full_path).unwrap();
        std::io::Write::write_all(&mut file, &vec![0; size]).unwrap(); // Writing `size` bytes
    }

    // Set the directories in `inaccessible_dir_relative_paths` to be inaccessible
    // which will in turn render the files within those directories inaccessible
    for dir_relative_path in inaccessible_dir_relative_paths {
        let dir_path = temp_dir.path().join(*dir_relative_path);
        make_directory_inaccessible(&dir_path);
    }

    temp_dir
}

// Platform-specific helper function to make directories inaccessible
#[cfg(target_family = "unix")]
fn make_directory_inaccessible(dir_path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(dir_path).unwrap().permissions();
    permissions.set_mode(0o000); // No permissions for anyone
    std::fs::set_permissions(dir_path, permissions).unwrap();
}

#[cfg(target_family = "windows")]
fn make_directory_inaccessible(_dir_path: &std::path::Path) {
    panic!("make_directory_inaccessible is not implemented for Windows");
}

/// drain/consume the body
pub async fn drain(handle: &mut DownloadHandle) -> Result<Bytes, Error> {
    let body = handle.body_mut();
    let mut data = BytesMut::new();
    let mut error: Option<Error> = None;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => data.put(chunk.data.into_bytes()),
            Err(err) => {
                error.get_or_insert(err);
            }
        }
    }

    if let Some(error) = error {
        return Err(error);
    }
    Ok(data.into())
}

// Generate UUID for the process to be used in tests to avoid conflicts between concurrent tests runs.
pub fn global_uuid_str() -> &'static str {
    static UUID_STR: OnceLock<String> = OnceLock::new();
    UUID_STR.get_or_init(|| Uuid::new_v4().to_string())
}
