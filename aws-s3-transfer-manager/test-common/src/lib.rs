/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(target_family = "unix")]

use std::{fs, io::Write};
use tempfile::{tempdir, TempDir};

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
) -> TempDir {
    let temp_dir = match recursion_root {
        Some(root) => TempDir::with_prefix(root).unwrap(),
        None => tempdir().unwrap(),
    };

    // Create the directory structure and files
    for (path, size) in files {
        let full_path = temp_dir.path().join(path);
        let parent = full_path.parent().unwrap();

        // Create the parent directories if they don't exist
        fs::create_dir_all(parent).unwrap();

        // Create the file with the specified size
        let mut file = fs::File::create(&full_path).unwrap();
        file.write_all(&vec![0; size]).unwrap(); // Writing `size` byte
    }

    // Set the directories in `inaccessible_dir_relative_paths` to be inaccessible,
    // which will in turn render the files within those directories inaccessible
    for dir_relative_path in inaccessible_dir_relative_paths {
        let dir_path = temp_dir.path().join(*dir_relative_path);
        let mut permissions = fs::metadata(&dir_path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o000); // No permissions for anyone
        fs::set_permissions(dir_path, permissions).unwrap();
    }

    temp_dir
}
