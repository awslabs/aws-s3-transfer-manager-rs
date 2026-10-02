/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Exclusive creation of the temporary file a path download writes before
//! renaming it to its destination.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// Most names tried for one temporary file.
///
/// Bounded so that creation fails, rather than retrying indefinitely, when
/// the suffix source keeps naming entries that already exist.
pub(crate) const TEMP_FILE_ATTEMPTS: usize = 3;

/// Creates a new temporary file next to `dest` and opens it for writing.
///
/// The file is named `{dest file name}.s3tmp.{suffix}`, with the suffix
/// written as 8 lower-case hex digits, in `dest`'s directory. It is opened
/// with [`create_new`](std::fs::OpenOptions::create_new), so it succeeds only
/// if no entry, including a dangling symbolic link, exists at that name. An
/// existing entry is never opened, truncated, or followed. When the name is
/// taken, the next suffix is tried, up to [`TEMP_FILE_ATTEMPTS`] suffixes in
/// total, taken in order from `suffixes`.
///
/// Returns the open file and its path. Returns
/// [`io::ErrorKind::AlreadyExists`] when every suffix tried named an existing
/// entry. Any other error from opening a candidate is returned at once.
pub(crate) fn create_temp_file(
    dest: &Path,
    suffixes: impl IntoIterator<Item = u32>,
) -> io::Result<(File, PathBuf)> {
    let file_name = dest.file_name().unwrap_or_default().to_string_lossy();
    let mut attempts = 0;
    for suffix in suffixes.into_iter().take(TEMP_FILE_ATTEMPTS) {
        attempts += 1;
        let path = dest.with_file_name(format!("{file_name}.s3tmp.{suffix:08x}"));
        match File::options().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("temporary file name already exists after {attempts} attempts"),
    ))
}

/// Runs [`create_temp_file`] on the blocking thread pool.
///
/// The first [`TEMP_FILE_ATTEMPTS`] values of `suffixes` are drawn on the
/// thread that polls this future, before the blocking task starts. A suffix
/// source backed by thread-local state, such as `fastrand`'s global
/// generator, therefore draws from the polling thread's state rather than a
/// pool thread's.
///
/// Returns the results of [`create_temp_file`], or an error if the blocking
/// task panics or is cancelled.
pub(crate) async fn create_temp_file_async(
    dest: &Path,
    suffixes: impl IntoIterator<Item = u32>,
) -> io::Result<(File, PathBuf)> {
    let suffixes: Vec<u32> = suffixes.into_iter().take(TEMP_FILE_ATTEMPTS).collect();
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || create_temp_file(&dest, suffixes)).await?
}

#[cfg(test)]
mod tests {
    use super::{create_temp_file, TEMP_FILE_ATTEMPTS};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};

    const A: u32 = 0x0000_00aa;
    const B: u32 = 0x0000_00bb;

    fn temp_name(dir: &Path, suffix: u32) -> PathBuf {
        dir.join(format!("out.dat.s3tmp.{suffix:08x}"))
    }

    /// Writes through the returned handle and checks the bytes land at the
    /// returned path, so the handle is known to refer to that file.
    fn assert_handle_writes_path(mut file: std::fs::File, path: &Path) {
        file.write_all(b"object").unwrap();
        drop(file);
        assert_eq!(std::fs::read(path).unwrap(), b"object");
    }

    #[test]
    fn create_temp_file_leaves_existing_file_and_uses_next_name() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.dat");
        let existing = temp_name(dir.path(), A);
        std::fs::write(&existing, b"CUSTOMER").unwrap();

        let (file, path) = create_temp_file(&dest, [A, A, B]).unwrap();

        assert_eq!(path, temp_name(dir.path(), B));
        assert_handle_writes_path(file, &path);
        assert_eq!(std::fs::read(&existing).unwrap(), b"CUSTOMER");
    }

    #[cfg(unix)]
    #[test]
    fn create_temp_file_does_not_follow_symlink_at_name() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.dat");
        let target = outside.path().join("victim.txt");
        std::fs::write(&target, b"VICTIM").unwrap();
        let link = temp_name(dir.path(), A);
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let (file, path) = create_temp_file(&dest, [A, B]).unwrap();

        assert_eq!(path, temp_name(dir.path(), B));
        assert_handle_writes_path(file, &path);
        assert_eq!(std::fs::read(&target).unwrap(), b"VICTIM");
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    }

    #[test]
    fn create_temp_file_fails_when_every_attempt_collides() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.dat");
        let taken: Vec<u32> = (1..=TEMP_FILE_ATTEMPTS as u32).collect();
        for &suffix in &taken {
            std::fs::write(temp_name(dir.path(), suffix), suffix.to_be_bytes()).unwrap();
        }
        // A free name after the bound must not be tried.
        let free = 0xffff_ffff;
        let suffixes = taken.iter().copied().chain([free]);

        let error = create_temp_file(&dest, suffixes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            error
                .to_string()
                .contains(&format!("after {TEMP_FILE_ATTEMPTS} attempts")),
            "{error}"
        );
        for &suffix in &taken {
            assert_eq!(
                std::fs::read(temp_name(dir.path(), suffix)).unwrap(),
                suffix.to_be_bytes()
            );
        }
        assert!(!temp_name(dir.path(), free).exists());
    }
}
