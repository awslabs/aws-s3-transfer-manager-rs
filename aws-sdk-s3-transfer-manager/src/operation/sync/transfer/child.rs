/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! This module builds the child transfers a sync run starts.
//!
//! An upload child sends one local file to the bucket. A download child writes one object to a
//! temporary file and renames the file after the bytes arrive. The run holds a `SyncChild` for
//! each child and asks it whether the child finished and how many bytes it moved.

use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

use super::local_path_for_key;

// Build a child from a source entry. Each direction supplies the client, bucket, and roots the
// child needs.
pub(crate) trait SpawnChild<S>: Send + Sync {
    // Enqueue a child for this key and hand back a way to ask after it. The key is the relative
    // one both sides agree on; turning it into an address is this implementation's business.
    fn spawn(&self, key: &str, source: &S, parent: u64) -> Result<SyncChild, crate::error::Error>;
}

// Sync asks a child whether it finished and how many bytes it moved.
pub(crate) struct SyncChild {
    pub(super) id: crate::transfer::TransferId,
    pub(super) inner: ChildInner,
}

pub(super) enum ChildInner {
    Upload(crate::operation::upload::UploadHandle),
    // Managed downloads write a temporary file and rename it after the bytes arrive. A failed
    // download leaves the existing destination file in place.
    Download(crate::operation::download::ManagedDownloadHandle),
    #[cfg(test)]
    Controlled {
        ended: Arc<std::sync::atomic::AtomicBool>,
        moved: u64,
        failed: bool,
    },
}

impl fmt::Debug for SyncChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncChild").field("id", &self.id).finish()
    }
}

impl SyncChild {
    pub(crate) fn id(&self) -> crate::transfer::TransferId {
        self.id
    }

    // Reap only terminal children. Joining a running child would keep the work item open for the
    // transfer duration.
    pub(crate) fn is_finished(&self) -> bool {
        match &self.inner {
            ChildInner::Upload(handle) => handle.status().is_terminal(),
            ChildInner::Download(handle) => handle.status().is_terminal(),
            #[cfg(test)]
            ChildInner::Controlled { ended, .. } => ended.load(std::sync::atomic::Ordering::SeqCst),
        }
    }

    // Read bytes moved without joining the child. The handle reports bytes while the child runs.
    // The reap later reports the outcome.
    pub(crate) fn bytes_so_far(&self) -> u64 {
        match &self.inner {
            ChildInner::Upload(handle) => handle.metrics().network_tx,
            ChildInner::Download(handle) => handle.metrics().network_rx,
            #[cfg(test)]
            ChildInner::Controlled { moved, .. } => *moved,
        }
    }

    // Join the child to read its outcome and final bytes. Joining consumes the handle.
    pub(crate) async fn join(self) -> Result<u64, crate::error::Error> {
        match self.inner {
            ChildInner::Upload(handle) => handle.join().await.map(|out| out.metrics.network_tx),
            // Joining a download renames the temporary file and stamps its modification time.
            ChildInner::Download(handle) => handle.join().await.map(|out| out.metrics.network_rx),
            #[cfg(test)]
            ChildInner::Controlled { moved, failed, .. } => {
                if failed {
                    Err(crate::error::Error::new(
                        crate::error::ErrorKind::IOError,
                        "a child that ended badly",
                    ))
                } else {
                    Ok(moved)
                }
            }
        }
    }
}

// Send a local file to a bucket. Upload and download use different client calls and destination paths.
pub(crate) struct SpawnUpload {
    handle: Arc<crate::client::Handle>,
    bucket: String,
    // The bucket prefix for uploads. Upload requests add the prefix to relative keys.
    root: String,
}

impl SpawnUpload {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> Self {
        Self {
            handle,
            bucket: bucket.into(),
            root: crate::io::key::stream::root_prefix(prefix).into_owned(),
        }
    }

    // The object a relative key names under this run's root.
    pub(crate) fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.root, key)
    }
}

impl SpawnChild<crate::io::walk::FsEntry> for SpawnUpload {
    fn spawn(
        &self,
        key: &str,
        source: &crate::io::walk::FsEntry,
        parent: u64,
    ) -> Result<SyncChild, crate::error::Error> {
        // Hand the builder the metadata the walk already read. Without it the builder stats the
        // path again: a blocking syscall inside a poll, once per key, for a size the comparison has
        // already decided from. A second read can also disagree with the first.
        let mut body = crate::io::InputStream::read_from().path(source.path());
        if let Some(metadata) = source.metadata() {
            body = body.metadata(metadata.clone());
        }
        let stream = body.build()?;
        let input = crate::operation::upload::UploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.object_key(key))
            .body(stream)
            .build()
            .expect("bucket, key and body are all set");
        let handle = crate::operation::upload::Upload::orchestrate_child(
            self.handle.clone(),
            input,
            parent,
        )?;
        Ok(SyncChild {
            id: handle.id(),
            inner: ChildInner::Upload(handle),
        })
    }
}

// Spawn a download child. The local-tree deleter uses the destination name; this type uses the
// child name.
pub(crate) struct SpawnDownload {
    handle: Arc<crate::client::Handle>,
    bucket: String,
    // The bucket prefix for downloads. Download requests add the prefix to relative keys.
    root: String,
    // The local root for downloaded keys.
    local_root: std::path::PathBuf,
    // Directories created for earlier keys. The cache avoids repeating `create_dir_all` for every
    // key.
    //
    // TODO(sync): Directory creation runs in `poll_work`. Measure one key per deep directory before
    // moving that work to a scheduled item.
    created_dirs: Mutex<std::collections::HashSet<std::path::PathBuf>>,
}

impl SpawnDownload {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
        local_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            handle,
            bucket: bucket.into(),
            root: crate::io::key::stream::root_prefix(prefix).into_owned(),
            local_root: local_root.into(),
            created_dirs: Mutex::new(std::collections::HashSet::new()),
        }
    }

    // The object a relative key names under this run's root.
    pub(crate) fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.root, key)
    }

    // An S3 key can resolve above the destination root. This path uses the directory download path
    // check.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        local_path_for_key(&self.local_root, key)
    }
}

impl SpawnChild<aws_sdk_s3::types::Object> for SpawnDownload {
    fn spawn(
        &self,
        key: &str,
        _source: &aws_sdk_s3::types::Object,
        parent: u64,
    ) -> Result<SyncChild, crate::error::Error> {
        let dest_path = self.file_path(key)?;
        if let Some(parent_dir) = dest_path.parent() {
            // A missing destination means every key is absent. The download creates directories as
            // keys need them.
            //
            // Two callers can create the same directory. `create_dir_all` and the set insert both
            // tolerate that race.
            let known = self.created_dirs.lock().contains(parent_dir);
            if !known {
                std::fs::create_dir_all(parent_dir).map_err(|e| {
                    crate::error::Error::new(
                        crate::error::ErrorKind::IOError,
                        format!("could not make a place for '{key}': {e}"),
                    )
                })?;
                self.created_dirs.lock().insert(parent_dir.to_path_buf());
            }
        }

        // The temporary name keeps concurrent downloads from writing the same half-finished file.
        let temp_path = dest_path.with_file_name(format!(
            "{}.s3tmp.{:08x}",
            dest_path.file_name().unwrap_or_default().to_string_lossy(),
            fastrand::u32(..)
        ));
        let file = std::fs::File::create(&temp_path).map_err(|e| {
            crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                format!("could not open a temporary file for '{key}': {e}"),
            )
        })?;

        let input = crate::operation::download::DownloadInput::builder()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .build()
            .expect("bucket and key are set");
        let inner = crate::operation::download::Download::orchestrate_with_sink(
            self.handle.clone(),
            input,
            file,
            0,
            true,
            Some(parent),
        )?;
        let handle =
            crate::operation::download::ManagedDownloadHandle::new(inner, temp_path, dest_path)
                .stamp_modified_time();
        Ok(SyncChild {
            id: handle.transfer_id(),
            inner: ChildInner::Download(handle),
        })
    }
}
