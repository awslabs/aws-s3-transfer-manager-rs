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
        cancelled: Arc<std::sync::atomic::AtomicUsize>,
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

    // Cancel the child and wait until it settles. A finished child keeps its own result, so the run
    // joins it. A running child aborts through its handle. An upload aborts its multipart upload,
    // and a download deletes its temporary file.
    pub(crate) async fn cancel(self) -> Result<u64, crate::error::Error> {
        if self.is_finished() {
            return self.join().await;
        }
        match self.inner {
            ChildInner::Upload(handle) => {
                handle.abort().await?;
            }
            ChildInner::Download(handle) => handle.abort().await,
            #[cfg(test)]
            ChildInner::Controlled { cancelled, .. } => {
                cancelled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Err(crate::error::Error::new(
            crate::error::ErrorKind::OperationCancelled,
            "the run stopped and cancelled this child",
        ))
    }
}

// `SpawnUpload` starts a child that sends one local file to a bucket.
pub(crate) struct SpawnUpload {
    handle: Arc<crate::client::Handle>,
    root: crate::io::key::BucketRoot,
}

impl SpawnUpload {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> Self {
        Self {
            handle,
            root: crate::io::key::BucketRoot::new(bucket, prefix),
        }
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
            .bucket(self.root.bucket())
            .key(self.root.object_key(key))
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

// `SpawnDownload` starts a child that writes one object into the local tree.
pub(crate) struct SpawnDownload {
    handle: Arc<crate::client::Handle>,
    root: crate::io::key::BucketRoot,
    // Each downloaded key lands below this directory.
    local_root: std::path::PathBuf,
    // This cache holds the directories the run created for earlier keys, so the run calls
    // `create_dir_all` once per directory.
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
            root: crate::io::key::BucketRoot::new(bucket, prefix),
            local_root: local_root.into(),
            created_dirs: Mutex::new(std::collections::HashSet::new()),
        }
    }

    // Return the file a key names below the local root. An S3 key can resolve above the root, or
    // onto the file of a different key, and this function refuses both.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        crate::io::key::local_path_strict(&self.local_root, key)
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
            .bucket(self.root.bucket())
            .key(self.root.object_key(key))
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

impl crate::transfer::composite::JoinChild for SyncChild {
    type Output = u64;

    fn id(&self) -> crate::transfer::TransferId {
        SyncChild::id(self)
    }

    fn is_finished(&self) -> bool {
        SyncChild::is_finished(self)
    }

    fn join(self) -> impl std::future::Future<Output = Result<u64, crate::error::Error>> + Send {
        SyncChild::join(self)
    }

    fn cancel(self) -> impl std::future::Future<Output = Result<u64, crate::error::Error>> + Send {
        SyncChild::cancel(self)
    }
}
