/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Ordered key streams for comparing a local directory against an S3 prefix.
//!
//! Comparing two sides reads absence from position, so both must be keyed the same
//! way: relative to their own root, in `ListObjectsV2` order. The walkers yield
//! their own item types, so this module derives a key and comparable metadata for
//! each.

use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::types::Object;

use crate::io::key::{derive_object_key, strip_key_prefix};
use crate::io::walk::{DirEntry, FsWalk, S3Walk, WalkError, WalkErrorKind};

// Whole seconds, because that is the granularity S3 reports last-modified at.
// Keeping finer local precision would make an identical pair differ every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMeta {
    pub(crate) size: u64,
    pub(crate) last_modified_secs: i64,
}

// `source` travels with the key because the key alone is not enough to act on:
// locally it is lossy, so a path rebuilt from it may not open, and on the S3 side
// the listing's storage class and restore status decide whether the object is
// readable at all.
#[derive(Debug, Clone)]
pub(crate) struct Entry<T> {
    pub(crate) key: String,
    pub(crate) meta: EntryMeta,
    pub(crate) source: T,
}

// Errors pass through as the walkers report them; what they mean is the caller's
// call, via `view_incomplete`. A run may continue past an unreadable directory, but
// not while also deleting keys it never saw.
pub(crate) trait KeyStream {
    type Source;

    // Named to avoid colliding with the walkers' inherent `next`.
    fn next_entry(
        &mut self,
    ) -> impl Future<Output = Option<Result<Entry<Self::Source>, WalkError>>> + Send;
}

// An unread subtree makes its side look emptier than it is, which position-based
// comparison cannot tell apart from deletion. A single failed entry is just one key
// unaccounted for.
pub(crate) fn view_incomplete(err: &WalkError) -> bool {
    err.is_fatal() || err.kind() == WalkErrorKind::DirectoryUnreadable
}

// Fall back to the epoch rather than failing an entry over a missing or pre-epoch
// timestamp.
fn secs_since_epoch(modified: std::io::Result<SystemTime>) -> i64 {
    modified
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// Keys are compared relative to their own root, so no prefix is applied here. The
// derivation only fails on a caller-configured delimiter appearing in a filename,
// which cannot happen while comparison fixes the delimiter at `/`.
//
// Lossy for names that are not valid UTF-8, as the upload path is: two names
// differing only in invalid bytes collapse to one key, and `Entry::source` still
// holds each true path.
fn local_key(entry: &DirEntry) -> String {
    let relative = entry.relative_path().to_string_lossy();
    derive_object_key(&relative, None, None)
        .expect("key derivation cannot fail without a custom delimiter")
        .into_owned()
}

impl KeyStream for FsWalk {
    type Source = DirEntry;

    async fn next_entry(&mut self) -> Option<Result<Entry<DirEntry>, WalkError>> {
        match self.next().await? {
            Ok(entry) => {
                let key = local_key(&entry);
                let meta = EntryMeta {
                    size: entry.metadata().len(),
                    last_modified_secs: secs_since_epoch(entry.metadata().modified()),
                };
                Some(Ok(Entry {
                    key,
                    meta,
                    source: entry,
                }))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

impl KeyStream for S3Walk {
    type Source = Object;

    async fn next_entry(&mut self) -> Option<Result<Entry<Object>, WalkError>> {
        loop {
            match self.next().await? {
                Err(err) => return Some(Err(err)),
                Ok(obj) => match key_and_meta(&obj, self.prefix()) {
                    Ok(None) => continue,
                    Ok(Some((key, meta))) => {
                        return Some(Ok(Entry {
                            key,
                            meta,
                            source: obj,
                        }))
                    }
                    Err(reason) => {
                        return Some(Err(WalkError::new(
                            obj.key().map(std::path::PathBuf::from),
                            WalkErrorKind::Service,
                            reason.into(),
                        )))
                    }
                },
            }
        }
    }
}

// `Ok(None)` is the prefix itself. `Err` means the listing was not what the API
// documents.
fn key_and_meta(
    obj: &Object,
    prefix: Option<&str>,
) -> Result<Option<(String, EntryMeta)>, &'static str> {
    let key = obj.key().ok_or("listing returned an object with no key")?;
    let relative = strip_key_prefix(key, prefix, None);
    if relative.is_empty() {
        return Ok(None);
    }
    let size = obj.size().ok_or("listing returned no size")?;
    let last_modified_secs = obj
        .last_modified()
        .ok_or("listing returned no last-modified")?
        .secs();
    Ok(Some((
        relative.to_string(),
        EntryMeta {
            size: size as u64,
            last_modified_secs,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::walk::{FsWalkContext, FsWalker};
    use aws_sdk_s3::types::{ObjectStorageClass, RestoreStatus};
    use aws_smithy_types::DateTime;
    use std::fs;
    use tempfile::tempdir;

    fn local(root: &std::path::Path) -> FsWalk {
        FsWalker::builder()
            .recursive(true)
            .key_order(true)
            .build()
            .walk(FsWalkContext::builder().root(root).build())
    }

    async fn keys<S: KeyStream>(s: &mut S) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(next) = s.next_entry().await {
            out.push(next.expect("stream failed").key);
        }
        out
    }

    fn object(key: &str, size: i64) -> Object {
        Object::builder()
            .key(key)
            .size(size)
            .last_modified(DateTime::from_secs(1_700_000_000))
            .build()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn local_keys_are_relative_and_slash_separated() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/c.txt"), "xyz").unwrap();
        fs::write(dir.path().join("top.txt"), "1").unwrap();

        assert_eq!(
            keys(&mut local(dir.path())).await,
            vec!["a/b/c.txt", "top.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn local_entries_carry_size_time_and_the_real_path() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("f"), "hello").unwrap();

        let entry = local(dir.path()).next_entry().await.unwrap().unwrap();
        assert_eq!(entry.meta.size, 5);
        assert!(entry.meta.last_modified_secs > 1_600_000_000);
        // What a transfer opens, not a path rebuilt from the lossy key.
        assert_eq!(entry.source.path(), dir.path().join("f"));
    }

    #[test]
    fn missing_or_pre_epoch_mtime_falls_back_to_the_epoch() {
        assert_eq!(
            secs_since_epoch(Err(std::io::Error::other("unsupported"))),
            0
        );
        let pre_epoch = UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert_eq!(secs_since_epoch(Ok(pre_epoch)), 0);
        assert_eq!(
            secs_since_epoch(Ok(UNIX_EPOCH + std::time::Duration::from_secs(42))),
            42
        );
    }

    #[test]
    fn object_keys_lose_the_root_prefix() {
        let (key, meta) = key_and_meta(&object("data/a/b.txt", 3), Some("data/"))
            .unwrap()
            .unwrap();
        assert_eq!(key, "a/b.txt");
        assert_eq!(meta.size, 3);
        assert_eq!(meta.last_modified_secs, 1_700_000_000);
    }

    // A prefix with and without its trailing delimiter must key alike, or the two
    // sides would disagree about every key.
    #[test]
    fn a_trailing_delimiter_on_the_prefix_makes_no_difference() {
        let with = key_and_meta(&object("data/a.txt", 1), Some("data/"))
            .unwrap()
            .unwrap();
        let without = key_and_meta(&object("data/a.txt", 1), Some("data"))
            .unwrap()
            .unwrap();
        assert_eq!(with.0, "a.txt");
        assert_eq!(with.0, without.0);
    }

    #[test]
    fn an_unprefixed_listing_keeps_whole_keys() {
        let (key, _) = key_and_meta(&object("a/b.txt", 1), None).unwrap().unwrap();
        assert_eq!(key, "a/b.txt");
    }

    #[test]
    fn the_prefix_itself_is_not_an_entry() {
        assert!(key_and_meta(&object("data/", 0), Some("data/"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_object_missing_comparison_metadata_is_rejected() {
        let no_size = Object::builder()
            .key("data/a")
            .last_modified(DateTime::from_secs(1))
            .build();
        assert!(key_and_meta(&no_size, Some("data/")).is_err());

        let no_time = Object::builder().key("data/a").size(1).build();
        assert!(key_and_meta(&no_time, Some("data/")).is_err());
    }

    // Readability is decided from the listing, without a HeadObject per key.
    #[test]
    fn archival_metadata_survives_on_the_source() {
        let obj = Object::builder()
            .key("data/cold")
            .size(1)
            .last_modified(DateTime::from_secs(1))
            .storage_class(ObjectStorageClass::Glacier)
            .restore_status(
                RestoreStatus::builder()
                    .is_restore_in_progress(true)
                    .build(),
            )
            .build();
        let entry = Entry {
            key: "cold".to_string(),
            meta: EntryMeta {
                size: 1,
                last_modified_secs: 1,
            },
            source: obj,
        };

        assert_eq!(
            entry.source.storage_class(),
            Some(&ObjectStorageClass::Glacier)
        );
        assert_eq!(
            entry
                .source
                .restore_status()
                .unwrap()
                .is_restore_in_progress(),
            Some(true)
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_unreadable_subdirectory_leaves_the_view_incomplete() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("hidden.txt"), "").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::read_dir(&locked).is_ok() {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        let mut walk = local(dir.path());
        let mut seen = Vec::new();
        let mut incomplete = false;
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => incomplete |= view_incomplete(&err),
            }
        }
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(seen.contains(&"a.txt".to_string()));
        assert!(incomplete, "an unread subtree must be an incomplete view");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_broken_symlink_costs_one_entry_not_the_view() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("broken")).unwrap();

        let mut walk = FsWalker::builder()
            .recursive(true)
            .key_order(true)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut seen = Vec::new();
        let mut errors = Vec::new();
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => errors.push(err),
            }
        }

        assert_eq!(seen, vec!["a.txt"]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind(), WalkErrorKind::BrokenSymlink);
        assert!(!view_incomplete(&errors[0]));
    }
}
