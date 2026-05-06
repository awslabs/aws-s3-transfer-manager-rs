/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use same_file::Handle;

use super::error::{WalkError, WalkErrorKind};

/// A directory queued for traversal, carrying the chain of ancestor directory
/// handles needed for per-path symlink cycle detection.
struct PendingDir {
    path: PathBuf,
    depth: usize,
    /// Handles of all directories on the path from the walk root to (and
    /// including) this directory's parent. Used to detect cycles: before
    /// entering a symlinked directory, we check whether its target handle
    /// already appears in this chain.
    ancestor_handles: Vec<Arc<Handle>>,
}

/// Result of reading a single directory.
///
/// Contains all files discovered in the directory (after filter application),
/// subdirectories to recurse into (subject to `max_depth`), and any non-fatal
/// non-fatal errors encountered during the read.
struct ReadDirResult {
    /// Files discovered in the directory, after filter application.
    files: Vec<DirEntry>,
    /// Subdirectories to recurse into. Empty if `depth >= max_depth`.
    subdirs: Vec<PendingDir>,
    /// Non-fatal errors encountered reading individual entries.
    errors: Vec<WalkError>,
}

/// A file entry discovered during a filesystem walk.
///
/// Contains the absolute path, the path relative to the walk root, and
/// the file metadata. Only regular files (and symlinks to regular files
/// when `follow_symlinks` is enabled) produce `DirEntry` values; directories
/// are traversed but not yielded.
#[derive(Debug)]
pub struct DirEntry {
    path: PathBuf,
    relative_path: PathBuf,
    metadata: Metadata,
}

impl DirEntry {
    /// Absolute path to the file on the local filesystem.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path of the file relative to the walk root.
    ///
    /// For a walk rooted at `/src/foo` that discovers `/src/foo/a/b.txt`,
    /// the relative path is `a/b.txt`. Useful for deriving destination keys
    /// for uploads.
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// File metadata.
    ///
    /// When the entry was produced by following a symlink, this is the
    /// metadata of the symlink target (not the symlink itself).
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

type FilterFn = Box<dyn Fn(&DirEntry) -> bool + Send + Sync>;

/// Configuration for walking a local filesystem directory.
///
/// Configuration for walking a local directory tree.
///
/// Describes what to look for and how (depth, symlink policy, sorting,
/// filtering). The walk root is supplied separately via [`FsWalkContext`]
/// when starting a walk.
///
/// Use [`FsWalker::builder`] to construct an instance.
///
/// # Traversal model
///
/// The walker reads one directory at a time. Subdirectories discovered during
/// a read are queued for subsequent reads. Only regular files produce yielded
/// [`DirEntry`] values; directories, symlinks, and special files (sockets,
/// fifos, block/char devices) are traversed or skipped according to
/// configuration but are never themselves yielded as entries.
///
/// # Symlink cycle handling
///
/// When `follow_symlinks` is enabled, the walker detects symlink cycles
/// by tracking the chain of directories on the current descent path. A
/// symlink whose target resolves to a directory already on that path
/// (including the current directory itself) is reported as a non-fatal
/// error with kind `SymlinkCycle` and the walk continues without entering
/// the cyclic link.
///
/// Non-cyclic duplicate symlinks (two different symlinks pointing to the
/// same target) are not deduplicated. Both are followed and the target's
/// content is yielded twice. If deduplication is desired, apply it at a
/// higher layer.
// TODO(walker): `dir_filter: Option<Box<dyn Fn(&DirEntry) -> bool>>` — subtree
//   prune predicate. Biggest perf improvement for bulk ops on trees with large
//   excluded subtrees (.git/, node_modules/).
// TODO(walker): `min_depth: usize` — skip entries above this depth.
// TODO(walker): `use_gitignore: bool` — integrate the `ignore` crate for
//   opt-in .gitignore support.
// TODO(walker): `normalize_unicode: Option<NormalizationForm>` — NFC/NFD
//   normalization of filenames before deriving relative paths / S3 keys.
// TODO(walker): `same_file_system: bool` — refuse to cross filesystem
//   boundaries during recursion.
pub struct FsWalker {
    follow_symlinks: bool,
    max_depth: usize,
    sort: bool,
    canonicalize_root: bool,
    filter: Option<FilterFn>,
}

impl std::fmt::Debug for FsWalker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsWalker")
            .field("follow_symlinks", &self.follow_symlinks)
            .field("max_depth", &self.max_depth)
            .field("sort", &self.sort)
            .field("canonicalize_root", &self.canonicalize_root)
            .finish()
    }
}

impl FsWalker {
    /// Create a builder for configuring an `FsWalker`.
    #[must_use]
    pub fn builder() -> FsWalkerBuilder {
        FsWalkerBuilder::default()
    }

    /// Start a walk with the given execution context.
    ///
    /// Returns an [`FsWalk`] that yields file entries via
    /// [`next`](FsWalk::next). The walk begins by validating the root
    /// directory from the context; invalid roots produce a fatal error
    /// on the first call to `next`.
    #[must_use]
    pub fn walk(self, ctx: FsWalkContext) -> FsWalk {
        let mut root = ctx.root;
        let mut pending_errors = VecDeque::new();
        let mut done = false;
        let mut pending_dirs = VecDeque::new();

        if self.canonicalize_root {
            match std::fs::canonicalize(&root) {
                Ok(canonical) => root = canonical,
                Err(e) => {
                    pending_errors.push_back(WalkError::new(
                        Some(root.clone()),
                        WalkErrorKind::SourceUnreadable,
                        Box::new(e),
                    ));
                    done = true;
                }
            }
        } else if !self.follow_symlinks {
            match std::fs::symlink_metadata(&root) {
                Ok(md) if md.file_type().is_symlink() => {
                    pending_errors.push_back(WalkError::new(
                        Some(root.clone()),
                        WalkErrorKind::NotADirectory,
                        Box::from("source root is a symlink; enable follow_symlinks or canonicalize_root to walk through it"),
                    ));
                    done = true;
                }
                Ok(_) => {}
                Err(_) => {
                    // Root doesn't exist / isn't accessible; let the first
                    // directory read report the fatal error.
                }
            }
        }

        if !done {
            pending_dirs.push_back(PendingDir {
                path: root.clone(),
                depth: 0,
                ancestor_handles: Vec::new(),
            });
        }

        tracing::debug!(
            ?root,
            follow_symlinks = self.follow_symlinks,
            max_depth = self.max_depth,
            sort = self.sort,
            "fs walk started",
        );

        FsWalk {
            config: self,
            root,
            pending_dirs,
            ready_files: VecDeque::new(),
            pending_errors,
            done,
        }
    }
}

/// Builder for [`FsWalker`].
///
/// All fields have sensible defaults: non-recursive, symlinks not followed,
/// unsorted, no filter, `canonicalize_root` disabled.
#[derive(Default)]
pub struct FsWalkerBuilder {
    follow_symlinks: bool,
    max_depth: usize,
    sort: bool,
    canonicalize_root: bool,
    filter: Option<FilterFn>,
}

impl std::fmt::Debug for FsWalkerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsWalkerBuilder")
            .field("follow_symlinks", &self.follow_symlinks)
            .field("max_depth", &self.max_depth)
            .field("sort", &self.sort)
            .field("canonicalize_root", &self.canonicalize_root)
            .finish()
    }
}

impl FsWalkerBuilder {
    /// Control whether symbolic links are followed.
    ///
    /// When `false` (default), symlinks are skipped entirely. When `true`,
    /// symlinks are resolved: file symlinks yield entries, directory
    /// symlinks are traversed (subject to `max_depth` and cycle detection),
    /// and broken symlinks produce non-fatal errors.
    #[must_use]
    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    /// Shortcut for setting recursion depth.
    ///
    /// `recursive(true)` sets `max_depth` to `usize::MAX` (walk the full
    /// tree). `recursive(false)` sets it to `0` (only the root directory's
    /// immediate contents). Default is non-recursive.
    #[must_use]
    pub fn recursive(mut self, recursive: bool) -> Self {
        self.max_depth = if recursive { usize::MAX } else { 0 };
        self
    }

    /// Set the maximum directory depth to traverse.
    ///
    /// Depth semantics:
    /// - `0` (default): read only the root directory's immediate contents.
    ///   Subdirectories are skipped.
    /// - `1`: descend into one level of subdirectories from the root.
    /// - `N`: descend up to N levels of subdirectories from the root.
    ///
    /// Example with tree:
    ///
    /// ```text
    /// root/
    /// ├── top.txt         ← depth 0
    /// └── a/              ← depth 0
    ///     ├── mid.txt     ← depth 1
    ///     └── b/          ← depth 1
    ///         └── deep.txt ← depth 2
    /// ```
    ///
    /// - `max_depth(0)` yields `top.txt`
    /// - `max_depth(1)` yields `top.txt, a/mid.txt`
    /// - `max_depth(2)` yields `top.txt, a/mid.txt, a/b/deep.txt`
    ///
    /// Depth counts subdirectory levels below the root. The root's
    /// immediate contents are always yielded regardless of this setting.
    #[must_use]
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    /// Enable lexicographic sorting of entries within each directory.
    ///
    /// When `true`, files and subdirectories from each directory read are
    /// sorted by full path. Entries are sorted *within* a directory level
    /// but not globally. Depth-first traversal produces entries
    /// level-by-level.
    ///
    /// When `false` (default), entries are returned in OS-native order.
    ///
    /// Sort is required for `sync`-style operations that merge-join against
    /// `ListObjectsV2` results (which are UTF-8 binary sorted by key).
    #[must_use]
    pub fn sort(mut self, sort: bool) -> Self {
        self.sort = sort;
        self
    }

    /// Resolve the walk root via `std::fs::canonicalize` before walking.
    ///
    /// When `true`, the root path is canonicalized (symlinks resolved,
    /// relative components removed) before the walk begins. This allows
    /// walking through a symlinked root directory even when
    /// `follow_symlinks` is `false`. If canonicalization fails (e.g. the
    /// path does not exist), the walk produces a fatal error.
    ///
    /// When `false` (default), the root is used as-is. A symlinked root
    /// with `follow_symlinks=false` produces a fatal error.
    #[must_use]
    pub fn canonicalize_root(mut self, canonicalize: bool) -> Self {
        self.canonicalize_root = canonicalize;
        self
    }

    /// Set a filter predicate applied to each discovered file.
    ///
    /// The predicate is called after a file's metadata has been read but
    /// before it is yielded. Returning `false` drops the entry silently.
    /// The filter does not apply to directories. Directory traversal is
    /// controlled by `max_depth` and `follow_symlinks`.
    ///
    /// **The filter does not prune subtrees.** A file-level filter that
    /// rejects everything under a given subdirectory still causes the
    /// walker to descend into and stat every file in that subtree. For
    /// performance on trees with large excludable subtrees (e.g. `.git/`,
    /// `node_modules/`), compose multiple walks rooted at smaller subtrees.
    #[must_use]
    pub fn filter(mut self, f: impl Fn(&DirEntry) -> bool + Send + Sync + 'static) -> Self {
        self.filter = Some(Box::new(f));
        self
    }

    /// Build the [`FsWalker`] configuration.
    #[must_use]
    pub fn build(self) -> FsWalker {
        FsWalker {
            follow_symlinks: self.follow_symlinks,
            max_depth: self.max_depth,
            sort: self.sort,
            canonicalize_root: self.canonicalize_root,
            filter: self.filter,
        }
    }
}

/// Execution context for an [`FsWalker`], providing the walk root.
#[derive(Debug, Clone)]
pub struct FsWalkContext {
    root: PathBuf,
}

impl FsWalkContext {
    /// Create a builder for an `FsWalkContext`.
    #[must_use]
    pub fn builder() -> FsWalkContextBuilder {
        FsWalkContextBuilder { root: None }
    }
}

/// Builder for [`FsWalkContext`].
#[derive(Debug, Default)]
pub struct FsWalkContextBuilder {
    root: Option<PathBuf>,
}

impl FsWalkContextBuilder {
    /// Set the root directory to walk.
    ///
    /// This field is required.
    #[must_use]
    pub fn root(mut self, root: impl Into<PathBuf>) -> Self {
        self.root = Some(root.into());
        self
    }

    /// Build the [`FsWalkContext`].
    ///
    /// # Panics
    ///
    /// Panics if `root` has not been set.
    #[must_use]
    pub fn build(self) -> FsWalkContext {
        FsWalkContext {
            root: self.root.expect("required field `root` should be set"),
        }
    }
}

/// A running filesystem walk, yielding file entries.
///
/// Created by [`FsWalker::walk`]. The walk is pull-based: each call to
/// [`next`](Self::next) drives the walk forward, reading directories as
/// needed and buffering discovered files.
///
/// Errors encountered during the walk are interleaved with successful
/// entries. Fatal errors terminate the walk; non-fatal errors do not.
pub struct FsWalk {
    config: FsWalker,
    root: PathBuf,
    pending_dirs: VecDeque<PendingDir>,
    ready_files: VecDeque<DirEntry>,
    pending_errors: VecDeque<WalkError>,
    done: bool,
}

impl std::fmt::Debug for FsWalk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsWalk")
            .field("root", &self.root)
            .field("done", &self.done)
            .finish()
    }
}

impl FsWalk {
    /// Return the next entry from the walk.
    ///
    /// Returns:
    /// - `Some(Ok(entry))` for a file that passed the filter.
    /// - `Some(Err(err))` for a fatal or non-fatal error. Fatal errors
    ///   are followed by `None` on the next call; non-fatal errors are
    ///   followed by more results as the walk continues.
    /// - `None` when the walk is complete.
    pub async fn next(&mut self) -> Option<Result<DirEntry, WalkError>> {
        loop {
            if let Some(entry) = self.ready_files.pop_front() {
                return Some(Ok(entry));
            }
            if let Some(err) = self.pending_errors.pop_front() {
                if !err.is_fatal() {
                    tracing::warn!(
                        path = ?err.path(),
                        kind = ?err.kind(),
                        "skipping entry",
                    );
                }
                return Some(Err(err));
            }
            if self.done {
                return None;
            }

            let pending = match self.pending_dirs.pop_front() {
                Some(d) => d,
                None => {
                    self.done = true;
                    return None;
                }
            };

            match self.read_dir(&pending.path, pending.depth, &pending.ancestor_handles) {
                Ok(result) => {
                    self.ready_files.extend(result.files);
                    self.pending_errors.extend(result.errors);
                    self.pending_dirs.extend(result.subdirs);
                }
                Err(err) => {
                    if err.is_fatal() {
                        self.done = true;
                    }
                    return Some(Err(err));
                }
            }
        }
    }

    /// Whether the walk has finished (no more entries will be produced).
    pub fn is_done(&self) -> bool {
        self.done
    }

    fn read_dir(
        &self,
        dir: &Path,
        depth: usize,
        ancestor_handles: &[Arc<Handle>],
    ) -> Result<ReadDirResult, WalkError> {
        let entries = std::fs::read_dir(dir).map_err(|e| {
            let kind = WalkError::classify_io(&e);
            let kind = if depth == 0
                && matches!(kind, WalkErrorKind::Io | WalkErrorKind::PermissionDenied)
            {
                WalkErrorKind::SourceUnreadable
            } else {
                kind
            };
            WalkError::new(Some(dir.to_path_buf()), kind, Box::new(e))
        })?;

        // Build the ancestor chain for children of this directory.
        let self_handle = Arc::new(Handle::from_path(dir).map_err(|e| {
            let kind = WalkError::classify_io(&e);
            let kind = if depth == 0
                && matches!(kind, WalkErrorKind::Io | WalkErrorKind::PermissionDenied)
            {
                WalkErrorKind::SourceUnreadable
            } else {
                kind
            };
            WalkError::new(Some(dir.to_path_buf()), kind, Box::new(e))
        })?);
        let mut next_ancestors = ancestor_handles.to_vec();
        next_ancestors.push(Arc::clone(&self_handle));

        let mut result = ReadDirResult {
            files: Vec::new(),
            subdirs: Vec::new(),
            errors: Vec::new(),
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    let kind = WalkError::classify_io(&e);
                    result
                        .errors
                        .push(WalkError::new(Some(dir.to_path_buf()), kind, Box::new(e)));
                    continue;
                }
            };

            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(e) => {
                    let kind = WalkError::classify_io(&e);
                    result
                        .errors
                        .push(WalkError::new(Some(path), kind, Box::new(e)));
                    continue;
                }
            };

            if file_type.is_symlink() {
                if !self.config.follow_symlinks {
                    continue;
                }
                let metadata = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    Err(e) => {
                        let kind = match e.kind() {
                            std::io::ErrorKind::NotFound => WalkErrorKind::BrokenSymlink,
                            _ => WalkError::classify_io(&e),
                        };
                        result
                            .errors
                            .push(WalkError::new(Some(path), kind, Box::new(e)));
                        continue;
                    }
                };

                if metadata.is_dir() {
                    // Cycle detection: check if the symlink target is already
                    // on the current descent path.
                    match Handle::from_path(&path) {
                        Ok(target_handle) => {
                            if next_ancestors.iter().any(|a| **a == target_handle) {
                                result.errors.push(WalkError::new(
                                    Some(path),
                                    WalkErrorKind::SymlinkCycle,
                                    Box::from("symlink target is an ancestor of the current path"),
                                ));
                                continue;
                            }
                            if depth < self.config.max_depth {
                                result.subdirs.push(PendingDir {
                                    path,
                                    depth: depth + 1,
                                    ancestor_handles: next_ancestors.clone(),
                                });
                            }
                        }
                        Err(e) => {
                            let kind = WalkError::classify_io(&e);
                            result
                                .errors
                                .push(WalkError::new(Some(path), kind, Box::new(e)));
                        }
                    }
                } else if metadata.is_file() {
                    self.push_file(&mut result.files, path, &metadata);
                }
            } else if file_type.is_file() {
                let metadata = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    Err(e) => {
                        let kind = WalkError::classify_io(&e);
                        result
                            .errors
                            .push(WalkError::new(Some(path), kind, Box::new(e)));
                        continue;
                    }
                };
                self.push_file(&mut result.files, path, &metadata);
            } else if file_type.is_dir() && depth < self.config.max_depth {
                result.subdirs.push(PendingDir {
                    path,
                    depth: depth + 1,
                    ancestor_handles: next_ancestors.clone(),
                });
            }
        }

        if self.config.sort {
            result.files.sort_by(|a, b| a.path.cmp(&b.path));
            result.subdirs.sort_by(|a, b| a.path.cmp(&b.path));
        }

        tracing::trace!(
            ?dir,
            files = result.files.len(),
            subdirs = result.subdirs.len(),
            errors = result.errors.len(),
            "directory read",
        );

        Ok(result)
    }

    fn push_file(&self, files: &mut Vec<DirEntry>, path: PathBuf, metadata: &Metadata) {
        let relative_path = path.strip_prefix(&self.root).unwrap_or(&path).to_path_buf();
        let entry = DirEntry {
            path,
            relative_path,
            metadata: metadata.clone(),
        };
        if self.config.filter.as_ref().is_none_or(|f| f(&entry)) {
            files.push(entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Normalize path separators to `/` for cross-platform test assertions.
    fn norm(path: &std::path::Path) -> String {
        path.to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    }

    async fn collect_entries(mut walk: FsWalk) -> (Vec<DirEntry>, Vec<WalkError>) {
        let mut entries = Vec::new();
        let mut errors = Vec::new();
        while let Some(result) = walk.next().await {
            match result {
                Ok(e) => entries.push(e),
                Err(e) => errors.push(e),
            }
        }
        (entries, errors)
    }

    fn walker() -> FsWalkerBuilder {
        FsWalker::builder()
    }

    fn ctx(root: impl Into<PathBuf>) -> FsWalkContext {
        FsWalkContext::builder().root(root).build()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_flat_directory() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "a").unwrap();
        fs::write(dir.path().join("b.txt"), "b").unwrap();
        fs::write(dir.path().join("c.txt"), "c").unwrap();

        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 3);

        let mut names: Vec<_> = entries
            .iter()
            .map(|e| e.relative_path().to_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                PathBuf::from("a.txt"),
                PathBuf::from("b.txt"),
                PathBuf::from("c.txt"),
            ]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_recursive() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/nested.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub/deep")).unwrap();
        fs::write(dir.path().join("sub/deep/deep.txt"), "").unwrap();

        let walk = walker().recursive(true).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 3);

        let mut names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(
            names,
            vec!["sub/deep/deep.txt", "sub/nested.txt", "top.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_non_recursive_skips_subdirs() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/hidden.txt"), "").unwrap();

        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("top.txt"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_filter() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        fs::write(dir.path().join("b.log"), "").unwrap();
        fs::write(dir.path().join("c.txt"), "").unwrap();

        let walk = walker()
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 2);

        let mut names: Vec<_> = entries
            .iter()
            .map(|e| e.relative_path().to_owned())
            .collect();
        names.sort();
        assert_eq!(names, vec![PathBuf::from("a.txt"), PathBuf::from("c.txt")]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_empty_directory() {
        let dir = tempdir().unwrap();
        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert!(entries.is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_max_depth() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("d0.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a")).unwrap();
        fs::write(dir.path().join("a/d1.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/d2.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b/c")).unwrap();
        fs::write(dir.path().join("a/b/c/d3.txt"), "").unwrap();

        let walk = walker().max_depth(2).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 3);

        let mut names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(names, vec!["a/b/d2.txt", "a/d1.txt", "d0.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_relative_path_correctness() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/file.txt"), "").unwrap();

        let walk = walker().recursive(true).build().walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("a/b/file.txt"));
        assert_eq!(entries[0].path(), dir.path().join("a/b/file.txt"));
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_follow_symlinks() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "content").unwrap();
        std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt"))
            .unwrap();

        let walk = walker().follow_symlinks(true).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 2);
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_no_follow_symlinks() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "content").unwrap();
        std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt"))
            .unwrap();

        let walk = walker()
            .follow_symlinks(false)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("real.txt"));
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_cycle_detection() {
        use tokio::time::{timeout, Duration};

        let dir = tempdir().unwrap();
        let dir_a = dir.path().join("a");
        let dir_b = dir.path().join("b");
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        std::os::unix::fs::symlink(&dir_b, dir_a.join("link_to_b")).unwrap();
        std::os::unix::fs::symlink(&dir_a, dir_b.join("link_to_a")).unwrap();
        fs::write(dir_a.join("file_a.txt"), "").unwrap();
        fs::write(dir_b.join("file_b.txt"), "").unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));

        let result = timeout(Duration::from_secs(5), collect_entries(walk)).await;
        let (entries, errors) = result.expect("walk should terminate via cycle detection");

        assert!(!entries.is_empty());
        assert!(
            !errors.is_empty(),
            "expected cycle detection errors but got none"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_root_does_not_exist() {
        let mut walk = walker()
            .build()
            .walk(ctx("/this/path/does/not/exist/abc123xyz"));
        let first = walk.next().await;
        match first {
            Some(Err(ref err)) => {
                assert!(err.is_fatal(), "expected fatal error for missing root");
                assert_eq!(
                    err.kind(),
                    WalkErrorKind::SourceUnreadable,
                    "NotFound at the walk root should map to SourceUnreadable"
                );
            }
            other => panic!("expected fatal error for missing root, got {other:?}"),
        }
        assert!(walk.next().await.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_recursive_false_equals_max_depth_zero() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/inner.txt"), "").unwrap();

        let (a, _) = collect_entries(walker().recursive(false).build().walk(ctx(dir.path()))).await;
        let (b, _) = collect_entries(walker().max_depth(0).build().walk(ctx(dir.path()))).await;

        let mut a_names: Vec<_> = a.iter().map(|e| e.relative_path().to_owned()).collect();
        let mut b_names: Vec<_> = b.iter().map(|e| e.relative_path().to_owned()).collect();
        a_names.sort();
        b_names.sort();
        assert_eq!(a_names, b_names);
        assert_eq!(a_names, vec![PathBuf::from("top.txt")]);
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_broken_symlink_follow() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "").unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", dir.path().join("broken")).unwrap();

        let walk = walker().follow_symlinks(true).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("real.txt"));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].path(), Some(dir.path().join("broken").as_path()));
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_broken_symlink_no_follow() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "").unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", dir.path().join("broken")).unwrap();

        let walk = walker()
            .follow_symlinks(false)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("real.txt"));
        assert!(errors.is_empty());
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_to_file() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "content").unwrap();
        std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt"))
            .unwrap();

        let walk = walker().follow_symlinks(true).build().walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;

        assert_eq!(entries.len(), 2);
        for e in &entries {
            assert!(e.metadata().is_file());
        }
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_chain() {
        let dir = tempdir().unwrap();
        let real = dir.path().join("real_dir");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("deep.txt"), "").unwrap();
        std::os::unix::fs::symlink(&real, dir.path().join("b")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("b"), dir.path().join("a")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;

        let deep_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.path().file_name().is_some_and(|n| n == "deep.txt"))
            .collect();
        assert!(
            !deep_entries.is_empty(),
            "expected deep.txt to be found via at least one path"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_skips_special_files() {
        use std::os::unix::net::UnixListener;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("regular.txt"), "").unwrap();
        let _listener = UnixListener::bind(dir.path().join("socket.sock")).unwrap();

        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("regular.txt"));
        assert!(errors.is_empty());
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_subdirectory_permission_denied() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("hidden.txt"), "").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        let can_read_denied = std::fs::read_dir(&locked).is_ok();
        if can_read_denied {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        let walk = walker().recursive(true).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(entries
            .iter()
            .any(|e| e.relative_path() == Path::new("top.txt")));
        assert!(
            !errors.is_empty(),
            "expected permission-denied error for unreadable subdir"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_source_is_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("file.txt");
        fs::write(&file, "").unwrap();

        let mut walk = walker().build().walk(ctx(&file));
        let first = walk.next().await;
        assert!(
            matches!(first, Some(Err(_))),
            "expected fatal error when source is a file"
        );
        assert!(walk.next().await.is_none());
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_source_is_symlink_follow_enabled() {
        let dir = tempdir().unwrap();
        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("a.txt"), "").unwrap();
        let link = dir.path().join("link_to_real");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let walk = walker().follow_symlinks(true).build().walk(ctx(&link));
        let (entries, errors) = collect_entries(walk).await;
        assert!(
            errors.is_empty(),
            "follow_symlinks=true should accept symlink root"
        );
        assert_eq!(entries.len(), 1);
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_source_is_symlink_follow_disabled() {
        let dir = tempdir().unwrap();
        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("a.txt"), "").unwrap();
        let link = dir.path().join("link_to_real");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let walk = walker().follow_symlinks(false).build().walk(ctx(&link));
        let (entries, errors) = collect_entries(walk).await;
        assert!(
            entries.is_empty(),
            "walk should not descend into symlinked root when follow_symlinks=false"
        );
        assert_eq!(errors.len(), 1, "expected exactly one fatal error");
        assert_eq!(errors[0].path(), Some(link.as_path()));
        assert_eq!(errors[0].kind(), WalkErrorKind::NotADirectory);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_lexicographic_sort() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("zebra.txt"), "").unwrap();
        fs::write(dir.path().join("apple.txt"), "").unwrap();
        fs::write(dir.path().join("mango.txt"), "").unwrap();

        let walk = walker().sort(true).build().walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;
        let names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        assert_eq!(names, vec!["apple.txt", "mango.txt", "zebra.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_recursive_sort_is_depth_first_per_dir() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("c-file.txt"), "").unwrap();
        fs::write(dir.path().join("b-file.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a-dir")).unwrap();
        fs::write(dir.path().join("a-dir/z.txt"), "").unwrap();
        fs::write(dir.path().join("a-dir/y.txt"), "").unwrap();

        let walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;
        let names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        assert_eq!(
            names,
            vec!["b-file.txt", "c-file.txt", "a-dir/y.txt", "a-dir/z.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_unsorted_by_default() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        fs::write(dir.path().join("b.txt"), "").unwrap();
        fs::write(dir.path().join("c.txt"), "").unwrap();

        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, _) = collect_entries(walk).await;
        let mut names: Vec<_> = entries
            .iter()
            .map(|e| e.relative_path().to_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                PathBuf::from("a.txt"),
                PathBuf::from("b.txt"),
                PathBuf::from("c.txt"),
            ]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_unicode_filenames() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("café.txt"), "").unwrap();
        fs::write(dir.path().join("日本語.txt"), "").unwrap();
        fs::write(dir.path().join("🦀.txt"), "").unwrap();
        fs::write(dir.path().join("file with spaces.txt"), "").unwrap();

        let walk = walker().sort(true).build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(
            errors.is_empty(),
            "unicode filenames should not produce errors"
        );
        assert_eq!(entries.len(), 4);

        let names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        assert!(names.contains(&"café.txt".to_string()));
        assert!(names.contains(&"日本語.txt".to_string()));
        assert!(names.contains(&"🦀.txt".to_string()));
        assert!(names.contains(&"file with spaces.txt".to_string()));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_large_directory() {
        let dir = tempdir().unwrap();
        for i in 0..10_000 {
            fs::write(dir.path().join(format!("file_{i:05}.txt")), "").unwrap();
        }

        let walk = walker().build().walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());
        assert_eq!(entries.len(), 10_000);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_error_kind_not_a_directory() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("notadir.txt");
        fs::write(&file, "").unwrap();

        let mut walk = walker().build().walk(ctx(&file));
        let first = walk.next().await;
        match first {
            Some(Err(err)) => {
                assert!(err.is_fatal(), "source-is-file should be fatal");
                assert_eq!(err.kind(), WalkErrorKind::NotADirectory);
            }
            other => panic!("expected fatal error, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_error_kind_symlink_cycle() {
        use tokio::time::{timeout, Duration};

        let dir = tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        fs::create_dir(&a).unwrap();
        fs::create_dir(&b).unwrap();
        std::os::unix::fs::symlink(&b, a.join("to_b")).unwrap();
        std::os::unix::fs::symlink(&a, b.join("to_a")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let result = timeout(Duration::from_secs(5), collect_entries(walk)).await;
        let (_, errors) = result.expect("should terminate");

        assert!(
            errors
                .iter()
                .any(|e| e.kind() == WalkErrorKind::SymlinkCycle),
            "expected at least one SymlinkCycle error, got kinds: {:?}",
            errors.iter().map(|e| e.kind()).collect::<Vec<_>>()
        );
        assert!(errors.iter().all(|e| !e.is_fatal()));
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_error_kind_broken_symlink() {
        let dir = tempdir().unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", dir.path().join("broken")).unwrap();

        let walk = walker().follow_symlinks(true).build().walk(ctx(dir.path()));
        let (_, errors) = collect_entries(walk).await;
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind(), WalkErrorKind::BrokenSymlink);
        assert!(!errors[0].is_fatal());
    }

    // --- NEW TESTS ---

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fswalker_canonicalize_root() {
        let dir = tempdir().unwrap();
        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("a.txt"), "").unwrap();
        let link = dir.path().join("link_to_real");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        // canonicalize_root=true resolves the symlink and walks the target
        let walk = walker().canonicalize_root(true).build().walk(ctx(&link));
        let (entries, errors) = collect_entries(walk).await;
        assert!(
            errors.is_empty(),
            "canonicalize_root should resolve symlink root"
        );
        assert_eq!(entries.len(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fswalker_canonicalize_root_nonexistent() {
        let mut walk = walker()
            .canonicalize_root(true)
            .build()
            .walk(ctx("/this/path/does/not/exist/abc123xyz"));
        let first = walk.next().await;
        assert!(
            matches!(first, Some(Err(ref e)) if e.is_fatal()),
            "expected fatal error for canonicalize of nonexistent root"
        );
        assert!(walk.next().await.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fswalker_is_done() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();

        let mut walk = walker().build().walk(ctx(dir.path()));
        assert!(!walk.is_done(), "walk should not be done before iteration");
        while walk.next().await.is_some() {}
        assert!(walk.is_done(), "walk should be done after exhausting");
    }

    // --- Symlink scenario tests ---

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_self_loop() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("file.txt"), "").unwrap();
        std::os::unix::fs::symlink(".", sub.join("self_link")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        assert!(
            entries
                .iter()
                .any(|e| e.relative_path().ends_with("file.txt")),
            "file.txt should be yielded"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.kind() == WalkErrorKind::SymlinkCycle),
            "self-loop should be detected as SymlinkCycle"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_siblings_same_target_both_traversed() {
        let dir = tempdir().unwrap();
        let shared = dir.path().join("shared");
        fs::create_dir(&shared).unwrap();
        fs::write(shared.join("file.txt"), "").unwrap();
        std::os::unix::fs::symlink(&shared, dir.path().join("link_a")).unwrap();
        std::os::unix::fs::symlink(&shared, dir.path().join("link_b")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        let file_count = entries
            .iter()
            .filter(|e| e.path().file_name().is_some_and(|n| n == "file.txt"))
            .count();
        assert_eq!(
            file_count, 3,
            "file.txt should appear 3 times (shared/, link_a/, link_b/)"
        );
        assert!(
            errors.is_empty(),
            "no errors expected for non-cyclic duplicates"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_chain_not_cycle() {
        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real_dir");
        fs::create_dir(&real_dir).unwrap();
        fs::write(real_dir.join("file.txt"), "").unwrap();
        std::os::unix::fs::symlink(&real_dir, dir.path().join("b")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("b"), dir.path().join("a")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        let file_count = entries
            .iter()
            .filter(|e| e.path().file_name().is_some_and(|n| n == "file.txt"))
            .count();
        assert_eq!(
            file_count, 3,
            "file.txt should appear 3 times (real_dir/, a/, b/)"
        );
        assert!(errors.is_empty(), "chain of symlinks is not a cycle");
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_deeper_cycle() {
        use tokio::time::{timeout, Duration};

        let dir = tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        let c = dir.path().join("c");
        fs::create_dir(&a).unwrap();
        fs::create_dir(&b).unwrap();
        fs::create_dir(&c).unwrap();
        std::os::unix::fs::symlink(&b, a.join("to_b")).unwrap();
        std::os::unix::fs::symlink(&c, b.join("to_c")).unwrap();
        std::os::unix::fs::symlink(&a, c.join("to_a")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let result = timeout(Duration::from_secs(5), collect_entries(walk)).await;
        let (_, errors) = result.expect("walk should terminate via cycle detection");

        assert!(
            errors
                .iter()
                .any(|e| e.kind() == WalkErrorKind::SymlinkCycle),
            "expected SymlinkCycle errors in A→B→C→A cycle"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_real_dir_reachable_via_symlink_yields_twice() {
        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real_dir");
        fs::create_dir(&real_dir).unwrap();
        fs::write(real_dir.join("file.txt"), "").unwrap();
        std::os::unix::fs::symlink(&real_dir, dir.path().join("link_to_real")).unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;

        let file_count = entries
            .iter()
            .filter(|e| e.path().file_name().is_some_and(|n| n == "file.txt"))
            .count();
        assert_eq!(
            file_count, 2,
            "file.txt should appear twice (real_dir/ and link_to_real/)"
        );
        assert!(
            errors.is_empty(),
            "non-cyclic duplicate should not produce errors"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_cycle_deep_in_subtree() {
        use tokio::time::{timeout, Duration};

        let dir = tempdir().unwrap();
        // Clean subtree
        fs::create_dir(dir.path().join("clean")).unwrap();
        fs::write(dir.path().join("clean/a.txt"), "").unwrap();
        // Dirty subtree with x↔y cycle
        fs::create_dir_all(dir.path().join("dirty/x")).unwrap();
        fs::create_dir(dir.path().join("dirty/y")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("dirty/y"), dir.path().join("dirty/x/to_y"))
            .unwrap();
        std::os::unix::fs::symlink(dir.path().join("dirty/x"), dir.path().join("dirty/y/to_x"))
            .unwrap();

        let walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(ctx(dir.path()));
        let result = timeout(Duration::from_secs(5), collect_entries(walk)).await;
        let (entries, errors) = result.expect("walk should terminate");

        assert!(
            entries
                .iter()
                .any(|e| e.relative_path() == Path::new("clean/a.txt")),
            "clean subtree should be walked"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.kind() == WalkErrorKind::SymlinkCycle),
            "dirty subtree should produce SymlinkCycle"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_broken_symlink_at_root_with_follow() {
        let dir = tempdir().unwrap();
        let broken_link = dir.path().join("broken_root");
        std::os::unix::fs::symlink("/nonexistent/target/xyz", &broken_link).unwrap();

        let mut walk = walker()
            .follow_symlinks(true)
            .build()
            .walk(ctx(&broken_link));
        let first = walk.next().await;
        match first {
            Some(Err(err)) => assert!(err.is_fatal(), "broken symlink root should be fatal"),
            other => panic!("expected fatal error, got {other:?}"),
        }
        assert!(walk.next().await.is_none());
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_broken_symlink_at_root_with_canonicalize() {
        let dir = tempdir().unwrap();
        let broken_link = dir.path().join("broken_root");
        std::os::unix::fs::symlink("/nonexistent/target/xyz", &broken_link).unwrap();

        let mut walk = walker()
            .canonicalize_root(true)
            .build()
            .walk(ctx(&broken_link));
        let first = walk.next().await;
        match first {
            Some(Err(err)) => assert!(
                err.is_fatal(),
                "canonicalize of broken symlink should be fatal"
            ),
            other => panic!("expected fatal error, got {other:?}"),
        }
        assert!(walk.next().await.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_filter_receives_correct_relative_path() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::write(dir.path().join("sub/nested.txt"), "").unwrap();

        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_clone = std::sync::Arc::clone(&collected);

        let walk = walker()
            .recursive(true)
            .sort(true)
            .filter(move |entry| {
                collected_clone
                    .lock()
                    .unwrap()
                    .push(entry.relative_path().to_owned());
                true
            })
            .build()
            .walk(ctx(dir.path()));
        let _ = collect_entries(walk).await;

        let mut paths = collected.lock().unwrap().clone();
        paths.sort();
        assert_eq!(
            paths,
            vec![PathBuf::from("sub/nested.txt"), PathBuf::from("top.txt")]
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_symlink_metadata_is_target() {
        let dir = tempdir().unwrap();
        let content = "hello world, this is test content";
        fs::write(dir.path().join("real.txt"), content).unwrap();
        std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt"))
            .unwrap();

        let walk = walker()
            .follow_symlinks(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty());

        let link_entry = entries
            .iter()
            .find(|e| e.relative_path() == Path::new("link.txt"))
            .expect("link.txt should be yielded");
        assert_eq!(
            link_entry.metadata().len(),
            content.len() as u64,
            "symlink metadata should reflect target file size"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_max_depth_explicit_boundaries() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a")).unwrap();
        fs::write(dir.path().join("a/mid.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/deep.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b/c")).unwrap();
        fs::write(dir.path().join("a/b/c/bottom.txt"), "").unwrap();

        // max_depth(0): only root contents
        let (e, _) = collect_entries(
            walker()
                .max_depth(0)
                .sort(true)
                .build()
                .walk(ctx(dir.path())),
        )
        .await;
        let names: Vec<_> = e.iter().map(|e| norm(e.relative_path())).collect();
        assert_eq!(names, vec!["top.txt"]);

        // max_depth(1): root + one level
        let (e, _) = collect_entries(
            walker()
                .max_depth(1)
                .sort(true)
                .build()
                .walk(ctx(dir.path())),
        )
        .await;
        let mut names: Vec<_> = e.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(names, vec!["a/mid.txt", "top.txt"]);

        // max_depth(2): root + two levels
        let (e, _) = collect_entries(
            walker()
                .max_depth(2)
                .sort(true)
                .build()
                .walk(ctx(dir.path())),
        )
        .await;
        let mut names: Vec<_> = e.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(names, vec!["a/b/deep.txt", "a/mid.txt", "top.txt"]);

        // max_depth(3): all files
        let (e, _) = collect_entries(
            walker()
                .max_depth(3)
                .sort(true)
                .build()
                .walk(ctx(dir.path())),
        )
        .await;
        let mut names: Vec<_> = e.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(
            names,
            vec!["a/b/c/bottom.txt", "a/b/deep.txt", "a/mid.txt", "top.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_fswalker_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FsWalker>();
        assert_send_sync::<FsWalk>();
        assert_send_sync::<FsWalkContext>();
        assert_send_sync::<FsWalkerBuilder>();
        assert_send_sync::<FsWalkContextBuilder>();
    }
}
