/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp::Ordering;
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
    /// Files and subdirectories interleaved, in emission order.
    children: Vec<Child>,
    /// Non-fatal errors encountered reading individual entries.
    errors: Vec<WalkError>,
}

/// How the walk is positioned. The two traversals serve different callers, so
/// they keep separate state rather than one being emulated by the other.
enum Cursor {
    /// Files of a directory are emitted before descending. Subdirectories wait
    /// in a queue, which is what makes them claimable.
    Breadth {
        pending_dirs: VecDeque<PendingDir>,
        ready_files: VecDeque<DirEntry>,
    },
    /// Emission follows key order, so a subtree is descended at the position
    /// where it sorts. Each frame is a partially consumed directory.
    KeyOrder { stack: Vec<VecDeque<Child>> },
}

/// One entry of a directory: a file to yield, or a subdirectory to descend
/// into. Kept in a single list so sorting interleaves them, which is what
/// lets a nested key be emitted between two sibling files.
enum Child {
    File(DirEntry),
    Dir(PendingDir),
}

impl Child {
    fn path(&self) -> &Path {
        match self {
            Child::File(e) => &e.path,
            Child::Dir(d) => &d.path,
        }
    }

    /// Name bytes plus whether this is a directory, for [`cmp_key_form`].
    fn sort_name(&self) -> (&[u8], bool) {
        match self {
            Child::File(e) => (name_bytes(&e.path), false),
            Child::Dir(d) => (name_bytes(&d.path), true),
        }
    }
}

fn name_bytes(path: &Path) -> &[u8] {
    path.file_name()
        .map(|n| n.as_encoded_bytes())
        .unwrap_or(b"")
}

// Order two children as `ListObjectsV2` orders the keys they produce: a
// directory sorts as if its name ended in '/' (0x2F), so `a.txt` precedes
// `a/c`. Names within a directory never contain '/', so one trailing byte
// decides it.
fn cmp_key_form(a: &Child, b: &Child) -> Ordering {
    let (an, ad) = a.sort_name();
    let (bn, bd) = b.sort_name();
    let n = an.len().min(bn.len());
    match an[..n].cmp(&bn[..n]) {
        Ordering::Equal => {
            let at = an.get(n).copied().or(ad.then_some(b'/'));
            let bt = bn.get(n).copied().or(bd.then_some(b'/'));
            at.cmp(&bt)
        }
        ord => ord,
    }
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

type FilterFn = Arc<dyn Fn(&DirEntry) -> bool + Send + Sync>;

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
///
/// # Cloning
///
/// `FsWalker` is cheap to clone: all configuration fields are small values
/// and the optional filter is stored as `Arc<dyn Fn>`. Clone a configured
/// walker to reuse it across multiple operations without re-specifying the
/// configuration.
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
#[derive(Clone)]
pub struct FsWalker {
    follow_symlinks: bool,
    max_depth: usize,
    sort: bool,
    key_order: bool,
    canonicalize_root: bool,
    filter: Option<FilterFn>,
}

impl std::fmt::Debug for FsWalker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsWalker")
            .field("follow_symlinks", &self.follow_symlinks)
            .field("max_depth", &self.max_depth)
            .field("sort", &self.sort)
            .field("key_order", &self.key_order)
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

        let root_dir = PendingDir {
            path: root.clone(),
            depth: 0,
            ancestor_handles: Vec::new(),
        };
        let cursor = match (done, self.key_order) {
            (true, true) => Cursor::KeyOrder { stack: Vec::new() },
            (true, false) => Cursor::Breadth {
                pending_dirs: VecDeque::new(),
                ready_files: VecDeque::new(),
            },
            (false, true) => Cursor::KeyOrder {
                stack: vec![VecDeque::from([Child::Dir(root_dir)])],
            },
            (false, false) => Cursor::Breadth {
                pending_dirs: VecDeque::from([root_dir]),
                ready_files: VecDeque::new(),
            },
        };

        tracing::debug!(
            ?root,
            follow_symlinks = self.follow_symlinks,
            max_depth = self.max_depth,
            sort = self.sort,
            "fs walk started",
        );

        let root: Arc<Path> = root.into();

        FsWalk {
            config: Arc::new(self),
            root,
            cursor,
            pending_errors,
            done,
        }
    }
}

/// Builder for [`FsWalker`].
///
/// All fields have sensible defaults: non-recursive, symlinks not followed,
/// unsorted, no filter, `canonicalize_root` disabled.
#[derive(Clone, Default)]
pub struct FsWalkerBuilder {
    follow_symlinks: bool,
    max_depth: usize,
    sort: bool,
    key_order: bool,
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
    /// When `true`, each directory's entries are sorted by full path. Sorting is
    /// per directory, not global: the traversal is breadth-first, so a
    /// directory's files are emitted before its subdirectories are descended.
    /// For a globally ordered walk see [`key_order`](Self::key_order).
    ///
    /// When `false` (default), entries are returned in OS-native order.
    #[must_use]
    pub fn sort(mut self, sort: bool) -> Self {
        self.sort = sort;
        self
    }

    /// Emit entries in `ListObjectsV2` key order, so the walk can be merge-joined
    /// against a bucket listing. Defaults to `false`.
    ///
    /// This selects a depth-first traversal and compares a directory as if its
    /// name ended in `/`, which is what places `a.txt` before `a/c`. Sorting
    /// alone is not sufficient: a breadth-first walk emits every file of a
    /// directory before descending, so it can never place a nested key between
    /// two siblings.
    ///
    /// Implies [`sort`](Self::sort). Two costs come with it:
    /// [`try_claim_subtree`](FsWalk::try_claim_subtree) is unavailable, since
    /// ordered emission has to descend before it can know what comes next, and
    /// time-to-first-entry grows because subtrees sorting ahead of a sibling file
    /// must be read first.
    #[must_use]
    pub fn key_order(mut self, key_order: bool) -> Self {
        self.key_order = key_order;
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
        self.filter = Some(Arc::new(f));
        self
    }

    /// Build the [`FsWalker`] configuration.
    #[must_use]
    pub fn build(self) -> FsWalker {
        FsWalker {
            follow_symlinks: self.follow_symlinks,
            max_depth: self.max_depth,
            sort: self.sort,
            key_order: self.key_order,
            canonicalize_root: self.canonicalize_root,
            filter: self.filter,
        }
    }
}

impl Default for FsWalker {
    /// A default walker: non-recursive, does not follow symlinks, no filter.
    /// Equivalent to `FsWalker::builder().build()`.
    fn default() -> Self {
        FsWalker::builder().build()
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
///
/// Parallel enumeration is supported via [`try_claim_subtree`](Self::try_claim_subtree),
/// which splits off an independent `FsWalk` for a pending subdirectory. The caller
/// controls when and how many subtrees to claim, enabling backpressure-aware fan-out
/// without hidden coordination.
pub struct FsWalk {
    config: Arc<FsWalker>,
    root: Arc<Path>,
    cursor: Cursor,
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

            let dir = match &mut self.cursor {
                Cursor::Breadth {
                    pending_dirs,
                    ready_files,
                } => {
                    if let Some(entry) = ready_files.pop_front() {
                        return Some(Ok(entry));
                    }
                    match pending_dirs.pop_front() {
                        Some(dir) => dir,
                        None => {
                            self.done = true;
                            return None;
                        }
                    }
                }
                Cursor::KeyOrder { stack } => {
                    // Work the newest frame, so a subtree is emitted where it
                    // sorts rather than after its parent's files.
                    let child = loop {
                        match stack.last_mut() {
                            None => break None,
                            Some(frame) => match frame.pop_front() {
                                Some(child) => break Some(child),
                                None => {
                                    stack.pop();
                                }
                            },
                        }
                    };
                    match child {
                        None => {
                            self.done = true;
                            return None;
                        }
                        Some(Child::File(entry)) => return Some(Ok(entry)),
                        Some(Child::Dir(dir)) => dir,
                    }
                }
            };

            match self.read_dir(&dir.path, dir.depth, &dir.ancestor_handles) {
                Ok(result) => {
                    self.pending_errors.extend(result.errors);
                    match &mut self.cursor {
                        Cursor::Breadth {
                            pending_dirs,
                            ready_files,
                        } => {
                            for child in result.children {
                                match child {
                                    Child::File(entry) => ready_files.push_back(entry),
                                    Child::Dir(dir) => pending_dirs.push_back(dir),
                                }
                            }
                        }
                        Cursor::KeyOrder { stack } => stack.push(result.children.into()),
                    }
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

    /// Try to claim an independent subtree for parallel walking.
    ///
    /// Returns a new `FsWalk` that will walk one pending directory (and its
    /// descendants) independently. The parent walk continues with the remaining
    /// pending directories and will not yield entries from the claimed subtree.
    ///
    /// Returns `None` when splitting would not produce parallelism:
    ///
    /// - The walk is `done`.
    /// - The walk has fewer than two pending directories. Splitting hands the
    ///   only pending directory to the child and leaves the parent exhausted —
    ///   the child does the same work the parent would have done, and no
    ///   concurrency is gained. Advance the walk via [`next`](Self::next) until
    ///   the walk has discovered additional subdirectories, then try again.
    ///
    /// Entries yielded from the claimed subtree have `relative_path` computed
    /// against the original walk root, not the subtree root. Both walks share
    /// the same configuration (filters, depth limits, symlink policy).
    ///
    /// The claimed subtree and the parent walk can be advanced concurrently on
    /// different threads. There is no shared mutable state between them.
    ///
    /// Subtrees are claimed in the order [`next`](Self::next) would have visited
    /// them.
    ///
    /// Always returns `None` for a [`key_order`](FsWalkerBuilder::key_order)
    /// walk: emitting in key order requires descending into a subtree before a
    /// sibling that sorts after it can be emitted, so by the time entries come
    /// out there is little left to hand off, and handing off the next-needed
    /// subtree would stall the consumer waiting for it.
    pub fn try_claim_subtree(&mut self) -> Option<FsWalk> {
        let pending_dirs = match &mut self.cursor {
            Cursor::Breadth { pending_dirs, .. } => pending_dirs,
            Cursor::KeyOrder { .. } => return None,
        };
        if self.done || pending_dirs.len() < 2 {
            return None;
        }
        let claimed = pending_dirs.pop_front()?;
        Some(FsWalk {
            config: Arc::clone(&self.config),
            root: Arc::clone(&self.root),
            cursor: Cursor::Breadth {
                pending_dirs: VecDeque::from([claimed]),
                ready_files: VecDeque::new(),
            },
            pending_errors: VecDeque::new(),
            done: false,
        })
    }

    /// Returns `true` if this walk has no more entries to yield.
    ///
    /// A walk is exhausted when it has no pending directories, no buffered files,
    /// and no buffered errors. Equivalent to `next()` having returned (or being
    /// about to return) `None`.
    pub fn is_exhausted(&self) -> bool {
        if self.done {
            return true;
        }
        let cursor_empty = match &self.cursor {
            Cursor::Breadth {
                pending_dirs,
                ready_files,
            } => pending_dirs.is_empty() && ready_files.is_empty(),
            Cursor::KeyOrder { stack } => stack.iter().all(|f| f.is_empty()),
        };
        cursor_empty && self.pending_errors.is_empty()
    }

    /// Number of files already read and queued for yield. Diagnostic accessor.
    pub(crate) fn ready_files_len(&self) -> usize {
        match &self.cursor {
            Cursor::Breadth { ready_files, .. } => ready_files.len(),
            Cursor::KeyOrder { stack } => stack
                .iter()
                .flatten()
                .filter(|c| matches!(c, Child::File(_)))
                .count(),
        }
    }

    /// Number of directories queued for read. Diagnostic accessor.
    pub(crate) fn pending_dirs_len(&self) -> usize {
        match &self.cursor {
            Cursor::Breadth { pending_dirs, .. } => pending_dirs.len(),
            Cursor::KeyOrder { stack } => stack
                .iter()
                .flatten()
                .filter(|c| matches!(c, Child::Dir(_)))
                .count(),
        }
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

        // Ancestor chain for children of this directory. Only symlinked
        // directories can form a cycle, so when symlinks are not followed the
        // chain is never consulted and the open()+fstat is pure cost.
        let next_ancestors = if self.config.follow_symlinks {
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
            let mut chain = ancestor_handles.to_vec();
            chain.push(self_handle);
            chain
        } else {
            Vec::new()
        };

        let mut result = ReadDirResult {
            children: Vec::new(),
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
                                result.children.push(Child::Dir(PendingDir {
                                    path,
                                    depth: depth + 1,
                                    ancestor_handles: next_ancestors.clone(),
                                }));
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
                    self.push_file(&mut result.children, path, &metadata);
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
                self.push_file(&mut result.children, path, &metadata);
            } else if file_type.is_dir() && depth < self.config.max_depth {
                result.children.push(Child::Dir(PendingDir {
                    path,
                    depth: depth + 1,
                    ancestor_handles: next_ancestors.clone(),
                }));
            }
        }

        if self.config.key_order {
            result.children.sort_by(cmp_key_form);
        } else if self.config.sort {
            result.children.sort_by(|a, b| a.path().cmp(b.path()));
        }

        tracing::trace!(
            ?dir,
            children = result.children.len(),
            errors = result.errors.len(),
            "directory read",
        );

        Ok(result)
    }

    fn push_file(&self, children: &mut Vec<Child>, path: PathBuf, metadata: &Metadata) {
        let relative_path = path.strip_prefix(&self.root).unwrap_or(&path).to_path_buf();
        let entry = DirEntry {
            path,
            relative_path,
            metadata: metadata.clone(),
        };
        if self.config.filter.as_ref().is_none_or(|f| f(&entry)) {
            children.push(Child::File(entry));
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
    async fn test_walk_recursive_sort_is_breadth_first_per_dir() {
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

    // --- try_claim_subtree / is_exhausted tests ---

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_try_claim_subtree_returns_none_on_empty_walk() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();

        let mut walk = walker().build().walk(ctx(dir.path()));
        while walk.next().await.is_some() {}
        assert!(walk.try_claim_subtree().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_try_claim_subtree_returns_none_on_done_walk() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();

        let mut walk = walker().build().walk(ctx(dir.path()));
        while walk.next().await.is_some() {}
        assert!(walk.is_done());
        assert!(walk.try_claim_subtree().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_is_exhausted_on_fresh_walk() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();

        let walk = walker().build().walk(ctx(dir.path()));
        // Root is pending, so not exhausted
        assert!(!walk.is_exhausted());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_is_exhausted_after_full_drain() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();

        let mut walk = walker().build().walk(ctx(dir.path()));
        while walk.next().await.is_some() {}
        assert!(walk.is_exhausted());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_is_exhausted_matches_next_none() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/b.txt"), "").unwrap();

        let mut walk = walker().recursive(true).build().walk(ctx(dir.path()));
        // Before calling next, not exhausted (root is pending)
        assert!(!walk.is_exhausted());
        // Drain
        while walk.next().await.is_some() {}
        // After next() returns None, is_exhausted is true
        assert!(walk.is_exhausted());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_claim_subtree_after_root_read() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("sub1")).unwrap();
        fs::create_dir(dir.path().join("sub2")).unwrap();
        fs::create_dir(dir.path().join("sub3")).unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        // Read root: next() reads the root dir, populating pending_dirs with
        // sub1, sub2, sub3 and ready_files with top.txt. First call yields top.txt.
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("top.txt"));

        // Pending_dirs has 3 entries; can claim while >= 2 remain.
        assert!(walk.try_claim_subtree().is_some()); // claims sub1, [sub2, sub3] remain
        assert!(walk.try_claim_subtree().is_some()); // claims sub2, [sub3] remains
        assert!(walk.try_claim_subtree().is_none()); // only sub3 left, no split
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_parent_does_not_yield_from_claimed_subtree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/child.txt"), "").unwrap();
        fs::create_dir(dir.path().join("other")).unwrap();
        fs::write(dir.path().join("other/keep.txt"), "").unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        // Read root to populate pending_dirs with [other, sub] (sorted).
        let _ = walk.next().await; // yields top.txt

        // Claim "other" (first in pending_dirs after sort).
        let claimed = walk.try_claim_subtree().unwrap();

        // Parent should yield only entries from "sub", not from "other".
        let (parent_entries, _) = collect_entries(walk).await;
        let parent_paths: Vec<_> = parent_entries
            .iter()
            .map(|e| norm(e.relative_path()))
            .collect();
        assert_eq!(parent_paths, vec!["sub/child.txt"]);

        // Claimed walk yields entries from "other".
        let (claimed_entries, _) = collect_entries(claimed).await;
        let claimed_paths: Vec<_> = claimed_entries
            .iter()
            .map(|e| norm(e.relative_path()))
            .collect();
        assert_eq!(claimed_paths, vec!["other/keep.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_claimed_subtree_yields_all_descendants() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir_all(dir.path().join("sub/deep")).unwrap();
        fs::write(dir.path().join("sub/a.txt"), "").unwrap();
        fs::write(dir.path().join("sub/deep/b.txt"), "").unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // yields top.txt; pending_dirs = [sub, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims sub
        let (entries, _) = collect_entries(claimed).await;
        let mut names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        assert_eq!(names, vec!["sub/a.txt", "sub/deep/b.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_relative_path_consistent_across_parent_and_claim() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("root_file.txt"), "").unwrap();
        fs::create_dir_all(dir.path().join("c/d")).unwrap();
        fs::write(dir.path().join("c/d/file.txt"), "").unwrap();
        // Sibling subdir to allow claiming.
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // yields root_file.txt; pending_dirs = [c, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims c
        let (entries, _) = collect_entries(claimed).await;
        assert_eq!(entries.len(), 1);
        // relative_path is from original root, not from "c"
        assert_eq!(entries[0].relative_path(), Path::new("c/d/file.txt"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_nested_claim() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::create_dir(dir.path().join("a/sibling")).unwrap();
        fs::write(dir.path().join("a/mid.txt"), "").unwrap();
        fs::write(dir.path().join("a/b/deep.txt"), "").unwrap();
        fs::write(dir.path().join("a/sibling/keep.txt"), "").unwrap();
        // Sibling at root to allow claiming "a".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // yields top.txt; pending_dirs = [a, zz_unrelated]

        let mut claimed_a = walk.try_claim_subtree().unwrap(); // claims a
                                                               // Read "a": yields mid.txt, pending_dirs gets [b, sibling]
        let entry = claimed_a.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("a/mid.txt"));

        // Now claim "b" from claimed_a (sorted: b before sibling).
        let claimed_b = claimed_a.try_claim_subtree().unwrap();
        let (b_entries, _) = collect_entries(claimed_b).await;
        assert_eq!(b_entries.len(), 1);
        assert_eq!(b_entries[0].relative_path(), Path::new("a/b/deep.txt"));

        // claimed_a still has "sibling" left.
        let (rest, _) = collect_entries(claimed_a).await;
        let rest_paths: Vec<_> = rest.iter().map(|e| norm(e.relative_path())).collect();
        assert_eq!(rest_paths, vec!["a/sibling/keep.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_parallel_vs_serial_yields_same_entries() {
        use std::collections::HashSet;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a")).unwrap();
        fs::write(dir.path().join("a/a1.txt"), "").unwrap();
        fs::write(dir.path().join("a/a2.txt"), "").unwrap();
        fs::create_dir(dir.path().join("b")).unwrap();
        fs::write(dir.path().join("b/b1.txt"), "").unwrap();
        fs::create_dir(dir.path().join("b/c")).unwrap();
        fs::write(dir.path().join("b/c/c1.txt"), "").unwrap();

        // Serial
        let walk = walker().recursive(true).build().walk(ctx(dir.path()));
        let (serial_entries, _) = collect_entries(walk).await;
        let serial_set: HashSet<_> = serial_entries
            .iter()
            .map(|e| norm(e.relative_path()))
            .collect();

        // Parallel: claim all subtrees
        let mut walk = walker().recursive(true).build().walk(ctx(dir.path()));
        let entry = walk.next().await.unwrap().unwrap(); // top.txt
        let mut parallel_set: HashSet<String> = HashSet::new();
        parallel_set.insert(norm(entry.relative_path()));

        // Claim all available subtrees and drain them
        let mut subtrees = Vec::new();
        while let Some(sub) = walk.try_claim_subtree() {
            subtrees.push(sub);
        }
        for sub in subtrees {
            let (entries, _) = collect_entries(sub).await;
            for e in entries {
                parallel_set.insert(norm(e.relative_path()));
            }
        }
        // Drain parent remainder
        let (remaining, _) = collect_entries(walk).await;
        for e in remaining {
            parallel_set.insert(norm(e.relative_path()));
        }

        assert_eq!(serial_set, parallel_set);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_filter_applied_in_claimed_subtrees() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::write(dir.path().join("top.log"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/a.txt"), "").unwrap();
        fs::write(dir.path().join("sub/b.log"), "").unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
            .build()
            .walk(ctx(dir.path()));

        // Read root: yields top.txt (top.log filtered out).
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("top.txt"));

        // Claim "sub" subtree.
        let claimed = walk.try_claim_subtree().unwrap();
        let (entries, _) = collect_entries(claimed).await;
        // Only a.txt should pass filter, b.log should be filtered.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("sub/a.txt"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_max_depth_honored_across_splits() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("d0.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a")).unwrap();
        fs::write(dir.path().join("a/d1.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/d2.txt"), "").unwrap();
        fs::create_dir(dir.path().join("a/b/c")).unwrap();
        fs::write(dir.path().join("a/b/c/d3.txt"), "").unwrap();
        // Sibling subdir to allow claiming "a".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        // max_depth=2: should yield d0.txt, a/d1.txt, a/b/d2.txt but NOT a/b/c/d3.txt.
        let mut walk = walker()
            .max_depth(2)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("d0.txt"));

        // Claim subtree "a" (sorted before zz_unrelated).
        let claimed = walk.try_claim_subtree().unwrap();
        let (entries, _) = collect_entries(claimed).await;
        let mut names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        names.sort();
        // a/d1.txt (depth 1) and a/b/d2.txt (depth 2) should be yielded.
        // a/b/c/d3.txt (depth 3) should NOT.
        assert_eq!(names, vec!["a/b/d2.txt", "a/d1.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_sort_within_claimed_subtree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/zebra.txt"), "").unwrap();
        fs::write(dir.path().join("sub/apple.txt"), "").unwrap();
        fs::write(dir.path().join("sub/mango.txt"), "").unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt; pending_dirs = [sub, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims sub
        let (entries, _) = collect_entries(claimed).await;
        let names: Vec<_> = entries.iter().map(|e| norm(e.relative_path())).collect();
        assert_eq!(
            names,
            vec!["sub/apple.txt", "sub/mango.txt", "sub/zebra.txt"]
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_symlink_cycle_detection_in_claimed_subtree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("file.txt"), "").unwrap();
        // Create symlink back to root inside sub.
        std::os::unix::fs::symlink(dir.path(), sub.join("link_to_root")).unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt; pending_dirs = [sub, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims sub
        let (entries, errors) = collect_entries(claimed).await;

        // Should yield file.txt from sub.
        assert!(entries
            .iter()
            .any(|e| norm(e.relative_path()) == "sub/file.txt"));
        // Should detect cycle.
        assert!(
            errors
                .iter()
                .any(|e| e.kind() == WalkErrorKind::SymlinkCycle),
            "expected SymlinkCycle in claimed subtree"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_follow_symlinks_applies_to_claimed_subtrees() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("real.txt"), "").unwrap();
        std::os::unix::fs::symlink(sub.join("real.txt"), sub.join("link.txt")).unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt; pending_dirs = [sub, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims sub
        let (entries, errors) = collect_entries(claimed).await;
        assert!(errors.is_empty());
        // Both real.txt and link.txt should be yielded.
        assert_eq!(entries.len(), 2);
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fatal_error_in_claimed_subtree_does_not_affect_parent() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("good")).unwrap();
        fs::write(dir.path().join("good/ok.txt"), "").unwrap();
        let bad = dir.path().join("bad");
        fs::create_dir(&bad).unwrap();
        fs::write(bad.join("hidden.txt"), "").unwrap();
        fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();
        // Third sibling so we can claim "bad" and "good" while still leaving
        // a pending directory in the parent.
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        // Check if we can actually trigger permission denied (not running as root)
        let can_read = std::fs::read_dir(&bad).is_ok();
        if can_read {
            fs::set_permissions(&bad, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root, skip test
        }

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("top.txt"));

        // Claim "bad" subtree (sorted: bad comes before good)
        let mut claimed_bad = walk.try_claim_subtree().unwrap();
        // Claim "good" subtree
        let claimed_good = walk.try_claim_subtree().unwrap();

        // Bad subtree produces an error and then terminates
        let result = claimed_bad.next().await;
        assert!(matches!(result, Some(Err(_))));
        // After the error, the claimed walk is done
        assert!(claimed_bad.next().await.is_none());
        assert!(claimed_bad.is_done());

        // Good subtree should work fine, unaffected by bad subtree's error
        let (good_entries, good_errors) = collect_entries(claimed_good).await;
        assert!(good_errors.is_empty());
        assert_eq!(good_entries.len(), 1);
        assert_eq!(good_entries[0].relative_path(), Path::new("good/ok.txt"));

        // Restore permissions for cleanup
        fs::set_permissions(&bad, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_non_fatal_error_in_claimed_subtree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("real.txt"), "").unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", sub.join("broken")).unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .follow_symlinks(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt; pending_dirs = [sub, zz_unrelated]

        let claimed = walk.try_claim_subtree().unwrap(); // claims sub
        let (entries, errors) = collect_entries(claimed).await;

        // Should yield real.txt.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("sub/real.txt"));
        // Should have non-fatal error for broken symlink.
        assert_eq!(errors.len(), 1);
        assert!(!errors[0].is_fatal());
        assert_eq!(errors[0].kind(), WalkErrorKind::BrokenSymlink);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_claim_when_ready_files_non_empty() {
        let dir = tempdir().unwrap();
        // Root has both files and subdirs.
        fs::write(dir.path().join("a.txt"), "").unwrap();
        fs::write(dir.path().join("b.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/c.txt"), "").unwrap();
        // Sibling subdir to allow claiming "sub".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        // First next() reads root: files [a.txt, b.txt] go to ready_files,
        // [sub, zz_unrelated] go to pending_dirs. Returns a.txt.
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("a.txt"));

        // Claim subtree - should NOT disturb ready_files (b.txt still there).
        let claimed = walk.try_claim_subtree().unwrap(); // claims sub

        // Parent should still yield b.txt next.
        let entry = walk.next().await.unwrap().unwrap();
        assert_eq!(entry.relative_path(), Path::new("b.txt"));

        // Claimed should yield c.txt.
        let (entries, _) = collect_entries(claimed).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].relative_path(), Path::new("sub/c.txt"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_claim_empty_dir_subtree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("empty")).unwrap();
        // Sibling subdir to allow claiming "empty".
        fs::create_dir(dir.path().join("zz_unrelated")).unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt; pending_dirs = [empty, zz_unrelated]

        let mut claimed = walk.try_claim_subtree().unwrap(); // claims empty
        assert!(claimed.next().await.is_none());
        assert!(claimed.is_exhausted());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_root_that_is_empty_dir() {
        let dir = tempdir().unwrap();

        let mut walk = walker().build().walk(ctx(dir.path()));
        assert!(walk.next().await.is_none());
        assert!(walk.is_exhausted());
        assert!(walk.try_claim_subtree().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_try_claim_subtree_is_fifo_with_next() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        fs::create_dir(dir.path().join("aaa")).unwrap();
        fs::write(dir.path().join("aaa/a.txt"), "").unwrap();
        fs::create_dir(dir.path().join("bbb")).unwrap();
        fs::write(dir.path().join("bbb/b.txt"), "").unwrap();
        fs::create_dir(dir.path().join("ccc")).unwrap();
        fs::write(dir.path().join("ccc/c.txt"), "").unwrap();
        // 4th sibling so we can claim 3 while always leaving >= 2 pending.
        fs::create_dir(dir.path().join("ddd")).unwrap();
        fs::write(dir.path().join("ddd/d.txt"), "").unwrap();

        // With sort, pending_dirs after root read will be [aaa, bbb, ccc, ddd].
        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let _ = walk.next().await; // top.txt

        // Claims should come in FIFO order (same as next() would visit).
        let claimed1 = walk.try_claim_subtree().unwrap();
        let claimed2 = walk.try_claim_subtree().unwrap();
        let claimed3 = walk.try_claim_subtree().unwrap();

        let (e1, _) = collect_entries(claimed1).await;
        let (e2, _) = collect_entries(claimed2).await;
        let (e3, _) = collect_entries(claimed3).await;

        assert_eq!(e1[0].relative_path(), Path::new("aaa/a.txt"));
        assert_eq!(e2[0].relative_path(), Path::new("bbb/b.txt"));
        assert_eq!(e3[0].relative_path(), Path::new("ccc/c.txt"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_mixed_claim_and_next_interleaved() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        fs::write(dir.path().join("b.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub1")).unwrap();
        fs::write(dir.path().join("sub1/s1.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub2")).unwrap();
        fs::write(dir.path().join("sub2/s2.txt"), "").unwrap();
        fs::create_dir(dir.path().join("sub3")).unwrap();
        fs::write(dir.path().join("sub3/s3.txt"), "").unwrap();

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));

        // 1. First next() reads root: ready_files=[a.txt, b.txt], pending_dirs=[sub1, sub2, sub3]
        let e1 = walk.next().await.unwrap().unwrap();
        assert_eq!(e1.relative_path(), Path::new("a.txt"));

        // 2. Second next() yields b.txt from ready_files
        let e2 = walk.next().await.unwrap().unwrap();
        assert_eq!(e2.relative_path(), Path::new("b.txt"));

        // 3. After yielding root files, 3 pending subdirs remain
        assert!(!walk.is_exhausted());

        // 4. Third next() pops sub1 from pending_dirs, reads it, yields s1.txt
        let e3 = walk.next().await.unwrap().unwrap();
        assert_eq!(norm(e3.relative_path()), "sub1/s1.txt");

        // 5. try_claim_subtree() claims sub2 (next in BFS queue)
        let claimed = walk.try_claim_subtree().unwrap();

        // 6. Parent's next() skips sub2 (claimed), pops sub3, yields s3.txt
        let e4 = walk.next().await.unwrap().unwrap();
        assert_eq!(norm(e4.relative_path()), "sub3/s3.txt");

        // 7. Parent exhausted
        assert!(walk.next().await.is_none());

        // 8. Claimed walk yields s2.txt exactly once, then None
        let (claimed_entries, claimed_errors) = collect_entries(claimed).await;
        assert!(claimed_errors.is_empty());
        assert_eq!(claimed_entries.len(), 1);
        assert_eq!(norm(claimed_entries[0].relative_path()), "sub2/s2.txt");

        // 9. Total entries: exactly the 5 files, no duplicates, no missing
        let all: Vec<_> = [
            norm(e1.relative_path()),
            norm(e2.relative_path()),
            norm(e3.relative_path()),
            norm(e4.relative_path()),
            norm(claimed_entries[0].relative_path()),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            all,
            vec![
                "a.txt",
                "b.txt",
                "sub1/s1.txt",
                "sub3/s3.txt",
                "sub2/s2.txt"
            ]
        );
        let as_set: std::collections::HashSet<_> = all.into_iter().collect();
        assert_eq!(as_set.len(), 5);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_parallel_walks_run_concurrently() {
        use std::collections::HashSet;

        let dir = tempdir().unwrap();
        // Build a moderate tree: 3 subdirs, each with 2 nested subdirs, each with files
        for i in 0..3 {
            let sub = dir.path().join(format!("dir{i}"));
            fs::create_dir(&sub).unwrap();
            for j in 0..3 {
                fs::write(sub.join(format!("file{j}.txt")), "").unwrap();
            }
            for j in 0..2 {
                let nested = sub.join(format!("nested{j}"));
                fs::create_dir(&nested).unwrap();
                for k in 0..3 {
                    fs::write(nested.join(format!("deep{k}.txt")), "").unwrap();
                }
            }
        }
        // Root-level files
        for i in 0..3 {
            fs::write(dir.path().join(format!("root{i}.txt")), "").unwrap();
        }
        // Total: 3 root files + 3 dirs * (3 files + 2 nested * 3 files) = 3 + 3*9 = 30 files

        // Serial walk for reference
        let serial_walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        let (serial_entries, _) = collect_entries(serial_walk).await;
        let serial_set: HashSet<String> = serial_entries
            .iter()
            .map(|e| norm(e.relative_path()))
            .collect();
        assert_eq!(serial_set.len(), 30);

        // Parallel walk: prime, claim all subtrees, spawn concurrently
        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));
        // Prime: first next() reads root, populating pending_dirs
        let first = walk.next().await.unwrap().unwrap();
        let first_path = norm(first.relative_path());

        // Claim all available subtrees
        let mut claimed = Vec::new();
        while let Some(sub) = walk.try_claim_subtree() {
            claimed.push(sub);
        }

        // Spawn parent and all claimed subtrees concurrently
        let parent_task = tokio::spawn(async move { collect_entries(walk).await });
        let sub_tasks: Vec<_> = claimed
            .into_iter()
            .map(|sub| tokio::spawn(async move { collect_entries(sub).await }))
            .collect();

        // Await all concurrently
        let (parent_entries, _) = parent_task.await.expect("parent task panicked");
        let sub_results: Vec<_> = futures_util::future::join_all(sub_tasks).await;

        // Combine all entries
        let mut parallel_set: HashSet<String> = HashSet::new();
        parallel_set.insert(first_path);
        for e in &parent_entries {
            parallel_set.insert(norm(e.relative_path()));
        }
        for result in sub_results {
            let (entries, _) = result.expect("subtree task panicked");
            for e in &entries {
                parallel_set.insert(norm(e.relative_path()));
            }
        }

        assert_eq!(
            parallel_set, serial_set,
            "concurrent walks must yield the same entries as serial walk"
        );
    }

    // --- S3 key-order tests ---

    // Entries must be emitted in UTF-8 byte order, as ListObjectsV2 returns keys.
    fn assert_s3_key_order(entries: &[DirEntry]) {
        let emitted: Vec<String> = entries.iter().map(|e| norm(e.relative_path())).collect();
        let mut expected = emitted.clone();
        expected.sort();
        assert_eq!(emitted, expected, "walk must emit keys in UTF-8 byte order");
    }

    fn emitted(entries: &[DirEntry]) -> Vec<String> {
        entries.iter().map(|e| norm(e.relative_path())).collect()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_sorted_walk_key_order_nested_before_sibling() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("a")).unwrap();
        fs::write(dir.path().join("a/c"), "").unwrap();
        fs::write(dir.path().join("az.txt"), "").unwrap();

        let walk = walker()
            .recursive(true)
            .key_order(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        // 'z' (0x7A) > '/' (0x2F), so the nested key comes first.
        assert_eq!(emitted(&entries), vec!["a/c", "az.txt"]);
        assert_s3_key_order(&entries);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_sorted_walk_key_order_shared_prefix() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("test")).unwrap();
        fs::write(dir.path().join("test/inner.txt"), "").unwrap();
        fs::write(dir.path().join("test-123.txt"), "").unwrap();
        fs::write(dir.path().join("test.txt"), "").unwrap();
        fs::write(dir.path().join("test0.txt"), "").unwrap();

        let walk = walker()
            .recursive(true)
            .key_order(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        // Divergent bytes: '-' 0x2D, '.' 0x2E, '/' 0x2F, '0' 0x30.
        assert_eq!(
            emitted(&entries),
            vec!["test-123.txt", "test.txt", "test/inner.txt", "test0.txt"]
        );
        assert_s3_key_order(&entries);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_sorted_walk_key_order_across_depths() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("b/c")).unwrap();
        fs::write(dir.path().join("b/c/d.txt"), "").unwrap();
        fs::write(dir.path().join("b/z.txt"), "").unwrap();
        fs::write(dir.path().join("b.txt"), "").unwrap();
        fs::write(dir.path().join("ba.txt"), "").unwrap();

        let walk = walker()
            .recursive(true)
            .key_order(true)
            .build()
            .walk(ctx(dir.path()));
        let (entries, errors) = collect_entries(walk).await;
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        assert_eq!(
            emitted(&entries),
            vec!["b.txt", "b/c/d.txt", "b/z.txt", "ba.txt"]
        );
        assert_s3_key_order(&entries);
    }

    // Random tree; returns the file count. Fragments bracket '/' (0x2F) in byte
    // order, where interleaving matters. ASCII only until NFC/NFD is settled.
    fn build_random_tree(rng: &mut fastrand::Rng, root: &Path, depth: usize) -> usize {
        const FRAGMENTS: &[&str] = &[
            "a", "a.txt", "a-1", "a+1", "a 1", "a%1", "a#1", "a0", "az", "b", "b.dat", "bz",
        ];
        let mut files = 0;
        for _ in 0..rng.usize(1..=6) {
            let path = root.join(FRAGMENTS[rng.usize(..FRAGMENTS.len())]);
            if path.exists() {
                continue;
            }
            if depth > 0 && rng.bool() {
                fs::create_dir(&path).unwrap();
                files += build_random_tree(rng, &path, depth - 1);
            } else {
                fs::write(&path, "").unwrap();
                files += 1;
            }
        }
        files
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_sorted_walk_key_order_property() {
        for seed in 0..32u64 {
            let mut rng = fastrand::Rng::with_seed(seed);
            let dir = tempdir().unwrap();
            let expected = build_random_tree(&mut rng, dir.path(), 3);

            let walk = walker()
                .recursive(true)
                .key_order(true)
                .build()
                .walk(ctx(dir.path()));
            let (entries, errors) = collect_entries(walk).await;
            assert!(
                errors.is_empty(),
                "seed {seed}: unexpected errors: {errors:?}"
            );
            assert_eq!(entries.len(), expected, "seed {seed}: wrong entry count");

            let keys = emitted(&entries);
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "seed {seed}: emitted out of key order");
        }
    }

    // Directories are read on demand, so removing an entry from a directory the
    // walk has not reached yet must not disturb it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_walk_entry_removed_before_its_directory_is_read() {
        let dir = tempdir().unwrap();
        for sub in ["a", "b"] {
            fs::create_dir(dir.path().join(sub)).unwrap();
            for name in ["1", "2", "3"] {
                fs::write(dir.path().join(sub).join(name), "").unwrap();
            }
        }

        let mut walk = walker()
            .recursive(true)
            .sort(true)
            .build()
            .walk(ctx(dir.path()));

        // The first entry comes from `a`, so `b` is still unread.
        let first = walk.next().await.unwrap().unwrap();
        assert!(norm(first.relative_path()).starts_with("a/"));
        fs::remove_file(dir.path().join("b/2")).unwrap();

        let mut keys = vec![norm(first.relative_path())];
        while let Some(result) = walk.next().await {
            keys.push(norm(result.expect("walk must not fail").relative_path()));
        }
        keys.sort();
        assert_eq!(keys, vec!["a/1", "a/2", "a/3", "b/1", "b/3"]);
    }
}
