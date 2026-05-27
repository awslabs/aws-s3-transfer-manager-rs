/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! State machine for `download_objects` composite transfer.
//!
//! Mirrors the `upload_objects` state machine: a single S3 listing walker
//! discovers objects, the parent spawns child downloads (via
//! [`Download::orchestrate_to_path_child`]), and reaps them as they complete.

#![allow(dead_code)] // Not wired into production yet (chunk 2)

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use aws_sdk_s3::types::Object;
use parking_lot::Mutex;
use path_clean::PathClean;

use crate::error::{self, Error, ErrorKind};
use crate::io::walk::S3Walk;
use crate::operation::download::{Download, DownloadInput, ManagedDownloadHandle};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::{FailedDownload, FailedTransferPolicy};

/// How many entries to claim from pending_entries per poll_work cycle.
/// Limits how long the state lock is released during child orchestration.
const SPAWN_BATCH_SIZE: usize = 32;

/// Maximum entries to drain from the walker per AdvanceWalker work item.
/// Prevents a single work item from holding the executor across multiple
/// ListObjectsV2 pages. Matches S3's default MaxKeys.
const MAX_ENTRIES_PER_WALK: usize = 1000;

/// Default S3 key delimiter.
const DEFAULT_DELIMITER: &str = "/";

/// Work items produced by [`DownloadObjectsTransfer::poll_work`].
pub(crate) enum DownloadObjectsWork {
    /// Advance the S3 listing walker by one page/entry.
    AdvanceWalker { walk: Option<S3Walk> },
    /// Join a batch of terminal child downloads.
    JoinChildren { batch: Vec<ChildTransfer> },
}

impl std::fmt::Debug for DownloadObjectsWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdvanceWalker { .. } => f.debug_struct("AdvanceWalker").finish_non_exhaustive(),
            Self::JoinChildren { batch } => f
                .debug_struct("JoinChildren")
                .field("count", &batch.len())
                .finish(),
        }
    }
}

/// Tracked child download — handle + the S3 key for error reporting.
pub(crate) struct ChildTransfer {
    pub(crate) handle: ManagedDownloadHandle,
    pub(crate) key: String,
}

struct State {
    /// The S3 listing walker (taken out during AdvanceWalker execution).
    walk: Option<S3Walk>,

    /// Whether a walker work item is currently in flight.
    walk_in_flight: bool,

    /// Objects discovered by the walker, waiting to be spawned as children.
    pending_entries: std::collections::VecDeque<Object>,

    /// Active child downloads keyed by their TransferId.
    children: HashMap<TransferId, ChildTransfer>,

    /// Entries claimed from pending_entries but not yet inserted into children.
    children_reserved: usize,

    /// JoinChildren batches currently executing.
    reaping_in_flight: usize,

    /// Failed downloads (for FailedTransferPolicy::Continue).
    failed: Vec<FailedDownload>,

    /// Count of successfully downloaded objects.
    successful_downloads: u64,
}

#[derive(Clone)]
pub(crate) struct DownloadObjectsTransfer {
    inner: Arc<DownloadObjectsTransferInner>,
}

impl std::fmt::Debug for DownloadObjectsTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadObjectsTransfer")
            .finish_non_exhaustive()
    }
}

struct DownloadObjectsTransferInner {
    ctx: TransferContext,
    state: Mutex<State>,
    bucket: String,
    destination: PathBuf,
    key_prefix: Option<String>,
    delimiter: Option<String>,
    failure_policy: FailedTransferPolicy,
    pipeline_depth: usize,
}

impl DownloadObjectsTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        walk: S3Walk,
        bucket: String,
        destination: PathBuf,
        key_prefix: Option<String>,
        delimiter: Option<String>,
        failure_policy: FailedTransferPolicy,
        pipeline_depth: usize,
    ) -> Self {
        Self {
            inner: Arc::new(DownloadObjectsTransferInner {
                ctx,
                state: Mutex::new(State {
                    walk: Some(walk),
                    walk_in_flight: false,
                    pending_entries: std::collections::VecDeque::new(),
                    children: HashMap::new(),
                    children_reserved: 0,
                    reaping_in_flight: 0,
                    failed: Vec::new(),
                    successful_downloads: 0,
                }),
                bucket,
                destination,
                key_prefix,
                delimiter,
                failure_policy,
                pipeline_depth,
            }),
        }
    }

    pub(crate) fn successful_downloads(&self) -> u64 {
        // Safe: only called after terminal
        self.inner.state.lock().successful_downloads
    }

    pub(crate) fn take_failed(&self) -> Option<Vec<FailedDownload>> {
        let mut state = self.inner.state.lock();
        if state.failed.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut state.failed))
        }
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let mut state = self.inner.state.lock();

        // Phase 1: drain terminal children
        if let Some(batch) = self.drain_terminal_children(&mut state) {
            state.reaping_in_flight += 1;
            return PollWork::Ready(IoRequest {
                data: Some(Box::new(DownloadObjectsWork::JoinChildren { batch })),
            });
        }

        // Check if we're done
        if !self.inner.ctx.is_active() {
            if let Some(result) = self.check_terminal(&state) {
                return result;
            }
        }

        // Phase 2: spawn children from pending entries (lock released)
        let to_spawn = self.claim_spawn_batch(&mut state);
        if !to_spawn.is_empty() {
            drop(state);
            let spawned = self.spawn_children(to_spawn);
            state = self.inner.state.lock();
            self.merge_spawned(&mut state, spawned);
        }

        // Phase 3: check terminal, dispatch walk
        if let Some(result) = self.check_terminal(&state) {
            return result;
        }

        self.dispatch_walk(&mut state)
    }

    /// Collect children that have reached a terminal state (success, failure,
    /// or cancellation) for reaping via JoinChildren.
    fn drain_terminal_children(&self, state: &mut State) -> Option<Vec<ChildTransfer>> {
        let terminal_ids: Vec<TransferId> = state
            .children
            .iter()
            .filter(|(_, child)| child.handle.status() != crate::types::TransferStatus::Active)
            .map(|(id, _)| *id)
            .collect();

        if terminal_ids.is_empty() {
            return None;
        }

        let batch: Vec<ChildTransfer> = terminal_ids
            .into_iter()
            .filter_map(|id| state.children.remove(&id))
            .collect();

        Some(batch)
    }

    /// Claim up to SPAWN_BATCH_SIZE entries from pending_entries for
    /// child spawning. Increments children_reserved so concurrent
    /// poll_work callers don't overshoot pipeline_depth.
    fn claim_spawn_batch(&self, state: &mut State) -> Vec<Object> {
        let active_children = state.children.len() + state.children_reserved;
        let available = self.inner.pipeline_depth.saturating_sub(active_children);
        let count = available
            .min(SPAWN_BATCH_SIZE)
            .min(state.pending_entries.len());

        let batch: Vec<Object> = state.pending_entries.drain(..count).collect();
        state.children_reserved += batch.len();
        batch
    }

    fn spawn_children(&self, entries: Vec<Object>) -> Vec<(Object, Result<ChildTransfer, Error>)> {
        let handle = &self.inner.ctx.handle;
        let parent_id = self.inner.ctx.id.id;

        entries
            .into_iter()
            .map(|obj| {
                let key = obj
                    .key()
                    .expect("S3Walk yields objects with keys")
                    .to_string();
                let result = self.spawn_single_child(handle, &key, parent_id);
                (obj, result.map(|h| ChildTransfer { handle: h, key }))
            })
            .collect()
    }

    fn spawn_single_child(
        &self,
        handle: &Arc<crate::client::Handle>,
        key: &str,
        parent_id: u64,
    ) -> Result<ManagedDownloadHandle, Error> {
        let dest_path = local_key_path(
            &self.inner.destination,
            key,
            self.inner.key_prefix.as_deref(),
            self.inner.delimiter.as_deref(),
        )?;

        // Create parent directories synchronously. On managed threads this
        // is inline; acceptable because it's in the orchestration path (not
        // the data-plane hot path) and typically a no-op after the first few.
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error::Error::new(
                    ErrorKind::IOError,
                    format!("failed to create directory for key '{key}': {e}"),
                )
            })?;
        }

        let input = DownloadInput::builder()
            .bucket(&self.inner.bucket)
            .key(key)
            .build()
            .expect("bucket and key are set");

        // Create temp file synchronously (we're in poll_work, not async).
        // orchestrate_to_path is async (tokio::fs); for child spawning we
        // use std::fs + orchestrate_with_sink directly.
        let unique_id = fastrand::u32(..);
        let temp_name = format!(
            "{}.s3tmp.{:08x}",
            dest_path.file_name().unwrap_or_default().to_string_lossy(),
            unique_id
        );
        let temp_path = dest_path.with_file_name(&temp_name);

        let file = std::fs::File::create(&temp_path).map_err(|e| {
            error::Error::new(
                ErrorKind::IOError,
                format!("failed to create temp file for key '{key}': {e}"),
            )
        })?;

        let inner = Download::orchestrate_with_sink(
            handle.clone(),
            input,
            file,
            0, // range_start
            true,
            Some(parent_id),
        )?;
        Ok(ManagedDownloadHandle::new(inner, temp_path, dest_path))
    }

    /// Merge results of child spawning back into state. Decrements
    /// `children_reserved` for each entry and either inserts the child
    /// into the active set or records the failure.
    fn merge_spawned(
        &self,
        state: &mut State,
        spawned: Vec<(Object, Result<ChildTransfer, Error>)>,
    ) {
        for (obj, result) in spawned {
            state.children_reserved -= 1;
            match result {
                Ok(child) => {
                    let id = child.handle.transfer_id();
                    state.children.insert(id, child);
                }
                Err(err) => {
                    let key = obj
                        .key()
                        .expect("S3Walk yields objects with keys")
                        .to_string();
                    let failed_input = DownloadInput::builder()
                        .bucket(&self.inner.bucket)
                        .key(&key)
                        .build()
                        .unwrap();
                    state.failed.push(FailedDownload {
                        input: failed_input,
                        error: err,
                    });
                    if self.inner.failure_policy == FailedTransferPolicy::Abort {
                        self.inner.ctx.set_failed(Error::new(
                            ErrorKind::ChildOperationFailed,
                            format!("download failed for key '{key}'"),
                        ));
                    }
                }
            }
        }
    }

    fn check_terminal(&self, state: &State) -> Option<PollWork> {
        if !self.inner.ctx.is_active() {
            // Wait for all in-flight work to drain
            if state.children.is_empty()
                && state.children_reserved == 0
                && state.reaping_in_flight == 0
                && !state.walk_in_flight
            {
                self.inner.ctx.signal_terminal();
                return Some(PollWork::Done);
            }
        }

        // Check if walk is done and no more work to do
        let walk_done = state.walk.as_ref().map_or(true, |w| w.is_done()) && !state.walk_in_flight;
        if walk_done
            && state.pending_entries.is_empty()
            && state.children.is_empty()
            && state.children_reserved == 0
            && state.reaping_in_flight == 0
        {
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        None
    }

    fn dispatch_walk(&self, state: &mut State) -> PollWork {
        // Don't dispatch if walk is already in flight or done
        if state.walk_in_flight {
            return PollWork::Pending;
        }

        // Don't dispatch if pipeline is full (backpressure on listing)
        let active = state.children.len() + state.children_reserved + state.pending_entries.len();
        if active >= self.inner.pipeline_depth {
            return PollWork::Pending;
        }

        // Take the walk out for execution
        let walk = match state.walk.take() {
            Some(w) if !w.is_done() => w,
            Some(w) => {
                // Walk is done, put it back
                state.walk = Some(w);
                return PollWork::Pending;
            }
            None => return PollWork::Pending,
        };

        state.walk_in_flight = true;
        PollWork::Ready(IoRequest {
            data: Some(Box::new(DownloadObjectsWork::AdvanceWalker {
                walk: Some(walk),
            })),
        })
    }

    // -----------------------------------------------------------------------
    // execute
    // -----------------------------------------------------------------------

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work
            .data
            .as_mut()
            .expect("work data must be set")
            .as_any_mut()
            .downcast_mut::<DownloadObjectsWork>()
            .expect("work data must be DownloadObjectsWork");

        match data {
            DownloadObjectsWork::AdvanceWalker { walk } => {
                self.execute_advance_walker(walk.take().unwrap()).await
            }
            DownloadObjectsWork::JoinChildren { batch } => {
                self.execute_join_children(std::mem::take(batch)).await
            }
        }
    }

    async fn execute_advance_walker(&self, mut walk: S3Walk) -> WorkOutcome {
        // Drain up to a page worth of entries from the walker
        let mut entries = Vec::new();
        let mut fatal_error = None;

        // Pull entries until the walker needs to make another network call
        // (returns None from ready_objects) or we hit a batch limit.
        loop {
            match walk.next().await {
                Some(Ok(obj)) => {
                    entries.push(obj);
                    // Limit to one page worth of results per work item so we
                    // don't hold the executor across multiple ListObjectsV2
                    // calls, starving other transfers.
                    if entries.len() >= MAX_ENTRIES_PER_WALK {
                        break;
                    }
                }
                Some(Err(err)) => {
                    if err.is_fatal() {
                        fatal_error = Some(err);
                        break;
                    }
                    // Non-fatal errors: log and continue
                    tracing::warn!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        error = %err,
                        "non-fatal walker error, continuing"
                    );
                }
                None => break, // walker exhausted or needs next page
            }
        }

        // Put entries and walk back into state
        let mut state = self.inner.state.lock();
        state.walk_in_flight = false;

        if let Some(err) = fatal_error {
            self.inner.ctx.set_failed(Error::new(
                ErrorKind::ObjectNotDiscoverable,
                format!("S3 listing failed: {err}"),
            ));
        } else {
            state.pending_entries.extend(entries);
            if !walk.is_done() {
                state.walk = Some(walk);
            }
        }

        drop(state);
        // Wake: new entries in pending_entries unblock poll_work from Pending.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_join_children(&self, batch: Vec<ChildTransfer>) -> WorkOutcome {
        for child in batch {
            let key = child.key;
            match child.handle.join().await {
                Ok(_output) => {
                    let mut state = self.inner.state.lock();
                    state.successful_downloads += 1;
                }
                Err(err) => {
                    let mut state = self.inner.state.lock();
                    let failed_input = DownloadInput::builder()
                        .bucket(&self.inner.bucket)
                        .key(&key)
                        .build()
                        .unwrap();
                    state.failed.push(FailedDownload {
                        input: failed_input,
                        error: err,
                    });
                    if self.inner.failure_policy == FailedTransferPolicy::Abort {
                        self.inner.ctx.set_failed(Error::new(
                            ErrorKind::ChildOperationFailed,
                            format!("download failed for key '{key}'"),
                        ));
                    }
                }
            }
        }

        let mut state = self.inner.state.lock();
        state.reaping_in_flight -= 1;
        drop(state);
        // Wake: freed pipeline capacity may unblock dispatch_walk or spawning.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }
}

impl Transfer for DownloadObjectsTransfer {
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    fn poll_work(&self) -> PollWork {
        self.poll_work()
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(self.execute(work))
    }

    fn on_terminal(&self) {}
}

fn strip_key_prefix<'a>(key: &'a str, prefix: Option<&str>, delimiter: Option<&str>) -> &'a str {
    let prefix = prefix.unwrap_or("");
    let delim = delimiter.unwrap_or(DEFAULT_DELIMITER);

    if key.is_empty() || prefix.is_empty() || !key.starts_with(prefix) || !key.contains(delim) {
        return key;
    }

    let stripped = &key[prefix.len()..];

    if prefix.ends_with(delim) || !stripped.starts_with(delim) {
        return stripped;
    }

    &stripped[1..]
}

fn replace_delim<'a>(key: &'a str, delimiter: Option<&str>, path_separator: &str) -> Cow<'a, str> {
    match delimiter {
        Some(delim) if delim != path_separator => {
            let replaced = key.replace(delim, path_separator);
            Cow::Owned(replaced)
        }
        _ => Cow::Borrowed(key),
    }
}

/// Derive the local filesystem path for a given S3 key.
///
/// Strips the configured prefix, replaces the delimiter with the OS path
/// separator, joins with the destination root, normalizes via `path_clean`,
/// and validates the result stays within the root (path traversal guard).
pub(crate) fn local_key_path(
    root_dir: &Path,
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Result<PathBuf, Error> {
    let stripped = strip_key_prefix(key, prefix, delimiter);
    let relative_path = replace_delim(stripped, delimiter, std::path::MAIN_SEPARATOR_STR);

    let local_path = root_dir.join(relative_path.as_ref()).clean();
    validate_path(root_dir, &local_path, key)?;

    Ok(local_path)
}

fn validate_path(root_dir: &Path, local_path: &Path, key: &str) -> Result<(), Error> {
    if !local_path.starts_with(root_dir) {
        return Err(Error::new(
            ErrorKind::InputInvalid,
            format!(
                "Unable to download key: '{key}', its relative path resolves \
                 outside the target destination directory"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::walk::{S3WalkContext, S3Walker};
    use crate::transfer::TransferContext;
    use crate::DEFAULT_CONCURRENCY;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::timeout;

    fn mock_s3_success() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").size(5).build(),
                    Object::builder().key("b.txt").size(5).build(),
                    Object::builder().key("c.txt").size(5).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get])
    }

    fn mock_s3_get_failure() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("a.txt").size(5).build()]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_error(|| {
            aws_sdk_s3::operation::get_object::GetObjectError::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("NoSuchKey")
                    .message("not found")
                    .build(),
            )
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get])
    }

    fn mock_s3_list_failure() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("access denied")
                    .build(),
            )
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list])
    }

    fn setup(
        dest: &Path,
        policy: FailedTransferPolicy,
        s3_client: aws_sdk_s3::Client,
    ) -> (
        DownloadObjectsTransfer,
        crate::transfer::StateMachineTerminalReceiver,
    ) {
        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::new_for_test(config, DEFAULT_CONCURRENCY);

        let walk = S3Walker::builder().build().walk(
            S3WalkContext::builder()
                .client(s3_client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle);
        ctx.handle
            .scheduler
            .register_empty_group_for_test(ctx.id.id);

        let transfer = DownloadObjectsTransfer::new(
            ctx,
            walk,
            "test-bucket".to_string(),
            dest.to_path_buf(),
            None,
            None,
            policy,
            1000,
        );
        (transfer, completion_rx)
    }

    async fn drive_transfer(transfer: &DownloadObjectsTransfer) {
        loop {
            match transfer.poll_work() {
                PollWork::Ready(mut work) => {
                    transfer.execute(&mut work).await;
                }
                PollWork::Pending => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                PollWork::Done => break,
            }
        }
    }

    // --- Key derivation tests (ported from worker.rs) ---

    #[test]
    fn test_strip_key_prefix() {
        let cases: &[(&str, Option<&str>, Option<&str>, &str)] = &[
            ("no-delim", None, None, "no-delim"),
            ("no-delim", Some(""), None, "no-delim"),
            (
                "delim/with/separator",
                Some(""),
                None,
                "delim/with/separator",
            ),
            ("", Some("no-delim"), None, ""),
            ("no-delim", Some("no-delim"), None, "no-delim"),
            ("delim/", Some("delim"), None, ""),
            ("not-in-key", Some("prefix"), None, "not-in-key"),
            ("notes/2021/1.txt", Some("notes/2021"), None, "1.txt"),
            ("notes/2021/1.txt", Some("notes/2021/"), None, "1.txt"),
            (
                "top-level/sub-folder/1.txt",
                Some("top-"),
                None,
                "level/sub-folder/1.txt",
            ),
            (
                "someInnerFolder/another/file1.txt",
                Some("someInner"),
                None,
                "Folder/another/file1.txt",
            ),
            (
                "someInnerF/another/file1.txt",
                Some("someInner"),
                None,
                "F/another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner"),
                None,
                "another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner/a"),
                None,
                "nother/file1.txt",
            ),
        ];
        for (key, prefix, delim, expected) in cases {
            let actual = strip_key_prefix(key, *prefix, *delim);
            assert_eq!(
                *expected, actual,
                "key={key:?} prefix={prefix:?} delim={delim:?}"
            );
        }
    }

    #[test]
    fn test_strip_key_prefix_delims() {
        for delim in ["/", "//", "\\", "|", "delim"] {
            let prefix = format!("notes{delim}2021{delim}");
            let key = format!("notes{delim}2021{delim}1.txt");
            let actual = strip_key_prefix(&key, Some(&prefix), Some(delim));
            assert_eq!("1.txt", actual, "delim={delim:?}");
        }
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn test_local_key_path_comprehensive() {
        let root = Path::new("test");
        let cases: &[(&str, Option<&str>, Option<&str>, Result<&str, &str>)] = &[
            ("2023/Jan/1.png", None, None, Ok("test/2023/Jan/1.png")),
            ("2023/Jan/1.png", Some("2023/Jan/"), None, Ok("test/1.png")),
            ("2023/Jan/1.png", Some("2023/Jan"), None, Ok("test/1.png")),
            ("2023-Jan-1.png", None, Some("-"), Ok("test/2023/Jan/1.png")),
            ("2023-Jan-.png", None, Some("-"), Ok("test/2023/Jan/.png")),
            (
                "many////delims-in-a-row",
                None,
                Some("/"),
                Ok("test/many/delims-in-a-row"),
            ),
            ("../2023/Jan/1.png", None, None, Err("outside the target")),
            ("/2023/Jan/1.png", None, None, Err("outside the target")),
            (
                "foo/../2023/../../Jan/1.png",
                None,
                None,
                Err("outside the target"),
            ),
            (
                "../test-2/object.dat",
                None,
                None,
                Err("outside the target"),
            ),
        ];
        for (key, prefix, delim, expected) in cases {
            let actual = local_key_path(root, key, *prefix, *delim);
            match expected {
                Ok(path) => {
                    let actual =
                        actual.unwrap_or_else(|e| panic!("expected Ok for key={key:?}: {e}"));
                    assert_eq!(Path::new(path), actual, "key={key:?}");
                }
                Err(_) => {
                    let err = actual.expect_err(&format!("expected Err for key={key:?}"));
                    assert_eq!(err.kind(), &ErrorKind::InputInvalid, "key={key:?}");
                }
            }
        }
    }

    // --- State machine tests ---

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_happy_path_3_files() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 3);
        assert!(transfer.take_failed().is_none());

        // Verify files exist
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
        assert!(dir.path().join("c.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_continue() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_get_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        let failed = transfer.take_failed().unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_abort() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Abort,
            mock_s3_get_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        assert!(transfer.inner.ctx.is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fatal_walker_error() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_list_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert!(transfer.inner.ctx.is_failed());
        assert_eq!(transfer.successful_downloads(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_empty_listing() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        assert!(transfer.take_failed().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_traversal_rejected_as_failure() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder()
                    .key("../../../etc/passwd")
                    .size(100)
                    .build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        let failed = transfer.take_failed().unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_nested_keys_create_directories() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder()
                        .key("photos/2023/jan/img.jpg")
                        .size(5)
                        .build(),
                    Object::builder().key("docs/readme.txt").size(5).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 2);
        assert!(dir.path().join("photos/2023/jan/img.jpg").exists());
        assert!(dir.path().join("docs/readme.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_prefix_stripping() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("backup/2023/a.txt").size(3).build(),
                    Object::builder().key("backup/2023/b.txt").size(3).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(3)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::new_for_test(config, DEFAULT_CONCURRENCY);

        let walk = S3Walker::builder().prefix("backup/2023/").build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle);
        ctx.handle
            .scheduler
            .register_empty_group_for_test(ctx.id.id);

        let transfer = DownloadObjectsTransfer::new(
            ctx,
            walk,
            "test-bucket".to_string(),
            dir.path().to_path_buf(),
            Some("backup/2023/".to_string()),
            None,
            FailedTransferPolicy::Continue,
            1000,
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 2);
        // Prefix stripped: "backup/2023/a.txt" → "a.txt"
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_many_objects() {
        let dir = tempdir().unwrap();

        let objects: Vec<Object> = (0..20)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i:02}.txt"))
                    .size(2)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(2)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 20);
        for i in 0..20 {
            assert!(dir.path().join(format!("file{i:02}.txt")).exists());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation() {
        let dir = tempdir().unwrap();

        // Large listing so the transfer doesn't complete before we cancel
        let objects: Vec<Object> = (0..100)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i}.txt"))
                    .size(5)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, _completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        // Cancel after first poll_work cycle
        transfer.inner.ctx.set_cancelled();

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
        })
        .await
        .expect("cancelled transfer should terminate");

        assert!(!transfer.inner.ctx.is_active());
    }
}
