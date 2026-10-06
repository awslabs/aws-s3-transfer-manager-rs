/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::io::key::stream::{KeyStream, StreamError};
use crate::operation::sync::compare::{Compare, Decision, Verdict};
use crate::operation::sync::walk::{Pairing, Progress, Walk};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};

// Pairings per merge work item. A merge draws from either side, so a listing page cannot size the
// batch. The bound limits how long one work item holds an executor slot.
const MERGE_BATCH: usize = 64;

// How many failures a run keeps. The walk reports failures per entry, so keeping every failure
// would grow memory with the tree. The run keeps the first failures and counts the rest.
//
// The sample comes from the first failing subtree. It does not represent the whole run.
const FAILURES_KEPT: usize = 64;

// Build a child from a source entry. Each direction supplies the client, bucket, and roots the
// child needs.
pub(crate) trait SpawnChild<S>: Send + Sync {
    // Enqueue a child for this key and hand back a way to ask after it. The key is the relative
    // one both sides agree on; turning it into an address is this implementation's business.
    fn spawn(&self, key: &str, source: &S, parent: u64)
        -> Result<ChildHandle, crate::error::Error>;
}

// How many keys go in one delete request. `DeleteObjects` takes no more. Batching makes a large
// delete affordable: a thousand keys sent singly cost a thousand round trips and a thousand
// dispatch charges, where one batch costs one of each.
const DELETE_BATCH: usize = 1000;

// How many times a batch asks again about keys S3 refused for load.
//
// A whole request failure and a per-key refusal need different retries. A whole request retries
// every key. A per-key retry sends only keys S3 still refuses. A batch can see both failures.
const DELETE_REFUSAL_ATTEMPTS: u32 = 3;

// Why one key was not removed: the category a caller acts on and the message a caller reads. The
// destination supplies the category because a bucket, a local tree, and an invalid key fail for
// different reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    kind: crate::error::ErrorKind,
    why: String,
}

impl Refusal {
    fn new(kind: crate::error::ErrorKind, why: impl Into<String>) -> Self {
        Self {
            kind,
            why: why.into(),
        }
    }

    #[cfg(test)]
    fn why(&self) -> &str {
        &self.why
    }

    #[cfg(test)]
    fn kind(&self) -> &crate::error::ErrorKind {
        &self.kind
    }
}

// What became of one key: removed, or refused with a reason naming it.
type KeyOutcome = Result<String, Refusal>;

// The delete path asks before it sends a batch. A throttled key can wait before its next attempt.
// The destination asks again after that wait, so a stopped run starts no new attempt.
pub(crate) type StopCheck<'a> = &'a (dyn Fn() -> bool + Send + Sync);

// Where a key lands, or the key a destination refuses.
//
// A key ending in the delimiter names a directory. Path derivation removes the trailing delimiter,
// so `photos/2019/` becomes a file named `2019`. Both local sync paths refuse that key.
//
// Directory download keeps its existing behavior. Sync owns the rejection for sync runs.
fn local_path_for_key(
    root: &std::path::Path,
    key: &str,
) -> Result<std::path::PathBuf, crate::error::Error> {
    if key.ends_with('/') {
        return Err(crate::error::Error::new(
            crate::error::ErrorKind::InputInvalid,
            format!("the key '{key}' names a place rather than a file"),
        ));
    }
    crate::io::key::local_key_path(root, key, None, None)
}

// Whether a run may remove destination keys. The named type shows a caller what `true` would
// enable.
//
// Delete mode is off by default. A delete can remove data the caller never sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DeleteMode {
    On,
    #[default]
    Off,
}

// What a run does when something fails. One policy covers every failure site.
//
// `Continue` is the default. A later run can finish work that a failed run left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FailurePolicy {
    #[default]
    Continue,
    Abort,
}

// Settings chosen by the caller. The comparison, child factory, and deleter choose the direction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunSettings {
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    pub(crate) max_children: usize,
    pub(crate) delete_mode: DeleteMode,
    pub(crate) failure_policy: FailurePolicy,
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            max_children: crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN,
            delete_mode: DeleteMode::default(),
            failure_policy: FailurePolicy::default(),
        }
    }
}

// How a run turned out. A run can finish without failures and still leave keys unaccounted for.
// `Warned` reports that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunOutcome {
    Failed,
    Warned,
    Clean,
}

// Where keys go when they leave the destination. A bucket removes up to 1,000 keys per request. A
// local tree removes one file per request.
pub(crate) enum Deleter {
    Bucket(DeleteFromBucket),
    LocalTree(DeleteFromLocalTree),
    #[cfg(test)]
    Recording(Arc<tests::RecordDeletes>),
}

impl Deleter {
    // How many keys a destination collects before sending. A destination with no batch request
    // returns one.
    pub(crate) fn batch_size(&self) -> usize {
        match self {
            Deleter::Bucket(d) => d.batch_size(),
            Deleter::LocalTree(d) => d.batch_size(),
            #[cfg(test)]
            Deleter::Recording(d) => d.batch_size(),
        }
    }

    // Remove keys and report one outcome per key. A batch result alone cannot name the keys that
    // survived.
    pub(crate) async fn delete<K, I>(&self, keys: I, stopped: StopCheck<'_>) -> Vec<KeyOutcome>
    where
        K: Into<String>,
        I: IntoIterator<Item = K>,
    {
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        match self {
            Deleter::Bucket(d) => d.delete(keys, stopped).await,
            Deleter::LocalTree(d) => d.delete(keys).await,
            #[cfg(test)]
            Deleter::Recording(d) => d.delete(keys).await,
        }
    }
}

// Remove files from a local tree. Directories stay in place. Sync may not own a directory that
// becomes empty.
#[derive(Debug)]
pub(crate) struct DeleteFromLocalTree {
    root: std::path::PathBuf,
}

impl DeleteFromLocalTree {
    pub(crate) fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    // Return the file a relative key names below the root. Reject paths outside the root and paths
    // that path cleaning changed.
    //
    // A local walk already produces cleaned paths. An S3 key can collapse onto another path after
    // cleaning. The check stops a future S3-to-local delete from removing the wrong file.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        let path = local_path_for_key(&self.root, key)?;
        // Containment and local deletion use the same remainder. The deletion path also requires
        // that the remainder still spells the original key.
        let named = crate::io::key::below_root(&self.root, &path)
            .and_then(std::path::Path::to_str)
            .is_some_and(|rest| rest == key.replace('/', std::path::MAIN_SEPARATOR_STR));
        if !named {
            return Err(crate::error::Error::new(
                crate::error::ErrorKind::InputInvalid,
                format!("the key '{key}' does not name the file this run would remove"),
            ));
        }
        Ok(path)
    }

    fn batch_size(&self) -> usize {
        1
    }

    async fn delete(&self, keys: Vec<String>) -> Vec<KeyOutcome> {
        let mut outcomes = Vec::with_capacity(keys.len());
        for key in keys {
            let path = match self.file_path(&key) {
                Ok(path) => path,
                Err(err) => {
                    // Path derivation reports invalid input. Filesystem removal reports an input/output error.
                    outcomes.push(Err(Refusal::new(
                        err.kind().clone(),
                        format!("{key}: {err}"),
                    )));
                    continue;
                }
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => outcomes.push(Ok(key)),
                // A missing file already has the delete result the run wanted. A second run reports
                // the same outcome.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => outcomes.push(Ok(key)),
                Err(e) => outcomes.push(Err(Refusal::new(
                    crate::error::ErrorKind::IOError,
                    format!("{key}: {e}"),
                ))),
            }
        }
        outcomes
    }
}

// Delete keys from a bucket. Sync retries this request after throttling or a transient transport
// failure. A child gets retry behavior from its SDK client.
pub(crate) struct DeleteFromBucket {
    client: aws_sdk_s3::Client,
    bucket: String,
    // As in `SpawnUpload`: keys arrive relative to the run's root, so the root goes back on before
    // naming an object. Deleting a relative key would reach for something at the bucket root.
    root: String,
}

impl DeleteFromBucket {
    pub(crate) fn new(
        client: aws_sdk_s3::Client,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            root: crate::io::key::stream::root_prefix(prefix).into_owned(),
        }
    }

    // The object a relative key names under this run's root.
    pub(crate) fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.root, key)
    }

    fn batch_size(&self) -> usize {
        DELETE_BATCH
    }

    async fn delete(&self, keys: Vec<String>, stopped: StopCheck<'_>) -> Vec<KeyOutcome> {
        // The object each relative key names, in the same order. The run matches a response entry
        // to the key it answers, and not to whatever sits at the same offset.
        let addressed: Vec<String> = keys.iter().map(|k| self.object_key(k)).collect();
        let mut at_object = std::collections::HashMap::with_capacity(addressed.len());
        for (at, object) in addressed.iter().enumerate() {
            at_object.insert(object.as_str(), at);
        }

        let unnamed = |at: usize, why: &str| {
            Err(Refusal::new(
                crate::error::ErrorKind::ServiceError,
                format!("{}: {why}", keys[at]),
            ))
        };
        let mut settled: Vec<Option<KeyOutcome>> = vec![None; keys.len()];
        // Keys that S3 still refuses. The next request contains only those keys.
        let mut outstanding: Vec<usize> = (0..keys.len()).collect();
        // The last refusal for each outstanding key. A stopped run returns that refusal to the
        // caller.
        let mut refused_with: std::collections::HashMap<usize, String> = Default::default();

        for attempt in 0..DELETE_REFUSAL_ATTEMPTS {
            if outstanding.is_empty() {
                break;
            }
            let last_attempt = attempt + 1 == DELETE_REFUSAL_ATTEMPTS;
            // Use the same backoff as a throttled request. An immediate retry adds to the load S3
            // is shedding.
            if attempt > 0 {
                let delay = crate::retry::Backoff::throttle().delay(attempt - 1, fastrand::f64());
                tokio::time::sleep(delay).await;
                // A stopped run starts no new attempt. Each outstanding key keeps the refusal that
                // earned its retry.
                if stopped() {
                    for &at in &outstanding {
                        let why = refused_with
                            .get(&at)
                            .map(String::as_str)
                            .unwrap_or("the run stopped before the service answered for this key");
                        settled[at].get_or_insert_with(|| unnamed(at, why));
                    }
                    break;
                }
            }

            let mut identifiers = Vec::with_capacity(outstanding.len());
            for &at in &outstanding {
                match aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(addressed[at].clone())
                    .build()
                {
                    Ok(id) => identifiers.push(id),
                    // Sync built that key, so an API refusal is a sync defect. The run records the
                    // key and continues with the batch.
                    Err(err) => settled[at] = Some(unnamed(at, &err.to_string())),
                }
            }
            if identifiers.is_empty() {
                break;
            }
            let delete = match aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(identifiers))
                // The run asks for a loud response. A quiet response omits successful keys.
                .quiet(false)
                .build()
            {
                Ok(delete) => delete,
                Err(err) => {
                    for &at in &outstanding {
                        settled[at].get_or_insert_with(|| unnamed(at, &err.to_string()));
                    }
                    break;
                }
            };

            let sent = crate::retry::retry(crate::retry::classify_discovery_retry, |_| {
                let delete = delete.clone();
                async move {
                    self.client
                        .delete_objects()
                        .bucket(&self.bucket)
                        .delete(delete)
                        .send()
                        .await
                        // `Error::from` keeps the service metadata. The retry classifier reads that
                        // metadata.
                        .map_err(|err| {
                            crate::retry::GuardError::Inner(crate::error::Error::from(err))
                        })
                }
            })
            .await;

            match sent {
                // The response lists deleted keys and refused keys separately. The run reads both
                // lists.
                Ok(output) => {
                    for deleted in output.deleted() {
                        if let Some(&at) = deleted.key().and_then(|k| at_object.get(k)) {
                            settled[at] = Some(Ok(keys[at].clone()));
                        }
                    }
                    let mut refused_again = Vec::new();
                    for error in output.errors() {
                        let Some(&at) = error.key().and_then(|k| at_object.get(k)) else {
                            continue;
                        };
                        // Keep the service code and message. The code names the refusal; the
                        // message explains it.
                        let why = match (error.code(), error.message()) {
                            (Some(code), Some(message)) => format!("{code}: {message}"),
                            (Some(code), None) => code.to_string(),
                            (None, Some(message)) => message.to_string(),
                            (None, None) => "no reason given".to_string(),
                        };
                        // Retry only the throttle codes used by the request retry policy. A
                        // terminal delete stays for the next run.
                        if !last_attempt && crate::retry::is_throttle_code(error.code()) {
                            refused_with.insert(at, why);
                            refused_again.push(at);
                        } else {
                            settled[at] = Some(unnamed(at, &why));
                        }
                    }
                    outstanding = refused_again;
                }
                // The request exhausted its retries. The run returns that failure for every
                // outstanding key.
                Err(err) => {
                    let why = err.to_string();
                    for &at in &outstanding {
                        settled[at].get_or_insert_with(|| unnamed(at, &why));
                    }
                    break;
                }
            }
        }

        // The run names every requested key. A response that omits a key still needs an outcome.
        settled
            .into_iter()
            .enumerate()
            .map(|(at, outcome)| {
                outcome.unwrap_or_else(|| unnamed(at, "the response did not mention this key"))
            })
            .collect()
    }
}

// Sync asks a child whether it finished and how many bytes it moved.
pub(crate) struct ChildHandle {
    id: crate::transfer::TransferId,
    inner: ChildInner,
}

enum ChildInner {
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

impl fmt::Debug for ChildHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildHandle").field("id", &self.id).finish()
    }
}

impl ChildHandle {
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
    ) -> Result<ChildHandle, crate::error::Error> {
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
        Ok(ChildHandle {
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
    ) -> Result<ChildHandle, crate::error::Error> {
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
        Ok(ChildHandle {
            id: handle.transfer_id(),
            inner: ChildInner::Download(handle),
        })
    }
}

// The merge leaves `State` while a work item advances it. `Walk::next` needs `&mut`, and the state
// lock cannot stay held across that wait.
pub(crate) enum SyncWork<S: KeyStream, D: KeyStream> {
    AdvanceMerge { walk: Option<Box<Walk<S, D>>> },
    // Terminal children waiting for a reap. `reap_in_flight` counts them after they leave
    // `children`.
    ReapChildren { children: Vec<ChildHandle> },
    // Keys waiting for deletion. `deletes_in_flight` counts them after a delete work item takes
    // them.
    DeleteKeys { keys: Vec<String> },
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncWork::AdvanceMerge { .. } => f.write_str("AdvanceMerge"),
            SyncWork::ReapChildren { children } => {
                write!(f, "ReapChildren({})", children.len())
            }
            SyncWork::DeleteKeys { keys } => write!(f, "DeleteKeys({})", keys.len()),
        }
    }
}

// A transfer decision with its pairing. The key names the destination; the source entry supplies
// bytes.
type Qualified<S, D> = (Pairing<S, D>, Decision);

// What the comparison decided by kind. Every pairing produces one decision.
#[derive(Debug, Default)]
struct Decided {
    transfers: u64,
    deletes: u64,
    skipped: Skipped,
    // Keys marked for removal. The count stays independent of delete mode, so callers can compare
    // intended removals with completed and refused removals.
    deletable: u64,
    // Which obstruction produced a skip. An obstruction is an entry, not a walk failure.
    obstructed: Obstructed,
}

// Every skip by reason. A single count cannot distinguish an unchanged key, a protected
// destination, and an incomplete plan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Skipped {
    unchanged: u64,
    destination_exists: u64,
    deferred: u64,
    unread: u64,
    obstructed: u64,
    // A delete decision that the mode turned into a skip.
    delete_not_allowed: u64,
}

impl Skipped {
    // The match is exhaustive, so a reason added to the comparison has to find a home here. A
    // wildcard arm could put that reason inside whichever count it happened to name.
    fn record(&mut self, reason: crate::operation::sync::compare::SkipReason) {
        use crate::operation::sync::compare::SkipReason;
        match reason {
            SkipReason::Unchanged => self.unchanged += 1,
            SkipReason::DestinationExists => self.destination_exists += 1,
            SkipReason::Deferred => self.deferred += 1,
            SkipReason::Unknown => self.unread += 1,
            SkipReason::Obstructed => self.obstructed += 1,
        }
    }

    // A removal the run was not allowed to make. Named apart from `record` because no comparison
    // reports it: the key arrives decided for removal and the mode turns it into a skip.
    fn record_delete_not_allowed(&mut self) {
        self.delete_not_allowed += 1;
    }

    fn total(&self) -> u64 {
        let Self {
            unchanged,
            destination_exists,
            deferred,
            unread,
            obstructed,
            delete_not_allowed,
        } = self;
        unchanged + destination_exists + deferred + unread + obstructed + delete_not_allowed
    }
}

impl std::ops::AddAssign for Skipped {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            unchanged,
            destination_exists,
            deferred,
            unread,
            obstructed,
            delete_not_allowed,
        } = batch;
        self.unchanged += unchanged;
        self.destination_exists += destination_exists;
        self.deferred += deferred;
        self.unread += unread;
        self.obstructed += obstructed;
        self.delete_not_allowed += delete_not_allowed;
    }
}

// Why a name held no bytes to transfer. An archive can become readable after a restore. Each reason
// needs its own count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Obstructed {
    // A socket, device, named pipe, or excluded symlink.
    nothing_to_read: u64,
    // Bytes in an archive, with no restored copy to read.
    archived: u64,
    // A restore under way, so a later run finds the bytes there.
    restoring: u64,
}

impl Obstructed {
    // The match is exhaustive, so a kind added later needs a deliberate destination. A wildcard arm
    // could put that kind inside whichever count it happened to name.
    fn record(&mut self, why: crate::io::key::stream::Obstruction) {
        use crate::io::key::stream::Obstruction;
        match why {
            Obstruction::NothingToRead => self.nothing_to_read += 1,
            Obstruction::Archived => self.archived += 1,
            Obstruction::BeingRestored => self.restoring += 1,
        }
    }

    fn any(&self) -> bool {
        self.nothing_to_read > 0 || self.archived > 0 || self.restoring > 0
    }
}

impl std::ops::AddAssign for Obstructed {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            nothing_to_read,
            archived,
            restoring,
        } = batch;
        self.nothing_to_read += nothing_to_read;
        self.archived += archived;
        self.restoring += restoring;
    }
}

impl std::ops::AddAssign for Decided {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            transfers,
            deletes,
            skipped,
            deletable,
            obstructed,
        } = batch;
        self.transfers += transfers;
        self.deletes += deletes;
        self.skipped += skipped;
        self.deletable += deletable;
        self.obstructed += obstructed;
    }
}

// What went wrong: an exact count and a bounded sample. The sample caps memory; the count records
// every failure.
#[derive(Debug)]
struct Reported<T> {
    kept: Vec<T>,
    total: u64,
}

impl<T> Default for Reported<T> {
    fn default() -> Self {
        Self {
            kept: Vec::new(),
            total: 0,
        }
    }
}

impl<T> Reported<T> {
    fn record(&mut self, item: T) {
        self.total += 1;
        if self.kept.len() < FAILURES_KEPT {
            self.kept.push(item);
        }
    }

    fn total(&self) -> u64 {
        self.total
    }

    fn any(&self) -> bool {
        self.total > 0
    }

    fn sample(&self) -> &[T] {
        &self.kept
    }
}

struct State<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, so the merge leaves
    // the state lock while it runs.
    walk: Option<Walk<S, D>>,
    merge_in_flight: bool,
    paired: u64,
    decided: Decided,
    // Transfer decisions waiting for child slots. The merge keeps pairing keys while children run.
    waiting: VecDeque<Qualified<S::Source, D::Source>>,
    // Live children.
    children: std::collections::HashMap<crate::transfer::TransferId, ChildHandle>,
    // Children waiting for a reap result. The child list no longer holds them.
    reap_in_flight: usize,
    // Delete decisions waiting for a batch.
    pending_deletes: Vec<String>,
    // Delete keys held by a work item.
    deletes_in_flight: usize,
    deleted: u64,
    bytes_moved: u64,
    // Transfers that arrived, one per reaped child. The comparison count records intent; this count
    // records completion.
    transferred: u64,
    // A child that the run could not enqueue or that ended badly.
    transfer_failures: Reported<String>,
    // Decisions the run did not carry out. This flag records a comparison error, dropped deletes,
    // and transfers that never started.
    plan_incomplete: bool,
    // Children released before a reap recorded their outcome. The run keeps their byte count and
    // marks the outcome unknown.
    outcomes_unknown: u64,
    // The walk plan-hole state when a work item returns. The flag accumulates across merge work
    // items.
    walk_plan_incomplete: bool,
    failures: Reported<StreamError>,
    // Warnings for names no transfer setting can carry.
    warnings: Reported<StreamError>,
    // The failure that stopped the run. The terminal path reads it after outstanding work returns.
    stopped_by: Option<crate::error::Error>,
    // Delete refusal messages by key.
    refusals: Reported<String>,
}

pub(crate) struct SyncTransfer<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    inner: Arc<Inner<S, D>>,
}

impl<S: KeyStream, D: KeyStream> Clone for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTransfer").finish_non_exhaustive()
    }
}

struct Inner<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    ctx: TransferContext,
    state: Mutex<State<S, D>>,
    // Compare each pair according to the sync direction. Upload and download choose opposite newer sides.
    comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
    // The other one. Unlike the comparison this holds state, because building a child needs the
    // client and the bucket.
    spawner: Arc<dyn SpawnChild<S::Source>>,
    // How keys leave the destination. A third direction-specific thing, and the one that differs
    // most between a bucket and a local tree.
    deleter: Deleter,
    // What to do when something fails. Read at every site a failure can arrive, so one answer
    // covers the run.
    failure_policy: FailurePolicy,
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    max_children: usize,
    // Allow sync to remove destination-only keys. Delete mode requires caller opt-in.
    delete_mode: DeleteMode,
}

impl<S, D> SyncTransfer<S, D>
where
    S: KeyStream + Send + 'static,
    D: KeyStream + Send + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    pub(crate) fn new(
        ctx: TransferContext,
        walk: Walk<S, D>,
        comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
        spawner: Arc<dyn SpawnChild<S::Source>>,
        deleter: Deleter,
        settings: RunSettings,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                ctx,
                comparison,
                spawner,
                deleter,
                max_children: settings.max_children.max(1),
                delete_mode: settings.delete_mode,
                failure_policy: settings.failure_policy,
                state: Mutex::new(State {
                    walk: Some(walk),
                    merge_in_flight: false,
                    paired: 0,
                    decided: Decided::default(),
                    waiting: VecDeque::new(),
                    children: std::collections::HashMap::new(),
                    reap_in_flight: 0,
                    pending_deletes: Vec::new(),
                    deletes_in_flight: 0,
                    deleted: 0,

                    bytes_moved: 0,
                    transferred: 0,
                    transfer_failures: Reported::default(),
                    plan_incomplete: false,
                    outcomes_unknown: 0,
                    walk_plan_incomplete: false,
                    failures: Reported::default(),
                    refusals: Reported::default(),
                    stopped_by: None,
                    warnings: Reported::default(),
                }),
            }),
        }
    }

    // Return true when the comparison reached every key and the finished run has no work
    // outstanding.
    //
    // The run stores comparison holes and walk holes in state. A merge work item can hold the walk,
    // so plan completeness cannot read the walk directly.
    pub(crate) fn is_plan_complete(&self) -> bool {
        let state = self.inner.state.lock();
        !state.plan_incomplete
            && !state.walk_plan_incomplete
            && !(!self.inner.ctx.is_active() && self.work_outstanding(&state))
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let mut state = self.inner.state.lock();

        // Check for stop between phases. A failed spawn can stop the run before the delete phase
        // starts.
        if !self.has_stopped(&state) {
            if let Some(work) = self.dispatch_merge(&mut state) {
                return work;
            }
            if self.spawn_one(&mut state) {
                return PollWork::Spawned;
            }
        }

        if !self.has_stopped(&state) {
            if let Some(work) = self.dispatch_deletes(&mut state) {
                return work;
            }
        }

        // Reap terminal children after cancellation. The run still needs each child outcome.
        if let Some(work) = self.dispatch_reap(&mut state) {
            return work;
        }

        if let Some(done) = self.check_terminal(&mut state) {
            return done;
        }

        // Mark the transfer pending before returning. A work item signal wakes only a pending
        // transfer.
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    // Return true when merge, reap, delete, or child work remains outstanding.
    fn work_outstanding(&self, state: &State<S, D>) -> bool {
        state.merge_in_flight
            || state.reap_in_flight > 0
            || state.deletes_in_flight > 0
            || !state.children.is_empty()
    }

    // Return true after cancellation or an aborting failure. The transfer stays active until
    // outstanding work returns.
    fn has_stopped(&self, state: &State<S, D>) -> bool {
        !self.inner.ctx.is_active() || state.stopped_by.is_some()
    }

    // Record the first aborting failure. The terminal path sets the transfer result after
    // outstanding work returns.
    fn stop_if_aborting(&self, state: &mut State<S, D>, why: impl Into<crate::error::Error>) {
        if self.inner.failure_policy == FailurePolicy::Abort && state.stopped_by.is_none() {
            state.stopped_by = Some(why.into());
        }
    }

    // Return the run outcome. Failures outrank warnings; warnings outrank a clean result.
    pub(crate) fn outcome(&self) -> RunOutcome {
        let state = self.inner.state.lock();
        if state.failures.any() || state.transfer_failures.any() || state.refusals.any() {
            return RunOutcome::Failed;
        }
        if state.warnings.any()
            || state.decided.obstructed.any()
            // A short plan warns the caller. The run may stop early or the comparison may produce
            // an invalid decision.
            || state.plan_incomplete
            || state.walk_plan_incomplete
            // A released child leaves its outcome unknown. The run reports that warning even when
            // every decision ran.
            || state.outcomes_unknown > 0
            // Outstanding work on an ended run is a warning. Reap, delete, and merge work each
            // carry results back to the run.
            || (!self.inner.ctx.is_active() && self.work_outstanding(&state))
        {
            return RunOutcome::Warned;
        }
        RunOutcome::Clean
    }

    // Report `Done` only after merge work returns. A work item can still hold keys after the merge
    // leaves state.
    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if self.has_stopped(state) {
            // Everything dispatched is still owed an answer, however the run ended.
            if self.work_outstanding(state) {
                return None;
            }
            // Set the terminal status after outstanding work returns. Every earlier phase reads
            // `stopped_by` and starts no new work.
            if let Some(why) = state.stopped_by.take() {
                self.inner.ctx.set_failed(why);
            }
            // Drop pending deletes and transfers after a stop. Their absence decisions came from an
            // incomplete source stream.
            //
            // The next run compares those keys from a complete view. The run marks the plan
            // incomplete.
            state.plan_incomplete |= !state.pending_deletes.is_empty();
            state.pending_deletes.clear();
            state.plan_incomplete |= !state.waiting.is_empty();
            // Drop waiting transfers after marking the plan incomplete. A terminal transfer has no
            // later poll to start them.
            state.waiting.clear();
            // The run recorded the terminal outcome. Signal the caller.
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        if !state.merge_in_flight
            && state.waiting.is_empty()
            && state.children.is_empty()
            && state.reap_in_flight == 0
            && state.pending_deletes.is_empty()
            && state.deletes_in_flight == 0
            && state
                .walk
                .as_ref()
                .is_some_and(|walk| walk.progress() != Progress::Pairing)
        {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
    }

    // Start one waiting transfer when a child slot is free. The spawned child uses its own
    // scheduler slot.
    fn spawn_one(&self, state: &mut State<S, D>) -> bool {
        // Count only live children. A child in a reap holds no network or disk concurrency.
        if state.children.len() >= self.inner.max_children {
            return false;
        }

        // Skip a decision that cannot create a child. Keep reading waiting decisions until one
        // starts or the queue empties.
        loop {
            // A stopped run starts no new child. Check before removing the key from the waiting
            // queue.
            if self.has_stopped(state) {
                return false;
            }
            let Some((pairing, _)) = state.waiting.pop_front() else {
                break;
            };
            let Some(entry) = pairing.source().entry() else {
                // A transfer needs a source entry. Record a comparison error when that entry is
                // absent.
                let why = crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "a transfer was decided for a key with no source entry",
                );
                state
                    .transfer_failures
                    .record(format!("{}: {why}", pairing.key()));
                self.stop_if_aborting(state, why);
                continue;
            };
            match self
                .inner
                .spawner
                .spawn(pairing.key(), &entry.source, self.inner.ctx.id.id)
            {
                Ok(child) => {
                    state.children.insert(child.id(), child);
                    return true;
                }
                Err(err) => {
                    state
                        .transfer_failures
                        .record(format!("{}: {err}", pairing.key()));
                    self.stop_if_aborting(state, err);
                    continue;
                }
            }
        }
        false
    }

    // Send a delete batch when it fills or the merge cannot add another key.
    fn dispatch_deletes(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let size = self.inner.deleter.batch_size();
        let merge_done = match state.walk.as_ref().map(Walk::progress) {
            // Both sides paired every key either of them holds, so nothing can grow a part batch.
            Some(Progress::Accounted) => true,
            // More pairings will come, and any of them could add to the batch. A merge away with a
            // work item says nothing either way.
            Some(Progress::Pairing) | None => false,
            // A failed merge leaves deletes from an incomplete source stream. Drop the batch and mark the
            // plan incomplete.
            Some(Progress::Stopped) => {
                state.plan_incomplete |= !state.pending_deletes.is_empty();
                state.pending_deletes.clear();
                return None;
            }
        };
        let full = state.pending_deletes.len() >= size;
        // A part batch waits until nothing can grow it. Sending early would cost a request per
        // handful of keys for no gain.
        let last_call = merge_done && !state.merge_in_flight && !state.pending_deletes.is_empty();
        if !full && !last_call {
            return None;
        }
        let take = state.pending_deletes.len().min(size);
        let keys: Vec<String> = state.pending_deletes.drain(..take).collect();
        state.deletes_in_flight += keys.len();
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::DeleteKeys { keys })),
        }))
    }

    // Collect terminal children for a reap. `reap_in_flight` counts them after they leave
    // `children`.
    fn dispatch_reap(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let finished: Vec<crate::transfer::TransferId> = state
            .children
            .iter()
            .filter(|(_, child)| child.is_finished())
            .map(|(id, _)| *id)
            .collect();
        if finished.is_empty() {
            return None;
        }
        let children: Vec<ChildHandle> = finished
            .into_iter()
            .map(|id| state.children.remove(&id).expect("id came from this map"))
            .collect();
        state.reap_in_flight += children.len();
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::ReapChildren { children })),
        }))
    }

    // Hand the merge to a work item. When no merge work remains, the poll chooses `Done` or
    // `Pending`.
    fn dispatch_merge(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if state.merge_in_flight {
            return None;
        }

        let walk = match state.walk.take() {
            Some(walk) if walk.progress() == Progress::Pairing => walk,
            // Exhausted. Put it back so the completion test can see it.
            Some(walk) => {
                state.walk = Some(walk);
                return None;
            }
            None => return None,
        };

        state.merge_in_flight = true;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::AdvanceMerge {
                walk: Some(Box::new(walk)),
            })),
        }))
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let work: &mut SyncWork<S, D> = work.data_mut();
        match work {
            SyncWork::AdvanceMerge { walk } => {
                let walk = walk.take().expect("the merge was already taken");
                self.execute_advance_merge(*walk).await
            }
            SyncWork::ReapChildren { children } => {
                let children = std::mem::take(children);
                self.execute_reap(children).await
            }
            SyncWork::DeleteKeys { keys } => {
                let keys = std::mem::take(keys);
                self.execute_deletes(keys).await
            }
        }
    }

    async fn execute_deletes(&self, keys: Vec<String>) -> WorkOutcome {
        // Check for stop again before sending a delete batch. A stopped run made absence decisions
        // from an incomplete source stream.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                // This work item owns the batch. Mark the plan incomplete before dropping it.
                state.plan_incomplete |= !keys.is_empty();
                state.deletes_in_flight = state.deletes_in_flight.saturating_sub(keys.len());
                if self.check_terminal(&mut state).is_some() {
                    return WorkOutcome::Cancelled;
                }
                drop(state);
                self.inner.ctx.try_wake();
                return WorkOutcome::Cancelled;
            }
        }

        let sent = keys.len();
        // Pass the stop check to the destination retry loop. The loop checks before each retry.
        let stopped = || {
            let state = self.inner.state.lock();
            self.has_stopped(&state)
        };
        let outcomes = self.inner.deleter.delete(keys, &stopped).await;

        let mut gone = 0u64;
        let mut refused = Vec::new();
        for outcome in &outcomes {
            match outcome {
                Ok(_) => gone += 1,
                Err(why) => refused.push(why.clone()),
            }
        }

        // S3 reports a per-key refusal inside a successful response. The run records the key and
        // reason.
        let mut state = self.inner.state.lock();
        if let Some(first) = refused.first() {
            let why = crate::error::Error::new(first.kind.clone(), first.why.clone());
            self.stop_if_aborting(&mut state, why);
        }
        state.deletes_in_flight -= sent;
        state.deleted += gone;

        for refusal in refused {
            state.refusals.record(refusal.why);
        }
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_reap(&self, children: Vec<ChildHandle>) -> WorkOutcome {
        let count = children.len();
        let mut moved = 0u64;
        let mut arrived = 0u64;
        let mut why = None;
        let mut reasons = Vec::new();
        for child in children {
            match child.join().await {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(err) => {
                    reasons.push(err.to_string());
                    if why.is_none() {
                        why = Some(err);
                    }
                }
            }
        }
        let mut state = self.inner.state.lock();
        if let Some(why) = why {
            self.stop_if_aborting(&mut state, why);
        }
        state.reap_in_flight -= count;
        state.bytes_moved += moved;
        state.transferred += arrived;
        // Record every child failure. The first failure controls `Abort`; callers still need every
        // reason.
        for reason in reasons {
            state.transfer_failures.record(reason);
        }
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_advance_merge(&self, mut walk: Walk<S, D>) -> WorkOutcome {
        // Release the state lock before the merge waits.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                state.walk = Some(walk);
                state.merge_in_flight = false;
                // Return the merge before checking for completion. The returned merge can finish the run.
                if self.check_terminal(&mut state).is_some() {
                    return WorkOutcome::Cancelled;
                }
                drop(state);
                self.inner.ctx.try_wake();
                return WorkOutcome::Cancelled;
            }
        }

        let mut paired = 0u64;
        let mut decided = Decided::default();
        let mut deferred = false;
        let mut failures = Vec::new();
        let mut batch = VecDeque::new();
        let mut pending_deletes = Vec::new();

        for _ in 0..MERGE_BATCH {
            match walk.next().await {
                Some(Ok(pairing)) => {
                    paired += 1;
                    // TODO(sync): Map this comparison `Decision` into the per-entry
                    // transfer event record before dispatching its work.
                    match self.inner.comparison.compare(&pairing) {
                        Verdict::Decided(decision @ Decision::Transfer(_)) => {
                            decided.transfers += 1;
                            batch.push_back((pairing, decision));
                        }
                        // A delete decision means the destination has a key the source lacks. Delete mode
                        // turns an unrequested delete into a skip.
                        Verdict::Decided(Decision::Delete(_)) => {
                            decided.deletable += 1;
                            match self.inner.delete_mode {
                                DeleteMode::On => {
                                    decided.deletes += 1;
                                    pending_deletes.push(pairing.key().to_string());
                                }
                                DeleteMode::Off => decided.skipped.record_delete_not_allowed(),
                            }
                        }
                        // A skip needs no scheduler work. Record the skip reason.
                        Verdict::Decided(Decision::Skip(skip)) => {
                            decided.skipped.record(skip.reason());
                            // Record the obstruction separately from the skip reason.
                            if let Some(why) = skip.obstruction() {
                                decided.obstructed.record(why);
                            }
                        }
                        // No shipped comparison returns `Deferred`. Record an unexpected deferred decision
                        // as a skip and a plan hole.
                        Verdict::Deferred(_) => {
                            decided
                                .skipped
                                .record(crate::operation::sync::compare::SkipReason::Deferred);
                            deferred = true;
                        }
                    }
                }
                Some(Err(err)) => failures.push(err),
                None => break,
            }
        }

        let mut state = self.inner.state.lock();
        state.walk_plan_incomplete |= !walk.is_plan_complete();
        state.walk = Some(walk);
        state.merge_in_flight = false;
        state.paired += paired;
        state.decided += decided;
        state.plan_incomplete |= deferred;
        state.waiting.append(&mut batch);
        state.pending_deletes.append(&mut pending_deletes);
        // Warnings continue the run. Failures enter the run record and can stop the run.
        //
        // Record every failure. A fatal failure stops the run under both policies.
        let mut failed = None;
        let mut nothing_left = None;
        for entry in failures {
            if entry.is_warning() {
                state.warnings.record(entry);
                continue;
            }
            // A fatal entry leaves no source stream to continue.
            if entry.is_fatal() && nothing_left.is_none() {
                nothing_left = Some((entry.category(), entry.to_string()));
            }
            if failed.is_none() {
                // Read the category and message before moving the failure into the run record.
                failed = Some((entry.category(), entry.to_string()));
            }
            state.failures.record(entry);
        }
        // The run record keeps every failure. The terminal result carries the first failure.
        // A fatal failure stops the run under both policies.
        if let Some((kind, why)) = nothing_left {
            if state.stopped_by.is_none() {
                state.stopped_by = Some(crate::error::Error::new(kind, why));
            }
        } else if let Some((kind, why)) = failed {
            self.stop_if_aborting(&mut state, crate::error::Error::new(kind, why));
        }

        // The last merge work item can end the run. Signal the caller after the terminal check.
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);

        // Wake the pending run after merge work returns.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }
}

impl<S, D> Transfer for SyncTransfer<S, D>
where
    S: KeyStream + Send + Sync + fmt::Debug + 'static,
    D: KeyStream + Send + Sync + fmt::Debug + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    // Handle terminal cleanup. The scheduler does not poll a terminal transfer again.
    //
    // Read bytes moved before releasing child handles. Released children have unknown outcomes.
    fn on_terminal(&self) {
        // Take child handles under the state lock and drop them after releasing it. Dropping a
        // child can wake the scheduler.
        let children = {
            let mut state = self.inner.state.lock();
            if !state.children.is_empty() {
                state.outcomes_unknown += state.children.len() as u64;
                // Read bytes moved before dropping child handles. Dropping a handle cancels its
                // child.
                let moved: u64 = state.children.values().map(ChildHandle::bytes_so_far).sum();
                state.bytes_moved += moved;
            }
            std::mem::take(&mut state.children)
        };
        drop(children);
    }

    fn poll_work(&self) -> PollWork {
        SyncTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(SyncTransfer::execute(self, work))
    }
}

// Upload and download each implement `Transfer`.
// A trait bound that excludes either direction fails to compile here.
const _: fn() = || {
    fn implements_transfer<T: Transfer>() {}
    implements_transfer::<SyncTransfer<crate::io::walk::FsWalk, crate::io::walk::S3Walk>>();
    implements_transfer::<
        SyncTransfer<crate::io::walk::S3Walk, crate::operation::sync::walk::LocalDestination>,
    >();
};

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    use crate::io::walk::{FsWalk, S3Walk};
    use crate::operation::sync::modes::Mode;
    use crate::operation::sync::walk::{LocalAndBucket, Walker};

    use super::*;

    fn a_bucket_holding(keys: &[&str]) -> aws_sdk_s3::Client {
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(0)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put])
    }

    fn a_bucket_recording_puts(keys: &[&str]) -> (aws_sdk_s3::Client, Arc<Mutex<Vec<String>>>) {
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(0)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |req| {
                if let Some(key) = req.key() {
                    recorder.lock().push(key.to_string());
                }
                true
            })
            .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put]);
        (client, seen)
    }

    fn still_running() -> StopCheck<'static> {
        &|| false
    }

    fn a_local_tree(root: &Path, keys: &[&str]) {
        for key in keys {
            let path = root.join(key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("a parent directory");
            }
            std::fs::write(&path, b"").expect("a file");
        }
    }

    fn uploading(
        local: &Path,
        bucket_keys: &[&str],
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(bucket_keys);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);

        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        (
            SyncTransfer::new(
                ctx.clone(),
                walk,
                Mode::default().uploading(),
                Arc::new(SpawnEnded::new(0, false)),
                Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
                RunSettings {
                    max_children: 2,
                    delete_mode: DeleteMode::On,
                    failure_policy: FailurePolicy::Continue,
                },
            ),
            ctx,
        )
    }

    async fn drive(transfer: &SyncTransfer<FsWalk, S3Walk>) -> u64 {
        let run = async {
            loop {
                match transfer.poll_work() {
                    PollWork::Ready { io: mut work, .. } => {
                        transfer.execute(&mut work).await;
                    }
                    PollWork::Done => break,
                    PollWork::Pending => {
                        panic!("nothing else can make progress, so this would park forever")
                    }
                    PollWork::Spawned => {}
                }
            }
            transfer.inner.state.lock().paired
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the run parked: a poll answered Pending with no wake to follow")
    }

    pub(crate) struct RecordDeletes {
        batch: usize,
        sent: Mutex<Vec<Vec<String>>>,
        refuse: bool,
    }

    impl RecordDeletes {
        fn new(batch: usize) -> Self {
            Self {
                batch,
                sent: Mutex::new(Vec::new()),
                refuse: false,
            }
        }

        fn refusing(batch: usize) -> Self {
            Self {
                batch,
                sent: Mutex::new(Vec::new()),
                refuse: true,
            }
        }

        fn batches(&self) -> Vec<Vec<String>> {
            self.sent.lock().clone()
        }

        fn keys_sent(&self) -> usize {
            self.sent.lock().iter().map(Vec::len).sum()
        }
    }

    impl RecordDeletes {
        pub(crate) fn batch_size(&self) -> usize {
            self.batch
        }

        pub(crate) async fn delete(&self, keys: Vec<String>) -> Vec<KeyOutcome> {
            self.sent.lock().push(keys.clone());
            let refuse = self.refuse;
            keys.into_iter()
                .map(|key| {
                    if refuse {
                        Err(Refusal::new(
                            crate::error::ErrorKind::ServiceError,
                            format!("{key}: refused"),
                        ))
                    } else {
                        Ok(key)
                    }
                })
                .collect()
        }
    }

    struct SpawnEnded {
        moved: u64,
        fails: bool,
        refuses: bool,
        refuse_at: Option<usize>,
        asked: std::sync::atomic::AtomicUsize,
        ended: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SpawnEnded {
        fn new(moved: u64, fails: bool) -> Self {
            Self {
                moved,
                fails,
                refuses: false,
                refuse_at: None,
                asked: std::sync::atomic::AtomicUsize::new(0),
                ended: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        fn refusing_to_spawn() -> Self {
            Self {
                refuses: true,
                ..Self::new(0, false)
            }
        }

        fn holding_open_but_refusing_at(at: usize) -> Self {
            let spawner = Self {
                refuse_at: Some(at),
                ..Self::new(0, false)
            };
            spawner
                .ended
                .store(false, std::sync::atomic::Ordering::SeqCst);
            spawner
        }

        fn holding_children_open() -> Self {
            Self::holding_children_open_having_moved(0)
        }

        fn holding_children_open_having_moved(moved: u64) -> Self {
            let spawner = Self::new(moved, false);
            spawner
                .ended
                .store(false, std::sync::atomic::Ordering::SeqCst);
            spawner
        }

        fn asked_count(&self) -> usize {
            self.asked.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn release(&self, ctx: &TransferContext) {
            self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
            ctx.try_wake();
        }
    }

    impl SpawnChild<crate::io::walk::FsEntry> for SpawnEnded {
        fn spawn(
            &self,
            _key: &str,
            _source: &crate::io::walk::FsEntry,
            _parent: u64,
        ) -> Result<ChildHandle, crate::error::Error> {
            let n = self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.refuses || self.refuse_at == Some(n) {
                return Err(crate::error::Error::new(
                    crate::error::ErrorKind::ObjectNotDiscoverable,
                    "the child could not be built",
                ));
            }
            Ok(ChildHandle {
                id: crate::transfer::TransferId {
                    id: 900_000 + n as u64,
                    parent: None,
                },
                inner: ChildInner::Controlled {
                    ended: self.ended.clone(),
                    moved: self.moved,
                    failed: self.fails,
                },
            })
        }
    }

    struct AlwaysDefers;

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysDefers {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden, so no arm reaches a described pair")
        }

        fn compare(
            &self,
            _pairing: &crate::operation::sync::walk::Pairing<
                crate::io::walk::FsEntry,
                aws_sdk_s3::types::Object,
            >,
        ) -> Verdict {
            Verdict::Deferred(crate::operation::sync::compare::Deferred {})
        }
    }

    struct AlwaysUnknown(crate::io::key::stream::KeysLost);

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysUnknown {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden, so no arm reaches a described pair")
        }

        fn compare(
            &self,
            _pairing: &crate::operation::sync::walk::Pairing<
                crate::io::walk::FsEntry,
                aws_sdk_s3::types::Object,
            >,
        ) -> Verdict {
            Verdict::decided(crate::operation::sync::compare::Decision::skip_unknown(
                self.0,
            ))
        }
    }

    struct AlwaysObstructed(crate::io::key::stream::Obstruction);

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysObstructed {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden, so no arm reaches a described pair")
        }

        fn compare(
            &self,
            _pairing: &crate::operation::sync::walk::Pairing<
                crate::io::walk::FsEntry,
                aws_sdk_s3::types::Object,
            >,
        ) -> Verdict {
            Verdict::decided(crate::operation::sync::compare::Decision::skip_obstructed(
                self.0,
            ))
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_archived_object_and_a_restoring_one_are_counted_apart() {
        use crate::io::key::stream::Obstruction;

        for why in [Obstruction::Archived, Obstruction::BeingRestored] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt"]);
            let comparison: &'static AlwaysObstructed = Box::leak(Box::new(AlwaysObstructed(why)));
            let (transfer, _ctx) = uploading_comparing_with(dir.path(), comparison);

            while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
                transfer.execute(&mut work).await;
            }

            let counts = transfer.inner.state.lock().decided.obstructed;
            let expected = match why {
                Obstruction::Archived => Obstructed {
                    archived: 1,
                    ..Obstructed::default()
                },
                Obstruction::BeingRestored => Obstructed {
                    restoring: 1,
                    ..Obstructed::default()
                },
                Obstruction::NothingToRead => unreachable!("not under test here"),
            };
            assert_eq!(counts, expected, "{why:?} was counted under another reason");
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_whose_absence_went_unread_is_counted_apart_from_an_unchanged_one() {
        use crate::io::key::stream::KeysLost;

        for lost in [KeysLost::OneKey, KeysLost::UnknownRange] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt"]);
            let comparison: &'static AlwaysUnknown = Box::leak(Box::new(AlwaysUnknown(lost)));
            let (transfer, _ctx) = uploading_comparing_with(dir.path(), comparison);

            while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
                transfer.execute(&mut work).await;
            }

            let state = transfer.inner.state.lock();
            assert_eq!(
                state.decided.skipped.total(),
                1,
                "the key was not skipped, so this proves nothing about the reason"
            );
            assert_eq!(
                state.decided.skipped.unread, 1,
                "a key the run could not compare, lost as {lost:?}, is counted as one it compared"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn every_pairing_yields_exactly_one_decision() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);

        let paired = drive(&transfer).await;
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers + state.decided.deletes + state.decided.skipped.total(),
            paired,
            "the decisions do not account for every key that was paired"
        );
        assert_eq!(state.decided.deletes, 1, "d.txt is on the bucket alone");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_deferred_verdict_is_skipped_and_shortens_the_plan() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            &AlwaysDefers,
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        let paired = drive(&transfer).await;
        assert_eq!(paired, 2);
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.skipped.total(),
            2,
            "a deferred key was not skipped"
        );
        assert_eq!(state.decided.transfers, 0);
        assert!(
            state.plan_incomplete,
            "the plan is short two keys and does not say so"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_plan_is_whole_only_when_neither_flag_is_set() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (transfer, _ctx) = uploading(dir.path(), &[]);

        for (mine, walks, whole) in [
            (false, false, true),
            (true, false, false),
            (false, true, false),
            (true, true, false),
        ] {
            {
                let mut state = transfer.inner.state.lock();
                state.plan_incomplete = mine;
                state.walk_plan_incomplete = walks;
            }
            assert_eq!(
                transfer.is_plan_complete(),
                whole,
                "a deferred verdict of {mine} and an unread stream of {walks} answered wrongly"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn taking_the_merge_away_does_not_change_the_answer() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &[]);

        let before = transfer.is_plan_complete();
        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        assert!(
            transfer.inner.state.lock().walk.is_none(),
            "the merge is still in state, so nothing was taken away"
        );
        assert_eq!(
            transfer.is_plan_complete(),
            before,
            "the answer changed while a work item held the merge"
        );

        transfer.execute(&mut work).await;
        assert!(
            transfer.is_plan_complete(),
            "a clean run reported a short plan"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_over_two_empty_sides_is_over_at_once() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (transfer, _ctx) = uploading(dir.path(), &[]);
        assert_eq!(drive(&transfer).await, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn every_key_either_side_holds_is_paired_once() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);
        assert_eq!(drive(&transfer).await, 4);
        assert!(
            transfer.inner.state.lock().failures.sample().is_empty(),
            "a well-formed listing produced a failure"
        );
    }

    fn a_bucket_to_download(keys: &[&str], at: i64) -> aws_sdk_s3::Client {
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(5)
                    .last_modified(aws_smithy_types::DateTime::from_secs(at))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(at))
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get])
    }

    fn a_bucket_recording_gets(
        keys: &[&str],
        at: i64,
    ) -> (aws_sdk_s3::Client, Arc<Mutex<Vec<String>>>) {
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(5)
                    .last_modified(aws_smithy_types::DateTime::from_secs(at))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let get = mock!(aws_sdk_s3::Client::get_object)
            .match_requests(move |req| {
                if let Some(key) = req.key() {
                    recorder.lock().push(key.to_string());
                }
                true
            })
            .then_output(move || {
                aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                    .content_length(5)
                    .last_modified(aws_smithy_types::DateTime::from_secs(at))
                    .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get]);
        (client, seen)
    }

    fn downloading_into(
        local: &Path,
        client: aws_sdk_s3::Client,
        delete_mode: DeleteMode,
    ) -> (
        SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
        TransferContext,
        crate::transfer::StateMachineTerminalReceiver,
    ) {
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .downloading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().downloading(),
            Arc::new(SpawnDownload::new(
                ctx.handle.clone(),
                "amzn-s3-demo-bucket",
                None,
                local,
            )),
            Deleter::LocalTree(DeleteFromLocalTree::new(local)),
            RunSettings {
                max_children: 4,
                delete_mode,
                failure_policy: FailurePolicy::Continue,
            },
        );
        (transfer, ctx, rx)
    }

    async fn run_managed(
        transfer: &SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
        ctx: &TransferContext,
        rx: crate::transfer::StateMachineTerminalReceiver,
    ) {
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(20), rx)
            .await
            .expect("the run did not finish inside twenty seconds")
            .expect("the terminal signal was dropped");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_download_lands_under_its_directories_with_the_objects_time() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let when = 1_600_000_000i64;
        let client = a_bucket_to_download(&["a.txt", "deep/down/c.txt"], when);
        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        for key in ["a.txt", "deep/down/c.txt"] {
            let path = dir.path().join(key);
            let meta = std::fs::metadata(&path)
                .unwrap_or_else(|e| panic!("{key} did not arrive at its final name: {e}"));
            let stamped = meta
                .modified()
                .expect("a modified time")
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .expect("a time after the epoch")
                .as_secs();
            assert_eq!(
                stamped, when as u64,
                "{key} kept the time it was written rather than the object's"
            );
        }
        let strays: Vec<_> = walkdir_s3tmp(dir.path());
        assert!(
            strays.is_empty(),
            "temporary files were left behind: {strays:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_time_that_cannot_be_applied_does_not_cost_the_download() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let client = a_bucket_to_download(&["a.txt"], i64::MAX);
        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        let path = dir.path().join("a.txt");
        assert!(
            path.exists(),
            "a stamp that could not be applied took the downloaded file with it"
        );
        assert_eq!(
            std::fs::read(&path).expect("the file reads"),
            b"hello",
            "the file arrived without its contents"
        );
        assert!(
            !ctx.is_failed(),
            "a stamp that could not be applied failed the run"
        );
        let stamped = std::fs::metadata(&path)
            .expect("the file has metadata")
            .modified()
            .expect("a modified time")
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("a time after the epoch")
            .as_secs();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_secs();
        assert!(
            stamped > now + 86_400 * 365,
            "the file is dated {stamped}, within a year of the {now} it was written at, \
             so the object's time never reached it"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_downloaded_file_carries_the_time_the_object_had() {
        const AT: i64 = 1_600_000_000;

        let dir = tempfile::tempdir().expect("a temp dir");
        let client = a_bucket_to_download(&["a.txt"], AT);
        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        let stamped = std::fs::metadata(dir.path().join("a.txt"))
            .expect("the file has metadata")
            .modified()
            .expect("a modified time")
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("a time after the epoch")
            .as_secs();
        assert_eq!(
            stamped, AT as u64,
            "the file holds its own write time, so the object's was never applied"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_local_delete_removes_the_file_and_leaves_its_directory() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["keep/gone.txt"]);
        let client = a_bucket_to_download(&[], 1_600_000_000);
        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        assert!(
            !dir.path().join("keep/gone.txt").exists(),
            "the file the source does not have is still there"
        );
        assert!(
            dir.path().join("keep").is_dir(),
            "the directory went with the file"
        );
        assert_eq!(transfer.inner.state.lock().deleted, 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_object_whose_key_names_a_place_is_reported_not_written() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let client = a_bucket_to_download(&["photos/2019/"], 1_600_000_000);
        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        assert!(
            !dir.path().join("photos/2019").exists(),
            "a key naming a place was written as a file"
        );
        assert_eq!(
            transfer.inner.state.lock().transfer_failures.total(),
            1,
            "the key was not accounted for"
        );
        assert!(!ctx.is_failed(), "a continuing run reported itself failed");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_naming_somewhere_above_the_root_is_refused() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let root = dir.path().join("inside");
        std::fs::create_dir(&root).expect("the root");
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, b"not sync's").expect("a file");

        let spawner = SpawnDownload::new(
            crate::client::Handle::test_handle_tokio(
                crate::Config::builder()
                    .client(a_bucket_holding(&[]))
                    .build(),
            ),
            "amzn-s3-demo-bucket",
            None,
            &root,
        );
        assert!(
            spawner.file_path("../outside.txt").is_err(),
            "a download would have written above its destination"
        );

        let deleter = DeleteFromLocalTree::new(&root);
        assert!(
            deleter.file_path("../outside.txt").is_err(),
            "a delete would have reached above its destination"
        );
        let outcomes = deleter.delete(vec!["../outside.txt".to_string()]).await;
        assert!(
            outcomes[0].is_err(),
            "the key was accepted rather than refused"
        );
        assert!(
            outside.exists(),
            "a file outside the destination was removed"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_refusal_carries_the_category_its_destination_answers_for() {
        use crate::error::ErrorKind;

        let dir = tempfile::tempdir().expect("a temp dir");
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("a directory to stand in for an unremovable file");

        let deleter = DeleteFromLocalTree::new(dir.path());
        let outcomes = deleter.delete(vec!["locked".to_string()]).await;
        let refusal = outcomes[0]
            .as_ref()
            .expect_err("the disk accepted a directory as a file");
        assert_eq!(
            refusal.kind(),
            &ErrorKind::IOError,
            "a local delete refused by the disk reports {:?}",
            refusal.kind()
        );

        let outcomes = deleter.delete(vec!["../outside.txt".to_string()]).await;
        let refusal = outcomes[0]
            .as_ref()
            .expect_err("a key reaching outside the destination was accepted");
        assert_eq!(
            refusal.kind(),
            &ErrorKind::InputInvalid,
            "a key naming no file this run would remove reports {:?}",
            refusal.kind()
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn deleting_a_file_that_is_already_gone_is_not_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = DeleteFromLocalTree::new(dir.path());

        let outcomes = deleter.delete(vec!["never-existed.txt".to_string()]).await;

        assert_eq!(outcomes, vec![Ok("never-existed.txt".to_string())]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_destination_that_does_not_exist_yet_is_not_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let root = dir.path().join("not-there-yet");
        let client = a_bucket_to_download(&["a.txt"], 1_600_000_000);
        let (transfer, ctx, rx) = downloading_into(&root, client, DeleteMode::On);

        run_managed(&transfer, &ctx, rx).await;

        assert!(
            !ctx.is_failed(),
            "a destination that did not exist yet failed the run"
        );
        assert!(
            root.join("a.txt").exists(),
            "the key did not arrive under a root that had to be made"
        );
        assert_eq!(transfer.outcome(), RunOutcome::Clean);
    }

    fn walkdir_s3tmp(root: &Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.to_string_lossy().contains(".s3tmp.") {
                    found.push(path);
                }
            }
        }
        found
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_run_lets_go_of_the_files_its_children_opened() {
        let dir = tempfile::tempdir().expect("a temp dir");

        let pending: Arc<Mutex<Option<TransferContext>>> = Arc::new(Mutex::new(None));
        let fetched = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cancel_on_first = pending.clone();
        let counter = fetched.clone();

        let contents: Vec<Object> = ["a.txt", "b.txt", "c.txt"]
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(5)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                if let Some(ctx) = cancel_on_first.lock().as_ref() {
                    ctx.handle.scheduler.cancel_transfer(ctx.id);
                }
            }
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get]);

        let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);
        *pending.lock() = Some(ctx.clone());
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        let _ = tokio::time::timeout(Duration::from_secs(20), rx).await;

        assert!(
            fetched.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "nothing was fetched, so the cancellation landed before any child opened a file"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !walkdir_s3tmp(dir.path()).is_empty() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let strays = walkdir_s3tmp(dir.path());
        assert!(
            strays.is_empty(),
            "a cancelled run is still holding the files its children opened: {strays:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_download_asks_for_the_key_under_its_prefix() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (client, asked) =
            a_bucket_recording_gets(&["backup/a.txt", "backup/nested/b.txt"], 1_600_000_000);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .downloading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .prefix("backup/")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().downloading(),
            Arc::new(SpawnDownload::new(
                ctx.handle.clone(),
                "amzn-s3-demo-bucket",
                Some("backup/"),
                dir.path(),
            )),
            Deleter::LocalTree(DeleteFromLocalTree::new(dir.path())),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::Off,
                failure_policy: FailurePolicy::Continue,
            },
        );
        run_managed(&transfer, &ctx, rx).await;

        let mut asked = asked.lock().clone();
        asked.sort();
        assert_eq!(
            asked,
            vec![
                "backup/a.txt".to_string(),
                "backup/nested/b.txt".to_string()
            ],
            "a download asked for keys without the prefix it was listing under"
        );
    }

    #[test]
    fn neither_local_site_acts_on_a_key_naming_a_place() {
        let root = Path::new("/tmp/root");
        for key in ["photos/2019/", "a/"] {
            assert!(
                local_path_for_key(root, key).is_err(),
                "a local site would have acted on {key:?}"
            );
        }
        assert_eq!(
            local_path_for_key(root, "photos/2019").expect("a file"),
            Path::new("/tmp/root/photos/2019")
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_that_only_met_an_obstruction_does_not_report_clean() {
        let dir = tempfile::tempdir().expect("a temp dir");
        nix::unistd::mkfifo(&dir.path().join("pipe"), nix::sys::stat::Mode::S_IRWXU)
            .expect("a named pipe");

        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        assert_eq!(
            spawner.asked_count(),
            0,
            "something was sent for a name that holds nothing readable"
        );
        assert_eq!(
            transfer.outcome(),
            RunOutcome::Warned,
            "a run whose only event was an obstruction called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_that_dropped_work_does_not_report_clean() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Continue,
        );

        let mut buffered = 0;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
            buffered = transfer.inner.state.lock().pending_deletes.len();
            if buffered > 0 {
                break;
            }
        }
        assert!(
            buffered > 0,
            "nothing was buffered, so this run had no decided work to drop"
        );

        ctx.set_cancelled();
        while !matches!(transfer.poll_work(), PollWork::Done | PollWork::Pending) {}

        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run that dropped {buffered} decided removal(s) called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_attaches_the_category_the_failure_had() {
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
        use aws_smithy_types::error::metadata::ErrorMetadata;

        let refused = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            ListObjectsV2Error::generic(ErrorMetadata::builder().code("AccessDenied").build())
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&refused]);

        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::Off,
                failure_policy: FailurePolicy::Abort,
            },
        );

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = transfer.poll_work();

        let err = ctx.take_error().expect("an aborting run attached no error");
        assert_eq!(
            *err.kind(),
            crate::error::ErrorKind::ServiceError,
            "a service refusal was handed to a caller under another category"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_listing_that_failed_does_not_release_the_part_batch() {
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
        use aws_smithy_types::error::metadata::ErrorMetadata;

        let page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder()
                    .key("a.txt")
                    .size(0)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                    .build()]))
                .is_truncated(true)
                .next_continuation_token("more")
                .build()
        });
        let refused = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            ListObjectsV2Error::generic(ErrorMetadata::builder().code("InternalError").build())
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&page, &refused]);

        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(deleter.clone()),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }

        assert_eq!(
            deleter.keys_sent(),
            0,
            "a run whose listing failed part way sent the removals it had buffered"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_still_holding_children_does_not_report_clean() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = spawn_until_something_else(&transfer);
        let held = transfer.inner.state.lock().children.len();
        assert!(held > 0, "no child is held, so this proves nothing");

        ctx.set_cancelled();

        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run holding {held} unjoined child(ren) called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_does_not_send_a_batch_that_was_already_in_flight() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Abort,
        );

        let mut held = None;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().deletes_in_flight > 0 {
                held = Some(work);
                break;
            }
            transfer.execute(&mut work).await;
        }
        let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");

        {
            let mut state = transfer.inner.state.lock();
            transfer.stop_if_aborting(
                &mut state,
                crate::error::Error::new(
                    crate::error::ErrorKind::IOError,
                    "a child failed while the batch was away",
                ),
            );
        }
        assert!(
            ctx.is_active(),
            "the status already moved, so the window this test is about is not open"
        );
        assert!(
            transfer.inner.state.lock().stopped_by.is_some(),
            "the run did not decide to stop, so this proves nothing"
        );

        transfer.execute(&mut batch).await;

        assert_eq!(
            deleter.keys_sent(),
            0,
            "an aborting run removed keys it had decided to abandon"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn bytes_a_dropped_child_moved_are_counted() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open_having_moved(512));
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = spawn_until_something_else(&transfer);
        let held = transfer.inner.state.lock().children.len();
        assert!(held > 0, "no child is held, so this proves nothing");
        assert_eq!(
            transfer.inner.state.lock().bytes_moved,
            0,
            "a child was already accounted for, so this would count it twice"
        );

        ctx.set_cancelled();
        transfer.on_terminal();

        assert_eq!(
            transfer.inner.state.lock().bytes_moved,
            512 * held as u64,
            "a run forgot what its dropped children had moved"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn children_dropped_on_notice_leave_their_outcome_unknown() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = spawn_until_something_else(&transfer);
        let held = transfer.inner.state.lock().children.len();
        assert!(held > 0, "no child is held, so this proves nothing");

        ctx.set_cancelled();
        transfer.on_terminal();

        assert!(
            transfer.inner.state.lock().children.is_empty(),
            "the notice left the children where the outstanding question could still see them"
        );
        assert_eq!(
            transfer.inner.state.lock().outcomes_unknown,
            held as u64,
            "a run forgot that it never learned how {held} transfer(s) went"
        );
        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run that cannot say how {held} transfer(s) went called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn keys_qualified_and_never_started_leave_the_plan_short() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
            if !transfer.inner.state.lock().waiting.is_empty() {
                break;
            }
        }
        let queued = transfer.inner.state.lock().waiting.len();
        assert!(queued > 0, "no key is queued, so this proves nothing");
        assert!(
            transfer.inner.state.lock().children.is_empty(),
            "a child is alive, so the children path could set the flag instead"
        );

        ctx.set_cancelled();
        let _ = transfer.poll_work();

        assert!(
            !transfer.is_plan_complete(),
            "a run that dropped {queued} qualified key(s) reported a complete plan"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_reap_that_never_ran_leaves_the_plan_short() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::new(0, true));
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = spawn_until_something_else(&transfer);

        let mut held = None;
        while let PollWork::Ready { io: work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().reap_in_flight > 0 {
                held = Some(work);
                break;
            }
        }
        let outstanding = transfer.inner.state.lock().reap_in_flight;
        assert!(
            outstanding > 0,
            "no reap was dispatched, so this proves nothing"
        );
        drop(held);

        ctx.set_cancelled();

        assert!(
            !transfer.is_plan_complete(),
            "a run that lost {outstanding} child record(s) reported a complete plan"
        );
        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run that lost {outstanding} child record(s) called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_abandoned_batch_leaves_the_plan_short() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Continue,
        );

        let mut held = None;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().deletes_in_flight > 0 {
                held = Some(work);
                break;
            }
            transfer.execute(&mut work).await;
        }
        let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");
        assert!(
            transfer.inner.state.lock().pending_deletes.is_empty(),
            "the keys are still in the buffer, so the accounting sites would see them"
        );

        ctx.set_cancelled();
        transfer.execute(&mut batch).await;

        assert!(
            !transfer.is_plan_complete(),
            "a run that let go of decided removals reported a complete plan"
        );
        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run that let go of decided removals called itself clean"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_refused_key_reaches_a_caller_as_a_service_refusal() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Abort,
        );

        drive(&transfer).await;

        let err = ctx
            .take_error()
            .expect("an aborting run attached no error for a refused key");
        assert_eq!(
            *err.kind(),
            crate::error::ErrorKind::ServiceError,
            "a key the service refused reached a caller under another category"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_source_root_nobody_can_list_fails_the_run() {
        for policy in [FailurePolicy::Continue, FailurePolicy::Abort] {
            let dir = tempfile::tempdir().expect("a temp dir");
            let absent = dir.path().join("not-there");
            let spawner = Arc::new(SpawnEnded::new(0, false));
            let (transfer, ctx) = uploading_with_policy(&absent, spawner, 4, policy);

            while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
                transfer.execute(&mut work).await;
            }

            assert!(
                ctx.is_failed(),
                "under {policy:?} a run that never read its source carried on to a status a \
                 caller reads as finished"
            );
            assert_eq!(
                transfer.outcome(),
                RunOutcome::Failed,
                "under {policy:?} a run that never read its source reported otherwise"
            );
        }
    }

    #[test]
    fn the_local_deleter_refuses_a_key_its_derivation_would_rewrite() {
        let deleter = DeleteFromLocalTree::new("/tmp/root");
        for key in ["a//b", "a/./b", "a/../b", "./a"] {
            assert!(
                deleter.file_path(key).is_err(),
                "the deleter accepted {key:?}, whose path names a different key's file"
            );
        }
        assert_eq!(
            deleter.file_path("a/b").expect("a key from a real path"),
            std::path::Path::new("/tmp/root/a/b")
        );
    }

    #[test]
    fn the_local_deleter_accepts_its_keys_under_a_root_written_the_long_way_round() {
        for root in [".", "./", "/tmp/other/../root", "/tmp/./root"] {
            let deleter = DeleteFromLocalTree::new(root);
            let got = deleter.file_path("a/b");
            assert!(
                got.is_ok(),
                "root {root:?} refused a key inside it: {}",
                got.unwrap_err()
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_abandoned_batch_gives_back_every_slot_it_took() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt", "gone-c.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Continue,
        );

        let mut held = None;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().deletes_in_flight > 0 {
                held = Some(work);
                break;
            }
            transfer.execute(&mut work).await;
        }
        let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");
        let took = transfer.inner.state.lock().deletes_in_flight;
        assert!(took > 1, "a batch of one slot cannot show the leak");

        ctx.set_cancelled();
        transfer.execute(&mut batch).await;

        assert_eq!(
            transfer.inner.state.lock().deletes_in_flight,
            0,
            "a batch of {took} keys gave back fewer slots than it took, so the run can never end"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_stopped_run_reports_fewer_arrivals_than_it_decided() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            let _ = transfer.execute(&mut work).await;
        }
        let _ = transfer.poll_work();
        let _ = transfer.poll_work();
        assert!(
            !transfer.inner.state.lock().waiting.is_empty(),
            "nothing was left buffered, so this proves nothing about the counts"
        );

        spawner.release(&ctx);
        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert!(state.waiting.is_empty(), "the teardown left keys buffered");
        assert_eq!(
            state.decided.transfers, 3,
            "the run decided {} transfers for three keys",
            state.decided.transfers
        );
        assert_eq!(
            state.transferred, 1,
            "one key arrived and the run reports {}",
            state.transferred
        );
        assert!(
            state.plan_incomplete,
            "a run that abandoned a decided transfer called its plan whole"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_starts_no_further_child() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            let _ = transfer.execute(&mut work).await;
        }
        assert!(
            !transfer.inner.state.lock().waiting.is_empty(),
            "nothing is waiting, so no spawn would have been attempted either way"
        );

        let asked_before = spawner.asked_count();
        ctx.set_cancelled();
        let spawned = {
            let mut state = transfer.inner.state.lock();
            transfer.spawn_one(&mut state)
        };

        assert!(!spawned, "a cancelled run enqueued another child");
        assert_eq!(
            spawner.asked_count(),
            asked_before,
            "a cancelled run asked the spawner for one more key"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_does_not_start_the_keys_after_the_one_that_failed() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = transfer.poll_work();
        let _ = transfer.poll_work();

        assert!(
            transfer.inner.state.lock().stopped_by.is_some(),
            "the run did not decide to stop, so the guard this test is about was never reached"
        );
        assert_eq!(
            spawner.asked_count(),
            2,
            "an aborting run asked for a key queued behind the one that ended it"
        );
        assert!(
            !ctx.is_failed(),
            "the status flipped before a pass could carry the run to its end"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_does_not_send_a_batch_already_handed_over() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Continue,
        );

        let mut held = None;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().deletes_in_flight > 0 {
                held = Some(work);
                break;
            }
            transfer.execute(&mut work).await;
        }
        let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");

        ctx.set_cancelled();
        transfer.execute(&mut batch).await;

        assert_eq!(
            deleter.keys_sent(),
            0,
            "a cancelled run sent a batch it had already handed over"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_does_not_start_the_keys_it_had_buffered() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let buffered = transfer.inner.state.lock().waiting.len();
        assert!(
            buffered > 0,
            "no key is waiting, so the guard this test is about holds nothing"
        );
        let started_before = spawner.asked_count();

        ctx.set_cancelled();
        let _ = transfer.poll_work();

        assert_eq!(
            spawner.asked_count(),
            started_before,
            "a cancelled run started {buffered} key(s) it had already qualified"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_keeps_the_children_it_already_sent() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let spawned = spawn_until_something_else(&transfer);
        assert!(
            matches!(spawned, PollWork::Pending),
            "the run did not park with children live, so this proves nothing"
        );
        let live = transfer.inner.state.lock().children.len();
        assert!(live > 0, "no child is live, so there is nothing to keep");

        ctx.set_cancelled();

        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a cancelled run called itself over with {live} children still away"
        );
        assert_eq!(
            spawner.asked_count(),
            live,
            "a cancelled run asked for another child"
        );

        spawner.release(&ctx);
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "the children ended and the cancelled run still did not finish"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_before_a_failure_still_reports_cancelled() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            let _ = transfer.execute(&mut work).await;
        }
        let _ = transfer.poll_work();
        let _ = transfer.poll_work();

        assert!(
            transfer.inner.state.lock().stopped_by.is_some(),
            "nothing failed, so the cancellation has no failure to beat"
        );
        assert!(
            !ctx.is_failed(),
            "the failure reached the status already, leaving no window to cancel in"
        );

        ctx.set_cancelled();
        spawner.release(&ctx);
        drive(&transfer).await;

        assert!(ctx.is_cancelled(), "the cancellation was lost");
        assert!(
            !ctx.is_failed(),
            "a cancelled run reported the failure it was carrying instead"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_aborting_run_answers_its_waiter_without_another_poll() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            spawner.clone(),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Abort,
            },
        );

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        spawner.release(&ctx);
        let answered = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("an aborting run never answered its waiter");
        assert!(
            answered.is_ok(),
            "the run was removed without signalling, so its waiter got nothing"
        );
        assert!(ctx.is_failed(), "the run did not report itself failed");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_does_not_send_the_deletes_it_buffered() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let deleter = Arc::new(RecordDeletes::new(1));
        let client = a_bucket_holding(&["zz-gone.txt"]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::refusing_to_spawn()),
            Deleter::Recording(deleter.clone()),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Abort,
            },
        );

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }

        assert!(ctx.is_failed(), "the refused spawn did not abort the run");
        assert_eq!(
            deleter.keys_sent(),
            0,
            "an aborting run sent {} keys it was supposed to drop",
            deleter.keys_sent()
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_loop_warns_under_either_policy() {
        for policy in [FailurePolicy::Continue, FailurePolicy::Abort] {
            let dir = tempfile::tempdir().expect("a temp dir");
            std::fs::write(dir.path().join("a.txt"), b"x").expect("a file");
            let inner = dir.path().join("down");
            std::fs::create_dir(&inner).expect("a directory");
            std::os::unix::fs::symlink(dir.path(), inner.join("up")).expect("a symlink");

            let client = a_bucket_holding(&[]);
            let config = crate::Config::builder().client(client.clone()).build();
            let handle = crate::client::Handle::test_handle_tokio(config);
            let (ctx, _rx) = TransferContext::new(handle);
            let walk = Walker::builder()
                .follow_symlinks(true)
                .build()
                .uploading(
                    LocalAndBucket::builder()
                        .local_root(dir.path())
                        .client(client)
                        .bucket("amzn-s3-demo-bucket")
                        .build(),
                )
                .expect("an ordinary bucket builds");
            let transfer = SyncTransfer::new(
                ctx.clone(),
                walk,
                Mode::default().uploading(),
                Arc::new(SpawnEnded::new(0, false)),
                Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
                RunSettings {
                    max_children: 2,
                    delete_mode: DeleteMode::On,
                    failure_policy: policy,
                },
            );

            drive(&transfer).await;

            let state = transfer.inner.state.lock();
            assert_eq!(
                state.warnings.sample().len(),
                1,
                "under {policy:?} the loop was not kept where a caller can read it"
            );
            assert!(
                state.failures.sample().is_empty(),
                "a loop nothing could transfer was filed as a failure: {:?}",
                state.failures
            );
            drop(state);
            assert_eq!(
                transfer.outcome(),
                RunOutcome::Warned,
                "under {policy:?} the run did not report a warning"
            );
            assert!(
                !ctx.is_failed(),
                "an aborting run failed over something it was never going to send"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_failure_ends_an_aborting_run_and_lets_its_batch_go() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(1));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Abort,
        );

        drive(&transfer).await;

        assert_eq!(
            transfer.outcome(),
            RunOutcome::Failed,
            "a refused delete did not make the run report a failure"
        );
        assert!(
            ctx.is_failed(),
            "a refused delete ended an aborting run without recording it as failed"
        );
        assert!(
            transfer.inner.state.lock().pending_deletes.is_empty(),
            "an ended run kept keys judged against a stream it stopped reading"
        );
        assert_eq!(
            deleter.keys_sent(),
            1,
            "an ended run sent a key judged against a stream it stopped reading"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_same_failure_leaves_a_continuing_run_going() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::On,
            FailurePolicy::Continue,
        );

        drive(&transfer).await;

        assert_eq!(transfer.outcome(), RunOutcome::Failed);
        assert_eq!(
            deleter.keys_sent(),
            2,
            "a continuing run stopped before asking about every key"
        );
        assert!(
            !ctx.is_active(),
            "the run should have completed rather than stayed open"
        );
        assert!(
            !ctx.is_failed(),
            "a continuing run reported itself as failed"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_badly_described_key_ends_an_aborting_run() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("d.txt").size(0).build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&rule]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Abort,
            },
        );

        drive(&transfer).await;

        assert_eq!(transfer.outcome(), RunOutcome::Failed);
        assert!(
            ctx.is_failed(),
            "a failure on the walk's own channel did not end an aborting run"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_child_that_failed_ends_an_aborting_run() {
        for (policy, expect_failed) in [
            (FailurePolicy::Continue, false),
            (FailurePolicy::Abort, true),
        ] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt", "b.txt"]);
            let (transfer, ctx) = with_spawner(dir.path(), SpawnEnded::new(0, true), policy);

            drive(&transfer).await;

            assert!(
                transfer.inner.state.lock().transfer_failures.any(),
                "under {policy:?} the failed child was not counted"
            );
            assert_eq!(
                ctx.is_failed(),
                expect_failed,
                "under {policy:?} the run's status does not match the policy"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_keeps_the_failure_the_site_had() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = with_spawner(
            dir.path(),
            SpawnEnded::refusing_to_spawn(),
            FailurePolicy::Abort,
        );

        drive(&transfer).await;

        let err = ctx.take_error().expect("an aborting run attached no error");
        assert_eq!(
            *err.kind(),
            crate::error::ErrorKind::ObjectNotDiscoverable,
            "the run replaced the site's failure with a category of its own"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_child_that_could_not_be_built_ends_an_aborting_run() {
        for (policy, expect_failed) in [
            (FailurePolicy::Continue, false),
            (FailurePolicy::Abort, true),
        ] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt", "b.txt"]);
            let (transfer, ctx) = with_spawner(dir.path(), SpawnEnded::refusing_to_spawn(), policy);

            drive(&transfer).await;

            assert!(
                transfer.inner.state.lock().transfer_failures.any(),
                "under {policy:?} the refused spawn was not counted"
            );
            assert_eq!(
                ctx.is_failed(),
                expect_failed,
                "under {policy:?} the run's status does not match the policy"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_with_nothing_wrong_reports_clean() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["gone.txt"], deleter);

        drive(&transfer).await;

        assert_eq!(transfer.outcome(), RunOutcome::Clean);
    }

    #[test]
    fn the_defaults_are_continue_and_no_deleting() {
        assert_eq!(FailurePolicy::default(), FailurePolicy::Continue);
        assert_eq!(DeleteMode::default(), DeleteMode::Off);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_the_listing_described_badly_is_kept_as_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("d.txt").size(0).build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        assert_eq!(
            drive(&transfer).await,
            0,
            "no key was described well enough to pair"
        );
        assert_eq!(
            transfer.inner.state.lock().failures.sample().len(),
            1,
            "the run ended without keeping what it could not account for"
        );
        assert!(
            !transfer.is_plan_complete(),
            "a key the listing described badly left the plan looking whole"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_tree_larger_than_one_batch_takes_several_work_items() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..MERGE_BATCH * 3)
            .map(|n| format!("f{n:04}.txt"))
            .collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let (transfer, _ctx) = uploading(dir.path(), &[]);
        let mut items = 0;
        loop {
            match transfer.poll_work() {
                PollWork::Ready { io: mut work, .. } => {
                    items += 1;
                    transfer.execute(&mut work).await;
                }
                PollWork::Spawned => {}
                PollWork::Done => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(transfer.inner.state.lock().paired, keys.len() as u64);
        assert!(
            items > 1,
            "a tree of {} keys came back in one work item, so the batch bound did nothing",
            keys.len()
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_scheduler_gets_the_run_to_the_end_on_its_own() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..MERGE_BATCH * 3)
            .map(|n| format!("f{n:04}.txt"))
            .collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run parked: a work item finished without waking the transfer")
            .expect("the terminal signal was dropped");

        assert_eq!(transfer.inner.state.lock().paired, keys.len() as u64);
        assert!(
            keys.len() > MERGE_BATCH,
            "the tree fits in one work item, so the run never parked and the wake went untested"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_batch_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut walk = transfer.inner.state.lock().walk.take().expect("the merge");
        while walk.next().await.is_some() {}
        assert_eq!(
            walk.progress(),
            Progress::Accounted,
            "the merge did not reach the end"
        );
        {
            let mut state = transfer.inner.state.lock();
            state.walk = Some(walk);
            state.merge_in_flight = true;
        }
        assert!(ctx.is_active(), "the transfer ended before the assertion");

        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a finished merge with a batch still out reported the run over"
        );

        transfer.inner.state.lock().merge_in_flight = false;
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "with nothing outstanding the run is not over"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_item_that_drains_the_merge_answers_the_waiter() {
        let dir = tempfile::tempdir().expect("a temp dir");

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        transfer.execute(&mut work).await;

        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run drained inside a work item and left its waiter unanswered")
            .expect("the terminal signal was dropped");
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_where_everything_fails_keeps_a_bounded_sample() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("a temp dir");
        let wanted = FAILURES_KEPT + 40;
        let mut locked = Vec::new();
        for n in 0..wanted {
            let sub = dir.path().join(format!("d{n:04}"));
            std::fs::create_dir(&sub).expect("a directory");
            std::fs::write(sub.join("inner.txt"), b"").expect("a file inside it");
            std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).expect("chmod");
            locked.push(sub);
        }
        let usable = locked.iter().all(|p| std::fs::read_dir(p).is_err());

        let (transfer, _ctx) = uploading(dir.path(), &[]);
        drive(&transfer).await;

        for sub in &locked {
            std::fs::set_permissions(sub, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        assert!(
            usable,
            "the fixture did not produce unreadable directories, so nothing was tested"
        );

        let state = transfer.inner.state.lock();
        assert!(
            state.failures.sample().len() <= FAILURES_KEPT,
            "the run kept {} failures, so peak memory follows the tree",
            state.failures.sample().len()
        );
        assert!(
            state.failures.total() > state.failures.sample().len() as u64,
            "the total did not outrun the sample, so the cap was never reached"
        );
    }

    fn spawn_until_something_else(transfer: &SyncTransfer<FsWalk, S3Walk>) -> PollWork {
        loop {
            match transfer.poll_work() {
                PollWork::Spawned => continue,
                other => return other,
            }
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_reports_the_transfers_that_arrived() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::new(7, true));
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers, 2,
            "the run decided {} transfers for two keys",
            state.decided.transfers
        );
        assert_eq!(
            state.transferred, 0,
            "both children ended badly and the run reports {} arrivals",
            state.transferred
        );
    }

    fn uploading_with(
        local: &Path,
        spawner: Arc<SpawnEnded>,
        cap: usize,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        uploading_with_policy(local, spawner, cap, FailurePolicy::Continue)
    }

    fn uploading_with_policy(
        local: &Path,
        spawner: Arc<SpawnEnded>,
        cap: usize,
        failure_policy: FailurePolicy,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            spawner,
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: cap,
                delete_mode: DeleteMode::On,
                failure_policy,
            },
        );
        (transfer, ctx)
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_decision_still_waiting_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        a_local_tree(dir.path(), &["b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 1);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let parked = spawn_until_something_else(&transfer);

        let state = transfer.inner.state.lock();
        assert!(
            !state.waiting.is_empty(),
            "nothing is waiting, so this proves nothing about the buffer"
        );
        drop(state);
        assert!(
            matches!(parked, PollWork::Pending),
            "a decision still waiting for a slot did not hold the run open"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_decision_a_stopped_run_gives_back_keeps_the_decision_it_was_made_with() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, _ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = transfer.poll_work();
        let _ = transfer.poll_work();
        let _ = transfer.poll_work();

        let state = transfer.inner.state.lock();
        assert!(
            state.stopped_by.is_some(),
            "the run did not stop, so nothing was given back"
        );
        assert!(
            !state.waiting.is_empty(),
            "nothing was given back, so this proves nothing about the decision"
        );
        for (pairing, decision) in state.waiting.iter() {
            assert!(
                matches!(decision, Decision::Transfer(_)),
                "the key {:?} was qualified for transfer and came back as {decision:?}",
                pairing.key(),
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_live_child_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let parked = spawn_until_something_else(&transfer);

        assert_eq!(spawner.asked_count(), 1, "no child was spawned");
        assert!(
            matches!(parked, PollWork::Pending),
            "a child that has not ended did not hold the run open"
        );

        spawner.release(&ctx);
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_reap_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, _ctx) = uploading_with(dir.path(), spawner, 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let mut reap = match spawn_until_something_else(&transfer) {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a reap, got {other:?}"),
        };
        {
            let state = transfer.inner.state.lock();
            assert!(state.children.is_empty(), "the child is still in the map");
            assert_eq!(state.reap_in_flight, 1, "the reap is not accounted for");
        }
        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a reap still out did not hold the run open, so its child's outcome would be lost"
        );

        transfer.execute(&mut reap).await;
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn no_more_children_are_live_than_the_cap_allows() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..12).map(|n| format!("f{n:02}.txt")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let cap = 3;
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), cap);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let mut seen = 0usize;
        let parked = loop {
            match transfer.poll_work() {
                PollWork::Spawned => {
                    let live = transfer.inner.state.lock().children.len();
                    seen = seen.max(live);
                    assert!(
                        live <= cap,
                        "{live} children were live against a cap of {cap}"
                    );
                }
                other => break other,
            }
        };
        assert_eq!(seen, cap, "the cap was never reached, so it was not tested");
        assert!(
            matches!(parked, PollWork::Pending),
            "with every slot full and nine keys still waiting, the poll had nothing else to answer"
        );
        assert_eq!(
            spawner.asked_count(),
            cap,
            "more children were built than the cap allows"
        );
    }

    struct AlwaysTransfers;

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysTransfers {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden")
        }

        fn compare(
            &self,
            _pairing: &Pairing<crate::io::walk::FsEntry, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            Verdict::decided(Decision::transfer(
                crate::operation::sync::compare::TransferReason::Missing,
            ))
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_transfer_decided_on_an_absent_source_does_not_strand_the_rest() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["b.txt"]);
        let client = a_bucket_holding(&["a.txt"]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let transfer = SyncTransfer::new(
            ctx,
            walk,
            &AlwaysTransfers,
            spawner.clone(),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        let paired = drive(&transfer).await;
        assert_eq!(paired, 2);
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.transfer_failures.total(),
            1,
            "the key with no source was not accounted for"
        );
        assert_eq!(
            spawner.asked_count(),
            1,
            "the key behind it was never spawned"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn spawning_uses_the_size_the_walk_read() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"0123456789").expect("a file");

        let mut walked = crate::io::walk::FsWalker::builder().build().walk(
            crate::io::walk::FsWalkContext::builder()
                .root(dir.path())
                .build(),
        );
        let entry = match walked.next().await {
            Some(Ok(entry)) => entry,
            Some(Err(err)) => panic!("the walk failed: {err}"),
            None => panic!("the walk produced nothing"),
        };
        assert!(
            entry.metadata().is_some(),
            "the walk read no metadata, so this test cannot tell the two paths apart"
        );

        std::fs::remove_file(&path).expect("remove");

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let spawner = SpawnUpload::new(handle, "amzn-s3-demo-bucket", None);
        assert!(
            spawner.spawn("a.txt", &entry, 1).is_ok(),
            "the spawn stat'd the path again instead of using the size the walk read"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_uploaded_key_is_named_under_the_runs_prefix() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "nested/c.txt"]);

        let (client, puts) = a_bucket_recording_puts(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .prefix("data")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnUpload::new(
                ctx.handle.clone(),
                "amzn-s3-demo-bucket",
                Some("data"),
            )),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run did not finish")
            .expect("the terminal signal was dropped");

        let mut written = puts.lock().clone();
        written.sort();
        assert_eq!(
            written,
            vec!["data/a.txt".to_string(), "data/nested/c.txt".to_string()],
            "the writes did not land under the run's prefix"
        );
    }

    fn deleting(
        local: &Path,
        bucket_keys: &[&str],
        deleter: Arc<RecordDeletes>,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        deleting_with(local, bucket_keys, deleter, DeleteMode::On)
    }

    fn with_spawner(
        local: &Path,
        spawner: SpawnEnded,
        failure_policy: FailurePolicy,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(spawner),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy,
            },
        );
        (transfer, ctx)
    }

    fn deleting_with_policy(
        local: &Path,
        bucket_keys: &[&str],
        deleter: Arc<RecordDeletes>,
        delete_mode: DeleteMode,
        failure_policy: FailurePolicy,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(bucket_keys);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(deleter),
            RunSettings {
                max_children: 4,
                delete_mode,
                failure_policy,
            },
        );
        (transfer, ctx)
    }

    fn uploading_comparing_with<C>(
        local: &Path,
        comparison: &'static C,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext)
    where
        C: Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> + Send + Sync + 'static,
    {
        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            comparison,
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::Off,
                failure_policy: FailurePolicy::Continue,
            },
        );
        (transfer, ctx)
    }

    fn deleting_with(
        local: &Path,
        bucket_keys: &[&str],
        deleter: Arc<RecordDeletes>,
        delete_mode: DeleteMode,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(bucket_keys);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Deleter::Recording(deleter),
            RunSettings {
                max_children: 4,
                delete_mode,
                failure_policy: FailurePolicy::Continue,
            },
        );
        (transfer, ctx)
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn destination_only_keys_are_deleted_in_batches() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..7).map(|n| format!("gone{n}.txt")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let deleter = Arc::new(RecordDeletes::new(3));
        let (transfer, _ctx) = deleting(dir.path(), &refs, deleter.clone());

        drive(&transfer).await;

        let batches = deleter.batches();
        assert_eq!(
            deleter.keys_sent(),
            7,
            "not every key reached the delete path"
        );
        assert!(
            batches.len() > 1,
            "seven keys went out in {} request(s), so nothing was batched",
            batches.len()
        );
        assert!(
            batches.iter().all(|b| b.len() <= 3),
            "a batch exceeded the limit: {batches:?}"
        );
        let state = transfer.inner.state.lock();
        assert_eq!(state.deleted, 7, "each key's outcome was not counted");
        assert_eq!(
            state.decided.deletable, 7,
            "a run allowed to delete does not report what the comparison marked"
        );
        assert_eq!(state.refusals.total(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_refused_delete_is_counted_against_its_own_key() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.refusals.total(),
            2,
            "a refusal was not attributed per key"
        );
        assert_eq!(state.deleted, 0, "a refused key was counted as removed");
        assert!(
            state
                .refusals
                .sample()
                .iter()
                .any(|why| why.starts_with("a.txt:"))
                && state
                    .refusals
                    .sample()
                    .iter()
                    .any(|why| why.starts_with("b.txt:")),
            "the refusal did not reach the record naming its key: {:?}",
            state.refusals
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_not_asked_to_delete_leaves_the_key_and_still_counts_it() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, _ctx) = deleting_with(
            dir.path(),
            &["gone-a.txt", "gone-b.txt"],
            deleter.clone(),
            DeleteMode::Off,
        );

        drive(&transfer).await;

        assert_eq!(
            deleter.keys_sent(),
            0,
            "a run that was not asked to delete issued {} keys anyway",
            deleter.keys_sent()
        );
        let state = transfer.inner.state.lock();
        assert_eq!(state.deleted, 0, "a key was reported as removed");
        assert_eq!(state.decided.deletes, 0, "a delete was decided");
        assert_eq!(
            state.decided.skipped.total(),
            2,
            "the two keys left alone were not accounted for"
        );
        assert_eq!(
            state.decided.deletable, 2,
            "the run cannot say how many objects turning deletion on would remove"
        );
        assert!(state.pending_deletes.is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_deleter_reports_each_key_from_the_response_it_got() {
        use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
        use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

        let quiet_flags = Arc::new(Mutex::new(Vec::<Option<bool>>::new()));
        let quiet_recorder = quiet_flags.clone();
        let answered = mock!(aws_sdk_s3::Client::delete_objects)
            .match_requests(move |req| {
                quiet_recorder
                    .lock()
                    .push(req.delete().and_then(|d| d.quiet()));
                true
            })
            .then_output(|| {
                DeleteObjectsOutput::builder()
                    .set_deleted(Some(vec![DeletedObject::builder()
                        .key("data/gone.txt")
                        .build()]))
                    .set_errors(Some(vec![
                        S3Error::builder()
                            .key("data/held.txt")
                            .code("AccessDenied")
                            .message("denied")
                            .build(),
                        S3Error::builder()
                            .key("data/terse.txt")
                            .code("InvalidArgument")
                            .build(),
                    ]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&answered]);

        let deleter = Deleter::Bucket(DeleteFromBucket::new(
            client,
            "amzn-s3-demo-bucket",
            Some("data"),
        ));
        let outcomes = deleter
            .delete(
                ["gone.txt", "held.txt", "silent.txt", "terse.txt"],
                still_running(),
            )
            .await;

        assert_eq!(
            outcomes.len(),
            4,
            "the outcomes do not account for every key sent"
        );
        assert_eq!(outcomes[0], Ok("gone.txt".to_string()));
        let held = outcomes[1]
            .as_ref()
            .expect_err("a refusal was read as success");
        assert!(
            held.why().starts_with("held.txt:") && held.why().contains("denied"),
            "a refusal did not name its key and reason: {held:?}"
        );
        let silent = outcomes[2]
            .as_ref()
            .expect_err("a key the response skipped was read as success");
        assert!(
            silent.why().starts_with("silent.txt:"),
            "an unmentioned key was not named: {silent:?}"
        );
        let terse = outcomes[3]
            .as_ref()
            .expect_err("a refusal was read as success");
        assert!(
            terse.why().starts_with("terse.txt:") && terse.why().contains("InvalidArgument"),
            "a refusal carrying only a code reported no reason: {terse:?}"
        );
        assert!(
            quiet_flags.lock().iter().all(|q| *q == Some(false)),
            "the request left the response's verbosity to chance: {:?}",
            quiet_flags.lock()
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn a_key_refused_for_load_is_asked_about_again() {
        use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
        use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

        let asked = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let recorder = asked.clone();
        let quiet_flags = Arc::new(Mutex::new(Vec::<Option<bool>>::new()));
        let quiet_recorder = quiet_flags.clone();
        let first = mock!(aws_sdk_s3::Client::delete_objects)
            .match_requests(move |req| {
                let names = req
                    .delete()
                    .map(|d| {
                        d.objects()
                            .iter()
                            .map(|o| o.key().to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                recorder.lock().push(names);
                quiet_recorder
                    .lock()
                    .push(req.delete().and_then(|d| d.quiet()));
                true
            })
            .sequence()
            .output(|| {
                DeleteObjectsOutput::builder()
                    .set_deleted(Some(vec![DeletedObject::builder().key("gone.txt").build()]))
                    .set_errors(Some(vec![S3Error::builder()
                        .key("busy.txt")
                        .code("SlowDown")
                        .message("slow down")
                        .build()]))
                    .build()
            })
            .output(|| {
                DeleteObjectsOutput::builder()
                    .set_deleted(Some(vec![DeletedObject::builder().key("busy.txt").build()]))
                    .build()
            })
            .build();
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&first]);

        let deleter = Deleter::Bucket(DeleteFromBucket::new(client, "amzn-s3-demo-bucket", None));
        let started = tokio::time::Instant::now();
        let outcomes = deleter
            .delete(["gone.txt", "busy.txt"], still_running())
            .await;
        let waited = started.elapsed();

        assert_eq!(
            outcomes,
            vec![Ok("gone.txt".to_string()), Ok("busy.txt".to_string())],
            "a key refused for load was not asked about again"
        );
        let batches = asked.lock().clone();
        assert_eq!(
            batches.len(),
            2,
            "the refusal did not cost a second request"
        );
        assert_eq!(
            batches[1],
            vec!["busy.txt".to_string()],
            "the second request named keys the first had already removed"
        );
        assert!(
            waited > Duration::ZERO,
            "the refused key was asked about again with no wait"
        );
        assert!(
            quiet_flags.lock().iter().all(|q| *q == Some(false)),
            "a request left the response's verbosity to chance: {:?}",
            quiet_flags.lock()
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn a_run_that_stops_while_a_key_waits_sends_no_further_attempt() {
        use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
        use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

        let asked = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let recorder = asked.clone();
        let rule = mock!(aws_sdk_s3::Client::delete_objects)
            .match_requests(move |req| {
                let names = req
                    .delete()
                    .map(|d| {
                        d.objects()
                            .iter()
                            .map(|o| o.key().to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                recorder.lock().push(names);
                true
            })
            .then_output(|| {
                DeleteObjectsOutput::builder()
                    .set_deleted(Some(vec![DeletedObject::builder().key("gone.txt").build()]))
                    .set_errors(Some(vec![S3Error::builder()
                        .key("busy.txt")
                        .code("SlowDown")
                        .message("slow down")
                        .build()]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&rule]);
        let deleter = Deleter::Bucket(DeleteFromBucket::new(client, "amzn-s3-demo-bucket", None));

        let stopped = || true;
        let outcomes = deleter.delete(["gone.txt", "busy.txt"], &stopped).await;

        let batches = asked.lock().clone();
        assert_eq!(
            batches.len(),
            1,
            "a stopped run asked again for a throttled key: {batches:?}"
        );
        assert_eq!(
            outcomes[0],
            Ok("gone.txt".to_string()),
            "the key the response removed was not reported as removed"
        );
        let busy = outcomes[1]
            .as_ref()
            .expect_err("a key never answered for was reported as removed");
        assert!(
            busy.why().contains("SlowDown"),
            "the key lost the reason the service gave for refusing it: {busy:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_delete_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["a.txt"], deleter);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            let is_delete = matches!(
                work.data.as_ref().map(|d| format!("{d:?}")),
                Some(ref s) if s.starts_with("DeleteKeys")
            );
            if is_delete {
                {
                    let state = transfer.inner.state.lock();
                    assert!(
                        state.pending_deletes.is_empty(),
                        "the keys did not leave the buffer when the batch was built"
                    );
                    assert_eq!(state.deletes_in_flight, 1);
                }
                assert!(
                    matches!(transfer.poll_work(), PollWork::Pending),
                    "a delete still out did not hold the run open"
                );
                transfer.execute(&mut work).await;
                break;
            }
            transfer.execute(&mut work).await;
        }
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_lets_the_pending_batch_go() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected the merge, got {other:?}"),
        };
        transfer.execute(&mut work).await;
        assert_eq!(
            transfer.inner.state.lock().pending_deletes.len(),
            2,
            "the keys were not waiting, so this proves nothing"
        );

        ctx.set_cancelled();
        assert!(matches!(transfer.poll_work(), PollWork::Done));
        assert_eq!(
            deleter.keys_sent(),
            0,
            "a cancelled run still issued deletes judged against an incomplete source"
        );
        assert!(transfer.inner.state.lock().pending_deletes.is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_qualified_key_reaches_the_bucket() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);

        let (client, put_keys) = a_bucket_recording_puts(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let spawner = Arc::new(SpawnUpload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            None,
        ));
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            spawner,
            Deleter::Recording(Arc::new(RecordDeletes::new(DELETE_BATCH))),
            RunSettings {
                max_children: 4,
                delete_mode: DeleteMode::On,
                failure_policy: FailurePolicy::Continue,
            },
        );

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run never finished, so a child was spawned and never reaped")
            .expect("the terminal signal was dropped");

        let mut sent = put_keys.lock().clone();
        sent.sort();
        assert_eq!(
            sent,
            vec!["a.txt".to_string(), "b.txt".to_string()],
            "the bucket did not receive both keys"
        );

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers, 2,
            "both keys should have been sent"
        );
        assert_eq!(
            state.transfer_failures.total(),
            0,
            "a child failed, so the upload path is not working"
        );
        assert!(
            state.children.is_empty() && state.reap_in_flight == 0,
            "the run ended with children unaccounted for"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_waits_for_the_batch_it_dispatched() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        ctx.set_cancelled();

        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a cancelled run reported itself over with a batch still out"
        );

        transfer.execute(&mut work).await;
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "the batch came back and the cancelled run still did not end"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_hands_the_merge_back() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, ctx) = uploading_with_policy(dir.path(), spawner, 4, FailurePolicy::Abort);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };

        {
            let mut state = transfer.inner.state.lock();
            transfer.stop_if_aborting(
                &mut state,
                crate::error::Error::new(crate::error::ErrorKind::IOError, "a child failed"),
            );
        }
        assert!(
            ctx.is_active(),
            "the status already moved, so the window this test is about is not open"
        );

        assert!(matches!(
            transfer.execute(&mut work).await,
            WorkOutcome::Cancelled
        ));
        let state = transfer.inner.state.lock();
        assert!(
            state.walk.is_some() && !state.merge_in_flight,
            "an aborting work item left the merge where no poll can reach it"
        );
        assert_eq!(
            state.decided.transfers, 0,
            "an aborting run paired and qualified keys after deciding to stop"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_hands_the_merge_back() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        ctx.set_cancelled();

        assert!(matches!(
            transfer.execute(&mut work).await,
            WorkOutcome::Cancelled
        ));
        let state = transfer.inner.state.lock();
        assert!(
            state.walk.is_some() && !state.merge_in_flight,
            "a cancelled work item left the merge where no poll can reach it"
        );
    }

    // ======================================================================
    // Real-bucket runs. `#[ignore]`d, so the ordinary suite never reaches S3:
    //
    //   S3_TEST_BUCKET_NAME_RS=<bucket> AWS_REGION=<region> \
    //   SSL_CERT_FILE=/etc/ssl/cert.pem \
    //     cargo test -p aws-sdk-s3-transfer-manager --lib real_bucket \
    //     -- --ignored --test-threads=1 --nocapture
    //
    // rustls may not read the platform root certificates. Set `SSL_CERT_FILE` before running ignored
    // real-bucket tests.
    //
    // TODO(sync): Ignored real-bucket tests call crate-private sync APIs. Move customer-facing cases to
    // `examples/` after the public API can start a sync run.
    // ======================================================================

    const BUCKET_VAR: &str = "S3_TEST_BUCKET_NAME_RS";

    fn named_bucket(var: &str) -> String {
        std::env::var(var).unwrap_or_else(|_| {
            panic!("set {var} to a bucket these tests may write to and delete from")
        })
    }

    fn regular_bucket() -> String {
        named_bucket(BUCKET_VAR)
    }

    async fn real_client() -> aws_sdk_s3::Client {
        let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_s3::Client::new(&sdk)
    }

    fn a_run_prefix(what: &str) -> String {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos();
        format!("sync-e2e/{what}-{stamp}/")
    }

    async fn bucket_keys(c: &aws_sdk_s3::Client, prefix: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = c.list_objects_v2().bucket(regular_bucket()).prefix(prefix);
            if let Some(t) = token {
                req = req.continuation_token(t);
            }
            let page = req.send().await.expect("the listing answers");
            for o in page.contents() {
                if let Some(k) = o.key() {
                    out.push(k.strip_prefix(prefix).unwrap_or(k).to_string());
                }
            }
            token = page.next_continuation_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        out.sort();
        out
    }

    fn tree_files(root: &Path) -> Vec<String> {
        fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, base, out);
                } else {
                    let rel = p.strip_prefix(base).expect("a path under the root");
                    out.push(
                        rel.to_string_lossy()
                            .replace(std::path::MAIN_SEPARATOR, "/"),
                    );
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out.sort();
        out
    }

    fn a_tree_with_contents(root: &Path, files: &[(&str, &str)]) {
        for (rel, body) in files {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().expect("a parent")).expect("directories are made");
            std::fs::write(&p, body).expect("the file is written");
        }
    }

    async fn remove_prefix(c: &aws_sdk_s3::Client, prefix: &str) {
        let keys = bucket_keys(c, prefix).await;
        for chunk in keys.chunks(1000) {
            let ids: Vec<_> = chunk
                .iter()
                .filter_map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(format!("{prefix}{k}"))
                        .build()
                        .ok()
                })
                .collect();
            if ids.is_empty() {
                continue;
            }
            let del = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(ids))
                .build()
                .expect("a delete request");
            let _ = c
                .delete_objects()
                .bucket(regular_bucket())
                .delete(del)
                .send()
                .await;
        }
    }

    #[derive(Debug)]
    struct RealRun {
        moved: u64,
        transfers: u64,
        transferred: u64,
        deleted: u64,
        failures: u64,
        skipped: u64,
    }

    fn counters<S: KeyStream, D: KeyStream>(t: &SyncTransfer<S, D>) -> RealRun {
        let state = t.inner.state.lock();
        RealRun {
            moved: state.bytes_moved,
            transfers: state.decided.transfers,
            transferred: state.transferred,
            deleted: state.deleted,
            failures: state.transfer_failures.total() + state.failures.total(),
            skipped: state.decided.skipped.total(),
        }
    }

    async fn real_upload(
        local: &Path,
        prefix: &str,
        c: aws_sdk_s3::Client,
        delete_mode: DeleteMode,
    ) -> RealRun {
        let config = crate::Config::builder().client(c.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(c.clone())
                    .bucket(regular_bucket())
                    .prefix(prefix)
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnUpload::new(
                ctx.handle.clone(),
                regular_bucket(),
                Some(prefix),
            )),
            Deleter::Bucket(DeleteFromBucket::new(c, regular_bucket(), Some(prefix))),
            RunSettings {
                max_children: 8,
                delete_mode,
                failure_policy: FailurePolicy::Continue,
            },
        );
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(300), rx)
            .await
            .expect("the upload did not finish inside five minutes")
            .expect("the terminal signal was dropped");
        counters(&transfer)
    }

    async fn real_download(
        local: &Path,
        prefix: &str,
        c: aws_sdk_s3::Client,
        delete_mode: DeleteMode,
    ) -> RealRun {
        let config = crate::Config::builder().client(c.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .downloading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(c)
                    .bucket(regular_bucket())
                    .prefix(prefix)
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().downloading(),
            Arc::new(SpawnDownload::new(
                ctx.handle.clone(),
                regular_bucket(),
                Some(prefix),
                local,
            )),
            Deleter::LocalTree(DeleteFromLocalTree::new(local)),
            RunSettings {
                max_children: 8,
                delete_mode,
                failure_policy: FailurePolicy::Continue,
            },
        );
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(300), rx)
            .await
            .expect("the download did not finish inside five minutes")
            .expect("the terminal signal was dropped");
        counters(&transfer)
    }

    const REAL_TREE: &[(&str, &str)] = &[
        ("a.txt", "alpha"),
        ("b.txt", "bravo"),
        ("nested/c.txt", "charlie"),
        ("nested/deep/d.txt", "delta"),
        ("empty.txt", ""),
    ];

    fn real_tree_keys() -> Vec<String> {
        let mut v: Vec<String> = REAL_TREE.iter().map(|(k, _)| k.to_string()).collect();
        v.sort();
        v
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_upload_puts_every_key_under_its_prefix() {
        let c = real_client().await;
        let prefix = a_run_prefix("up-prefix");
        let dir = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(dir.path(), REAL_TREE);

        let run = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload: {run:?}");

        assert_eq!(
            bucket_keys(&c, &prefix).await,
            real_tree_keys(),
            "the bucket does not hold the tree under {prefix}"
        );
        assert_eq!(run.failures, 0, "a key failed");
        let stray = bucket_keys(&c, "a.txt").await;
        assert!(
            stray.is_empty(),
            "a key landed at the bucket root: {stray:?}"
        );

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_download_strips_the_prefix_from_every_key() {
        let c = real_client().await;
        let prefix = a_run_prefix("down-prefix");
        let src = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(src.path(), REAL_TREE);
        real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;

        let dst = tempfile::tempdir().expect("a temp dir");
        let run = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download: {run:?}");

        assert_eq!(
            tree_files(dst.path()),
            real_tree_keys(),
            "the local tree does not mirror the keys under {prefix}"
        );
        assert_eq!(run.failures, 0, "a key failed");
        assert!(
            !dst.path().join("sync-e2e").exists(),
            "the prefix reached the local tree as a directory"
        );
        for (rel, body) in REAL_TREE {
            let got = std::fs::read_to_string(dst.path().join(rel)).expect("the file reads");
            assert_eq!(&got, body, "{rel} arrived with the wrong contents");
        }

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_second_run_moves_nothing() {
        let c = real_client().await;
        let prefix = a_run_prefix("idempotent");
        let dir = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(dir.path(), REAL_TREE);

        let up1 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload 1: {up1:?}");
        let up2 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload 2: {up2:?}");
        assert_eq!(
            up2.transfers,
            0,
            "the second upload re-sent {} of {} keys",
            up2.transfers,
            REAL_TREE.len()
        );

        let dst = tempfile::tempdir().expect("a temp dir");
        let down1 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 1: {down1:?}");
        let down2 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 2: {down2:?}");
        assert_eq!(
            down2.transfers,
            0,
            "the second download re-fetched {} of {} keys",
            down2.transfers,
            REAL_TREE.len()
        );

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_moves_an_object_past_the_multipart_threshold() {
        let c = real_client().await;
        let prefix = a_run_prefix("multipart");
        let src = tempfile::tempdir().expect("a temp dir");
        let body: Vec<u8> = (0..20 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.path().join("big.dat"), &body).expect("the file is written");

        let up = real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload: {up:?}");
        assert_eq!(up.failures, 0, "the multipart upload failed");
        assert_eq!(
            up.moved,
            body.len() as u64,
            "the run moved {} bytes of {}",
            up.moved,
            body.len()
        );

        let dst = tempfile::tempdir().expect("a temp dir");
        let down = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download: {down:?}");
        assert_eq!(down.failures, 0, "the multipart download failed");
        let back = std::fs::read(dst.path().join("big.dat")).expect("the copy reads");
        assert_eq!(
            back.len(),
            body.len(),
            "the copy is {} bytes against {}",
            back.len(),
            body.len()
        );
        assert!(back == body, "the bytes came back in a different order");

        let again = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 2: {again:?}");
        assert_eq!(
            again.transfers, 0,
            "the second download fetched the object again"
        );

        remove_prefix(&c, &prefix).await;
    }
}
