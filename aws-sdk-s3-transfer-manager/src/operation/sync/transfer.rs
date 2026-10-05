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
use crate::operation::sync::walk::{Pairing, Walk};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};

// Pairings taken per work item. The merge pulls from whichever side answers the next key, so a
// batch spans an unknown number of entries on either side. The bound counts pairings for that
// reason, and no listing page can size it. The bound caps how long a single item occupies an
// executor slot, and one slot is a share of what the whole client has.
const MERGE_BATCH: usize = 64;

// How many failures a run keeps. The walk reports most failures per entry and carries on, so a tree
// where every entry fails would otherwise put the number of entries into peak memory. Past the cap
// the run counts a failure and holds none of it. A result reports the count, and the reporting
// layer names each key.
//
// The sample holds the first failures seen, not a representative spread. Where failures cluster in
// one early subtree, the whole sample comes from there. A reader taking the sample as typical of
// the run would be wrong.
const FAILURES_KEPT: usize = 64;

// Turning a decision into a child. The other thing that differs by direction, and unlike the
// comparison it cannot be a static: building a child needs the client, the bucket, and the roots.
pub(crate) trait SpawnChild<S>: Send + Sync {
    // Enqueue a child for this key and hand back a way to ask after it. The key is the relative
    // one both sides agree on; turning it into an address is this implementation's business.
    fn spawn(&self, key: &str, source: &S, parent: u64)
        -> Result<ChildHandle, crate::error::Error>;
}

// How many keys go in one delete request. `DeleteObjects` takes no more, and batching at all is what
// makes a large delete affordable: a thousand keys sent singly cost a thousand round trips and a
// thousand dispatch charges, together one of each.
const DELETE_BATCH: usize = 1000;

// How many times a batch asks again about keys S3 refused for load.
//
// Two loops wrap one delete because the unit of failure and the unit of request differ here, which is
// true of no other request this crate makes. The inner loop re-issues a request that failed whole and
// cannot narrow what it sends; this one re-sends a smaller set of keys, which is the only way to stop a
// retry from naming keys the service already removed. Neither replaces the other: a request can fail
// with no key refused, and a key can be refused by a request that succeeded.
//
// Their bounds therefore multiply, and a batch can cost nine requests. That is accepted rather than
// trimmed: the two bounds cover different failure modes, and the wait between asks now dominates the
// cost, so a smaller bound would buy nothing and give up coverage.
const DELETE_REFUSAL_ATTEMPTS: u32 = 3;

// What became of one key: removed, or refused with a reason naming it.
type KeyOutcome = Result<String, String>;

// Whether a run may remove keys from the destination. Named rather than passed as a bare bool, because
// a caller reading `true` at a call site cannot see what it turns on.
// Off by default: the one operation here that destroys data a caller never handed over is the one
// nobody gets without asking for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DeleteMode {
    On,
    #[default]
    Off,
}

// What a run does when something fails. One answer for the whole run, so a caller reasons about
// failure once rather than per kind of thing that can go wrong.
//
// Continue is the default because a sync converges: it does part of the work and the next run picks
// up the rest, so stopping at the first failed key throws away progress that the next run has to
// repeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FailurePolicy {
    #[default]
    Continue,
    Abort,
}

// What a caller chose, as against what the direction decided. Grouped because these answer one
// question — how this run should behave — where the comparison, the spawner and the deleter answer
// which direction it goes.
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

// How a run turned out. Three answers rather than two, because a run that met a name nothing could
// have transferred is not clean and is not broken either, and a caller deciding whether to look into
// it needs those apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunOutcome {
    Failed,
    Warned,
    Clean,
}

// Where keys go when they leave the destination. A third thing that differs by direction, and it
// differs more deeply than the other two: a bucket takes a thousand keys in one request, a local tree
// takes them one file at a time. So the caller hands over a whole batch and the destination decides
// what one request means.
//
// Three destinations exist: a bucket, a local tree, and a recorder a test watches. A value rather
// than a trait object because nothing here varies by type: a key is a key on either side, which is
// what separates this from `SpawnChild`, whose implementations each accept one walk entry type.
// Holding it as a value also keeps the future unboxed and lets `delete` take anything a key can be
// read from.
pub(crate) enum Deleter {
    Bucket(DeleteFromBucket),
    LocalTree(DeleteFromLocalTree),
    // Named rather than inlined, unlike `ChildInner::Controlled`, because the assertions are built on
    // this double's accessors. Inlining its fields would cost them. `ChildInner` can inline because
    // `ChildHandle` hides it, where this enum is named by whoever builds a transfer and has nowhere
    // to hide a payload.
    #[cfg(test)]
    Recording(Arc<tests::RecordDeletes>),
}

impl Deleter {
    // How many keys to collect before sending. A destination with no batch request answers one.
    pub(crate) fn batch_size(&self) -> usize {
        match self {
            Deleter::Bucket(d) => d.batch_size(),
            Deleter::LocalTree(d) => d.batch_size(),
            #[cfg(test)]
            Deleter::Recording(d) => d.batch_size(),
        }
    }

    // Remove these keys, reporting what happened to each. The count of outcomes equals the count of
    // keys: "the batch failed" tells a caller nothing about which keys survived.
    //
    // The keys are collected here rather than by each destination, because every one of them needs a
    // length up front and a key at a known position to name an outcome against.
    pub(crate) async fn delete<K, I>(&self, keys: I) -> Vec<KeyOutcome>
    where
        K: Into<String>,
        I: IntoIterator<Item = K>,
    {
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        match self {
            Deleter::Bucket(d) => d.delete(keys).await,
            Deleter::LocalTree(d) => d.delete(keys).await,
            #[cfg(test)]
            Deleter::Recording(d) => d.delete(keys).await,
        }
    }
}

// Removes files from a local tree. One file per call, because there is no request that takes a batch
// of them and pretending otherwise would hide how much work a batch is.
//
// Directories are left where they are, even when the last file under one goes: a directory sync did
// not create is not sync's to remove, and one it did create may be where a caller puts something else.
// Pruning them is a separate thing to ask for.
#[derive(Debug)]
pub(crate) struct DeleteFromLocalTree {
    root: std::path::PathBuf,
}

impl DeleteFromLocalTree {
    pub(crate) fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    // The file a relative key names under this run's root, refusing one that names anything outside
    // it. Deleting is where that matters most: a key resolving above the root would remove a file
    // nobody put in the destination.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        crate::io::key::local_key_path(&self.root, key, None, None)
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
                    outcomes.push(Err(format!("{key}: {err}")));
                    continue;
                }
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => outcomes.push(Ok(key)),
                // A file already gone is the state the delete wanted, and a run that reports it as a
                // failure would make a second run over the same tree look worse than the first.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => outcomes.push(Ok(key)),
                Err(e) => outcomes.push(Err(format!("{key}: {e}"))),
            }
        }
        outcomes
    }
}

// Deletes keys from a bucket. One request per batch, retried on throttling and transient transport
// failure like any other request sync makes on its own behalf — a child gets that from the SDK client
// it is built with, and a request issued here has no such inheritance.
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

    async fn delete(&self, keys: Vec<String>) -> Vec<KeyOutcome> {
        // The object each relative key names, in the same order, so a response entry can be
        // attributed to the key it answers rather than to whatever sits at the same offset.
        let addressed: Vec<String> = keys.iter().map(|k| self.object_key(k)).collect();
        let mut at_object = std::collections::HashMap::with_capacity(addressed.len());
        for (at, object) in addressed.iter().enumerate() {
            at_object.insert(object.as_str(), at);
        }

        let unnamed = |at: usize, why: &str| Err(format!("{}: {why}", keys[at]));
        let mut settled: Vec<Option<KeyOutcome>> = vec![None; keys.len()];
        // Which keys still have no answer. S3 reports partial throttling against individual keys
        // rather than by failing the request, so a key refused that way is asked again with
        // whichever others were refused the same way, and nothing already removed is named again.
        let mut outstanding: Vec<usize> = (0..keys.len()).collect();

        for attempt in 0..DELETE_REFUSAL_ATTEMPTS {
            if outstanding.is_empty() {
                break;
            }
            let last_attempt = attempt + 1 == DELETE_REFUSAL_ATTEMPTS;
            // A refusal for load is answered on the schedule the request-level retry would use for
            // the same code, so how long a throttle waits does not depend on whether it arrived
            // against the request or against one key. Asking again immediately is how shedding load
            // turns into more of it.
            if attempt > 0 {
                let delay = crate::retry::Backoff::throttle().delay(attempt - 1, fastrand::f64());
                tokio::time::sleep(delay).await;
            }

            let mut identifiers = Vec::with_capacity(outstanding.len());
            for &at in &outstanding {
                match aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(addressed[at].clone())
                    .build()
                {
                    Ok(id) => identifiers.push(id),
                    // A key this layer produced that the API will not name is this layer's fault,
                    // so it is reported against that key rather than failing the batch around it.
                    Err(err) => settled[at] = Some(unnamed(at, &err.to_string())),
                }
            }
            if identifiers.is_empty() {
                break;
            }
            let delete = match aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(identifiers))
                // Set rather than left to the server default, because reading the outcomes depends
                // on it: a quiet response names only refusals, so every removed key would arrive
                // unmentioned and a batch that fully succeeded would be reported as fully failed.
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
                        // `Error::from` rather than wrapping by kind: only the conversion carries
                        // the service metadata, which is what the classifier reads to tell a
                        // throttle from a refusal.
                        .map_err(|err| {
                            crate::retry::GuardError::Inner(crate::error::Error::from(err))
                        })
                }
            })
            .await;

            match sent {
                // The response's two lists are what tell a caller which keys survived, so they are
                // read apart rather than collapsed into one verdict.
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
                        let why = error.message().unwrap_or("no reason given");
                        // The crate's own set, rather than a wider one for this path: a refusal
                        // declined here leaves the object in place and the next run decides it
                        // again from a whole view, so reading the set narrowly costs one deferred
                        // delete. Treating a code as retryable here and terminal elsewhere would
                        // cost a reader one answer to what the code means.
                        if !last_attempt && crate::retry::is_throttle_code(error.code()) {
                            refused_again.push(at);
                        } else {
                            settled[at] = Some(unnamed(at, why));
                        }
                    }
                    outstanding = refused_again;
                }
                // The request itself failed and has already exhausted its own attempts, so every
                // key still waiting on it is answered with that failure.
                Err(err) => {
                    let why = err.to_string();
                    for &at in &outstanding {
                        settled[at].get_or_insert_with(|| unnamed(at, &why));
                    }
                    break;
                }
            }
        }

        // A key the response never mentioned is named rather than left out, because the count of
        // outcomes has to match the count of keys and an unexplained key is worse than a failed one.
        settled
            .into_iter()
            .enumerate()
            .map(|(at, outcome)| {
                outcome.unwrap_or_else(|| unnamed(at, "the response did not mention this key"))
            })
            .collect()
    }
}

// What sync asks of any child, which is all it asks: whether the child finished, and what it moved.
// An upload handle and a download handle are unrelated types returning different outputs, and both
// carry the part sync needs in the same field.
pub(crate) struct ChildHandle {
    id: crate::transfer::TransferId,
    inner: ChildInner,
}

enum ChildInner {
    Upload(crate::operation::upload::UploadHandle),
    // Managed, because that handle is what writes to a temporary name and renames once the bytes are
    // all there — so a download that fails leaves whatever was already at the destination.
    Download(crate::operation::download::ManagedDownloadHandle),
    // A child a test controls, for tests about the loop rather than about the transfer. Without it a
    // test would have to run a real child through the scheduler to assert anything about spawning or
    // reaping, and could not hold one open to watch the parent park.
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

    // Whether the child has reached an end, of any kind. A reap has to wait for this, because
    // joining one that has not is what would hold a work item open.
    pub(crate) fn is_finished(&self) -> bool {
        match &self.inner {
            ChildInner::Upload(handle) => handle.status().is_terminal(),
            ChildInner::Download(handle) => handle.status().is_terminal(),
            #[cfg(test)]
            ChildInner::Controlled { ended, .. } => ended.load(std::sync::atomic::Ordering::SeqCst),
        }
    }

    // What the child moved, and whether it got there. Consuming, because joining is the only way to
    // learn either.
    pub(crate) async fn join(self) -> Result<u64, crate::error::Error> {
        match self.inner {
            ChildInner::Upload(handle) => handle.join().await.map(|out| out.metrics.network_tx),
            // Joining is also what renames the file into place and stamps it, so this arm is where a
            // download becomes visible at its final name.
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

// Sends a local file to a bucket. `SpawnDownload` below is the other half, and the trait above
// holds for both. Two things differ by direction: which client call carries the bytes, and what the
// destination needs in place before they land.
pub(crate) struct SpawnUpload {
    handle: Arc<crate::client::Handle>,
    bucket: String,
    // The place in the bucket the run is against, already ending in its delimiter. Both sides
    // compare on keys relative to their own root. The listing strips the prefix off, so naming an
    // object means putting the prefix back. Without it a prefixed run writes to the bucket root.
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
        // Hand the builder the metadata the walk already read. Without it the builder stats the path
        // again — a blocking syscall inside a poll, once per key, for a size the comparison has
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

// Builds a download child. Named for what it spawns rather than for where it writes, because an
// upload and a copy both write to a bucket and a name taken from the destination could not tell them
// apart. The deleter beside it is named the other way round for the opposite reason: deleting does not
// vary by operation, only by where the key lives, so an upload-direction run and a copy-direction run
// delete from a bucket with the same code.
pub(crate) struct SpawnDownload {
    handle: Arc<crate::client::Handle>,
    bucket: String,
    // The place in the bucket the run is against, as in `SpawnUpload`: keys arrive relative to the
    // run's root and naming an object means putting the root back.
    root: String,
    // Where the keys land. The destination address this spawner holds, the way `SpawnUpload` holds a
    // bucket and prefix.
    local_root: std::path::PathBuf,
    // Directories already made, so the ancestor chain is walked once per directory rather than once
    // per key. `create_dir_all` stats the whole chain, which for many small files in few directories
    // is the larger per-child cost.
    //
    // TODO(sync): this runs inline in `poll_work`, which runs on the dispatch loop, so it blocks every
    // transfer on the client while it stats. The cost scales with distinct directories rather than
    // with keys, so the case to measure is one key per directory at depth — a date-partitioned layout
    // — where this cache never hits. `download_objects` has the same call with the same cache and the
    // same question outstanding.
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

    // The file a relative key names under this run's root, refusing one that names anything outside it.
    //
    // A key is arbitrary text and `..` is ordinary text in one, so a bucket can hold a key that
    // resolves above the destination. Writing there would put a file where nobody asked for one, and
    // the directory download already answers this, so its answer is reused rather than restated.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        crate::io::key::local_key_path(&self.local_root, key, None, None)
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
            // A destination that does not exist yet is not a failure: every key is missing there,
            // which is a complete answer rather than an absent one, and the directories appear as the
            // keys that need them do.
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

        // A name nothing else will take, so two runs over one tree cannot collide on the half-written
        // file. Built synchronously because this is a poll rather than an async context.
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

// The merge, moved out of `State` for as long as one work item holds it. `next` needs
// `&mut`, and holding the state lock across it would block every poll on this transfer.
pub(crate) enum SyncWork<S: KeyStream, D: KeyStream> {
    AdvanceMerge { walk: Option<Box<Walk<S, D>>> },
    // Children that have reached an end. Joining one waits, so collecting them is a work item like
    // any other, and they leave `State::children` as the item takes them.
    ReapChildren { children: Vec<ChildHandle> },
    // Keys to remove from the destination. They leave `State::pending_deletes` when this is built,
    // so `deletes_in_flight` has to stand in for them.
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

// A key the comparison qualified for a transfer, with the pairing behind the decision. Spawning
// needs both: the key names the destination, and a child reads its bytes from the source entry.
type Qualified<S, D> = (Pairing<S, D>, Decision);

// What a run decided, by kind. Every pairing yields exactly one decision, so the three counts sum
// to `paired`. Grouped so a count added later reaches every site at once.
#[derive(Debug, Default)]
struct Decided {
    transfers: u64,
    deletes: u64,
    skips: u64,
    // Keys the comparison marked for removal, counted whether or not the run was allowed to act on them.
    // Independent of the mode on purpose: it gives both modes the same denominator, so a caller can
    // reconcile what was removed and what was refused against what was intended, and can ask how much a
    // delete would take away before allowing one.
    deletable: u64,
}

impl std::ops::AddAssign for Decided {
    fn add_assign(&mut self, batch: Self) {
        self.transfers += batch.transfers;
        self.deletes += batch.deletes;
        self.skips += batch.skips;
        self.deletable += batch.deletable;
    }
}

struct State<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, so holding the
    // state lock across it would block every poll on this transfer. The merge moves into the item
    // for as long as the item runs.
    walk: Option<Walk<S, D>>,
    merge_in_flight: bool,
    paired: u64,
    decided: Decided,
    // Transfers waiting for a child slot. The comparison decides a key as soon as the merge pairs
    // it. The decision then waits here for room, so the number of children already running never
    // holds the merge back.
    waiting: VecDeque<Qualified<S::Source, D::Source>>,
    // Children enqueued and not yet reaped.
    children: std::collections::HashMap<crate::transfer::TransferId, ChildHandle>,
    // Children handed to a reap and not yet joined. They have left `children`, so without this a
    // poll would see no children and call the run over while their outcomes were still coming back.
    reap_in_flight: usize,
    // Keys decided for deletion and not yet sent. A key joins here when it is decided, and the batch
    // leaves when it is full or when the run ends cleanly.
    pending_deletes: Vec<String>,
    // A batch handed to a work item. Like `reap_in_flight`, it stands in for keys that have left the
    // buffer and whose outcomes are still coming back.
    deletes_in_flight: usize,
    deleted: u64,
    delete_failures: u64,
    bytes_moved: u64,
    // Transfers that arrived, one per child the reap joins. `decided.transfers` counts the other
    // end, when the comparison picks a key out. A caller reconciling the run against the
    // destination reads this one: the decision count answers a different question, and deriving
    // arrivals from it means subtracting every population that intervened and knowing those do
    // not overlap.
    transferred: u64,
    // A child that could not be enqueued, or that ended badly. Counted rather than kept: a
    // `StreamError` describes a walk and says nothing about a transfer, and naming the key that
    // failed is the reporting layer's job.
    transfer_failures: u64,
    // Set when a comparison answers something this layer cannot act on. Kept here rather than
    // written into the walk, whose own flag means a stream went unread and is set nowhere but
    // inside its `next`.
    plan_incomplete: bool,
    // What the walk knew when a work item last handed it back. Read from the walk rather than
    // asked of it on demand, because the walk is away while an item holds it and an absent walk
    // has no answer to give — a plan either has holes or it does not, and that cannot depend on
    // whether a batch happens to be in flight.
    //
    // Accumulated rather than assigned, so a hole stays reported whatever the walk says later.
    // Assigning would be correct only while the walk's own flag is never cleared, which is not this
    // module's invariant to rely on.
    walk_plan_incomplete: bool,
    // Capped by `FAILURES_KEPT`; anything past that is counted in `failures_dropped`.
    failures: Vec<StreamError>,
    failures_dropped: u64,
    // Names nothing could have transferred, which are reported and never acted on. Kept apart from
    // failures because folding them together would report a run as broken over a name it was never
    // going to send. Capped like the failures beside them, with the count exact either way.
    warnings: Vec<StreamError>,
    warnings_dropped: u64,
    // Why individual keys were not removed, each naming its key. A count alone cannot answer which key
    // survived, which is the question a per-key outcome exists to answer. Capped at `FAILURES_KEPT`,
    // and needing no dropped counter of its own: `delete_failures` already holds the exact total, so
    // what this sample leaves out is the difference between the two.
    refusals: Vec<String>,
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
    // Which comparison to ask. One of the two things that differ by direction, passed in rather
    // than chosen here: an upload and a download disagree about which side being newer wins.
    comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
    // The other one. Unlike the comparison this holds state, because building a child needs the
    // client and the bucket.
    spawner: Arc<dyn SpawnChild<S::Source>>,
    // How keys leave the destination. A third direction-specific thing, and the one that differs most
    // between a bucket and a local tree.
    deleter: Deleter,
    // What to do when something fails. Read at every site a failure can arrive, so one answer covers
    // the run.
    failure_policy: FailurePolicy,
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    max_children: usize,
    // Whether a key the source does not have may be removed. Deleting is the one thing here that
    // destroys data the caller never handed over, so nobody gets it without asking.
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
                    delete_failures: 0,
                    bytes_moved: 0,
                    transferred: 0,
                    transfer_failures: 0,
                    plan_incomplete: false,
                    walk_plan_incomplete: false,
                    failures: Vec::new(),
                    failures_dropped: 0,
                    refusals: Vec::new(),
                    warnings: Vec::new(),
                    warnings_dropped: 0,
                }),
            }),
        }
    }

    // Whether the comparison reached every key. Two things leave holes, and they are independent.
    // First, a stream the walk could not finish reading. Second, a comparison the run could not act
    // on. Either one alone means the plan has holes.
    //
    // The run reads both flags from its own state and never asks the walk. An absent walk answers
    // nothing, so asking the question would let the same run report its plan as complete at one
    // moment and incomplete at another.
    pub(crate) fn is_plan_complete(&self) -> bool {
        let state = self.inner.state.lock();
        !state.plan_incomplete && !state.walk_plan_incomplete
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let active = self.inner.ctx.is_active();
        let mut state = self.inner.state.lock();

        if active {
            if let Some(work) = self.dispatch_merge(&mut state) {
                return work;
            }
            if self.spawn_one(&mut state) {
                return PollWork::Spawned;
            }
        }

        if active {
            if let Some(work) = self.dispatch_deletes(&mut state) {
                return work;
            }
        }

        // Reaping runs whether or not the transfer is still active: a child that has ended has an
        // outcome owed to the run, and cancelling does not make it go away.
        if let Some(work) = self.dispatch_reap(&mut state) {
            return work;
        }

        if let Some(done) = self.check_terminal(&mut state) {
            return done;
        }

        // Mark the transfer as waiting before returning. `try_wake` only signals a transfer already
        // marked, so without the mark a finishing work item signals nothing. The transfer would
        // then sit outside the ready set with nothing to put it back.
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    // Whether the run is over, and the signal that tells a waiter so. Answering `Done`
    // while `merge_in_flight` is set would report a finished run with a batch still out, and
    // the keys in that batch would go unreported.
    // End the run if the policy says a failure should. The teardown is the one every ended run uses:
    // `set_failed` takes the run out of the active state, and the next pass through `check_terminal`
    // answers the waiter and lets the pending batch go. A caller's cancellation travels the same path
    // and differs only in which status it lands on, which is what a result needs to tell them apart.
    //
    // Takes the failure itself rather than a description of it, so a service refusal arrives with its
    // code, message and request ids intact. A category chosen here would be less than what the site
    // already had.
    //
    // The error that lands is *an* error rather than *the* error: the status is first-write-wins, so
    // under concurrency whichever site got there first is the one a waiter sees. Every failure is
    // still in the run's own records, which is where a caller reads the set.
    //
    // Must not grow a signal or a wake. Two of the four callers hold the state lock, and a wake runs
    // `generate_work` into `poll_work`, which takes that same non-reentrant lock. `set_failed` only
    // compares and swaps a status and writes an error slot, which is why this is safe today; the
    // signalling variant beside it would not be.
    fn stop_if_aborting(&self, why: impl Into<crate::error::Error>) {
        if self.inner.failure_policy == FailurePolicy::Abort {
            self.inner.ctx.set_failed(why);
        }
    }

    // How the run turned out. A failure anywhere outranks a warning, and a warning outranks nothing,
    // because the three answer different questions: whether to look into it, whether to glance, and
    // whether to move on.
    pub(crate) fn outcome(&self) -> RunOutcome {
        let state = self.inner.state.lock();
        if !state.failures.is_empty()
            || state.failures_dropped > 0
            || state.transfer_failures > 0
            || state.delete_failures > 0
        {
            return RunOutcome::Failed;
        }
        if !state.warnings.is_empty() || state.warnings_dropped > 0 {
            return RunOutcome::Warned;
        }
        RunOutcome::Clean
    }

    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if !self.inner.ctx.is_active() {
            // Everything dispatched is still owed an answer, however the run ended.
            if state.merge_in_flight
                || state.reap_in_flight > 0
                || state.deletes_in_flight > 0
                || !state.children.is_empty()
            {
                return None;
            }
            // Whatever has not been sent is let go. A key reached this buffer by being absent from
            // the source, and absence is read from the merge having passed its position — so a run
            // that stopped early judged these keys against a stream with a hole in it. The next run
            // decides them again from a whole view, where sending them now could remove a file that
            // exists.
            state.pending_deletes.clear();
            // Cancelled or failed: the run already recorded the outcome, so the only thing left is
            // to signal the caller.
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        if !state.merge_in_flight
            && state.waiting.is_empty()
            && state.children.is_empty()
            && state.reap_in_flight == 0
            && state.pending_deletes.is_empty()
            && state.deletes_in_flight == 0
            && state.walk.as_ref().is_some_and(Walk::is_done)
        {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
    }

    // Turn one waiting decision into a child, if a slot is free. `true` means the run enqueued a
    // child. The scheduler charges for the spawn without taking a dispatch ticket, because the
    // child takes its own ticket once the scheduler polls it.
    fn spawn_one(&self, state: &mut State<S, D>) -> bool {
        // Only live children count. One sitting in a reap has already ended and holds no network or
        // disk concurrency. Counting it would idle a share of the budget for the length of every
        // reap.
        if state.children.len() >= self.inner.max_children {
            return false;
        }

        // Keep going past anything that cannot become a child, so one key nothing can act on does
        // not strand the keys behind it. Answering the poll with no child enqueued leaves the run
        // waiting, with keys still buffered and no work item left to signal.
        while let Some((pairing, _)) = state.waiting.pop_front() {
            let Some(entry) = pairing.source().entry() else {
                // A transfer is only decided for a source that is present, so reaching here means a
                // comparison answered something it had no grounds for.
                state.transfer_failures += 1;
                self.stop_if_aborting(crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "a transfer was decided for a key with no source entry",
                ));
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
                    state.transfer_failures += 1;
                    self.stop_if_aborting(err);
                    continue;
                }
            }
        }
        false
    }

    // Send a batch of deletes when it is full, or when nothing else will add to it. Holding a part
    // batch until the merge is done is what lets a thousand keys cost one request.
    fn dispatch_deletes(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let size = self.inner.deleter.batch_size();
        let merge_done = state.walk.as_ref().is_some_and(Walk::is_done);
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

    // Collect children that have ended into a work item. They leave `children` here, so
    // `reap_in_flight` stands in for them until their outcomes are back.
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

    // Hand the merge to a work item. `None` leaves the poll to choose between `Done` and
    // `Pending`.
    fn dispatch_merge(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if state.merge_in_flight {
            return None;
        }

        let walk = match state.walk.take() {
            Some(walk) if !walk.is_done() => walk,
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
        let sent = keys.len();
        let outcomes = self.inner.deleter.delete(keys).await;

        let mut gone = 0u64;
        let mut refused = Vec::new();
        for outcome in &outcomes {
            match outcome {
                Ok(_) => gone += 1,
                Err(why) => refused.push(why.clone()),
            }
        }

        // Summarised rather than carried: a key S3 refuses is reported inside a successful response
        // as a code and a message rather than as an error, so there is nothing here to hand over. The
        // reason naming its key is in the run's own records either way.
        if let Some(why) = refused.first() {
            self.stop_if_aborting(crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                why.clone(),
            ));
        }

        let mut state = self.inner.state.lock();
        state.deletes_in_flight -= sent;
        state.deleted += gone;
        state.delete_failures += refused.len() as u64;
        for why in refused {
            if state.refusals.len() < FAILURES_KEPT {
                state.refusals.push(why);
            }
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
        let mut failed = 0u64;
        let mut why = None;
        for child in children {
            match child.join().await {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(err) => {
                    failed += 1;
                    if why.is_none() {
                        why = Some(err);
                    }
                }
            }
        }
        if let Some(why) = why {
            self.stop_if_aborting(why);
        }

        let mut state = self.inner.state.lock();
        state.reap_in_flight -= count;
        state.bytes_moved += moved;
        state.transferred += arrived;
        state.transfer_failures += failed;
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_advance_merge(&self, mut walk: Walk<S, D>) -> WorkOutcome {
        if !self.inner.ctx.is_active() {
            let mut state = self.inner.state.lock();
            state.walk = Some(walk);
            state.merge_in_flight = false;
            return WorkOutcome::Cancelled;
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
                        // A delete decision says the destination holds a key the source does not.
                        // Acting on that is this layer's business rather than the comparison's, so an
                        // unasked-for delete becomes a skip — counted and reported like any other key,
                        // which keeps it out of the buffer rather than merely out of a request.
                        Verdict::Decided(Decision::Delete(_)) => {
                            decided.deletable += 1;
                            match self.inner.delete_mode {
                                DeleteMode::On => {
                                    decided.deletes += 1;
                                    pending_deletes.push(pairing.key().to_string());
                                }
                                DeleteMode::Off => decided.skips += 1,
                            }
                        }
                        // A skip needs nothing done to it, so it never waits for a slot.
                        Verdict::Decided(Decision::Skip(_)) => decided.skips += 1,
                        // Nothing shipped here defers, so one arriving is a defect in whatever
                        // comparison produced it. The key is skipped and the plan is marked short
                        // of the keys it should have covered. Sending the key or dropping it
                        // without a word would turn the defect into either wasted bandwidth or a
                        // file nobody was told about. Which key deferred is not recorded: what
                        // survives here is a count and a run-level flag.
                        Verdict::Deferred(_) => {
                            decided.skips += 1;
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
        // A warning and a failure are recorded in different places and only one of them can stop the
        // run. Both are kept, because a key reported nowhere cannot be told from a key the run never
        // reached.
        let mut failed = None;
        for entry in failures {
            if entry.is_warning() {
                if state.warnings.len() < FAILURES_KEPT {
                    state.warnings.push(entry);
                } else {
                    state.warnings_dropped += 1;
                }
                continue;
            }
            if failed.is_none() {
                failed = Some(entry.to_string());
            }
            if state.failures.len() < FAILURES_KEPT {
                state.failures.push(entry);
            } else {
                state.failures_dropped += 1;
            }
        }
        // The failure itself goes to the records and a description goes to the waiter, which is the
        // right way round for one value with two readers. The records are where a caller reads
        // failures and must hold every one of them; the attached error is one of possibly many,
        // settled by whichever site got there first. Fidelity belongs to the complete surface, not to
        // the representative one.
        if let Some(why) = failed {
            self.stop_if_aborting(crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                why,
            ));
        }

        // Draining the last batch makes this work item the one that ends the run, so it
        // signals here rather than leaving a waiter for one more poll.
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);

        // Without this the run parks: the poll that dispatched this item answered `Pending`
        // and left the ready set, and nothing else puts it back.
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

    // Called by the scheduler when the run reaches a terminal status, which is the only notice sync
    // gets: a terminal transfer is not polled again, so anything owed at that moment has to be
    // settled here rather than on a later pass.
    //
    // What is owed is the children. Each holds the temporary file it opened, and dropping one before
    // it finished clears that file as it goes, so keeping the handles leaves half-written names in the
    // destination for as long as anything holds the run. Nothing is lost by letting go: a
    // cancellation reaches the children before it reaches here, so their answer is already settled.
    fn on_terminal(&self) {
        self.inner.state.lock().children.clear();
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

    // A bucket that answers one page and nothing more. Every object carries a last-modified,
    // without which the walk reports the listing malformed rather than producing the key.
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
        // A spawned child puts an object, so the bucket has to accept writes as well as answer a
        // listing.
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put])
    }

    // The same bucket, but keeping the key each write named. Asserting on a helper that builds a key
    // proves only that the helper is right; what a caller needs is the key the request carried.
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

    fn a_local_tree(root: &Path, keys: &[&str]) {
        for key in keys {
            let path = root.join(key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("a parent directory");
            }
            std::fs::write(&path, b"").expect("a file");
        }
    }

    // An upload-direction transfer over a real local tree and a mocked bucket, registered with
    // the scheduler so a poll answering `Pending` is parked the way production parks it.
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

    // Drive the loop the way the scheduler does, under a deadline. A lost wake shows up as a
    // hang rather than a failure, so without the deadline this test would never report.
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

    // Records the batches it was asked to send, so a test can see how many requests a run would
    // have cost and which keys were in each.
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
                        Err(format!("{key}: refused"))
                    } else {
                        Ok(key)
                    }
                })
                .collect()
        }
    }

    // Hands back children that have already ended, so a test can assert on spawning and reaping
    // without a scheduler to run real ones. Counts what it was asked to spawn.
    struct SpawnEnded {
        moved: u64,
        fails: bool,
        // Whether building the child fails, as against the child failing once it runs. The two reach
        // the parent at different moments and only one of them ever holds a slot.
        refuses: bool,
        asked: std::sync::atomic::AtomicUsize,
        // Shared by every child it hands out, so a test can hold them all open and then release
        // them together.
        ended: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SpawnEnded {
        fn new(moved: u64, fails: bool) -> Self {
            Self {
                moved,
                fails,
                refuses: false,
                asked: std::sync::atomic::AtomicUsize::new(0),
                ended: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        // Refuses to build a child at all.
        fn refusing_to_spawn() -> Self {
            Self {
                refuses: true,
                ..Self::new(0, false)
            }
        }

        fn holding_children_open() -> Self {
            let spawner = Self::new(0, false);
            spawner
                .ended
                .store(false, std::sync::atomic::Ordering::SeqCst);
            spawner
        }

        fn asked_count(&self) -> usize {
            self.asked.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn release(&self) {
            self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
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
            if self.refuses {
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

    // Every pairing produces exactly one decision, so the three counts account for every key the
    // merge paired. A key counted twice or not at all would show here.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn every_pairing_yields_exactly_one_decision() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);

        let paired = drive(&transfer).await;
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers + state.decided.deletes + state.decided.skips,
            paired,
            "the decisions do not account for every key that was paired"
        );
        assert_eq!(state.decided.deletes, 1, "d.txt is on the bucket alone");
    }

    // A verdict the run cannot act on carries two obligations. First, the run skips the key.
    // Second, the run records the plan as incomplete. Counting the key while calling the plan
    // complete would report a complete plan missing a key.
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
        assert_eq!(state.decided.skips, 2, "a deferred key was not skipped");
        assert_eq!(state.decided.transfers, 0);
        assert!(
            state.plan_incomplete,
            "the plan is short two keys and does not say so"
        );
    }

    // The two flags are independent, and either alone leaves the plan short.
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

    // The answer is about the plan, not about what is happening right now, so taking the merge away
    // for a work item must not change it.
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
        // `b.txt` is on both sides, so the union is four keys, not five.
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);
        assert_eq!(drive(&transfer).await, 4);
        assert!(
            transfer.inner.state.lock().failures.is_empty(),
            "a well-formed listing produced a failure"
        );
    }

    // A bucket holding keys, and the body every download of them returns.
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

    // A run whose source is the bucket and whose destination is a local tree.
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

    // Runs a download transfer the way the scheduler would, which is the only way a real child runs.
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

    // The other direction, end to end: keys become files, under the directories they need, carrying
    // the time the object had rather than the time they were written. Without that time a later run
    // reads every file as newer than its object and fetches the lot again.
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
        // Nothing half-written is left beside them.
        let strays: Vec<_> = walkdir_s3tmp(dir.path());
        assert!(
            strays.is_empty(),
            "temporary files were left behind: {strays:?}"
        );
    }

    // A time far outside the usual range still leaves the download intact. What makes that true is
    // structural rather than tested — the stamp has no error to report — because no portable input
    // makes the underlying call fail: a filesystem that cannot hold a far-future time clamps it. So
    // this covers the path end to end without being able to force the failure.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_time_that_cannot_be_applied_does_not_cost_the_download() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // Far outside what a filesystem can record, and outside what the arithmetic can represent.
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
        // The stamp ran and the platform took what it could of the time. The arithmetic holds
        // `i64::MAX` seconds, so nothing here declines. The filesystem truncates to the furthest
        // date it can store — centuries from now, nowhere near the write time. The assertion
        // measures that distance and not an exact value, because each filesystem truncates to its
        // own limit.
        //
        // This pins the stamp still running. A stamp that silently stopped would leave the file
        // holding its write time, which this run set a moment ago, and fail here.
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

    // Deleting into a local tree removes the file and leaves the directory holding it. A directory
    // sync did not create is not sync's to remove, and pruning is something to ask for separately.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_local_delete_removes_the_file_and_leaves_its_directory() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["keep/gone.txt"]);
        // An empty bucket, so the local file is the key the source does not have.
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

    // A key is arbitrary text and `..` is ordinary text in one, so a bucket can hold a key naming a
    // place above the destination. Neither writing nor removing may follow it there. The delete side
    // is the sharper half: a run with deletion on would otherwise remove a file nobody put in the
    // destination at all.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_naming_somewhere_above_the_root_is_refused() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let root = dir.path().join("inside");
        std::fs::create_dir(&root).expect("the root");
        // A file outside the root that no key should be able to reach.
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

    // A key already absent locally is the state the delete wanted, so a second run over the same tree
    // does not look worse than the first.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn deleting_a_file_that_is_already_gone_is_not_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = DeleteFromLocalTree::new(dir.path());

        let outcomes = deleter.delete(vec!["never-existed.txt".to_string()]).await;

        assert_eq!(outcomes, vec![Ok("never-existed.txt".to_string())]);
    }

    // A destination that does not exist yet is not a root that could not be listed: every key is
    // missing there, which is a complete answer rather than an absent one, and the directories appear
    // as the keys needing them do.
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

    // Every `.s3tmp.` file under a root, so a test can say nothing was left half-written.
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

    // A cancelled run lets go of the children it was holding, so each one clears the temporary file it
    // had opened. Holding them keeps those files on disk for as long as anything holds the run, which
    // is a destination littered with half-written names nobody asked for.
    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_run_lets_go_of_the_files_its_children_opened() {
        let dir = tempfile::tempdir().expect("a temp dir");

        // Filled in once the transfer exists, so the cancellation can land while bodies are moving.
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
        // The terminal signal fires the moment the run is cancelled, which can be while a reap is
        // still joining the children it took. Those joins are what clear the files of children that
        // had already left `children`, so the question is only answerable once nothing is outstanding.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let outstanding = {
                let st = transfer.inner.state.lock();
                st.reap_in_flight > 0 || st.merge_in_flight || st.deletes_in_flight > 0
            };
            if !outstanding || std::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let strays = walkdir_s3tmp(dir.path());
        assert!(
            strays.is_empty(),
            "a cancelled run is still holding the files its children opened: {strays:?}"
        );
    }

    // What a cancelled run owes is to stop starting work, not to stop work already started. A child
    // already away keeps going, and the run stays open until it ends — so cancelling is prompt about
    // the one thing it promises and patient about the rest.
    //
    // Untested until now because the earlier cancellation tests had the merge and the delete batch
    // outstanding, never a live child, which is where stopping and reporting meet.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_keeps_the_children_it_already_sent() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        // Decide every key, then send what the comparison qualified.
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

        // Still open, because the children it sent are still owed an answer.
        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a cancelled run called itself over with {live} children still away"
        );
        assert_eq!(
            spawner.asked_count(),
            live,
            "a cancelled run asked for another child"
        );

        // Once they end, the reap collects them and the run answers its waiter.
        spawner.release();
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "the children ended and the cancelled run still did not finish"
        );
    }

    // Cancelling and failing land on the same status, first write winning, so which one a caller is
    // told depends on ordering nobody controls. A caller who cancelled should hear that they
    // cancelled, rather than hearing about a key that failed on the way out.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_before_a_failure_still_reports_cancelled() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
        let (transfer, ctx) = deleting_with_policy(
            dir.path(),
            &["gone.txt"],
            deleter,
            DeleteMode::On,
            FailurePolicy::Abort,
        );

        ctx.set_cancelled();
        drive(&transfer).await;

        assert!(ctx.is_cancelled(), "the cancellation was lost");
        assert!(
            !ctx.is_failed(),
            "a cancelled run reported itself as failed instead"
        );
    }

    // A loop is reported and does not make the run look broken, because no setting would have sent it:
    // following it never terminates, and declining to follow makes it an obstruction the comparison
    // handles. The same run under abort keeps going for the same reason — stopping here would cost
    // every other key for something sync was never going to send.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_loop_warns_under_either_policy() {
        for policy in [FailurePolicy::Continue, FailurePolicy::Abort] {
            let dir = tempfile::tempdir().expect("a temp dir");
            std::fs::write(dir.path().join("a.txt"), b"x").expect("a file");
            let inner = dir.path().join("down");
            std::fs::create_dir(&inner).expect("a directory");
            // A link back to the directory above it, which has no end to follow.
            #[cfg(unix)]
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
                state.warnings.len(),
                1,
                "under {policy:?} the loop was not kept where a caller can read it"
            );
            assert!(
                state.failures.is_empty(),
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

    // A failure is the other half of that split, and under abort it ends the run. The teardown is the
    // one every ended run uses, so the pending batch goes the same way a cancellation would send it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_failure_ends_an_aborting_run_and_lets_its_batch_go() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
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
        // Not `!is_active`, which a run that completed normally also satisfies. What distinguishes an
        // aborted run from a finished one is the status it lands on, and that is what a caller reads.
        assert!(
            ctx.is_failed(),
            "a refused delete ended an aborting run without recording it as failed"
        );
        assert!(
            transfer.inner.state.lock().pending_deletes.is_empty(),
            "an ended run kept keys judged against a stream it stopped reading"
        );
    }

    // The same refusal under the default policy leaves the run going, which is the whole reason the
    // default is what it is: a sync converges, so one refused key is not worth the rest of the work.
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

    // The fourth site, and the one where a warning and a failure arrive through the same channel: a
    // listing that left out a field a comparison needs should have carried it, so it is a failure and
    // it stops an aborting run — where the loop above, on the same channel, does not.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_badly_described_key_ends_an_aborting_run() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // No last-modified, which is what the walk calls a malformed listing.
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

    // One policy means every site asks it, so each site needs its own proof. A child that failed after
    // it started reaches the parent at the reap, which is a different moment from a child that could
    // never be built.
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
                transfer.inner.state.lock().transfer_failures > 0,
                "under {policy:?} the failed child was not counted"
            );
            assert_eq!(
                ctx.is_failed(),
                expect_failed,
                "under {policy:?} the run's status does not match the policy"
            );
        }
    }

    // A site holding a real failure hands it over rather than a category of it, so a caller reads the
    // reason the site had. The kind is the test's subject because that is what a synthesized error
    // would have replaced.
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
        // A kind sync never synthesizes, so this can only have come from the site that refused.
        assert_eq!(
            *err.kind(),
            crate::error::ErrorKind::ObjectNotDiscoverable,
            "the run replaced the site's failure with a category of its own"
        );
    }

    // The other moment: a child that could not be built at all. It holds no slot and never reaches
    // the reap, so the site that reports it is the spawn itself.
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
                transfer.inner.state.lock().transfer_failures > 0,
                "under {policy:?} the refused spawn was not counted"
            );
            assert_eq!(
                ctx.is_failed(),
                expect_failed,
                "under {policy:?} the run's status does not match the policy"
            );
        }
    }

    // Nothing wrong at all is the third answer, and it has to be reachable or the other two mean
    // nothing.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_with_nothing_wrong_reports_clean() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["gone.txt"], deleter);

        drive(&transfer).await;

        assert_eq!(transfer.outcome(), RunOutcome::Clean);
    }

    // The defaults are the answer a caller gets without saying anything, and both are chosen rather
    // than inherited: deletion off because it destroys data nobody handed over, failure handling on
    // continue because a sync converges.
    #[test]
    fn the_defaults_are_continue_and_no_deleting() {
        assert_eq!(FailurePolicy::default(), FailurePolicy::Continue);
        assert_eq!(DeleteMode::default(), DeleteMode::Off);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_the_listing_described_badly_is_kept_as_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // No last-modified, which is what the walk calls a malformed listing.
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
            transfer.inner.state.lock().failures.len(),
            1,
            "the run ended without keeping what it could not account for"
        );
        // The walk sets its own flag for this, so the plan is short by the key nobody could
        // describe. This is the half the four-case table cannot reach: that one sets both flags by
        // hand, so it proves how they combine and not where either value came from.
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

    // The only test leaving the re-polling to the scheduler. Every other one calls `poll_work` in a
    // loop and never needs a signal to come back. So this is the one test failing if a work item
    // finishes without signalling. The tree has to be larger than one batch: a run finishing inside
    // a single work item signals from `execute` and never waits.
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
        // Managed threads, so dispatched work actually runs and the signal brings the transfer back
        // for the poll that ends it.
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

    // A run holding a delete batch the scheduler has not answered must not report itself over.
    // Emptying the merge by hand keeps the transfer active, so the assertion lands on the
    // completion test itself. Emptying it through `execute` would mark the transfer completed, the
    // poll would answer through the inactive path, and the test would prove nothing.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_batch_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut walk = transfer.inner.state.lock().walk.take().expect("the merge");
        while walk.next().await.is_some() {}
        assert!(walk.is_done(), "the merge did not reach the end");
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

    // A run finishing inside a work item tells its caller there. Nothing else will. The scheduler
    // tells a caller when it cancels a transfer, and when a work item fails, and its `Done` branch
    // drops a completed transfer silently. So the last item to empty the merge has to do it, and
    // this test asserts that without letting another poll run.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_item_that_drains_the_merge_answers_the_waiter() {
        // Nothing on either side, so the merge advance is the only work item and the one ending the
        // run. A key to transfer would make the last item a reap, and the claim would be about the
        // reap path. No further poll runs here.
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

        // No further poll. The caller has to already have its answer, and the deadline turns a
        // missing signal into a failure the test can see.
        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run drained inside a work item and left its waiter unanswered")
            .expect("the terminal signal was dropped");
    }

    // A run whose every entry fails must not grow with the tree. The walk reports most failures
    // per entry and carries on, so what stops peak memory tracking the entry count is the cap.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_where_everything_fails_keeps_a_bounded_sample() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("a temp dir");
        // Each unreadable directory costs one failure the walk reports and carries on from.
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
            state.failures.len() <= FAILURES_KEPT,
            "the run kept {} failures, so peak memory follows the tree",
            state.failures.len()
        );
        assert!(
            state.failures_dropped > 0,
            "nothing was dropped, so the cap was never reached and this proves nothing"
        );
    }

    // Spawn until the poll answers something else, handing that answer back. Looping on
    // `matches!(poll(), Spawned)` instead would poll once more than it consumes and silently discard
    // the work item that poll produced.
    fn spawn_until_something_else(transfer: &SyncTransfer<FsWalk, S3Walk>) -> PollWork {
        loop {
            match transfer.poll_work() {
                PollWork::Spawned => continue,
                other => return other,
            }
        }
    }

    // Builds a transfer whose children a test controls, so spawning and reaping can be driven a step
    // at a time.
    // A run reports the transfers that arrived, not the ones it decided to send.
    //
    // The two counts come from opposite ends of the run. The comparison picks a key out and
    // `decided.transfers` moves; the reap joins a child and `transferred` moves. A caller
    // reconciling the run against the destination reads the second. Deriving it from the first
    // means subtracting whatever intervened, and a key that never got a child survives that
    // subtraction to read as an arrival.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_reports_the_transfers_that_arrived() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        // Every child this spawner hands out has already ended badly. Both keys get a decision
        // and neither arrives.
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
                failure_policy: FailurePolicy::Continue,
            },
        );
        (transfer, ctx)
    }

    // Each of these holds the run open on its own. Adding a term to the completion test and not a
    // test for it is how a run reports itself finished with work still owed, which is the symptom
    // that made the completion test subtle in the first place.

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_decision_still_waiting_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        // A cap of zero clamps to one, so fill the one slot and leave a second decision waiting.
        a_local_tree(dir.path(), &["b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 1);

        // Drain the merge, then spawn until the slot is full.
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
    async fn a_live_child_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let parked = spawn_until_something_else(&transfer);

        assert_eq!(spawner.asked_count(), 1, "no child was spawned");
        assert!(
            matches!(parked, PollWork::Pending),
            "a child that has not ended did not hold the run open"
        );

        // Release it and the run finishes through a reap.
        spawner.release();
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
        // Spawning ends with the poll that produces the reap, so this is that work item.
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

    // The cap is the caller's share of the client, so it has to bound what is live rather than what
    // has been asked for.
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

    // Answers transfer for every key, including ones with no source to send.
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

    // A key with nothing to send cannot become a child. The run has to carry on past it: answering
    // the poll without enqueueing anything parks the run with the rest of the buffer still in it and
    // no work item left to wake it, which hangs rather than fails.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_transfer_decided_on_an_absent_source_does_not_strand_the_rest() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["b.txt"]);
        // `a.txt` is on the bucket alone, so its pairing has no source — and this comparison still
        // calls for a transfer.
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

        // `drive` panics on `Pending`, and a stranded buffer produces a `Pending`.
        let paired = drive(&transfer).await;
        assert_eq!(paired, 2);
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.transfer_failures, 1,
            "the key with no source was not accounted for"
        );
        assert_eq!(
            spawner.asked_count(),
            1,
            "the key behind it was never spawned"
        );
    }

    // The builder stats the path whenever metadata is missing, so a file gone away between the walk
    // and the spawn separates the two cases. With the walk's metadata the body still builds.
    // Without it the stat fails. The poll therefore reads no size the comparison has already
    // decided from.
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
        // One file in the tree, so the first entry is it.
        let entry = match walked.next().await {
            Some(Ok(entry)) => entry,
            Some(Err(err)) => panic!("the walk failed: {err}"),
            None => panic!("the walk produced nothing"),
        };
        assert!(
            entry.metadata().is_some(),
            "the walk read no metadata, so this test cannot tell the two paths apart"
        );

        // Remove the file. A spawn that stats again fails here; one that uses what the walk read
        // does not.
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

    // Both sides compare on keys relative to their own root, which the listing produces by stripping the
    // run's prefix off, so naming an object means putting the prefix back. The assertion watches the key
    // each write carried rather than a key a helper computes, because a mocked `PutObject` accepts any
    // key and a helper can be correct while its caller ignores it.
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

    // Builds a transfer whose deletes a test can watch.
    fn deleting(
        local: &Path,
        bucket_keys: &[&str],
        deleter: Arc<RecordDeletes>,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        deleting_with(local, bucket_keys, deleter, DeleteMode::On)
    }

    // A transfer whose spawner is a test's subject, against an empty bucket so every local key is sent.
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

    // The same, with the policy a test's subject.
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

    // The same, with the mode a test's subject.
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

    // Keys the source does not have reach the destination's delete path, batched rather than sent one
    // at a time, and each one's outcome comes back on its own.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn destination_only_keys_are_deleted_in_batches() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..7).map(|n| format!("gone{n}.txt")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        // A batch of three, so seven keys cannot come out as one request by accident.
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
        // The same count in both modes, so a caller can reconcile what happened against what was meant.
        assert_eq!(
            state.decided.deletable, 7,
            "a run allowed to delete does not report what the comparison marked"
        );
        assert_eq!(state.delete_failures, 0);
    }

    // A batch that fails partially has to say which keys survived, so the two counts are kept apart.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_refused_delete_is_counted_against_its_own_key() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.delete_failures, 2,
            "a refusal was not attributed per key"
        );
        assert_eq!(state.deleted, 0, "a refused key was counted as removed");
        // The reason reaches the run's record naming its key, because a count cannot answer which key
        // survived and that is the question a per-key outcome exists to answer.
        assert!(
            state.refusals.iter().any(|why| why.starts_with("a.txt:"))
                && state.refusals.iter().any(|why| why.starts_with("b.txt:")),
            "the refusal did not reach the record naming its key: {:?}",
            state.refusals
        );
    }

    // Deleting destroys data the caller never handed over, so a run that was not asked to delete leaves
    // the key alone. Reporting is the other half: the key is still accounted for, as a skip, because a
    // destination-only key that no column mentions is indistinguishable from one sync never saw.
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
            state.decided.skips, 2,
            "the two keys left alone were not accounted for"
        );
        // How much a run with deletion on would remove is the number a caller wants before turning it
        // on, so it survives separately from the skips it is folded into.
        assert_eq!(
            state.decided.deletable, 2,
            "the run cannot say how many objects turning deletion on would remove"
        );
        assert!(state.pending_deletes.is_empty());
    }

    // The shipped deleter, driven against a mocked bucket. Every other delete test substitutes
    // `RecordDeletes`, which exercises the loop's batching and never the request: the payload, the
    // retry, and reading the response's two lists apart all live here and nowhere else.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_deleter_reports_each_key_from_the_response_it_got() {
        use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
        use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

        // One key removed, one refused for good, and one the response never mentions.
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
                    .set_errors(Some(vec![S3Error::builder()
                        .key("data/held.txt")
                        .code("AccessDenied")
                        .message("denied")
                        .build()]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&answered]);

        let deleter = Deleter::Bucket(DeleteFromBucket::new(
            client,
            "amzn-s3-demo-bucket",
            Some("data"),
        ));
        let outcomes = deleter.delete(["gone.txt", "held.txt", "silent.txt"]).await;

        assert_eq!(
            outcomes.len(),
            3,
            "the outcomes do not account for every key sent"
        );
        // Each outcome sits at its own key's position, and each names that key.
        assert_eq!(outcomes[0], Ok("gone.txt".to_string()));
        let held = outcomes[1]
            .as_ref()
            .expect_err("a refusal was read as success");
        assert!(
            held.starts_with("held.txt:") && held.contains("denied"),
            "a refusal did not name its key and reason: {held}"
        );
        let silent = outcomes[2]
            .as_ref()
            .expect_err("a key the response skipped was read as success");
        assert!(
            silent.starts_with("silent.txt:"),
            "an unmentioned key was not named: {silent}"
        );
        // Treating an unmentioned key as unexplained is only sound while the response names its
        // successes, so the guard sits in the test that exercises the unmentioned path: under a quiet
        // response every removed key would arrive here and a whole successful batch would read as failed.
        assert!(
            quiet_flags.lock().iter().all(|q| *q == Some(false)),
            "the request left the response's verbosity to chance: {:?}",
            quiet_flags.lock()
        );
    }

    // A refusal S3 reports against one key is the normal way it sheds load, so that key is asked about
    // again rather than counted as lost. The keys the same response removed are not named again, and the
    // second ask waits: answering a request for less load with an immediate retry asks for more of it.
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
        let outcomes = deleter.delete(["gone.txt", "busy.txt"]).await;
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
        // The schedule is full-jittered, so only its presence is assertable, not its length. Zero means
        // no wait at all, which is the defect this guards.
        assert!(
            waited > Duration::ZERO,
            "the refused key was asked about again with no wait"
        );
        // Reading the outcomes depends on the response naming successes, which a quiet response does not
        // do, so the request says so rather than trusting a server default to stay put.
        assert!(
            quiet_flags.lock().iter().all(|q| *q == Some(false)),
            "a request left the response's verbosity to chance: {:?}",
            quiet_flags.lock()
        );
    }

    // A batch still out holds the run open, like every other kind of outstanding work.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_delete_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, _ctx) = deleting(dir.path(), &["a.txt"], deleter);

        // Drain the merge, which leaves one key waiting to be deleted.
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

    // Cancelling lets the pending batch go. A key reached that buffer by being absent from the source,
    // and absence is read from the merge having passed its position — so a run that stopped early
    // judged these keys against a stream with a hole in it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_lets_the_pending_batch_go() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
        let (transfer, ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

        // Advance the merge once so both keys are decided and waiting, without letting the flush run.
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

    // The only test using the real spawner. Every other one hands back a controlled child, so no
    // other test shows an object reaching the bucket. Managed threads, because a real child is a
    // scheduled transfer and the tokio test handle never runs dispatched work.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_qualified_key_reaches_the_bucket() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);

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

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers, 2,
            "both keys should have been sent"
        );
        assert_eq!(
            state.transfer_failures, 0,
            "a child failed, so the upload path is not working"
        );
        assert!(
            state.children.is_empty() && state.reap_in_flight == 0,
            "the run ended with children unaccounted for"
        );
    }

    // Cancelling does not make a dispatched work item go away. Reporting the run over while one is
    // out would have the scheduler retire the transfer under an item still holding the merge.
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
}
