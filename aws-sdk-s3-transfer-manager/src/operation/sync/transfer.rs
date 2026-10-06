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
// Why one key was not removed: the category a caller acts on, and the text they read.
//
// The category travels with the refusal because only the destination settles it. A bucket refusing
// a key answers for the service. A filesystem refusing one answers for the disk. A key naming no
// file this run would remove answers for the input. The path collecting these serves all three, so
// a category chosen there would fit one and mislead the others.
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

type KeyOutcome = Result<String, Refusal>;

// Whether the run has decided to stop, asked from inside a destination's own retry loop.
//
// The delete path asks once before handing a batch over, which covers every key that goes out on
// the first try. A throttled key then waits seconds for its next attempt, and nothing inside that
// wait could learn the run had stopped, so one guard covered one attempt of three.
pub(crate) type StopCheck<'a> = &'a (dyn Fn() -> bool + Send + Sync);

// Where a key's bytes go, or the key it is refused for.
//
// A key ending in the delimiter names a place rather than a file, and the trailing separator does
// not survive the derivation: `photos/2019/` arrives as a file called `2019`, which is the name the
// directory holding every key under it needs. Writing there collides with that directory and
// deleting there removes the file belonging to a different key, so both of sync's local sites
// refuse it rather than acting on a path that means something else.
//
// The rule is sync's rather than the derivation's, because the directory download has shipped
// accepting such a key and refusing it there would fail a transfer that used to work.
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
    //
    // Also refuses a key the derivation did not leave alone. Every key that reaches here came from
    // the local walk, which reports real paths, so normalising one changes nothing and the check
    // never fires. A key of S3 origin would be different: two distinct objects can share a
    // normalised path, and a removal decided for one would take the file belonging to the other.
    // The check costs a comparison and states the invariant that keeps this path safe, so a
    // direction added later that feeds it bucket keys stops here instead of removing the wrong
    // file.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        let path = local_path_for_key(&self.root, key)?;
        // The containment check derived this remainder to decide the path is under the root; the
        // question here is whether it still spells the key, so both read the one answer.
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
                    // The derivation's own category. It reports that the key named no file this
                    // run would remove; the disk refusing to unlink one reports something else.
                    outcomes.push(Err(Refusal::new(
                        err.kind().clone(),
                        format!("{key}: {err}"),
                    )));
                    continue;
                }
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => outcomes.push(Ok(key)),
                // A file already gone is the state the delete wanted, and a run that reports it as a
                // failure would make a second run over the same tree look worse than the first.
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

    async fn delete(&self, keys: Vec<String>, stopped: StopCheck<'_>) -> Vec<KeyOutcome> {
        // The object each relative key names, in the same order, so a response entry can be
        // attributed to the key it answers rather than to whatever sits at the same offset.
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
        // Which keys still have no answer. S3 reports partial throttling against individual keys
        // rather than by failing the request, so a key refused that way is asked again with
        // whichever others were refused the same way, and nothing already removed is named again.
        let mut outstanding: Vec<usize> = (0..keys.len()).collect();
        // What the service last said about each key still outstanding. A key chosen for another
        // attempt already has an answer — the throttle that refused it — so a run stopping
        // mid-wait settles these with the service's own words.
        let mut refused_with: std::collections::HashMap<usize, String> = Default::default();

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
                // A run that stopped during that wait asks for nothing more. These keys keep the
                // refusal that earned them another attempt, so the next run decides them from a
                // whole view the way this one would have.
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
                        // Both, because the service sends either on its own. A code names what
                        // the service objected to and a message explains it, so dropping the code
                        // leaves a refusal that carried one reporting no reason at all.
                        let why = match (error.code(), error.message()) {
                            (Some(code), Some(message)) => format!("{code}: {message}"),
                            (Some(code), None) => code.to_string(),
                            (None, Some(message)) => message.to_string(),
                            (None, None) => "no reason given".to_string(),
                        };
                        // The crate's own set, rather than a wider one for this path: a refusal
                        // declined here leaves the object in place and the next run decides it
                        // again from a whole view, so reading the set narrowly costs one deferred
                        // delete. Treating a code as retryable here and terminal elsewhere would
                        // cost a reader one answer to what the code means.
                        if !last_attempt && crate::retry::is_throttle_code(error.code()) {
                            refused_with.insert(at, why);
                            refused_again.push(at);
                        } else {
                            settled[at] = Some(unnamed(at, &why));
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

    // What this child has moved so far, read without joining.
    //
    // Joining is the only way to learn the outcome and not the only way to learn the count, which
    // the handle reports live. A run dropping a child without joining can still say what that
    // child moved, which is the difference between an incomplete total and a wrong one.
    pub(crate) fn bytes_so_far(&self) -> u64 {
        match &self.inner {
            ChildInner::Upload(handle) => handle.metrics().network_tx,
            ChildInner::Download(handle) => handle.metrics().network_rx,
            #[cfg(test)]
            ChildInner::Controlled { moved, .. } => *moved,
        }
    }

    // What the child moved, and whether it got there. Consuming, because learning the outcome
    // spends the handle — where the count alone comes off a borrow, which `bytes_so_far` above
    // does for a child the run lets go without joining.
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
            // A destination that does not exist yet is not a failure: every key is missing there,
            // which is a complete answer rather than an absent one, and the directories appear as the
            // keys that need them do.
            //
            // Asked and answered under two separate acquisitions, with a gap between them. Two
            // callers could both find a directory unknown and both create it, which costs a
            // redundant walk of the chain and nothing else, because creating a directory that is
            // already there succeeds and recording a name twice is the same as recording it once.
            // Holding one acquisition across the creation would close the gap at the price of
            // blocking every other caller on a filesystem call.
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
    skipped: Skipped,
    // Keys the comparison marked for removal, counted whether or not the run was allowed to act on them.
    // Independent of the mode on purpose: it gives both modes the same denominator, so a caller can
    // reconcile what was removed and what was refused against what was intended, and can ask how much a
    // delete would take away before allowing one.
    deletable: u64,
    // Of the obstructed skips, which kind of obstruction. Counted and not kept, because an
    // obstruction arrives as an entry the walk read successfully and not as a failure, leaving no
    // error to file beside the ones a run collects.
    obstructed: Obstructed,
}

// Every skip, by the reason the run skipped it.
//
// One total answers none of the questions a caller has. An unchanged key needed nothing, where a
// key whose absence went unread may have needed everything and the run could not tell; a key the
// destination already holds was a choice the mode made, where a deferred one is a defect. Reporting
// a single number says the run compared every one of them and found them current.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Skipped {
    unchanged: u64,
    destination_exists: u64,
    deferred: u64,
    unread: u64,
    obstructed: u64,
    // A removal the comparison decided and the mode did not allow. The comparison offers no reason
    // for this one, because the mode decides it here.
    delete_not_allowed: u64,
}

impl Skipped {
    // An exhaustive match, so a reason added to the comparison has to find a home here instead of
    // arriving inside whichever count a wildcard arm happened to name.
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
        // Taken apart by name, so a count added above stops the build here instead of going
        // missing from every total.
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

// Why a name held nothing a transfer could read, counted apart.
//
// A caller told only that an object was archived would take asking again as pointless, where a
// restore already under way means a later run gets the bytes. Telling the two apart is the whole
// reason for keeping three counts rather than one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Obstructed {
    // A socket, a device, a named pipe, or a symlink the walk was told not to follow.
    nothing_to_read: u64,
    // Bytes in an archive, with no restored copy to read.
    archived: u64,
    // A restore under way, so a later run finds the bytes there.
    restoring: u64,
}

impl Obstructed {
    // Matched exhaustively, so a kind added later has to be placed deliberately instead of
    // arriving inside whichever count a wildcard arm happened to name.
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
        // Taken apart by name rather than read field by field, so that a count added to this struct
        // stops the build here instead of being dropped from every batch without a word.
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

// What went wrong, as a sample a caller can read and a count that stays exact.
//
// A run can meet more failures than anyone wants to carry, so the sample stops at `FAILURES_KEPT`
// while the total keeps counting. Holding the two together means a caller asking whether anything
// went wrong asks once: a sample that filled up and a count that outran it are the same answer, and
// a question in two parts is one a reader can get half right.
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
    bytes_moved: u64,
    // Transfers that arrived, one per child the reap joins. `decided.transfers` counts the other
    // end, when the comparison picks a key out. A caller reconciling the run against the
    // destination reads this one: the decision count answers a different question, and deriving
    // arrivals from it means subtracting every population that intervened and knowing those do
    // not overlap.
    transferred: u64,
    // A child that could not be enqueued, or that ended badly, with why. A `StreamError` describes
    // a walk and says nothing about a transfer, so each of these arrives as its own description.
    transfer_failures: Reported<String>,
    // Decided work the run never did: a comparison this layer cannot act on, removals let go
    // before they were sent, keys qualified and never started. Kept here rather than written into
    // the walk, whose own flag means a stream went unread and is set nowhere but inside its `next`.
    plan_incomplete: bool,
    // Children let go before anyone joined them. Their bytes are counted, because a handle reports
    // those live, and their outcome is not: joining is the only way to learn whether a child
    // succeeded. Separate from `plan_incomplete` because every key was decided and acted on — the
    // plan was carried out, and what the run cannot say is how it went.
    outcomes_unknown: u64,
    // What the walk knew when a work item last handed it back. Read from the walk rather than
    // asked of it on demand, because the walk is away while an item holds it and an absent walk
    // has no answer to give — a plan either has holes or it does not, and that cannot depend on
    // whether a batch happens to be in flight.
    //
    // Accumulated rather than assigned, so a hole stays reported whatever the walk says later.
    // Assigning would be correct only while the walk's own flag is never cleared, which is not this
    // module's invariant to rely on.
    walk_plan_incomplete: bool,
    failures: Reported<StreamError>,
    // Names nothing could have transferred, which are reported and never acted on. Kept apart from
    // failures because folding them together would report a run as broken over a name it was never
    // going to send.
    warnings: Reported<StreamError>,
    // The failure that stopped the run, held until the run can be put out of the active state.
    //
    // Set where the failure is met, which is inside a poll, and read where the run ends. Flipping the
    // status there instead would end the poll's own reading of it half way through — one phase acting
    // on a run that another phase had already stopped — and a transfer out of the active state is not
    // polled again, so the end has to be reached on the same pass that decides it.
    stopped_by: Option<crate::error::Error>,
    // Why individual keys were not removed, each naming its key. A count alone cannot answer which
    // key survived, which is the question a per-key outcome exists to answer.
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

    // Whether every key was decided. Three things can leave holes and they are independent: a
    // stream the walk could not finish reading, a comparison this layer could not act on, and work
    // a run that is over never got back. Any one of them alone means the plan is short.
    //
    // The first two are read from state rather than from the walk, which is away while a work item
    // holds it. Asking an absent walk would make the answer depend on whether a batch is in
    // flight, and that is not a fact about the plan.
    //
    // The third is gated on the run being over, because a healthy run has work outstanding for
    // most of its life.
    pub(crate) fn is_plan_complete(&self) -> bool {
        let state = self.inner.state.lock();
        !state.plan_incomplete
            && !state.walk_plan_incomplete
            && !(!self.inner.ctx.is_active() && self.work_outstanding(&state))
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let mut state = self.inner.state.lock();

        // Read between the phases rather than once at the top, because a phase can be the thing that
        // stops the run: a spawn that fails under an aborting policy decides it half way through this
        // poll, and the phases after it must not act for a run that is already over.
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

    // Whether anything the run handed out is still owed back.
    //
    // Three work items carry state away with them: a merge takes the walk, a reap takes the child
    // handles, a delete batch takes keys that have already left the buffer. Each is counted by a
    // stand-in while it is gone, and `children` holds the ones no reap has taken yet. Asking about
    // all four in one place is what keeps a fifth from being forgotten: a stand-in nobody consults
    // is the same as no stand-in.
    fn work_outstanding(&self, state: &State<S, D>) -> bool {
        state.merge_in_flight
            || state.reap_in_flight > 0
            || state.deletes_in_flight > 0
            || !state.children.is_empty()
    }

    // Whether the run is over, by a caller cancelling or by a failure under an aborting policy.
    //
    // The status alone does not answer. A run that met a failure stays formally active until a pass
    // can carry it to its end, because a transfer out of the active state is never polled again. So
    // a site reading only the status acts for a run already over, and the window where the two
    // disagree is exactly the window where work is still outstanding.
    //
    // Every site deciding whether to do more work asks here. A delete batch reached the service
    // because its site read the status directly and found a run that had already decided to stop.
    //
    // Whether the run has *ended* is a different question, and `outcome()` reads the status for
    // that one: a run that decided to stop is not over while work is still outstanding.
    fn has_stopped(&self, state: &State<S, D>) -> bool {
        !self.inner.ctx.is_active() || state.stopped_by.is_some()
    }

    // End the run if the policy says a failure should. Recording the decision is the whole of what
    // happens here, and the next pass through `check_terminal` takes the run out of the active
    // state, answers the waiter and lets the pending batch go. A caller's cancellation reaches that
    // same teardown by another route and differs only in which status the run lands on, which is
    // what a result needs to tell the two apart.
    //
    // Takes the failure itself rather than a description of it, so a service refusal arrives
    // with its code, message and request ids intact. A category chosen here would be less than
    // what the site already had.
    //
    // The error that lands is *an* error rather than *the* error: the record is first-write-wins,
    // so under concurrency whichever site got there first is the one a waiter sees. Every failure
    // is still in the run's own records, which is where a caller reads the set.
    //
    // Writes to the run's own state and nothing else. Three of its five callers hold the state
    // lock, so anything here that reached the scheduler could come back round to a poll wanting
    // that same lock. Recording the failure and letting `check_terminal` act on it keeps that
    // impossible rather than merely unlikely.
    fn stop_if_aborting(&self, state: &mut State<S, D>, why: impl Into<crate::error::Error>) {
        if self.inner.failure_policy == FailurePolicy::Abort && state.stopped_by.is_none() {
            state.stopped_by = Some(why.into());
        }
    }

    // How the run turned out. A failure anywhere outranks a warning, and a warning outranks nothing,
    // because the three answer different questions: whether to look into it, whether to glance, and
    // whether to move on.
    pub(crate) fn outcome(&self) -> RunOutcome {
        let state = self.inner.state.lock();
        if state.failures.any() || state.transfer_failures.any() || state.refusals.any() {
            return RunOutcome::Failed;
        }
        if state.warnings.any()
            || state.decided.obstructed.any()
            // A plan that came out short of the keys a run decided on is not nothing to look into,
            // whether a caller stopped the run or a comparison asked for a key to be decided twice.
            || state.plan_incomplete
            || state.walk_plan_incomplete
            // A child let go before anyone joined it leaves the run unable to say how that
            // transfer went, which is worth a caller's attention even where the plan was carried
            // out in full.
            || state.outcomes_unknown > 0
            // Work still owed back by a run that is over was never accounted for. A reap carries
            // its children's byte counts and failures, a delete batch carries keys that already
            // left the buffer, a merge carries the walk. Ending normally requires all of them
            // returned, so reading one here means the scheduler discarded a work item before it
            // ran, or retired the run by a route that gave this layer no notice.
            //
            // Gated on the run being over. A healthy run has work outstanding for most of its
            // life, and an ungated read would call every one of those moments a warning.
            || (!self.inner.ctx.is_active() && self.work_outstanding(&state))
        {
            return RunOutcome::Warned;
        }
        RunOutcome::Clean
    }

    // Signal telling the caller whether the run is over. Answering `Done` with `merge_in_flight`
    // still set would report a finished run while the scheduler holds a work item, and the keys in
    // that item would go unreported.
    //
    // A run that an aborting policy stopped lands on its status here rather than where the failure
    // was met, because a transfer that is not active is never polled again: flipping the status at
    // the failure would spend the pass that still owed its waiter an answer.
    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if self.has_stopped(state) {
            // Everything dispatched is still owed an answer, however the run ended.
            if self.work_outstanding(state) {
                return None;
            }
            // A run told to stop goes out of the active state here and nowhere else, and only once
            // this pass can carry the run all the way to its end. Flipping it above the guard would
            // make a run with work still out terminal and then park: a transfer that is no longer
            // active is never polled again, so the answer owed to a waiter would wait for a pass
            // that cannot come. Every phase reads `stopped_by` instead of the status, so a run that
            // has decided to stop starts nothing further while the flip waits.
            if let Some(why) = state.stopped_by.take() {
                self.inner.ctx.set_failed(why);
            }
            // Whatever has not been sent is let go. A key reached this buffer by being absent from
            // the source, and absence is read from the merge having passed its position — so a run
            // that stopped early judged these keys against a stream with a hole in it. The next run
            // decides them again from a whole view, where sending them now could remove a file that
            // exists.
            // Letting those keys go leaves the plan short of what the run decided, which is what a
            // caller asking how the run went has to be told. Recorded here rather than inferred
            // from the status, because a plan can come out short while the run ends normally.
            state.plan_incomplete |= !state.pending_deletes.is_empty();
            state.pending_deletes.clear();
            state.plan_incomplete |= !state.waiting.is_empty();
            // Dropped once counted against the plan, the way the pending batch above is. Nothing
            // polls a terminal transfer, so anything left here would sit undispatched forever.
            state.waiting.clear();
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
        loop {
            // A run that has stopped — cancelled by its caller, or failed under an aborting
            // policy — starts nothing more, including the key this loop was about to reach. Asked
            // before the key leaves the buffer, so a run that stops has nothing to put back.
            if self.has_stopped(state) {
                return false;
            }
            let Some((pairing, _)) = state.waiting.pop_front() else {
                break;
            };
            let Some(entry) = pairing.source().entry() else {
                // A transfer is only decided for a source that is present, so reaching here means a
                // comparison answered something it had no grounds for.
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

    // Send a batch of deletes when it is full, or when nothing else will add to it. Holding a part
    // batch until the merge has finished lets a thousand keys cost one request.
    fn dispatch_deletes(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let size = self.inner.deleter.batch_size();
        let merge_done = match state.walk.as_ref().map(Walk::progress) {
            // Both sides paired every key either of them holds, so nothing can grow a part batch.
            Some(Progress::Accounted) => true,
            // More pairings will come, and any of them could add to the batch. A merge away with a
            // work item says nothing either way.
            Some(Progress::Pairing) | None => false,
            // A failure ended the merge, so the keys already buffered were judged against a stream
            // with a hole in it. The dispatch lets them go: holding them would leave a run that
            // cannot end, since ending asks for this buffer to be empty.
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
        // Asked again here, because a batch is decided in one pass and sent in another. A run that
        // stopped in between never finished reading, and a key counts as removable only because the
        // merge passed its position, so sending now would act on a position the run never reached.
        // Scoped, so the lock is gone before the round trip below. Asking needs the state and
        // sending must not hold it.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                // Recorded here because the keys are here. Both sites that account for let-go
                // removals read the buffer, and these keys left it when the batch was handed over.
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
        // The same question the guard above asked, handed to the destination so its own retry loop
        // can ask it again between attempts. Taking the lock here is safe because the guard above
        // scoped its own: nothing holds it across the round trip.
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

        // Summarised rather than carried: a key S3 refuses is reported inside a successful response
        // as a code and a message rather than as an error, so there is nothing here to hand over. The
        // reason naming its key is in the run's own records either way.
        //
        // The category comes from the refusal, because this path serves both directions. Telling a
        // caller the service refused a key the local filesystem refused would send them looking in
        // the wrong place, and deciding whether to try again turns on the difference.
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
        // Every reason, not only the first. One error reaches the attached failure under an
        // aborting policy and nowhere at all under the default one, so a run could report
        // transfers that failed while saying nothing about any of them.
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
        // Scoped, so the lock is gone before the pairing loop below awaits. Asking needs the
        // state and pairing must not hold it.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                state.walk = Some(walk);
                state.merge_in_flight = false;
                // Ends the same way every other arm does. Handing the walk back may leave the run
                // with nothing outstanding, and the pass that discovers as much is this one, so a
                // run that skipped both steps would owe its waiter an answer no later pass gives.
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
                                DeleteMode::Off => decided.skipped.record_delete_not_allowed(),
                            }
                        }
                        // A skip needs nothing done to it, so it never waits for a slot. What it
                        // was skipped for still matters: a name nothing could be sent for is worth
                        // telling a caller about, where a key already up to date is not.
                        Verdict::Decided(Decision::Skip(skip)) => {
                            decided.skipped.record(skip.reason());
                            // Which obstruction. The reason above separates an obstructed skip
                            // from the others and stops there.
                            if let Some(why) = skip.obstruction() {
                                decided.obstructed.record(why);
                            }
                        }
                        // Nothing shipped here defers, so one arriving is a defect in whatever
                        // comparison produced it. The key is skipped and the plan is marked short
                        // of the keys it should have covered. Sending the key or dropping it
                        // without a word would turn the defect into either wasted bandwidth or a
                        // file nobody was told about. Which key deferred is not recorded: what
                        // survives here is a count and a run-level flag.
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
        // A warning and a failure are recorded in different places and only one of them can stop the
        // run. Both are kept, because a key reported nowhere cannot be told from a key the run never
        // reached.
        let mut failed = None;
        let mut nothing_left = None;
        for entry in failures {
            if entry.is_warning() {
                state.warnings.record(entry);
                continue;
            }
            // A failure that leaves nothing to carry on with is kept apart from the rest. One entry
            // failing leaves a usable run; a root nobody could list means the run never learned
            // what was there, and carrying on past that reports a plan drawn from nothing.
            if entry.is_fatal() && nothing_left.is_none() {
                nothing_left = Some((entry.category(), entry.to_string()));
            }
            if failed.is_none() {
                // Both projections taken before the value moves below. The records want every
                // failure; a waiter wants one of them described, and a description is a category
                // and a message.
                failed = Some((entry.category(), entry.to_string()));
            }
            state.failures.record(entry);
        }
        // The failure itself goes to the records and a description goes to the waiter, which is the
        // right way round for one value with two readers. The records are where a caller reads
        // failures and must hold every one of them; the attached error is one of possibly many,
        // settled by whichever site got there first. Fidelity belongs to the complete surface, not to
        // the representative one.
        // A fatal failure ends the run under either policy, so it records the decision directly.
        // The default policy carries on past a failure because the rest of the run is still worth
        // doing, which is an argument that does not survive having no rest.
        if let Some((kind, why)) = nothing_left {
            if state.stopped_by.is_none() {
                state.stopped_by = Some(crate::error::Error::new(kind, why));
            }
        } else if let Some((kind, why)) = failed {
            self.stop_if_aborting(&mut state, crate::error::Error::new(kind, why));
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
    // Each child holds the temporary file it opened. Dropping one before it finished clears that
    // file as it goes, so keeping the handles leaves half-written names in the destination for as
    // long as anything holds the run.
    //
    // Letting go costs the outcome and not the bytes. A child reports what it has moved from a
    // borrow, so the count is read here while the handles are still held. Whether that work
    // succeeded is a different question, and joining is the only way to ask it: a reap is
    // asynchronous where this is not. The run records how many outcomes it will never learn, which
    // keeps it from reporting that it finished cleanly.
    fn on_terminal(&self) {
        // Taken out under the lock and dropped without it. A child's drop asks the scheduler to
        // cancel it, and the scheduler can come back round to this transfer, so dropping one while
        // holding the state lock would be waiting on a lock this thread already has.
        let children = {
            let mut state = self.inner.state.lock();
            if !state.children.is_empty() {
                state.outcomes_unknown += state.children.len() as u64;
                // What they moved, taken before the handles go. Dropping a handle cancels its
                // child, so the count at this moment is the count the run achieved.
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

    // A run that has not stopped, for the tests that are not about stopping.
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
                        // Standing in for a bucket, the destination every test that substitutes
                        // this double has in mind.
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

    // Hands back children that have already ended, so a test can assert on spawning and reaping
    // without a scheduler to run real ones. Counts what it was asked to spawn.
    struct SpawnEnded {
        moved: u64,
        fails: bool,
        // Whether building the child fails, as against the child failing once it runs. The two reach
        // the parent at different moments and only one of them ever holds a slot.
        refuses: bool,
        refuse_at: Option<usize>,
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
                refuse_at: None,
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

        // Holds its children open and refuses the one at this position, which leaves a run that has
        // decided to stop parked with a child still away.
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

        // The same, with a byte count each child reports while it is still running.
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

        // Ends every child this spawner handed out, and wakes the run as a real child would.
        //
        // A real child wakes its parent while signalling terminal, which is how a run learns it
        // has nothing left outstanding. These children are a flag with no scheduler behind them,
        // so the wake they cannot send is sent here. The context is a parameter to make that
        // unavoidable: a test cannot end a child and forget the wake, and forgetting it does not
        // fail the test. It leaves the test passing for some other reason.
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

    // A comparison that answers obstructed with a kind a test chooses. Reaching the archived kinds
    // through a real listing would mean a restore status on a mock, and what this tests is the
    // counting rather than the reading.
    // A comparison that cannot read absence, which no shipped mode produces: every mode here
    // decides from two described sides, where this answers before describing either.
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

    // An object whose restore is under way is told apart from one that is simply archived. A caller
    // hearing only that the bytes are archived would take asking again as pointless, where a
    // restore already running means a later run finds them there.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_archived_object_and_a_restoring_one_are_counted_apart() {
        use crate::io::key::stream::Obstruction;

        for why in [Obstruction::Archived, Obstruction::BeingRestored] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt"]);
            // Leaked, because `SyncTransfer` holds its comparison for the run's lifetime and the
            // run outlives this loop iteration.
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

    // A run skips a key whose absence it could not read, and it skips an unchanged key for a
    // different reason. One count for both tells a caller the run compared the key and found it
    // current, where the run never managed to compare it at all.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_whose_absence_went_unread_is_counted_apart_from_an_unchanged_one() {
        use crate::io::key::stream::KeysLost;

        for lost in [KeysLost::OneKey, KeysLost::UnknownRange] {
            let dir = tempfile::tempdir().expect("a temp dir");
            a_local_tree(dir.path(), &["a.txt"]);
            // Leaked for the same reason as the obstruction double above.
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
            state.decided.transfers + state.decided.deletes + state.decided.skipped.total(),
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
            transfer.inner.state.lock().failures.sample().is_empty(),
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

    // As above, and it records the key every GET carried. A canned response cannot see the request
    // that asked for it, so the only way to hold the asked-for key to account is to look at the
    // request going out.
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

    // A downloaded file carries the object's modification time, so a second run skips it. The
    // far-future test above leans on this one: a stamp that never ran leaves the file holding its
    // own write time, and that is the state the far-future test asserts it finds.
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

    // An object with contents whose key ends in the delimiter survives the folder-marker filter, which
    // only drops the zero-byte ones. It has no local name, and the run says so rather than discovering
    // it as a rename that could not go through.
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

    // A refusal names the category a caller should act on, and the two destinations answer
    // differently. Telling a caller the service refused a key the local disk refused sends them to
    // look at S3 for a filesystem problem, and whether retrying could help turns on the answer.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_refusal_carries_the_category_its_destination_answers_for() {
        use crate::error::ErrorKind;

        let dir = tempfile::tempdir().expect("a temp dir");
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("a directory to stand in for an unremovable file");

        // Removing a directory as though it were a file is the portable way to make the disk
        // refuse: no permission games, and the same answer on every platform.
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

        // The same deleter refusing for a different reason answers for the input: this key names no
        // file the run would remove. The derivation decided that, where the disk decided the one
        // above.
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
        // Letting go is not instant. The terminal signal fires as the run is cancelled, while a reap
        // may still be joining children it took, and the run's own release happens once the state
        // lock is free rather than while it is held. So the question is whether the files go, not
        // whether they are gone the moment a waiter hears anything.
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

    // The key a download asks the service for, read off the request rather than off the helper
    // that built it. A run under a prefix compares on names taken relative to it, so the prefix
    // has to go back on before the GET goes out. Asserting on the helper would pass even if the
    // call site stopped using it, which is how the same mistake reached the bucket on the upload
    // side.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_download_asks_for_the_key_under_its_prefix() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // Listed as the service lists them: under the prefix, which the walk then takes off so both
        // sides compare on the same names.
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

    // Both of sync's local sites refuse a key naming a place, for the same reason and with
    // different consequences: writing there lands on the directory every key under it needs, and
    // deleting there removes the file belonging to the key without the trailing separator.
    #[test]
    fn neither_local_site_acts_on_a_key_naming_a_place() {
        let root = Path::new("/tmp/root");
        for key in ["photos/2019/", "a/"] {
            assert!(
                local_path_for_key(root, key).is_err(),
                "a local site would have acted on {key:?}"
            );
        }
        // The key without it is a file, and still resolves.
        assert_eq!(
            local_path_for_key(root, "photos/2019").expect("a file"),
            Path::new("/tmp/root/photos/2019")
        );
    }

    // A name holding nothing a transfer could read is not a clean run. Nothing was sent for that
    // name and nothing ever could be, so a caller told the run was clean would take it that every
    // key arrived — which is the one thing the answer is for.
    //
    // Unix only, because the obstruction has to be real: a named pipe is the cheapest name that
    // holds nothing readable. The attribute belongs on the test and not just on the fixture, or
    // the assertions run on a platform where nothing created the case they are about.
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

    // A cancelled run let go of removals it had already decided on. Reporting the run as clean
    // would tell a caller that nothing needs looking into, when what happened is that keys the run
    // judged removable were dropped unjudged.
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

        // Stop once the removals are buffered and before any of them leave.
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

    // The failure a caller is handed keeps the category the failure arrived with. A service refusal
    // and a filesystem error call for different answers from whoever reads the kind, and deciding
    // whether to try again is the obvious one.
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

    // A listing that failed part way leaves the keys after it unread, so the keys it did report
    // cannot be called absent from the other side. The walk stops either way, and a stop is what
    // releases the part batch, so the release has to ask which kind of stop happened.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_listing_that_failed_does_not_release_the_part_batch() {
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
        use aws_smithy_types::error::metadata::ErrorMetadata;

        // One page of keys, then a refusal. The keys are destination-only against an empty local
        // tree, so they reach the buffer before anything fails.
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

    // A run retired while it still holds children has unjoined bytes, whichever way the scheduler
    // retired it. The notice this layer gets arrives on some of those routes and not others, so the
    // answer comes from reading the children at the moment someone asks.
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

        // Terminal by status alone, which is the route that gives this layer no notice.
        ctx.set_cancelled();

        assert_ne!(
            transfer.outcome(),
            RunOutcome::Clean,
            "a run holding {held} unjoined child(ren) called itself clean"
        );
    }

    // A batch in flight when the run decides to abort must not be sent. The status cannot answer
    // that question: the run stays formally active for exactly as long as the batch is outstanding,
    // because flipping the status early would spend the last poll the run gets. So the window
    // where the status says active and the run has already stopped is the whole window this guard
    // covers.
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

        // Take the batch without running it, which leaves the slots counted as outstanding.
        let mut held = None;
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            if transfer.inner.state.lock().deletes_in_flight > 0 {
                held = Some(work);
                break;
            }
            transfer.execute(&mut work).await;
        }
        let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");

        // A failure arrives while the batch is away. The run decides to stop, and the status stays
        // active because the batch is still counted.
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

    // Bytes a dropped child moved are still bytes the run moved. A join is the only way to learn
    // a child's outcome, and not the only way to learn its byte count: the handle reports that
    // live. Dropping the handle cancels the child, so the count at that moment is the count.
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

    // Children dropped on notice leave the run unable to say how those transfers went. Joining is
    // the only way to learn a child's outcome, and the notice arrives before anyone joined. The
    // plan itself was carried out: every key was decided and acted on, so a caller asking about
    // the plan hears yes, and a caller asking how the run went hears otherwise.
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

    // Keys qualified and never started leave the plan short. The run drops them where it drops the
    // delete buffer, and the same accounting has to cover both.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn keys_qualified_and_never_started_leave_the_plan_short() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        // Stop as soon as keys are qualified and before any of them start. A held child would set
        // the same flag by another route, and the teardown below never runs while one is alive.
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

        // Cancel, then poll so the teardown runs with nothing else outstanding.
        ctx.set_cancelled();
        let _ = transfer.poll_work();

        assert!(
            !transfer.is_plan_complete(),
            "a run that dropped {queued} qualified key(s) reported a complete plan"
        );
    }

    // A reap carries its children's byte counts and failures away with it, so a reap that never
    // runs takes a failed child's record with it. The delete batch was reported and fixed; this
    // arm reaches the same state by the same route and was reported by nothing.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_reap_that_never_ran_leaves_the_plan_short() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        // Children that end, and end badly, so the reap would have had a failure to record.
        let spawner = Arc::new(SpawnEnded::new(0, true));
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let _ = spawn_until_something_else(&transfer);

        // Take the reap without running it, which is what the scheduler's purge does.
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

    // Keys let go by an abandoned batch leave the plan short. The two places that account for
    // let-go removals both read the buffer, and these keys left the buffer when the batch was
    // handed over, so a run could report a complete plan while a thousand decided removals never
    // happened.
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

    // A key the service refused is a service refusal, whatever the request's own status was. S3
    // reports it inside a successful response as a code and a message, so nothing arrives here as
    // an error — which says where the reason came from, not what kind of failure it is.
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

    // A source root nobody can list ends the run whichever policy is set. The default policy
    // carries on past a failure because the rest of the run is still worth doing; here there is no
    // rest. One entry failing leaves a usable run, where a root that cannot be listed means sync
    // never learned what was there.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_source_root_nobody_can_list_fails_the_run() {
        for policy in [FailurePolicy::Continue, FailurePolicy::Abort] {
            let dir = tempfile::tempdir().expect("a temp dir");
            let absent = dir.path().join("not-there");
            let spawner = Arc::new(SpawnEnded::new(0, false));
            let (transfer, ctx) = uploading_with_policy(&absent, spawner, 4, policy);

            // Bounded on purpose. Without the decision below the run never ends, so driving to
            // terminal would report the defect as a hang.
            while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
                transfer.execute(&mut work).await;
            }

            // The decision is consumed where the run ends, so the status is what remains to read.
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

    // The local deleter acts only on a key the derivation leaves alone. Keys reaching it come from
    // the local walk, which reports real paths, so this never fires in the directions that exist.
    // A key of S3 origin is the case it guards: two distinct objects can normalise onto one path,
    // and a removal decided for one would take the file belonging to the other.
    #[test]
    fn the_local_deleter_refuses_a_key_its_derivation_would_rewrite() {
        let deleter = DeleteFromLocalTree::new("/tmp/root");
        for key in ["a//b", "a/./b", "a/../b", "./a"] {
            assert!(
                deleter.file_path(key).is_err(),
                "the deleter accepted {key:?}, whose path names a different key's file"
            );
        }
        // A key the local walk could have produced resolves as it reads.
        assert_eq!(
            deleter.file_path("a/b").expect("a key from a real path"),
            std::path::Path::new("/tmp/root/a/b")
        );
    }

    // `local_path_for_key` cleans the path it returns, and the root reaches the deleter as the
    // caller wrote it, so the deleter has to clean the root before comparing the two. A root of `.`
    // shows why: cleaning strips that prefix from every key beneath it, leaving a bare name that a
    // raw `.` does not prefix. A deleter that reads every key as renamed removes nothing the run
    // decided to remove.
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

    // A batch abandoned on cancellation has to give back every slot it took. The run cannot end
    // while any delete is counted as outstanding, so a batch of many keys that returns one slot
    // leaves the rest counted forever and the waiter never hears anything.
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

    // An aborting run stops spawning inside the pass that met the refusal. The guard above it in
    // the loop is the only thing that stops it: the phase gate on the next poll is too late,
    // because the keys after the refused one are reached before this poll ends.
    //
    // The pair of conditions here is {cancelled, aborting} against {spawn, delete}, and the
    // The other way the two counts come apart: a run that stops with decisions still buffered.
    //
    // Above, the keys got children and the children failed. Here they never got one — the
    // population a caller subtracting from the decision count cannot see.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_stopped_run_reports_fewer_arrivals_than_it_decided() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        // The first key is held, the second refused. That refusal stops the run with the third
        // still buffered behind it.
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

        // Letting the held child go carries the run to the pass that tears it down.
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

    // A caller's cancellation stops the next key as surely as a failure does. Cancelling flips the
    // status without touching `stopped_by`, and the status flips under a compare-exchange that
    // takes no state lock, so it can land after the phase gate in `poll_work` has already let this
    // pass through. A spawn site reading only `stopped_by` starts one more child for a run the
    // caller has already called off.
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

        // The cancellation lands with the gate already behind us. A concurrent `cancel_transfer`
        // opens exactly that window.
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

    // cancelled-spawn and aborting-delete cells were covered while this one was not.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_does_not_start_the_keys_after_the_one_that_failed() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        // The first key is taken and held, the second is refused. The refusal ends the run while
        // the third key is still waiting behind it.
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        // The first spawn, which succeeds.
        let _ = transfer.poll_work();
        // The pass that meets the refusal.
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

    // A delete batch handed to a work item is not yet sent, and cancelling between those two
    // moments has to stop the send. Dropping buffered deletes is pointless otherwise: a key counts
    // as absent because the merge passed its position, and a run that stopped early never finished
    // reading, so the batch in flight rests on the same hole as the batch still queued.
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

        // Decide every key, which buffers the removals the empty local tree calls for, and stop
        // holding the delete batch rather than running it. Merge work and delete work both arrive
        // as ready work, so the one in hand is told apart by what the dispatch recorded.
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

    // Cancellation has to stop work that was qualified and not yet started, which is the case the
    // other cancellation tests cannot reach: they cancel once the queue is already empty, so the
    // guard over the spawn phase holds nothing. Here keys are left buffered on purpose.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_does_not_start_the_keys_it_had_buffered() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        // Room for every key, so that what stops the spawn below can only be the cancellation. A
        // cap of one would stop it too, and the test would pass without the guard it is about.
        let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        // The loop ends on the first spawn, because deciding and starting share one poll sequence
        // and `Spawned` is not `Ready`. With one child allowed, the keys decided after it stay
        // buffered behind the one that went.
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
        spawner.release(&ctx);
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
    //
    // A live child opens the window the test needs. The refusal records its reason, and the status
    // write waits for the last child to go, so the cancellation can land between the two. With no
    // child the refusal and the write fall in one poll and nothing can get between them. A refused
    // delete key will not do either: the run files that as a refusal, and a refusal does not end a
    // run.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_before_a_failure_still_reports_cancelled() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        // The spawner holds the first key and refuses the second.
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            let _ = transfer.execute(&mut work).await;
        }
        // The first spawn, then the pass that meets the refusal.
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

        // The cancellation lands first. Letting the child go then carries the run to the pass that
        // would write the failure it is still holding.
        ctx.set_cancelled();
        spawner.release(&ctx);
        drive(&transfer).await;

        assert!(ctx.is_cancelled(), "the cancellation was lost");
        assert!(
            !ctx.is_failed(),
            "a cancelled run reported the failure it was carrying instead"
        );
    }

    // An aborting run answers its waiter. The status it sets is terminal, and the scheduler does not
    // poll a terminal transfer, so a poll that both aborts and returns without dispatching anything
    // is the last poll the run ever gets — and the signal a waiter is owed has to have happened by
    // then rather than on a pass that never comes.
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
            // The first child is built and held open; the second refuses, which aborts the run while
            // that child is still away. The poll then parks, and the only thing that could finish the
            // run is a later poll the scheduler will not make.
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
        // Let the held child end, so the only thing left is for the run to notice.
        tokio::time::sleep(Duration::from_millis(100)).await;
        spawner.release(&ctx);
        // Two ways this goes wrong and only one is a timeout: a receiver also resolves when its sender
        // is dropped, which is what happens to a descriptor removed without signalling. So the inner
        // result is the one that says a waiter was actually answered.
        let answered = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("an aborting run never answered its waiter");
        assert!(
            answered.is_ok(),
            "the run was removed without signalling, so its waiter got nothing"
        );
        assert!(ctx.is_failed(), "the run did not report itself failed");
    }

    // A run that aborts drops its pending deletes rather than sending them. The keys in that buffer
    // were judged absent from the source by the merge having passed their position, and a run that
    // stopped early judged them against a stream with a hole in it.
    //
    // The hazard is one poll deciding twice: the spawn phase can abort the run, and the delete phase
    // that follows must not act on what the spawn phase learned was no longer wanted.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_aborting_run_does_not_send_the_deletes_it_buffered() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // One local file to spawn for, and one bucket key with no local file, so the same merge
        // produces both a transfer and a delete.
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
            // The spawn refuses, which is what aborts the run mid-poll.
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

    // A loop is reported and does not make the run look broken, because no setting would have sent it:
    // following it never terminates, and declining to follow makes it an obstruction the comparison
    // handles. The same run under abort keeps going for the same reason — stopping here would cost
    // every other key for something sync was never going to send.
    // Unix only, because the case is a symlink pointing back up its own descent and only this
    // platform's fixture below creates one. Without the guard on the test, the assertions run where
    // nothing made the loop they are about.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_loop_warns_under_either_policy() {
        for policy in [FailurePolicy::Continue, FailurePolicy::Abort] {
            let dir = tempfile::tempdir().expect("a temp dir");
            std::fs::write(dir.path().join("a.txt"), b"x").expect("a file");
            let inner = dir.path().join("down");
            std::fs::create_dir(&inner).expect("a directory");
            // A link back to the directory above it, which has no end to follow.
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

    // A failure is the other half of that split, and under abort it ends the run. The teardown is the
    // one every ended run uses, so the pending batch goes the same way a cancellation would send it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_failure_ends_an_aborting_run_and_lets_its_batch_go() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // One key per batch, so that the refusal of the first arrives while the second is still
        // buffered. At the full batch size both keys leave in a single drain before any refusal
        // exists, and an empty buffer afterwards would say nothing about what the abort dropped.
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
        // The buffer being empty is only worth asserting alongside what left it. One key was sent
        // and refused; the other was dropped rather than following it.
        assert_eq!(
            deleter.keys_sent(),
            1,
            "an ended run sent a key judged against a stream it stopped reading"
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
            transfer.inner.state.lock().failures.sample().len(),
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
            state.failures.sample().len() <= FAILURES_KEPT,
            "the run kept {} failures, so peak memory follows the tree",
            state.failures.sample().len()
        );
        // The total outrunning the sample is what says the cap was reached. A total above zero
        // only says a failure arrived, which one failure satisfies, and the bound this test exists
        // for would then go unexamined.
        assert!(
            state.failures.total() > state.failures.sample().len() as u64,
            "the total did not outrun the sample, so the cap was never reached"
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
        uploading_with_policy(local, spawner, cap, FailurePolicy::Continue)
    }

    // The same, with the policy a test's subject. A run that stops on the first failure and a run
    // that carries on take different paths out of the spawn loop.
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

    // Each of these holds the run open, and the completion test reads every one of them. Adding a
    // term to that test and not a test for it is how a run reports itself finished with work still
    // owed, which is the symptom that made the completion test subtle in the first place.
    //
    // No test isolates the buffer. While a run is active, any pass that finds a decision waiting
    // and a free slot spawns it, so a buffer holding a decision with no child alive lasts less than
    // one poll. The two tests below take the two states a run does reach: the slot full, and the
    // run stopped.

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

    // A decision the run stops before reaching goes back to the buffer, and it goes back as the
    // decision the comparison made. A key qualified for transfer that returns as "the two sides
    // matched" says the opposite of what happened to it, and the run counts it against a plan it
    // reports as short without being able to say which key it dropped.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_decision_a_stopped_run_gives_back_keeps_the_decision_it_was_made_with() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
        // The spawner holds the first key and refuses the second. That refusal stops the run with
        // the third key still buffered behind it.
        let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
        let (transfer, _ctx) =
            uploading_with_policy(dir.path(), spawner.clone(), 4, FailurePolicy::Abort);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        // The first spawn, the pass that meets the refusal, then the pass that finds the run
        // stopped and gives the third key back.
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

        // Release it and the run finishes through a reap.
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
    // An upload whose comparison a test supplies, for the arms no shipped mode produces.
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
        assert_eq!(state.refusals.total(), 0);
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
            state.refusals.total(),
            2,
            "a refusal was not attributed per key"
        );
        assert_eq!(state.deleted, 0, "a refused key was counted as removed");
        // The reason reaches the run's record naming its key, because a count cannot answer which key
        // survived and that is the question a per-key outcome exists to answer.
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
            state.decided.skipped.total(),
            2,
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
                    .set_errors(Some(vec![
                        S3Error::builder()
                            .key("data/held.txt")
                            .code("AccessDenied")
                            .message("denied")
                            .build(),
                        // A refusal the service named by code alone. Reporting only the message
                        // would tell a caller nothing about a key it still holds.
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
        // Each outcome sits at its own key's position, and each names that key.
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
        // The service named this refusal by code and said nothing else. A caller reading the reason
        // learns what the service objected to, or learns nothing at all.
        let terse = outcomes[3]
            .as_ref()
            .expect_err("a refusal was read as success");
        assert!(
            terse.why().starts_with("terse.txt:") && terse.why().contains("InvalidArgument"),
            "a refusal carrying only a code reported no reason: {terse:?}"
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

    // A run that stops while a refused key waits for another attempt asks for nothing more. The
    // wait before that attempt runs for seconds, which makes it the one place a destination can
    // learn mid-task that the run is over. The key keeps the refusal that earned it the attempt:
    // the service's own answer, and the one the next run would see.
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

        // Stopped throughout. The first attempt still goes, because the batch was already judged
        // and handed over; what the answer governs is whether a second one follows.
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

        // What the bucket received, which is the only evidence a child did its work. Deciding to
        // transfer a key happens before any child exists. A run that decided two keys and sent
        // none reports the same two counts below.
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
    async fn an_aborting_run_hands_the_merge_back() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, ctx) = uploading_with_policy(dir.path(), spawner, 4, FailurePolicy::Abort);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };

        // A failure decides the run while the merge is away. The status stays active, because the
        // merge is outstanding work and flipping early would spend the run's last poll. So the
        // status cannot answer whether this item should carry on.
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
    // `SSL_CERT_FILE` is needed where rustls cannot read the platform's root
    // certificates; without it the TLS provider panics before any request goes out.
    //
    // TODO(sync): Tests below demonstrate functional correctness of sync within `pub(crate)`,
    // before a public API exists for a caller to start a run. `#[ignore]` keeps the tests off an
    // ordinary run, and tests are planned to move to `examples/` for customer demonstration.
    // ======================================================================

    // Both named by the environment, so nothing here says which account it runs against. The
    // region and credentials come from the environment too, through `load_defaults`.
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

    // A prefix nothing else is using, so a run that deletes can reach only its own keys.
    fn a_run_prefix(what: &str) -> String {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos();
        format!("sync-e2e/{what}-{stamp}/")
    }

    // What the bucket holds under a prefix, relative to it, sorted.
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

    // Every file under a root, relative and sorted, with the separator written as `/` so a tree
    // and a listing compare directly.
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

    // Removes only what this run put in the bucket.
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

    // An upload under a prefix puts every key beneath it and nowhere else. The walk reports keys
    // relative to the run's root, so naming an object means putting the prefix back; a run that
    // forgets writes to the bucket root.
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

    // The mirror, in the direction nothing has exercised: the prefix comes off rather than going
    // on. A run that forgets writes `root/<prefix>/a.txt` for `root/a.txt`.
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

    // What every caller does: sync, change little, sync again. The comparison has to agree with S3
    // about size and time well enough to leave an unchanged key alone. A run that disagrees
    // re-sends everything on every pass, and no mocked test would show it, because the mocks
    // answer with whatever time the test asked for.
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

        // The same the other way, which is where the stamp earns its place: a downloaded file
        // keeps the time its object had, so a later run reads it as current.
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

    // One object past the multipart threshold, up and back.
    //
    // Every other run here moves bytes small enough for a single request, so the multipart path —
    // the parts, the completion, and the stamp that attaches once the file is whole — has never
    // executed against S3. The threshold is 16 MiB, so twenty crosses it with room to spare.
    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_moves_an_object_past_the_multipart_threshold() {
        let c = real_client().await;
        let prefix = a_run_prefix("multipart");
        let src = tempfile::tempdir().expect("a temp dir");
        // Not all one byte: a run that mixed up part order would still match a uniform file.
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

        // And a second run leaves it alone, which is the stamp working on a file big enough to
        // have taken several parts.
        let again = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 2: {again:?}");
        assert_eq!(
            again.transfers, 0,
            "the second download fetched the object again"
        );

        remove_prefix(&c, &prefix).await;
    }
}
