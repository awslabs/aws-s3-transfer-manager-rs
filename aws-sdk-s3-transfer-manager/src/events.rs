/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-entry transfer events on a bounded channel the caller drains.
//!
//! An *entry* is one thing a run acts on: an object uploaded, an object
//! downloaded, the root of a directory operation. Every entry produces one
//! [`TransferEvent::Planned`] the moment its action is chosen, and — only if that
//! action was attempted — one [`TransferEvent::Ended`] when the attempt ends. A
//! skip attempts nothing, so its `Planned` is its terminal; a run that decides
//! without attempting produces decisions only, which is the absence of the second
//! event rather than a flag on the first.
//!
//! **Every event stands alone.** Delivery is bounded and lossy, so no event may
//! depend on the caller having seen an earlier one: [`TransferRef`] and
//! [`Decision`] appear on both variants, and a consumer needs no side map.
//!
//! **Lifecycle is pushed; quantities are pulled.** No event carries a byte count or an
//! object count. Each [`TransferEvent::Planned`] instead hands over a
//! [`TransferView`](crate::types::TransferView) — a read-only handle you keep and read
//! whenever you want to repaint.
//!
//! **At most one `Ended` arrives per `Planned`**, however the entry ended. Because delivery
//! is lossy, a total summed from the stream is only a lower bound; read a counter off the
//! view, such as [`TransferView::entries_settled`](crate::types::TransferView::entries_settled),
//! for the number the operation itself reports.
//!
//! **Emitting never blocks, awaits, or runs your code.** Your sink is not called back on a
//! transfer's own thread, and an event that finds a full channel is dropped and counted by
//! [`TransferEventStream::dropped`] rather than making a transfer wait on you.
//!
//! Printing one of the familiar per-entry lines takes one event and nothing else:
//!
//! ```
//! use aws_sdk_s3_transfer_manager::events::{Decision, Direction, Outcome, TransferEvent};
//!
//! /// `upload: <src> to <dest>` / `download:` / `copy:` / `delete: <dest>`.
//! ///
//! /// `dry_run` is the caller's own fact, not the event's: a run that attempts
//! /// nothing produces the same `Planned` a run that attempts everything does, so
//! /// the events carry no mode flag for whoever chose the mode to read back.
//! fn line(event: &TransferEvent, dry_run: bool) -> Option<String> {
//!     let transfer = event.transfer();
//!     // Every pattern takes `{ .. }`: that is what `#[non_exhaustive]` costs an
//!     // external consumer, and what keeps a later variant additive.
//!     let verb = match event.decision() {
//!         Decision::Delete { .. } => "delete",
//!         // A skip prints a warning or nothing, never one of the four lines.
//!         Decision::Skip { .. } => return None,
//!         _ => match transfer.direction() {
//!             Direction::Upload => "upload",
//!             Direction::Download => "download",
//!             Direction::Copy => "copy",
//!             _ => "transfer",
//!         },
//!     };
//!     // A delete names one end, which is the CLI's single-location line.
//!     let location = match event.decision() {
//!         Decision::Delete { .. } => transfer.destination().to_string(),
//!         _ => format!("{} to {}", transfer.source(), transfer.destination()),
//!     };
//!     Some(match event {
//!         TransferEvent::Planned(_) if dry_run => format!("(dryrun) {verb}: {location}"),
//!         // Planned, and the attempt is still to come: the line prints at `Ended`.
//!         TransferEvent::Planned(_) => return None,
//!         TransferEvent::Ended(ended) => match ended.outcome() {
//!             Outcome::Failed { .. } => format!("{verb} failed: {location}"),
//!             Outcome::Cancelled { .. } => return None,
//!             _ => format!("{verb}: {location}"),
//!         },
//!         _ => return None,
//!     })
//! }
//! let _ = line;
//! ```
//!
//! And the other half — a whole-transfer bar, pulled rather than pushed. The root is the
//! one entry with no parent, so one subscription feeds both the per-entry lines above and
//! the bar below:
//!
//! ```
//! use aws_sdk_s3_transfer_manager::events::{TransferEvent, TransferEventStream, TryNextError};
//! use aws_sdk_s3_transfer_manager::types::{Total, TransferView};
//!
//! /// Drain whatever has arrived, then draw. Called on the caller's own clock — every
//! /// 100 ms, on a keypress, whenever suits — not once per event.
//! fn repaint(stream: &mut TransferEventStream, root: &mut Option<TransferView>) -> Option<String> {
//!     loop {
//!         match stream.try_next() {
//!             // `Planned` for the root carries the view the bar is drawn from.
//!             Ok(TransferEvent::Planned(p)) if p.parent().is_none() => {
//!                 *root = p.view().cloned();
//!             }
//!             Ok(_) => continue,
//!             // Nothing new. The view is still live, so draw from what is already held.
//!             Err(TryNextError::Empty) => break,
//!             // Every sink is gone: this is the last frame.
//!             Err(TryNextError::Disconnected) => break,
//!             // `TryNextError` is `#[non_exhaustive]`, so external code needs this arm.
//!             // Stopping the drain is the safe default for an unfamiliar reason: it
//!             // still repaints from the view it holds, and it cannot spin.
//!             Err(_) => break,
//!         }
//!     }
//!
//!     let view = root.as_ref()?;
//!     let done = view.metrics().network_rx;
//!     let bytes = match view.byte_total() {
//!         // A percentage is defined only against a final total.
//!         Total::Final(total) if total > 0 => {
//!             format!("{:.1}%", (done as f64 / total as f64) * 100.0)
//!         }
//!         // Still enumerating: the denominator can grow, so a bar drawn on it would
//!         // walk backwards. Show bytes instead.
//!         Total::Provisional(total) => format!("{done} of {total}+ bytes"),
//!         _ => format!("{done} bytes"),
//!     };
//!
//!     // The other half of a progress line, and the half bytes cannot supply: how many
//!     // entries are left. Counts endings, so it reaches zero even when entries fail --
//!     // where the byte bar above stops short by whatever never moved.
//!     let settled = view.entries_settled();
//!     let files = match view.entry_total() {
//!         Total::Final(total) => format!("{} file(s) remaining", total - settled),
//!         // `~` because enumeration can still raise the total.
//!         Total::Provisional(total) => {
//!             format!("~{} file(s) remaining", total.saturating_sub(settled))
//!         }
//!         _ => format!("{settled} file(s) done"),
//!     };
//!     Some(format!("{bytes} with {files}"))
//! }
//! let _ = repaint;
//! ```

use std::fmt;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

// The loom compat layer for the atomics, not `std::sync::atomic` directly: the
// terminal obligation is a two-thread protocol (a claim races every other terminal
// site) and this is what makes those two operations model-checkable. Pointers stay
// `std::sync::Arc` — they appear in the public types, which must not change shape
// under a test cfg.
use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::transfer::TransferId;

/// Which way an entry's bytes move.
///
/// Unit variants with no per-variant `#[non_exhaustive]`: a direction has nothing
/// to attach, so it can only grow by gaining a variant, which the enum-level
/// attribute already admits. `#[non_exhaustive]` on a *unit* variant would make it
/// unmatchable by name to external code rather than field-additive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Direction {
    /// Local filesystem to S3.
    Upload,
    /// S3 to local filesystem.
    Download,
    /// S3 to S3.
    Copy,
}

/// One end of an entry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Endpoint {
    /// An S3 object.
    #[non_exhaustive]
    S3 {
        /// Bucket, access point ARN, or outpost ARN, as the caller gave it.
        /// Interned once per operation rather than once per entry.
        bucket: Arc<str>,
        /// The full object key, prefix included. Not a key relative to any root: a
        /// key prefix or a custom delimiter makes the two different strings, so
        /// neither derives the other.
        key: Arc<str>,
    },
    /// A file on the local filesystem.
    #[non_exhaustive]
    Local {
        /// The path the operation used, never canonicalized, so a caller that
        /// passed a relative root reads its own `./a` back.
        ///
        /// `None` when the operation was handed an already-open file rather than a
        /// path, as by [`write_to_file`](crate::operation::download::builders::DownloadFluentBuilder::write_to_file):
        /// the end is a local file, but this layer was never told which one.
        path: Option<Arc<Path>>,
    },
    /// An end the caller owns: the body of a streaming download, or an in-memory
    /// or caller-supplied upload body.
    ///
    /// Distinct from [`Endpoint::Unresolved`]: this end has no path because none
    /// was ever wanted, which is the transfer working as asked.
    #[non_exhaustive]
    Stream {},
    /// An end whose address could not be derived, so the entry never reached it.
    ///
    /// A separate variant rather than an empty key or the operation's root: those
    /// read as real addresses to a consumer, and one of them would be printed as if
    /// the transfer manager had touched it.
    #[non_exhaustive]
    Unresolved {},
}

impl fmt::Display for Endpoint {
    /// `s3://bucket/key`, the path (lossy for a non-UTF-8 name), `(local file)` for a
    /// local file whose path is unknown, `-` for a caller-owned stream, or
    /// `(unresolved)`. This is the rendering the `upload:` / `download:` / `copy:` /
    /// `delete:` lines use, so no consumer writes a formatter of its own.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::S3 { bucket, key } => write!(f, "s3://{bucket}/{key}"),
            Endpoint::Local { path: Some(path) } => write!(f, "{}", path.display()),
            Endpoint::Local { path: None } => f.write_str("(local file)"),
            Endpoint::Stream {} => f.write_str("-"),
            Endpoint::Unresolved {} => f.write_str("(unresolved)"),
        }
    }
}

/// What an event is about: one entry, and where its two ends are.
///
/// Private fields, so [`TransferRef::direction`] is whatever the constructor that
/// built the value fixed it to and nothing can set it separately from the ends. The
/// same privacy is what keeps a later field additive, so no `#[non_exhaustive]` is
/// needed on top of it. Cloning is refcount-only, so repeating this on both events
/// of a pair allocates nothing.
#[derive(Debug, Clone)]
pub struct TransferRef {
    direction: Direction,
    source: Endpoint,
    destination: Endpoint,
}

impl TransferRef {
    /// An entry moving up: a local file or a caller-supplied body, to an S3 object.
    ///
    /// One constructor per direction, taking both ends. The endpoint *kinds* cannot
    /// carry the direction on their own — an upload source may be a stream and a
    /// destination may be unresolved — so the constructor's name is what fixes it,
    /// and a call site cannot name one direction while meaning another.
    pub(crate) fn upload(source: Endpoint, destination: Endpoint) -> Self {
        Self {
            direction: Direction::Upload,
            source,
            destination,
        }
    }

    /// An entry moving down: an S3 object to a local file, or to a body the caller
    /// consumes.
    pub(crate) fn download(source: Endpoint, destination: Endpoint) -> Self {
        Self {
            direction: Direction::Download,
            source,
            destination,
        }
    }

    /// Which way the bytes move.
    ///
    /// Carried rather than derived from the endpoint pair, because a delete has one
    /// meaningful end: an S3 object removed at the destination could belong to an
    /// upload run or to a copy run, and the pair cannot tell them apart.
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Where the entry is read from.
    pub fn source(&self) -> &Endpoint {
        &self.source
    }

    /// Where the entry is written to, and the end a [`Decision::Delete`] removes.
    pub fn destination(&self) -> &Endpoint {
        &self.destination
    }
}

/// Why an entry needs moving.
///
/// Every variant is a fact about comparing two present-or-absent ends, so none can
/// reach a delete or a skip: those decisions have no field of this type. Braced
/// throughout with a per-variant `#[non_exhaustive]`, because each has an obvious
/// field to gain — the two sizes, the two times, what forced it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransferReason {
    /// The destination has no entry under this name.
    #[non_exhaustive]
    NotAtDestination {},
    /// Both ends have the entry and their sizes differ.
    #[non_exhaustive]
    SizeDiffers {},
    /// Both ends have the entry and their modification times differ.
    #[non_exhaustive]
    TimeDiffers {},
    /// No comparison was made: the transfer was asked for unconditionally.
    #[non_exhaustive]
    Forced {},
}

/// Why an entry was left alone.
///
/// Carries no severity. The same reason is silent, a warning, a skip, or a hard
/// failure depending on how the run was configured, so severity belongs to whoever
/// holds the configuration and cannot be structural here.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SkipReason {
    /// Present at both ends, and they compared equal.
    ///
    /// A variant of its own rather than a shade of the two unknowns below, because
    /// the three read alike in a log line and the first means the opposite of the
    /// other two: this one says everything matched, those say the run could not
    /// look.
    #[non_exhaustive]
    Unchanged {},
    /// Removed by an include/exclude rule, on whichever side the rule applied to.
    ///
    /// One variant, not one per side: a caller counting filtered entries must be
    /// able to count them by matching a single reason.
    #[non_exhaustive]
    ExcludedByFilter {},
    /// In an archival storage class and not restored, so its bytes cannot be read.
    #[non_exhaustive]
    ArchivalStorageClass {},
    /// Another entry differing from this one only by case already claims the
    /// destination this entry would have taken.
    #[non_exhaustive]
    CaseConflict {},
    /// Present, and not a regular file — a device, FIFO, or socket.
    ///
    /// Distinct from the two unknowns, and the distinction is a safety property
    /// rather than a wording one: this entry **was** classified, so it is present,
    /// so it holds a delete back.
    #[non_exhaustive]
    Untransferable {},
    /// Present only at the destination, with delete mode off.
    #[non_exhaustive]
    DestinationOnly {},
    /// The source side could not be seen here, so nothing may be deleted: "nothing
    /// is there" licenses a delete, "I could not look" does not.
    #[non_exhaustive]
    ///
    /// Braced so it can gain the cause, for the same reason [`Outcome::Failed`] does
    /// not carry one yet.
    SourceUnknown {},
    /// The destination side could not be seen here, so nothing may be written:
    /// treating an unknown destination as absent skips the comparison and can
    /// overwrite a newer entry.
    #[non_exhaustive]
    DestinationUnknown {},
}

/// The action decided for an entry, and the reason for it.
///
/// The reason lives on the decision rather than on the outcome, because a decision
/// is the thing that has a reason: four of the reasons a caller must distinguish
/// explain why an entry *was* moved, and a deleted or never-attempted entry has no
/// outcome to hang one on. Splitting the reason set per action is what makes the
/// compiler reject a transfer paired with "excluded by a filter", rather than a doc
/// comment asking a reader not to write it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Decision {
    /// Move the entry. The direction is [`TransferRef::direction`].
    #[non_exhaustive]
    Transfer {
        /// Why it needs moving.
        reason: TransferReason,
    },
    /// Remove [`TransferRef::destination`].
    ///
    /// Carries no reason because the reason is the action: present at the
    /// destination, absent at the source, delete mode on. Braced-but-empty so it
    /// can gain one compatibly if a second way to reach a delete appears.
    #[non_exhaustive]
    Delete {},
    /// Leave the entry alone. Nothing is attempted, so this decision is terminal on
    /// arrival and no [`TransferEvent::Ended`] follows it.
    #[non_exhaustive]
    Skip {
        /// Why nothing was done.
        reason: SkipReason,
    },
}

/// How an attempted action ended.
///
/// No byte count: delivery is lossy, so a total summed from this stream would
/// silently disagree with the operation's own result, which the handle reports
/// exactly.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Outcome {
    /// The action completed.
    #[non_exhaustive]
    Succeeded {},
    /// The action failed. Read the error from the handle's `join()`.
    ///
    /// The error is not carried here: a transfer's error has a single owner, and
    /// handing a copy to an observer would take it from `join()`. Braced and
    /// `#[non_exhaustive]`, so the payload can be added without breaking a consumer
    /// once a failed transfer's error can be read without consuming it.
    #[non_exhaustive]
    Failed {},
    /// The action was cancelled, including by a sibling's failure under the
    /// `Abort` policy.
    ///
    /// Not folded into [`Outcome::Failed`]: one interrupt is one cancellation, not
    /// N per-entry failures, and the crate's own status machine keeps the two
    /// terminals apart everywhere else. Braced so it can gain the cause.
    #[non_exhaustive]
    Cancelled {},
}

/// One fact about one entry.
///
/// `parent: None` marks the operation the caller invoked. For
/// [`upload`](crate::Client::upload) or [`download`](crate::Client::download) that event is
/// the file itself; for [`upload_objects`](crate::Client::upload_objects) or
/// [`download_objects`](crate::Client::download_objects) it is the directory, whose endpoints
/// are a source directory and a key prefix, and every object under it carries
/// `parent: Some(root_id)`.
///
/// A consumer acting per file should therefore skip the `parent: None` event for the
/// directory operations only — skipping it always drops every single-file transfer.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransferEvent {
    /// An action was planned for the entry. Exactly one per entry.
    Planned(Planned),
    /// A planned action reached a terminal state. At most one per
    /// [`TransferEvent::Planned`], and none at all for a plan that attempts nothing.
    Ended(Ended),
}

/// An action was planned for one entry.
///
/// The fields are private and read through accessors, so a later field is additive and
/// the representation stays free after the first release. That is also what permits an
/// opaque id that can grow a generation without breaking a caller's pattern.
#[derive(Debug, Clone)]
pub struct Planned {
    id: TransferId,
    transfer: TransferRef,
    decision: Decision,
    view: Option<crate::types::TransferView>,
}

impl Planned {
    /// Opaque entry id, unique within the client.
    pub fn id(&self) -> u64 {
        self.id.id
    }

    /// The id of the directory operation this entry belongs to, or `None` when this
    /// event *is* that operation.
    pub fn parent(&self) -> Option<u64> {
        self.id.parent
    }

    /// The entry, and where its two ends are.
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
    }

    /// What was planned, and why.
    pub fn decision(&self) -> &Decision {
        &self.decision
    }

    /// Read-only view of this entry's byte counters, for as long as you keep it.
    ///
    /// `None` when the entry never became a transfer — a skip, or an entry the
    /// operation abandoned before it could be started — so there are no counters to
    /// read. Distinct from a live transfer sitting at zero bytes.
    pub fn view(&self) -> Option<&crate::types::TransferView> {
        self.view.as_ref()
    }
}

/// A planned action reached a terminal state.
///
/// Private fields for the same reason as [`Planned`].
#[derive(Debug, Clone)]
pub struct Ended {
    id: TransferId,
    transfer: TransferRef,
    decision: Decision,
    outcome: Outcome,
}

impl Ended {
    /// Opaque entry id, matching the [`Planned`] this ends.
    pub fn id(&self) -> u64 {
        self.id.id
    }

    /// The id of the directory operation this entry belongs to, or `None` when this
    /// event *is* that operation.
    pub fn parent(&self) -> Option<u64> {
        self.id.parent
    }

    /// The entry. Repeated so no consumer needs a side map.
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
    }

    /// Repeated for the same reason: a lost [`Planned`] must not cost the caller the
    /// verb it prints or the reason it reports.
    pub fn decision(&self) -> &Decision {
        &self.decision
    }

    /// How it ended.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }
}

impl TransferEvent {
    /// The entry this event is about. Present on every variant.
    pub fn transfer(&self) -> &TransferRef {
        match self {
            TransferEvent::Planned(e) => e.transfer(),
            TransferEvent::Ended(e) => e.transfer(),
        }
    }

    /// What was planned for the entry, and why. Present on every variant.
    pub fn decision(&self) -> &Decision {
        match self {
            TransferEvent::Planned(e) => e.decision(),
            TransferEvent::Ended(e) => e.decision(),
        }
    }
}

/// The largest capacity [`channel`] will honour.
///
/// The bound comes from the underlying channel's permit ceiling, not from this crate.
pub const MAX_CAPACITY: usize = usize::MAX >> 3;

/// Create a sink/stream pair. The caller owns the stream and drains it.
///
/// Delivery is bounded and lossy: a transfer never waits on the consumer, so a stream that
/// is drained slowly — or not at all — misses events once `capacity` is full.
/// [`TransferEventStream::dropped`] counts what was lost. Size `capacity` against how far
/// behind the consumer may fall.
///
/// Capacities above [`MAX_CAPACITY`] are clamped to it rather than panicking.
pub fn channel(capacity: NonZeroUsize) -> (TransferEventSink, TransferEventStream) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity.get().min(MAX_CAPACITY));
    let dropped = Arc::new(AtomicU64::new(0));
    (
        TransferEventSink {
            outlets: Arc::from(vec![Outlet {
                tx,
                dropped: dropped.clone(),
            }]),
        },
        TransferEventStream { rx, dropped },
    )
}

/// Combine the client-level sink with the request-level one.
///
/// Merged rather than overridden, both directions: a client-wide observer must not be
/// switched off by a request that registers its own sink, and a request's sink must not be
/// ignored because the client has one. Called at every operation entry point, which is why it
/// lives here rather than being rewritten six times.
pub(crate) fn resolve_sink(
    client_level: Option<&TransferEventSink>,
    request_level: Option<TransferEventSink>,
) -> Option<TransferEventSink> {
    match (client_level, request_level) {
        (None, request) => request,
        (Some(client), None) => Some(client.clone()),
        (Some(client), Some(request)) => Some(client.clone().merge(request)),
    }
}

/// One stream's receiving end, as seen from the producing side.
#[derive(Debug)]
struct Outlet {
    tx: tokio::sync::mpsc::Sender<TransferEvent>,
    /// This stream's own loss counter. Per-outlet and not shared: one slow consumer
    /// losing events says nothing about another, and a shared counter would report a
    /// fast consumer's stream as lossy because a different one fell behind.
    dropped: Arc<AtomicU64>,
}

/// Registration endpoint. Cheap to clone; every clone feeds the same stream or streams.
///
/// A sink carries one or more outlets. [`channel`] produces one; [`merge`](Self::merge)
/// combines sinks so a single registration feeds several independent consumers.
#[derive(Debug, Clone)]
pub struct TransferEventSink {
    outlets: Arc<[Outlet]>,
}

impl TransferEventSink {
    /// A sink that emits to this sink's consumers **and** `other`'s.
    ///
    /// Distinct consumers cannot affect each other: each has its own channel, its own capacity
    /// and its own [`dropped`](TransferEventStream::dropped) count, so a consumer that stops
    /// draining loses its own events and nobody else's.
    ///
    /// Merging the same sink twice is a no-op rather than a doubling, so registering one
    /// sink at both the client and the request level delivers each event once.
    pub fn merge(self, other: TransferEventSink) -> TransferEventSink {
        let mut outlets: Vec<Outlet> = Vec::with_capacity(self.outlets.len() + other.outlets.len());
        for src in [&self.outlets, &other.outlets] {
            for outlet in src.iter() {
                // Keyed on the channel rather than on the sink, because a sink is `Clone` and
                // two clones are the same consumer. `owes_finish` guarantees one *emit*; the
                // fan-out in `emit` is downstream of it, so a duplicate outlet breaks
                // exactly-once at the only place it means anything -- the consumer.
                if outlets
                    .iter()
                    .any(|kept: &Outlet| kept.tx.same_channel(&outlet.tx))
                {
                    continue;
                }
                outlets.push(Outlet {
                    tx: outlet.tx.clone(),
                    dropped: outlet.dropped.clone(),
                });
            }
        }
        TransferEventSink {
            outlets: Arc::from(outlets),
        }
    }

    /// Non-blocking send to every outlet. Counts the event as dropped on each outlet whose
    /// channel was full.
    ///
    /// Returns whether the event reached **at least one** consumer. A `false` return covers
    /// two different facts, and only the first is counted in
    /// [`TransferEventStream::dropped`]: the channel was full (the consumer lost an event),
    /// or the receiver is gone (there is no consumer to lose anything). Either way the
    /// producer does not stall, on any outlet.
    fn emit(&self, event: TransferEvent) -> bool {
        // The last outlet takes the event by move and the others clone, so a single-outlet
        // sink — the common case by far — clones nothing. Cloning is refcount-only anyway:
        // the endpoints a `TransferRef` carries are `Arc<str>`/`Arc<Path>`.
        let Some((last, rest)) = self.outlets.split_last() else {
            return false;
        };
        let mut delivered = false;
        for outlet in rest {
            delivered |= Self::push(outlet, event.clone());
        }
        delivered | Self::push(last, event)
    }

    /// Try one outlet, counting a full channel as a loss and a closed one as nothing.
    fn push(outlet: &Outlet, event: TransferEvent) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        match outlet.tx.try_send(event) {
            Ok(()) => true,
            // Counted: the consumer is still there and lost this one, which is what
            // `dropped()` reports.
            Err(TrySendError::Full(_)) => {
                outlet.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            // NOT counted. The receiver is gone, so every subsequent event is also
            // undeliverable and counting them would run `dropped()` up by the number of
            // remaining entries — turning "you lost some events" into a number that says
            // nothing about delivery. A consumer that stopped listening did not lose data.
            Err(TrySendError::Closed(_)) => false,
        }
    }
}

/// Receiving half. One consumer.
#[derive(Debug)]
pub struct TransferEventStream {
    rx: tokio::sync::mpsc::Receiver<TransferEvent>,
    dropped: Arc<AtomicU64>,
}

impl TransferEventStream {
    /// Next event, or `None` once every sink is gone and the queue is drained.
    pub async fn next(&mut self) -> Option<TransferEvent> {
        self.rx.recv().await
    }

    /// Non-blocking receive.
    ///
    /// The two failures are distinct answers for a polling consumer: [`Empty`]
    /// means come back, [`Disconnected`] means the operation is over and there
    /// will never be another event. Collapsing them into one `None` makes a
    /// consumer either spin forever or stop early.
    ///
    /// [`Empty`]: TryNextError::Empty
    /// [`Disconnected`]: TryNextError::Disconnected
    pub fn try_next(&mut self) -> Result<TransferEvent, TryNextError> {
        use tokio::sync::mpsc::error::TryRecvError;
        self.rx.try_recv().map_err(|e| match e {
            TryRecvError::Empty => TryNextError::Empty,
            TryRecvError::Disconnected => TryNextError::Disconnected,
        })
    }

    /// Events the consumer lost because the channel was full.
    ///
    /// Read this before trusting any total assembled from the stream: such a total
    /// is complete only while this is zero.
    ///
    /// Counts only events dropped for lack of room. Events not delivered because this
    /// stream was already dropped are not counted — there is no consumer left to lose
    /// them, and counting them would make this number grow with the size of the
    /// remaining transfer rather than with anything about delivery.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Why [`TransferEventStream::try_next`] had nothing to return.
///
/// Not a failure in the usual sense: `Empty` is the ordinary answer for a consumer
/// polling faster than events arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TryNextError {
    /// No event is queued, but the operation is still running. Poll again.
    Empty,
    /// Every sink is gone and the queue is drained. There will never be another
    /// event, so a consumer should stop rather than poll again.
    Disconnected,
}

impl fmt::Display for TryNextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryNextError::Empty => write!(f, "no event queued"),
            TryNextError::Disconnected => write!(f, "event stream closed"),
        }
    }
}

impl std::error::Error for TryNextError {}

/// Per-entry emitter, owning the terminal obligation.
///
/// The obligation is deliberately **not** the transfer status. A status CAS says
/// which thread won the transition; this flag says whether a `Ended` is still
/// owed. They come apart in both directions: an entry refused by the failure
/// policy has an obligation and no status, and a transfer that was never announced
/// has a status and no obligation.
pub(crate) struct TransferLifecycle {
    /// Released by whichever site discharges the terminal emit, which is why it is
    /// shared and optional rather than owned outright.
    ///
    /// Nothing drops a lifecycle at the end of an operation: it hangs off the transfer's
    /// inner, whose context holds a strong `Arc<Handle>`, so holding the sink by value
    /// kept a request-level `Sender` -- and with it the scheduler, runtime, memory budget
    /// and retry cache -- alive for the process. A consumer then has no way to drain to
    /// completion: `next()` never returns `None` and `try_next` never answers
    /// [`TryNextError::Disconnected`], because "every sink is gone" never becomes true.
    /// So the release is explicit, at the one instant the transfer is known to be over.
    sink: Arc<Mutex<Option<TransferEventSink>>>,
    id: TransferId,
    transfer: TransferRef,
    /// The decision both of this entry's events report.
    ///
    /// Stored once so the pair cannot disagree. The transfer manager compares
    /// nothing — it has no second side to compare against and no filters — so
    /// every entry it decides is an unconditional transfer, and the field is not a
    /// constructor parameter until a producer exists that can vary it.
    decision: Decision,
    /// The view `announce` hands out, or `None` for an entry with no transfer behind it.
    ///
    /// Held rather than passed to `announce`, because the obligation and the thing it
    /// announces are fixed together at construction: a site that can build a lifecycle
    /// already knows whether a transfer exists.
    view: Option<crate::types::TransferView>,
    /// Set by `announce`, cleared by whichever site claims the terminal emit.
    ///
    /// Shared with the [`PendingEmit`] the claim produces, so an emit dropped
    /// without being sent can hand the obligation back.
    owes_finish: Arc<AtomicBool>,
}

impl fmt::Debug for TransferLifecycle {
    /// Hand-written because `owes_finish` reads as an `Arc<AtomicBool>` address
    /// otherwise, and its value is the only interesting thing about it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransferLifecycle")
            .field("id", &self.id)
            .field("transfer", &self.transfer)
            .field("owes_finish", &self.owes_finish.load(Ordering::Relaxed))
            .finish()
    }
}

/// Lock the shared sink slot, tolerating poison.
///
/// A terminal path runs during a worker panic, so an unwind that poisoned this lock must
/// not take the terminal event with it. The slot holds a usable sink or `None`, and an
/// unwind cannot leave it in any third state, so the inner value is always safe to take.
fn lock_sink(
    slot: &Mutex<Option<TransferEventSink>>,
) -> std::sync::MutexGuard<'_, Option<TransferEventSink>> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl TransferLifecycle {
    /// A clone of the sink, so a parent can build emitters for its children
    /// without the caller also holding a sink.
    pub(crate) fn child_sink(&self) -> TransferEventSink {
        self.sink.clone()
    }

    pub(crate) fn new(
        sink: TransferEventSink,
        id: TransferId,
        transfer: TransferRef,
        view: Option<crate::types::TransferView>,
    ) -> Self {
        Self {
            sink: Arc::new(Mutex::new(Some(sink))),
            id,
            transfer,
            decision: Decision::Transfer {
                reason: TransferReason::Forced {},
            },
            view,
            owes_finish: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Announce the decision and take on the terminal obligation.
    ///
    /// The obligation is taken even if the send is dropped, so a lost `Planned`
    /// cannot silently cancel the matching `Ended`. That keeps the two counts
    /// comparable, which is what makes a dropped event visible.
    ///
    /// A [`Decision::Skip`] takes no obligation, which is what enforces that variant's
    /// "no `Ended` follows it": `finish` cannot answer for a lifecycle that never owed a
    /// terminal, so no shared terminal path can emit one for an entry nothing was attempted
    /// on. That matters to a consumer keying per-entry state off `Planned` and freeing it on
    /// `Ended`: a terminal for an entry that was never attempted is state it never
    /// allocated, and the decision alone is what tells the two shapes apart.
    /// Currently unreachable — [`new`](Self::new) constructs only `Transfer`.
    pub(crate) fn announce(&self) {
        if !matches!(self.decision, Decision::Skip { .. }) {
            self.owes_finish.store(true, Ordering::Release);
        }
        if let Some(sink) = &*lock_sink(&self.sink) {
            sink.emit(TransferEvent::Planned(Planned {
                id: self.id,
                transfer: self.transfer.clone(),
                decision: self.decision.clone(),
                view: self.view.clone(),
            }));
        }
    }

    /// Claim the terminal obligation. Returns the emit as a value; sending is the
    /// caller's, so it can happen after a state guard is released.
    ///
    /// The swap is the whole guarantee: exactly one caller sees `true`, so a
    /// terminal path reached twice emits once.
    pub(crate) fn finish(&self, outcome: Outcome) -> Option<PendingEmit> {
        if !self.owes_finish.swap(false, Ordering::AcqRel) {
            return None;
        }
        Some(PendingEmit {
            sink: self.sink.clone(),
            owes_finish: self.owes_finish.clone(),
            event: Some(TransferEvent::Ended(Ended {
                id: self.id,
                transfer: self.transfer.clone(),
                decision: self.decision.clone(),
                outcome,
            })),
        })
    }
}

/// A claimed terminal emit that has not been sent yet.
///
/// Claiming and sending are separate so a site holding a state guard can claim
/// under the guard and publish after releasing it.
pub(crate) struct PendingEmit {
    /// The lifecycle's own handle, not a copy of the sink: sending is also what releases
    /// it, so both sides must see the same slot.
    sink: Arc<Mutex<Option<TransferEventSink>>>,
    /// `None` once sent. Distinguishes a discharged emit from an abandoned one in
    /// [`Drop`].
    event: Option<TransferEvent>,
    owes_finish: Arc<AtomicBool>,
}

impl fmt::Debug for PendingEmit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingEmit")
            .field("event", &self.event)
            .finish()
    }
}

impl PendingEmit {
    /// Send it. Never blocks and never awaits, so this is legal from any site,
    /// including one holding a state guard.
    ///
    /// A full channel drops the event and counts it. Nothing was reserved for it:
    /// the one consumer that needs every terminal reads it from `join()`, which is
    /// exact.
    ///
    /// Sending is also where the sink is released, in the same critical section. This is
    /// the only point of no return: `finish` can be claimed and abandoned, and
    /// [`Drop`](PendingEmit::drop) hands that obligation back, so releasing any earlier
    /// would leave a re-armed obligation with nothing to emit through. After this the
    /// consumer's stream reports `Disconnected` once drained, which is what lets a drain
    /// loop end.
    pub(crate) fn send(mut self) {
        let Some(event) = self.event.take() else {
            return;
        };
        let mut slot = lock_sink(&self.sink);
        if let Some(sink) = &*slot {
            sink.emit(event);
        }
        *slot = None;
    }
}

impl Drop for PendingEmit {
    /// Give the obligation back if this emit was never sent.
    ///
    /// Without this, an early `return` or a `?` on a path that has already
    /// claimed consumes the obligation with a value that never reached the
    /// channel: `finish` returns `None` forever after, so the `Ended` is lost
    /// and nothing records that it was owed. Re-arming makes the loss recoverable
    /// by the next terminal site instead.
    ///
    /// Symmetric with `announce`, which takes the obligation even when its own
    /// send is dropped.
    fn drop(&mut self) {
        if self.event.is_some() {
            self.owes_finish.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(bucket: &str, key: &str) -> Endpoint {
        Endpoint::S3 {
            bucket: Arc::from(bucket),
            key: Arc::from(key),
        }
    }

    fn local(path: &str) -> Endpoint {
        Endpoint::Local {
            path: Some(Arc::from(std::path::Path::new(path))),
        }
    }

    fn upload_ref() -> TransferRef {
        TransferRef::upload(local("./a.txt"), s3("bucket", "k/a.txt"))
    }

    fn tid(id: u64, parent: Option<u64>) -> TransferId {
        TransferId { id, parent }
    }

    fn lifecycle() -> (TransferLifecycle, TransferEventStream) {
        let (sink, stream) = channel(NonZeroUsize::new(8).unwrap());
        (
            TransferLifecycle::new(sink, tid(1, None), upload_ref(), None),
            stream,
        )
    }

    fn failed() -> Outcome {
        Outcome::Failed {}
    }

    /// The doc example, run against every shape of event a run can produce. It is
    /// the requirement as an assertion: the four familiar lines come from one event,
    /// with no side map and no second lookup.
    ///
    /// One difference from the doc example, which is compiled as an external crate:
    /// `#[non_exhaustive]` does not apply in the defining crate, so the wildcard
    /// [`Direction`] arm an external consumer must write is unreachable here.
    fn line(event: &TransferEvent, dry_run: bool) -> Option<String> {
        let transfer = event.transfer();
        let verb = match event.decision() {
            Decision::Delete { .. } => "delete",
            Decision::Skip { .. } => return None,
            _ => match transfer.direction() {
                Direction::Upload => "upload",
                Direction::Download => "download",
                Direction::Copy => "copy",
            },
        };
        let location = match event.decision() {
            Decision::Delete { .. } => transfer.destination().to_string(),
            _ => format!("{} to {}", transfer.source(), transfer.destination()),
        };
        Some(match event {
            TransferEvent::Planned(_) if dry_run => format!("(dryrun) {verb}: {location}"),
            TransferEvent::Planned(_) => return None,
            TransferEvent::Ended(ended) => match ended.outcome() {
                Outcome::Failed { .. } => format!("{verb} failed: {location}"),
                Outcome::Cancelled { .. } => return None,
                _ => format!("{verb}: {location}"),
            },
        })
    }

    fn ended(transfer: TransferRef, decision: Decision, outcome: Outcome) -> TransferEvent {
        TransferEvent::Ended(Ended {
            id: tid(1, Some(0)),
            transfer,
            decision,
            outcome,
        })
    }

    fn forced() -> Decision {
        Decision::Transfer {
            reason: TransferReason::Forced {},
        }
    }

    #[test]
    fn every_printed_line_comes_from_one_event() {
        let ok = Outcome::Succeeded {};
        assert_eq!(
            line(&ended(upload_ref(), forced(), ok.clone()), false).as_deref(),
            Some("upload: ./a.txt to s3://bucket/k/a.txt"),
            "an upload's line must be printable from the event alone"
        );

        let down = TransferRef::download(s3("bucket", "k/a.txt"), local("./a.txt"));
        assert_eq!(
            line(&ended(down, forced(), ok.clone()), false).as_deref(),
            Some("download: s3://bucket/k/a.txt to ./a.txt"),
            "a download's line must name the S3 side as the source"
        );

        // Copy and delete have no producer in this crate yet, so they are built
        // here from the fields directly. They are what fixes `direction` as a
        // carried value: a delete names one end, and that end cannot say which
        // kind of run removed it.
        let copy = TransferRef {
            direction: Direction::Copy,
            source: s3("src", "k"),
            destination: s3("dst", "k"),
        };
        assert_eq!(
            line(&ended(copy, forced(), ok.clone()), false).as_deref(),
            Some("copy: s3://src/k to s3://dst/k"),
            "a copy's line must name both S3 ends"
        );

        let del = TransferRef {
            direction: Direction::Upload,
            source: Endpoint::Unresolved {},
            destination: s3("dst", "gone"),
        };
        assert_eq!(
            line(&ended(del.clone(), Decision::Delete {}, ok), false).as_deref(),
            Some("delete: s3://dst/gone"),
            "a delete's line must name only the end it removes"
        );

        // The entry a failure names comes from the event itself, so a consumer never
        // has to pair a failure with a separate lookup to know which entry broke. The
        // cause is not on the event -- it is read from the handle's `join()`.
        assert_eq!(
            line(&ended(upload_ref(), forced(), failed()), false).as_deref(),
            Some("upload failed: ./a.txt to s3://bucket/k/a.txt"),
            "a failure must name its entry from the same event"
        );

        // A dry run's only event is the decision, and whether the run is dry is the
        // caller's own fact, which is why nothing on the event says so.
        let planned = TransferEvent::Planned(Planned {
            id: tid(1, Some(0)),
            transfer: upload_ref(),
            decision: forced(),
            view: None,
        });
        assert_eq!(
            line(&planned, true).as_deref(),
            Some("(dryrun) upload: ./a.txt to s3://bucket/k/a.txt"),
            "a decision must print the same line without an outcome"
        );
        assert_eq!(
            line(&planned, false),
            None,
            "outside a dry run the line prints at the terminal, not the decision"
        );
    }

    /// The four line kinds, each formatted from a single event with nothing else in
    /// hand: no operation input, no side map, no second lookup. FR-Obs-1 is met only
    /// if all four strings come out of `line`, which reads `direction`, `decision`,
    /// `source`, `destination`, and `outcome` off the event and nothing more.
    ///
    /// Two of the four have no producer in this crate, so they are built from the
    /// fields directly: `TransferRef` exposes `upload` and `download` constructors
    /// only, and `TransferLifecycle::new` fixes every decision to
    /// `Transfer { Forced }`. That is a gap in the *emitters*, not in the event
    /// shape — the shape carries both, which is what this asserts.
    #[test]
    fn the_four_cli_line_kinds_each_come_from_one_event() {
        let ok = Outcome::Succeeded {};
        let cases = [
            (
                "upload: ./local/a to s3://bucket/a",
                TransferRef::upload(local("./local/a"), s3("bucket", "a")),
                forced(),
            ),
            (
                "download: s3://bucket/a to ./local/a",
                TransferRef::download(s3("bucket", "a"), local("./local/a")),
                forced(),
            ),
            (
                "copy: s3://src/a to s3://dst/a",
                TransferRef {
                    direction: Direction::Copy,
                    source: s3("src", "a"),
                    destination: s3("dst", "a"),
                },
                forced(),
            ),
            (
                // A delete names one end. `direction` is still carried and still
                // ignored by the line: an S3 object removed at the destination could
                // belong to an upload run or a copy run, so the endpoint pair cannot
                // supply the verb and the decision must.
                "delete: s3://bucket/a",
                TransferRef {
                    direction: Direction::Upload,
                    source: Endpoint::Unresolved {},
                    destination: s3("bucket", "a"),
                },
                Decision::Delete {},
            ),
        ];

        for (expected, transfer, decision) in cases {
            let event = ended(transfer, decision, ok.clone());
            assert_eq!(
                line(&event, false).as_deref(),
                Some(expected),
                "the line must be printable from this event alone: {event:?}"
            );
        }
    }

    /// Each of the reasons a caller must be able to tell apart, named once. The
    /// test is the enumeration: a reason that is not reachable from a decision or an
    /// outcome cannot be reported at all.
    #[test]
    fn every_required_reason_is_reachable_from_a_decision_or_an_outcome() {
        let transfer_reasons = [
            TransferReason::NotAtDestination {},
            TransferReason::SizeDiffers {},
            TransferReason::TimeDiffers {},
            TransferReason::Forced {},
        ];
        for reason in transfer_reasons {
            assert!(
                matches!(
                    Decision::Transfer { reason },
                    Decision::Transfer {
                        reason: TransferReason::NotAtDestination {}
                            | TransferReason::SizeDiffers {}
                            | TransferReason::TimeDiffers {}
                            | TransferReason::Forced {}
                    }
                ),
                "a reason for moving an entry must reach a transfer decision"
            );
        }

        let skip_reasons = [
            SkipReason::Unchanged {},
            SkipReason::ExcludedByFilter {},
            SkipReason::ArchivalStorageClass {},
            SkipReason::CaseConflict {},
            SkipReason::Untransferable {},
            SkipReason::DestinationOnly {},
            SkipReason::SourceUnknown {},
            SkipReason::DestinationUnknown {},
        ];
        assert_eq!(
            skip_reasons.len(),
            8,
            "the seven reasons for holding an entry back, plus unchanged: an \
             unknown entry and an unchanged one read alike in a log line and mean \
             opposite things, so they are separate variants"
        );
        for reason in skip_reasons {
            assert!(
                matches!(Decision::Skip { reason }, Decision::Skip { .. }),
                "a reason for leaving an entry alone must reach a skip decision"
            );
        }

        assert!(
            matches!(failed(), Outcome::Failed { .. }),
            "failure is an outcome, not a reason: it does not replace the reason the \
             entry was selected, which stays on the decision"
        );
    }

    #[test]
    fn both_events_carry_the_entry_and_the_decision() {
        let (lc, mut stream) = lifecycle();
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        for expected_outcome in [false, true] {
            let ev = stream.try_next().expect("both events reached the channel");
            assert_eq!(
                matches!(ev, TransferEvent::Ended(_)),
                expected_outcome,
                "the decision arrives before the terminal: {ev:?}"
            );
            assert!(
                matches!(ev.decision(), Decision::Transfer { .. }),
                "delivery is lossy, so the decision must be on the terminal too, or \
                 a consumer that missed the first event has no verb to print: {ev:?}"
            );
            assert_eq!(
                ev.transfer().destination().to_string(),
                "s3://bucket/k/a.txt",
                "both events must name the entry: {ev:?}"
            );
        }
    }

    #[test]
    fn finish_is_claimed_once() {
        let (lc, _stream) = lifecycle();
        lc.announce();
        // Hold the claim: dropping it would hand the obligation back, which is a
        // different property, tested below.
        let first = lc.finish(failed()).expect("first claim wins");
        assert!(
            lc.finish(failed()).is_none(),
            "a terminal path reached twice must not emit twice"
        );
        first.send();
        assert!(
            lc.finish(failed()).is_none(),
            "sending discharges the obligation for good"
        );
    }

    #[test]
    fn a_claim_dropped_unsent_hands_the_obligation_back() {
        let (lc, _stream) = lifecycle();
        lc.announce();
        // A site that claims and then returns early without publishing.
        drop(lc.finish(failed()));
        assert!(
            lc.finish(failed()).is_some(),
            "an abandoned claim must re-arm: otherwise the obligation is consumed \
             by a value that never reached the channel, and the `Ended` is lost \
             with nothing recording that it was owed"
        );
    }

    #[test]
    fn finish_without_announce_emits_nothing() {
        let (lc, _stream) = lifecycle();
        assert!(
            lc.finish(Outcome::Cancelled {}).is_none(),
            "nothing is owed for an entry that was never announced"
        );
    }

    #[test]
    fn a_full_channel_drops_and_counts_rather_than_waiting() {
        // Capacity 1: the decision fits, the terminal does not. Nothing is
        // reserved, so the loss is at the end rather than the beginning -- and it is
        // counted, which is the whole of what bounded delivery promises.
        let (sink, mut stream) = channel(NonZeroUsize::new(1).unwrap());
        let lc = TransferLifecycle::new(sink, tid(7, None), upload_ref(), None);
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        let ev = stream.try_next().expect("the decision reached the channel");
        assert!(
            matches!(&ev, TransferEvent::Planned(p) if p.id() == 7),
            "the first event through an empty channel is the decision: {ev:?}"
        );
        assert_eq!(
            stream.dropped(),
            1,
            "an event with no room is discarded and counted, and the run does not \
             slow down waiting for the consumer"
        );
        assert!(
            matches!(stream.try_next(), Err(TryNextError::Disconnected)),
            "a dropped event must not be delivered late -- and since `send` released the \
             sink, the stream is closed rather than merely empty"
        );
    }

    /// `dropped()` counts events the consumer lost, and only those.
    ///
    /// A full channel is a loss: the consumer is listening and did not get the event, so
    /// any total it assembles is short and `dropped()` is how it finds out. A gone
    /// receiver is not a loss — nobody is there to lose anything — and counting it made
    /// `dropped()` grow with however much transfer remained after the consumer stopped,
    /// so the number reported the size of the tail rather than anything about delivery.
    #[test]
    fn dropped_counts_a_full_channel_but_not_a_closed_one() {
        let (sink, mut stream) = channel(NonZeroUsize::new(1).unwrap());
        let ev = || {
            TransferEvent::Planned(Planned {
                id: tid(1, None),
                transfer: upload_ref(),
                decision: Decision::Transfer {
                    reason: TransferReason::Forced {},
                },
                view: None,
            })
        };

        assert!(sink.emit(ev()), "the first event fits");
        assert_eq!(0, stream.dropped());

        assert!(!sink.emit(ev()), "capacity 1 is now full");
        assert_eq!(1, stream.dropped(), "a full channel is a lost event");

        // Drain, so the next failure can only be attributed to closure.
        stream.try_next().expect("the queued event");
        let dropped_before_close = stream.dropped();

        drop(stream);
        assert!(!sink.emit(ev()), "no receiver, so it cannot be delivered");
        assert!(!sink.emit(ev()), "and again");
        assert_eq!(
            1, dropped_before_close,
            "sanity: exactly one loss happened before the close"
        );
        assert_eq!(
            1,
            sink.outlets[0].dropped.load(Ordering::Relaxed),
            "a closed receiver must not be counted as lost events"
        );
    }
    /// The same sink at both registration levels must not double-deliver.
    ///
    /// `Config::builder().events(sink.clone())` plus `.download_objects().events(sink)` is the
    /// documented way to observe at both levels, and a caller reaches it by accident as easily
    /// as on purpose. `resolve_sink` merges the two, so without deduplication one emit is sent
    /// twice down one channel. `owes_finish` does not catch it: it guarantees a single emit, and
    /// the fan-out is downstream of that -- so the count is exact at the producer and doubled at
    /// the consumer, which is where a `mv --recursive` consumer deletes each source twice.
    #[tokio::test]
    async fn the_same_sink_at_both_levels_delivers_each_event_once() {
        let (sink, mut stream) = channel(std::num::NonZeroUsize::new(16).expect("capacity > 0"));
        let resolved =
            resolve_sink(Some(&sink), Some(sink.clone())).expect("both levels registered");

        resolved.emit(TransferEvent::Ended(Ended {
            id: tid(7, Some(1)),
            transfer: TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            decision: Decision::Transfer {
                reason: TransferReason::Forced {},
            },
            outcome: Outcome::Succeeded {},
        }));
        drop(resolved);
        drop(sink);

        let mut settled = 0;
        while let Some(ev) = stream.next().await {
            if matches!(ev, TransferEvent::Ended(_)) {
                settled += 1;
            }
        }
        assert_eq!(
            1, settled,
            "one terminal must reach one stream once; a second delivery is a second delete"
        );
        assert_eq!(
            0,
            stream.dropped(),
            "nothing was lost, so nothing is counted"
        );
    }

    /// Two genuinely different consumers both receive, which is what `merge` is for.
    ///
    /// The other half of the deduplication contract: the check is keyed on the channel, so it
    /// must not collapse two distinct streams into one.
    #[tokio::test]
    async fn merging_two_distinct_sinks_delivers_to_both() {
        let (a, mut sa) = channel(std::num::NonZeroUsize::new(8).expect("capacity > 0"));
        let (b, mut sb) = channel(std::num::NonZeroUsize::new(8).expect("capacity > 0"));
        let merged = a.merge(b);

        merged.emit(TransferEvent::Ended(Ended {
            id: tid(9, None),
            transfer: TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            decision: Decision::Transfer {
                reason: TransferReason::Forced {},
            },
            outcome: Outcome::Succeeded {},
        }));
        drop(merged);

        let count = |s: &mut TransferEventStream| {
            let mut n = 0;
            while s.try_next().is_ok() {
                n += 1;
            }
            n
        };
        assert_eq!(1, count(&mut sa), "the first consumer receives");
        assert_eq!(1, count(&mut sb), "the second consumer receives");
    }
}
