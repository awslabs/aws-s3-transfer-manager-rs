/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-entry transfer events on a bounded channel the caller drains.
//!
//! An *entry* is one thing a run acts on: an object uploaded, an object
//! downloaded, the root of a directory operation. Every entry produces one
//! [`TransferEvent::Decided`] the moment its action is chosen, and — only if that
//! action was attempted — one [`TransferEvent::Settled`] when the attempt ends. A
//! skip attempts nothing, so its `Decided` is its terminal; a run that decides
//! without attempting produces decisions only, which is the absence of the second
//! event rather than a flag on the first.
//!
//! **Every event stands alone.** Delivery is bounded and lossy, so no event may
//! depend on the caller having seen an earlier one: [`TransferRef`] and
//! [`Decision`] appear on both variants, and a consumer needs no side map.
//!
//! **Lifecycle is pushed; quantities are pulled.** No event carries a byte count or an
//! object count. Each [`TransferEvent::Decided`] instead hands over a
//! [`TransferView`](crate::types::TransferView) — a read-only handle you keep and read
//! whenever you want to repaint. Nothing pushes a number at you.
//!
//! Both halves of that split are forced. Pushing counts would mean running caller code on
//! the thread that records them, which holds a scheduler dispatch ticket and one of the
//! fixed per-core threads, so it would stall every transfer in the client. And any total
//! *summed from* a lossy stream is a lower bound that silently disagrees with the
//! operation's own result, where a counter read off a view is the number the operation
//! reports — which is why `entries_settled()` exists rather than leaving a consumer to tally
//! `Settled` events itself.
//!
//! **Two invariants carry the transport.**
//!
//! *At most one `Settled` per `Decided`.* Announcing an entry takes an obligation;
//! whichever of the four terminal sites claims it first wins the swap and the rest
//! are no-ops. The status CAS answers a different question — who *transitioned* —
//! and the two come apart, because the transitioner is often the scheduler, which
//! holds no sink.
//!
//! *No emit blocks, awaits, or runs caller code.* Every emit is a value rather than
//! a send, claimed under a state guard and published after it is released. Nothing is reserved and no event is owed: an emit that
//! finds a full channel is dropped and counted by [`TransferEventStream::dropped`].
//!
//! Printing one of the familiar per-entry lines takes one event and nothing else:
//!
//! ```
//! use aws_sdk_s3_transfer_manager::events::{Decision, Direction, Outcome, TransferEvent};
//!
//! /// `upload: <src> to <dest>` / `download:` / `copy:` / `delete: <dest>`.
//! ///
//! /// `dry_run` is the caller's own fact, not the event's: a run that attempts
//! /// nothing produces the same `Decided` a run that attempts everything does, so
//! /// the events carry no mode flag for whoever chose the mode to read back.
//! fn line(event: &TransferEvent, dry_run: bool) -> Option<String> {
//!     let transfer = event.transfer();
//!     // Every pattern takes `{ .. }`: that is what per-variant `#[non_exhaustive]`
//!     // costs an external consumer, and what keeps a later field additive.
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
//!     Some(match event.outcome() {
//!         None if dry_run => format!("(dryrun) {verb}: {location}"),
//!         // Decided, and the attempt is still to come: the line prints at `Settled`.
//!         None => return None,
//!         Some(Outcome::Failed { error, .. }) => format!("{verb} failed: {location} {error}"),
//!         Some(Outcome::Cancelled { .. }) => return None,
//!         Some(_) => format!("{verb}: {location}"),
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
//! use aws_sdk_s3_transfer_manager::types::{ByteTotal, EntryTotal, TransferView};
//!
//! /// Drain whatever has arrived, then draw. Called on the caller's own clock — every
//! /// 100 ms, on a keypress, whenever suits — not once per event.
//! fn repaint(stream: &mut TransferEventStream, root: &mut Option<TransferView>) -> Option<String> {
//!     loop {
//!         match stream.try_next() {
//!             // `Decided` for the root carries the view the bar is drawn from.
//!             Ok(TransferEvent::Decided { parent: None, view, .. }) => *root = view,
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
//!         ByteTotal::Final(total) if total > 0 => {
//!             format!("{:.1}%", (done as f64 / total as f64) * 100.0)
//!         }
//!         // Still enumerating: the denominator can grow, so a bar drawn on it would
//!         // walk backwards. Show bytes instead.
//!         ByteTotal::Provisional(total) => format!("{done} of {total}+ bytes"),
//!         _ => format!("{done} bytes"),
//!     };
//!
//!     // The other half of a progress line, and the half bytes cannot supply: how many
//!     // entries are left. Counts endings, so it reaches zero even when entries fail --
//!     // where the byte bar above stops short by whatever never moved.
//!     let settled = view.entries_settled();
//!     let files = match view.entry_total() {
//!         EntryTotal::Final(total) => format!("{} file(s) remaining", total - settled),
//!         // `~` because enumeration can still raise the total.
//!         EntryTotal::Provisional(total) => {
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
use std::sync::Arc;

// The loom compat layer for the atomics, not `std::sync::atomic` directly: the
// terminal obligation is a two-thread protocol (a claim races every other terminal
// site) and this is what makes those two operations model-checkable. Pointers stay
// `std::sync::Arc` — they appear in the public types, which must not change shape
// under a test cfg.
use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::Error;

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
    ///
    /// Hidden for the same reason as [`SkipReason`]: nothing constructs it —
    /// [`TransferRef`] builds only uploads and downloads — so the name has not been
    /// exercised by the layer that will populate it. It becomes visible with `CopyObject`.
    #[doc(hidden)]
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
        path: Arc<Path>,
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
    /// `s3://bucket/key`, the path (lossy for a non-UTF-8 name), `-` for a
    /// caller-owned stream, or `(unresolved)`. This is the rendering the
    /// `upload:` / `download:` / `copy:` / `delete:` lines use, so no consumer
    /// writes a formatter of its own.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::S3 { bucket, key } => write!(f, "s3://{bucket}/{key}"),
            Endpoint::Local { path } => write!(f, "{}", path.display()),
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
/// Hidden while the only producer of eleven of the twelve reasons lives outside this
/// crate: comparison decides them, and this crate emits `Forced {}` alone. Hiding
/// keeps every name renameable, and hidden→public is additive where the reverse is
/// breaking. Usable by name regardless of the attribute.
#[doc(hidden)]
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
/// Hidden for the same reason as [`TransferReason`]: no in-crate producer, so no
/// name here has been exercised by the layer that will populate it.
#[doc(hidden)]
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
    SourceUnknown {
        /// What stopped the entry from being classified. Cloning it is a refcount
        /// bump, so one unreadable directory that makes every key under its prefix
        /// unknown costs one allocation rather than one per event.
        cause: Error,
    },
    /// The destination side could not be seen here, so nothing may be written:
    /// treating an unknown destination as absent skips the comparison and can
    /// overwrite a newer entry.
    #[non_exhaustive]
    DestinationUnknown {
        /// What stopped the entry from being classified.
        cause: Error,
    },
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
    ///
    /// Hidden until `sync` produces it: nothing in the crate decides a delete today.
    #[doc(hidden)]
    #[non_exhaustive]
    Delete {},
    /// Leave the entry alone. Nothing is attempted, so this decision is terminal on
    /// arrival and no [`TransferEvent::Settled`] follows it.
    ///
    /// Hidden until `sync` produces it, which is also why its [`SkipReason`] payload is
    /// hidden — a visible variant carrying a hidden type is the inconsistency this removes.
    #[doc(hidden)]
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
    /// The action failed.
    #[non_exhaustive]
    Failed {
        /// What went wrong. Cloned from the transfer's own error — a refcount bump,
        /// since [`Error`] shares its source internally — so reading it here does
        /// not consume it for `join()`.
        error: Error,
    },
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
/// `parent` says which operation an event belongs to. `None` means *this event is the
/// operation you called* — so what that implies depends on which one you called, and reading
/// it as "not a real entry" is wrong.
///
/// For [`upload`](crate::Client::upload) or [`download`](crate::Client::download), the single
/// event with `parent: None` **is** the file. For
/// [`upload_objects`](crate::Client::upload_objects) or
/// [`download_objects`](crate::Client::download_objects), it is the root — its endpoints are
/// the source directory and the key prefix, not a file and a key — and every child carries
/// `parent: Some(root_id)`.
///
/// So a consumer acting per entry — deleting each source once its upload succeeds — skips the
/// `parent: None` event **only for the directory operations**, where acting on it would act on
/// the whole tree. Skipping it unconditionally is wrong: it drops every single-file transfer,
/// whose sole event is the one with no parent.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransferEvent {
    /// An action was decided for the entry. Exactly one per entry.
    #[non_exhaustive]
    Decided {
        /// Opaque entry id, unique within the client.
        id: u64,
        /// The id of the directory operation this entry belongs to, or `None` when
        /// this event *is* that operation.
        parent: Option<u64>,
        /// The entry, and where its two ends are.
        transfer: TransferRef,
        /// What was decided, and why.
        decision: Decision,
        /// Read-only view of this entry's byte counters, for as long as you keep it.
        ///
        /// `None` when the entry never became a transfer — a skip, or an entry the
        /// operation abandoned before it could be started — so there are no counters
        /// to read. Distinct from a live transfer sitting at zero bytes.
        view: Option<crate::types::TransferView>,
    },
    /// An attempted action reached a terminal state. At most one per
    /// [`TransferEvent::Decided`], and none at all for a decision that attempts
    /// nothing.
    #[non_exhaustive]
    Settled {
        /// Opaque entry id, matching the `Decided` this settles.
        id: u64,
        /// The id of the directory operation this entry belongs to, or `None` when
        /// this event *is* that operation.
        parent: Option<u64>,
        /// The entry. Repeated so no consumer needs a side map.
        transfer: TransferRef,
        /// Repeated for the same reason: a lost `Decided` must not cost the caller
        /// the verb it prints or the reason it reports.
        decision: Decision,
        /// How it ended.
        outcome: Outcome,
    },
}

impl TransferEvent {
    /// The entry this event is about. Present on every variant.
    pub fn transfer(&self) -> &TransferRef {
        match self {
            TransferEvent::Decided { transfer, .. } | TransferEvent::Settled { transfer, .. } => {
                transfer
            }
        }
    }

    /// What was decided for the entry, and why. Present on every variant.
    pub fn decision(&self) -> &Decision {
        match self {
            TransferEvent::Decided { decision, .. } | TransferEvent::Settled { decision, .. } => {
                decision
            }
        }
    }

    /// How the attempt ended, or `None` if this event is the decision itself.
    pub fn outcome(&self) -> Option<&Outcome> {
        match self {
            TransferEvent::Decided { .. } => None,
            TransferEvent::Settled { outcome, .. } => Some(outcome),
        }
    }
}

/// Create a sink/stream pair. The caller owns the stream and drains it.
///
/// # Capacity
///
/// Bounded and lossy: a caller that reads slowly misses events and the run does not
/// slow down waiting. `capacity` is therefore a pure lossiness dial — nothing is
/// reserved and no event is owed — so size it against how far behind the consumer
/// may fall, not against the transfer manager's concurrency, and read
/// [`TransferEventStream::dropped`] for what it cost.
///
/// A stream that is never drained is the same case: undrained slots are occupied
/// slots, and every event after the first `capacity` of them is counted and
/// discarded.
pub fn channel(capacity: NonZeroUsize) -> (TransferEventSink, TransferEventStream) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity.get());
    let dropped = Arc::new(AtomicU64::new(0));
    (
        TransferEventSink {
            tx,
            dropped: dropped.clone(),
        },
        TransferEventStream { rx, dropped },
    )
}

/// Registration endpoint. Cheap to clone; every clone feeds one stream.
#[derive(Debug, Clone)]
pub struct TransferEventSink {
    tx: tokio::sync::mpsc::Sender<TransferEvent>,
    dropped: Arc<AtomicU64>,
}

impl TransferEventSink {
    /// Non-blocking send. Counts the event as dropped if the channel was full.
    ///
    /// Returns whether the event reached the channel. A `false` return covers two
    /// different facts, and only the first is counted in [`TransferEventStream::dropped`]:
    /// the channel was full (the consumer lost an event), or the receiver is gone (there
    /// is no consumer to lose anything). Either way the producer does not stall.
    fn emit(&self, event: TransferEvent) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        match self.tx.try_send(event) {
            Ok(()) => true,
            // Counted: the consumer is still there and lost this one, which is what
            // `dropped()` reports.
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
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
/// which thread won the transition; this flag says whether a `Settled` is still
/// owed. They come apart in both directions: an entry refused by the failure
/// policy has an obligation and no status, and a transfer that was never announced
/// has a status and no obligation.
pub(crate) struct TransferLifecycle {
    sink: TransferEventSink,
    id: u64,
    parent: Option<u64>,
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
            .field("parent", &self.parent)
            .field("transfer", &self.transfer)
            .field("owes_finish", &self.owes_finish.load(Ordering::Relaxed))
            .finish()
    }
}

impl TransferLifecycle {
    /// A clone of the sink, so a parent can build emitters for its children
    /// without the caller also holding a sink.
    pub(crate) fn child_sink(&self) -> TransferEventSink {
        self.sink.clone()
    }

    pub(crate) fn new(
        sink: TransferEventSink,
        id: u64,
        parent: Option<u64>,
        transfer: TransferRef,
        view: Option<crate::types::TransferView>,
    ) -> Self {
        Self {
            sink,
            id,
            parent,
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
    /// The obligation is taken even if the send is dropped, so a lost `Decided`
    /// cannot silently cancel the matching `Settled`. That keeps the two counts
    /// comparable, which is what makes a dropped event visible.
    ///
    /// A [`Decision::Skip`] takes no obligation, which is what enforces that variant's
    /// "no `Settled` follows it": `finish` cannot answer for a lifecycle that never owed a
    /// terminal, so no shared terminal path can emit one for an entry nothing was attempted
    /// on. That matters to a consumer keying per-entry state off `Decided` and freeing it on
    /// `Settled`: a terminal for an entry that was never attempted is state it never
    /// allocated, and the decision alone is what tells the two shapes apart.
    /// Currently unreachable — [`new`](Self::new) constructs only `Transfer`.
    pub(crate) fn announce(&self) {
        if !matches!(self.decision, Decision::Skip { .. }) {
            self.owes_finish.store(true, Ordering::Release);
        }
        self.sink.emit(TransferEvent::Decided {
            id: self.id,
            parent: self.parent,
            transfer: self.transfer.clone(),
            decision: self.decision.clone(),
            view: self.view.clone(),
        });
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
            event: Some(TransferEvent::Settled {
                id: self.id,
                parent: self.parent,
                transfer: self.transfer.clone(),
                decision: self.decision.clone(),
                outcome,
            }),
        })
    }
}

/// A claimed terminal emit that has not been sent yet.
///
/// Claiming and sending are separate so a site holding a state guard can claim
/// under the guard and publish after releasing it.
pub(crate) struct PendingEmit {
    sink: TransferEventSink,
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
    pub(crate) fn send(mut self) {
        let Some(event) = self.event.take() else {
            return;
        };
        self.sink.emit(event);
    }
}

impl Drop for PendingEmit {
    /// Give the obligation back if this emit was never sent.
    ///
    /// Without this, an early `return` or a `?` on a path that has already
    /// claimed consumes the obligation with a value that never reached the
    /// channel: `finish` returns `None` forever after, so the `Settled` is lost
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
            path: Arc::from(std::path::Path::new(path)),
        }
    }

    fn upload_ref() -> TransferRef {
        TransferRef::upload(local("./a.txt"), s3("bucket", "k/a.txt"))
    }

    fn lifecycle() -> (TransferLifecycle, TransferEventStream) {
        let (sink, stream) = channel(NonZeroUsize::new(8).unwrap());
        (
            TransferLifecycle::new(sink, 1, None, upload_ref(), None),
            stream,
        )
    }

    fn boom() -> Error {
        Error::new(crate::error::ErrorKind::ChildOperationFailed, "boom")
    }

    fn failed() -> Outcome {
        Outcome::Failed { error: boom() }
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
        Some(match event.outcome() {
            None if dry_run => format!("(dryrun) {verb}: {location}"),
            None => return None,
            Some(Outcome::Failed { error, .. }) => format!("{verb} failed: {location} {error}"),
            Some(Outcome::Cancelled { .. }) => return None,
            Some(_) => format!("{verb}: {location}"),
        })
    }

    fn settled(transfer: TransferRef, decision: Decision, outcome: Outcome) -> TransferEvent {
        TransferEvent::Settled {
            id: 1,
            parent: Some(0),
            transfer,
            decision,
            outcome,
        }
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
            line(&settled(upload_ref(), forced(), ok.clone()), false).as_deref(),
            Some("upload: ./a.txt to s3://bucket/k/a.txt"),
            "an upload's line must be printable from the event alone"
        );

        let down = TransferRef::download(s3("bucket", "k/a.txt"), local("./a.txt"));
        assert_eq!(
            line(&settled(down, forced(), ok.clone()), false).as_deref(),
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
            line(&settled(copy, forced(), ok.clone()), false).as_deref(),
            Some("copy: s3://src/k to s3://dst/k"),
            "a copy's line must name both S3 ends"
        );

        let del = TransferRef {
            direction: Direction::Upload,
            source: Endpoint::Unresolved {},
            destination: s3("dst", "gone"),
        };
        assert_eq!(
            line(&settled(del.clone(), Decision::Delete {}, ok), false).as_deref(),
            Some("delete: s3://dst/gone"),
            "a delete's line must name only the end it removes"
        );

        // `Error`'s own `Display` is its kind, not its cause -- a caller that wants
        // the chain wraps it in `DisplayErrorContext` -- so what the line carries is
        // whatever the error prints, from the same event as the entry.
        assert_eq!(
            line(&settled(upload_ref(), forced(), failed()), false).as_deref(),
            Some("upload failed: ./a.txt to s3://bucket/k/a.txt child operation failed"),
            "a failure must print the entry and the error from the same event"
        );

        // A dry run's only event is the decision, and whether the run is dry is the
        // caller's own fact, which is why nothing on the event says so.
        let decided = TransferEvent::Decided {
            id: 1,
            parent: Some(0),
            transfer: upload_ref(),
            decision: forced(),
            view: None,
        };
        assert_eq!(
            line(&decided, true).as_deref(),
            Some("(dryrun) upload: ./a.txt to s3://bucket/k/a.txt"),
            "a decision must print the same line without an outcome"
        );
        assert_eq!(
            line(&decided, false),
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
            let event = settled(transfer, decision, ok.clone());
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
            SkipReason::SourceUnknown { cause: boom() },
            SkipReason::DestinationUnknown { cause: boom() },
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
                ev.outcome().is_some(),
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
             by a value that never reached the channel, and the `Settled` is lost \
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
        let lc = TransferLifecycle::new(sink, 7, None, upload_ref(), None);
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        let ev = stream.try_next().expect("the decision reached the channel");
        assert!(
            matches!(ev, TransferEvent::Decided { id: 7, .. }),
            "the first event through an empty channel is the decision: {ev:?}"
        );
        assert_eq!(
            stream.dropped(),
            1,
            "an event with no room is discarded and counted, and the run does not \
             slow down waiting for the consumer"
        );
        assert!(
            matches!(stream.try_next(), Err(TryNextError::Empty)),
            "a dropped event must not be delivered late"
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
        let ev = || TransferEvent::Decided {
            id: 1,
            parent: None,
            transfer: upload_ref(),
            decision: Decision::Transfer {
                reason: TransferReason::Forced {},
            },
            view: None,
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
            sink.dropped.load(Ordering::Relaxed),
            "a closed receiver must not be counted as lost events"
        );
    }
}
