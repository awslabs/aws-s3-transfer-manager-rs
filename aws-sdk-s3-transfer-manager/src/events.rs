/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-entry transfer events on a bounded channel the caller drains.
//!
//! An *entry* is one thing a run acts on: an object uploaded, an object
//! downloaded, the root of a directory operation. Every entry produces one
//! [`TransferEvent::Started`] when its transfer starts and one
//! [`TransferEvent::Finished`] when that transfer ends.
//!
//! **Every event stands alone.** Delivery is bounded and lossy, so no event may
//! depend on the caller having seen an earlier one: [`TransferRef`] and
//! [`TransferId`] appear on both variants, and a consumer needs no side map.
//!
//! **Lifecycle is pushed; quantities are pulled.** No event carries a byte count or an
//! object count. Each [`TransferEvent::Started`] instead hands over a
//! [`TransferView`](crate::types::TransferView) — a read-only handle you keep and read
//! whenever you want to repaint.
//!
//! **At most one `Finished` arrives per `Started`**, however the entry ended. Because delivery
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
//! use aws_sdk_s3_transfer_manager::events::{Direction, Outcome, TransferEvent};
//!
//! /// `upload: <src> to <dest>` or `download: <src> to <dest>`.
//! fn line(event: &TransferEvent) -> Option<String> {
//!     let transfer = event.transfer();
//!     let verb = match transfer.direction() {
//!         Direction::Upload => "upload",
//!         Direction::Download => "download",
//!         _ => "transfer",
//!     };
//!     let location = format!("{} to {}", transfer.source(), transfer.destination());
//!     Some(match event {
//!         // The transfer has only started: the line prints at `Finished`.
//!         TransferEvent::Started(_) => return None,
//!         TransferEvent::Finished(finished) => match finished.outcome() {
//!             // `Outcome`'s variants are `#[non_exhaustive]`, so external code matches
//!             // them with `{ .. }`, and the `_` arm admits later variants.
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
//! use aws_sdk_s3_transfer_manager::events::{TransferEvent, TransferEventStream, TryRecvError};
//! use aws_sdk_s3_transfer_manager::types::{Total, TransferView};
//!
//! /// Drain whatever has arrived, then draw. Called on the caller's own clock — every
//! /// 100 ms, on a keypress, whenever suits — not once per event.
//! fn repaint(stream: &mut TransferEventStream, root: &mut Option<TransferView>) -> Option<String> {
//!     loop {
//!         match stream.try_recv() {
//!             // `Started` for the root carries the view the bar is drawn from.
//!             Ok(TransferEvent::Started(p)) if p.id().parent().is_none() => {
//!                 *root = p.view().cloned();
//!             }
//!             Ok(_) => continue,
//!             // Nothing new. The view is still live, so draw from what is already held.
//!             Err(TryRecvError::Empty) => break,
//!             // Every sink is gone: this is the last frame.
//!             Err(TryRecvError::Disconnected) => break,
//!             // `TryRecvError` is `#[non_exhaustive]`, so external code needs this arm.
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
pub use crate::transfer::TransferId;

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
    /// `(unresolved)`. This is the rendering the `upload:` / `download:` lines use, so no
    /// consumer writes a formatter of its own.
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
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Where the entry is read from.
    pub fn source(&self) -> &Endpoint {
        &self.source
    }

    /// Where the entry is written to.
    pub fn destination(&self) -> &Endpoint {
        &self.destination
    }
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
/// An entry whose [`TransferId::parent`] is `None` is the operation the caller invoked. For
/// [`upload`](crate::Client::upload) or [`download`](crate::Client::download) that entry is
/// the file itself; for [`upload_objects`](crate::Client::upload_objects) or
/// [`download_objects`](crate::Client::download_objects) it is the directory, whose endpoints
/// are a source directory and a key prefix, and every object under it has the directory's
/// id as its parent.
///
/// A consumer acting per file should therefore skip the root event for the directory
/// operations only — skipping it always drops every single-file transfer.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransferEvent {
    /// The entry's transfer started. Exactly one per entry.
    Started(Started),
    /// The entry's transfer reached a terminal state. At most one per
    /// [`TransferEvent::Started`].
    Finished(Finished),
}

/// One entry's transfer started.
#[derive(Debug, Clone)]
pub struct Started {
    id: TransferId,
    transfer: TransferRef,
    view: Option<crate::types::TransferView>,
}

impl Started {
    /// This entry's id.
    pub fn id(&self) -> TransferId {
        self.id
    }

    /// The entry, and where its two ends are.
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
    }

    /// Read-only view of this entry's counters, for as long as you keep it.
    ///
    /// `None` when no counters can be read for this entry. Distinct from a live transfer
    /// sitting at zero bytes.
    pub fn view(&self) -> Option<&crate::types::TransferView> {
        self.view.as_ref()
    }
}

/// One entry's transfer reached a terminal state.
#[derive(Debug, Clone)]
pub struct Finished {
    id: TransferId,
    transfer: TransferRef,
    outcome: Outcome,
}

impl Finished {
    /// This entry's id, equal to its [`Started::id`].
    pub fn id(&self) -> TransferId {
        self.id
    }

    /// The entry. Repeated so no consumer needs a side map.
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
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
            TransferEvent::Started(e) => e.transfer(),
            TransferEvent::Finished(e) => e.transfer(),
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
    /// Merging the same sink twice is a no-op rather than a doubling, so a consumer
    /// registered twice receives each event once.
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
    /// Waits for the next event, or returns `None` once every sink is gone and the queue is
    /// drained.
    pub async fn recv(&mut self) -> Option<TransferEvent> {
        self.rx.recv().await
    }

    /// Returns the next queued event without waiting.
    ///
    /// Fails with [`TryRecvError::Empty`] while no event is queued and a sink still exists,
    /// and with [`TryRecvError::Disconnected`] once every sink is gone and the queue is
    /// drained.
    pub fn try_recv(&mut self) -> Result<TransferEvent, TryRecvError> {
        use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
        self.rx.try_recv().map_err(|e| match e {
            MpscTryRecvError::Empty => TryRecvError::Empty,
            MpscTryRecvError::Disconnected => TryRecvError::Disconnected,
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

/// Why [`TransferEventStream::try_recv`] returned no event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TryRecvError {
    /// No event is queued and a sink still exists, so an event may still arrive.
    Empty,
    /// Every sink is gone and the queue is drained, so no event will arrive.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "no event queued"),
            TryRecvError::Disconnected => write!(f, "event stream closed"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Per-entry emitter, owning the terminal obligation.
///
/// The obligation is deliberately **not** the transfer status. A status CAS says
/// which thread won the transition; this flag says whether a `Finished` is still
/// owed. They come apart in both directions: an entry refused by the failure
/// policy has an obligation and no status, and a transfer that was never announced
/// has a status and no obligation.
pub(crate) struct TransferLifecycle {
    /// The sink, shared with each claimed emit and released by the one that sends.
    ///
    /// Released at that send rather than when the lifecycle drops: the lifecycle lives as
    /// long as any clone of its transfer's context, and a consumer's stream reports
    /// [`TryRecvError::Disconnected`] only once every sink is gone.
    sink: Arc<Mutex<Option<TransferEventSink>>>,
    id: TransferId,
    transfer: TransferRef,
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
            view,
            owes_finish: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Emit [`TransferEvent::Started`] and take on the terminal obligation.
    ///
    /// The obligation is taken even if the send is dropped, so a lost `Started`
    /// cannot silently cancel the matching `Finished`. That keeps the two counts
    /// comparable, which is what makes a dropped event visible.
    pub(crate) fn announce(&self) {
        self.owes_finish.store(true, Ordering::Release);
        if let Some(sink) = &*lock_sink(&self.sink) {
            sink.emit(TransferEvent::Started(Started {
                id: self.id,
                transfer: self.transfer.clone(),
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
            event: Some(TransferEvent::Finished(Finished {
                id: self.id,
                transfer: self.transfer.clone(),
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
    /// channel: `finish` returns `None` forever after, so the `Finished` is lost
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

    /// The doc example's line printer: each line it prints comes from one event, with no
    /// side map and no second lookup.
    ///
    /// Unlike the doc example, which compiles as an external crate, this has no wildcard
    /// [`Direction`] arm: `#[non_exhaustive]` does not apply in the defining crate.
    fn line(event: &TransferEvent) -> Option<String> {
        let transfer = event.transfer();
        let verb = match transfer.direction() {
            Direction::Upload => "upload",
            Direction::Download => "download",
        };
        let location = format!("{} to {}", transfer.source(), transfer.destination());
        Some(match event {
            TransferEvent::Started(_) => return None,
            TransferEvent::Finished(finished) => match finished.outcome() {
                Outcome::Failed { .. } => format!("{verb} failed: {location}"),
                Outcome::Cancelled { .. } => return None,
                _ => format!("{verb}: {location}"),
            },
        })
    }

    fn finished(transfer: TransferRef, outcome: Outcome) -> TransferEvent {
        TransferEvent::Finished(Finished {
            id: tid(1, Some(0)),
            transfer,
            outcome,
        })
    }

    #[test]
    fn every_printed_line_comes_from_one_event() {
        let ok = Outcome::Succeeded {};
        assert_eq!(
            line(&finished(upload_ref(), ok.clone())).as_deref(),
            Some("upload: ./a.txt to s3://bucket/k/a.txt"),
            "an upload's line must be printable from the event alone"
        );

        let down = TransferRef::download(s3("bucket", "k/a.txt"), local("./a.txt"));
        assert_eq!(
            line(&finished(down, ok)).as_deref(),
            Some("download: s3://bucket/k/a.txt to ./a.txt"),
            "a download's line must name the S3 side as the source"
        );

        // The entry a failure names comes from the event itself, so a consumer never
        // has to pair a failure with a separate lookup to know which entry broke. The
        // cause is not on the event -- it is read from the handle's `join()`.
        assert_eq!(
            line(&finished(upload_ref(), failed())).as_deref(),
            Some("upload failed: ./a.txt to s3://bucket/k/a.txt"),
            "a failure must name its entry from the same event"
        );

        let started = TransferEvent::Started(Started {
            id: tid(1, Some(0)),
            transfer: upload_ref(),
            view: None,
        });
        assert_eq!(
            line(&started),
            None,
            "the line prints when the transfer finishes, not when it starts"
        );
    }

    #[test]
    fn both_events_carry_the_entry_and_its_id() {
        let (lc, mut stream) = lifecycle();
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        for expected_outcome in [false, true] {
            let ev = stream.try_recv().expect("both events reached the channel");
            assert_eq!(
                matches!(ev, TransferEvent::Finished(_)),
                expected_outcome,
                "the start arrives before the terminal: {ev:?}"
            );
            let id = match &ev {
                TransferEvent::Started(started) => started.id(),
                TransferEvent::Finished(finished) => finished.id(),
            };
            assert_eq!(
                tid(1, None),
                id,
                "delivery is lossy, so the terminal must carry the start's id, or a \
                 consumer that missed the first event cannot tell which entry ended: {ev:?}"
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
             by a value that never reached the channel, and the `Finished` is lost \
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
        // Capacity 1: the `Started` fits, the `Finished` does not. Nothing is
        // reserved, so the loss is at the end rather than the beginning -- and it is
        // counted, which is the whole of what bounded delivery promises.
        let (sink, mut stream) = channel(NonZeroUsize::new(1).unwrap());
        let lc = TransferLifecycle::new(sink, tid(7, None), upload_ref(), None);
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        let ev = stream.try_recv().expect("the start reached the channel");
        assert!(
            matches!(&ev, TransferEvent::Started(p) if p.id() == tid(7, None)),
            "the first event through an empty channel is the start: {ev:?}"
        );
        assert_eq!(
            stream.dropped(),
            1,
            "an event with no room is discarded and counted, and the run does not \
             slow down waiting for the consumer"
        );
        assert!(
            matches!(stream.try_recv(), Err(TryRecvError::Disconnected)),
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
            TransferEvent::Started(Started {
                id: tid(1, None),
                transfer: upload_ref(),
                view: None,
            })
        };

        assert!(sink.emit(ev()), "the first event fits");
        assert_eq!(0, stream.dropped());

        assert!(!sink.emit(ev()), "capacity 1 is now full");
        assert_eq!(1, stream.dropped(), "a full channel is a lost event");

        // Drain, so the next failure can only be attributed to closure.
        stream.try_recv().expect("the queued event");
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

    /// One sink registered twice on a request must not double-deliver.
    ///
    /// A request's `events()` merges each sink it is given, so registering the same sink
    /// twice merges it with itself. `owes_finish` guarantees one emit and the fan-out is
    /// downstream of it, so without deduplication the consumer receives that emit twice.
    #[tokio::test]
    async fn merging_a_sink_with_itself_delivers_each_event_once() {
        let (sink, mut stream) = channel(std::num::NonZeroUsize::new(16).expect("capacity > 0"));
        let registered = sink.clone().merge(sink);

        registered.emit(TransferEvent::Finished(Finished {
            id: tid(7, Some(1)),
            transfer: TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            outcome: Outcome::Succeeded {},
        }));
        drop(registered);

        let mut settled = 0;
        while let Some(ev) = stream.recv().await {
            if matches!(ev, TransferEvent::Finished(_)) {
                settled += 1;
            }
        }
        assert_eq!(1, settled, "one terminal must reach one stream once");
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

        merged.emit(TransferEvent::Finished(Finished {
            id: tid(9, None),
            transfer: TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            outcome: Outcome::Succeeded {},
        }));
        drop(merged);

        let count = |s: &mut TransferEventStream| {
            let mut n = 0;
            while s.try_recv().is_ok() {
                n += 1;
            }
            n
        };
        assert_eq!(1, count(&mut sa), "the first consumer receives");
        assert_eq!(1, count(&mut sb), "the second consumer receives");
    }
}
