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
//! transfer's own thread, and a transfer that finds no room for its events is left off the
//! stream, with its events counted by [`TransferEventStream::dropped`], rather than made to
//! wait on you.
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
/// A transfer never waits for room on the stream. One that starts with at least two slots
/// free holds room for both of its events until the consumer receives them; one that starts
/// with fewer is left off the stream, and [`TransferEventStream::dropped`] counts both of its
/// events. A sink used by one transfer never drops an event at capacity 2. A stream fed by
/// several transfers, through clones or [`merge`](TransferEventSink::merge), drops nothing
/// while its capacity is at least twice the number of transfers that are running or whose
/// events the consumer has not yet received. A capacity below 2 is raised to 2.
///
/// Capacities above [`MAX_CAPACITY`] are clamped to it rather than panicking.
pub fn channel(capacity: NonZeroUsize) -> (TransferEventSink, TransferEventStream) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity.get().clamp(2, MAX_CAPACITY));
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

/// A slot on one outlet, reserved before the event that fills it.
type Permit = tokio::sync::mpsc::OwnedPermit<TransferEvent>;

impl Outlet {
    /// Reserves two slots, or none.
    ///
    /// Counts two dropped events when the channel has room for fewer than two, and nothing
    /// when the receiver is gone.
    fn reserve_pair(&self) -> Option<(Permit, Permit)> {
        use tokio::sync::mpsc::error::TrySendError;
        let refused = match self.tx.clone().try_reserve_owned() {
            Ok(first) => match self.tx.clone().try_reserve_owned() {
                Ok(second) => return Some((first, second)),
                Err(refused) => {
                    // Released, not kept: a lone slot would deliver a `Started` with no room
                    // for its `Finished`, and would hold room a later transfer could use.
                    drop(first);
                    refused
                }
            },
            Err(refused) => refused,
        };
        // A gone receiver is not counted: there is no consumer left to lose the events.
        if let TrySendError::Full(_) = refused {
            self.dropped.fetch_add(2, Ordering::Relaxed);
        }
        None
    }
}

/// Sends `event` through every permit.
fn deliver(permits: Vec<Permit>, event: TransferEvent) {
    let mut permits = permits.into_iter();
    let Some(mut permit) = permits.next() else {
        return;
    };
    // The last permit takes `event` by move, so a single-outlet sink clones nothing.
    for next in permits {
        permit.send(event.clone());
        permit = next;
    }
    permit.send(event);
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
                // fan-out to each outlet is downstream of it, so a duplicate outlet breaks
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

    /// Reserves two slots on every outlet with room for both, split into the slots for a
    /// `Started` and the slots for its `Finished`.
    fn reserve_pairs(&self) -> (Vec<Permit>, Vec<Permit>) {
        self.outlets.iter().filter_map(Outlet::reserve_pair).unzip()
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

    /// Events the consumer lost for lack of room, two for each transfer that found no room
    /// for both of its events.
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
    /// The sink and the slots held for the `Finished`, shared with each claimed emit and
    /// released by the one that sends.
    ///
    /// Released at that send rather than when the lifecycle drops: the lifecycle lives as
    /// long as any clone of its transfer's context, and a consumer's stream reports
    /// [`TryRecvError::Disconnected`] only once every sink, and every slot held on it, is
    /// gone.
    route: Arc<Mutex<Option<Route>>>,
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

/// Where a lifecycle's events go.
#[derive(Debug)]
struct Route {
    /// Kept until the `Finished` is sent: an outlet that had no room holds no slot, and its
    /// stream must not end while the transfer is still running.
    sink: TransferEventSink,
    /// A slot for the `Finished` on each outlet that had room for both events at `announce`.
    finish: Vec<Permit>,
}

/// Lock the shared route slot, tolerating poison.
///
/// A terminal path runs during a worker panic, so an unwind that poisoned this lock must
/// not take the terminal event with it. The slot holds a usable route or `None`, and an
/// unwind cannot leave it in any third state, so the inner value is always safe to take.
fn lock_route(slot: &Mutex<Option<Route>>) -> std::sync::MutexGuard<'_, Option<Route>> {
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
            route: Arc::new(Mutex::new(Some(Route {
                sink,
                finish: Vec::new(),
            }))),
            id,
            transfer,
            view,
            owes_finish: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Emit [`TransferEvent::Started`] and take on the terminal obligation.
    ///
    /// Each outlet with room for both events receives the `Started` and holds a slot for the
    /// `Finished`. Each outlet without that room receives neither and counts both as dropped,
    /// unless its receiver is gone.
    ///
    /// The obligation is taken even where no outlet had room, because discharging it is what
    /// releases the sink.
    pub(crate) fn announce(&self) {
        let mut slot = lock_route(&self.route);
        // Under the lock, so a terminal claimed from here on sends only after the slots it
        // sends through are reserved and the `Started` is queued ahead of it.
        self.owes_finish.store(true, Ordering::Release);
        let Some(route) = slot.as_mut() else {
            return;
        };
        let (start, finish) = route.sink.reserve_pairs();
        route.finish = finish;
        deliver(
            start,
            TransferEvent::Started(Started {
                id: self.id,
                transfer: self.transfer.clone(),
                view: self.view.clone(),
            }),
        );
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
            route: self.route.clone(),
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
    /// The lifecycle's own route, not a copy: sending is also what releases it, and a claim
    /// dropped unsent leaves the held slots there for the next claim.
    route: Arc<Mutex<Option<Route>>>,
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
    /// The event goes through the slots `announce` held for it, so a full channel cannot
    /// drop it. An outlet that had no room at `announce` receives nothing.
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
        let mut slot = lock_route(&self.route);
        if let Some(route) = slot.take() {
            deliver(route.finish, event);
        }
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

    /// A lifecycle on `sink` for transfer `id`, already announced.
    fn announced(sink: &TransferEventSink, id: u64) -> TransferLifecycle {
        let lc = TransferLifecycle::new(sink.clone(), tid(id, None), upload_ref(), None);
        lc.announce();
        lc
    }

    /// Each queued event as its variant and transfer id, in arrival order.
    fn drain_ids(stream: &mut TransferEventStream) -> Vec<(&'static str, TransferId)> {
        let mut seen = Vec::new();
        while let Ok(ev) = stream.try_recv() {
            seen.push(match ev {
                TransferEvent::Started(started) => ("Started", started.id()),
                TransferEvent::Finished(finished) => ("Finished", finished.id()),
            });
        }
        seen
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
                "the terminal must carry the start's id, or a consumer of several transfers \
                 cannot tell which one ended: {ev:?}"
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
        let (lc, mut stream) = lifecycle();
        lc.announce();
        // A site that claims and then returns early without publishing.
        drop(lc.finish(failed()));
        lc.finish(failed())
            .expect(
                "an abandoned claim must re-arm: otherwise the obligation is consumed \
                 by a value that never reached the channel, and the `Finished` is lost \
                 with nothing recording that it was owed",
            )
            .send();
        assert_eq!(
            vec![("Started", tid(1, None)), ("Finished", tid(1, None))],
            drain_ids(&mut stream),
            "the abandoned claim must leave the slot `announce` held for the `Finished`, \
             so the re-armed claim still has room to send through"
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

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A capacity below 2 is raised to 2, so a transfer alone on its sink delivers both of
    /// its events.
    ///
    /// Capacity 1 could never hold a pair: every transfer would be granted one slot, refused
    /// the second, and left off the stream.
    #[test]
    fn a_capacity_of_one_still_delivers_a_whole_transfer() {
        let (sink, mut stream) = channel(NonZeroUsize::new(1).unwrap());
        let lc = TransferLifecycle::new(sink, tid(7, None), upload_ref(), None);
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        assert_eq!(
            vec![("Started", tid(7, None)), ("Finished", tid(7, None))],
            drain_ids(&mut stream),
            "a transfer alone on its sink must deliver both of its events"
        );
        assert_eq!(
            0,
            stream.dropped(),
            "nothing was lost, so nothing is counted"
        );
        assert!(
            matches!(stream.try_recv(), Err(TryRecvError::Disconnected)),
            "sending the `Finished` released the sink, so the stream is closed rather than \
             merely empty"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// `dropped()` counts events the consumer lost, and only those.
    ///
    /// A transfer that finds no room is a loss: the consumer is listening and gets neither
    /// of its events, so any total it assembles is short and `dropped()` is how it finds
    /// out. A gone receiver is not a loss, because nobody is there to lose anything, and
    /// counting it would make `dropped()` grow with however much transfer remains after the
    /// consumer stops rather than with anything about delivery.
    #[test]
    fn dropped_counts_a_transfer_without_room_but_not_a_closed_stream() {
        let (sink, stream) = channel(NonZeroUsize::new(2).unwrap());
        let _holder = announced(&sink, 1);
        assert_eq!(
            0,
            stream.dropped(),
            "the first transfer has room for both of its events"
        );

        let _crowded_out = announced(&sink, 2);
        assert_eq!(
            2,
            stream.dropped(),
            "a transfer with no room loses both of its events"
        );

        drop(stream);
        let _after_close = announced(&sink, 3);
        assert_eq!(
            2,
            sink.outlets[0].dropped.load(Ordering::Relaxed),
            "a closed receiver must not be counted as lost events"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A transfer granted one slot and refused the second keeps neither.
    ///
    /// Capacity 3: the first transfer holds two slots, so the second is granted the third and
    /// refused a fourth. Its `Started` must not arrive, because its `Finished` would have no
    /// room. The slot it was granted must go back to the channel: once the first transfer's
    /// `Started` is received, a third transfer needs both free slots. The second finishes
    /// last, with every slot free, so a `Finished` sent without a held slot would arrive as
    /// half a pair.
    #[test]
    fn a_transfer_granted_one_slot_keeps_neither() {
        let (sink, mut stream) = channel(NonZeroUsize::new(3).unwrap());
        let first = announced(&sink, 1);
        let second = announced(&sink, 2);

        let ev = stream.try_recv().expect("the first transfer's `Started`");
        assert!(
            matches!(&ev, TransferEvent::Started(started) if started.id() == tid(1, None)),
            "the first transfer had room for both of its events: {ev:?}"
        );
        assert!(
            matches!(stream.try_recv(), Err(TryRecvError::Empty)),
            "the second transfer's `Started` must not arrive without room for its `Finished`"
        );
        assert_eq!(
            2,
            stream.dropped(),
            "both of the second transfer's events are counted"
        );

        let third = announced(&sink, 3);
        assert_eq!(
            2,
            stream.dropped(),
            "the slot the second transfer was granted must go back to the channel, so the \
             third has room for both of its events"
        );

        for lc in [&first, &third] {
            lc.finish(Outcome::Succeeded {}).expect("owed").send();
        }
        assert_eq!(
            vec![
                ("Started", tid(3, None)),
                ("Finished", tid(1, None)),
                ("Finished", tid(3, None)),
            ],
            drain_ids(&mut stream),
            "each transfer with room delivers its `Finished` through the slot it held"
        );

        second.finish(Outcome::Succeeded {}).expect("owed").send();
        assert_eq!(
            Vec::<(&str, TransferId)>::new(),
            drain_ids(&mut stream),
            "a transfer left off the stream must not deliver its `Finished` once room frees up"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A lifecycle dropped without sending its `Finished` releases the slot it held, and the
    /// sender inside it.
    ///
    /// Capacity 2: the abandoned transfer's `Started` is queued and its held slot fills the
    /// channel. A slot that outlived the lifecycle would leave the next transfer one slot
    /// short once that `Started` is received, and would keep the stream open with no
    /// transfer left to close it.
    #[test]
    fn a_lifecycle_dropped_unsent_releases_its_slot() {
        let (sink, mut stream) = channel(NonZeroUsize::new(2).unwrap());
        let abandoned = announced(&sink, 1);
        // A claim dropped unsent re-arms, so the lifecycle still owes its `Finished` when it
        // goes.
        drop(abandoned.finish(failed()));
        drop(abandoned);
        assert_eq!(
            vec![("Started", tid(1, None))],
            drain_ids(&mut stream),
            "a lifecycle that never sends delivers no `Finished`"
        );

        let next = announced(&sink, 2);
        drop(sink);
        assert_eq!(
            0,
            stream.dropped(),
            "the abandoned slot must go back to the channel, so the next transfer has room \
             for both of its events"
        );
        next.finish(Outcome::Succeeded {}).expect("owed").send();
        assert_eq!(
            vec![("Started", tid(2, None)), ("Finished", tid(2, None))],
            drain_ids(&mut stream),
            "the next transfer delivers both of its events"
        );
        assert!(
            matches!(stream.try_recv(), Err(TryRecvError::Disconnected)),
            "no sender may outlive the transfers on the sink, or the stream never ends"
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

        let lc = TransferLifecycle::new(
            registered,
            tid(7, Some(1)),
            TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            None,
        );
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

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

        let lc = TransferLifecycle::new(
            a.merge(b),
            tid(9, None),
            TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            None,
        );
        lc.announce();
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        let both = vec![("Started", tid(9, None)), ("Finished", tid(9, None))];
        assert_eq!(both, drain_ids(&mut sa), "the first consumer receives");
        assert_eq!(both, drain_ids(&mut sb), "the second consumer receives");
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A stream with no room for a transfer does not keep that transfer off another stream of
    /// the same merged sink.
    ///
    /// Capacity 2 on both streams, with `a`'s two slots held by transfer 1: transfer 2 has room
    /// on `b` only. Reserving on every stream or on none would keep both of its events from
    /// `b`.
    #[test]
    fn a_full_stream_does_not_keep_a_transfer_off_another() {
        let (a, mut sa) = channel(NonZeroUsize::new(2).unwrap());
        let (b, mut sb) = channel(NonZeroUsize::new(2).unwrap());
        let _holder = announced(&a, 1);

        let lc = announced(&a.merge(b), 2);
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        assert_eq!(
            vec![("Started", tid(2, None)), ("Finished", tid(2, None))],
            drain_ids(&mut sb),
            "`b` had room for both events, so a full `a` must not keep the transfer off it"
        );
        assert_eq!(0, sb.dropped(), "nothing was lost on `b`");
        assert_eq!(
            vec![("Started", tid(1, None))],
            drain_ids(&mut sa),
            "`a` had no room for the second transfer"
        );
        assert_eq!(
            2,
            sa.dropped(),
            "both of the second transfer's events are counted on `a`"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A stream whose receiver is gone does not keep a transfer off another stream of the same
    /// merged sink, and counts nothing.
    #[test]
    fn a_closed_stream_does_not_keep_a_transfer_off_another() {
        let (a, sa) = channel(NonZeroUsize::new(2).unwrap());
        let (b, mut sb) = channel(NonZeroUsize::new(2).unwrap());
        drop(sa);

        let lc = announced(&a.clone().merge(b), 1);
        lc.finish(Outcome::Succeeded {}).expect("owed").send();

        assert_eq!(
            vec![("Started", tid(1, None)), ("Finished", tid(1, None))],
            drain_ids(&mut sb),
            "a gone receiver on `a` must not keep the transfer off `b`"
        );
        assert_eq!(0, sb.dropped(), "nothing was lost on `b`");
        assert_eq!(
            0,
            a.outlets[0].dropped.load(Ordering::Relaxed),
            "a gone receiver is not counted as lost events"
        );
    }
}
