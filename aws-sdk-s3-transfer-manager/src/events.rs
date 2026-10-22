/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Lifecycle events for transfers, delivered on a bounded channel the caller drains.
//!
//! [`channel`] creates a [`TransferEventSink`] and the [`TransferEventStream`] it feeds.
//! Register the sink with a request's `events()` method, such as
//! [`DownloadFluentBuilder::events`](crate::operation::download::builders::DownloadFluentBuilder::events).
//! Each transfer the request runs sends one [`TransferEvent::Started`] when it starts and at
//! most one [`TransferEvent::Finished`] when it reaches a terminal state. Both carry the
//! transfer's [`TransferId`] and [`TransferRef`].
//!
//! Events carry no byte or object counts. A `Started` carries a
//! [`TransferView`](crate::types::TransferView) instead, which reads the transfer's live
//! counters on demand, so a progress display redraws on its own schedule rather than once
//! per event.
//!
//! Sending never blocks a transfer or runs caller code. A stream receives a transfer's
//! `Started` only if it has room for the `Finished` as well, so a `Finished` is never lost to
//! a full stream. A transfer that finds no room is left off the stream, and
//! [`TransferEventStream::dropped`] counts both of its events.
//!
//! ```no_run
//! use aws_sdk_s3_transfer_manager::events::{self, TransferEvent};
//! use std::num::NonZeroUsize;
//!
//! # async fn example(client: aws_sdk_s3_transfer_manager::Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
//! let (sink, mut stream) = events::channel(NonZeroUsize::new(2).unwrap());
//! let handle = client
//!     .download()
//!     .bucket("my-bucket")
//!     .key("my-key")
//!     .events(sink)
//!     .write_to_path("my-key")
//!     .await?;
//! // Ends once the transfer has sent its `Finished` and released the sink.
//! while let Some(event) = stream.recv().await {
//!     if let TransferEvent::Finished(finished) = event {
//!         println!("{:?}: {}", finished.outcome(), finished.transfer().destination());
//!     }
//! }
//! handle.join().await?;
//! # Ok(())
//! # }
//! ```
//!
//! `examples/progress.rs` draws a progress bar from the view.

use std::fmt;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub use crate::transfer::TransferId;

/// Which way a transfer's bytes move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Direction {
    /// To S3.
    Upload,
    /// From S3.
    Download,
}

/// One end of a transfer.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Endpoint {
    /// An S3 object.
    #[non_exhaustive]
    S3 {
        /// The bucket, access point ARN, or Outposts ARN, as the caller gave it.
        bucket: Arc<str>,
        /// The full object key.
        key: Arc<str>,
    },
    /// A file on the local filesystem.
    #[non_exhaustive]
    Local {
        /// The path as the caller gave it, not canonicalized.
        ///
        /// `None` for a file the caller opened, as with
        /// [`write_to_file`](crate::operation::download::builders::DownloadFluentBuilder::write_to_file).
        path: Option<Arc<Path>>,
    },
    /// A body the caller owns: an upload body not read from a path, or the body of a
    /// streaming download.
    #[non_exhaustive]
    Stream {},
    /// An end whose address could not be determined, so the transfer never reached it.
    #[non_exhaustive]
    Unresolved {},
}

impl fmt::Display for Endpoint {
    /// `s3://bucket/key`, the path (lossy for a non-UTF-8 name), `(local file)` for a
    /// local file whose path is unknown, `-` for a caller-owned stream, or
    /// `(unresolved)`.
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

/// The transfer an event is about: its direction and its two ends.
#[derive(Debug, Clone)]
pub struct TransferRef {
    direction: Direction,
    source: Endpoint,
    destination: Endpoint,
}

impl TransferRef {
    /// An upload from `source`, a local file or a caller-supplied body, to the S3 object
    /// `destination`.
    pub(crate) fn upload(source: Endpoint, destination: Endpoint) -> Self {
        Self {
            direction: Direction::Upload,
            source,
            destination,
        }
    }

    /// A download from the S3 object `source` to `destination`, a local file or a body the
    /// caller reads.
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

    /// Where the transfer reads from.
    pub fn source(&self) -> &Endpoint {
        &self.source
    }

    /// Where the transfer writes to.
    pub fn destination(&self) -> &Endpoint {
        &self.destination
    }
}

/// How a transfer ended.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Outcome {
    /// The transfer succeeded.
    #[non_exhaustive]
    Succeeded {},
    /// The transfer failed. The handle's `join()` returns the error.
    #[non_exhaustive]
    Failed {},
    /// The transfer was cancelled.
    #[non_exhaustive]
    Cancelled {},
}

/// An event in one transfer's lifecycle.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransferEvent {
    /// The transfer started. Sent once per transfer.
    Started(Started),
    /// The transfer reached a terminal state. At most one follows each
    /// [`TransferEvent::Started`].
    Finished(Finished),
}

/// A transfer started.
#[derive(Debug, Clone)]
pub struct Started {
    id: TransferId,
    transfer: TransferRef,
    view: Option<crate::types::TransferView>,
}

impl Started {
    /// The transfer's id.
    pub fn id(&self) -> TransferId {
        self.id
    }

    /// The transfer's direction and ends.
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
    }

    /// A read-only view of the transfer's live counters.
    ///
    /// `None` when the transfer has no counters to read.
    pub fn view(&self) -> Option<&crate::types::TransferView> {
        self.view.as_ref()
    }
}

/// A transfer reached a terminal state.
#[derive(Debug, Clone)]
pub struct Finished {
    id: TransferId,
    transfer: TransferRef,
    outcome: Outcome,
}

impl Finished {
    /// The transfer's id, equal to its [`Started::id`].
    pub fn id(&self) -> TransferId {
        self.id
    }

    /// The transfer's direction and ends, as on its [`Started`].
    pub fn transfer(&self) -> &TransferRef {
        &self.transfer
    }

    /// How the transfer ended.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }
}

impl TransferEvent {
    /// The transfer this event is about.
    pub fn transfer(&self) -> &TransferRef {
        match self {
            TransferEvent::Started(e) => e.transfer(),
            TransferEvent::Finished(e) => e.transfer(),
        }
    }
}

/// The largest capacity tokio's bounded channel accepts without panicking.
const MAX_CAPACITY: usize = usize::MAX >> 3;

/// Create a sink/stream pair. The caller owns the stream and drains it.
///
/// A transfer never waits for room on the stream. One that starts with at least two slots
/// free holds room for both of its events until the consumer receives them; one that starts
/// with fewer is left off the stream, and [`TransferEventStream::dropped`] counts both of its
/// events. A sink used by one transfer never drops an event at capacity 2. A stream fed by
/// several transfers, through clones or [`merge`](TransferEventSink::merge), drops nothing
/// while its capacity is at least twice the number of transfers that are running or whose
/// events the consumer has not yet received. A capacity below 2 is raised to 2.
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

/// The sending end of one stream.
#[derive(Debug)]
struct Outlet {
    tx: tokio::sync::mpsc::Sender<TransferEvent>,
    /// This stream's loss counter, read by [`TransferEventStream::dropped`].
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

/// The sending half of an event channel, registered on a request with its `events()` method.
///
/// Cheap to clone; every clone feeds the same streams. [`merge`](Self::merge) combines sinks
/// so one registration feeds several streams.
#[derive(Debug, Clone)]
pub struct TransferEventSink {
    outlets: Arc<[Outlet]>,
}

impl TransferEventSink {
    /// Returns a sink that feeds both this sink's streams and `other`'s.
    ///
    /// Each stream keeps its own capacity and [`dropped`](TransferEventStream::dropped) count,
    /// so a stream that stops draining loses only its own events. A stream fed by both sinks
    /// receives each event once.
    pub fn merge(self, other: TransferEventSink) -> TransferEventSink {
        let mut outlets: Vec<Outlet> = Vec::with_capacity(self.outlets.len() + other.outlets.len());
        for src in [&self.outlets, &other.outlets] {
            for outlet in src.iter() {
                // By channel, not by sink: clones of one sink feed the same stream.
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

/// The receiving half of an event channel, created by [`channel`].
///
/// [`recv`](Self::recv) waits for the next event. It returns `None` once every sink feeding
/// this stream has been dropped and every queued event received. A transfer drops the sink
/// it was registered with after sending its [`TransferEvent::Finished`], so a stream whose
/// sinks were all moved into requests ends after the last of those transfers finishes.
///
/// [`try_recv`](Self::try_recv) returns at once, for a loop that redraws on its own
/// schedule: take whatever events are queued, then read the
/// [`TransferView`](crate::types::TransferView) each `Started` carried.
///
/// [`dropped`](Self::dropped) counts the events this stream lost for lack of room.
///
/// ```
/// use aws_sdk_s3_transfer_manager::events::{TransferEvent, TransferEventStream, TryRecvError};
/// use aws_sdk_s3_transfer_manager::types::TransferView;
///
/// /// Takes every queued event without waiting. Returns `false` once the stream has ended.
/// fn drain(stream: &mut TransferEventStream, view: &mut Option<TransferView>) -> bool {
///     loop {
///         match stream.try_recv() {
///             Ok(TransferEvent::Started(started)) => *view = started.view().cloned(),
///             Ok(_) => {}
///             Err(TryRecvError::Empty) => return true,
///             Err(_) => return false,
///         }
///     }
/// }
/// ```
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

    /// Events this stream lost for lack of room: two for each transfer that found no room
    /// for both of its events.
    ///
    /// A total assembled from received events is complete only while this is zero.
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

/// Emits one transfer's events, and owns the obligation to emit its `Finished`.
pub(crate) struct TransferLifecycle {
    /// The sink and the slots held for the `Finished`, taken by the send that delivers it.
    ///
    /// Taken at that send rather than when the lifecycle drops: the lifecycle lives as long
    /// as any clone of the transfer's context, and a stream cannot end while its sink is held.
    route: Arc<Mutex<Option<Route>>>,
    transfer: TransferRef,
    /// The view the `Started` carries.
    view: Option<crate::types::TransferView>,
    /// Whether a `Finished` is owed: set by [`announce`](Self::announce), cleared by the
    /// [`finish`](Self::finish) that claims it, and set again by a [`PendingEmit`] dropped
    /// unsent.
    owes_finish: Arc<AtomicBool>,
}

impl fmt::Debug for TransferLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransferLifecycle")
            .field("transfer", &self.transfer)
            .field("owes_finish", &self.owes_finish.load(Ordering::Relaxed))
            .finish()
    }
}

/// Where a lifecycle's events go.
#[derive(Debug)]
struct Route {
    /// Held until the `Finished` is sent, so a stream on which this transfer holds no slot
    /// does not end while the transfer runs.
    sink: TransferEventSink,
    /// A slot for the `Finished` on each outlet that had room for both events at `announce`.
    finish: Vec<Permit>,
}

/// Locks the route, ignoring poison.
///
/// A terminal path can run after a caught panic, so a lock that panic poisoned must not lose
/// the `Finished`. The slot holds a route or `None`, and no unwind leaves it in any other
/// state, so the value is always safe to take.
fn lock_route(slot: &Mutex<Option<Route>>) -> std::sync::MutexGuard<'_, Option<Route>> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl TransferLifecycle {
    pub(crate) fn new(
        sink: TransferEventSink,
        transfer: TransferRef,
        view: Option<crate::types::TransferView>,
    ) -> Self {
        Self {
            route: Arc::new(Mutex::new(Some(Route {
                sink,
                finish: Vec::new(),
            }))),
            transfer,
            view,
            owes_finish: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Emits [`TransferEvent::Started`] for the transfer `id` and takes on the obligation to
    /// emit its `Finished`.
    ///
    /// Each outlet with room for both events receives the `Started` and holds a slot for the
    /// `Finished`. Each outlet without that room receives neither and counts both as dropped,
    /// unless its receiver is gone.
    ///
    /// The obligation is taken even where no outlet had room, because discharging it is what
    /// releases the sink.
    pub(crate) fn announce(&self, id: TransferId) {
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
                id,
                transfer: self.transfer.clone(),
                view: self.view.clone(),
            }),
        );
    }

    /// Claims the `Finished` for the transfer `id`, the id [`announce`](Self::announce) was
    /// given, or returns `None` when none is owed: before `announce`, while another claim is
    /// outstanding, or once it has been sent.
    pub(crate) fn finish(&self, id: TransferId, outcome: Outcome) -> Option<PendingEmit> {
        if !self.owes_finish.swap(false, Ordering::AcqRel) {
            return None;
        }
        Some(PendingEmit {
            route: self.route.clone(),
            owes_finish: self.owes_finish.clone(),
            event: Some(TransferEvent::Finished(Finished {
                id,
                transfer: self.transfer.clone(),
                outcome,
            })),
        })
    }
}

/// A claimed `Finished` that has not been sent.
///
/// Claiming and sending are separate, so a claim made under a state guard can be sent after
/// the guard is released.
pub(crate) struct PendingEmit {
    /// The lifecycle's route: the send takes it, and a claim dropped unsent leaves it, with
    /// its held slots, for the next claim.
    route: Arc<Mutex<Option<Route>>>,
    /// `None` once sent, so [`Drop`] can tell a sent claim from an abandoned one.
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
    /// Sends the `Finished` through the slots `announce` held for it, so a full channel cannot
    /// drop it, then releases the sink. An outlet that had no room at `announce` receives
    /// nothing.
    ///
    /// Never awaits or waits for room, so it can be called while holding a state guard.
    ///
    /// The sink is released here rather than at the claim, because a claim dropped unsent
    /// re-arms the obligation, and the next claim still needs the slots to send through.
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
    /// Hands the obligation back if the `Finished` was never sent, so the next
    /// [`TransferLifecycle::finish`] can claim it.
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

    /// The transfer a [`lifecycle`] reports on.
    const ID: TransferId = TransferId {
        id: 1,
        parent: None,
    };

    fn lifecycle() -> (TransferLifecycle, TransferEventStream) {
        let (sink, stream) = channel(NonZeroUsize::new(8).unwrap());
        (TransferLifecycle::new(sink, upload_ref(), None), stream)
    }

    /// A lifecycle on `sink` for transfer `id`, already announced.
    fn announced(sink: &TransferEventSink, id: u64) -> TransferLifecycle {
        let lc = TransferLifecycle::new(sink.clone(), upload_ref(), None);
        lc.announce(tid(id, None));
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

    /// The line a finished transfer prints, such as `upload: <source> to <destination>`, built
    /// from the event alone.
    ///
    /// No wildcard [`Direction`] or [`TransferEvent`] arm: `#[non_exhaustive]` does not apply
    /// in the defining crate.
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

        // A failure names its transfer from the event alone; the cause comes from `join()`.
        assert_eq!(
            line(&finished(upload_ref(), failed())).as_deref(),
            Some("upload failed: ./a.txt to s3://bucket/k/a.txt"),
            "a failure must name its transfer from the same event"
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
    fn both_events_carry_the_transfer_and_its_id() {
        let (lc, mut stream) = lifecycle();
        lc.announce(ID);
        lc.finish(ID, Outcome::Succeeded {}).expect("owed").send();

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
                ID, id,
                "the terminal must carry the start's id, or a consumer of several transfers \
                 cannot tell which one ended: {ev:?}"
            );
            assert_eq!(
                ev.transfer().destination().to_string(),
                "s3://bucket/k/a.txt",
                "both events must name the transfer: {ev:?}"
            );
        }
    }

    #[test]
    fn finish_is_claimed_once() {
        let (lc, _stream) = lifecycle();
        lc.announce(ID);
        // Held: dropping the claim would hand the obligation back, which the next test covers.
        let first = lc.finish(ID, failed()).expect("first claim wins");
        assert!(
            lc.finish(ID, failed()).is_none(),
            "a terminal path reached twice must not emit twice"
        );
        first.send();
        assert!(
            lc.finish(ID, failed()).is_none(),
            "sending discharges the obligation for good"
        );
    }

    #[test]
    fn a_claim_dropped_unsent_hands_the_obligation_back() {
        let (lc, mut stream) = lifecycle();
        lc.announce(ID);
        // A site that claims and then returns early without publishing.
        drop(lc.finish(ID, failed()));
        lc.finish(ID, failed())
            .expect(
                "an abandoned claim must re-arm: otherwise the obligation is consumed \
                 by a value that never reached the channel, and the `Finished` is lost \
                 with nothing recording that it was owed",
            )
            .send();
        assert_eq!(
            vec![("Started", ID), ("Finished", ID)],
            drain_ids(&mut stream),
            "the abandoned claim must leave the slot `announce` held for the `Finished`, \
             so the re-armed claim still has room to send through"
        );
    }

    #[test]
    fn finish_without_announce_emits_nothing() {
        let (lc, _stream) = lifecycle();
        assert!(
            lc.finish(ID, Outcome::Cancelled {}).is_none(),
            "nothing is owed for a transfer that was never announced"
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
        let lc = TransferLifecycle::new(sink, upload_ref(), None);
        lc.announce(tid(7, None));
        lc.finish(tid(7, None), Outcome::Succeeded {})
            .expect("owed")
            .send();

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
    /// `dropped()` counts a transfer that found no room, and not one that started after the
    /// stream was dropped.
    ///
    /// Capacity 2: the first transfer holds both slots, so the second finds none and both of
    /// its events are counted. The third starts after the stream is dropped; counting it would
    /// make the count grow with the work left after the consumer stopped.
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

        for (lc, id) in [(&first, 1), (&third, 3)] {
            lc.finish(tid(id, None), Outcome::Succeeded {})
                .expect("owed")
                .send();
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

        second
            .finish(tid(2, None), Outcome::Succeeded {})
            .expect("owed")
            .send();
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
        drop(abandoned.finish(tid(1, None), failed()));
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
        next.finish(tid(2, None), Outcome::Succeeded {})
            .expect("owed")
            .send();
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

    /// A sink merged with itself delivers each event once.
    ///
    /// Registering one sink twice on a request merges it with itself. The fan-out to outlets
    /// follows the single claim, so a duplicated outlet would deliver the `Finished` twice.
    #[tokio::test]
    async fn merging_a_sink_with_itself_delivers_each_event_once() {
        let (sink, mut stream) = channel(std::num::NonZeroUsize::new(16).expect("capacity > 0"));
        let registered = sink.clone().merge(sink);

        let lc = TransferLifecycle::new(
            registered,
            TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            None,
        );
        lc.announce(tid(7, Some(1)));
        lc.finish(tid(7, Some(1)), Outcome::Succeeded {})
            .expect("owed")
            .send();

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

    /// Merging two distinct sinks delivers every event to both streams.
    ///
    /// Deduplication is keyed on the channel, so it must not collapse two distinct streams into
    /// one.
    #[tokio::test]
    async fn merging_two_distinct_sinks_delivers_to_both() {
        let (a, mut sa) = channel(std::num::NonZeroUsize::new(8).expect("capacity > 0"));
        let (b, mut sb) = channel(std::num::NonZeroUsize::new(8).expect("capacity > 0"));

        let lc = TransferLifecycle::new(
            a.merge(b),
            TransferRef::download(Endpoint::Stream {}, Endpoint::Stream {}),
            None,
        );
        lc.announce(tid(9, None));
        lc.finish(tid(9, None), Outcome::Succeeded {})
            .expect("owed")
            .send();

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
        lc.finish(tid(2, None), Outcome::Succeeded {})
            .expect("owed")
            .send();

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
        lc.finish(tid(1, None), Outcome::Succeeded {})
            .expect("owed")
            .send();

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
