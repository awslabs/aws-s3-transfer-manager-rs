/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types that define what a transfer is and what it produces.

mod pending;

use crate::error;
use crate::scheduler::concurrency::ErrorKind;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};

use pending::TransferPendingState;
pub(crate) use pending::{emit_pending_details, PendingCauseSlot, TransferPendingStats};
pub use pending::{PendingCategory, PendingCause};

/// Edge-triggered wake flag for transfer state machines.
///
/// Scoped to a sub-module so the underlying atomic is sourced from the
/// `runtime/sync` compat layer (loom under `cfg(s3_tm_loom)`, std otherwise)
/// without forcing the rest of this file's atomics onto the same path.
///
/// See [`TransferContext::set_pending`] for the wake primitive protocol —
/// the discipline the poller and mutator must follow to avoid lost wakes.
pub(crate) mod wake_flag {
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};

    pub(crate) struct WakeFlag {
        pending: AtomicBool,
    }

    impl WakeFlag {
        pub(crate) fn new() -> Self {
            Self {
                pending: AtomicBool::new(false),
            }
        }

        /// Set the pending flag.
        pub(crate) fn set_pending(&self) {
            self.pending.store(true, Ordering::Release);
        }

        /// Atomically clear the flag and return whether it was set.
        pub(crate) fn take_pending(&self) -> bool {
            self.pending.swap(false, Ordering::AcqRel)
        }
    }
}

/// A transfer operation that the scheduler polls for work and the runtime executes.
///
/// Each transfer (upload, download) is a state machine that produces IO requests
/// on demand via `poll_work()` and executes them via `execute()`. The scheduler
/// calls `poll_work()` when capacity is available; the runtime calls `execute()`
/// on whatever thread or executor it manages.
///
/// # Contracts
///
/// Implementations must uphold:
/// - **Failed lifecycle**: record the error and signal termination before returning
///   `WorkOutcome::Failed`.
/// - **Pending/wake obligation**: every `PollWork::Pending` must first call
///   `ctx.set_pending(cause)` and have a future wake path. See
///   [`TransferContext`] for the wake primitive protocol.
/// - **Panic safety**: `execute` panics are caught by the runtime's
///   `catch_unwind` wrapper and converted to a terminal transition. `poll_work`
///   panics are caught by the scheduler inside `generate_work`, which
///   force-terminates the panicking transfer (cascading to children).
///   Implementations SHOULD NOT rely on panic recovery — a caught panic still
///   corrupts the transfer's internal state and forces termination.
///
/// # Per-method expectations
///
/// ## `poll_work` — synchronous, short, bounded
/// - No `.await`, no blocking I/O, no unbounded loops.
/// - Cost is O(1) per call with a bounded critical section on state. Long
///   `poll_work` calls pin the caller's runtime and starve its other async tasks.
/// - At most one thread is inside `poll_work` for a given transfer at a time
///   (scheduler-enforced). Per-transfer state mutexes are therefore
///   effectively uncontended in steady state.
/// - Composite transfers whose `poll_work` recursively calls
///   `scheduler.enqueue_transfer` to spawn children must bound the per-call
///   fan-out: cost becomes `O(batch × enqueue_cost)` and a single unbounded
///   call can starve the rest of the scheduler.
///
/// ## `execute` — the async surface
/// - The only place `.await` is permitted. All blocking or long-running work
///   belongs here.
/// - State locks MUST NOT be held across `.await`. Acquire the lock, mutate,
///   drop it, then await.
/// - May call `scheduler.wake(id)` (via `ctx.try_wake()`) mid-execution to
///   re-queue the transfer if intra-execute state changes unblock work.
/// - The concurrency slot is held for the duration of this future; the
///   scheduler only decrements its dispatched counter after `execute` returns.
/// - Cancelling the transfer drops this future at its next `.await`. State
///   visible to other work or to the handle must be consistent at every
///   `.await` point.
///
/// ## `on_terminal` — termination hook
/// - Called after the status becomes terminal, to let the transfer release held
///   resources and notify waiters. Not guaranteed; see [`Transfer::on_terminal`].
/// - Must be short and must not block.
pub(crate) trait Transfer: Send + Sync + std::fmt::Debug {
    /// The transfer's shared context (id, handle, status).
    fn ctx(&self) -> &TransferContext;

    /// Poll for the next IO request. Returns `Ready` with work, `Pending` if
    /// blocked, or `Done` when all work has been generated.
    ///
    /// See the trait-level "Per-method expectations" for the cost and threading
    /// contracts this call must respect.
    fn poll_work(&self) -> PollWork;

    /// Execute an IO request. Called by the runtime, not the scheduler.
    ///
    /// The only async surface on a transfer. State locks must not be held
    /// across `.await` points.
    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;

    /// Called after the transfer's status becomes terminal. Allows the transfer to clean up
    /// resources and notify waiters that would otherwise block indefinitely.
    ///
    /// Must be short and non-blocking. May be called more than once. When `poll_work` returns
    /// [`PollWork::Done`] while the status is still `Active`, the scheduler removes the
    /// transfer without calling this; such a transfer does its terminal work where it sets
    /// its status.
    fn on_terminal(&self) {}
}

pub(crate) type BoxTransfer = Box<dyn Transfer>;

/// Opaque work data carried by work items. Each state machine defines its own type.
/// The scheduler never inspects this; it ferries it across the scheduling boundary
/// for the transfer to reclaim via `IoRequest::data_mut::<T>()`.
pub(crate) trait WorkData: Any + Send + std::fmt::Debug {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any + Send + std::fmt::Debug> WorkData for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Identifies one transfer: a [`Started`](crate::events::Started) and its
/// [`Finished`](crate::events::Finished) carry the same id, and each object a directory
/// operation transfers has a [`parent`](Self::parent) equal to the operation's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransferId {
    pub(crate) id: u64,
    pub(crate) parent: Option<u64>,
}

impl TransferId {
    /// The id of the directory operation this transfer belongs to, or `None` when this
    /// transfer is the operation the caller invoked.
    pub fn parent(&self) -> Option<TransferId> {
        self.parent.map(|id| TransferId { id, parent: None })
    }
}

impl std::fmt::Display for TransferId {
    /// `parent/child`, or just the id for a root.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parent {
            Some(parent) => write!(f, "{}/{}", parent, self.id),
            None => write!(f, "{}", self.id),
        }
    }
}

/// A unit of I/O to be scheduled and executed by the runtime.
#[derive(Debug)]
pub(crate) struct IoRequest {
    pub(crate) data: Option<Box<dyn WorkData>>,
}

impl IoRequest {
    /// Downcast data to a concrete type. Panics if wrong type or None.
    pub(crate) fn data_mut<T: 'static>(&mut self) -> &mut T {
        (**self.data.as_mut().expect("work item has no data"))
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("work data type mismatch")
    }

    /// Take the data as a concrete type, leaving the request without data.
    /// For work whose payload `execute` consumes. Panics if wrong type or None.
    pub(crate) fn take_data<T: 'static>(&mut self) -> T {
        let data: Box<dyn Any> = self.data.take().expect("work item has no data");
        *data.downcast::<T>().expect("work data type mismatch")
    }
}

/// Result of polling a transfer for work.
#[derive(Debug)]
pub(crate) enum PollWork {
    /// Work is available to execute.
    ///
    /// `spawned` records whether this same poll ALSO spawned a child (a
    /// composite that reaped terminal children and refilled in one turn). When
    /// true, the scheduler dispatches `io` as usual (one dispatch ticket, full
    /// `work_generated`) AND additionally charges one spawn's reduced vruntime
    /// ([`work_generated_spawn`]) -- WITHOUT an extra dispatch ticket, since the
    /// spawned child accounts for itself when later polled (the `Spawned`
    /// contract). It is a bool, not a count: single-ticket spawning means a poll
    /// spawns at most one child. Non-composite transfers always set it false;
    /// use [`PollWork::ready`] for that common case.
    ///
    /// Fusing reap and spawn into one poll lets a composite retire a terminal
    /// child and spawn its replacement in the same turn instead of alternating
    /// turns, so a continuous completion stream cannot starve refill by
    /// monopolizing the parent's poll turns with reaping.
    ///
    /// [`work_generated_spawn`]: crate::scheduler::descriptor::TransferDescriptor::work_generated_spawn
    Ready { io: IoRequest, spawned: bool },
    /// The transfer enqueued one child transfer into the scheduler this poll. It
    /// produced no dispatchable `IoRequest` and is not blocked: re-poll while
    /// capacity remains (next poll may spawn again, block, or complete). Distinct
    /// from `Ready` (which carries a work item and consumes a dispatch ticket) --
    /// `Spawned` dispatches nothing and does not count against the concurrency
    /// target; the spawned child accounts for itself when it is later polled. The
    /// scheduler charges spawn vruntime (the reduced [`work_generated_spawn`]
    /// cost) so a composite parent stays scheduling-competitive with its
    /// children.
    ///
    /// [`work_generated_spawn`]: crate::scheduler::descriptor::TransferDescriptor::work_generated_spawn
    Spawned,
    /// Transfer is blocked waiting for in-flight work to complete.
    /// Scheduler should not poll again until `wake(transfer_id)` is called.
    Pending,
    /// Transfer has completed all work.
    Done,
}

impl PollWork {
    /// Construct a `Ready` that dispatched work but did not spawn a child --
    /// the common case for non-composite transfers and for composite polls that
    /// only reaped. Equivalent to `Ready { io, spawned: false }`.
    pub(crate) fn ready(io: IoRequest) -> Self {
        PollWork::Ready { io, spawned: false }
    }
}

/// Result of executing a work item.
///
/// Contract between transfer state machines and the scheduler:
/// - `Success`: Work completed. Scheduler continues polling the transfer for more work.
/// - `Yielded`: Dispatched execution retired without completing a reportable I/O operation or
///   reporting a failure. Transfer state has retained any continuation or has retracted or retired
///   the speculative operation. The scheduler releases the execution slot without producing a
///   concurrency-controller sample.
/// - `Failed`: Transfer has already transitioned itself to terminal state (via `set_failed` +
///   `signal_terminal`). Scheduler will not poll it again and will remove it once idle.
/// - `Cancelled`: Transfer is already terminal (failed or cancelled by another work item).
///   Same cleanup as `Failed`.
pub(crate) enum WorkOutcome {
    /// Work completed successfully.
    Success { data: Option<Box<dyn WorkData>> },
    /// Dispatched execution retired without completing an I/O operation or reporting a failure.
    ///
    /// Before returning, transfer state must reconcile the work exactly once. If it retains a
    /// continuation, that continuation must retain its progress and future wake path. The scheduler
    /// releases the execution slot but does not report a completion sample to the concurrency
    /// controller.
    Yielded,
    /// Work failed. Transfer must have called `set_failed` + `signal_terminal` before returning.
    Failed { classification: Option<ErrorKind> },
    /// Work was skipped or aborted because the transfer is already terminal.
    Cancelled,
}

impl std::fmt::Debug for WorkOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkOutcome::Success { data } => f
                .debug_struct("Success")
                .field("has_data", &data.is_some())
                .finish(),
            WorkOutcome::Yielded => write!(f, "Yielded"),
            WorkOutcome::Failed { classification } => f
                .debug_struct("Failed")
                .field("classification", classification)
                .finish(),
            WorkOutcome::Cancelled => write!(f, "Cancelled"),
        }
    }
}

static NEXT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_transfer_id() -> TransferId {
    TransferId {
        id: NEXT_TRANSFER_ID.fetch_add(1, Ordering::Relaxed),
        parent: None,
    }
}

// Type aliases for state machine completion signal (one-to-one, state machine → handle)
pub(crate) type StateMachineTerminalSender = tokio::sync::oneshot::Sender<()>;
pub(crate) type StateMachineTerminalReceiver = tokio::sync::oneshot::Receiver<()>;

/// Transfer lifecycle status.
///
/// ## State Machine
///
/// ```text
///              ┌──────────┐
///  (enqueued)──►  Active  │
///              └────┬─────┘
///                   │
///      ┌────────────┼────────────┐
///      │            │            │
///      ▼            ▼            ▼
/// ┌──────────┐ ┌──────────┐ ┌───────────┐
/// │Completed │ │  Failed  │ │ Cancelled │
/// └──────────┘ └──────────┘ └───────────┘
///
/// Terminal states: Completed, Failed, Cancelled
/// ```
///
/// ## Transitions
///
/// - `Active → Completed`: All work finished successfully, output available
/// - `Active → Failed`: A work item failed, error stored in context
/// - `Active → Cancelled`: User requested cancellation or handle dropped
///
/// ## First-Write-Wins Semantics
///
/// Only one transition from `Active` succeeds. Concurrent failures, cancellations,
/// or completion attempts race - the first one wins, others are no-ops.
///
/// ## Terminal State Signaling
///
/// After transitioning to a terminal state, call `signal_terminal()` to notify
/// waiters (e.g., `join()`). This signals "state machine has reached terminal
/// state" - in-flight work may still be completing/draining.
///
/// ## Data Availability
///
/// | Status    | Error Available | Output Available |
/// |-----------|-----------------|------------------|
/// | Active    | No              | No               |
/// | Completed | No              | Yes              |
/// | Failed    | Yes (take once) | No               |
/// | Cancelled | No              | No               |
#[derive(Clone)]
pub(crate) struct StateMachineStatus(
    crate::runtime::sync::sync::Arc<crate::runtime::sync::sync::atomic::AtomicU8>,
);

// Status constants
const STATUS_ACTIVE: u8 = 0;
const STATUS_COMPLETED: u8 = 1;
const STATUS_FAILED: u8 = 2;
const STATUS_CANCELLED: u8 = 3;

impl StateMachineStatus {
    /// Create a new status in the Active state
    #[inline]
    pub(crate) fn new() -> Self {
        Self(crate::runtime::sync::sync::Arc::new(
            crate::runtime::sync::sync::atomic::AtomicU8::new(STATUS_ACTIVE),
        ))
    }

    /// Transition to Completed. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_completed(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to Failed. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_failed(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_FAILED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to Cancelled. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_cancelled(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_ACTIVE
    }

    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_FAILED
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_CANCELLED
    }

    /// The status, read in one load. Derive a status from this, not from several `is_*`
    /// calls, which can straddle a transition.
    #[inline]
    pub(crate) fn status(&self) -> crate::types::TransferStatus {
        use crate::types::TransferStatus;
        match self.0.load(Ordering::Acquire) {
            STATUS_COMPLETED => TransferStatus::Completed,
            STATUS_FAILED => TransferStatus::Failed,
            STATUS_CANCELLED => TransferStatus::Cancelled,
            _ => TransferStatus::Active,
        }
    }

    fn as_str(&self) -> &'static str {
        match self.0.load(Ordering::Acquire) {
            STATUS_ACTIVE => "Active",
            STATUS_COMPLETED => "Completed",
            STATUS_FAILED => "Failed",
            STATUS_CANCELLED => "Cancelled",
            _ => "Unknown",
        }
    }
}

impl fmt::Display for StateMachineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Debug for StateMachineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("StateMachineStatus")
            .field(&self.as_str())
            .finish()
    }
}

/// Sets `Failed` and stores the error under `slot`'s lock, so a reader that sees `Failed`
/// and then locks the slot finds the error. Returns whether this call made the transition.
fn fail_with<E>(
    status: &StateMachineStatus,
    slot: &crate::runtime::sync::Mutex<Option<E>>,
    err: impl FnOnce() -> E,
) -> bool {
    let mut held = slot.lock();
    let won = status.set_failed();
    if won {
        *held = Some(err());
    }
    won
}

/// Sets `Cancelled` under `slot`'s lock, so it cannot land inside a [`complete_with`].
/// Returns whether this call made the transition.
fn cancel_with<E>(
    status: &StateMachineStatus,
    slot: &crate::runtime::sync::Mutex<Option<E>>,
) -> bool {
    let _held = slot.lock();
    status.set_cancelled()
}

/// Runs `publish` and then sets `Completed`, both under `slot`'s lock. Failure and
/// cancellation take that lock too, so neither lands between the two. Skips `publish` and
/// returns `Ok(false)` if the transfer has already left `Active`. A `publish` error is
/// returned with the transfer still `Active`, so `publish` must make nothing visible when
/// it fails.
fn complete_with<E, P>(
    status: &StateMachineStatus,
    slot: &crate::runtime::sync::Mutex<Option<E>>,
    publish: impl FnOnce() -> Result<(), P>,
) -> Result<bool, P> {
    let _held = slot.lock();
    if !status.is_active() {
        return Ok(false);
    }
    publish()?;
    Ok(status.set_completed())
}

/// Per-transfer cumulative metrics.
pub(crate) struct MetricsState {
    /// The parent transfer's metrics, which every sample recorded here also reaches.
    parent: Option<Arc<MetricsState>>,
    network_tx: AtomicU64,
    network_rx: AtomicU64,
    disk_read: AtomicU64,
    disk_write: AtomicU64,
    bytes_transferred: AtomicU64,
    /// Expected payload bytes, reported as a provisional byte total while `total_bytes`
    /// is unset.
    discovered_bytes: AtomicU64,
    /// Expected entries, reported as a provisional entry total while `total_entries` is
    /// unset.
    discovered_entries: AtomicU64,
    /// Entries that have reached a terminal state, however they ended.
    settled_entries: AtomicU64,
    total_bytes: std::sync::OnceLock<u64>,
    /// The final entry count, once known.
    total_entries: std::sync::OnceLock<u64>,
    started_at: std::time::Instant,
    finished_at: std::sync::OnceLock<std::time::Instant>,
    terminal_reported: AtomicBool,
    request_metrics: crate::metrics::RequestMetricsState,
}

impl MetricsState {
    /// Metrics with no parent.
    pub(crate) fn new() -> Self {
        Self::with_parent(None)
    }

    /// Metrics whose samples also roll up into `parent`, held strongly since links only point
    /// upward.
    pub(crate) fn with_parent(parent: Option<Arc<MetricsState>>) -> Self {
        Self {
            parent,
            network_tx: AtomicU64::new(0),
            network_rx: AtomicU64::new(0),
            disk_read: AtomicU64::new(0),
            disk_write: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            discovered_bytes: AtomicU64::new(0),
            discovered_entries: AtomicU64::new(0),
            settled_entries: AtomicU64::new(0),
            total_bytes: std::sync::OnceLock::new(),
            total_entries: std::sync::OnceLock::new(),
            started_at: std::time::Instant::now(),
            finished_at: std::sync::OnceLock::new(),
            terminal_reported: AtomicBool::new(false),
            request_metrics: crate::metrics::RequestMetricsState::default(),
        }
    }

    /// Record an IO sample into this transfer and every ancestor, one counter at a time.
    pub(crate) fn record_io(&self, sample: &crate::metrics::IoSample) {
        let mut cur = Some(self);
        while let Some(m) = cur {
            m.network_tx.fetch_add(sample.network_tx, Ordering::Relaxed);
            m.network_rx.fetch_add(sample.network_rx, Ordering::Relaxed);
            m.disk_read.fetch_add(sample.disk_read, Ordering::Relaxed);
            m.disk_write.fetch_add(sample.disk_write, Ordering::Relaxed);
            m.bytes_transferred
                .fetch_add(sample.bytes_transferred, Ordering::Relaxed);
            cur = m.parent.as_deref();
        }
    }

    /// Set the expected total payload bytes. No-op if already set.
    pub(crate) fn set_total_bytes(&self, n: u64) {
        let _ = self.total_bytes.set(n);
    }

    /// Add `bytes` and `entries` to the expected totals, in one call so the two describe
    /// the same work.
    pub(crate) fn add_discovered(&self, bytes: u64, entries: u64) {
        self.discovered_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.discovered_entries
            .fetch_add(entries, Ordering::Relaxed);
    }

    /// Entries that reached a terminal state.
    pub(crate) fn entries_settled(&self) -> u64 {
        self.settled_entries.load(Ordering::Relaxed)
    }

    /// This transfer's byte total: `Final` once `total_bytes` is set, and until then
    /// `Provisional` with the expected bytes, or `Unknown` if none are expected.
    pub(crate) fn byte_total(&self) -> crate::types::Total {
        use crate::types::Total;
        if let Some(n) = self.total_bytes.get().copied() {
            return Total::Final(n);
        }
        match self.discovered_bytes.load(Ordering::Relaxed) {
            0 => Total::Unknown,
            n => Total::Provisional(n),
        }
    }

    /// Like [`byte_total`](Self::byte_total), for entries.
    pub(crate) fn entry_total(&self) -> crate::types::Total {
        use crate::types::Total;
        if let Some(n) = self.total_entries.get().copied() {
            return Total::Final(n);
        }
        match self.discovered_entries.load(Ordering::Relaxed) {
            0 => Total::Unknown,
            n => Total::Provisional(n),
        }
    }

    /// Mark the transfer as finished. No-op if already set.
    pub(crate) fn set_finished(&self) {
        let _ = self.finished_at.set(std::time::Instant::now());
    }

    /// Claims the single terminal tracing record for this transfer.
    ///
    /// Normal completion, failure, cancellation, and scheduler-owned cleanup
    /// may converge on terminal reporting. Only the first caller emits.
    pub(crate) fn claim_terminal_report(&self) -> bool {
        !self.terminal_reported.swap(true, Ordering::AcqRel)
    }

    /// Merge one completed service-request measurement.
    pub(crate) fn record_request(&self, metrics: &crate::metrics::RequestMetrics) {
        self.request_metrics.record(metrics);
    }

    /// Return the cumulative request measurement for this transfer.
    pub(crate) fn request_metrics(&self) -> crate::metrics::RequestMetrics {
        self.request_metrics.snapshot()
    }

    /// Snapshot current metrics into the public type.
    pub(crate) fn snapshot(&self) -> crate::types::TransferMetrics {
        crate::types::TransferMetrics {
            network_tx: self.network_tx.load(Ordering::Relaxed),
            network_rx: self.network_rx.load(Ordering::Relaxed),
            disk_read: self.disk_read.load(Ordering::Relaxed),
            disk_write: self.disk_write.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.get().copied(),
            started_at: self.started_at,
            finished_at: self.finished_at.get().copied(),
        }
    }
}

impl std::fmt::Debug for MetricsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsState")
            .field("network_tx", &self.network_tx.load(Ordering::Relaxed))
            .field("network_rx", &self.network_rx.load(Ordering::Relaxed))
            .field("disk_read", &self.disk_read.load(Ordering::Relaxed))
            .field("disk_write", &self.disk_write.load(Ordering::Relaxed))
            .field(
                "bytes_transferred",
                &self.bytes_transferred.load(Ordering::Relaxed),
            )
            .field("request_metrics", &self.request_metrics())
            .finish()
    }
}

/// Common lifecycle management for transfer state machines.
///
/// Handles status tracking, error storage, completion signaling, and scheduler
/// wake coordination. Each transfer type (upload, download, etc.) owns a
/// `TransferContext` along with its operation-specific state.
///
/// Cheap to clone - all fields are behind `Arc`.
#[derive(Clone)]
pub(crate) struct TransferContext {
    /// Unique identifier for this transfer
    pub(crate) id: TransferId,
    /// Access to client handle (scheduler, S3 client, config)
    pub(crate) handle: Arc<crate::client::Handle>,
    /// Transfer lifecycle status
    status: StateMachineStatus,
    /// The error, once the status is Failed. Also the lock that failure, cancellation and
    /// [`Self::complete_with`] change the status under, so neither failure nor cancellation
    /// lands inside `complete_with`. [`Self::set_completed`] does not take it.
    error: Arc<crate::runtime::sync::Mutex<Option<Box<error::Error>>>>,
    /// Completion signal sender - signals "state machine reached terminal state"
    completion_tx: Arc<Mutex<Option<StateMachineTerminalSender>>>,
    /// Set when `poll_work` returns `Pending`, cleared by a wake or resumed poll.
    wake_flag: Arc<wake_flag::WakeFlag>,
    /// The cause of the current park, if any.
    pending_cause: Arc<PendingCauseSlot>,
    /// Optional scheduler-visible pending interval accounting.
    pending_state: Option<Arc<TransferPendingState>>,
    /// Per-transfer metrics backing store
    pub(crate) metrics: Arc<MetricsState>,
    /// Emitter of this transfer's events, `None` unless a caller registered a sink.
    lifecycle: Option<Arc<crate::events::TransferLifecycle>>,
}

/// In-progress measurement for one logical transfer-manager request.
///
/// The guard publishes exactly once when finished or dropped. Drop publication
/// keeps early returns and cancellation paths from losing a request after it
/// started. A ranged discovery may carry the guard with its response body so
/// headers and validated body collection remain one logical request.
///
/// The published elapsed time is the sum of the intervals during which the
/// measurement was running. A measurement starts running and accrues nothing
/// while paused.
///
/// Time is read from [`tokio::time::Instant`], which follows the tokio test
/// clock when it is paused and the system monotonic clock otherwise.
#[derive(Debug)]
pub(crate) struct RequestMeasurement {
    ctx: TransferContext,
    /// Elapsed time accrued by completed running intervals.
    accrued: std::time::Duration,
    /// Start of the current running interval; `None` while paused.
    running_since: Option<tokio::time::Instant>,
    metrics: crate::metrics::RequestMetrics,
    published: bool,
}

impl RequestMeasurement {
    fn new(ctx: TransferContext) -> Self {
        Self {
            ctx,
            accrued: std::time::Duration::ZERO,
            running_since: Some(tokio::time::Instant::now()),
            metrics: crate::metrics::RequestMetrics::default(),
            published: false,
        }
    }

    /// Return the request aggregate updated by the retry loop.
    pub(crate) fn metrics_mut(&mut self) -> &mut crate::metrics::RequestMetrics {
        &mut self.metrics
    }

    /// Stop accruing elapsed time while the request's work is not in progress,
    /// e.g. while its response body waits for memory before being read.
    ///
    /// No-op when already paused.
    pub(crate) fn pause(&mut self) {
        if let Some(since) = self.running_since.take() {
            self.accrued += since.elapsed();
        }
    }

    /// Resume accruing elapsed time.
    ///
    /// No-op when already running.
    pub(crate) fn resume(&mut self) {
        if self.running_since.is_none() {
            self.running_since = Some(tokio::time::Instant::now());
        }
    }

    /// Elapsed time accrued so far, including the current running interval.
    fn elapsed(&self) -> std::time::Duration {
        self.accrued
            + self
                .running_since
                .map_or(std::time::Duration::ZERO, |s| s.elapsed())
    }

    /// Complete and publish this logical request.
    pub(crate) fn finish(mut self) -> crate::metrics::RequestMetrics {
        self.publish();
        self.metrics
    }

    fn publish(&mut self) {
        if self.published {
            return;
        }
        self.metrics.record_request(self.elapsed());
        self.ctx.record_request_metrics(&self.metrics);
        self.published = true;
    }
}

impl Drop for RequestMeasurement {
    fn drop(&mut self) {
        self.publish();
    }
}

/// Direction-specific attribution for one logical request measurement.
///
/// The common measurement records transfer totals. Implementations add the
/// same result to the request-kind aggregate owned by one transfer direction.
pub(crate) trait RequestMetricsAttribution: Clone {
    /// Direction-specific request classification.
    type Kind: Copy + std::fmt::Debug;

    /// Records one finished request under its direction-specific kind.
    fn record_request_metrics(&self, kind: Self::Kind, metrics: &crate::metrics::RequestMetrics);
}

/// Request measurement published to common and direction-specific aggregates.
///
/// The guard publishes exactly once when explicitly finished or dropped.
pub(crate) struct AttributedRequestMeasurement<A: RequestMetricsAttribution> {
    measurement: Option<RequestMeasurement>,
    attribution: A,
    kind: A::Kind,
}

impl<A: RequestMetricsAttribution> AttributedRequestMeasurement<A> {
    /// Starts a logical request associated with one direction-specific kind.
    pub(crate) fn new(ctx: &TransferContext, attribution: A, kind: A::Kind) -> Self {
        Self {
            measurement: Some(ctx.start_request_metrics()),
            attribution,
            kind,
        }
    }

    /// Returns the request metrics updated by the retry loop.
    pub(crate) fn metrics_mut(&mut self) -> &mut crate::metrics::RequestMetrics {
        self.measurement
            .as_mut()
            .expect("request measurement already finished")
            .metrics_mut()
    }

    /// Stop accruing elapsed time while the request's work is not in progress,
    /// e.g. while its response body waits for memory before being read.
    ///
    /// No-op when already paused.
    pub(crate) fn pause(&mut self) {
        self.measurement
            .as_mut()
            .expect("request measurement already finished")
            .pause();
    }

    /// Resume accruing elapsed time.
    ///
    /// No-op when already running.
    pub(crate) fn resume(&mut self) {
        self.measurement
            .as_mut()
            .expect("request measurement already finished")
            .resume();
    }

    /// Publishes the request measurement to both aggregate scopes.
    pub(crate) fn finish(mut self) -> crate::metrics::RequestMetrics {
        self.publish()
    }

    fn publish(&mut self) -> crate::metrics::RequestMetrics {
        let measurement = self
            .measurement
            .take()
            .expect("request measurement already finished");
        let metrics = measurement.finish();
        self.attribution.record_request_metrics(self.kind, &metrics);
        metrics
    }
}

impl<A: RequestMetricsAttribution> std::fmt::Debug for AttributedRequestMeasurement<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttributedRequestMeasurement")
            .field("kind", &self.kind)
            .field("finished", &self.measurement.is_none())
            .finish()
    }
}

impl<A: RequestMetricsAttribution> Drop for AttributedRequestMeasurement<A> {
    fn drop(&mut self) {
        if self.measurement.is_some() {
            let _ = self.publish();
        }
    }
}

/// Task wake adapter for futures polled from a transfer's synchronous state machine.
///
/// This deliberately enters the scheduler directly instead of using
/// [`TransferContext::try_wake`]. The scheduler's descriptor claim records a wake
/// that races the active `poll_work` call, so the future can register this waker
/// before `poll_work` returns `Pending` without relying on the edge-triggered
/// [`wake_flag::WakeFlag`].
struct SchedulerWake {
    scheduler: crate::scheduler::Scheduler,
    id: TransferId,
}

impl Wake for SchedulerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.scheduler.wake(self.id);
    }
}

impl fmt::Display for TransferContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer(id={}, status={})", self.id.id, self.status)
    }
}

impl fmt::Debug for TransferContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransferContext")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl TransferContext {
    /// Create a new transfer context.
    /// Returns the context and a receiver for terminal state notification.
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> (Self, StateMachineTerminalReceiver) {
        Self::new_inner(handle, next_transfer_id(), Arc::new(MetricsState::new()))
    }

    /// Returns a context + receiver for a child transfer of `parent`, linked by id and by
    /// metrics.
    ///
    /// Child transfers share the scheduler with their parent. `signal_terminal` on the
    /// child wakes the parent so the parent state machine can reap it, and
    /// `scheduler.cancel_transfer` on the parent cascades to children via the same
    /// linkage. Bytes the child records roll up into the parent's counters as they move.
    pub(crate) fn new_child(
        handle: Arc<crate::client::Handle>,
        parent: &TransferContext,
    ) -> (Self, StateMachineTerminalReceiver) {
        let mut id = next_transfer_id();
        id.parent = Some(parent.id.id);
        Self::new_inner(
            handle,
            id,
            Arc::new(MetricsState::with_parent(Some(parent.metrics.clone()))),
        )
    }

    fn new_inner(
        handle: Arc<crate::client::Handle>,
        id: TransferId,
        metrics: Arc<MetricsState>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let diagnostics = handle.config.diagnostics().transfer();
        let pending_state = diagnostics
            .enable_summaries()
            .then(|| Arc::new(TransferPendingState::new(diagnostics.events_enabled())));
        let ctx = Self {
            id,
            metrics,
            handle,
            status: StateMachineStatus::new(),
            error: Arc::new(crate::runtime::sync::Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            wake_flag: Arc::new(wake_flag::WakeFlag::new()),
            pending_cause: Arc::new(PendingCauseSlot::new()),
            pending_state,
            lifecycle: None,
        };
        (ctx, completion_rx)
    }

    /// Reports this transfer's events to `sink`. Only the returned context and its later
    /// clones emit, so attach the sink before cloning.
    pub(crate) fn with_events(
        mut self,
        sink: crate::events::TransferEventSink,
        transfer: crate::events::TransferRef,
    ) -> Self {
        self.lifecycle = Some(Arc::new(crate::events::TransferLifecycle::new(
            sink,
            transfer,
            Some(self.view()),
        )));
        self
    }

    /// Create a new transfer context with a specific ID (for testing).
    /// Returns the context and a receiver for terminal state notification.
    #[cfg(test)]
    pub(crate) fn with_id(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
    ) -> (Self, StateMachineTerminalReceiver) {
        Self::new_inner(handle, id, Arc::new(MetricsState::new()))
    }

    /// Record that `poll_work` is about to return `Pending`.
    ///
    /// The transfer's views report `cause` while it is active, until [`Self::begin_poll`]
    /// clears it. Storing it takes no part in the wake protocol below.
    ///
    /// # Wake primitive protocol
    ///
    /// The wake primitive is edge-triggered: a wake only fires if the
    /// pending flag is set at the moment the mutator calls `try_wake`. Lost-
    /// wake avoidance is a cooperation between two sides.
    ///
    /// ## The race this protocol prevents
    ///
    /// Without a shared lock, the poller and mutator can interleave:
    ///
    /// ```text
    /// Poller:                              Mutator:
    ///   check condition -> blocked
    ///                                        mutate state (now unblocking)
    ///                                        try_wake -> sees pending=false
    ///                                                 -> no-op
    ///   set_pending(true)
    ///   return Pending
    /// ```
    ///
    /// The state is unblocked, the poller is pending, and nothing will
    /// wake it. The transfer is stuck.
    ///
    /// ## How the lock prevents this
    ///
    /// Both the poller's condition check + `set_pending` and the mutator's
    /// state mutation happen under the same lock (the transfer's state
    /// mutex). This serializes them:
    ///
    /// - If the mutator acquires the lock first: it mutates state and
    ///   releases. The poller then acquires the lock, observes the new
    ///   state, and returns `Ready` (never calls `set_pending`).
    ///
    /// - If the poller acquires the lock first: it checks the condition
    ///   (still blocked), calls `set_pending`, and releases. The mutator
    ///   then acquires the lock, mutates, releases, and calls `try_wake`
    ///   which observes pending=true and fires the wake.
    ///
    /// The mutator's discipline is `lock → mutate → unlock → try_wake`.
    /// Calling `try_wake` after releasing the lock lets the woken poller
    /// acquire the lock immediately. `try_wake` is unconditionally cheap
    /// (a single atomic swap when no wake is pending).
    ///
    /// Spurious wakes (mutator fires when no poll was actually pending)
    /// are harmless — `wake` on a descriptor not in pending state is a
    /// no-op.
    #[inline]
    pub(crate) fn set_pending(&self, cause: impl Into<PendingCause>) {
        let cause = cause.into();
        self.wake_flag.set_pending();
        self.pending_cause.set(cause);
        if let Some(pending) = &self.pending_state {
            pending.record_pending(self.id, cause);
        }
        tracing::trace!(
            target: crate::telemetry::TARGET_SCHEDULING,
            tid = %self.id,
            category = cause.category().as_str(),
            reason = cause.reason(),
            "ctx.set_pending",
        );
    }

    /// Wake the scheduler if this transfer was marked `Pending`.
    ///
    /// Call after any state mutation that might unblock a pending poll,
    /// following the mutator pattern `lock → mutate → unlock → try_wake`.
    /// Idempotent: spurious calls cost only an atomic swap. See
    /// [`Self::set_pending`] for the full wake primitive protocol.
    #[inline]
    pub(crate) fn try_wake(&self) {
        if self.wake_flag.take_pending() {
            tracing::trace!(
                target: crate::telemetry::TARGET_SCHEDULING,
                tid = %self.id,
                "ctx.try_wake.fired",
            );
            self.wake();
        } else {
            tracing::trace!(
                target: crate::telemetry::TARGET_SCHEDULING,
                tid = %self.id,
                "ctx.try_wake.skipped",
            );
        }
    }

    /// Unconditionally notify the scheduler that this transfer should be polled.
    ///
    /// Futures registered from `poll_work` use this path because their wake may
    /// race before or after the transfer publishes `Pending`. The descriptor's
    /// wake-requested protocol retains either ordering.
    #[inline]
    pub(crate) fn wake(&self) {
        self.handle.scheduler.wake(self.id);
    }

    /// Returns a task waker that requeues this transfer in the scheduler.
    ///
    /// Use this for a `Future` polled inside `poll_work`. Its wake is level-like
    /// at the scheduler boundary: a wake concurrent with the current poll is
    /// retained by the descriptor's release-and-recheck protocol. Ordinary
    /// state mutations should continue to use [`Self::set_pending`] and
    /// [`Self::try_wake`] under their shared state lock.
    pub(crate) fn waker(&self) -> Waker {
        Waker::from(Arc::new(SchedulerWake {
            scheduler: self.handle.scheduler.clone(),
            id: self.id,
        }))
    }

    /// Reconcile the previous pending interval before the scheduler polls again.
    ///
    /// Clearing the edge-triggered flag here also handles registered-future
    /// wakes, which enter through [`Self::waker`] rather than [`Self::try_wake`].
    /// The cause [`Self::set_pending`] stored is cleared too.
    pub(crate) fn begin_poll(&self) {
        self.wake_flag.take_pending();
        self.pending_cause.clear();
        if let Some(pending) = &self.pending_state {
            pending.record_poll_started(self.id);
        }
    }

    /// Record the first scheduler wake observed for the current pending interval.
    pub(crate) fn record_wake(&self) {
        if let Some(pending) = &self.pending_state {
            pending.record_wake(self.id);
        }
    }

    /// Return optional scheduler-visible pending statistics.
    pub(crate) fn pending_stats(&self) -> Option<TransferPendingStats> {
        self.pending_state
            .as_ref()
            .map(|pending| pending.snapshot())
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn s3_client(&self) -> &aws_sdk_s3::Client {
        &self.handle.s3_client
    }

    /// Mark transfer as failed and store the error.
    /// First-write-wins - returns true if this call set the status.
    ///
    /// Setting the status alone is not observable to the owning handle: a
    /// terminal transition must be signalled via [`signal_terminal`] (directly,
    /// or once in-flight work drains). For the common fail-and-abort case use
    /// [`set_failed_and_signal`], which bundles both.
    ///
    /// The status changes while the error slot is locked, so a reader that sees `Failed`
    /// and then locks the slot finds the error, unless it has been taken.
    ///
    /// [`signal_terminal`]: Self::signal_terminal
    /// [`set_failed_and_signal`]: Self::set_failed_and_signal
    pub(crate) fn set_failed(&self, err: impl Into<error::Error>) -> bool {
        if fail_with(&self.status, &self.error, || Box::new(err.into())) {
            self.metrics.set_finished();
            true
        } else {
            false
        }
    }

    /// Mark transfer as completed.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_completed(&self) -> bool {
        if self.status.set_completed() {
            self.metrics.set_finished();
            true
        } else {
            false
        }
    }

    /// Runs `publish`, then marks the transfer completed, with no failure or cancellation able
    /// to land in between: one that arrives while `publish` runs waits for it. Skips `publish`
    /// and returns `Ok(false)` if the transfer has already left `Active`. A `publish` error is
    /// returned with the transfer still `Active`, so `publish` must make nothing visible when
    /// it fails.
    pub(crate) fn complete_with<E>(
        &self,
        publish: impl FnOnce() -> Result<(), E>,
    ) -> Result<bool, E> {
        let completed = complete_with(&self.status, &self.error, publish)?;
        if completed {
            self.metrics.set_finished();
        }
        Ok(completed)
    }

    /// Mark transfer as cancelled.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_cancelled(&self) -> bool {
        if cancel_with(&self.status, &self.error) {
            self.metrics.set_finished();
            true
        } else {
            false
        }
    }

    /// Take the error if the transfer failed. `None` if it did not fail or the error was
    /// already taken. The error belongs to the operation's result; [`Self::error_kind`] reads
    /// the kind without taking it.
    pub(crate) fn take_error(&self) -> Option<error::Error> {
        if self.status.is_failed() {
            self.error.lock().take().map(|e| *e)
        } else {
            None
        }
    }

    /// The event outcome this transfer's terminal status implies, read without taking the
    /// error. Requires a terminal status: a still-`Active` transfer maps to `Cancelled`.
    pub(crate) fn terminal_outcome(&self) -> crate::events::Outcome {
        match self.transfer_status() {
            crate::types::TransferStatus::Completed => crate::events::Outcome::Succeeded {},
            crate::types::TransferStatus::Failed => crate::events::Outcome::Failed {},
            _ => crate::events::Outcome::Cancelled {},
        }
    }

    /// Emits [`TransferEvent::Started`](crate::events::TransferEvent::Started) if a sink
    /// is registered, and makes the matching `Finished` owed.
    pub(crate) fn announce(&self) {
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.announce(self.id);
        }
    }

    /// Emits [`TransferEvent::Finished`](crate::events::TransferEvent::Finished) with the
    /// outcome the terminal status implies, once per [`Self::announce`]. Requires a terminal
    /// status; call it before [`Self::signal_terminal`] so the event is sent before any joiner
    /// wakes.
    pub(crate) fn emit_finished(&self) {
        if let Some(emit) = self
            .lifecycle
            .as_ref()
            .and_then(|lifecycle| lifecycle.finish(self.id, self.terminal_outcome()))
        {
            emit.send();
        }
    }

    /// Peek at the error kind if transfer failed. `None` if it did not fail or the error
    /// was already taken.
    pub(crate) fn error_kind(&self) -> Option<error::ErrorKind> {
        if self.status.is_failed() {
            self.error.lock().as_ref().map(|e| e.kind().clone())
        } else {
            None
        }
    }

    /// Check if transfer has failed
    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.status.is_failed()
    }

    /// Check if transfer was cancelled
    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.status.is_cancelled()
    }

    /// Check if transfer is still active (not completed, failed, or cancelled)
    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.status.is_active()
    }

    /// Signal that the transfer has reached a terminal state.
    ///
    /// Call this after `set_completed()`/`set_failed()`/`set_cancelled()`. Wakes
    /// any waiters (`join()`) and the parent transfer (so it can reap this child).
    ///
    /// Setting a terminal status is not enough on its own. A transfer that has
    /// gone terminal but returns idle to the scheduler WITHOUT signalling can be
    /// removed by the scheduler with its completion channel never fired — a
    /// permanent hang of the owning handle, not merely a delay. Every terminal
    /// transition must therefore reach exactly one `signal_terminal`. It is safe
    /// to call while in-flight work is still draining.
    pub(crate) fn signal_terminal(&self) {
        self.finalize_terminal_metrics();
        if let Some(tx) = self.completion_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        // Wake the parent transfer (if any) so it can reap this child.
        if let Some(parent) = self.id.parent() {
            self.handle.scheduler.wake(parent);
        }
    }

    /// Closes common timing intervals before a terminal diagnostic is emitted.
    ///
    /// This operation is idempotent. State machines may call it before
    /// [`Self::signal_terminal`] so their terminal summary observes the same
    /// finished timestamp and pending interval that the owning handle will see.
    pub(crate) fn finalize_terminal_metrics(&self) {
        if let Some(pending) = &self.pending_state {
            pending.record_terminal(self.id);
        }
        self.wake_flag.take_pending();
        self.metrics.set_finished();
    }

    /// Mark the transfer failed and immediately signal terminal (the
    /// fail-and-abort path: the transfer is terminal now, with nothing to wait
    /// for — see [`signal_terminal`]). Bundling the two calls keeps an abort site
    /// from setting the status without making the transition observable. When the
    /// signal must instead wait for in-flight work to drain, call [`set_failed`]
    /// alone and signal once that work completes.
    ///
    /// Returns the result of [`set_failed`] (first-write-wins: `true` if this
    /// call set the status).
    ///
    /// [`signal_terminal`]: Self::signal_terminal
    /// [`set_failed`]: Self::set_failed
    pub(crate) fn set_failed_and_signal(&self, err: impl Into<error::Error>) -> bool {
        let set = self.set_failed(err);
        self.signal_terminal();
        set
    }

    /// Record an IO sample to per-transfer metrics and per-client telemetry.
    pub(crate) fn record_io(&self, sample: &crate::metrics::IoSample) {
        self.metrics.record_io(sample);
        self.handle.telemetry.io_counters.record(sample);
    }

    /// Record one request in the transfer aggregate.
    pub(crate) fn record_request_metrics(&self, metrics: &crate::metrics::RequestMetrics) {
        self.metrics.record_request(metrics);
    }

    /// Start measuring one logical service request.
    pub(crate) fn start_request_metrics(&self) -> RequestMeasurement {
        RequestMeasurement::new(self.clone())
    }

    /// Set the expected total payload bytes for this transfer.
    pub(crate) fn set_total_bytes(&self, n: u64) {
        self.metrics.set_total_bytes(n);
    }

    /// Record `n` as a provisional byte total, until [`Self::set_total_bytes`] sets the final
    /// one.
    pub(crate) fn set_expected_bytes(&self, n: u64) {
        self.metrics.add_discovered(n, 0);
    }

    /// Get current transfer status as a public enum.
    pub(crate) fn transfer_status(&self) -> crate::types::TransferStatus {
        self.status.status()
    }

    /// Claims the single terminal tracing record for this transfer.
    pub(crate) fn claim_terminal_report(&self) -> bool {
        self.metrics.claim_terminal_report()
    }

    /// Snapshot current transfer metrics.
    pub(crate) fn metrics(&self) -> crate::types::TransferMetrics {
        self.metrics.snapshot()
    }

    /// A read-only view of this transfer's counters, status and pending cause. It holds no
    /// `handle`, so a view a consumer keeps cannot delay runtime shutdown.
    pub(crate) fn view(&self) -> crate::types::TransferView {
        crate::types::TransferView::new(
            self.metrics.clone(),
            self.status.clone(),
            self.pending_cause.clone(),
        )
    }

    /// Get scheduling controls for this transfer.
    pub(crate) fn scheduling(&self) -> SchedulingCtl<'_> {
        SchedulingCtl { ctx: self }
    }
}

/// Scheduling controls for a transfer.
///
/// Provides methods to adjust how this transfer is scheduled relative to others.
/// Obtained via [`UploadHandle::scheduling()`] or [`DownloadHandle::scheduling()`].
///
/// [`UploadHandle::scheduling()`]: crate::operation::upload::UploadHandle::scheduling
/// [`DownloadHandle::scheduling()`]: crate::operation::download::DownloadHandle::scheduling
#[derive(Debug)]
pub struct SchedulingCtl<'a> {
    ctx: &'a TransferContext,
}

impl SchedulingCtl<'_> {
    /// Set the priority of this transfer.
    ///
    /// Priority affects how work is scheduled relative to other transfers:
    /// - Higher priority (255) = more work share, runs more often
    /// - Lower priority (1) = less work share, runs less often
    /// - Default priority is 128
    ///
    /// The scheduler uses CFS-style fair scheduling. Priority affects how fast
    /// "virtual runtime" accumulates - higher priority transfers accumulate slower,
    /// so they stay ahead in scheduling and get more work done.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = tm.download()
    ///     .bucket("my-bucket")
    ///     .key("my-key")
    ///     .initiate()
    ///     .await?;
    ///
    /// // Start as lower priority background transfer
    /// handle.scheduling().set_priority(64);
    ///
    /// // ...
    /// // User requests this data - boost priority
    /// handle.scheduling().set_priority(255);
    /// ```
    pub fn set_priority(&self, priority: u8) {
        self.ctx
            .handle
            .scheduler
            .set_priority(self.ctx.id, priority);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod state_machine_status {
        use super::*;

        #[test]
        fn starts_active() {
            let s = StateMachineStatus::new();
            assert!(s.is_active());
            assert!(!s.is_failed());
            assert!(!s.is_cancelled());
        }

        #[test]
        fn set_completed_from_active() {
            let s = StateMachineStatus::new();
            assert!(s.set_completed());
            assert!(!s.is_active());
            assert!(!s.is_failed());
            assert!(!s.is_cancelled());
        }

        #[test]
        fn set_failed_from_active() {
            let s = StateMachineStatus::new();
            assert!(s.set_failed());
            assert!(!s.is_active());
            assert!(s.is_failed());
        }

        #[test]
        fn set_cancelled_from_active() {
            let s = StateMachineStatus::new();
            assert!(s.set_cancelled());
            assert!(!s.is_active());
            assert!(s.is_cancelled());
        }

        #[test]
        fn only_first_transition_wins() {
            let s = StateMachineStatus::new();
            assert!(s.set_failed());
            // second transition fails — already terminal
            assert!(!s.set_completed());
            assert!(!s.set_cancelled());
            assert!(s.is_failed());
        }

        #[test]
        fn completed_blocks_other_transitions() {
            let s = StateMachineStatus::new();
            assert!(s.set_completed());
            assert!(!s.set_failed());
            assert!(!s.set_cancelled());
        }

        #[test]
        fn display() {
            let s = StateMachineStatus::new();
            assert_eq!(s.to_string(), "Active");
            s.set_failed();
            assert_eq!(s.to_string(), "Failed");
        }
    }

    mod transfer_status {
        use crate::types::TransferStatus;

        #[test]
        fn is_terminal() {
            assert!(!TransferStatus::Active.is_terminal());
            assert!(TransferStatus::Completed.is_terminal());
            assert!(TransferStatus::Failed.is_terminal());
            assert!(TransferStatus::Cancelled.is_terminal());
        }
    }

    mod transfer_context_status_and_metrics {
        use super::*;
        use std::sync::Arc;

        fn test_handle() -> Arc<crate::client::Handle> {
            let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
            let config = crate::Config::builder().sdk_client(s3_client).build();
            crate::client::Handle::new_for_test(config, 4)
        }

        fn test_handle_with_diagnostics(detail: u64) -> Arc<crate::client::Handle> {
            let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
            let config = crate::Config::builder()
                .sdk_client(s3_client)
                .diagnostics_for_test(crate::config::MemoryDiagnosticsConfig::default(), detail)
                .build();
            crate::client::Handle::new_for_test(config, 4)
        }

        #[derive(Clone, Default)]
        struct TestRequestAttribution {
            records: Arc<Mutex<Vec<(u8, crate::metrics::RequestMetrics)>>>,
        }

        impl RequestMetricsAttribution for TestRequestAttribution {
            type Kind = u8;

            fn record_request_metrics(
                &self,
                kind: Self::Kind,
                metrics: &crate::metrics::RequestMetrics,
            ) {
                self.records.lock().unwrap().push((kind, *metrics));
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn status_transitions() {
            let handle = test_handle();

            // Completed
            let (ctx, _rx) = TransferContext::new(handle.clone());
            assert_eq!(ctx.transfer_status(), crate::types::TransferStatus::Active);
            ctx.set_completed();
            ctx.signal_terminal();
            assert_eq!(
                ctx.transfer_status(),
                crate::types::TransferStatus::Completed
            );

            // Failed
            let (ctx, _rx) = TransferContext::new(handle.clone());
            ctx.set_failed(crate::error::Error::new(
                crate::error::ErrorKind::InputInvalid,
                "test",
            ));
            ctx.signal_terminal();
            assert_eq!(ctx.transfer_status(), crate::types::TransferStatus::Failed);

            // Cancelled
            let (ctx, _rx) = TransferContext::new(handle);
            ctx.set_cancelled();
            ctx.signal_terminal();
            assert_eq!(
                ctx.transfer_status(),
                crate::types::TransferStatus::Cancelled
            );
        }

        /// A child's `parent()` is its operation's own id, so a consumer can pair each
        /// object of a directory operation with the operation's events.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn a_child_id_names_its_operation_as_parent() {
            let handle = test_handle();
            let (operation, _rx) = TransferContext::new(handle.clone());
            let (child, _child_rx) = TransferContext::new_child(handle, &operation);
            assert_eq!(None, operation.id.parent(), "the operation has no parent");
            assert_eq!(
                Some(operation.id),
                child.id.parent(),
                "a child's parent must equal the operation's own id"
            );
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn stats_initial_values() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            let stats = ctx.metrics();
            assert_eq!(stats.network_tx, 0);
            assert_eq!(stats.network_rx, 0);
            assert_eq!(stats.disk_read, 0);
            assert_eq!(stats.disk_write, 0);
            assert_eq!(stats.bytes_transferred, 0);
            assert_eq!(stats.total_bytes, None);
            assert!(stats.started_at.elapsed().as_secs() < 1);
            assert!(stats.finished_at.is_none());
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn stats_record_io() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            ctx.record_io(&crate::metrics::IoSample {
                network_tx: 100,
                network_rx: 200,
                disk_read: 300,
                disk_write: 400,
                bytes_transferred: 500,
            });
            ctx.record_io(&crate::metrics::IoSample {
                network_tx: 10,
                network_rx: 20,
                disk_read: 30,
                disk_write: 40,
                bytes_transferred: 50,
            });
            let stats = ctx.metrics();
            assert_eq!(stats.network_tx, 110);
            assert_eq!(stats.network_rx, 220);
            assert_eq!(stats.disk_read, 330);
            assert_eq!(stats.disk_write, 440);
            assert_eq!(stats.bytes_transferred, 550);
        }

        /// Each terminal status maps to its own outcome, and deriving the outcome leaves a
        /// failed transfer's error in place.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn terminal_outcome_derives_each_terminal() {
            let (ctx, _rx) = TransferContext::new(test_handle());
            assert!(ctx.set_completed());
            assert!(
                matches!(
                    ctx.terminal_outcome(),
                    crate::events::Outcome::Succeeded { .. }
                ),
                "a completed transfer licenses deleting a source"
            );

            let (ctx, _rx) = TransferContext::new(test_handle());
            assert!(ctx.set_cancelled());
            assert!(
                matches!(
                    ctx.terminal_outcome(),
                    crate::events::Outcome::Cancelled { .. }
                ),
                "a cancelled transfer licenses no retry and no alert"
            );

            let (ctx, _rx) = TransferContext::new(test_handle());
            assert!(ctx.set_failed(crate::error::Error::new(
                crate::error::ErrorKind::ObjectNotDiscoverable,
                "the real cause",
            )));
            match ctx.terminal_outcome() {
                crate::events::Outcome::Failed { .. } => {}
                other => panic!("a failed transfer must report Failed, got {other:?}"),
            }
            // A derivation that took the error would leave `join()` nothing to return.
            let err = ctx
                .take_error()
                .expect("deriving the outcome must leave the error for join()");
            assert!(
                matches!(err.kind(), crate::error::ErrorKind::ObjectNotDiscoverable),
                "the joiner gets the real cause, got {:?}",
                err.kind()
            );
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn request_metrics_merge_into_transfer_aggregate() {
            let handle = test_handle();
            assert!(!handle.config.diagnostics().transfer().enable_summaries());
            let (ctx, _rx) = TransferContext::new(handle);
            let req_metrics = crate::metrics::RequestMetrics {
                requests: 1,
                elapsed: std::time::Duration::from_millis(15),
                max_elapsed: std::time::Duration::from_millis(15),
                retry_reissues: 2,
                throttle_reissues: 3,
                hedge_reissues: 1,
                retry_exhaustions: 1,
                backoff_duration: std::time::Duration::from_millis(9),
            };

            ctx.record_request_metrics(&req_metrics);

            assert_eq!(ctx.metrics.request_metrics(), req_metrics);
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn request_measurement_publishes_once_on_finish_or_drop() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);

            ctx.start_request_metrics().finish();
            drop(ctx.start_request_metrics());

            assert_eq!(ctx.metrics.request_metrics().requests, 2);
        }

        #[cfg_attr(miri, ignore)]
        #[tokio::test(start_paused = true)]
        async fn request_measurement_excludes_paused_time() {
            use std::time::Duration;
            let (ctx, _rx) = TransferContext::new(test_handle());

            let mut measurement = ctx.start_request_metrics();
            tokio::time::advance(Duration::from_secs(1)).await;
            measurement.pause();
            tokio::time::advance(Duration::from_secs(10)).await;
            measurement.resume();
            tokio::time::advance(Duration::from_secs(2)).await;
            let metrics = measurement.finish();

            assert_eq!(metrics.elapsed, Duration::from_secs(3));
            assert_eq!(metrics.max_elapsed, Duration::from_secs(3));
            assert_eq!(
                ctx.metrics.request_metrics().elapsed,
                Duration::from_secs(3)
            );
        }

        #[cfg_attr(miri, ignore)]
        #[tokio::test(start_paused = true)]
        async fn request_measurement_repeated_pause_and_resume_are_noops() {
            use std::time::Duration;
            let (ctx, _rx) = TransferContext::new(test_handle());

            let mut measurement = ctx.start_request_metrics();
            tokio::time::advance(Duration::from_secs(1)).await;
            measurement.pause();
            tokio::time::advance(Duration::from_secs(5)).await;
            // A second pause must not accrue the paused interval.
            measurement.pause();
            tokio::time::advance(Duration::from_secs(5)).await;
            measurement.resume();
            tokio::time::advance(Duration::from_secs(1)).await;
            // A second resume must not restart the running interval.
            measurement.resume();
            tokio::time::advance(Duration::from_secs(1)).await;

            assert_eq!(measurement.finish().elapsed, Duration::from_secs(3));
        }

        #[cfg_attr(miri, ignore)]
        #[tokio::test(start_paused = true)]
        async fn request_measurement_published_while_paused_counts_active_time() {
            use std::time::Duration;
            let (ctx, _rx) = TransferContext::new(test_handle());

            let mut finished = ctx.start_request_metrics();
            tokio::time::advance(Duration::from_secs(2)).await;
            finished.pause();
            tokio::time::advance(Duration::from_secs(10)).await;
            assert_eq!(finished.finish().elapsed, Duration::from_secs(2));

            let mut dropped = ctx.start_request_metrics();
            tokio::time::advance(Duration::from_secs(3)).await;
            dropped.pause();
            tokio::time::advance(Duration::from_secs(10)).await;
            drop(dropped);

            let total = ctx.metrics.request_metrics();
            assert_eq!(total.requests, 2);
            assert_eq!(total.elapsed, Duration::from_secs(5));
            assert_eq!(total.max_elapsed, Duration::from_secs(3));
        }

        #[cfg_attr(miri, ignore)]
        #[tokio::test(start_paused = true)]
        async fn attributed_request_measurement_forwards_pause_and_resume() {
            use std::time::Duration;
            let (ctx, _rx) = TransferContext::new(test_handle());
            let attribution = TestRequestAttribution::default();

            let mut measurement = AttributedRequestMeasurement::new(&ctx, attribution.clone(), 1);
            tokio::time::advance(Duration::from_secs(1)).await;
            measurement.pause();
            tokio::time::advance(Duration::from_secs(10)).await;
            measurement.resume();
            tokio::time::advance(Duration::from_secs(1)).await;
            drop(measurement);

            let records = attribution.records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].1.elapsed, Duration::from_secs(2));
            assert_eq!(
                ctx.metrics.request_metrics().elapsed,
                Duration::from_secs(2)
            );
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn attributed_request_measurement_publishes_to_both_aggregates() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            let attribution = TestRequestAttribution::default();

            let mut finished = AttributedRequestMeasurement::new(&ctx, attribution.clone(), 7);
            finished
                .metrics_mut()
                .record_retry_reissue(std::time::Duration::from_millis(3));
            let finished_metrics = finished.finish();

            drop(AttributedRequestMeasurement::new(
                &ctx,
                attribution.clone(),
                9,
            ));

            let records = attribution.records.lock().unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], (7, finished_metrics));
            assert_eq!(records[1].0, 9);
            assert_eq!(records[1].1.requests, 1);
            assert_eq!(ctx.metrics.request_metrics().requests, 2);
            assert_eq!(ctx.metrics.request_metrics().retry_reissues, 1);
        }

        // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn disabled_diagnostics_do_not_allocate_pending_stats() {
            let (ctx, _rx) = TransferContext::new(test_handle());
            ctx.set_pending(PendingCause::new(PendingCategory::Memory, "test"));
            ctx.record_wake();
            ctx.begin_poll();

            assert_eq!(ctx.pending_stats(), None);
        }

        // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn pending_cause_is_accounted_across_wake_and_next_poll() {
            let (ctx, _rx) = TransferContext::new(test_handle_with_diagnostics(1));
            ctx.set_pending(PendingCause::new(PendingCategory::Memory, "test"));
            ctx.record_wake();
            ctx.record_wake();
            ctx.begin_poll();

            let stats = ctx.pending_stats().expect("summary diagnostics enabled");
            let memory = stats.category(PendingCategory::Memory);
            assert_eq!(memory.count, 1);
            assert_eq!(stats.terminal_cause, None);
        }

        // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn terminal_signal_closes_pending_interval_once() {
            let (ctx, _rx) = TransferContext::new(test_handle_with_diagnostics(1));
            let cause = PendingCause::in_flight_work("test");
            ctx.set_pending(cause);
            ctx.set_cancelled();
            ctx.signal_terminal();
            ctx.signal_terminal();

            let stats = ctx.pending_stats().expect("summary diagnostics enabled");
            assert_eq!(stats.terminal_cause, Some(cause));
            assert_eq!(stats.category(PendingCategory::InFlightWork).count, 1);
        }

        // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn terminal_report_can_be_claimed_once() {
            let (ctx, _rx) = TransferContext::new(test_handle());

            assert!(ctx.claim_terminal_report());
            assert!(!ctx.claim_terminal_report());
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn finished_at_set_on_terminal() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            assert!(ctx.metrics().finished_at.is_none());
            ctx.set_completed();
            ctx.signal_terminal();
            assert!(ctx.metrics().finished_at.is_some());
        }

        // ref: awslabs/aws-s3-transfer-manager-rs#197
        /// `finished_at` is stamped by the status transition itself, not by
        /// `signal_terminal`.
        ///
        /// No transition here is followed by `signal_terminal`, so the test fails if the
        /// stamp is made only there.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn finished_at_set_without_signal_terminal() {
            for transition in ["cancelled", "completed", "failed"] {
                let (ctx, _rx) = TransferContext::new(test_handle());
                assert!(ctx.metrics().finished_at.is_none());
                match transition {
                    "cancelled" => assert!(ctx.set_cancelled()),
                    "completed" => assert!(ctx.set_completed()),
                    _ => assert!(ctx.set_failed(crate::error::Error::new(
                        crate::error::ErrorKind::ChildOperationFailed,
                        "test",
                    ))),
                }
                assert!(
                    ctx.metrics().finished_at.is_some(),
                    "{transition} must stamp finished_at without signal_terminal"
                );
                assert!(ctx.transfer_status().is_terminal());
            }
        }

        /// The stamp is first-write-wins, like the status CAS it rides on, so a losing
        /// transition cannot move a time the winner already published.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn finished_at_is_first_write_wins() {
            let (ctx, _rx) = TransferContext::new(test_handle());
            assert!(ctx.set_completed());
            let first = ctx.metrics().finished_at.expect("stamped by set_completed");
            assert!(!ctx.set_cancelled(), "second transition must lose");
            ctx.signal_terminal();
            assert_eq!(
                Some(first),
                ctx.metrics().finished_at,
                "a losing transition, and signal_terminal, must not re-stamp"
            );
        }

        // ref: awslabs/aws-s3-transfer-manager-rs#197
        /// A child's bytes reach its parent as they are recorded, and the parent equals
        /// the sum of its children once everything is quiescent.
        ///
        /// No child finishes here, so a parent that collected a child's bytes only when
        /// the child finished, or only when it succeeded, reads zero.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn record_io_rolls_up_into_the_parent() {
            let sample = |n: u64| crate::metrics::IoSample {
                network_tx: n,
                network_rx: 2 * n,
                disk_read: 3 * n,
                disk_write: 4 * n,
                bytes_transferred: 5 * n,
            };

            let parent = Arc::new(MetricsState::new());
            let child_a = MetricsState::with_parent(Some(parent.clone()));
            let child_b = MetricsState::with_parent(Some(parent.clone()));

            child_a.record_io(&sample(10));
            child_b.record_io(&sample(7));
            child_a.record_io(&sample(3));

            // Each child keeps its own total.
            assert_eq!(13, child_a.snapshot().network_tx);
            assert_eq!(7, child_b.snapshot().network_tx);

            // The parent is the sum, on every counter.
            let p = parent.snapshot();
            assert_eq!(20, p.network_tx);
            assert_eq!(40, p.network_rx);
            assert_eq!(60, p.disk_read);
            assert_eq!(80, p.disk_write);
            assert_eq!(100, p.bytes_transferred);
        }

        /// A sample reaches every ancestor, not only the immediate parent.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn record_io_rolls_up_through_more_than_one_level() {
            let root = Arc::new(MetricsState::new());
            let mid = Arc::new(MetricsState::with_parent(Some(root.clone())));
            let leaf = MetricsState::with_parent(Some(mid.clone()));

            leaf.record_io(&crate::metrics::IoSample {
                network_tx: 5,
                network_rx: 0,
                disk_read: 0,
                disk_write: 0,
                bytes_transferred: 0,
            });

            assert_eq!(5, leaf.snapshot().network_tx);
            assert_eq!(5, mid.snapshot().network_tx);
            assert_eq!(5, root.snapshot().network_tx, "the root must see it too");
        }

        /// The rollup carries bytes only: a child's `total_bytes` and `finished_at` stay its
        /// own, so a child finishing does not finish its parent.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn rollup_does_not_propagate_total_bytes_or_finished_at() {
            let parent = Arc::new(MetricsState::new());
            let child = MetricsState::with_parent(Some(parent.clone()));

            child.set_total_bytes(999);
            child.set_finished();

            assert_eq!(Some(999), child.snapshot().total_bytes);
            assert_eq!(None, parent.snapshot().total_bytes);
            assert!(child.snapshot().finished_at.is_some());
            assert!(parent.snapshot().finished_at.is_none());
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn set_total_bytes() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            assert_eq!(ctx.metrics().total_bytes, None);
            ctx.set_total_bytes(42);
            assert_eq!(ctx.metrics().total_bytes, Some(42));
        }

        /// Only IO samples roll up: a child's expected bytes stay out of its parent's byte
        /// total.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn discovered_bytes_do_not_roll_up() {
            let parent = Arc::new(MetricsState::new());
            let child = MetricsState::with_parent(Some(parent.clone()));

            child.add_discovered(7, 1);

            assert_eq!(crate::types::Total::Provisional(7), child.byte_total());
            assert_eq!(crate::types::Total::Unknown, parent.byte_total());
        }

        /// A single-object transfer has no entry total and no settled entries: it is one
        /// entry, not a set of them.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn a_leaf_has_no_entry_total() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            ctx.set_total_bytes(512);

            let view = ctx.view();
            assert_eq!(crate::types::Total::Final(512), view.byte_total());
            assert_eq!(
                crate::types::Total::Unknown,
                view.entry_total(),
                "a single-object transfer enumerates nothing"
            );
            assert_eq!(0, view.entries_settled());
        }

        /// A view outlives the context it came from, and still reads the live counters.
        ///
        /// `join(self)` consumes the operation handle, and a view must stay readable after
        /// it.
        #[cfg_attr(miri, ignore)]
        #[test]
        fn a_view_outlives_its_context_and_still_reads() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            let view = ctx.view();

            ctx.record_io(&crate::metrics::IoSample {
                network_tx: 11,
                network_rx: 0,
                disk_read: 0,
                disk_write: 0,
                bytes_transferred: 0,
            });
            ctx.set_total_bytes(11);
            drop(ctx);

            assert_eq!(11, view.metrics().network_tx);
            assert_eq!(crate::types::Total::Final(11), view.byte_total());
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::wake_flag::WakeFlag;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A status read while `set_cancelled` lands is never `Completed`.
    ///
    /// Nothing here sets completed, so `Completed` can only come from a derivation that
    /// loads the status more than once and sees the cancel land between its loads.
    #[test]
    fn status_loom_never_reports_completed_for_a_cancelled_transfer() {
        loom::model(|| {
            let status = super::StateMachineStatus::new();

            let writer = {
                let status = status.clone();
                thread::spawn(move || assert!(status.set_cancelled()))
            };

            let observed = status.status();

            writer.join().unwrap();

            assert_ne!(
                crate::types::TransferStatus::Completed,
                observed,
                "nothing set Completed, and the derived status reads Completed"
            );
        });
    }

    /// The real [`StateMachineStatus`](super::StateMachineStatus) and error slot type, both
    /// loom-backed under `s3_tm_loom`, moved through the [`fail_with`](super::fail_with),
    /// [`cancel_with`](super::cancel_with) and [`complete_with`](super::complete_with) that
    /// `TransferContext` calls, and read as its readers read them.
    ///
    /// Stands in for `TransferContext`, which needs a client `Handle` to build.
    struct StatusHarness {
        status: super::StateMachineStatus,
        error: crate::runtime::sync::Mutex<Option<u32>>,
    }

    impl StatusHarness {
        fn new() -> Self {
            Self {
                status: super::StateMachineStatus::new(),
                error: crate::runtime::sync::Mutex::new(None),
            }
        }

        fn set_failed(&self, err: u32) -> bool {
            super::fail_with(&self.status, &self.error, || err)
        }

        fn set_cancelled(&self) -> bool {
            super::cancel_with(&self.status, &self.error)
        }

        /// Completes with a `publish` that sets `published`, as a download's rename puts its
        /// object at the destination.
        fn complete(&self, published: &AtomicBool) -> bool {
            super::complete_with(&self.status, &self.error, || {
                published.store(true, Ordering::SeqCst);
                Ok::<(), ()>(())
            })
            .unwrap()
        }

        /// A reader's ordering, as in `take_error` and `error_kind`: check the status, then
        /// take the lock.
        ///
        /// `None` if the reader did not see `Failed`, otherwise what the slot held.
        /// `Some(None)` is the defect: `Failed` with an empty slot.
        fn error(&self) -> Option<Option<u32>> {
            if self.status.is_failed() {
                Some(*self.error.lock())
            } else {
                None
            }
        }
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// Two callers race to fail one transfer while a third reads. Exactly one wins, and
    /// the reader never sees `Failed` with an empty error slot.
    ///
    /// Publishing the status outside the slot's lock opens a window between the CAS and
    /// the store, and a reader landing there fails the test.
    #[test]
    fn set_failed_loom_publishes_status_after_its_error() {
        loom::model(|| {
            let h = Arc::new(StatusHarness::new());

            let h2 = h.clone();
            let first = thread::spawn(move || h2.set_failed(1));

            let h3 = h.clone();
            let second = thread::spawn(move || h3.set_failed(2));

            let h4 = h.clone();
            let reader = thread::spawn(move || h4.error());

            let won_first = first.join().unwrap();
            let won_second = second.join().unwrap();
            let seen = reader.join().unwrap();

            assert!(
                won_first ^ won_second,
                "exactly one caller wins the CAS: first={won_first} second={won_second}"
            );

            // The lock keeps the reader out of the window between the winner's CAS and its
            // store, so a reader that saw `Failed` finds an error.
            if let Some(slot) = seen {
                let err = slot.expect(
                    "observed Failed with an empty error slot: the status was published \
                     before the error that explains it",
                );
                assert!(
                    err == 1 || err == 2,
                    "a reader that saw Failed must see one of the two real errors, got {err}"
                );
            }

            // And once every writer is done, the error is unconditionally there.
            assert_eq!(
                Some(true),
                h.error().map(|slot| slot.is_some()),
                "a Failed status must always have an error behind it"
            );
        });
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#197
    /// A completion that publishes a result races a cancel and a failure. Exactly one wins,
    /// and the result is published only if the completion won.
    ///
    /// A cancel or failure that changed the status outside the lock could land after
    /// `publish` and before the completion, leaving the result behind another status: a
    /// download's object renamed over the caller's file and reported as cancelled.
    #[test]
    fn complete_with_loom_publishes_only_for_a_completed_transfer() {
        loom::model(|| {
            let h = Arc::new(StatusHarness::new());
            let published = Arc::new(AtomicBool::new(false));

            let completer = {
                let (h, published) = (h.clone(), published.clone());
                thread::spawn(move || h.complete(&published))
            };
            let h2 = h.clone();
            let canceller = thread::spawn(move || h2.set_cancelled());
            let h3 = h.clone();
            let failer = thread::spawn(move || h3.set_failed(1));

            let completed = completer.join().unwrap();
            let cancelled = canceller.join().unwrap();
            let failed = failer.join().unwrap();

            assert_eq!(
                1,
                [completed, cancelled, failed]
                    .into_iter()
                    .filter(|won| *won)
                    .count(),
                "exactly one transition wins: completed={completed} cancelled={cancelled} \
                 failed={failed}"
            );
            assert_eq!(
                completed,
                published.load(Ordering::SeqCst),
                "a result was published for a transfer that did not complete"
            );
            assert_eq!(
                completed,
                h.status.status() == crate::types::TransferStatus::Completed
            );
        });
    }

    /// Harness wrapping the real `WakeFlag` with a simulated gated
    /// resource (a `Mutex<u32>` where 0 = blocked and non-zero = unblocked).
    struct Harness {
        flag: WakeFlag,
        state: Mutex<u32>,
        wake_fired: AtomicBool,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                flag: WakeFlag::new(),
                state: Mutex::new(0),
                wake_fired: AtomicBool::new(false),
            }
        }

        /// Deliver a wake iff the flag was pending. Mirrors the production
        /// `TransferContext::try_wake` pattern without dragging the full
        /// scheduler into the test.
        fn try_wake(&self) {
            if self.flag.take_pending() {
                self.wake_fired.store(true, Ordering::Release);
            }
        }
    }

    /// If the mutator runs after the poller set the pending flag, the
    /// poller's later return of `PollWork::Pending` must be matched by a
    /// delivered wake. Verifies the poller+mutator disciplines prevent
    /// the lost-wake state.
    #[test]
    fn set_pending_then_try_wake_no_lost_wake() {
        loom::model(|| {
            let harness = Arc::new(Harness::new());

            let h2 = harness.clone();
            let poller = thread::spawn(move || {
                // Poller discipline: inside the critical section, set
                // pending first, then check the gating condition.
                let guard = h2.state.lock().unwrap();
                h2.flag.set_pending();
                let blocked = *guard == 0;
                drop(guard);
                // Returns true if the poller would have returned Pending.
                blocked
            });

            let h3 = harness.clone();
            let mutator = thread::spawn(move || {
                // Mutator discipline: lock, mutate, unlock, try_wake.
                {
                    let mut guard = h3.state.lock().unwrap();
                    *guard = 1;
                }
                h3.try_wake();
            });

            let poller_returned_pending = poller.join().unwrap();
            mutator.join().unwrap();

            let mutated = *harness.state.lock().unwrap() == 1;
            let wake_fired = harness.wake_fired.load(Ordering::Acquire);

            // If the poller committed to Pending and the mutator unblocked
            // the state, a wake must have been delivered. Otherwise the
            // poller is stuck forever.
            if poller_returned_pending && mutated {
                assert!(
                    wake_fired,
                    "lost wake: poller returned Pending, state was mutated, but wake did not fire"
                );
            }
        });
    }
}
