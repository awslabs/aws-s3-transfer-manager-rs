/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types that define what a transfer is and what it produces.

use crate::error;
use crate::scheduler::concurrency::ErrorKind;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};

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
/// - **Pending/wake obligation**: every `PollWork::Pending` must have a corresponding
///   future call to `scheduler.wake(id)`. See [`TransferContext`] for the wake
///   primitive protocol.
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
///
/// ## `on_terminal` — external termination hook
/// - Called from outside the normal lifecycle (panic, cancellation, drop) to
///   let the transfer release held resources and notify waiters.
/// - Must be short and must not block.
pub(crate) trait Transfer: Send + Sync + std::fmt::Debug {
    /// The transfer's shared context (id, handle, status, cancellation).
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

    /// Called when the transfer is forced into a terminal state from outside
    /// (e.g. worker panic, external cancellation). Allows the transfer to
    /// clean up resources and notify waiters that would otherwise block
    /// indefinitely.
    ///
    /// Must be short and non-blocking.
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

/// Unique identifier for a transfer, with optional parent for hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TransferId {
    pub(crate) id: u64,
    pub(crate) parent: Option<u64>,
}

impl std::fmt::Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parent {
            Some(parent) => write!(f, "{}-{}", self.id, parent),
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
/// - `Failed`: Transfer has already transitioned itself to terminal state (via `set_failed` +
///   `signal_terminal`). Scheduler will not poll it again and will remove it once idle.
/// - `Cancelled`: Transfer is already terminal (failed or cancelled by another work item).
///   Same cleanup as `Failed`.
pub(crate) enum WorkOutcome {
    /// Work completed successfully.
    Success { data: Option<Box<dyn WorkData>> },
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
pub(crate) struct StateMachineStatus(Arc<AtomicU8>);

// Status constants
const STATUS_ACTIVE: u8 = 0;
const STATUS_COMPLETED: u8 = 1;
const STATUS_FAILED: u8 = 2;
const STATUS_CANCELLED: u8 = 3;

impl StateMachineStatus {
    /// Create a new status in the Active state
    #[inline]
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(STATUS_ACTIVE)))
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

/// Per-transfer cumulative metrics.
pub(crate) struct MetricsState {
    network_tx: AtomicU64,
    network_rx: AtomicU64,
    disk_read: AtomicU64,
    disk_write: AtomicU64,
    total_bytes: std::sync::OnceLock<u64>,
    started_at: std::time::Instant,
    finished_at: std::sync::OnceLock<std::time::Instant>,
}

impl MetricsState {
    pub(crate) fn new() -> Self {
        Self {
            network_tx: AtomicU64::new(0),
            network_rx: AtomicU64::new(0),
            disk_read: AtomicU64::new(0),
            disk_write: AtomicU64::new(0),
            total_bytes: std::sync::OnceLock::new(),
            started_at: std::time::Instant::now(),
            finished_at: std::sync::OnceLock::new(),
        }
    }

    /// Record an IO sample (per-transfer cumulative counters only).
    pub(crate) fn record_io(&self, sample: &crate::metrics::IoSample) {
        self.network_tx
            .fetch_add(sample.network_tx, Ordering::Relaxed);
        self.network_rx
            .fetch_add(sample.network_rx, Ordering::Relaxed);
        self.disk_read
            .fetch_add(sample.disk_read, Ordering::Relaxed);
        self.disk_write
            .fetch_add(sample.disk_write, Ordering::Relaxed);
    }

    /// Set the expected total payload bytes. No-op if already set.
    pub(crate) fn set_total_bytes(&self, n: u64) {
        let _ = self.total_bytes.set(n);
    }

    /// Mark the transfer as finished. No-op if already set.
    pub(crate) fn set_finished(&self) {
        let _ = self.finished_at.set(std::time::Instant::now());
    }

    /// Snapshot current metrics into the public type.
    pub(crate) fn snapshot(&self) -> crate::types::TransferMetrics {
        crate::types::TransferMetrics {
            network_tx: self.network_tx.load(Ordering::Relaxed),
            network_rx: self.network_rx.load(Ordering::Relaxed),
            disk_read: self.disk_read.load(Ordering::Relaxed),
            disk_write: self.disk_write.load(Ordering::Relaxed),
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
    /// Error storage (only used when status == Failed)
    error: Arc<Mutex<Option<Box<error::Error>>>>,
    /// Completion signal sender - signals "state machine reached terminal state"
    completion_tx: Arc<Mutex<Option<StateMachineTerminalSender>>>,
    /// Set when poll_work returns Pending, cleared on try_wake
    wake_flag: Arc<wake_flag::WakeFlag>,
    /// Cancellation token for cooperative cancellation
    cancellation_token: tokio_util::sync::CancellationToken,
    /// Per-transfer metrics backing store
    pub(crate) metrics: Arc<MetricsState>,
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
        Self::new_inner(handle, next_transfer_id())
    }

    /// Returns a context + receiver for a child transfer linked to `parent_id`.
    ///
    /// Child transfers share the scheduler with their parent. `signal_terminal`
    /// on the child wakes the parent so the parent state machine can reap it.
    /// `scheduler.cancel_transfer(parent_id)` cascades to children via the
    /// parent linkage.
    pub(crate) fn new_child(
        handle: Arc<crate::client::Handle>,
        parent_id: u64,
    ) -> (Self, StateMachineTerminalReceiver) {
        let mut id = next_transfer_id();
        id.parent = Some(parent_id);
        Self::new_inner(handle, id)
    }

    fn new_inner(
        handle: Arc<crate::client::Handle>,
        id: TransferId,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let ctx = Self {
            id,
            metrics: Arc::new(MetricsState::new()),
            handle,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            wake_flag: Arc::new(wake_flag::WakeFlag::new()),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        };
        (ctx, completion_rx)
    }

    /// Create a new transfer context with a specific ID (for testing).
    /// Returns the context and a receiver for terminal state notification.
    #[cfg(test)]
    pub(crate) fn with_id(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let ctx = Self {
            id,
            metrics: Arc::new(MetricsState::new()),
            handle,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            wake_flag: Arc::new(wake_flag::WakeFlag::new()),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        };
        (ctx, completion_rx)
    }

    /// Record that `poll_work` is about to return `Pending`.
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
    pub(crate) fn set_pending(&self) {
        self.wake_flag.set_pending();
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %self.id,
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
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.id,
                "ctx.try_wake.fired",
            );
            self.handle.scheduler.wake(self.id);
        } else {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.id,
                "ctx.try_wake.skipped",
            );
        }
    }

    /// Returns a task waker that requeues this transfer in the scheduler.
    ///
    /// Use this for a `Future` polled inside `poll_work`. Its wake is level-like
    /// at the scheduler boundary: a wake concurrent with the current poll is
    /// retained by the descriptor's release-and-recheck protocol. Ordinary
    /// state mutations should continue to use [`Self::set_pending`] and
    /// [`Self::try_wake`] under their shared state lock.
    pub(crate) fn scheduler_waker(&self) -> Waker {
        Waker::from(Arc::new(SchedulerWake {
            scheduler: self.handle.scheduler.clone(),
            id: self.id,
        }))
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn s3_client(&self) -> &aws_sdk_s3::Client {
        &self.handle.s3_client
    }

    /// The cancellation token for this transfer
    pub(crate) fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation_token
    }

    /// Mark transfer as failed and store the error.
    /// First-write-wins - returns true if this call set the status.
    ///
    /// Setting the status alone is not observable to the owning handle: a
    /// terminal transition must be signalled via [`signal_terminal`] (directly,
    /// or once in-flight work drains). For the common fail-and-abort case use
    /// [`set_failed_and_signal`], which bundles both.
    ///
    /// [`signal_terminal`]: Self::signal_terminal
    /// [`set_failed_and_signal`]: Self::set_failed_and_signal
    pub(crate) fn set_failed(&self, err: impl Into<error::Error>) -> bool {
        if self.status.set_failed() {
            *self.error.lock().unwrap() = Some(Box::new(err.into()));
            true
        } else {
            false
        }
    }

    /// Mark transfer as completed.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_completed(&self) -> bool {
        self.status.set_completed()
    }

    /// Mark transfer as cancelled.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_cancelled(&self) -> bool {
        self.status.set_cancelled()
    }

    /// Take the error if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn take_error(&self) -> Option<error::Error> {
        if self.status.is_failed() {
            self.error.lock().unwrap().take().map(|e| *e)
        } else {
            None
        }
    }

    /// Peek at the error kind if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn error_kind(&self) -> Option<error::ErrorKind> {
        if self.status.is_failed() {
            self.error
                .lock()
                .unwrap()
                .as_ref()
                .map(|e| e.kind().clone())
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
        self.metrics.set_finished();
        if let Some(tx) = self.completion_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        // Wake the parent transfer (if any) so it can reap this child.
        if let Some(parent_id) = self.id.parent {
            self.handle.scheduler.wake(TransferId {
                id: parent_id,
                parent: None,
            });
        }
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

    /// Set the expected total payload bytes for this transfer.
    pub(crate) fn set_total_bytes(&self, n: u64) {
        self.metrics.set_total_bytes(n);
    }

    /// Get current transfer status as a public enum.
    pub(crate) fn transfer_status(&self) -> crate::types::TransferStatus {
        use crate::types::TransferStatus;
        if self.status.is_cancelled() {
            TransferStatus::Cancelled
        } else if self.is_failed() {
            TransferStatus::Failed
        } else if !self.is_active() {
            TransferStatus::Completed
        } else {
            TransferStatus::Active
        }
    }

    /// Snapshot current transfer metrics.
    pub(crate) fn metrics(&self) -> crate::types::TransferMetrics {
        self.metrics.snapshot()
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
            let config = crate::Config::builder().client(s3_client).build();
            crate::client::Handle::new_for_test(config, 4)
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
            });
            ctx.record_io(&crate::metrics::IoSample {
                network_tx: 10,
                network_rx: 20,
                disk_read: 30,
                disk_write: 40,
            });
            let stats = ctx.metrics();
            assert_eq!(stats.network_tx, 110);
            assert_eq!(stats.network_rx, 220);
            assert_eq!(stats.disk_read, 330);
            assert_eq!(stats.disk_write, 440);
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

        #[cfg_attr(miri, ignore)]
        #[test]
        fn set_total_bytes() {
            let handle = test_handle();
            let (ctx, _rx) = TransferContext::new(handle);
            assert_eq!(ctx.metrics().total_bytes, None);
            ctx.set_total_bytes(42);
            assert_eq!(ctx.metrics().total_bytes, Some(42));
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::wake_flag::WakeFlag;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

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
