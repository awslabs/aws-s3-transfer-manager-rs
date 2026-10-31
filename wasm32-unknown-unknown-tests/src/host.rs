/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A host implemented inside the module: a cooperative single-threaded
//! executor, a logical monotonic clock, timers on that clock, and simulated
//! I/O completions.
//!
//! The executor polls one ready task at a time. When nothing is ready it first
//! delivers pending I/O completions, then advances the clock to the earliest
//! live timer. If neither exists while the awaited future is incomplete, the
//! run has stalled and [`block_on`] reports it instead of looping. [`Driver`]
//! instead returns to the embedding host while one of the host's operations is
//! outstanding, and resumes when the host calls it again. Every scheduling
//! step advances the clock by [`TICK`], so measured intervals are nonzero and
//! ordered.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3_transfer_manager::wasm::{BoxFuture, WasmRuntime};
use aws_smithy_async::rt::sleep::{AsyncSleep, Sleep};
use aws_smithy_async::time::TimeSource;

/// Logical time added by each scheduling step.
const TICK: Duration = Duration::from_micros(1);

/// Scheduling steps after which [`block_on`] reports a runaway run.
const MAX_STEPS: u64 = 50_000_000;

/// Wall-clock time at the logical clock's epoch.
const WALL_EPOCH_SECS: u64 = 1_735_689_600;

/// The module's only host.
static HOST: Host = Host::new();

struct Host {
    ready: Mutex<VecDeque<Arc<Task>>>,
    now_nanos: AtomicU64,
    timers: Mutex<Vec<Arc<TimerEntry>>>,
    io: Mutex<Vec<Arc<IoCompletion>>>,
    live_tasks: AtomicUsize,
    spawned_tasks: AtomicUsize,
    fired_sleeps: AtomicUsize,
    io_pending_polls: AtomicUsize,
}

impl Host {
    const fn new() -> Self {
        Self {
            ready: Mutex::new(VecDeque::new()),
            now_nanos: AtomicU64::new(0),
            timers: Mutex::new(Vec::new()),
            io: Mutex::new(Vec::new()),
            live_tasks: AtomicUsize::new(0),
            spawned_tasks: AtomicUsize::new(0),
            fired_sleeps: AtomicUsize::new(0),
            io_pending_polls: AtomicUsize::new(0),
        }
    }

    fn now(&self) -> Duration {
        Duration::from_nanos(self.now_nanos.load(Ordering::SeqCst))
    }

    fn advance_to(&self, deadline: Duration) {
        let nanos = u64::try_from(deadline.as_nanos()).expect("logical clock overflowed");
        self.now_nanos.fetch_max(nanos, Ordering::SeqCst);
    }
}

/// Counters observed by scenarios.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
    /// Futures accepted by [`LogicalRuntime::spawn`].
    pub(crate) spawned_tasks: usize,
    /// Spawned futures that have not completed.
    pub(crate) live_tasks: usize,
    /// Sleeps that completed because their deadline passed.
    pub(crate) fired_sleeps: usize,
    /// Polls in which a simulated I/O operation returned `Pending`.
    pub(crate) io_pending_polls: usize,
}

/// Returns the current counters.
pub(crate) fn stats() -> Stats {
    Stats {
        spawned_tasks: HOST.spawned_tasks.load(Ordering::SeqCst),
        live_tasks: HOST.live_tasks.load(Ordering::SeqCst),
        fired_sleeps: HOST.fired_sleeps.load(Ordering::SeqCst),
        io_pending_polls: HOST.io_pending_polls.load(Ordering::SeqCst),
    }
}

/// The [`WasmRuntime`] backed by this host.
#[derive(Debug)]
pub(crate) struct LogicalRuntime;

impl WasmRuntime for LogicalRuntime {
    fn monotonic_now(&self) -> Duration {
        HOST.now()
    }

    fn spawn(&self, future: BoxFuture) {
        HOST.spawned_tasks.fetch_add(1, Ordering::SeqCst);
        HOST.live_tasks.fetch_add(1, Ordering::SeqCst);
        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            queued: AtomicBool::new(false),
        });
        task.wake_by_ref_impl();
    }
}

struct Task {
    future: Mutex<Option<BoxFuture>>,
    queued: AtomicBool,
}

impl Task {
    fn wake_by_ref_impl(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::SeqCst) {
            HOST.ready.lock().unwrap().push_back(Arc::clone(self));
        }
    }

    fn run(self: Arc<Self>) {
        self.queued.store(false, Ordering::SeqCst);
        let waker = Waker::from(Arc::clone(&self));
        let mut cx = Context::from_waker(&waker);
        let mut slot = self.future.lock().unwrap();
        let Some(future) = slot.as_mut() else {
            return;
        };
        if future.as_mut().poll(&mut cx).is_ready() {
            *slot = None;
            HOST.live_tasks.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref_impl();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wake_by_ref_impl();
    }
}

struct MainWake(AtomicBool);

impl Wake for MainWake {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Polls `future` and every spawned task until `future` completes.
///
/// Returns an error if no task can make progress, no I/O is pending, and no
/// timer is live while `future` is incomplete, or after [`MAX_STEPS`] steps.
pub(crate) fn block_on<F: Future>(future: F) -> Result<F::Output, String> {
    let main = Arc::new(MainWake(AtomicBool::new(true)));
    match drive(pin!(future), &main, &|| false)? {
        Poll::Ready(output) => Ok(output),
        Poll::Pending => unreachable!("block_on never waits for the embedding host"),
    }
}

/// Result of one [`Driver::drive`] call.
pub(crate) enum Progress<T> {
    /// The driven future completed with this output.
    Complete(T),
    /// Nothing can progress until the embedding host completes an operation.
    AwaitingHost,
}

/// Drives one future across calls from the embedding host.
pub(crate) struct Driver<T> {
    main: Arc<MainWake>,
    future: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T> Driver<T> {
    pub(crate) fn new(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self {
            main: Arc::new(MainWake(AtomicBool::new(true))),
            future: Box::pin(future),
        }
    }

    /// Polls the future and every spawned task as [`block_on`] does, except
    /// that when nothing is ready, no simulated I/O is pending, and
    /// `awaiting_host` returns true, it returns [`Progress::AwaitingHost`]
    /// instead of advancing the clock. A later call resumes the future.
    pub(crate) fn drive(
        &mut self,
        awaiting_host: &dyn Fn() -> bool,
    ) -> Result<Progress<T>, String> {
        let poll = drive(self.future.as_mut(), &self.main, awaiting_host)?;
        Ok(match poll {
            Poll::Ready(output) => Progress::Complete(output),
            Poll::Pending => Progress::AwaitingHost,
        })
    }
}

/// Polls `future` and every spawned task. Returns `Pending` when nothing can
/// progress and `awaiting_host` returns true. Timers advance only while no
/// host operation is outstanding.
fn drive<F: Future + ?Sized>(
    mut future: Pin<&mut F>,
    main: &Arc<MainWake>,
    awaiting_host: &dyn Fn() -> bool,
) -> Result<Poll<F::Output>, String> {
    let waker = Waker::from(Arc::clone(main));
    let mut cx = Context::from_waker(&waker);
    for _ in 0..MAX_STEPS {
        HOST.advance_to(HOST.now() + TICK);
        if main.0.swap(false, Ordering::SeqCst) {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return Ok(Poll::Ready(output));
            }
        }
        fire_due_timers();
        if let Some(task) = next_ready() {
            task.run();
            continue;
        }
        if main.0.load(Ordering::SeqCst) || deliver_io() {
            continue;
        }
        if awaiting_host() {
            return Ok(Poll::Pending);
        }
        if advance_to_next_timer() {
            continue;
        }
        return Err(format!(
            "executor stalled at {:?}: no ready task, pending I/O, or live timer ({:?})",
            HOST.now(),
            stats()
        ));
    }
    Err(format!(
        "executor exceeded {MAX_STEPS} steps ({:?})",
        stats()
    ))
}

/// Runs spawned tasks and delivers I/O until neither remains, without
/// advancing the clock to a timer.
pub(crate) fn run_until_idle() -> Result<(), String> {
    for _ in 0..MAX_STEPS {
        HOST.advance_to(HOST.now() + TICK);
        fire_due_timers();
        if let Some(task) = next_ready() {
            task.run();
            continue;
        }
        if deliver_io() {
            continue;
        }
        return Ok(());
    }
    Err(format!(
        "executor exceeded {MAX_STEPS} steps ({:?})",
        stats()
    ))
}

fn next_ready() -> Option<Arc<Task>> {
    HOST.ready.lock().unwrap().pop_front()
}

/// Wakes every I/O operation registered so far. Returns whether any existed.
fn deliver_io() -> bool {
    let completions = std::mem::take(&mut *HOST.io.lock().unwrap());
    let delivered = !completions.is_empty();
    for completion in completions {
        completion.done.store(true, Ordering::SeqCst);
        if let Some(waker) = completion.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    delivered
}

struct TimerEntry {
    deadline: Duration,
    state: Mutex<TimerState>,
}

#[derive(Default)]
struct TimerState {
    waker: Option<Waker>,
    finished: bool,
}

/// Drops finished timers and wakes those whose deadline has passed.
fn fire_due_timers() {
    let now = HOST.now();
    let mut due = Vec::new();
    HOST.timers.lock().unwrap().retain(|entry| {
        let mut state = entry.state.lock().unwrap();
        if state.finished {
            return false;
        }
        if entry.deadline <= now {
            if let Some(waker) = state.waker.take() {
                due.push(waker);
            }
        }
        true
    });
    for waker in due {
        waker.wake();
    }
}

/// Advances the clock to the earliest live timer. Returns whether one exists.
fn advance_to_next_timer() -> bool {
    let next = HOST
        .timers
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.state.lock().unwrap().finished)
        .map(|entry| entry.deadline)
        .min();
    match next {
        Some(deadline) => {
            HOST.advance_to(deadline);
            fire_due_timers();
            true
        }
        None => false,
    }
}

/// An [`AsyncSleep`] whose sleeps complete when the logical clock reaches
/// their deadline.
#[derive(Debug)]
pub(crate) struct LogicalSleep;

impl AsyncSleep for LogicalSleep {
    fn sleep(&self, duration: Duration) -> Sleep {
        let entry = Arc::new(TimerEntry {
            deadline: HOST.now().saturating_add(duration),
            state: Mutex::new(TimerState::default()),
        });
        HOST.timers.lock().unwrap().push(Arc::clone(&entry));
        Sleep::new(SleepFuture { entry })
    }
}

struct SleepFuture {
    entry: Arc<TimerEntry>,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.entry.state.lock().unwrap();
        if state.finished {
            return Poll::Ready(());
        }
        if self.entry.deadline <= HOST.now() {
            state.finished = true;
            HOST.fired_sleeps.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(());
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for SleepFuture {
    fn drop(&mut self) {
        self.entry.state.lock().unwrap().finished = true;
    }
}

/// A wall clock that follows the logical clock from a fixed date.
#[derive(Debug)]
pub(crate) struct LogicalWallClock;

impl TimeSource for LogicalWallClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(WALL_EPOCH_SECS) + HOST.now()
    }
}

struct IoCompletion {
    done: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// Completes after the host delivers one I/O completion.
///
/// The first poll registers the operation and returns `Pending`; the executor
/// wakes it only when no task is ready, as a host would after its event loop
/// regains control.
pub(crate) async fn host_io() {
    let completion = Arc::new(IoCompletion {
        done: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    HOST.io.lock().unwrap().push(Arc::clone(&completion));
    std::future::poll_fn(|cx| {
        if completion.done.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *completion.waker.lock().unwrap() = Some(cx.waker().clone());
        HOST.io_pending_polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    })
    .await;
}
