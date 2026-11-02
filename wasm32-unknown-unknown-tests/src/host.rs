/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A cooperative single-threaded executor, clocks, and timers on the
//! embedding host's clocks.
//!
//! [`Driver`] polls one ready task at a time and fires each timer whose
//! deadline has passed on the host's monotonic clock. When no task is ready it
//! completes the [`idle`] futures, and when none remain it returns to the
//! embedding host with the earliest live timer deadline. HTTP responses and
//! time arrive only between calls, so the host waits for either and calls
//! again.

use std::collections::VecDeque;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, SystemTime};

use aws_sdk_s3_transfer_manager::wasm::{BoxFuture, WasmRuntime};
use aws_smithy_async::rt::sleep::{AsyncSleep, Sleep};
use aws_smithy_async::time::TimeSource;

use crate::imports;

/// Polls after which one [`Driver::drive`] call reports a runaway executor.
const MAX_STEPS: u64 = 50_000_000;

/// The module's only executor.
static HOST: Host = Host::new();

struct Host {
    ready: Mutex<VecDeque<Arc<Task>>>,
    /// The latest monotonic reading, so the clock never goes backwards.
    now_nanos: AtomicU64,
    timers: Mutex<Vec<Arc<TimerEntry>>>,
    idle: Mutex<Vec<IdleWaiter>>,
    live_tasks: AtomicUsize,
    spawned_tasks: AtomicUsize,
    fired_sleeps: AtomicUsize,
}

impl Host {
    const fn new() -> Self {
        Self {
            ready: Mutex::new(VecDeque::new()),
            now_nanos: AtomicU64::new(0),
            timers: Mutex::new(Vec::new()),
            idle: Mutex::new(Vec::new()),
            live_tasks: AtomicUsize::new(0),
            spawned_tasks: AtomicUsize::new(0),
            fired_sleeps: AtomicUsize::new(0),
        }
    }

    fn now(&self) -> Duration {
        let reading = u64::try_from(imports::monotonic_now().as_nanos()).unwrap_or(u64::MAX);
        let previous = self.now_nanos.fetch_max(reading, Ordering::SeqCst);
        Duration::from_nanos(previous.max(reading))
    }
}

/// Executor counters observed by scenarios.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
    /// Futures accepted by [`HostRuntime::spawn`].
    pub(crate) spawned_tasks: usize,
    /// Spawned futures that have not completed.
    pub(crate) live_tasks: usize,
    /// Sleeps that completed because their deadline passed.
    pub(crate) fired_sleeps: usize,
}

/// Returns the current counters.
pub(crate) fn stats() -> Stats {
    Stats {
        spawned_tasks: HOST.spawned_tasks.load(Ordering::SeqCst),
        live_tasks: HOST.live_tasks.load(Ordering::SeqCst),
        fired_sleeps: HOST.fired_sleeps.load(Ordering::SeqCst),
    }
}

/// The [`WasmRuntime`] backed by this executor and the host's monotonic clock.
#[derive(Debug)]
pub(crate) struct HostRuntime;

impl WasmRuntime for HostRuntime {
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

/// Result of one [`Driver::drive`] call.
pub(crate) enum Progress<T> {
    /// The driven future completed with this output.
    Complete(T),
    /// Nothing can progress until a timer's deadline, the earliest of which
    /// this holds on the host's monotonic clock, or until the host answers a
    /// request. `None` means no timer is live.
    Waiting(Option<Duration>),
}

/// Drives one future, and every spawned task, across calls from the
/// embedding host.
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

    /// Polls the future and every spawned task until the future completes or
    /// nothing can progress.
    ///
    /// Returns an error after [`MAX_STEPS`] polls without either.
    pub(crate) fn drive(&mut self) -> Result<Progress<T>, String> {
        let waker = Waker::from(Arc::clone(&self.main));
        let mut cx = Context::from_waker(&waker);
        for _ in 0..MAX_STEPS {
            if self.main.0.swap(false, Ordering::SeqCst) {
                if let Poll::Ready(output) = self.future.as_mut().poll(&mut cx) {
                    return Ok(Progress::Complete(output));
                }
            }
            fire_due_timers();
            if let Some(task) = next_ready() {
                task.run();
                continue;
            }
            if self.main.0.load(Ordering::SeqCst) || complete_idle() {
                continue;
            }
            return Ok(Progress::Waiting(next_deadline()));
        }
        Err(format!(
            "executor ran {MAX_STEPS} polls without becoming idle ({:?})",
            stats()
        ))
    }
}

fn next_ready() -> Option<Arc<Task>> {
    HOST.ready.lock().unwrap().pop_front()
}

struct IdleWaiter {
    done: Arc<AtomicBool>,
    waker: Waker,
}

/// Completes once no spawned task is ready to run.
///
/// Every task has then run until it waits for a timer, an HTTP response, or
/// another task.
pub(crate) async fn idle() {
    let done = Arc::new(AtomicBool::new(false));
    poll_fn(|cx| {
        if done.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        HOST.idle.lock().unwrap().push(IdleWaiter {
            done: Arc::clone(&done),
            waker: cx.waker().clone(),
        });
        Poll::Pending
    })
    .await;
}

/// Completes every pending [`idle`] future. Returns whether any existed.
fn complete_idle() -> bool {
    let waiters = std::mem::take(&mut *HOST.idle.lock().unwrap());
    let completed = !waiters.is_empty();
    for waiter in waiters {
        waiter.done.store(true, Ordering::SeqCst);
        waiter.waker.wake();
    }
    completed
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

/// The earliest deadline among live timers.
fn next_deadline() -> Option<Duration> {
    HOST.timers
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.state.lock().unwrap().finished)
        .map(|entry| entry.deadline)
        .min()
}

/// An [`AsyncSleep`] whose sleeps complete when the host's monotonic clock
/// reaches their deadline.
#[derive(Debug)]
pub(crate) struct HostSleep;

impl AsyncSleep for HostSleep {
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

/// The host's wall clock.
#[derive(Debug)]
pub(crate) struct HostWallClock;

impl TimeSource for HostWallClock {
    fn now(&self) -> SystemTime {
        imports::wall_clock_now()
    }
}
