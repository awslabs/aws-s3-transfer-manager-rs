/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bridges Rust futures to blocking and `asyncio` callers.
//!
//! Every operation is a Rust future run on a shared Tokio runtime. Blocking
//! callers wait for it with the GIL released, waking periodically so signals
//! such as Ctrl-C interrupt the wait; `asyncio` callers await it through
//! `pyo3-async-runtimes`. Either way, abandoning the wait drops the future,
//! which cancels the transfer it drives.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::convert::Output;
use crate::progress::Reporter;

/// How long a blocked caller waits before checking for pending signals.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

/// Configures the runtime that drives operation futures.
///
/// Transfers execute on the transfer manager's own worker threads; this
/// runtime only awaits their completion and moves data to and from Python, so
/// it needs few threads.
pub(crate) fn init() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(2)
        .thread_name("s3-transfer-manager-py")
        .enable_all();
    pyo3_async_runtimes::tokio::init(builder);
}

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

/// Runs a future to completion on the shared runtime, blocking the calling
/// thread with the GIL released.
///
/// Progress reports are delivered to `progress` on the calling thread. If the
/// wait is interrupted by a signal or the callback raises, the future is
/// dropped and the error propagates.
pub(crate) fn block_on<T, F, Fut>(
    py: Python<'_>,
    progress: Option<&Bound<'_, PyAny>>,
    make: F,
) -> PyResult<T>
where
    T: Send + 'static,
    F: FnOnce(Option<Reporter>) -> Fut,
    Fut: Future<Output = PyResult<T>> + Send + 'static,
{
    enum Event<T> {
        Progress(u64),
        Done(PyResult<T>),
    }

    let (tx, rx) = mpsc::channel();
    let reporter = progress.map(|_| {
        let tx = tx.clone();
        Reporter::new(move |bytes| {
            let _ = tx.send(Event::Progress(bytes));
        })
    });
    let future = make(reporter);
    let _task = AbortOnDrop(runtime().spawn(async move {
        let _ = tx.send(Event::Done(future.await));
    }));

    // `Receiver` is not `Sync`, so it cannot be borrowed across `detach` directly.
    let rx = Mutex::new(rx);
    loop {
        let event = py.detach(|| {
            rx.lock()
                .expect("receiver lock poisoned")
                .recv_timeout(SIGNAL_CHECK_INTERVAL)
        });
        match event {
            Ok(Event::Done(result)) => return result,
            Ok(Event::Progress(bytes)) => {
                if let Some(callback) = progress {
                    callback.call1((bytes,))?;
                }
            }
            Err(RecvTimeoutError::Timeout) => py.check_signals()?,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(PyRuntimeError::new_err(
                    "transfer task terminated unexpectedly",
                ))
            }
        }
    }
}

/// Wraps a future in a Python awaitable bound to the running event loop.
///
/// Progress reports are scheduled on the event loop with
/// `call_soon_threadsafe`. Cancelling the awaitable drops the future.
pub(crate) fn future_into_py<'py, T, F, Fut>(
    py: Python<'py>,
    progress: Option<Py<PyAny>>,
    make: F,
) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    F: FnOnce(Option<Reporter>) -> Fut,
    Fut: Future<Output = PyResult<T>> + Send + 'static,
{
    let reporter = match progress {
        Some(callback) => {
            let event_loop = pyo3_async_runtimes::tokio::get_current_loop(py)?.unbind();
            Some(Reporter::new(move |bytes| {
                Python::attach(|py| {
                    // Fails only once the loop is closed, when nobody is listening.
                    let _ = event_loop.call_method1(
                        py,
                        "call_soon_threadsafe",
                        (callback.clone_ref(py), bytes),
                    );
                })
            }))
        }
        None => None,
    };
    pyo3_async_runtimes::tokio::future_into_py(py, make(reporter))
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type BoxFuture = Pin<Box<dyn Future<Output = PyResult<Output>> + Send>>;
type Job = Box<dyn FnOnce(Option<Reporter>) -> BoxFuture + Send>;

/// A single-use operation that can be waited on from blocking or async code.
#[pyclass(frozen, module = "aws_s3_transfer._core")]
pub(crate) struct Operation {
    job: Mutex<Option<Job>>,
}

impl Operation {
    pub(crate) fn new<F, Fut>(job: F) -> Self
    where
        F: FnOnce(Option<Reporter>) -> Fut + Send + 'static,
        Fut: Future<Output = PyResult<Output>> + Send + 'static,
    {
        let job: Job = Box::new(move |reporter| Box::pin(job(reporter)));
        Self {
            job: Mutex::new(Some(job)),
        }
    }

    fn take(&self) -> PyResult<Job> {
        self.job
            .lock()
            .expect("operation lock poisoned")
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("operation has already been started"))
    }
}

#[pymethods]
impl Operation {
    #[pyo3(signature = (progress=None))]
    fn wait(&self, py: Python<'_>, progress: Option<Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let job = self.take()?;
        block_on(py, progress.as_ref(), job)?.into_py_any(py)
    }

    #[pyo3(signature = (progress=None))]
    fn wait_async<'py>(
        &self,
        py: Python<'py>,
        progress: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let job = self.take()?;
        future_into_py(py, progress, job)
    }
}
