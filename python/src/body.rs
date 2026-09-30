/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload bodies supplied from Python.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use aws_sdk_s3_transfer_manager::io::adapters::TokioIo;
use aws_sdk_s3_transfer_manager::io::{InputStream, SizeHint};
use bytes::{Bytes, BytesMut};
use pyo3::exceptions::{PyStopAsyncIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyIterator, PyMemoryView, PyString};
use pyo3_async_runtimes::TaskLocals;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

use crate::runtime::runtime;

/// Size of each `read()` issued against a file object.
const READ_SIZE: usize = 1024 * 1024;

/// Chunks buffered between a Python producer and the transfer.
const CHANNEL_CAPACITY: usize = 4;

/// Data to upload.
pub(crate) enum Body {
    /// Contents already in memory.
    Bytes(Bytes),
    /// Contents produced incrementally by a Python object.
    Stream { source: Source, size: Option<u64> },
}

pub(crate) enum Source {
    /// A binary file object, read with `read(n)`.
    Reader(Py<PyAny>),
    /// An iterator of bytes-like objects.
    Iterator(Py<PyIterator>),
    /// An object with a coroutine `read(n)` method, awaited on its event loop.
    AsyncReader(Py<PyAny>, TaskLocals),
    /// An async iterator of bytes-like objects, awaited on its event loop.
    AsyncIterator(Py<PyAny>, TaskLocals),
}

impl Body {
    /// Classifies a Python object as an upload body.
    ///
    /// `content_length`, when given, declares the size of a streamed body.
    /// Asynchronous sources are accepted only when `allow_async` is set, as
    /// they must be awaited on an event loop that is not blocked by the caller.
    pub(crate) fn extract(
        body: &Bound<'_, PyAny>,
        content_length: Option<u64>,
        allow_async: bool,
    ) -> PyResult<Self> {
        if body.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(
                "upload body must be bytes-like, a binary file object, or an iterable of bytes; \
                 encode str bodies first, or use upload_file() to upload a path",
            ));
        }
        if let Some(bytes) = bytes_like(body)? {
            return Ok(Body::Bytes(bytes));
        }
        let mut size = content_length;
        let source = if let Ok(read) = body.getattr("read") {
            if is_coroutine_function(&read)? {
                Source::AsyncReader(body.clone().unbind(), event_loop_locals(body, allow_async)?)
            } else {
                if size.is_none() {
                    size = remaining_size(body)?;
                }
                Source::Reader(body.clone().unbind())
            }
        } else if body.hasattr("__aiter__")? {
            let locals = event_loop_locals(body, allow_async)?;
            Source::AsyncIterator(body.call_method0("__aiter__")?.unbind(), locals)
        } else if let Ok(iterator) = body.try_iter() {
            Source::Iterator(iterator.unbind())
        } else {
            return Err(PyTypeError::new_err(format!(
                "unsupported upload body type: {}",
                type_name(body)
            )));
        };
        Ok(Body::Stream { source, size })
    }

    /// Converts the body into an [`InputStream`].
    ///
    /// Streamed bodies smaller than `multipart_threshold` are buffered so they
    /// are sent in a single request, as in-memory bodies are. Errors raised
    /// by the Python object are recorded in the returned [`BodyErrors`].
    pub(crate) async fn into_input_stream(
        self,
        multipart_threshold: u64,
    ) -> PyResult<(InputStream, Arc<BodyErrors>)> {
        let errors = Arc::new(BodyErrors::default());
        let (source, size) = match self {
            Body::Bytes(bytes) => return Ok((InputStream::from(bytes), errors)),
            Body::Stream { source, size } => (source, size),
        };

        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        source.spawn(tx, errors.clone());

        let buffer_limit = match size {
            Some(size) if size >= multipart_threshold => 0,
            _ => multipart_threshold,
        };
        let mut buffered = VecDeque::new();
        let mut buffered_len = 0;
        while buffered_len < buffer_limit {
            match rx.recv().await {
                Some(Ok(chunk)) => {
                    buffered_len += chunk.len() as u64;
                    buffered.push_back(chunk);
                }
                Some(Err(err)) => return Err(errors.take().unwrap_or_else(|| err.into())),
                None => {
                    if let Some(size) = size.filter(|&size| size != buffered_len) {
                        return Err(size_mismatch(size, buffered_len));
                    }
                    return Ok((InputStream::from(concat(buffered)), errors));
                }
            }
        }

        let size_hint = size.map_or_else(SizeHint::default, SizeHint::exact);
        let reader = ChunkReader { buffered, rx };
        let stream = InputStream::from_part_stream(TokioIo::new(reader, size_hint));
        Ok((stream, errors))
    }
}

impl Source {
    /// Starts producing chunks into `tx` in the background.
    ///
    /// Synchronous sources are read on the blocking pool; asynchronous ones
    /// are awaited on the event loop they were supplied from.
    fn spawn(self, tx: mpsc::Sender<io::Result<Bytes>>, errors: Arc<BodyErrors>) {
        match self {
            Source::Reader(reader) => {
                runtime().spawn_blocking(move || {
                    produce(&tx, &errors, |py| {
                        read_result(reader.bind(py).call_method1("read", (READ_SIZE,)))
                    })
                });
            }
            Source::Iterator(iterator) => {
                runtime().spawn_blocking(move || {
                    produce(&tx, &errors, |py| {
                        let mut iterator = iterator.bind(py).clone();
                        iterator.next().map(|item| to_bytes(&item?)).transpose()
                    })
                });
            }
            Source::AsyncReader(reader, locals) => {
                runtime().spawn(produce_async(
                    tx,
                    errors,
                    locals,
                    move |py| reader.bind(py).call_method1("read", (READ_SIZE,)),
                    |py, item| read_result(item.map(|item| item.into_bound(py))),
                ));
            }
            Source::AsyncIterator(iterator, locals) => {
                runtime().spawn(produce_async(
                    tx,
                    errors,
                    locals,
                    move |py| iterator.bind(py).call_method0("__anext__"),
                    |py, item| match item {
                        Ok(item) => to_bytes(item.bind(py)).map(Some),
                        Err(err) if err.is_instance_of::<PyStopAsyncIteration>(py) => Ok(None),
                        Err(err) => Err(err),
                    },
                ));
            }
        }
    }
}

/// Interprets the result of `read(n)`, where an empty result marks the end.
fn read_result(result: PyResult<Bound<'_, PyAny>>) -> PyResult<Option<Bytes>> {
    let chunk = to_bytes(&result?)?;
    Ok((!chunk.is_empty()).then_some(chunk))
}

/// Pulls chunks with `next` until it returns `None`, the transfer stops
/// consuming, or the Python object raises.
fn produce(
    tx: &mpsc::Sender<io::Result<Bytes>>,
    errors: &BodyErrors,
    mut next: impl FnMut(Python<'_>) -> PyResult<Option<Bytes>> + Send,
) {
    loop {
        let message = match Python::attach(&mut next) {
            Ok(Some(chunk)) if chunk.is_empty() => continue,
            Ok(Some(chunk)) => Ok(chunk),
            Ok(None) => return,
            Err(err) => Err(errors.record(err)),
        };
        let failed = message.is_err();
        if tx.blocking_send(message).is_err() || failed {
            return;
        }
    }
}

/// Like [`produce`], but each step awaits the awaitable returned by `start`
/// and interprets its result with `finish`.
async fn produce_async(
    tx: mpsc::Sender<io::Result<Bytes>>,
    errors: Arc<BodyErrors>,
    locals: TaskLocals,
    start: impl for<'py> Fn(Python<'py>) -> PyResult<Bound<'py, PyAny>> + Send,
    finish: impl Fn(Python<'_>, PyResult<Py<PyAny>>) -> PyResult<Option<Bytes>> + Send,
) {
    loop {
        let step =
            Python::attach(|py| pyo3_async_runtimes::into_future_with_locals(&locals, start(py)?));
        let result = match step {
            Ok(step) => step.await,
            Err(err) => Err(err),
        };
        let message = match Python::attach(|py| finish(py, result)) {
            Ok(Some(chunk)) if chunk.is_empty() => continue,
            Ok(Some(chunk)) => Ok(chunk),
            Ok(None) => return,
            Err(err) => Err(errors.record(err)),
        };
        let failed = message.is_err();
        if tx.send(message).await.is_err() || failed {
            return;
        }
    }
}

fn event_loop_locals(body: &Bound<'_, PyAny>, allow_async: bool) -> PyResult<TaskLocals> {
    let unsupported = || {
        PyTypeError::new_err(format!(
            "{} is an asynchronous upload body; upload it with AsyncTransferManager",
            type_name(body)
        ))
    };
    if !allow_async {
        return Err(unsupported());
    }
    pyo3_async_runtimes::tokio::get_current_locals(body.py()).map_err(|_| unsupported())
}

fn is_coroutine_function(function: &Bound<'_, PyAny>) -> PyResult<bool> {
    static ISCOROUTINEFUNCTION: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    ISCOROUTINEFUNCTION
        .import(function.py(), "inspect", "iscoroutinefunction")?
        .call1((function,))?
        .is_truthy()
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "<unknown>".to_owned(), |name| name.to_string())
}

/// Copies the contents of a bytes-like object; `bytes` are shared, not copied.
///
/// Returns `None` if `value` does not support the buffer protocol.
fn bytes_like(value: &Bound<'_, PyAny>) -> PyResult<Option<Bytes>> {
    let bytes = match value.cast::<PyBytes>() {
        Ok(bytes) => bytes.clone(),
        Err(_) => match PyMemoryView::from(value) {
            Ok(view) => view.call_method0("tobytes")?.cast_into::<PyBytes>()?,
            Err(_) => return Ok(None),
        },
    };
    Ok(Some(Bytes::from_owner(PyBackedBytes::from(bytes))))
}

fn to_bytes(value: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    bytes_like(value)?.ok_or_else(|| {
        PyTypeError::new_err(format!(
            "expected a bytes-like object from the upload body, got {}",
            type_name(value)
        ))
    })
}

/// Remaining bytes in a seekable file object, or `None` if it cannot seek.
fn remaining_size(file: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    let seekable = file
        .call_method0("seekable")
        .and_then(|seekable| seekable.is_truthy())
        .unwrap_or(false);
    if !seekable {
        return Ok(None);
    }
    let position: u64 = file.call_method0("tell")?.extract()?;
    let end: u64 = file.call_method1("seek", (0, 2))?.extract()?;
    file.call_method1("seek", (position,))?;
    Ok(Some(end.saturating_sub(position)))
}

fn concat(mut chunks: VecDeque<Bytes>) -> Bytes {
    if chunks.len() <= 1 {
        return chunks.pop_front().unwrap_or_default();
    }
    let mut joined = BytesMut::with_capacity(chunks.iter().map(Bytes::len).sum());
    for chunk in chunks {
        joined.extend_from_slice(&chunk);
    }
    joined.freeze()
}

fn size_mismatch(declared: u64, actual: u64) -> PyErr {
    PyValueError::new_err(format!(
        "upload body produced {actual} bytes but its declared size is {declared} bytes"
    ))
}

/// The first error raised by the Python object producing an upload body.
///
/// The transfer only sees an I/O error; the original exception is re-raised
/// when the upload fails.
#[derive(Default)]
pub(crate) struct BodyErrors(Mutex<Option<PyErr>>);

impl BodyErrors {
    fn record(&self, err: PyErr) -> io::Error {
        let message = format!("reading the upload body failed: {err}");
        self.0
            .lock()
            .expect("body error lock poisoned")
            .get_or_insert(err);
        io::Error::other(message)
    }

    pub(crate) fn take(&self) -> Option<PyErr> {
        self.0.lock().expect("body error lock poisoned").take()
    }
}

/// Reads the chunks a producer sends, after any already buffered.
struct ChunkReader {
    buffered: VecDeque<Bytes>,
    rx: mpsc::Receiver<io::Result<Bytes>>,
}

impl AsyncRead for ChunkReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if let Some(chunk) = self.buffered.front_mut() {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk.split_to(n));
                if chunk.is_empty() {
                    self.buffered.pop_front();
                }
                return Poll::Ready(Ok(()));
            }
            match ready!(self.rx.poll_recv(cx)) {
                Some(Ok(chunk)) => self.buffered.push_back(chunk),
                Some(Err(err)) => return Poll::Ready(Err(err)),
                None => return Poll::Ready(Ok(())),
            }
        }
    }
}
