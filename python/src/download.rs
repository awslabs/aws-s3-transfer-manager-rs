/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::{Arc, OnceLock};

use aws_sdk_s3_transfer_manager::memory::SegmentedBytes;
use aws_sdk_s3_transfer_manager::operation::download::{
    DownloadHandle, DownloadOutput, ObjectMetadata,
};
use bytes::Buf;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::IntoPyObjectExt;
use tokio::sync::Mutex;

use crate::convert::{self, Output};
use crate::error::to_pyerr;
use crate::runtime::{block_on, future_into_py};

/// A download whose object metadata is known and whose body is ready to stream.
pub(crate) struct OpenedStream {
    pub(crate) handle: DownloadHandle,
    pub(crate) metadata: ObjectMetadata,
}

impl OpenedStream {
    pub(crate) fn into_pyobject(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let stream = DownloadStream {
            metadata: convert::object_metadata(py, &self.metadata)?.unbind(),
            state: Arc::new(Mutex::new(State::Streaming(self.handle))),
            output: Arc::new(OnceLock::new()),
        };
        stream.into_bound_py_any(py)
    }
}

enum State {
    Streaming(DownloadHandle),
    Finished,
    Closed,
}

/// The body of an object being downloaded, delivered in order.
#[pyclass(frozen, module = "aws_s3_transfer._core")]
pub(crate) struct DownloadStream {
    #[pyo3(get)]
    metadata: Py<PyAny>,
    state: Arc<Mutex<State>>,
    output: Arc<OnceLock<DownloadOutput>>,
}

impl DownloadStream {
    fn next(&self) -> impl std::future::Future<Output = PyResult<Option<Chunk>>> + Send + 'static {
        let state = self.state.clone();
        let output = self.output.clone();
        async move {
            let mut state = state.lock_owned().await;
            let handle = match &mut *state {
                State::Streaming(handle) => handle,
                State::Finished => return Ok(None),
                State::Closed => return Err(PyValueError::new_err("download stream is closed")),
            };
            let body_err = match handle.body_mut().next().await {
                Some(Ok(chunk)) => return Ok(Some(Chunk(chunk.data))),
                Some(Err(err)) => Some(err),
                None => None,
            };
            let State::Streaming(handle) = std::mem::replace(&mut *state, State::Finished) else {
                unreachable!("state was checked above");
            };
            // `join` reports the transfer's authoritative error and validates
            // the object's checksum once the body has been consumed.
            match (handle.join().await, body_err) {
                (Ok(result), None) => {
                    let _ = output.set(result);
                    Ok(None)
                }
                (Err(err), _) | (Ok(_), Some(err)) => Err(to_pyerr(err)),
            }
        }
    }
}

#[pymethods]
impl DownloadStream {
    /// Returns the next chunk of the body, or `None` once it is complete.
    fn next_chunk(&self, py: Python<'_>) -> PyResult<Option<Chunk>> {
        block_on(py, None, |_| self.next())
    }

    fn next_chunk_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, None, |_| self.next())
    }

    /// The download's output, available once the body has been consumed.
    fn output(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.output
            .get()
            .map(|output| Output::Download(output.clone()).into_py_any(py))
            .transpose()
    }

    /// Cancels the download if it has not finished.
    fn close(&self, py: Python<'_>) {
        py.detach(|| *self.state.blocking_lock() = State::Closed);
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, None, |_| async move {
            *state.lock().await = State::Closed;
            Ok(())
        })
    }
}

/// A chunk of a downloaded object, converted to `bytes` with a single copy.
pub(crate) struct Chunk(SegmentedBytes);

impl<'py> IntoPyObject<'py> for Chunk {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = PyErr;

    fn into_pyobject(mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        PyBytes::new_with(py, self.0.remaining(), |buf| {
            self.0.copy_to_slice(buf);
            Ok(())
        })
    }
}
