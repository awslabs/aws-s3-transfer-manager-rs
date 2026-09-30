/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use aws_config::{BehaviorVersion, Region};
use aws_runtime::user_agent::FrameworkMetadata;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::memory::{BufferPool, MemoryBudgetConfig, MemoryConfig};
use aws_sdk_s3_transfer_manager::types::{
    ConcurrencyMode, FailedTransferPolicy, PartSize, ReadAhead, TargetThroughput,
};
use aws_sdk_s3_transfer_manager::{Config, S3ClientConfig};
use pyo3::exceptions::{
    PyIsADirectoryError, PyNotADirectoryError, PyOSError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::body::Body;
use crate::convert::Output;
use crate::download::OpenedStream;
use crate::error::{os_error, to_pyerr};
use crate::options::{DownloadOptions, UploadOptions};
use crate::progress::track;
use crate::runtime::Operation;

/// The transfer manager's multipart threshold when left at `PartSize::Auto`.
const AUTO_MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// A transfer manager client, loaded from the environment on first use.
#[pyclass(frozen, module = "aws_s3_transfer._core")]
pub(crate) struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    settings: Settings,
    state: Mutex<State>,
    /// Serializes loading so concurrent first operations share one client.
    loading: tokio::sync::Mutex<()>,
}

enum State {
    Unloaded,
    Loaded(aws_sdk_s3_transfer_manager::Client),
    Closed,
}

impl Inner {
    async fn client(&self) -> PyResult<aws_sdk_s3_transfer_manager::Client> {
        if let Some(client) = self.loaded()? {
            return Ok(client);
        }
        let _loading = self.loading.lock().await;
        if let Some(client) = self.loaded()? {
            return Ok(client);
        }
        let client = self.settings.load().await?;
        let mut state = self.state.lock().expect("client state lock poisoned");
        if matches!(*state, State::Closed) {
            return Err(closed());
        }
        *state = State::Loaded(client.clone());
        Ok(client)
    }

    fn loaded(&self) -> PyResult<Option<aws_sdk_s3_transfer_manager::Client>> {
        match &*self.state.lock().expect("client state lock poisoned") {
            State::Unloaded => Ok(None),
            State::Loaded(client) => Ok(Some(client.clone())),
            State::Closed => Err(closed()),
        }
    }
}

fn closed() -> PyErr {
    PyRuntimeError::new_err("transfer manager is closed")
}

struct Settings {
    region_name: Option<String>,
    profile_name: Option<String>,
    endpoint_url: Option<String>,
    force_path_style: Option<bool>,
    credentials: Option<Credentials>,
    part_size: Option<u64>,
    multipart_threshold: Option<u64>,
    concurrency: ConcurrencyMode,
    memory_limit: Option<usize>,
    read_ahead: Option<usize>,
}

impl Settings {
    async fn load(&self) -> PyResult<aws_sdk_s3_transfer_manager::Client> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &self.region_name {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(profile) = &self.profile_name {
            loader = loader.profile_name(profile);
        }
        if let Some(url) = &self.endpoint_url {
            loader = loader.endpoint_url(url);
        }
        if let Some(credentials) = &self.credentials {
            loader = loader.credentials_provider(credentials.clone());
        }
        let sdk_config = loader.load().await;
        if sdk_config.region().is_none() {
            return Err(PyValueError::new_err(
                "no AWS region is configured; pass region_name or set AWS_REGION",
            ));
        }
        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
        s3_config.set_force_path_style(self.force_path_style);

        let mut config = Config::builder()
            .s3_config(S3ClientConfig::new(s3_config))
            .framework_metadata(
                FrameworkMetadata::new("python-tm", Some(env!("CARGO_PKG_VERSION"))).ok(),
            )
            .concurrency(self.concurrency.clone());
        if let Some(part_size) = self.part_size {
            config = config.part_size(PartSize::Target(part_size));
        }
        if let Some(threshold) = self.multipart_threshold {
            config = config.multipart_threshold(PartSize::Target(threshold));
        }
        if let Some(parts) = self.read_ahead {
            config = config.read_ahead(ReadAhead::Parts(parts));
        }
        if let Some(limit) = self.memory_limit {
            let pool = BufferPool::builder()
                .memory_budget(MemoryBudgetConfig::Limit(limit))
                .build()
                .map_err(|err| PyValueError::new_err(format!("invalid memory_limit: {err}")))?;
            config = config.memory(MemoryConfig::Explicit(pool));
        }
        Ok(aws_sdk_s3_transfer_manager::Client::new(config.build()))
    }
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (
        *,
        region_name=None,
        profile_name=None,
        endpoint_url=None,
        force_path_style=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        part_size=None,
        multipart_threshold=None,
        max_concurrency=None,
        target_throughput_gbps=None,
        memory_limit=None,
        read_ahead=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        region_name: Option<String>,
        profile_name: Option<String>,
        endpoint_url: Option<String>,
        force_path_style: Option<bool>,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_session_token: Option<String>,
        part_size: Option<u64>,
        multipart_threshold: Option<u64>,
        max_concurrency: Option<usize>,
        target_throughput_gbps: Option<u64>,
        memory_limit: Option<usize>,
        read_ahead: Option<usize>,
    ) -> PyResult<Self> {
        let credentials = match (aws_access_key_id, aws_secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(Credentials::new(
                access_key_id,
                secret_access_key,
                aws_session_token,
                None,
                "aws-s3-transfer",
            )),
            (None, None) if aws_session_token.is_none() => None,
            _ => {
                return Err(PyValueError::new_err(
                    "aws_access_key_id and aws_secret_access_key must be given together",
                ))
            }
        };
        let max_concurrency = positive("max_concurrency", max_concurrency)?;
        let target_throughput_gbps = positive("target_throughput_gbps", target_throughput_gbps)?;
        let concurrency = match (max_concurrency, target_throughput_gbps) {
            (None, None) => ConcurrencyMode::Auto,
            (Some(limit), None) => ConcurrencyMode::Explicit(limit),
            (None, Some(gbps)) => {
                ConcurrencyMode::TargetThroughput(TargetThroughput::new_gigabits_per_sec(gbps))
            }
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "max_concurrency and target_throughput_gbps are mutually exclusive",
                ))
            }
        };
        let settings = Settings {
            region_name,
            profile_name,
            endpoint_url,
            force_path_style,
            credentials,
            part_size: positive("part_size", part_size)?,
            multipart_threshold: positive("multipart_threshold", multipart_threshold)?,
            concurrency,
            memory_limit: positive("memory_limit", memory_limit)?,
            read_ahead,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                settings,
                state: Mutex::new(State::Unloaded),
                loading: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// Loads configuration and starts the transfer manager.
    fn load(&self) -> Operation {
        let inner = self.inner.clone();
        Operation::new(move |_| async move {
            inner.client().await?;
            Ok(Output::None)
        })
    }

    /// Releases the transfer manager. In-flight transfers run to completion.
    fn close(&self, py: Python<'_>) {
        let state = std::mem::replace(
            &mut *self.inner.state.lock().expect("client state lock poisoned"),
            State::Closed,
        );
        py.detach(move || drop(state));
    }

    #[pyo3(signature = (body, bucket, key, options=None, content_length=None, allow_async=false))]
    fn upload(
        &self,
        body: &Bound<'_, PyAny>,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
        content_length: Option<u64>,
        allow_async: bool,
    ) -> PyResult<Operation> {
        let body = Body::extract(body, content_length, allow_async)?;
        let options = UploadOptions::extract(options)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |reporter| async move {
            let client = inner.client().await?;
            let threshold = match client.config().multipart_threshold() {
                PartSize::Target(threshold) => *threshold,
                _ => AUTO_MULTIPART_THRESHOLD,
            };
            let (stream, body_errors) = body.into_input_stream(threshold).await?;
            let handle = options
                .apply(client.upload().bucket(bucket).key(key).body(stream))
                .initiate()
                .map_err(to_pyerr)?;
            track(&handle, reporter.as_ref()).await;
            let output = handle
                .join()
                .await
                .map_err(|err| body_errors.take().unwrap_or_else(|| to_pyerr(err)))?;
            Ok(Output::Upload(output))
        }))
    }

    #[pyo3(signature = (filename, bucket, key, options=None))]
    fn upload_file(
        &self,
        filename: PathBuf,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Operation> {
        let options = UploadOptions::extract(options)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |reporter| async move {
            let client = inner.client().await?;
            let stream = file_stream(&filename).await?;
            let handle = options
                .apply(client.upload().bucket(bucket).key(key).body(stream))
                .initiate()
                .map_err(to_pyerr)?;
            track(&handle, reporter.as_ref()).await;
            Ok(Output::Upload(handle.join().await.map_err(to_pyerr)?))
        }))
    }

    #[pyo3(signature = (bucket, key, options=None))]
    fn download(
        &self,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Operation> {
        let options = DownloadOptions::extract(options)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |_| async move {
            let client = inner.client().await?;
            let handle = options
                .apply(client.download().bucket(bucket).key(key))
                .initiate()
                .map_err(to_pyerr)?;
            let metadata = match handle.object_meta().await {
                Ok(metadata) => metadata.clone(),
                // `join` reports why discovery failed, e.g. that the object does not exist.
                Err(err) => return Err(to_pyerr(handle.join().await.err().unwrap_or(err))),
            };
            Ok(Output::Stream(OpenedStream { handle, metadata }))
        }))
    }

    #[pyo3(signature = (bucket, key, filename, options=None))]
    fn download_file(
        &self,
        bucket: String,
        key: String,
        filename: PathBuf,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Operation> {
        let options = DownloadOptions::extract(options)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |reporter| async move {
            let client = inner.client().await?;
            check_destination_file(&filename).await?;
            let handle = options
                .apply(client.download().bucket(bucket).key(key))
                .write_to_path(filename)
                .await
                .map_err(to_pyerr)?;
            track(&handle, reporter.as_ref()).await;
            Ok(Output::Download(handle.join().await.map_err(to_pyerr)?))
        }))
    }

    #[pyo3(signature = (
        directory,
        bucket,
        *,
        key_prefix=None,
        delimiter=None,
        recursive=true,
        follow_symlinks=false,
        failure_policy="abort",
        max_concurrent_uploads=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn upload_directory(
        &self,
        directory: PathBuf,
        bucket: String,
        key_prefix: Option<String>,
        delimiter: Option<String>,
        recursive: bool,
        follow_symlinks: bool,
        failure_policy: &str,
        max_concurrent_uploads: Option<usize>,
    ) -> PyResult<Operation> {
        let failure_policy = parse_failure_policy(failure_policy)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |reporter| async move {
            let client = inner.client().await?;
            check_directory(&directory).await?;
            let walker = FsWalker::builder()
                .recursive(recursive)
                .follow_symlinks(follow_symlinks)
                .build();
            let handle = client
                .upload_objects()
                .bucket(bucket)
                .source(directory)
                .walker(walker)
                .set_key_prefix(key_prefix)
                .set_delimiter(delimiter)
                .failure_policy(failure_policy)
                .set_max_concurrent_uploads(max_concurrent_uploads)
                .initiate()
                .map_err(to_pyerr)?;
            track(&handle, reporter.as_ref()).await;
            Ok(Output::UploadDirectory(
                handle.join().await.map_err(to_pyerr)?,
            ))
        }))
    }

    #[pyo3(signature = (
        bucket,
        directory,
        *,
        key_prefix=None,
        delimiter=None,
        failure_policy="abort",
        max_concurrent_downloads=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn download_directory(
        &self,
        bucket: String,
        directory: PathBuf,
        key_prefix: Option<String>,
        delimiter: Option<String>,
        failure_policy: &str,
        max_concurrent_downloads: Option<usize>,
    ) -> PyResult<Operation> {
        let failure_policy = parse_failure_policy(failure_policy)?;
        let inner = self.inner.clone();
        Ok(Operation::new(move |reporter| async move {
            let client = inner.client().await?;
            create_directory(&directory).await?;
            let handle = client
                .download_objects()
                .bucket(bucket)
                .destination(directory)
                .set_key_prefix(key_prefix)
                .set_delimiter(delimiter)
                .failure_policy(failure_policy)
                .set_max_concurrent_downloads(max_concurrent_downloads)
                .initiate()
                .map_err(to_pyerr)?;
            track(&handle, reporter.as_ref()).await;
            Ok(Output::DownloadDirectory(
                handle.join().await.map_err(to_pyerr)?,
            ))
        }))
    }
}

fn positive<T: Default + PartialEq>(name: &str, value: Option<T>) -> PyResult<Option<T>> {
    match value {
        Some(value) if value == T::default() => {
            Err(PyValueError::new_err(format!("{name} must be positive")))
        }
        value => Ok(value),
    }
}

fn parse_failure_policy(policy: &str) -> PyResult<FailedTransferPolicy> {
    match policy {
        "abort" => Ok(FailedTransferPolicy::Abort),
        "continue" => Ok(FailedTransferPolicy::Continue),
        other => Err(PyValueError::new_err(format!(
            "failure_policy must be 'abort' or 'continue', got {other:?}"
        ))),
    }
}

async fn file_stream(path: &Path) -> PyResult<InputStream> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|err| os_error(err, path))?;
    if metadata.is_dir() {
        return Err(PyIsADirectoryError::new_err(format!(
            "is a directory: '{}'",
            path.display()
        )));
    }
    InputStream::read_from()
        .path(path)
        .length(metadata.len())
        .build()
        .map_err(|err| PyOSError::new_err(format!("{err}: '{}'", path.display())))
}

async fn check_directory(path: &Path) -> PyResult<()> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|err| os_error(err, path))?;
    if !metadata.is_dir() {
        return Err(PyNotADirectoryError::new_err(format!(
            "not a directory: '{}'",
            path.display()
        )));
    }
    Ok(())
}

/// Creates `path` and its parents unless it is already a directory.
async fn create_directory(path: &Path) -> PyResult<()> {
    match tokio::fs::metadata(path).await {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => tokio::fs::create_dir_all(path)
            .await
            .map_err(|err| os_error(err, path)),
        _ => check_directory(path).await,
    }
}

/// Checks that a download can be written to `path` before starting it.
async fn check_destination_file(path: &Path) -> PyResult<()> {
    if tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_dir())
    {
        return Err(PyIsADirectoryError::new_err(format!(
            "is a directory: '{}'",
            path.display()
        )));
    }
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => check_directory(parent).await,
        _ => Ok(()),
    }
}
