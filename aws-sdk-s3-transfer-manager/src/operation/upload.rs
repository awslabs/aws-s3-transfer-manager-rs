/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Single-object uploads using either a single PUT or multipart upload.
//!
//! Construct an upload through the client's fluent builder or
//! [`UploadInput::builder`](crate::model::UploadInput::builder).
//! Wait for [`UploadHandle::join`](crate::operation::upload::UploadHandle::join) to obtain
//! [`UploadOutput`](crate::model::UploadOutput), which combines service response metadata with
//! transfer metrics. Optional response fields depend on the selected path
//! and the values returned by S3; see [`UploadOutput`](crate::model::UploadOutput)
//! for their provenance.
//!
//! # Integrity inputs
//!
//! An upload fails with [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) when a
//! checksum you supplied is invalid. These are the invalid cases:
//!
//! - A [`PartStream`](crate::io::PartStream) part carries a
//!   [checksum](crate::io::PartData::with_checksum), but the upload has no [`ChecksumStrategy`] set.
//! - [`UploadInput::content_md5`] is set, and the upload is sent as a multipart upload. A
//!   Content-MD5 covers the whole object, which only a single-request upload can carry.

/// Operation builders
pub mod builders;
mod checksum_strategy;
mod request;

mod context;
pub(crate) mod file_body;
mod handle;
mod observability;
mod part_body;
mod transfer;

pub use checksum_strategy::{ChecksumStrategy, ChecksumStrategyBuilder};
pub(crate) use transfer::UploadTransfer;

use crate::config::user_agent::OperationFeatures;
use crate::error;
pub use crate::model::builders::UploadInputBuilder;
pub use crate::model::builders::UploadOutputBuilder;
/// Request type for uploads to Amazon S3
pub use crate::model::UploadInput;
/// Response type for uploads to Amazon S3
pub use crate::model::UploadOutput;
use crate::transfer::TransferContext;
use crate::types::BucketType;
use aws_runtime::sdk_feature::AwsSdkFeature;
pub use handle::UploadHandle;

use std::sync::Arc;

/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
pub(crate) struct Upload;

impl Upload {
    /// Execute a single `Upload` transfer operation.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        Self::orchestrate_inner(handle, input, None)
    }

    /// Execute an `Upload` as a child of another transfer.
    ///
    /// The child's `TransferContext` is linked to `parent_id` so that
    /// `signal_terminal` on the child wakes the parent (letting the parent's
    /// state machine reap it) and cancelling the parent cascades to this child.
    pub(crate) fn orchestrate_child(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::upload::UploadInput,
        parent_id: u64,
    ) -> Result<UploadHandle, error::Error> {
        Self::orchestrate_inner(handle, input, Some(parent_id))
    }

    fn orchestrate_inner(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
        parent_id: Option<u64>,
    ) -> Result<UploadHandle, error::Error> {
        if input.checksum_strategy.is_none() {
            // User didn't explicitly set checksum strategy.
            // If SDK is configured to send checksums: use default checksum strategy.
            // Else: continue with no checksums
            if handle
                .s3_client
                .config()
                .request_checksum_calculation()
                .cloned()
                .unwrap_or_default()
                == aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported
            {
                input.checksum_strategy = Some(ChecksumStrategy::default());
            }
        }

        let stream = input.take_body();

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        // Create transfer context — linked to parent when this is a child
        // transfer, so signal_terminal wakes the parent and cancellation
        // cascades from the parent.
        let (ctx, completion_rx) = match parent_id {
            Some(pid) => TransferContext::new_child(handle.clone(), pid),
            None => TransferContext::new(handle.clone()),
        };

        let features = OperationFeatures::new(
            parent_id
                .map(|_| AwsSdkFeature::S3TransferUploadDirectory)
                .into_iter()
                .chain(stream.is_file_backed().then_some(AwsSdkFeature::S3FilePath)),
        );
        let transfer =
            UploadTransfer::try_new(ctx.with_features(features), bucket_type, input, stream)?;

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(UploadHandle::new(completion_rx, transfer))
    }
}

#[cfg(test)]
mod test {

    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_runtime_api::client::http::{
        http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::UploadInput;
    use crate::types::{
        AbortOutcome, ConcurrencyMode, FailedMultipartUploadPolicy, PartSize, TransferStatus,
    };
    use crate::RuntimeConfig;

    /// Polls `future` once and returns the result of that poll.
    ///
    /// `UploadHandle::abort()` calls `request_abort()` before its first
    /// `.await`, so one poll is enough to know the abort has been requested.
    async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
    }

    /// Connector that holds CreateMultipartUpload until `create_release` gets
    /// a permit, then accepts it, and records the URI of every
    /// AbortMultipartUpload.
    #[derive(Debug)]
    struct HeldCreateMpuConnector {
        create_started: Arc<Semaphore>,
        create_release: Arc<Semaphore>,
        abort_uris: Arc<Mutex<Vec<String>>>,
    }

    impl HttpConnector for HeldCreateMpuConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let uri = request.uri().to_string();
            match request.method() {
                "POST" if uri.contains("uploads") && !uri.contains("uploadId=") => {
                    let create_started = self.create_started.clone();
                    let create_release = self.create_release.clone();
                    HttpConnectorFuture::new(async move {
                        create_started.add_permits(1);
                        create_release
                            .acquire()
                            .await
                            .expect("release semaphore closed")
                            .forget();
                        Ok(HttpResponse::new(
                            200.try_into().unwrap(),
                            SdkBody::from(
                                r#"<?xml version="1.0" encoding="UTF-8"?>
                                    <InitiateMultipartUploadResult>
                                        <Bucket>test-bucket</Bucket>
                                        <Key>test-key</Key>
                                        <UploadId>test-upload-id</UploadId>
                                    </InitiateMultipartUploadResult>"#,
                            ),
                        ))
                    })
                }
                "DELETE" if uri.contains("uploadId=") => {
                    self.abort_uris.lock().unwrap().push(uri);
                    HttpConnectorFuture::ready(Ok(HttpResponse::new(
                        204.try_into().unwrap(),
                        SdkBody::empty(),
                    )))
                }
                method => panic!("unexpected request: {method} {uri}"),
            }
        }
    }

    /// An abort requested while CreateMultipartUpload is in flight waits for
    /// Create's response, and no part is uploaded. Under `AbortUpload` it
    /// aborts the upload Create returned and reports `Cancelled`; under
    /// `Retain` it leaves the upload open and reports `Retained`. Both carry
    /// the upload ID.
    async fn abort_during_create_mpu(
        runtime_config: RuntimeConfig,
        policy: FailedMultipartUploadPolicy,
    ) {
        let retain = matches!(policy, FailedMultipartUploadPolicy::Retain);
        let create_started = Arc::new(Semaphore::new(0));
        let create_release = Arc::new(Semaphore::new(0));
        let abort_uris = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(HeldCreateMpuConnector {
            create_started: create_started.clone(),
            create_release: create_release.clone(),
            abort_uris: abort_uris.clone(),
        });
        let http_client = http_client_fn(move |_, _| connector.clone());
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client)
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .runtime_config(runtime_config)
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client)
            .build();
        let tm = crate::Client::new(tm_config);
        let handle = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from(Bytes::from_static(
                b"force multipart upload",
            )))
            .failed_multipart_upload_policy(policy)
            .initiate_with(&tm)
            .unwrap();

        let permit = tokio::time::timeout(Duration::from_secs(5), create_started.acquire())
            .await
            .expect("CreateMultipartUpload did not start")
            .expect("start semaphore closed");
        permit.forget();

        let mut abort = Box::pin(handle.abort());
        assert!(
            poll_once(&mut abort).await.is_pending(),
            "abort returned while CreateMultipartUpload was held"
        );
        create_release.add_permits(1);

        let aborted = tokio::time::timeout(Duration::from_secs(5), abort)
            .await
            .expect("abort hung after CreateMultipartUpload returned")
            .expect("abort failed");

        assert_eq!(aborted.upload_id(), Some("test-upload-id"));
        let abort_uris = abort_uris.lock().unwrap();
        if retain {
            assert_eq!(aborted.outcome(), AbortOutcome::Retained);
            assert!(
                abort_uris.is_empty(),
                "unexpected AbortMultipartUpload under Retain: {abort_uris:?}"
            );
        } else {
            assert_eq!(aborted.outcome(), AbortOutcome::Cancelled);
            assert_eq!(
                abort_uris.len(),
                1,
                "expected one AbortMultipartUpload: {abort_uris:?}"
            );
            assert!(
                abort_uris[0].contains("uploadId=test-upload-id"),
                "AbortMultipartUpload for the wrong upload: {}",
                abort_uris[0]
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_during_create_mpu_on_tokio_runtime() {
        abort_during_create_mpu(
            RuntimeConfig::multi_thread_tokio(),
            FailedMultipartUploadPolicy::AbortUpload,
        )
        .await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_during_create_mpu_on_managed_runtime() {
        abort_during_create_mpu(
            RuntimeConfig::default(),
            FailedMultipartUploadPolicy::AbortUpload,
        )
        .await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_during_create_mpu_retains_upload_on_tokio_runtime() {
        abort_during_create_mpu(
            RuntimeConfig::multi_thread_tokio(),
            FailedMultipartUploadPolicy::Retain,
        )
        .await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_during_create_mpu_retains_upload_on_managed_runtime() {
        abort_during_create_mpu(
            RuntimeConfig::default(),
            FailedMultipartUploadPolicy::Retain,
        )
        .await;
    }

    /// How [`CompleteMpuConnector`] answers CompleteMultipartUpload.
    #[derive(Debug, Clone, Copy)]
    enum CompleteMpuBehavior {
        /// Wait until `complete_release` gets a permit, then answer
        /// `NoSuchUpload`, as S3 does when the upload was aborted before
        /// Complete committed.
        HoldThenNoSuchUpload,
        /// Wait like `HoldThenNoSuchUpload`, then answer with the same error
        /// as `Fail`.
        HoldThenFail,
        /// Respond with a non-retryable error.
        Fail,
        /// Wait like `HoldThenNoSuchUpload`, then answer 200: Complete
        /// committed before the abort took effect. AbortMultipartUpload is
        /// also held, until `abort_release` gets a permit.
        HoldThenOk,
    }

    /// Connector that serves a single-part multipart upload, applies
    /// [`CompleteMpuBehavior`] to CompleteMultipartUpload, and records the
    /// upload ID of every AbortMultipartUpload. Each AbortMultipartUpload adds
    /// a permit to `abort_started`.
    #[derive(Debug)]
    struct CompleteMpuConnector {
        behavior: CompleteMpuBehavior,
        complete_started: Arc<Semaphore>,
        complete_release: Arc<Semaphore>,
        abort_started: Arc<Semaphore>,
        abort_release: Arc<Semaphore>,
        abort_uris: Arc<Mutex<Vec<String>>>,
    }

    impl HttpConnector for CompleteMpuConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let uri = request.uri().to_string();
            let has_upload_id = uri.contains("uploadId=");
            let response = match (request.method(), has_upload_id) {
                ("POST", false) if uri.contains("uploads") => HttpResponse::new(
                    200.try_into().unwrap(),
                    SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                            <InitiateMultipartUploadResult>
                                <Bucket>test-bucket</Bucket>
                                <Key>test-key</Key>
                                <UploadId>test-upload-id</UploadId>
                            </InitiateMultipartUploadResult>"#,
                    ),
                ),
                ("PUT", true) if uri.contains("partNumber=") => {
                    let mut response = HttpResponse::new(200.try_into().unwrap(), SdkBody::empty());
                    response.headers_mut().insert("ETag", "\"test-etag\"");
                    response
                }
                ("POST", true) => {
                    let complete_started = self.complete_started.clone();
                    let complete_release = self.complete_release.clone();
                    let behavior = self.behavior;
                    return HttpConnectorFuture::new(async move {
                        complete_started.add_permits(1);
                        let behavior = match behavior {
                            CompleteMpuBehavior::HoldThenFail => {
                                complete_release
                                    .acquire()
                                    .await
                                    .expect("release semaphore closed")
                                    .forget();
                                CompleteMpuBehavior::Fail
                            }
                            other => other,
                        };
                        match behavior {
                            CompleteMpuBehavior::HoldThenNoSuchUpload => {
                                complete_release
                                    .acquire()
                                    .await
                                    .expect("release semaphore closed")
                                    .forget();
                                Ok(HttpResponse::new(
                                    404.try_into().unwrap(),
                                    SdkBody::from(
                                        r#"<?xml version="1.0" encoding="UTF-8"?>
                                    <Error>
                                        <Code>NoSuchUpload</Code>
                                        <Message>The specified upload does not exist.</Message>
                                    </Error>"#,
                                    ),
                                ))
                            }
                            CompleteMpuBehavior::Fail => Ok(HttpResponse::new(
                                400.try_into().unwrap(),
                                SdkBody::from(
                                    r#"<?xml version="1.0" encoding="UTF-8"?>
                                    <Error>
                                        <Code>InvalidPart</Code>
                                        <Message>One or more of the specified parts could not be found.</Message>
                                    </Error>"#,
                                ),
                            )),
                            CompleteMpuBehavior::HoldThenOk => {
                                complete_release
                                    .acquire()
                                    .await
                                    .expect("release semaphore closed")
                                    .forget();
                                Ok(HttpResponse::new(
                                    200.try_into().unwrap(),
                                    SdkBody::from(
                                        r#"<?xml version="1.0" encoding="UTF-8"?>
                                    <CompleteMultipartUploadResult>
                                        <Bucket>test-bucket</Bucket>
                                        <Key>test-key</Key>
                                        <ETag>"test-object-etag"</ETag>
                                    </CompleteMultipartUploadResult>"#,
                                    ),
                                ))
                            }
                            CompleteMpuBehavior::HoldThenFail => unreachable!("resolved above"),
                        }
                    });
                }
                ("DELETE", true) => {
                    self.abort_uris.lock().unwrap().push(uri);
                    self.abort_started.add_permits(1);
                    if let CompleteMpuBehavior::HoldThenOk = self.behavior {
                        let abort_release = self.abort_release.clone();
                        return HttpConnectorFuture::new(async move {
                            abort_release
                                .acquire()
                                .await
                                .expect("release semaphore closed")
                                .forget();
                            Ok(HttpResponse::new(204.try_into().unwrap(), SdkBody::empty()))
                        });
                    }
                    HttpResponse::new(204.try_into().unwrap(), SdkBody::empty())
                }
                _ => panic!("unexpected request: {} {uri}", request.method()),
            };
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    /// Semaphores and recorded requests shared with a [`CompleteMpuConnector`].
    struct CompleteMpuProbe {
        complete_started: Arc<Semaphore>,
        complete_release: Arc<Semaphore>,
        abort_started: Arc<Semaphore>,
        abort_release: Arc<Semaphore>,
        abort_uris: Arc<Mutex<Vec<String>>>,
    }

    /// Starts a single-part multipart upload against a
    /// [`CompleteMpuConnector`] and waits until CompleteMultipartUpload has
    /// been sent.
    async fn start_upload_until_complete_mpu(
        runtime_config: RuntimeConfig,
        behavior: CompleteMpuBehavior,
        concurrency: usize,
    ) -> (crate::operation::upload::UploadHandle, CompleteMpuProbe) {
        let probe = CompleteMpuProbe {
            complete_started: Arc::new(Semaphore::new(0)),
            complete_release: Arc::new(Semaphore::new(0)),
            abort_started: Arc::new(Semaphore::new(0)),
            abort_release: Arc::new(Semaphore::new(0)),
            abort_uris: Arc::new(Mutex::new(Vec::new())),
        };
        let connector = SharedHttpConnector::new(CompleteMpuConnector {
            behavior,
            complete_started: probe.complete_started.clone(),
            complete_release: probe.complete_release.clone(),
            abort_started: probe.abort_started.clone(),
            abort_release: probe.abort_release.clone(),
            abort_uris: probe.abort_uris.clone(),
        });
        let http_client = http_client_fn(move |_, _| connector.clone());
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client)
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(concurrency))
            .runtime_config(runtime_config)
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client)
            .build();
        let tm = crate::Client::new(tm_config);
        let handle = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from(Bytes::from_static(
                b"force multipart upload",
            )))
            .initiate_with(&tm)
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), probe.complete_started.acquire())
            .await
            .expect("CompleteMultipartUpload did not start")
            .expect("start semaphore closed")
            .forget();
        (handle, probe)
    }

    /// An abort requested while CompleteMultipartUpload is in flight sends
    /// AbortMultipartUpload without interrupting Complete. When Complete then
    /// returns `NoSuchUpload` (the abort won), the upload is cancelled.
    ///
    /// Concurrency 2 so AbortMultipartUpload can go out while Complete holds a
    /// slot; Complete is released only after S3 has seen the abort, as in the
    /// real race.
    async fn abort_during_complete_mpu(runtime_config: RuntimeConfig) {
        let (handle, probe) = start_upload_until_complete_mpu(
            runtime_config,
            CompleteMpuBehavior::HoldThenNoSuchUpload,
            2,
        )
        .await;

        let mut abort = Box::pin(handle.abort());
        assert!(
            poll_once(&mut abort).await.is_pending(),
            "abort returned while CompleteMultipartUpload was held"
        );
        tokio::time::timeout(Duration::from_secs(5), probe.abort_started.acquire())
            .await
            .expect("AbortMultipartUpload was not sent while Complete was in flight")
            .expect("abort semaphore closed")
            .forget();
        probe.complete_release.add_permits(1);

        let aborted = tokio::time::timeout(Duration::from_secs(5), abort)
            .await
            .expect("abort hung after CompleteMultipartUpload returned")
            .expect("abort failed");

        assert_eq!(aborted.outcome(), AbortOutcome::Cancelled);
        assert_eq!(aborted.upload_id(), Some("test-upload-id"));
        let abort_uris = probe.abort_uris.lock().unwrap();
        assert_eq!(
            abort_uris.len(),
            1,
            "expected one AbortMultipartUpload: {abort_uris:?}"
        );
        assert!(
            abort_uris[0].contains("uploadId=test-upload-id"),
            "AbortMultipartUpload for the wrong upload: {}",
            abort_uris[0]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_during_complete_mpu_aborts_upload_on_tokio_runtime() {
        abort_during_complete_mpu(RuntimeConfig::multi_thread_tokio()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_during_complete_mpu_aborts_upload_on_managed_runtime() {
        abort_during_complete_mpu(RuntimeConfig::default()).await;
    }

    /// An abort requested while CompleteMultipartUpload is in flight sends
    /// AbortMultipartUpload. When Complete then fails with an error other
    /// than `NoSuchUpload`, `abort()` returns that error instead of
    /// `Cancelled`.
    async fn complete_mpu_failure_during_abort(runtime_config: RuntimeConfig) {
        let (handle, probe) =
            start_upload_until_complete_mpu(runtime_config, CompleteMpuBehavior::HoldThenFail, 2)
                .await;

        let mut abort = Box::pin(handle.abort());
        assert!(
            poll_once(&mut abort).await.is_pending(),
            "abort returned while CompleteMultipartUpload was held"
        );
        tokio::time::timeout(Duration::from_secs(5), probe.abort_started.acquire())
            .await
            .expect("AbortMultipartUpload was not sent while Complete was in flight")
            .expect("abort semaphore closed")
            .forget();
        probe.complete_release.add_permits(1);

        let error = tokio::time::timeout(Duration::from_secs(5), abort)
            .await
            .expect("abort hung after CompleteMultipartUpload returned")
            .expect_err("a Complete failure during abort must be returned");
        assert_eq!(error.code(), Some("InvalidPart"));
        let abort_uris = probe.abort_uris.lock().unwrap();
        assert_eq!(
            abort_uris.len(),
            1,
            "expected one AbortMultipartUpload: {abort_uris:?}"
        );
        assert!(
            abort_uris[0].contains("uploadId=test-upload-id"),
            "AbortMultipartUpload for the wrong upload: {}",
            abort_uris[0]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn complete_mpu_failure_during_abort_returns_error_on_tokio_runtime() {
        complete_mpu_failure_during_abort(RuntimeConfig::multi_thread_tokio()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn complete_mpu_failure_during_abort_returns_error_on_managed_runtime() {
        complete_mpu_failure_during_abort(RuntimeConfig::default()).await;
    }

    /// A failed CompleteMultipartUpload aborts the multipart upload, and
    /// `abort()` on the failed upload returns the failure without sending a
    /// second AbortMultipartUpload.
    async fn abort_after_failed_complete_mpu(runtime_config: RuntimeConfig) {
        let (handle, probe) =
            start_upload_until_complete_mpu(runtime_config, CompleteMpuBehavior::Fail, 1).await;

        tokio::time::timeout(Duration::from_secs(5), async {
            while handle.status() != TransferStatus::Failed {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("upload did not fail after CompleteMultipartUpload error");

        let error = tokio::time::timeout(Duration::from_secs(5), handle.abort())
            .await
            .expect("abort hung on a failed upload")
            .expect_err("abort on a failed upload must return its error");
        assert_eq!(error.code(), Some("InvalidPart"));
        let abort_uris = probe.abort_uris.lock().unwrap();
        assert_eq!(
            abort_uris.len(),
            1,
            "expected one AbortMultipartUpload: {abort_uris:?}"
        );
        assert!(
            abort_uris[0].contains("uploadId=test-upload-id"),
            "AbortMultipartUpload for the wrong upload: {}",
            abort_uris[0]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_after_failed_complete_mpu_returns_error_on_tokio_runtime() {
        abort_after_failed_complete_mpu(RuntimeConfig::multi_thread_tokio()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_after_failed_complete_mpu_returns_error_on_managed_runtime() {
        abort_after_failed_complete_mpu(RuntimeConfig::default()).await;
    }

    /// An abort requested while CompleteMultipartUpload is in flight sends
    /// AbortMultipartUpload. When Complete then returns 200 (S3 committed
    /// first), `abort()` reports `AlreadyCompleted` with the upload output,
    /// and returns only after AbortMultipartUpload has finished.
    async fn complete_mpu_wins_race_with_abort(runtime_config: RuntimeConfig) {
        let (handle, probe) =
            start_upload_until_complete_mpu(runtime_config, CompleteMpuBehavior::HoldThenOk, 2)
                .await;

        let mut abort = Box::pin(handle.abort());
        assert!(
            poll_once(&mut abort).await.is_pending(),
            "abort returned while CompleteMultipartUpload was held"
        );
        tokio::time::timeout(Duration::from_secs(5), probe.abort_started.acquire())
            .await
            .expect("AbortMultipartUpload was not sent while Complete was in flight")
            .expect("abort semaphore closed")
            .forget();
        probe.complete_release.add_permits(1);

        // Complete has committed, but AbortMultipartUpload is still held.
        // There is no signal for when the transfer has handled Complete's
        // response, so give it time; a regression returns early here.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            poll_once(&mut abort).await.is_pending(),
            "abort returned while AbortMultipartUpload was still in flight"
        );
        probe.abort_release.add_permits(1);

        let aborted = tokio::time::timeout(Duration::from_secs(5), abort)
            .await
            .expect("abort hung after AbortMultipartUpload returned")
            .expect("abort failed");

        assert_eq!(aborted.outcome(), AbortOutcome::AlreadyCompleted);
        assert!(
            aborted.output().is_some(),
            "AlreadyCompleted must carry the upload output"
        );
        let abort_uris = probe.abort_uris.lock().unwrap();
        assert_eq!(
            abort_uris.len(),
            1,
            "expected one AbortMultipartUpload: {abort_uris:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn complete_mpu_wins_race_with_abort_on_tokio_runtime() {
        complete_mpu_wins_race_with_abort(RuntimeConfig::multi_thread_tokio()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn complete_mpu_wins_race_with_abort_on_managed_runtime() {
        complete_mpu_wins_race_with_abort(RuntimeConfig::default()).await;
    }

    /// Connector that holds PutObject forever, adding a permit to
    /// `put_started` when it arrives, and records the URI of any other
    /// request.
    #[derive(Debug)]
    struct HeldPutObjectConnector {
        put_started: Arc<Semaphore>,
        other_uris: Arc<Mutex<Vec<String>>>,
    }

    impl HttpConnector for HeldPutObjectConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let uri = request.uri().to_string();
            if request.method() == "PUT" && !uri.contains("uploadId=") {
                self.put_started.add_permits(1);
                return HttpConnectorFuture::new(std::future::pending());
            }
            self.other_uris
                .lock()
                .unwrap()
                .push(format!("{} {uri}", request.method()));
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                500.try_into().unwrap(),
                SdkBody::empty(),
            )))
        }
    }

    /// An abort interrupts an in-flight PutObject instead of waiting for it.
    /// The upload ends as cancelled with no upload ID, and nothing else is
    /// sent.
    async fn abort_interrupts_put_object(runtime_config: RuntimeConfig) {
        let put_started = Arc::new(Semaphore::new(0));
        let other_uris = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(HeldPutObjectConnector {
            put_started: put_started.clone(),
            other_uris: other_uris.clone(),
        });
        let http_client = http_client_fn(move |_, _| connector.clone());
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client)
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .runtime_config(runtime_config)
            .set_multipart_threshold(PartSize::Target(ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client)
            .build();
        let tm = crate::Client::new(tm_config);
        let handle = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from(Bytes::from_static(b"single put object")))
            .initiate_with(&tm)
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), put_started.acquire())
            .await
            .expect("PutObject did not start")
            .expect("start semaphore closed")
            .forget();

        let aborted = tokio::time::timeout(Duration::from_secs(5), handle.abort())
            .await
            .expect("abort waited for the held PutObject")
            .expect("abort failed");

        assert_eq!(aborted.outcome(), AbortOutcome::Cancelled);
        assert_eq!(aborted.upload_id(), None);
        let other_uris = other_uris.lock().unwrap();
        assert!(other_uris.is_empty(), "unexpected requests: {other_uris:?}");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_interrupts_put_object_on_tokio_runtime() {
        abort_interrupts_put_object(RuntimeConfig::multi_thread_tokio()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_interrupts_put_object_on_managed_runtime() {
        abort_interrupts_put_object(RuntimeConfig::default()).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_abort_upload() {
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let create_mpu =
            mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
                CreateMultipartUploadOutput::builder()
                    .upload_id("test-upload-id")
                    .build()
            });

        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());

        let abort_mpu = mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .then_output(|| AbortMultipartUploadOutput::builder().build());

        // The test races abort against the upload completion. Under slow execution
        // (e.g. ASAN-instrumented), the single-part upload can reach
        // CompleteMultipartUpload before abort dispatches. Provide a rule for that
        // case so the mock doesn't panic on no-rule-matched, which would crash a
        // managed thread and leave Arc cycles for LeakSanitizer to flag.
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .then_output(|| CompleteMultipartUploadOutput::builder().build());

        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            &[create_mpu, upload_part, abort_mpu, complete_mpu]
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client)
            .build();

        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);
        let handle = request.initiate_with(&tm).unwrap();

        // Small delay to let scheduler start processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Abort should complete without error
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.abort())
            .await
            .expect("abort timed out");
        assert!(result.is_ok());
    }
}

/// Integration-style tests using StaticReplayClient for retry behavior
#[cfg(test)]
mod retry_tests {
    use aws_sdk_s3::config::Region;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::types::{ConcurrencyMode, PartSize};

    fn dummy_request() -> http::Request<SdkBody> {
        http::Request::builder().body(SdkBody::empty()).unwrap()
    }

    /// Test that SDK retries transient errors for upload_part.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_upload_part_retry() {
        // Responses in order: CreateMPU, UploadPart (500), UploadPart (200 retry), CompleteMPU
        let http_client = StaticReplayClient::new(vec![
            // CreateMultipartUpload - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <InitiateMultipartUploadResult>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <UploadId>test-upload-id</UploadId>
                        </InitiateMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
            // UploadPart - first attempt fails with 500
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(500)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>InternalError</Code>
                            <Message>Internal Server Error</Message>
                        </Error>"#,
                    ))
                    .unwrap(),
            ),
            // UploadPart - retry succeeds
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .header("ETag", "\"test-etag\"")
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // CompleteMultipartUpload - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult>
                            <Location>https://test-bucket.s3.amazonaws.com/test-key</Location>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <ETag>"final-etag"</ETag>
                        </CompleteMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
        ]);

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client.clone())
                .region(Region::from_static("us-west-2"))
                .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(s3_client)
            .build();

        let tm = crate::Client::new(tm_config);

        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream)
            .initiate()
            .unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.join())
            .await
            .expect("join timed out");
        assert!(
            result.is_ok(),
            "upload should succeed after retry: {:?}",
            result.err()
        );

        // Verify all 4 requests were made (including the retry)
        let requests: Vec<_> = http_client.actual_requests().collect();
        assert_eq!(
            4,
            requests.len(),
            "should have made 4 requests (including retry)"
        );
    }

    /// Test that SDK retries an `InternalError` embedded in a CompleteMultipartUpload HTTP 200.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_complete_mpu_http_200_internal_error_retry() {
        // Responses in order: CreateMPU, UploadPart, CompleteMPU (200 + error), CompleteMPU (retry)
        let http_client = StaticReplayClient::new(vec![
            // CreateMultipartUpload - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <InitiateMultipartUploadResult>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <UploadId>test-upload-id</UploadId>
                        </InitiateMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
            // UploadPart - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .header("ETag", "\"test-etag\"")
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // CompleteMultipartUpload - HTTP 200 carrying an InternalError body
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>InternalError</Code>
                            <Message>We encountered an internal error. Please try again.</Message>
                            <RequestId>656c76696e6727732072657175657374</RequestId>
                            <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPf==</HostId>
                        </Error>"#,
                    ))
                    .unwrap(),
            ),
            // CompleteMultipartUpload - retry succeeds
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult>
                            <Location>https://test-bucket.s3.amazonaws.com/test-key</Location>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <ETag>"final-etag"</ETag>
                        </CompleteMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
        ]);

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client.clone())
                .region(Region::from_static("us-west-2"))
                .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(s3_client)
            .build();

        let tm = crate::Client::new(tm_config);

        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream)
            .initiate()
            .unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.join())
            .await
            .expect("join timed out");
        assert!(
            result.is_ok(),
            "upload should succeed after CompleteMultipartUpload retry: {:?}",
            result.err()
        );

        // Verify all 4 requests were made (including the retry)
        let requests: Vec<_> = http_client.actual_requests().collect();
        assert_eq!(
            4,
            requests.len(),
            "should have made 4 requests (including retry)"
        );
    }
}
