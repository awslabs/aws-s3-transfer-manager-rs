/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;
mod checksum_strategy;
mod input;
mod output;

mod context;
pub(crate) mod file_body;
mod handle;
mod observability;
mod part_body;
mod transfer;

pub use checksum_strategy::{ChecksumStrategy, ChecksumStrategyBuilder};
pub(crate) use transfer::UploadTransfer;

use crate::error;
use crate::transfer::TransferContext;
use crate::types::BucketType;
pub use handle::UploadHandle;
/// Request type for uploads to Amazon S3
pub use input::{UploadInput, UploadInputBuilder};
/// Response type for uploads to Amazon S3
pub use output::{UploadOutput, UploadOutputBuilder};

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

        let transfer = UploadTransfer::try_new(ctx, bucket_type, input, stream)?;

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
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::UploadInput;
    use crate::types::{ConcurrencyMode, PartSize, RuntimeMode, TransferStatus};

    /// Connector that keeps CreateMultipartUpload active until its future is
    /// interrupted by the transfer runtime.
    #[derive(Debug)]
    struct StalledCreateMpuConnector {
        create_started: Arc<Semaphore>,
    }

    impl HttpConnector for StalledCreateMpuConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            assert_eq!("POST", request.method());
            assert!(
                request.uri().contains("uploads"),
                "unexpected request: {}",
                request.uri()
            );

            let create_started = self.create_started.clone();
            HttpConnectorFuture::new(async move {
                create_started.add_permits(1);
                std::future::pending::<Result<HttpResponse, ConnectorError>>().await
            })
        }
    }

    /// Abort must terminate after CreateMultipartUpload enters execution.
    async fn abort_stalled_create_mpu(runtime_mode: RuntimeMode) {
        let create_started = Arc::new(Semaphore::new(0));
        let connector = SharedHttpConnector::new(StalledCreateMpuConnector {
            create_started: create_started.clone(),
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
            .runtime_mode(runtime_mode)
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .client(client)
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

        let permit = tokio::time::timeout(Duration::from_secs(5), create_started.acquire())
            .await
            .expect("CreateMultipartUpload did not start")
            .expect("start semaphore closed");
        permit.forget();

        tokio::time::timeout(Duration::from_secs(1), handle.abort())
            .await
            .expect("abort hung after CreateMultipartUpload entered execution")
            .expect("abort failed");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_stalled_create_mpu_on_tokio_runtime() {
        abort_stalled_create_mpu(RuntimeMode::MultiThreadTokio).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_stalled_create_mpu_on_managed_runtime() {
        abort_stalled_create_mpu(RuntimeMode::Managed).await;
    }

    /// How [`CompleteMpuConnector`] answers CompleteMultipartUpload.
    #[derive(Debug, Clone, Copy)]
    enum CompleteMpuBehavior {
        /// Never respond; the request stays active until interrupted.
        Stall,
        /// Respond with a non-retryable error.
        Fail,
    }

    /// Connector that serves a single-part multipart upload, applies
    /// [`CompleteMpuBehavior`] to CompleteMultipartUpload, and records the
    /// upload ID of every AbortMultipartUpload.
    #[derive(Debug)]
    struct CompleteMpuConnector {
        behavior: CompleteMpuBehavior,
        complete_started: Arc<Semaphore>,
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
                    let behavior = self.behavior;
                    return HttpConnectorFuture::new(async move {
                        complete_started.add_permits(1);
                        match behavior {
                            CompleteMpuBehavior::Stall => {
                                std::future::pending::<Result<HttpResponse, ConnectorError>>().await
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
                        }
                    });
                }
                ("DELETE", true) => {
                    self.abort_uris.lock().unwrap().push(uri);
                    HttpResponse::new(204.try_into().unwrap(), SdkBody::empty())
                }
                _ => panic!("unexpected request: {} {uri}", request.method()),
            };
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    /// Abort after CompleteMultipartUpload started must still abort the upload,
    /// whether the completion is interrupted or has already failed.
    async fn abort_during_complete_mpu(runtime_mode: RuntimeMode, behavior: CompleteMpuBehavior) {
        let complete_started = Arc::new(Semaphore::new(0));
        let abort_uris = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(CompleteMpuConnector {
            behavior,
            complete_started: complete_started.clone(),
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
            .runtime_mode(runtime_mode)
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .client(client)
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

        let permit = tokio::time::timeout(Duration::from_secs(5), complete_started.acquire())
            .await
            .expect("CompleteMultipartUpload did not start")
            .expect("start semaphore closed");
        permit.forget();

        if let CompleteMpuBehavior::Fail = behavior {
            tokio::time::timeout(Duration::from_secs(5), async {
                while handle.status() != TransferStatus::Failed {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("upload did not fail after CompleteMultipartUpload error");
        }

        let aborted = tokio::time::timeout(Duration::from_secs(5), handle.abort())
            .await
            .expect("abort hung after CompleteMultipartUpload started")
            .expect("abort failed");

        let abort_uris = abort_uris.lock().unwrap();
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
        assert_eq!(aborted.upload_id(), Some("test-upload-id"));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_during_complete_mpu_aborts_upload_on_tokio_runtime() {
        abort_during_complete_mpu(RuntimeMode::MultiThreadTokio, CompleteMpuBehavior::Stall).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_during_complete_mpu_aborts_upload_on_managed_runtime() {
        abort_during_complete_mpu(RuntimeMode::Managed, CompleteMpuBehavior::Stall).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_after_failed_complete_mpu_aborts_upload_on_tokio_runtime() {
        abort_during_complete_mpu(RuntimeMode::MultiThreadTokio, CompleteMpuBehavior::Fail).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_after_failed_complete_mpu_aborts_upload_on_managed_runtime() {
        abort_during_complete_mpu(RuntimeMode::Managed, CompleteMpuBehavior::Fail).await;
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
            .client(client)
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
            .client(s3_client)
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
}
