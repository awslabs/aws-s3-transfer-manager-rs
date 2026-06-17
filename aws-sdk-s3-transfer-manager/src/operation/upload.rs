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

        // TODO: Relax this constraint - unknown content length implies MPU
        if stream.size_hint().upper().is_none() {
            return Err(crate::io::error::Error::upper_bound_size_hint_required().into());
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        // Create transfer context — linked to parent when this is a child
        // transfer, so signal_terminal wakes the parent and cancellation
        // cascades from the parent.
        let (ctx, completion_rx) = match parent_id {
            Some(pid) => TransferContext::new_child(handle.clone(), pid),
            None => TransferContext::new(handle.clone()),
        };

        let transfer = UploadTransfer::new(ctx, bucket_type, input, stream);

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
    use bytes::Bytes;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::UploadInput;
    use crate::types::{ConcurrencyMode, PartSize};

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
            .then_output(|| UploadPartOutput::builder().build());

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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use aws_sdk_s3::config::Region;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
    };
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

    /// Stalls the first `UploadPart` past the adaptive deadline. The stall
    /// sleeps before delegating, so the guard drops the timed-out future before
    /// the inner connector sees that attempt; only the retry reaches it.
    #[derive(Debug, Clone)]
    struct StallFirstUploadPart {
        inner: StaticReplayClient,
        stalled: Arc<std::sync::atomic::AtomicBool>,
        upload_part_calls: Arc<AtomicUsize>,
    }

    impl HttpClient for StallFirstUploadPart {
        fn http_connector(
            &self,
            settings: &aws_smithy_runtime_api::client::http::HttpConnectorSettings,
            components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(StallConnector {
                inner: self.inner.http_connector(settings, components),
                stalled: self.stalled.clone(),
                upload_part_calls: self.upload_part_calls.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct StallConnector {
        inner: SharedHttpConnector,
        stalled: Arc<std::sync::atomic::AtomicBool>,
        upload_part_calls: Arc<AtomicUsize>,
    }

    impl HttpConnector for StallConnector {
        fn call(
            &self,
            request: aws_smithy_runtime_api::client::orchestrator::HttpRequest,
        ) -> HttpConnectorFuture {
            // UploadPart is the only POST carrying `partNumber` in the query.
            let is_upload_part = request.uri().contains("partNumber=");
            if is_upload_part {
                self.upload_part_calls.fetch_add(1, Ordering::SeqCst);
            }
            let stall = is_upload_part
                && self
                    .stalled
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok();
            let inner = self.inner.clone();
            HttpConnectorFuture::new(async move {
                if stall {
                    // Outlive the deadline; the guard drops this future before
                    // the inner connector is called.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                inner.call(request).await
            })
        }
    }

    /// The TM's per-part deadline guard re-issues a part whose send exceeds the
    /// adaptive latency deadline — distinct from SDK retry, which is DISABLED
    /// here so the deadline-retry layer is the only thing that can recover.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_upload_part_deadline_retry() {
        // Sequence the inner connector sees: CreateMPU, UploadPart (ok),
        // CompleteMPU. The first (stalled) UploadPart attempt is dropped at the
        // deadline before it reaches the connector, so only the retry hits replay.
        let inner = StaticReplayClient::new(vec![
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
            // UploadPart - the retry (the stalled first attempt never arrives).
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .header("ETag", "\"test-etag\"")
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
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

        let upload_part_calls = Arc::new(AtomicUsize::new(0));
        let http_client = StallFirstUploadPart {
            inner,
            stalled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            upload_part_calls: upload_part_calls.clone(),
        };

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client)
                .region(Region::from_static("us-west-2"))
                // SDK retry OFF: the deadline-retry layer is the only recovery.
                .retry_config(aws_config::retry::RetryConfig::disabled())
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

        // Warm the send-latency tracker so the adaptive deadline is active
        // (~200ms); without warmup the guard applies no timeout and the stall
        // would never be cut short.
        for _ in 0..15 {
            tm.handle
                .telemetry
                .send_latencies
                .record(Duration::from_millis(100));
        }

        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream)
            .initiate()
            .unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), handle.join())
            .await
            .expect("join timed out");
        assert!(
            result.is_ok(),
            "upload should succeed after the deadline-driven part retry: {:?}",
            result.err()
        );

        // The part was attempted twice: the stalled attempt (cut at the deadline)
        // and the retry that succeeded. SDK retry is disabled, so the second
        // attempt is the TM deadline-retry layer.
        assert_eq!(
            upload_part_calls.load(Ordering::SeqCst),
            2,
            "expected one stalled UploadPart attempt plus one retry",
        );
    }
}
