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
mod handle;
mod transfer;

pub use checksum_strategy::{ChecksumStrategy, ChecksumStrategyBuilder};
pub(crate) use transfer::UploadTransfer;

use crate::error;
use crate::operation::TransferContext;
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
    /// Execute a single `Upload` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        // TODO(redux): we were getting checksum behavior for free from SDK, moving to presigning and dedicated HTTP stack requires us to consider that
        if input.checksum_strategy.is_none() {
            // User didn't explicitly set checksum strategy.
            // If SDK is configured to send checksums: use default checksum strategy.
            // Else: continue with no checksums
            if handle
                .config
                .client()
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

        // TODO(redux): Relax this constraint - unknown content length implies MPU
        if stream.size_hint().upper().is_none() {
            return Err(crate::io::error::Error::upper_bound_size_hint_required().into());
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        // Create transfer context - completion_rx signals terminal state
        let (ctx, _completion_rx) = TransferContext::new(handle.clone());

        // Result channel for upload output
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let transfer = UploadTransfer::new(ctx, bucket_type, input, stream, result_tx);

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(UploadHandle::new(result_rx, transfer))
    }
}

#[cfg(test)]
mod test {

    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use bytes::Bytes;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::UploadInput;
    use crate::types::{ConcurrencyMode, PartSize};

    // TODO(redux): This test should migrate to an integration test with better mock server
    // support for timing coordination. Currently it just verifies abort() doesn't panic
    // and returns an AbortedUpload. A proper test would verify AbortMultipartUpload is
    // called with the correct upload_id after CreateMPU completes.
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

        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            &[create_mpu, upload_part, abort_mpu]
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
