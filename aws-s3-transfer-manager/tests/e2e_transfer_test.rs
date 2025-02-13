#![cfg(e2e_test)]
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Tests here requires AWS account with pre-configured S3 bucket to run the tests.
// Refer to https://github.com/awslabs/aws-c-s3/tree/main/tests/test_helper to help set up the S3 in the account
// Set CRT_S3_TEST_BUCKET_NAME environment variables to the bucket created.
// By default, it uses s3-tm-rs-test-bucket

use aws_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3::types::ChecksumMode;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use test_common::{drain, global_uuid_str};

use tokio::time::Sleep;

use aws_s3_transfer_manager::types::PartSize;

const PUT_OBJECT_PREFIX: &str = "upload/";

fn get_bucket_names() -> (String, String) {
    let bucket_name = option_env!("CRT_S3_TEST_BUCKET_NAME")
        .unwrap_or("s3-tm-rs-test-bucket")
        .to_owned();
    let express_bucket_name = format!("{}--usw2-az1--x-s3", bucket_name.as_str());
    (bucket_name, express_bucket_name)
}

fn generate_key(readable_key: &str) -> String {
    format!(
        "{}{}/{}",
        PUT_OBJECT_PREFIX,
        global_uuid_str(),
        readable_key
    )
}

async fn test_tm() -> (aws_s3_transfer_manager::Client, aws_sdk_s3::Client) {
    let tm_config = aws_s3_transfer_manager::from_env()
        .part_size(PartSize::Target(8 * ByteUnit::Mebibyte.as_bytes_u64()))
        .load()
        .await;
    let client = tm_config.client().clone();
    let tm = aws_s3_transfer_manager::Client::new(tm_config);
    (tm, client)
}

fn create_input_stream(size: usize) -> InputStream {
    "This is a test"
        .bytes()
        .cycle()
        .take(size)
        .collect::<Vec<u8>>()
        .into()
}

async fn perform_upload(
    tm: &aws_s3_transfer_manager::Client,
    bucket_name: &str,
    key: &str,
    strategy: Option<ChecksumStrategy>,
    stream: InputStream,
) {
    let mut upload = tm.upload().bucket(bucket_name).key(key).body(stream);

    if let Some(strategy) = strategy {
        upload = upload.checksum_strategy(strategy);
    }

    upload.initiate().unwrap().join().await.unwrap();
}

async fn test_round_trip_helper(file_size: usize, bucket_name: &str, object_key: &str) {
    let (tm, _) = test_tm().await;
    perform_upload(
        &tm,
        bucket_name,
        object_key,
        None,
        create_input_stream(file_size),
    )
    .await;
    let mut download_handle = tm
        .download()
        .bucket(bucket_name)
        .key(object_key)
        .initiate()
        .unwrap();

    let body = drain(&mut download_handle).await.unwrap();

    assert_eq!(body.len(), file_size);
}

#[tokio::test]
async fn test_single_part_file_round_trip() {
    let file_size = 1024 * 1024; // 1MB
    let object_key = generate_key("1MB");
    let (bucket_name, express_bucket_name) = get_bucket_names();
    test_round_trip_helper(file_size, bucket_name.as_str(), &object_key).await;
    test_round_trip_helper(file_size, express_bucket_name.as_str(), &object_key).await;
}

#[tokio::test]
async fn test_multi_part_file_round_trip() {
    let file_size = 20 * 1024 * 1024; // 20MB
    let object_key = generate_key("20MB");
    let (bucket_name, express_bucket_name) = get_bucket_names();
    test_round_trip_helper(file_size, bucket_name.as_str(), &object_key).await;
    test_round_trip_helper(file_size, express_bucket_name.as_str(), &object_key).await;
}

async fn get_checksum(s3_client: &aws_sdk_s3::Client, bucket_name: &str, key: &str) -> String {
    s3_client
        .head_object()
        .bucket(bucket_name)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
        .checksum_crc32()
        .unwrap()
        .to_owned()
}

async fn upload_and_get_checksum(
    tm: &aws_s3_transfer_manager::Client,
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    strategy: ChecksumStrategy,
    size: usize,
) -> String {
    perform_upload(tm, bucket, key, Some(strategy), create_input_stream(size)).await;
    get_checksum(s3_client, bucket, key).await
}

async fn run_checksum_test(size_mb: usize, key_suffix: &str, is_multi_part: bool) {
    let file_size = size_mb * 1024 * 1024;
    let object_key = generate_key(key_suffix);
    let (tm, s3_client) = test_tm().await;

    let (bucket_name, express_bucket_name) = get_bucket_names();
    // Test both regular S3 and S3 express
    for bucket in [bucket_name.as_str(), express_bucket_name.as_str()] {
        // First upload: calculated CRC32
        let checksum = upload_and_get_checksum(
            &tm,
            &s3_client,
            bucket,
            &object_key,
            ChecksumStrategy::with_calculated_crc32(),
            file_size,
        )
        .await;

        // Second upload: precomputed CRC32
        upload_and_get_checksum(
            &tm,
            &s3_client,
            bucket,
            &object_key,
            ChecksumStrategy::with_crc32(&checksum),
            file_size,
        )
        .await;

        // Third upload: composite CRC32
        let composite_key = format!("{}-composite", object_key);
        let composite_checksum = upload_and_get_checksum(
            &tm,
            &s3_client,
            bucket,
            &composite_key,
            ChecksumStrategy::with_calculated_crc32_composite_if_multipart(),
            file_size,
        )
        .await;

        if is_multi_part {
            // A multipart upload, so that the composite checksum should ends with `-<number of parts>`
            let num_parts = (size_mb as f64 / 8f64).ceil() as u32;
            assert!(composite_checksum.ends_with(&format!("-{}", num_parts)));
        } else {
            // A single object upload, so that the composite checksum should not contain `-` and matches the checksum before
            assert!(!composite_checksum.contains('-'));
            assert_eq!(composite_checksum, checksum);
        }
    }
}

#[tokio::test]
async fn test_multi_part_file_checksum_upload() {
    run_checksum_test(20, "20MB-crc32", true).await;
}

#[tokio::test]
async fn test_single_part_file_checksum_upload() {
    run_checksum_test(1, "1MB-crc32", false).await;
}

// TODO: add checksum validation tests for get object

#[derive(Debug)]
struct TestStream {
    idx: usize,
    remaining: usize,
    delay: Option<Pin<Box<Sleep>>>,
}

impl TestStream {
    fn new_delay(total_size: usize) -> Self {
        let delay = Box::pin(tokio::time::sleep(Duration::from_secs(5)));
        Self {
            idx: 0,
            remaining: total_size,
            delay: Some(delay),
        }
    }
}

impl PartStream for TestStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let part_size = stream_cx.part_size();
        // Check if we need to wait for the delay
        if let Some(delay) = &mut self.delay {
            match delay.as_mut().poll(cx) {
                Poll::Ready(_) => self.delay = None,
                Poll::Pending => return Poll::Pending,
            }
        }

        if self.remaining == 0 {
            // End of stream
            Poll::Ready(None)
        } else {
            let part_data_size = std::cmp::min(part_size, self.remaining);
            let data = "This is a test"
                .bytes()
                .cycle()
                .take(part_data_size)
                .collect::<Vec<u8>>();
            let part = PartData::new((self.idx + 1) as u64, data);

            // Update state
            self.idx += 1;
            self.remaining -= part_data_size;
            // Schedule delay for NEXT part
            self.delay = Some(Box::pin(tokio::time::sleep(Duration::from_secs(3))));
            Poll::Ready(Some(Ok(part)))
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.remaining as u64)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_upload_with_long_running_stream() {
    let (tm, _) = test_tm().await;
    let file_size = 10 * 1024 * 1024; // 10MB
    let num_uploads = 10;
    let (bucket_name, express_bucket_name) = get_bucket_names();
    for bucket in [bucket_name.as_str(), express_bucket_name.as_str()] {
        let object_keys: Vec<String> = (0..num_uploads)
            .map(|i| generate_key(i.to_string().as_str()))
            .collect();

        let mut handles = vec![];
        for i in 0..num_uploads {
            let stream = TestStream::new_delay(file_size);

            let upload = tm
                .upload()
                .bucket(bucket)
                .key(object_keys[i].as_str())
                .body(InputStream::from_part_stream(stream));

            handles.push(upload.initiate().unwrap());
        }
        for handle in handles {
            handle.join().await.unwrap();
        }
    }
}
