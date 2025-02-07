/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// #![cfg(e2e)]

// Tests here requires AWS account with pre-configured S3 bucket to run the tests.
// Refer to https://github.com/awslabs/aws-c-s3/tree/main/tests/test_helper to help set up the S3 in the account
// Set CRT_S3_TEST_BUCKET_NAME environment variables to the bucket created.
// By default, it uses s3-tm-rs-test-bucket

mod test_utils;

use core::num;

use aws_s3_transfer_manager::io::InputStream;
use aws_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3::types::ChecksumMode;
use test_utils::drain;

use crate::test_utils::setup_tracing;
use aws_s3_transfer_manager::types::PartSize;

const PUT_OBJECT_PREFIX: &str = "upload/put-object-test/";

fn get_bucket_names() -> (String, String) {
    let bucket_name = option_env!("CRT_S3_TEST_BUCKET_NAME").unwrap_or("aws-c-s3-test-bucket-099565").to_owned();
    let express_bucket_name = format!("{}--usw2-az1--x-s3", bucket_name.as_str());
    (bucket_name, express_bucket_name)
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
    size: usize,
) {
    let mut upload = tm
        .upload()
        .bucket(bucket_name)
        .key(key)
        .body(create_input_stream(size));

    if let Some(strategy) = strategy {
        upload = upload.checksum_strategy(strategy);
    }

    upload.initiate().unwrap().join().await.unwrap();
}

async fn test_round_trip_helper(file_size: usize, bucket_name: &str, object_key: &str) {
    let (tm, _) = test_tm().await;
    perform_upload(&tm, bucket_name, object_key, None, file_size).await;
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
    let file_size = 1 * 1024 * 1024;
    let object_key = format!("{}{}", PUT_OBJECT_PREFIX, "1MB");
    let (bucket_name, express_bucket_name) = get_bucket_names();
    test_round_trip_helper(file_size, bucket_name.as_str(), &object_key).await;
    test_round_trip_helper(file_size, express_bucket_name.as_str(), &object_key).await;
}

#[tokio::test]
async fn test_multi_part_file_round_trip() {
    let file_size = 20 * 1024 * 1024;
    let object_key = format!("{}{}", PUT_OBJECT_PREFIX, "20MB");
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
    perform_upload(tm, bucket, key, Some(strategy), size).await;
    get_checksum(s3_client, bucket, key).await
}

async fn run_checksum_test(size_mb: usize, key_suffix: &str, is_multi_part: bool) {
    let file_size = size_mb * 1024 * 1024;
    let object_key = format!("{}{}", PUT_OBJECT_PREFIX, key_suffix);
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
