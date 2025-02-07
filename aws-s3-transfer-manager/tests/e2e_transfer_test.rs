/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod test_utils;

use aws_sdk_s3::types::ChecksumMode;
use tracing_subscriber::EnvFilter;
use aws_s3_transfer_manager::io::InputStream;
use aws_s3_transfer_manager::operation::upload::ChecksumStrategy;
use test_utils::drain;
// #![cfg(e2e)]

use aws_s3_transfer_manager::types::PartSize;

async fn test_tm() -> (aws_s3_transfer_manager::Client, aws_sdk_s3::Client) {
    let tm_config = aws_s3_transfer_manager::from_env()
        .part_size(PartSize::Target(8))
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

const BUCKET_NAME: &str = "aws-c-s3-test-bucket-099565";
const PUT_OBJECT_PREFIX: &str = "/upload/put-object-test/";

async fn test_round_trip_helper(file_size: usize, object_key: &str) {
    let (tm, _) = test_tm().await;

    let upload_handle = tm
        .upload()
        .bucket(BUCKET_NAME)
        .key(object_key)
        .body(create_input_stream(file_size))
        .initiate()
        .unwrap();

    upload_handle.join().await.unwrap();

    let mut download_handle = tm
        .download()
        .bucket(BUCKET_NAME)
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
    test_round_trip_helper(file_size, &object_key).await;
}

#[tokio::test]
async fn test_multi_part_file_round_trip() {
    let file_size = 20 * 1024 * 1024;
    let object_key = format!("{}{}", PUT_OBJECT_PREFIX, "20MB");
    test_round_trip_helper(file_size, &object_key).await;
}

fn setup_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}


#[tokio::test]
async fn test_multi_part_file_checksum_round_trip() {
    setup_tracing();
    let file_size = 20 * 1024 * 1024;
    let object_key = format!("{}{}", PUT_OBJECT_PREFIX, "20MB-crc32");
    let object_key_ref = object_key.as_str();
    let (tm, s3_client) = test_tm().await;

    // Transfer manager to calculated the crc32.
    let upload_handle = tm
        .upload()
        .bucket(BUCKET_NAME)
        .key(object_key_ref)
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(create_input_stream(file_size))
        .initiate()
        .unwrap();

    upload_handle.join().await.unwrap();

    let output = s3_client
        .head_object()
        .bucket(BUCKET_NAME)
        .key(object_key_ref)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    // Check we do get the crc64 nvme checksum
    let checksum = output.checksum_crc32().unwrap();

    // Do another upload with the checksum we got, and verify it upload successfully.
    // Transfer manager upload with pre-calculated checksum
    let upload_handle = tm
        .upload()
        .bucket(BUCKET_NAME)
        .key(object_key_ref)
        .checksum_strategy(ChecksumStrategy::with_crc32(checksum))
        .body(create_input_stream(file_size))
        .initiate()
        .unwrap();

    upload_handle.join().await.unwrap();
}
