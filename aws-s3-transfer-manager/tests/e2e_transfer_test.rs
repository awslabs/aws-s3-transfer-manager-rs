/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod test_utils;

use aws_s3_transfer_manager::io::InputStream;
use aws_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3::types::ChecksumMode;
use aws_s3_transfer_manager::metrics::unit::ByteUnit;
use test_utils::drain;
// #![cfg(e2e)]

use aws_s3_transfer_manager::types::PartSize;
use crate::test_utils::setup_tracing;

async fn test_tm() -> (aws_s3_transfer_manager::Client, aws_sdk_s3::Client) {
    let tm_config = aws_s3_transfer_manager::from_env()
        .part_size(PartSize::Target(8*ByteUnit::Mebibyte.as_bytes_u64()))
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
const PUT_OBJECT_PREFIX: &str = "upload/put-object-test/";

async fn perform_upload(
    tm: &aws_s3_transfer_manager::Client,
    key: &str,
    strategy: Option<ChecksumStrategy>,
    size: usize,
) {
    let mut upload = tm.upload()
        .bucket(BUCKET_NAME)
        .key(key)
        .body(create_input_stream(size));

    if let Some(strategy) = strategy {
        upload = upload.checksum_strategy(strategy);
    }

    upload
        .initiate()
        .unwrap()
        .join()
        .await
        .unwrap();
}

async fn test_round_trip_helper(file_size: usize, object_key: &str) {
    let (tm, _) = test_tm().await;
    perform_upload(&tm, object_key, None, file_size).await;
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

async fn get_checksum(s3_client: &aws_sdk_s3::Client, key: &str) -> String {
    s3_client
        .head_object()
        .bucket(BUCKET_NAME)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
        .checksum_crc32()
        .unwrap()
        .to_owned()
}
#[tokio::test]
async fn test_multi_part_file_checksum_upload() {
    let file_size = 20 * 1024 * 1024;
    let (tm, s3_client) = test_tm().await;

    // First upload: calculated CRC32
    let object_key = format!("{}20MB-crc32", PUT_OBJECT_PREFIX);
    perform_upload(&tm, &object_key, Some(ChecksumStrategy::with_calculated_crc32()), file_size).await;
    let checksum = get_checksum(&s3_client, &object_key).await;

    // Second upload: precomputed CRC32
    perform_upload(&tm, &object_key, Some(ChecksumStrategy::with_crc32(checksum)), file_size).await;

    // Third upload: composite CRC32
    let composite_key = format!("{}20MB-crc32-composite", PUT_OBJECT_PREFIX);
    perform_upload(&tm, &composite_key, Some(ChecksumStrategy::with_calculated_crc32_composite_if_multipart()), file_size).await;

    let composite_checksum = get_checksum(&s3_client, &composite_key).await;
    assert!(composite_checksum.ends_with("-3"));
}
