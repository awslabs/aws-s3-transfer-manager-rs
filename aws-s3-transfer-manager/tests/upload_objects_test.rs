/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(target_family = "unix")]

use aws_s3_transfer_manager::{
    error::ErrorKind,
    metrics::unit::ByteUnit,
    types::{FailedTransferPolicy, PartSize},
};
use aws_sdk_s3::{
    error::DisplayErrorContext,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput,
        create_multipart_upload::CreateMultipartUploadOutput, put_object::PutObjectOutput,
        upload_part::UploadPartOutput,
    },
    Client,
};
use aws_smithy_mocks_experimental::{mock, mock_client, RuleMode};
use test_common::create_test_dir;

// Create an S3 client with mock behavior configured for `PutObject`
fn mock_s3_client_for_put_object(bucket_name: String) -> Client {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(&bucket_name))
        .then_output(|| PutObjectOutput::builder().build());

    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object])
}

// Create an S3 client with mock behavior configured for `MultipartUpload`
//
// We intentionally avoid being specific about the expected input and output for mocks,
// as long as the execution of uploading multiple objects completes successfully.
// Setting expectations that are too precise can lead to brittle tests.
fn mock_s3_client_for_multipart_upload(bucket_name: String) -> Client {
    let upload_id = "test-upload-id".to_owned();

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    let upload_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests({
            let upload_id = upload_id.clone();
            move |input| {
                input.upload_id.as_ref() == Some(&upload_id) && input.bucket() == Some(&bucket_name)
            }
        })
        .then_output(|| UploadPartOutput::builder().build());

    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests({
            let upload_id = upload_id.clone();
            move |r| r.upload_id.as_ref() == Some(&upload_id)
        })
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    )
}

#[tokio::test]
async fn test_successful_multiple_objects_upload_via_put_object() {
    let recursion_root = "test";
    let files = vec![
        ("sample.jpg", 1),
        ("photos/2022/January/sample.jpg", 1),
        ("photos/2022/February/sample1.jpg", 1),
        ("photos/2022/February/sample2.jpg", 1),
        ("photos/2022/February/sample3.jpg", 1),
    ];
    let test_dir = create_test_dir(Some(&recursion_root), files.clone(), &[]);

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .recursive(true)
        .send()
        .await
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(5, output.objects_uploaded());
    assert!(output.failed_transfers().is_empty());
    assert_eq!(5, output.total_bytes_transferred());
}

#[tokio::test]
async fn test_successful_multiple_objects_upload_via_multipart_upload() {
    let recursion_root = "test";
    // should be in sync with `aws-s3-transfer-manager::config::MIN_MULTIPART_PART_SIZE_BYTES`
    const MIN_MULTIPART_PART_SIZE_BYTES: u64 = 5 * ByteUnit::Mebibyte.as_bytes_u64();
    let files = vec![
        ("sample.jpg", MIN_MULTIPART_PART_SIZE_BYTES as usize),
        (
            "photos/2022/January/sample.jpg",
            MIN_MULTIPART_PART_SIZE_BYTES as usize,
        ),
    ];
    let test_dir = create_test_dir(Some(&recursion_root), files.clone(), &[]);

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_multipart_upload(bucket_name.to_owned()))
        .multipart_threshold(PartSize::Target(5))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .recursive(true)
        .send()
        .await
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(2, output.objects_uploaded());
    assert!(output.failed_transfers().is_empty());
    assert_eq!(
        2 * MIN_MULTIPART_PART_SIZE_BYTES,
        output.total_bytes_transferred()
    );
}

#[tokio::test]
async fn test_failed_upload_policy_continue() {
    let recursion_root = "test";
    let files = vec![
        ("sample.jpg", 1),
        ("photos/2022/January/sample.jpg", 1),
        ("photos/2022/February/sample1.jpg", 1),
        ("photos/2022/February/sample2.jpg", 1),
        ("photos/2022/February/sample3.jpg", 1),
    ];
    // Make all files inaccessible under `photos/2022/February`
    let inaccessible_dir_relative_path = "photos/2022/February";
    let test_dir = create_test_dir(
        Some(&recursion_root),
        files.clone(),
        &[inaccessible_dir_relative_path],
    );

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .recursive(true)
        .failure_policy(FailedTransferPolicy::Continue)
        .send()
        .await
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(2, output.objects_uploaded());
    assert_eq!(1, output.failed_transfers().len()); // Cannot traverse inaccessible dir to count how many files are in it
    assert_eq!(2, output.total_bytes_transferred());
}

/// Fail when source is not a directory
#[tokio::test]
async fn test_source_dir_not_valid() {
    let source = tempfile::NamedTempFile::new().unwrap();

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let err = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(source.path())
        .send()
        .await
        .unwrap_err();

    assert_eq!(&ErrorKind::InputInvalid, err.kind());
    assert!(format!("{}", DisplayErrorContext(err)).contains("is not a directory"));
}

#[tokio::test]
async fn test_error_when_custom_delimiter_appears_in_filename() {
    let recursion_root = "test";
    let files = vec![
        ("sample.jpg", 1),
        ("photos/2022-January/sample.jpg", 1),
        ("photos/2022-February/sample1.jpg", 1),
        ("photos/2022-February/sample2.jpg", 1),
        ("photos/2022-February/sample3.jpg", 1),
    ];
    let test_dir = create_test_dir(Some(&recursion_root), files.clone(), &[]);

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    // Getting `handle` is ok, i.e., tasks should be spawned from `UploadObjects::orchestrate`
    let handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .recursive(true)
        .delimiter("-")
        .send()
        .await
        .unwrap();

    // But unwrapping it should fail
    let err = handle.join().await.unwrap_err();

    assert_eq!(&ErrorKind::InputInvalid, err.kind());
    assert!(format!("{}", DisplayErrorContext(err))
        .contains("a custom delimiter `-` should not appear"));
}
