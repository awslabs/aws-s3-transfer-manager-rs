/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(target_family = "unix")]

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use aws_s3_transfer_manager::{
    error::ErrorKind,
    metrics::unit::ByteUnit,
    types::{FailedTransferPolicy, PartSize},
};
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::DisplayErrorContext,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput,
        create_multipart_upload::CreateMultipartUploadOutput, put_object::PutObjectOutput,
        upload_part::UploadPartOutput,
    },
    Client,
};
use aws_smithy_mocks_experimental::{mock, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use test_common::{create_test_dir, mock_client_with_stubbed_http_client};
use tokio::{fs::symlink, sync::watch};

// Create an S3 client with mock behavior configured for `PutObject`
fn mock_s3_client_for_put_object(bucket_name: String) -> Client {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(&bucket_name))
        .then_output(|| PutObjectOutput::builder().build());

    mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object])
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

    mock_client_with_stubbed_http_client!(
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
    let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

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
    let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

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
async fn test_successful_multiple_objects_upload_with_symlinks() {
    let temp_dir1 = create_test_dir(Some("temp1"), vec![("sample.jpg", 1)], &[]);

    let temp_dir2 = create_test_dir(
        Some("temp2"),
        vec![
            ("sample.txt", 1),
            ("docs/2022/January/sample.txt", 1),
            ("docs/2022/February/sample1.txt", 1),
            ("docs/2022/February/sample2.txt", 1),
            ("docs/2022/February/sample3.txt", 1),
        ],
        &[],
    );

    let temp_dir3 = create_test_dir(Some("temp3"), vec![("sample3.png", 1)], &[]);

    // Crate a symbolic link from `temp1/symlink` to `temp2`
    symlink(&temp_dir2, temp_dir1.path().join("symlink"))
        .await
        .unwrap();
    // Crate a symbolic link from `temp1/symlink2` to `temp3/sample.png`
    symlink(
        temp_dir3.path().join("sample3.png"),
        temp_dir1.path().join("symlink2"),
    )
    .await
    .unwrap();

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    // Test with following symbolic links while uploading recursively
    {
        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(temp_dir1.path())
            .recursive(true)
            .follow_symlinks(true)
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(7, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(7, output.total_bytes_transferred());
    }

    // Test without following symbolic links while uploading recursively
    {
        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(temp_dir1.path())
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(1, output.objects_uploaded()); // should only include "temp1/sample.jpg"
        assert!(output.failed_transfers().is_empty());
        assert_eq!(1, output.total_bytes_transferred());
    }
}

#[tokio::test]
async fn test_source_dir_is_symlink() {
    let temp_dir1 = create_test_dir(Some("temp1"), vec![], &[]);

    let temp_dir2 = create_test_dir(Some("temp2"), vec![("sample.txt", 1)], &[]);

    // Crate a symbolic link from `temp1/symlink` to `temp2`
    let symlink_path = temp_dir1.path().join("symlink");
    symlink(&temp_dir2, &symlink_path).await.unwrap();

    let bucket_name = "test-bucket";
    let config = aws_s3_transfer_manager::Config::builder()
        .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    // should fail when the source is a symbolic link to a directory but the operation does not follow symbolic links
    {
        let err = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(&symlink_path)
            .send()
            .await
            .unwrap_err();

        assert_eq!(&ErrorKind::InputInvalid, err.kind());
        assert!(format!("{}", DisplayErrorContext(err)).contains("is a symbolic link to a directory but the current upload operation does not follow symbolic links"));
    }

    // should succeed when the source is a symbolic link to a directory and the operation follows symbolic links
    {
        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(symlink_path)
            .follow_symlinks(true)
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(1, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(1, output.total_bytes_transferred());
    }
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
        Some(recursion_root),
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
    assert!(output.failed_transfers()[0].input().is_none()); // No `UploadInput` in the case of client error
    assert_eq!(2, output.total_bytes_transferred());
}

#[tokio::test]
async fn test_server_error_should_be_recorded_as_such_in_failed_transfers() {
    let test_dir = create_test_dir(Some("test"), vec![("sample.jpg", 1)], &[]);

    let bucket_name = "test-bucket";
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(bucket_name))
        .then_http_response(|| {
            HttpResponse::new(StatusCode::try_from(500).unwrap(), SdkBody::empty())
        });
    let s3_client =
        mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
    let config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .failure_policy(FailedTransferPolicy::Continue)
        .send()
        .await
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(0, output.objects_uploaded());
    assert_eq!(1, output.failed_transfers().len());
    assert!(output.failed_transfers()[0]
        .input()
        .map(|input| input.bucket() == Some(bucket_name) && input.key() == Some("sample.jpg"))
        .unwrap_or_default());
    assert_eq!(0, output.total_bytes_transferred());
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
    let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

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

#[tokio::test]
async fn test_abort_on_handle_should_terminate_tasks_gracefully() {
    let (_guard, rx) = capture_test_logs();

    let recursion_root = "test";
    let files = vec![
        ("sample.jpg", 1),
        ("photos/2022-January/sample.jpg", 1),
        ("photos/2022-February/sample1.jpg", 1),
        ("photos/2022-February/sample2.jpg", 1),
        ("photos/2022-February/sample3.jpg", 1),
    ];
    let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

    let (watch_tx, watch_rx) = watch::channel(());

    let bucket_name = "test-bucket";
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(bucket_name))
        .then_output({
            let rx = watch_rx.clone();
            move || {
                while !rx.has_changed().unwrap() {}
                PutObjectOutput::builder().build()
            }
        });

    let s3_client =
        mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
    let config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .build();
    let sut = aws_s3_transfer_manager::Client::new(config);

    let mut handle = sut
        .upload_objects()
        .bucket(bucket_name)
        .source(test_dir.path())
        .recursive(true)
        .send()
        .await
        .unwrap();

    watch_tx.send(()).unwrap();

    handle.abort().await.unwrap();

    assert!(rx.contents().contains("received cancellation signal"));
}

#[tokio::test]
async fn test_failed_child_operation_should_cause_ongoing_requests_to_be_cancelled() {
    let (_guard, rx) = capture_test_logs();

    let recursion_root = "test";
    let files = vec![
        ("sample.jpg", 1),
        ("photos/2022-January/sample.jpg", 1),
        ("photos/2022-February/sample1.jpg", 1),
        ("photos/2022-February/sample2.jpg", 1),
        ("photos/2022-February/sample3.jpg", 1),
    ];
    let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

    let bucket_name = "test-bucket";

    let accessed = Arc::new(AtomicBool::new(false));
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(bucket_name))
        .then_http_response(move || {
            let already_accessed = accessed.swap(true, Ordering::SeqCst);
            if already_accessed {
                HttpResponse::new(StatusCode::try_from(200).unwrap(), SdkBody::empty())
            } else {
                // Force the first call to PubObject to fail, triggering operation cancellation for all subsequent PutObject calls.
                HttpResponse::new(StatusCode::try_from(500).unwrap(), SdkBody::empty())
            }
        });

    let s3_client =
        mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
    let config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
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

    // `join` will report the actual error that triggered cancellation.
    assert_eq!(
        &ErrorKind::ChildOperationFailed,
        handle.join().await.unwrap_err().kind()
    );
    // The execution should either receive at least one cancellation signal or successfully complete the current task.
    let logs = rx.contents();
    assert!(
        logs.contains("received cancellation signal")
            || logs.contains("ls channel closed, worker finished")
    );
}

#[tokio::test]
async fn test_drop_upload_objects_handle() {
    let test_dir = create_test_dir(
        Some("test"),
        vec![
            ("sample.jpg", 1),
            ("photos/2022-January/sample.jpg", 1),
            ("photos/2022-February/sample1.jpg", 1),
            ("photos/2022-February/sample2.jpg", 1),
            ("photos/2022-February/sample3.jpg", 1),
        ],
        &[],
    );

    let (watch_tx, watch_rx) = watch::channel(());

    let bucket_name = "test-bucket";
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(bucket_name))
        .then_output({
            move || {
                watch_tx.send(()).unwrap();
                PutObjectOutput::builder().build()
            }
        });
    let s3_client =
        mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
    let config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
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

    // Wait until execution reaches the point just before returning `PutObjectOutput`,
    // as dropping `handle` immediately after creation may not be interesting for testing.
    while !watch_rx.has_changed().unwrap() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Give some time so spawned tasks might be able to proceed with their tasks a bit.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // should not panic
    drop(handle)
}
