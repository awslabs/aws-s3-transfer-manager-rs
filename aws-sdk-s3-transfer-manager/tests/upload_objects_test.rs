/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(target_family = "unix")]

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
use aws_sdk_s3_transfer_manager::{
    error::ErrorKind,
    io::walk::FsWalker,
    metrics::unit::ByteUnit,
    types::{FailedTransferPolicy, PartSize},
};
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use std::time::Duration;
use test_common::create_test_dir;
use tokio::{fs::symlink, sync::watch, time::timeout};

/// Default test timeout. Any test body that exceeds this likely reveals a
/// hang in the TM rather than a real slow operation.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    timeout(TEST_TIMEOUT, async {
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
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(5, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(5, output.metrics.network_tx);
    })
    .await
    .expect("test_successful_multiple_objects_upload_via_put_object timed out");
}

#[tokio::test]
async fn test_successful_multiple_objects_upload_via_multipart_upload() {
    timeout(TEST_TIMEOUT, async {
        let recursion_root = "test";
        // should be in sync with `aws_sdk_s3_transfer_manager::config::MIN_MULTIPART_PART_SIZE_BYTES`
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
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_multipart_upload(bucket_name.to_owned()))
            .multipart_threshold(PartSize::Target(5))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(2, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(2 * MIN_MULTIPART_PART_SIZE_BYTES, output.metrics.network_tx);
    })
    .await
    .expect("test_successful_multiple_objects_upload_via_multipart_upload timed out");
}

#[tokio::test]
async fn test_successful_multiple_objects_upload_with_symlinks() {
    timeout(TEST_TIMEOUT, async {
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
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        // Test with following symbolic links while uploading recursively
        {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(temp_dir1.path())
                .walker(
                    FsWalker::builder()
                        .recursive(true)
                        .follow_symlinks(true)
                        .build(),
                )
                .send()
                .await
                .unwrap();

            let output = handle.join().await.unwrap();
            assert_eq!(7, output.objects_uploaded());
            assert!(output.failed_transfers().is_empty());
            assert_eq!(7, output.metrics.network_tx);
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
            assert_eq!(1, output.metrics.network_tx);
        }
    })
    .await
    .expect("test_successful_multiple_objects_upload_with_symlinks timed out");
}

#[tokio::test]
async fn test_source_dir_is_symlink() {
    timeout(TEST_TIMEOUT, async {
        let temp_dir1 = create_test_dir(Some("temp1"), vec![], &[]);

        let temp_dir2 = create_test_dir(Some("temp2"), vec![("sample.txt", 1)], &[]);

        // Create a symbolic link from `temp1/symlink` to `temp2`
        let symlink_path = temp_dir1.path().join("symlink");
        symlink(&temp_dir2, &symlink_path).await.unwrap();

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        // When the source is a symbolic link to a directory and follow_symlinks is false,
        // the walker rejects the root on first iteration; the error surfaces from join()
        // as IOError (not InputInvalid as in the legacy eager-validation path).
        {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(&symlink_path)
                .send()
                .await
                .unwrap();

            let err = handle.join().await.unwrap_err();
            assert_eq!(&ErrorKind::IOError, err.kind());
        }

        // should succeed when the source is a symbolic link to a directory and the operation follows symbolic links
        {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(symlink_path)
                .walker(FsWalker::builder().follow_symlinks(true).build())
                .send()
                .await
                .unwrap();

            let output = handle.join().await.unwrap();
            assert_eq!(1, output.objects_uploaded());
            assert!(output.failed_transfers().is_empty());
            assert_eq!(1, output.metrics.network_tx);
        }
    })
    .await
    .expect("test_source_dir_is_symlink timed out");
}

#[tokio::test]
async fn test_failed_upload_policy_continue() {
    timeout(TEST_TIMEOUT, async {
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
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .failure_policy(FailedTransferPolicy::Continue)
            .send()
            .await
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(2, output.objects_uploaded());
        // One failure recorded for the inaccessible subdir (cannot enumerate its contents)
        assert_eq!(1, output.failed_transfers().len());
        assert!(output.failed_transfers()[0].input().is_none());
        assert_eq!(2, output.metrics.network_tx);
    })
    .await
    .expect("test_failed_upload_policy_continue timed out");
}

#[tokio::test]
async fn test_server_error_should_be_recorded_as_such_in_failed_transfers() {
    timeout(TEST_TIMEOUT, async {
        let test_dir = create_test_dir(Some("test"), vec![("sample.jpg", 1)], &[]);

        let bucket_name = "test-bucket";
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| input.bucket() == Some(bucket_name))
            .then_http_response(|| {
                HttpResponse::new(StatusCode::try_from(500).unwrap(), SdkBody::empty())
            });
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3_client)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

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
        // The new state machine records source_path for failed uploads; the UploadInput
        // cannot be captured because the InputStream body is non-clonable.
        assert!(output.failed_transfers()[0].source_path().is_some());
        assert_eq!(0, output.metrics.network_tx);
    })
    .await
    .expect("test_server_error_should_be_recorded_as_such_in_failed_transfers timed out");
}

/// Fail when source is not a directory.
#[tokio::test]
async fn test_source_dir_not_valid() {
    timeout(TEST_TIMEOUT, async {
        let source = tempfile::NamedTempFile::new().unwrap();

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        // The walker detects the non-directory root on first iteration; the error
        // surfaces from join() as IOError rather than the legacy InputInvalid.
        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(source.path())
            .send()
            .await
            .unwrap();

        let err = handle.join().await.unwrap_err();
        assert_eq!(&ErrorKind::IOError, err.kind());
    })
    .await
    .expect("test_source_dir_not_valid timed out");
}

#[tokio::test]
async fn test_error_when_custom_delimiter_appears_in_filename() {
    timeout(TEST_TIMEOUT, async {
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
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .delimiter("-")
            .send()
            .await
            .unwrap();

        // Under the default Abort policy, the first key derivation failure aborts
        // the transfer. The terminal error is ChildOperationFailed with the cause
        // text threaded through abort().
        let err = handle.join().await.unwrap_err();
        assert_eq!(&ErrorKind::ChildOperationFailed, err.kind());
        assert!(format!("{}", DisplayErrorContext(err)).contains("key derivation failure"));
    })
    .await
    .expect("test_error_when_custom_delimiter_appears_in_filename timed out");
}

#[tokio::test]
async fn test_abort_on_handle_should_terminate_tasks_gracefully() {
    timeout(TEST_TIMEOUT, async {
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

        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3_client)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .send()
            .await
            .unwrap();

        // Release the mock's spin wait so in-flight PutObjects can return.
        watch_tx.send(()).unwrap();

        // abort() should complete (cancel + wait_for_idle) without hanging.
        handle.abort().await;
    })
    .await
    .expect("test_abort_on_handle_should_terminate_tasks_gracefully timed out");
}

#[tokio::test]
async fn test_failed_child_operation_should_cause_ongoing_requests_to_be_cancelled() {
    timeout(TEST_TIMEOUT, async {
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

        // Fail every PutObject attempt so SDK retries exhaust and each child
        // upload's join() surfaces an error. Under the default Abort policy, the
        // first failure aborts the parent transfer, which cancels all ongoing
        // siblings. `handle.join()` returns the triggering error.
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| input.bucket() == Some(bucket_name))
            .then_http_response(|| {
                HttpResponse::new(StatusCode::try_from(500).unwrap(), SdkBody::empty())
            });

        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3_client)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .send()
            .await
            .unwrap();

        let err = handle.join().await.unwrap_err();
        assert_eq!(&ErrorKind::ChildOperationFailed, err.kind());
    })
    .await
    .expect("test_failed_child_operation_should_cause_ongoing_requests_to_be_cancelled timed out");
}

#[tokio::test]
async fn test_drop_upload_objects_handle() {
    timeout(TEST_TIMEOUT, async {
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
                    let _ = watch_tx.send(());
                    PutObjectOutput::builder().build()
                }
            });
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3_client)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .send()
            .await
            .unwrap();

        // Wait until at least one PutObject has been invoked so drop happens
        // against an in-flight transfer, not immediately after spawning.
        let rx = watch_rx.clone();
        while !rx.has_changed().unwrap() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Give spawned tasks a moment to progress.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Dropping must not panic and must cancel the transfer cleanly.
        drop(handle);
    })
    .await
    .expect("test_drop_upload_objects_handle timed out");
}
