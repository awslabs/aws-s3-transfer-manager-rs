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
            .initiate()
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
            .initiate()
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
                .initiate()
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
                .initiate()
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
        // as InputInvalid (a bad source root is invalid input, not an I/O failure).
        {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(&symlink_path)
                .initiate()
                .unwrap();

            let err = handle.join().await.unwrap_err();
            assert_eq!(&ErrorKind::InputInvalid, err.kind());
        }

        // should succeed when the source is a symbolic link to a directory and the operation follows symbolic links
        {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(symlink_path)
                .walker(FsWalker::builder().follow_symlinks(true).build())
                .initiate()
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
            .initiate()
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
            .initiate()
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
            .initiate()
            .unwrap();

        let err = handle.join().await.unwrap_err();
        assert_eq!(&ErrorKind::InputInvalid, err.kind());
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
            .initiate()
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
            .initiate()
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
            .initiate()
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
            .initiate()
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

/// Metrics correctness on a successful multi-object upload.
///
/// After a fully successful upload of files with known sizes:
/// - `objects_uploaded` matches the number of source files
/// - `network_tx` equals the sum of source file sizes
/// - `disk_read` equals the sum of source file sizes
/// - `started_at` is populated and precedes `finished_at`
#[tokio::test]
async fn test_metrics_correctness_on_success() {
    timeout(TEST_TIMEOUT, async {
        let files = vec![
            ("a.bin", 7),
            ("b.bin", 13),
            ("nested/c.bin", 19),
            ("nested/deep/d.bin", 23),
        ];
        let total_bytes: u64 = files.iter().map(|(_, s)| *s as u64).sum();
        let test_dir = create_test_dir(Some("test"), files.clone(), &[]);

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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(4, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(
            total_bytes, output.metrics.network_tx,
            "network_tx should equal sum of source file sizes"
        );
        assert_eq!(
            total_bytes, output.metrics.disk_read,
            "disk_read should equal sum of source file sizes"
        );
        let finished = output
            .metrics
            .finished_at
            .expect("finished_at must be set on successful transfer");
        assert!(
            output.metrics.started_at <= finished,
            "started_at must not be after finished_at"
        );
    })
    .await
    .expect("test_metrics_correctness_on_success timed out");
}

/// Serial execution correctness with `max_concurrent_uploads(1)`.
///
/// Forces the state machine through the serial-execution regime: only one
/// child spawned at a time, reaped before the next is spawned. All files
/// must still upload and metrics must still aggregate correctly.
#[tokio::test]
async fn test_max_concurrent_uploads_one_serial_execution() {
    timeout(TEST_TIMEOUT, async {
        let files: Vec<(String, usize)> = (0..10).map(|i| (format!("f{i}.bin"), 3)).collect();
        let files_ref: Vec<(&str, usize)> = files.iter().map(|(p, s)| (p.as_str(), *s)).collect();
        let test_dir = create_test_dir(Some("test"), files_ref, &[]);

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
            .max_concurrent_uploads(1)
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(10, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(30, output.metrics.network_tx);
        assert_eq!(30, output.metrics.disk_read);
    })
    .await
    .expect("test_max_concurrent_uploads_one_serial_execution timed out");
}

/// Deep tree exercises subtree claiming beyond `MAX_PARALLEL_WALKS`.
///
/// Builds a tree wider than the hardcoded 16-walker cap so the state
/// machine's `claim_subtrees` path is exercised: one walker splits off
/// sub-walkers up to the cap. Every file must still be uploaded exactly
/// once, with no lost entries.
#[tokio::test]
async fn test_deep_tree_subtree_claiming() {
    timeout(TEST_TIMEOUT, async {
        // 20 sibling subdirectories under the root, each with one file.
        // Combined with the root, that's 20 directories to walk, exceeding
        // the 16-walker cap so subtree claiming is forced to queue.
        let files: Vec<(String, usize)> = (0..20)
            .map(|i| (format!("sub_{i:02}/file.bin"), 5))
            .collect();
        let files_ref: Vec<(&str, usize)> = files.iter().map(|(p, s)| (p.as_str(), *s)).collect();
        let test_dir = create_test_dir(Some("test"), files_ref, &[]);

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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(
            20,
            output.objects_uploaded(),
            "all 20 files in sibling subdirs must upload"
        );
        assert!(output.failed_transfers().is_empty());
        assert_eq!(100, output.metrics.network_tx);
    })
    .await
    .expect("test_deep_tree_subtree_claiming timed out");
}

/// Custom walker filter limits the set of uploaded files.
///
/// Exercises the walker-first API surface: a filter closure is attached to
/// the `FsWalker`, and only files the filter accepts should be uploaded.
#[tokio::test]
async fn test_walker_filter_restricts_uploads() {
    timeout(TEST_TIMEOUT, async {
        let files = vec![
            ("keep.txt", 4),
            ("skip.log", 4),
            ("nested/keep.txt", 4),
            ("nested/skip.log", 4),
        ];
        let test_dir = create_test_dir(Some("test"), files.clone(), &[]);

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(
                FsWalker::builder()
                    .recursive(true)
                    .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "txt"))
                    .build(),
            )
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(
            2,
            output.objects_uploaded(),
            "only .txt files should upload"
        );
        assert!(output.failed_transfers().is_empty());
        assert_eq!(8, output.metrics.network_tx);
    })
    .await
    .expect("test_walker_filter_restricts_uploads timed out");
}

/// Empty source directory terminates cleanly with zero metrics.
///
/// Regression target for the `reaping_in_flight` race: when the walker
/// yields no entries and no children are ever spawned, `check_terminal`
/// must still fire and the handle must settle with an empty output.
#[tokio::test]
async fn test_empty_source_directory() {
    timeout(TEST_TIMEOUT, async {
        let test_dir = create_test_dir(Some("test"), vec![], &[]);

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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(0, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(0, output.metrics.network_tx);
        assert_eq!(0, output.metrics.disk_read);
        assert!(
            output.metrics.finished_at.is_some(),
            "finished_at must be set even on zero-work transfer"
        );
    })
    .await
    .expect("test_empty_source_directory timed out");
}

/// Interleaved per-child success and failure under `Continue` policy.
///
/// One specific key always returns 500 (persistent failure); the others
/// succeed. The transfer must complete (not abort), and the output must
/// record:
/// - successful uploads with `objects_uploaded`
/// - the failed upload with a populated `source_path`
/// - metrics reflecting only the on-wire bytes actually sent
#[tokio::test]
async fn test_interleaved_success_failure_continue() {
    timeout(TEST_TIMEOUT, async {
        let files = vec![
            ("a.bin", 4),
            ("b.bin", 4),
            ("doomed.bin", 4),
            ("c.bin", 4),
            ("d.bin", 4),
        ];
        let test_dir = create_test_dir(Some("test"), files.clone(), &[]);

        let bucket_name = "test-bucket";
        let fail_match = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| {
                input.bucket() == Some(bucket_name)
                    && input.key().is_some_and(|k| k.ends_with("doomed.bin"))
            })
            .then_http_response(|| {
                HttpResponse::new(StatusCode::try_from(500).unwrap(), SdkBody::empty())
            });
        let succeed_match = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| input.bucket() == Some(bucket_name))
            .then_output(|| PutObjectOutput::builder().build());

        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[fail_match, succeed_match]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3_client)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(4, output.objects_uploaded());
        assert_eq!(1, output.failed_transfers().len());
        let failed = &output.failed_transfers()[0];
        assert!(
            failed.source_path().is_some(),
            "failed upload must carry source_path"
        );
        assert!(
            failed
                .source_path()
                .unwrap()
                .to_string_lossy()
                .ends_with("doomed.bin"),
            "failed source_path should point at the failing file"
        );
    })
    .await
    .expect("test_interleaved_success_failure_continue timed out");
}

/// Dropping the handle while the walk is still in progress must not
/// panic and must tear down cleanly.
///
/// Creates a larger tree than the previous drop test and drops the handle
/// without any synchronization, so the drop can race with walker dispatch
/// and child spawning.
#[tokio::test]
async fn test_drop_during_walk_in_progress() {
    timeout(TEST_TIMEOUT, async {
        let files: Vec<(String, usize)> =
            (0..50).map(|i| (format!("dir_{i:02}/f.bin"), 2)).collect();
        let files_ref: Vec<(&str, usize)> = files.iter().map(|(p, s)| (p.as_str(), *s)).collect();
        let test_dir = create_test_dir(Some("test"), files_ref, &[]);

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
            .initiate()
            .unwrap();

        // Drop immediately — walker is almost certainly still producing
        // entries, and some children may be mid-flight.
        drop(handle);

        // Give tasks a moment to settle to ensure Drop did not deadlock.
        tokio::time::sleep(Duration::from_millis(50)).await;
    })
    .await
    .expect("test_drop_during_walk_in_progress timed out");
}

/// Multi-file multipart uploads aggregate `network_tx` and `disk_read`
/// correctly at the parent level.
///
/// Uses the multipart path exclusively (which instruments `disk_read` via
/// `part_reader`) to verify the parent aggregates both counters across
/// multiple multipart children.
#[tokio::test]
async fn test_multipart_metrics_aggregate_across_children() {
    timeout(TEST_TIMEOUT, async {
        const MIN_MULTIPART: u64 = 5 * ByteUnit::Mebibyte.as_bytes_u64();
        let files = vec![
            ("a.bin", MIN_MULTIPART as usize),
            ("b.bin", MIN_MULTIPART as usize),
            ("nested/c.bin", MIN_MULTIPART as usize),
        ];
        let expected_total: u64 = MIN_MULTIPART * 3;
        let test_dir = create_test_dir(Some("test"), files.clone(), &[]);

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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(3, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(
            expected_total, output.metrics.network_tx,
            "network_tx should sum across all multipart children"
        );
        assert_eq!(
            expected_total, output.metrics.disk_read,
            "disk_read should sum across all multipart children (part_reader instruments it)"
        );
    })
    .await
    .expect("test_multipart_metrics_aggregate_across_children timed out");
}

/// Handle `status()` returns a terminal variant after join.
#[tokio::test]
async fn test_status_transitions_to_terminal() {
    timeout(TEST_TIMEOUT, async {
        let test_dir = create_test_dir(Some("test"), vec![("a.bin", 1)], &[]);

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
            .initiate()
            .unwrap();

        // Status at this point may be Running or (for a fast completion on
        // 1-byte files) already terminal; don't assert on pre-join state.
        let output = handle.join().await.unwrap();
        assert_eq!(1, output.objects_uploaded());
        // Output being Ok implies the transfer reached a non-error terminal
        // state. No separate status accessor after join since the handle is
        // consumed.
    })
    .await
    .expect("test_status_transitions_to_terminal timed out");
}

/// Hidden files (dotfiles) are uploaded by default.
///
/// The default walker does not filter dotfiles. A custom filter is the
/// opt-in path for users who want to exclude them.
#[tokio::test]
async fn test_hidden_files_uploaded_by_default() {
    timeout(TEST_TIMEOUT, async {
        let files = vec![
            (".hidden.txt", 3),
            ("visible.txt", 3),
            (".dotdir/inner.txt", 3),
        ];
        let test_dir = create_test_dir(Some("test"), files.clone(), &[]);

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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(
            3,
            output.objects_uploaded(),
            "all files including dotfiles and files in dotdirs should upload"
        );
        assert!(output.failed_transfers().is_empty());
    })
    .await
    .expect("test_hidden_files_uploaded_by_default timed out");
}

/// Multiple `upload_objects` calls on the same `Client` must each
/// complete independently. Verifies no state leaks between iterations
/// (thread-locals, ready_set descriptors, counter residue).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_iterations_on_same_client_do_not_hang() {
    timeout(Duration::from_secs(60), async {
        const FILE_COUNT: usize = 2000;
        const ITERATIONS: usize = 3;
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("f/{i:04}.bin"), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        for iter in 0..ITERATIONS {
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(test_dir.path())
                .walker(FsWalker::builder().recursive(true).build())
                .key_prefix(format!("iter{iter}/"))
                .initiate()
                .unwrap();
            let output = handle.join().await.unwrap();
            assert_eq!(
                FILE_COUNT as u64,
                output.objects_uploaded(),
                "iteration {iter}: all files should upload"
            );
            assert!(
                output.failed_transfers().is_empty(),
                "iteration {iter}: no failures expected"
            );
        }
    })
    .await
    .expect("test_multiple_iterations_on_same_client_do_not_hang timed out");
}

/// Regression test for a hang where a re-entrant scheduler path left work
/// items in the submission queue undispatched.
///
/// The scenario: parent `poll_work` inside `spawn_children` called
/// `Upload::orchestrate_child`, which called `scheduler.enqueue_transfer`,
/// which called `scheduler.generate_work` — all while holding the parent's
/// state lock. Worker threads firing child-completion wakes would then
/// block on the parent state lock, each holding a pending count on the
/// shared submission queue. The queue's pending counter never dropped to
/// zero so flushes were skipped. The fix spans three layers (see
/// `GenerateWorkGuard` in scheduler, `SUBMISSION_DEPTH` in submission, and
/// the claim/orchestrate/merge phases in `poll_work`). This test asserts
/// that a burst of concurrent child completions completes through all of
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_child_completion_terminates_cleanly() {
    timeout(Duration::from_secs(30), async {
        // 300 small files, all succeeding. 300 is comfortably above the
        // default pipeline depth (100) so spawn_children will iterate
        // multiple times and the scheduler will process many child
        // completions concurrently.
        const FILE_COUNT: usize = 300;
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("f/{i:04}.bin"), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
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
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(FILE_COUNT as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
    })
    .await
    .expect("test_concurrent_child_completion_terminates_cleanly timed out");
}

/// Same shape as `test_concurrent_child_completion_terminates_cleanly`
/// but under `FailedTransferPolicy::Continue` with every child failing.
/// Exercises the failure path through the 3-phase spawn restructure and
/// verifies `children_reserved` is released correctly on the error side
/// of `merge_spawned`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_continue_policy_records_all_failures_under_concurrent_orchestration() {
    timeout(Duration::from_secs(30), async {
        const FILE_COUNT: usize = 200;
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("f/{i:04}.bin"), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";

        // Every PutObject returns a 503 server error. Under Continue
        // policy these accumulate in `failed_transfers`.
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| input.bucket() == Some(bucket_name))
            .then_http_response(|| {
                HttpResponse::new(StatusCode::try_from(503).unwrap(), SdkBody::from(""))
            });

        let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(0, output.objects_uploaded());
        assert_eq!(FILE_COUNT, output.failed_transfers().len());
    })
    .await
    .expect("test_continue_policy_records_all_failures_under_concurrent_orchestration timed out");
}

/// Verify `max_concurrent_uploads` is respected across the claim/orchestrate/merge
/// split even when many children complete concurrently. Counts the maximum
/// in-flight PutObjects observed through the mock and asserts it never
/// exceeds `max_concurrent_uploads`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_max_concurrent_uploads_respected_during_concurrent_orchestration() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    timeout(Duration::from_secs(30), async {
        const FILE_COUNT: usize = 100;
        const MAX_CONCURRENT: usize = 8;
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("{i:04}.bin"), 16))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));
        let in_flight_cb = Arc::clone(&in_flight);
        let max_observed_cb = Arc::clone(&max_observed);

        let bucket_name = "test-bucket";
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |input| input.bucket() == Some(bucket_name))
            .then_output(move || {
                let current = in_flight_cb.fetch_add(1, Ordering::AcqRel) + 1;
                max_observed_cb.fetch_max(current, Ordering::AcqRel);
                // Yielding is not available inside a blocking `then_output`
                // closure, so fake concurrency by sleeping briefly to widen
                // the window where many children can be in-flight at once.
                std::thread::sleep(Duration::from_millis(2));
                in_flight_cb.fetch_sub(1, Ordering::AcqRel);
                PutObjectOutput::builder().build()
            });

        let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]);
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3)
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .max_concurrent_uploads(MAX_CONCURRENT)
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(FILE_COUNT as u64, output.objects_uploaded());
        let observed = max_observed.load(Ordering::Acquire);
        assert!(
            observed <= MAX_CONCURRENT,
            "max concurrent PutObjects ({observed}) exceeded max_concurrent_uploads ({MAX_CONCURRENT}); \
             children_reserved must prevent over-spawning during lock-released orchestration"
        );
    })
    .await
    .expect("test_max_concurrent_uploads_respected_during_concurrent_orchestration timed out");
}

/// Abort policy with mid-burst latency must terminate cleanly without
/// leaking the counter protocol. The mock succeeds for the first
/// `SUCCESS_COUNT` calls then 503's; with synchronous per-call latency
/// of `LATENCY`, by the time the first 503 lands the parent already has
/// many successful children in `state.children` (and possibly more
/// reservations in flight). Hitting `abort()` from
/// `execute_join_children` while the protocol is non-trivially
/// populated exercises the lock-released orchestration window plus the
/// merge-spawned reservation-release path.
///
/// Asserts: `join()` returns `Err`, the surfaced error string contains
/// the abort cause, and the test completes within its timeout (no hang
/// on stale `children_reserved` or `reaping_in_flight`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_abort_policy_terminates_cleanly_during_concurrent_orchestration() {
    timeout(Duration::from_secs(30), async {
        const FILE_COUNT: usize = 100;
        const SUCCESS_COUNT: usize = 30;
        const LATENCY: Duration = Duration::from_millis(15);

        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("f/{i:04}.bin"), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object_succeeds_then_fails(
                bucket_name.to_owned(),
                SUCCESS_COUNT,
                LATENCY,
            ))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .failure_policy(FailedTransferPolicy::Abort)
            .initiate()
            .unwrap();

        let result = handle.join().await;
        let err = result.expect_err("Abort policy must surface an error");
        assert!(
            matches!(
                err.kind(),
                aws_sdk_s3_transfer_manager::error::ErrorKind::ChildOperationFailed
            ),
            "expected ChildOperationFailed, got: {:?}",
            err.kind(),
        );
        let source = std::error::Error::source(&err)
            .map(|s| s.to_string())
            .unwrap_or_default();
        assert!(
            source.contains("upload_objects aborted"),
            "expected abort cause in error source, got: {source:?}",
        );
    })
    .await
    .expect("test_abort_policy_terminates_cleanly_during_concurrent_orchestration timed out");
}

/// Dropping the handle while child uploads are mid-flight must cascade
/// cancellation through the scheduler without deadlocking on the state
/// lock and without hanging on counters that the orphaned in-flight
/// work would otherwise have decremented. With per-call latency of
/// `LATENCY`, the burst saturates the pipeline and the drop fires
/// while children occupy `state.children`, `children_reserved`, and
/// (statistically) `reaping_in_flight` simultaneously.
///
/// Asserts: drop returns within 50 ms (no deadlock), and within a 5 s
/// settle window every child `UploadHandle::status()` reaches a
/// terminal state (cascade reached the children).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_drop_during_active_uploads_cancels_without_hang() {
    timeout(Duration::from_secs(30), async {
        const FILE_COUNT: usize = 200;
        const LATENCY: Duration = Duration::from_millis(20);

        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("f/{i:04}.bin"), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object_with_latency(
                bucket_name.to_owned(),
                LATENCY,
            ))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .initiate()
            .unwrap();

        // Let several children dispatch and complete so the protocol
        // is non-trivially populated when the drop fires. 50 ms covers
        // ~2 LATENCY rounds, enough to seed reservations + reaping.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let drop_start = std::time::Instant::now();
        drop(handle);
        let drop_elapsed = drop_start.elapsed();
        assert!(
            drop_elapsed < Duration::from_millis(50),
            "drop took {drop_elapsed:?}; expected < 50ms, indicates a deadlock"
        );

        // Settle window for the cascade to complete. If the cascade
        // is broken the timeout above (30 s) will fire instead.
        tokio::time::sleep(Duration::from_secs(2)).await;
    })
    .await
    .expect("test_drop_during_active_uploads_cancels_without_hang timed out");
}

/// Multi-iteration upload_objects stress test exercising the state machine's
/// re-entrancy guards.
///
/// Runs three iterations of a 10 000-file upload back-to-back on a single
/// client. Each iteration uses a fresh key prefix so nothing is shared
/// between runs except the scheduler, its thread-local re-entry guards
/// (`IN_GENERATE_WORK`, `SUBMISSION_DEPTH`), and the submission queue.
///
/// The purpose is to catch re-entrancy defects that only manifest when
/// scheduler state persists across transfer boundaries — for example a
/// guard that is cleared on the happy path but not on an early return,
/// or a thread-local that accumulates across iterations. Instant-response
/// mocks keep the test isolated to the TM layer (no network, no hyper, no
/// HTTP) so any stall is attributable to scheduler or state-machine
/// behaviour.
///
/// Run with: `cargo test ... --release -- --ignored --nocapture`.
#[ignore = "stress repro, run with --ignored"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_multi_iter_reproduce_ec2_hang() {
    timeout(Duration::from_secs(240), async {
        const FILE_COUNT: usize = 10_000;
        const ITERATIONS: usize = 3;
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("d{:03}/{i:05}.bin", i / 100), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object(bucket_name.to_owned()))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        for iter in 0..ITERATIONS {
            let start = std::time::Instant::now();
            let handle = sut
                .upload_objects()
                .bucket(bucket_name)
                .source(test_dir.path())
                .walker(FsWalker::builder().recursive(true).build())
                .key_prefix(format!("iter{iter}/"))
                .initiate()
                .unwrap();
            let output = handle.join().await.unwrap();
            let elapsed = start.elapsed();
            eprintln!(
                "iter {iter} done in {elapsed:?}: uploaded={} failed={}",
                output.objects_uploaded(),
                output.failed_transfers().len()
            );
            assert_eq!(
                FILE_COUNT as u64,
                output.objects_uploaded(),
                "iteration {iter}: all files should upload"
            );
            assert!(
                output.failed_transfers().is_empty(),
                "iteration {iter}: no failures expected"
            );
        }
    })
    .await
    .expect("test_stress_multi_iter_reproduce_ec2_hang timed out");
}

/// Mock S3 `PutObject` that introduces a small, evenly-distributed delay
/// before returning success.
///
/// Mock `PutObject` that responds successfully after a real-time delay
/// of `latency`.
///
/// Uses `std::thread::sleep` rather than `tokio::time::sleep` because
/// `then_compute_output` takes a sync closure. In our test setup the
/// SDK pipeline executes on the transfer manager's managed thread pool
/// (sized to CPU count), not the tokio multi-thread runtime that drives
/// the test. Blocking a managed thread for the duration of a "request"
/// approximates the real SDK path well enough to widen the concurrent
/// in-flight window without starving the test's tokio runtime.
///
/// # Runtime constraint
///
/// Only valid when the transfer manager uses `ManagedThreadRuntime`
/// (the default). Under `TokioMultiThreadRuntime` the SDK pipeline
/// runs on tokio worker threads; blocking them with `thread::sleep`
/// starves the runtime and deadlocks the test.
fn mock_s3_client_for_put_object_with_latency(bucket_name: String, latency: Duration) -> Client {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(&bucket_name))
        .then_compute_output(move |_input| {
            std::thread::sleep(latency);
            PutObjectOutput::builder().build()
        });

    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object])
}

/// Mock `PutObject` that succeeds for the first `success_count` calls,
/// then returns HTTP 503 for every subsequent call. Each call sleeps
/// for `latency` before responding.
///
/// Lets a test pin the timing of a state transition: the `success_count + 1`
/// th call is guaranteed to be the first failure observed, so the parent
/// state machine reaches a known mix of in-flight and pre-failure children.
/// Used to drive the abort path with deterministic mid-burst timing
/// rather than racing instant-response mocks.
///
/// Like `mock_s3_client_for_put_object_with_latency`, the synchronous
/// sleep blocks a managed thread (where the SDK pipeline runs), not the
/// tokio runtime driving the test.
///
/// # Runtime constraint
///
/// Only valid when the transfer manager uses `ManagedThreadRuntime`
/// (the default). See `mock_s3_client_for_put_object_with_latency`.
fn mock_s3_client_for_put_object_succeeds_then_fails(
    bucket_name: String,
    success_count: usize,
    latency: Duration,
) -> Client {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let counter = Arc::new(AtomicUsize::new(0));
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |input| input.bucket() == Some(&bucket_name))
        .then_http_response(move || {
            std::thread::sleep(latency);
            let n = counter.fetch_add(1, Ordering::AcqRel);
            if n < success_count {
                let mut resp =
                    HttpResponse::new(StatusCode::try_from(200).unwrap(), SdkBody::from(""));
                resp.headers_mut()
                    .insert("etag", "\"d41d8cd98f00b204e9800998ecf8427e\"");
                resp
            } else {
                HttpResponse::new(StatusCode::try_from(503).unwrap(), SdkBody::from(""))
            }
        });

    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object])
}

/// Stress test for single-poll exclusivity on a composite transfer's
/// descriptor under concurrent child completions.
///
/// **What this verifies.** The scheduler guarantees that at most one
/// thread is inside `poll_work(desc)` for a given descriptor at a time.
/// This test exercises the code paths that uphold that invariant under a
/// realistic pressure pattern for composite transfers (`upload_objects`
/// in particular): many child uploads completing within a narrow time
/// window each fire `on_completion` on their dispatching managed thread,
/// which calls `scheduler.generate_work()` synchronously on that thread.
/// Without the single-poll guarantee, several of those threads can enter
/// the composite's `poll_work` concurrently, contend on its state mutex,
/// and — since they are running synchronous scheduler code on managed
/// thread tokio runtimes — starve those runtimes of the async task
/// polling they need to drive the remaining in-flight work. The system
/// then has no path back to forward progress.
///
/// **Test shape.** 10 000 small files (32 bytes each, enough to exercise
/// one PutObject per file without making the test dataset expensive) are
/// uploaded through a mock S3 client that returns successfully after a
/// ~10 ms synchronous sleep. The sleep is what allows dispatches to
/// saturate target concurrency (~128) with in-flight work; the first
/// wave of responses then completes in a tight burst that stresses the
/// contention window. The assertion is simply that the upload completes
/// within the test timeout — failure to uphold the invariant manifests
/// as an indefinite stall.
///
/// **Why `#[ignore]`.** ~2 min happy-path runtime keeps it out of
/// routine CI. Run explicitly as part of scheduler-regression
/// validation:
///
/// ```text
/// cargo test -p aws-sdk-s3-transfer-manager --release --test upload_objects_test \
///     -- --ignored --nocapture test_stress_parent_lock_contention
/// ```
#[ignore = "stress repro (~2 min), run with --ignored"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_parent_lock_contention() {
    timeout(Duration::from_secs(180), async {
        const FILE_COUNT: usize = 10_000;
        const PUT_LATENCY: Duration = Duration::from_millis(10);

        // 32 bytes per file: dataset cost is trivial while every file
        // still produces one PutObject. The contention pattern we care
        // about scales with completion count, not per-object size.
        let files: Vec<(String, usize)> = (0..FILE_COUNT)
            .map(|i| (format!("d{:03}/{i:05}.bin", i / 100), 32))
            .collect();
        let test_dir = create_test_dir(
            Some("test"),
            files.iter().map(|(k, s)| (k.as_str(), *s)).collect(),
            &[],
        );

        let bucket_name = "test-bucket";
        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(mock_s3_client_for_put_object_with_latency(
                bucket_name.to_owned(),
                PUT_LATENCY,
            ))
            .build();
        let sut = aws_sdk_s3_transfer_manager::Client::new(config);

        let start = std::time::Instant::now();
        let handle = sut
            .upload_objects()
            .bucket(bucket_name)
            .source(test_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .initiate()
            .unwrap();
        let output = handle.join().await.unwrap();
        let elapsed = start.elapsed();
        eprintln!(
            "upload done in {elapsed:?}: uploaded={} failed={}",
            output.objects_uploaded(),
            output.failed_transfers().len()
        );
        assert_eq!(
            FILE_COUNT as u64,
            output.objects_uploaded(),
            "all files should upload — a stall here indicates the scheduler is not draining in-flight work, which usually means single-poll exclusivity has broken",
        );
        assert!(
            output.failed_transfers().is_empty(),
            "no failures expected under happy-path mock",
        );
    })
    .await
    .expect("test_stress_parent_lock_contention timed out — single-poll exclusivity on the composite descriptor is likely broken");
}
