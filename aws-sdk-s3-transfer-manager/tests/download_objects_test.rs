/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
#![cfg(target_family = "unix")]

use aws_sdk_s3::{
    error::DisplayErrorContext,
    operation::{
        get_object::GetObjectOutput,
        list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output},
    },
    primitives::ByteStream,
};
use aws_sdk_s3_transfer_manager::{error::ErrorKind, types::FailedTransferPolicy};
use aws_smithy_mocks::{mock, mock_client, Rule, RuleMode};
use aws_smithy_runtime_api::{client::orchestrator::HttpResponse, http::StatusCode};
use bytes::Bytes;
use std::{io, iter, path::Path, sync::Arc};
use tokio::sync::watch;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct MockObject {
    object: aws_sdk_s3::types::Object,
    contents: Bytes,
    error_on_get: bool,
}

impl MockObject {
    /// Create a new mock object with the given key and contents
    fn new(key: impl Into<String>, data: impl Into<Bytes>) -> Self {
        let contents: Bytes = data.into();
        let object = aws_sdk_s3::types::Object::builder()
            .key(key.into())
            .size(contents.len() as i64)
            .build();
        Self {
            object,
            contents,
            error_on_get: false,
        }
    }

    /// Create a new mock object with the given key and content size using random data
    fn new_random(key: impl Into<String>, size: usize) -> Self {
        let data: String = iter::repeat_with(fastrand::alphanumeric)
            .take(size)
            .collect();
        Self::new(key, data)
    }

    /// Create a new mock object that fails when `get_object` is invoked on it
    fn new_with_error(key: impl Into<String>) -> Self {
        let mut obj = Self::new_random(key, 10);
        obj.error_on_get = true;
        obj
    }

    /// Get the output for the `get_object` invocation
    fn get_object_output(&self) -> GetObjectOutput {
        assert!(!self.error_on_get, "mock object expects error");
        GetObjectOutput::builder()
            .body(ByteStream::from(self.contents.clone()))
            .content_length(self.contents.len() as i64)
            .build()
    }
}

fn error_http_resp() -> HttpResponse {
    HttpResponse::new(StatusCode::try_from(403).unwrap(), Bytes::new().into())
}

/// Get the mock rule for this object when `get_object` API is invoked for the corresponding key
fn get_object_rule(mobj: &MockObject) -> Rule {
    let mock_obj = Arc::new(mobj.clone());

    if mobj.error_on_get {
        mock!(aws_sdk_s3::Client::get_object)
            .match_requests({
                let mock_obj = mock_obj.clone();
                move |r| r.key() == mock_obj.object.key()
            })
            .then_http_response(error_http_resp)
    } else {
        mock!(aws_sdk_s3::Client::get_object)
            .match_requests({
                let mock_obj = mock_obj.clone();
                move |r| r.key() == mock_obj.object.key()
            })
            .then_output(move || mock_obj.get_object_output())
    }
}

/// Mock bucket with the set of objects for testing transfer manager.
///
/// NOTE: This is limited to simple test scenarios where only a single `ListObjectsV2` is used
/// and all objects have content length less than the part size (such that all downloads
/// only require a single `GetObject` request).
#[derive(Debug)]
struct MockBucket {
    objects: Vec<MockObject>,
}

impl MockBucket {
    fn builder() -> MockBucketBuilder {
        MockBucketBuilder::default()
    }

    /// Configure the mock behavior listing `objects` stored in this `MockBucket`.
    fn list_objects_rule(&self) -> Rule {
        let contents = self.objects.iter().map(|m| m.object.clone()).collect();

        let list_output = ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();

        mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || list_output.clone())
    }

    /// Configure the mock behavior of `GetObject` for `objects` stored in this `MockBucket`.
    fn get_object_rules(&self) -> Vec<aws_smithy_mocks::Rule> {
        self.objects.iter().map(get_object_rule).collect()
    }

    /// Return the mock rules representing this bucket. This includes
    /// the `ListObjectsV2` call as well as all of the `GetObject` calls.
    fn rules(&self) -> Vec<aws_smithy_mocks::Rule> {
        let mut rules = self.get_object_rules();
        rules.push(self.list_objects_rule());
        rules
    }
}

#[derive(Debug, Default)]
struct MockBucketBuilder {
    objects: Vec<MockObject>,
}

impl MockBucketBuilder {
    /// Create a new key with the given content size using random data
    fn key_with_size(mut self, key: impl Into<String>, size: usize) -> Self {
        self.objects.push(MockObject::new_random(key, size));
        self
    }

    /// Create a new key that returns an error when `get_object` API operation is invoked
    fn key_with_error(mut self, key: impl Into<String>) -> Self {
        self.objects.push(MockObject::new_with_error(key));
        self
    }

    /// Consume the builder and build a `MockBucket`
    fn build(self) -> MockBucket {
        MockBucket {
            objects: self.objects,
        }
    }
}

/// Walk the directory rooted at `dir` and gather all of the relative path filenames (sans
/// directory names)
fn relative_path_names(dir: &Path) -> Result<Vec<String>, io::Error> {
    let mut paths: Vec<String> = WalkDir::new(dir)
        .into_iter()
        .map(Result::unwrap)
        .filter(|e| !e.file_type().is_dir())
        .map(|e| {
            e.path()
                .strip_prefix(dir)
                .expect("prefix present")
                .to_str()
                .expect("valid utf8 path")
                .to_string()
        })
        .collect();

    paths.sort();
    Ok(paths)
}

/// Should remove the prefix in the local filepath
#[tokio::test]
async fn test_strip_prefix_in_destination_path() {
    use std::time::Duration;
    use tokio::time::timeout;

    let result = timeout(Duration::from_secs(10), async {
        let bucket = MockBucket::builder()
            .key_with_size("abc/def/image.jpg", 12)
            .key_with_size("abc/def/title.jpg", 7)
            .key_with_size("abc/def/ghi/xyz.txt", 5)
            .build();

        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build();
        let tm = aws_sdk_s3_transfer_manager::Client::new(config);

        let dest = tempfile::tempdir().unwrap();

        let handle = tm
            .download_objects()
            .bucket("test-bucket")
            .key_prefix("abc/def/")
            .destination(dest.path())
            .initiate()
            .unwrap();

        let output = handle.join().await.unwrap();
        assert_eq!(3, output.objects_downloaded());

        let paths = relative_path_names(dest.path()).unwrap();
        let mut expected = vec!["image.jpg", "title.jpg", "ghi/xyz.txt"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<String>>();
        expected.sort();
        assert_eq!(expected, paths);
    })
    .await;
    result.expect("timed out");
}

/// Should not strip prefix from object name
#[tokio::test]
async fn test_object_with_prefix_included() {
    let bucket = MockBucket::builder()
        .key_with_size("abc/def/image.jpg", 12)
        .key_with_size("abc/def/title.jpg", 7)
        .key_with_size("abcd", 5)
        .build();

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .key_prefix("abc")
        .destination(dest.path())
        .initiate()
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(3, output.objects_downloaded());

    let paths = relative_path_names(dest.path()).unwrap();
    let mut expected = vec!["def/image.jpg", "def/title.jpg", "abcd"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<String>>();
    expected.sort();
    assert_eq!(expected, paths);
}

/// Should provide failed download(s)
#[tokio::test]
async fn test_failed_download_policy_continue() {
    let bucket = MockBucket::builder()
        .key_with_size("key1", 12)
        .key_with_size("key2", 7)
        .key_with_error("key3")
        .build();

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .failure_policy(FailedTransferPolicy::Continue)
        .initiate()
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(2, output.objects_downloaded());

    let paths = relative_path_names(dest.path()).unwrap();
    let expected = vec!["key1", "key2"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<String>>();
    assert_eq!(expected, paths);

    let failures = output.failed_transfers();
    assert_eq!(1, failures.len());

    let failed_transfer = &failures[0];
    assert_eq!(Some("key3"), failed_transfer.input().key());
}

#[tokio::test]
async fn test_recursively_downloads() {
    let mut expected_keys = vec![
        "root.jpg",
        "photos/2020/October/16/image1.jpg",
        "photos/2020/October/16/image2.jpg",
        "photos/2022/July/7/image3.jpg",
        "photos/2022/July/7/image4.jpg",
        "photos/2022/February/17/image5.jpg",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<String>>();
    expected_keys.sort();

    let bucket = {
        let mut builder = MockBucket::builder();
        for key in &expected_keys {
            builder = builder.key_with_size(key, fastrand::usize(1..64));
        }
        builder.build()
    };

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .initiate()
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(expected_keys.len() as u64, output.objects_downloaded());
    assert_eq!(0, output.failed_transfers().len());

    let paths = relative_path_names(dest.path()).unwrap();
    assert_eq!(expected_keys, paths);
}

/// Should convert delimiter correctly
#[tokio::test]
async fn test_delimiter() {
    let bucket = MockBucket::builder()
        .key_with_size("1.png", 12)
        .key_with_size("2020|1.png", 7)
        .key_with_size("2021|1.png", 5)
        .key_with_size("2022|1.png", 5)
        .key_with_size("2023|1|1.png", 5)
        .build();

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .delimiter('|')
        .destination(dest.path())
        .initiate()
        .unwrap();

    let output = handle.join().await.unwrap();
    assert_eq!(bucket.objects.len() as u64, output.objects_downloaded());
    assert_eq!(0, output.failed_transfers().len());

    let paths = relative_path_names(dest.path()).unwrap();
    let mut expected_paths: Vec<String> = bucket
        .objects
        .iter()
        .map(|o| o.object.key.as_ref().unwrap().to_owned().replace('|', "/"))
        .collect();

    expected_paths.sort();
    assert_eq!(expected_paths, paths);
}

/// Fail when destination is not a directory
#[tokio::test]
async fn test_destination_dir_not_valid() {
    let bucket = MockBucket::builder().key_with_size("image.png", 12).build();

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dest = tempfile::NamedTempFile::new().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .initiate()
        .unwrap();

    let err = handle.join().await.unwrap_err();
    let err_str = format!("{}", DisplayErrorContext(err));
    assert!(err_str.contains("target is not a directory"));
}

/// Calling `abort()` on the handle should complete without hanging.
/// The transfer is cancelled and all in-flight work settles.
#[tokio::test]
async fn test_abort_on_handle_should_terminate_tasks_gracefully() {
    use std::time::Duration;
    use tokio::time::timeout;

    timeout(Duration::from_secs(10), async {
        let bucket = MockBucket::builder()
            .key_with_size("key1", 12)
            .key_with_size("key2", 7)
            .key_with_size("key3", 5)
            .build();

        let (watch_tx, watch_rx) = watch::channel(());

        // GetObject blocks until we signal, ensuring abort fires against in-flight work
        let get = mock!(aws_sdk_s3::Client::get_object).then_output({
            let rx = watch_rx.clone();
            move || {
                while !rx.has_changed().unwrap() {}
                GetObjectOutput::builder()
                    .content_length(5)
                    .body(ByteStream::from_static(b"hello"))
                    .build()
            }
        });
        let list = bucket.list_objects_rule();
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build();
        let tm = aws_sdk_s3_transfer_manager::Client::new(config);

        let dest = tempfile::tempdir().unwrap();

        let handle = tm
            .download_objects()
            .bucket("test-bucket")
            .destination(dest.path())
            .initiate()
            .unwrap();

        // Release the mock's spin wait so in-flight GetObjects can return.
        watch_tx.send(()).unwrap();

        // abort() should complete (cancel + wait_for_idle) without hanging.
        handle.abort().await;
    })
    .await
    .expect("test_abort_on_handle_should_terminate_tasks_gracefully timed out");
}

/// When ListObjectsV2 fails, `join()` returns an error with the SDK error as source.
#[tokio::test]
async fn test_failed_list_objects_should_cancel_the_operation() {
    use std::time::Duration;
    use tokio::time::timeout;

    timeout(Duration::from_secs(10), async {
        // ListObjectsV2 returns a modeled error (no retries on modeled errors)
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            ListObjectsV2Error::NoSuchBucket(
                aws_sdk_s3::types::error::NoSuchBucket::builder()
                    .meta(
                        aws_smithy_types::error::ErrorMetadata::builder()
                            .code("NoSuchBucket")
                            .message("The specified bucket does not exist")
                            .build(),
                    )
                    .build(),
            )
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list]);

        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build();
        let tm = aws_sdk_s3_transfer_manager::Client::new(config);

        let dest = tempfile::tempdir().unwrap();

        let handle = tm
            .download_objects()
            .bucket("test-bucket")
            .destination(dest.path())
            .initiate()
            .unwrap();

        let err = handle.join().await.unwrap_err();
        // A ListObjectsV2 failure surfaces as a service error carrying the
        // operation and recovered service metadata.
        assert_eq!(&ErrorKind::ServiceError, err.kind());
        assert_eq!(Some("ListObjectsV2"), err.operation_name());
        assert!(err.is_not_found(), "NoSuchBucket should be not-found");
    })
    .await
    .expect("test_failed_list_objects_should_cancel_the_operation timed out");
}

/// When a child GetObject fails under Abort policy, `join()` returns an error.
#[tokio::test]
async fn test_failed_get_object_should_cancel_the_operation() {
    use std::time::Duration;
    use tokio::time::timeout;

    timeout(Duration::from_secs(10), async {
        let bucket = MockBucket::builder()
            .key_with_size("key1", 12)
            .key_with_error("key2")
            .key_with_size("key3", 7)
            .build();

        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build();
        let tm = aws_sdk_s3_transfer_manager::Client::new(config);

        let dest = tempfile::tempdir().unwrap();

        let handle = tm
            .download_objects()
            .bucket("test-bucket")
            .destination(dest.path())
            .initiate()
            .unwrap();

        let err = handle.join().await.unwrap_err();
        // Under Abort policy the parent surfaces ChildOperationFailed, carrying
        // the per-object failures so they are reachable on the error path.
        assert_eq!(&ErrorKind::ChildOperationFailed, err.kind());
        let failed = err
            .failed_downloads()
            .expect("ChildOperationFailed carries the failed downloads");
        assert!(
            !failed.is_empty(),
            "expected at least one failed download to be attached"
        );
    })
    .await
    .expect("test_failed_get_object_should_cancel_the_operation timed out");
}

/// Dropping the handle mid-transfer should not panic.
#[tokio::test]
async fn test_drop_download_objects_handle() {
    use std::time::Duration;
    use tokio::time::timeout;

    timeout(Duration::from_secs(10), async {
        let bucket = MockBucket::builder()
            .key_with_size("key1", 12)
            .key_with_size("key2", 7)
            .key_with_size("key3", 5)
            .build();

        let (watch_tx, watch_rx) = watch::channel(());

        let get = mock!(aws_sdk_s3::Client::get_object).then_output({
            move || {
                let _ = watch_tx.send(());
                GetObjectOutput::builder()
                    .content_length(5)
                    .body(ByteStream::from_static(b"hello"))
                    .build()
            }
        });
        let list = bucket.list_objects_rule();
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build();
        let tm = aws_sdk_s3_transfer_manager::Client::new(config);

        let dest = tempfile::tempdir().unwrap();

        let handle = tm
            .download_objects()
            .bucket("test-bucket")
            .destination(dest.path())
            .initiate()
            .unwrap();

        // Wait until at least one GetObject has been invoked so drop happens
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
    .expect("test_drop_download_objects_handle timed out");
}
