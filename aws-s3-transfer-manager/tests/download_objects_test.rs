/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_s3_transfer_manager::types::FailedTransferPolicy;
use aws_sdk_s3::{
    error::DisplayErrorContext,
    operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Output},
    primitives::ByteStream,
};
use aws_smithy_mocks_experimental::{mock, mock_client, Rule, RuleMode};
use aws_smithy_runtime_api::{client::orchestrator::HttpResponse, http::StatusCode};
use bytes::Bytes;
use std::{io, iter, path::Path, sync::Arc};
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

fn get_object_error_http_resp() -> HttpResponse {
    HttpResponse::new(StatusCode::try_from(500).unwrap(), Bytes::new().into())
}

/// Get the mock rule for this object when `get_object` is API is invoked for the corresponding key
fn get_object_rule(mobj: &MockObject) -> Rule {
    let share1 = Arc::new(mobj.clone());
    let share2 = share1.clone();

    if mobj.error_on_get {
        mock!(aws_sdk_s3::Client::get_object)
            .match_requests(move |r| r.key() == share1.object.key())
            .then_http_response(get_object_error_http_resp)
    } else {
        mock!(aws_sdk_s3::Client::get_object)
            .match_requests(move |r| r.key() == share1.object.key())
            .then_output(move || share2.get_object_output())
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

    /// Return the mock rules representing this bucket. This includes
    /// the `ListObjectsV2` call as well as all of the `GetObject` calls.
    fn rules(&self) -> Vec<aws_smithy_mocks_experimental::Rule> {
        let contents = self.objects.iter().map(|m| m.object.clone()).collect();

        let list_output = ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();

        let list_rule =
            mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || list_output.clone());

        let mut rules: Vec<Rule> = self.objects.iter().map(get_object_rule).collect();

        rules.push(list_rule);
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
    let bucket = MockBucket::builder()
        .key_with_size("abc/def/image.jpg", 12)
        .key_with_size("abc/def/title.jpg", 7)
        .key_with_size("abc/def/ghi/xyz.txt", 5)
        .build();

    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, bucket.rules().as_slice());

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .key_prefix("abc/def/")
        .destination(dest.path())
        .send()
        .await
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

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .key_prefix("abc")
        .destination(dest.path())
        .send()
        .await
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

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .failure_policy(FailedTransferPolicy::Continue)
        .send()
        .await
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

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .send()
        .await
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

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::tempdir().unwrap();

    let handle = tm
        .download_objects()
        .bucket("test-bucket")
        .delimiter('|')
        .destination(dest.path())
        .send()
        .await
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

    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    let dest = tempfile::NamedTempFile::new().unwrap();

    let err = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dest.path())
        .send()
        .await
        .unwrap_err();

    let err_str = format!("{}", DisplayErrorContext(err));
    assert!(err_str.contains("destination is not a directory"));
}
