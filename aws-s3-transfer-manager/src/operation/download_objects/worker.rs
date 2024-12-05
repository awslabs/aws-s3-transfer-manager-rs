/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::operation::download::DownloadOutput;
use async_channel::{Receiver, Sender};
use path_clean::PathClean;
use std::borrow::Cow;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::error::{self, ErrorKind};
use crate::operation::download::{DownloadInput, DownloadInputBuilder};
use crate::operation::DEFAULT_DELIMITER;
use crate::types::{DownloadFilter, FailedDownload, FailedTransferPolicy};

use super::list_objects::ListObjectsStream;
use super::DownloadObjectsContext;

#[derive(Debug)]
pub(super) struct DownloadObjectJob {
    object: aws_sdk_s3::types::Object,
}

impl DownloadObjectJob {
    /// Get the input used to download this object
    pub(super) fn input(&self, ctx: &DownloadObjectsContext) -> DownloadInput {
        DownloadInputBuilder::default()
            .set_bucket(ctx.state.input.bucket.to_owned())
            .set_key(self.object.key.to_owned())
            .build()
            .expect("valid input")
    }
}

// TODO(optimization) - we already will have the object metadata/size for when we goto download it
// which could be used to seed/skip object discovery.

// worker to enumerate objects from a bucket
pub(super) async fn discover_objects(
    ctx: DownloadObjectsContext,
    work_tx: Sender<DownloadObjectJob>,
) -> Result<(), error::Error> {
    let mut stream = ListObjectsStream::new(ctx.clone());

    let default_filter = &DownloadFilter::default();
    let filter = ctx.state.input.filter().unwrap_or(default_filter);

    let mut cancel_rx = ctx.state.cancel_rx.clone();

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => {
                tracing::error!("received cancellation signal, exiting and not listing new objects");
                return Err(error::operation_cancelled());
            }
            obj_result = stream.next() => {
                match obj_result {
                    None => break,
                    Some(obj_result) => {
                        let object = obj_result?;

                        if !(filter.predicate)(&object) {
                            // TODO(SEP) - The S3 Transfer Manager MAY add validation to handle the case for the objects whose
                            // keys differ only by case in case-insensitive filesystems such as Windows. For example, throw
                            // validation exception if a user attempts to download a bucket that contains "foobar" and "FOOBAR" in Windows.
                            tracing::debug!("skipping object due to filter: {:?}", object);
                            continue;
                        }

                        let job = DownloadObjectJob { object };
                        if work_tx.send(job).await.is_err() {
                            tracing::error!("all receiver ends have been dropped, unable to send a job!");
                            break;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// worker to download an object
pub(super) async fn download_objects(
    ctx: DownloadObjectsContext,
    work_rx: Receiver<DownloadObjectJob>,
) -> Result<(), error::Error> {
    let mut cancel_rx = ctx.state.cancel_rx.clone();
    loop {
        tokio::select! {
            _ = cancel_rx.changed() => {
                tracing::error!("received cancellation signal, exiting and not downloading a new object");
                return Err(error::operation_cancelled());
            }
            job = work_rx.recv() => {
                match job {
                    Err(_) => break,
                    Ok(job) => {
                        tracing::debug!(
                            "worker recv'd request for key {:?} ({:?} bytes)",
                            job.object.key,
                            job.object.size()
                        );

                        let dl_result = download_single_obj(&ctx, &job).await;
                        match dl_result {
                            Ok(_) => {
                                ctx.state
                                    .successful_downloads
                                    .fetch_add(1, Ordering::SeqCst);

                                let bytes_transferred: u64 = job
                                    .object
                                    .size()
                                    .unwrap_or_default()
                                    .try_into()
                                    .unwrap_or_default();

                                ctx.state
                                    .total_bytes_transferred
                                    .fetch_add(bytes_transferred, Ordering::SeqCst);

                                tracing::debug!("worker finished downloading key {:?}", job.object.key);
                            }
                            Err(err) => {
                                tracing::debug!(
                                    "worker failed to download key {:?}: {}",
                                    job.object.key,
                                    err
                                );
                                match ctx.state.input.failure_policy() {
                                    FailedTransferPolicy::Abort => {
                                        // Sending a cancellation signal during graceful shutdown would be redundant.
                                        if err.kind() != &ErrorKind::OperationCancelled
                                            && ctx.state.cancel_tx.send(true).is_err()
                                        {
                                            tracing::warn!(
                                                "all receiver ends have been dropped, unable to send a cancellation signal"
                                            );
                                        }
                                        return Err(err);
                                    }
                                    FailedTransferPolicy::Continue => {
                                        let mut failures = ctx.state.failed_downloads.lock().unwrap();

                                        let failed_transfer = FailedDownload {
                                            input: job.input(&ctx),
                                            error: err,
                                        };

                                        failures.push(failed_transfer);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    tracing::trace!("req channel closed, worker finished");
    Ok(())
}

async fn download_single_obj(
    ctx: &DownloadObjectsContext,
    job: &DownloadObjectJob,
) -> Result<(), error::Error> {
    let input = job.input(ctx);
    let root_dir = ctx.state.input.destination().expect("destination set");
    let key = input.key.as_ref().expect("key set");
    let prefix = ctx.state.input.key_prefix();
    let delim = ctx.state.input.delimiter();

    let key_path = local_key_path(root_dir, key.as_str(), prefix, delim)?;
    let mut handle =
        crate::operation::download::Download::orchestrate(ctx.handle.clone(), input, true)?;

    // The cancellation process would work fine without this if statement.
    // It's here so we can save a single download operation that would otherwise
    // be wasted if the system is already in graceful shutdown mode.
    if ctx
        .state
        .cancel_rx
        .has_changed()
        .expect("the channel should be open as it is owned by `DownloadObjectsState`")
    {
        handle.abort().await;
        return Err(error::operation_cancelled());
    }

    let _ = handle.object_meta().await?;
    let mut output: DownloadOutput = mem::replace(&mut handle.output, DownloadOutput::empty());

    let parent_dir = key_path.parent().expect("valid parent dir for key");
    fs::create_dir_all(parent_dir).await?;
    let mut dest = fs::File::create(key_path).await?;

    while let Some(chunk) = output.next().await {
        let chunk = chunk?;
        for segment in chunk.data.into_segments() {
            dest.write_all(segment.as_ref()).await?;
        }
    }

    Ok(())
}

/// If the prefix is not empty AND the key contains the delimiter, strip the prefix from the key.
///
/// If delimiter is `None`, uses "/" by default.
///
///
/// # Examples
///
/// ```ignore
/// let actual = strip_key_prefix("notes/2021/1.txt", Some("notes/2021/"), None);
/// assert_eq!("1.txt", actual);
///
/// // If the prefix is not the full name of the folder, the folder name will be truncated.
/// let actual = strip_key_prefix("top-level/sub-folder/1.txt", Some("top-"), Some("/"));
/// assert_eq!("level/sub-folder/1.txt", actual);
/// ```
///
fn strip_key_prefix<'a>(key: &'a str, prefix: Option<&str>, delimiter: Option<&str>) -> &'a str {
    let prefix = prefix.unwrap_or("");
    let delim = delimiter.unwrap_or(DEFAULT_DELIMITER);

    if key.is_empty() || prefix.is_empty() || !key.starts_with(prefix) || !key.contains(delim) {
        return key;
    }

    let stripped = &key[prefix.len()..];

    if prefix.ends_with(delim) || !stripped.starts_with(delim) {
        return stripped;
    }

    &stripped[1..]
}

/// Replace `delimiter` in `key` if it does not match the `path_separator`
fn replace_delim<'a>(key: &'a str, delimiter: Option<&str>, path_separator: &str) -> Cow<'a, str> {
    match delimiter {
        Some(delim) if delim != path_separator => {
            let replaced = key.replace(delim, path_separator);
            Cow::Owned(replaced)
        }
        _ => Cow::Borrowed(key),
    }
}

/// Derive the local path for a given S3 key
fn local_key_path(
    root_dir: &Path,
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Result<PathBuf, error::Error> {
    let stripped = strip_key_prefix(key, prefix, delimiter);
    let relative_path = replace_delim(stripped, delimiter, std::path::MAIN_SEPARATOR_STR);

    let local_path = root_dir.join(relative_path.as_ref()).clean();
    validate_path(root_dir, &local_path, key)?;

    Ok(local_path)
}

fn validate_path(root_dir: &Path, local_path: &Path, key: &str) -> Result<(), error::Error> {
    // validate the resolved key path doesn't resolve outside the destination directory
    if !local_path.starts_with(root_dir) {
        let err = error::Error::new(error::ErrorKind::InputInvalid, format!("Unable to download key: '{key}', it's relative path resolves outside the target destination directory"));
        return Err(err);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::operation::download_objects::{DownloadObjectsContext, DownloadObjectsInput};

    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_smithy_mocks_experimental::mock;
    use test_common::mock_client_with_stubbed_http_client;

    use std::path::PathBuf;

    struct ObjectKeyPathTest {
        key: &'static str,
        prefix: Option<&'static str>,
        delimiter: Option<&'static str>,
        expected: Result<&'static str, &'static str>,
    }

    fn success_path_test(
        key: &'static str,
        prefix: Option<&'static str>,
        delimiter: Option<&'static str>,
        expected: &'static str,
    ) -> ObjectKeyPathTest {
        ObjectKeyPathTest {
            key,
            prefix,
            delimiter,
            expected: Ok(expected),
        }
    }

    // we only use this on linux so windows only test complains
    #[allow(dead_code)]
    fn error_path_test(
        key: &'static str,
        prefix: Option<&'static str>,
        delimiter: Option<&'static str>,
        expected_err: &'static str,
    ) -> ObjectKeyPathTest {
        ObjectKeyPathTest {
            key,
            prefix,
            delimiter,
            expected: Err(expected_err),
        }
    }

    #[test]
    fn test_strip_key_prefix() {
        let tests = &[
            success_path_test("no-delim", None, None, "no-delim"),
            success_path_test("no-delim", Some(""), None, "no-delim"),
            success_path_test(
                "delim/with/separator",
                Some(""),
                None,
                "delim/with/separator",
            ),
            success_path_test("", Some("no-delim"), None, ""),
            success_path_test("no-delim", Some("no-delim"), None, "no-delim"),
            success_path_test("delim/", Some("delim"), None, ""),
            success_path_test("not-in-key", Some("prefix"), None, "not-in-key"),
            success_path_test("notes/2021/1.txt", Some("notes/2021"), None, "1.txt"),
            success_path_test("notes/2021/1.txt", Some("notes/2021/"), None, "1.txt"),
            success_path_test(
                "top-level/sub-folder/1.txt",
                Some("top-"),
                None,
                "level/sub-folder/1.txt",
            ),
            success_path_test(
                "someInnerFolder/another/file1.txt",
                Some("someInner"),
                None,
                "Folder/another/file1.txt",
            ),
            success_path_test(
                "someInnerF/another/file1.txt",
                Some("someInner"),
                None,
                "F/another/file1.txt",
            ),
            success_path_test(
                "someInner/another/file1.txt",
                Some("someInner"),
                None,
                "another/file1.txt",
            ),
            success_path_test(
                "someInner/another/file1.txt",
                Some("someInner/a"),
                None,
                "nother/file1.txt",
            ),
        ];

        for test in tests {
            let actual = strip_key_prefix(test.key, test.prefix, test.delimiter);
            assert_eq!(*test.expected.as_ref().unwrap(), actual);
        }
    }

    #[test]
    fn test_strip_key_prefix_delims() {
        let delims = ["/", "//", "\\", "|", "delim"];
        for delim in delims {
            let prefix = format!("notes{delim}2021{delim}");
            let key = format!("notes{delim}2021{delim}1.txt");
            let actual = strip_key_prefix(key.as_str(), Some(prefix.as_str()), Some(delim));
            assert_eq!("1.txt", actual);
        }
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn test_local_key_path_linux() {
        use aws_sdk_s3::error::DisplayErrorContext;

        let tests = &[
            success_path_test("2023/Jan/1.png", None, None, "test/2023/Jan/1.png"),
            success_path_test("2023/Jan/1.png", Some("2023/Jan/"), None, "test/1.png"),
            success_path_test("2023/Jan/1.png", Some("2023/Jan"), None, "test/1.png"),
            success_path_test("2023-Jan-1.png", None, Some("-"), "test/2023/Jan/1.png"),
            success_path_test("2023-Jan-.png", None, Some("-"), "test/2023/Jan/.png"),
            success_path_test(
                "many////delims-in-a-row",
                None,
                Some("/"),
                "test/many/delims-in-a-row",
            ),
            // FIXME - figure out if this test case is valid, Java v2 TM fails with exception stating it's outside the target directory
            // success_path_test("2023/Jan-1.png",	Some("2023"),	Some("-"),	"test/Jan/1.png"),

            // resolves outside parent folder
            error_path_test(
                "../2023/Jan/1.png",
                None,
                None,
                "Unable to download key: '../2023/Jan/1.png'",
            ),
            error_path_test(
                "/2023/Jan/1.png",
                None,
                None,
                "Unable to download key: '/2023/Jan/1.png'",
            ),
            error_path_test(
                "foo/../2023/../../Jan/1.png",
                None,
                None,
                "Unable to download key: 'foo/../2023/../../Jan/1.png'",
            ),
            error_path_test(
                "../test-2/object.dat",
                None,
                None,
                "Unable to download key: '../test-2/object.dat'",
            ),
        ];

        for test in tests {
            let root_dir = PathBuf::from("test");
            let actual = local_key_path(&root_dir, test.key, test.prefix, test.delimiter);
            if test.expected.is_ok() {
                let actual = actual.expect("expected success");
                let actual_str = actual.to_str().expect("valid utf-8 path");
                assert_eq!(*test.expected.as_ref().unwrap(), actual_str);
            } else {
                let err =
                    actual.expect_err("path resolves outside of parent folder, expected error");
                let actual_err = format!("{}", DisplayErrorContext(err));
                let expected_err_substr = test.expected.as_ref().unwrap_err();
                assert!(
                    actual_err.contains(expected_err_substr),
                    "'{actual_err}' does not contain '{expected_err_substr}'"
                );
            }
        }
    }

    #[cfg(target_family = "windows")]
    #[test]
    fn test_local_key_path_windows() {
        let test = success_path_test("2023/Jan/1.png", None, None, "test\\2023\\Jan\\1.png");
        let root_dir = PathBuf::from("test");
        let actual = local_key_path(&root_dir, test.key, test.prefix, test.delimiter).unwrap();
        let actual_str = actual.to_str().expect("valid utf-8 path");
        assert_eq!(*test.expected.as_ref().unwrap(), actual_str);
    }

    #[tokio::test]
    async fn test_skip_folder_objects() {
        let list_objects_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("key1")
                        .size(10)
                        .build(),
                )
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("key2")
                        .size(0)
                        .build(),
                )
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("folder/")
                        .size(0)
                        .build(),
                )
                .build()
        });

        let s3_client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&list_objects_rule]);
        let config = crate::Config::builder().client(s3_client).build();
        let client = crate::Client::new(config);
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .build()
            .unwrap();

        let ctx = DownloadObjectsContext::new(client.handle.clone(), input);

        let (work_tx, work_rx) = async_channel::unbounded();

        let join_handle = tokio::spawn(discover_objects(ctx, work_tx));

        let mut keys = Vec::new();

        while let Ok(job) = work_rx.recv().await {
            keys.push(job.object.key.unwrap());
        }

        join_handle.await.unwrap().unwrap();

        assert_eq!(keys, vec!["key1", "key2"]);
    }

    #[tokio::test]
    async fn test_user_filter() {
        let list_objects_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("key1")
                        .size(10)
                        .build(),
                )
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("key2")
                        .size(0)
                        .build(),
                )
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("key3")
                        .size(7)
                        .build(),
                )
                .contents(
                    aws_sdk_s3::types::Object::builder()
                        .key("foobar")
                        .size(12)
                        .build(),
                )
                .build()
        });

        let s3_client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&list_objects_rule]);
        let config = crate::Config::builder().client(s3_client).build();
        let client = crate::Client::new(config);
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .filter(|obj| obj.size().unwrap_or_default() > 0 && obj.key().unwrap().contains("key"))
            .build()
            .unwrap();

        let ctx = DownloadObjectsContext::new(client.handle.clone(), input);

        let (work_tx, work_rx) = async_channel::unbounded();

        let join_handle = tokio::spawn(discover_objects(ctx, work_tx));

        let mut keys = Vec::new();

        while let Ok(job) = work_rx.recv().await {
            keys.push(job.object.key.unwrap());
        }

        join_handle.await.unwrap().unwrap();

        assert_eq!(keys, vec!["key1", "key3"]);
    }
}
