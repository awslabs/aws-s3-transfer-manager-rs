/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::borrow::Cow;
use std::path::{MAIN_SEPARATOR, MAIN_SEPARATOR_STR};
use std::sync::atomic::Ordering;

use super::{UploadObjectsContext, UploadObjectsInput};
use async_channel::{Receiver, Sender};
use blocking::Unblock;
use futures_util::StreamExt;
use walkdir::WalkDir;

use crate::error::ErrorKind;
use crate::io::InputStream;
use crate::operation::upload::UploadInputBuilder;
use crate::operation::DEFAULT_DELIMITER;
use crate::types::{FailedTransferPolicy, FailedUploadTransfer, UploadFilter};
use crate::{error, types::UploadFilterItem};

#[derive(Debug)]
pub(super) struct UploadObjectJob {
    key: String,
    object: InputStream,
}

impl UploadObjectJob {
    fn new(key: String, object: InputStream) -> Self {
        Self { key, object }
    }
}

pub(super) async fn list_directory_contents(
    input: UploadObjectsInput,
    list_directory_tx: Sender<Result<UploadObjectJob, error::Error>>,
) -> Result<(), error::Error> {
    // TODO - Reevaluate the need for the `blocking` crate once we implement stricter task cancellation for download and upload.
    // If we switch to using `tokio::task::spawn_blocking` instead of the `blocking` crate, the entire `list_directory_contents` function
    // would need to be passed to `spawn_blocking`, which implies the following:
    // - `list_directory_contents` would need to become a regular, non-async function, complicating the use of `async_channel::Sender` within it.
    // - The `AbortHandle` returned by `spawn_blocking` would not have any effect when calling `abort`, which may impact our task cancellation behavior.

    // Move a blocking I/O to a dedicated thread pool
    let mut walker = Unblock::new(walker(&input).into_iter());

    let default_filter = &UploadFilter::default();
    let filter = input.filter().unwrap_or(default_filter);

    while let Some(entry) = walker.next().await {
        let job = match entry {
            Ok(entry) => {
                let metadata = tokio::fs::metadata(entry.path()).await?;
                let filter_item = UploadFilterItem::builder()
                    .path(entry.path())
                    .metadata(metadata.clone());
                let filter_item = if !input.follow_symlinks() {
                    filter_item.symlink_metadata(tokio::fs::symlink_metadata(entry.path()).await?)
                } else {
                    filter_item
                }
                .build();
                if !(filter.predicate)(&filter_item) {
                    tracing::debug!("skipping object due to filter: {:?}", entry.path());
                    continue;
                }

                let recursion_root_dir_path = input.source().expect("source set");
                let entry_path = entry.path();
                let relative_filename = entry_path
                    .strip_prefix(recursion_root_dir_path)
                    .expect("{entry_path:?} should be a path entry directly or indirectly under {recursion_root_dir_path:?}")
                    .to_str()
                    .expect("valid utf-8 path");
                let object_key =
                    derive_object_key(relative_filename, input.key_prefix(), input.delimiter())?;
                let object = InputStream::read_from()
                    .path(entry.path())
                    .metadata(metadata)
                    .build();

                match object {
                    Ok(object) => {
                        tracing::info!(
                            "uploading {relative_filename} with object key {object_key}..."
                        );
                        Ok(UploadObjectJob::new(object_key.into_owned(), object))
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(walkdir_error) => {
                let error_kind = if walkdir_error.io_error().is_some() {
                    ErrorKind::IOError
                } else {
                    ErrorKind::InputInvalid
                };

                // We avoid converting `walkdir::Error` into `std::io::Error` to preserve important information,
                // such as which path entry triggered a `PermissionDenied` error.
                Err(crate::error::Error::new(error_kind, walkdir_error))
            }
        };
        list_directory_tx.send(job).await.expect("channel valid");
    }

    Ok(())
}

fn walker(input: &UploadObjectsInput) -> WalkDir {
    let source = input.source().expect("source set");
    let mut walker = WalkDir::new(source);
    if input.follow_symlinks() {
        walker = walker.follow_links(true);
    }
    if !input.recursive() {
        walker = walker.max_depth(1);
    }
    walker
}

fn derive_object_key<'a>(
    relative_filename: &'a str,
    object_key_prefix: Option<&str>,
    object_key_delimiter: Option<&str>,
) -> Result<Cow<'a, str>, error::Error> {
    if let Some(delim) = object_key_delimiter {
        if delim != DEFAULT_DELIMITER && relative_filename.contains(delim) {
            return Err(error::invalid_input(format!(
                "a custom delimiter `{delim}` should not appear in `{relative_filename}`"
            )));
        }
    }

    let delim = object_key_delimiter.unwrap_or(DEFAULT_DELIMITER);

    let relative_filename = if delim == MAIN_SEPARATOR_STR {
        Cow::Borrowed(relative_filename)
    } else {
        Cow::Owned(relative_filename.replace(MAIN_SEPARATOR, delim))
    };

    let object_key = if let Some(prefix) = object_key_prefix {
        if prefix.ends_with(delim) {
            Cow::Owned(format!("{prefix}{relative_filename}"))
        } else {
            Cow::Owned(format!("{prefix}{delim}{relative_filename}"))
        }
    } else {
        relative_filename
    };

    Ok(object_key)
}

pub(super) async fn upload_objects(
    ctx: UploadObjectsContext,
    list_directory_rx: Receiver<Result<UploadObjectJob, error::Error>>,
) -> Result<(), error::Error> {
    while let Ok(job) = list_directory_rx.recv().await {
        match job {
            Ok(job) => {
                let key = job.key.clone();
                let result = upload_single_obj(&ctx, job).await;
                match result {
                    Ok(bytes_transferred) => {
                        ctx.state.successful_uploads.fetch_add(1, Ordering::SeqCst);

                        ctx.state
                            .total_bytes_transferred
                            .fetch_add(bytes_transferred, Ordering::SeqCst);

                        tracing::debug!("worker finished uploading object {key:?}");
                    }
                    Err(err) => {
                        tracing::debug!("worker failed to upload object {key:?}: {err}");
                        handle_failed_upload(err, &ctx, Some(key))?;
                    }
                }
            }
            Err(err) => {
                tracing::debug!("worker received an error from the `list_directory` task: {err}");
                handle_failed_upload(err, &ctx, None)?;
            }
        }
    }

    tracing::trace!("req channel closed, worker finished");
    Ok(())
}

async fn upload_single_obj(
    ctx: &UploadObjectsContext,
    job: UploadObjectJob,
) -> Result<u64, error::Error> {
    let UploadObjectJob { object, key } = job;

    // `object` gets consumed by `input` so calculate the content length in advance.
    // While true for file-based workloads, the upper `size_hint` might not be equal to the actual bytes transferred.
    let bytes_transferred: u64 = object
        .size_hint()
        .upper()
        .ok_or_else(crate::io::error::Error::upper_bound_size_hint_required)
        .unwrap();

    let input = UploadInputBuilder::default()
        .set_bucket(ctx.state.input.bucket.to_owned())
        .set_body(Some(object))
        .set_key(Some(key))
        .build()
        .expect("valid input");

    let handle = crate::operation::upload::Upload::orchestrate(ctx.handle.clone(), input).await?;

    handle.join().await?;

    Ok(bytes_transferred)
}

fn handle_failed_upload(
    err: error::Error,
    ctx: &UploadObjectsContext,
    object_key: Option<String>,
) -> Result<(), error::Error> {
    match ctx.state.input.failure_policy() {
        // TODO - this will abort this worker, the rest of the workers will be aborted
        // when the handle is joined and the error is propagated and the task set is
        // dropped. This _may_ be later/too passive and we might consider aborting all
        // the tasks on error rather than relying on join and then drop.
        FailedTransferPolicy::Abort => Err(err),
        FailedTransferPolicy::Continue => {
            let mut failures = ctx.state.failed_uploads.lock().unwrap();

            let failed_transfer = FailedUploadTransfer {
                input: match object_key {
                    key @ Some(_) => Some(
                        UploadInputBuilder::default()
                            .set_bucket(ctx.state.input.bucket.to_owned())
                            // We avoid creating a new `InputStream` to pass to `set_body`, as it incurs unnecessary
                            // overhead just for error reporting purposes.
                            .set_key(key)
                            .build()
                            .expect("valid input"),
                    ),
                    None => None,
                },
                error: err,
            };

            failures.push(failed_transfer);

            Ok(())
        }
    }
}

#[cfg(test)]
mod unit {
    #[cfg(target_family = "unix")]
    mod unix_tests {
        use crate::operation::upload_objects::worker::*;
        use crate::operation::upload_objects::UploadObjectsInputBuilder;
        use std::collections::BTreeMap;
        use std::error::Error as _;
        use test_common::create_test_dir;
        use tokio::fs::symlink;

        #[test]
        fn test_derive_object_key() {
            assert_eq!(
                "2023/Jan/1.png",
                derive_object_key("2023/Jan/1.png", None, None).unwrap()
            );
            assert_eq!(
                "foobar/2023/Jan/1.png",
                derive_object_key("2023/Jan/1.png", Some("foobar"), None).unwrap()
            );
            assert_eq!(
                "foobar/2023/Jan/1.png",
                derive_object_key("2023/Jan/1.png", Some("foobar/"), None).unwrap()
            );
            assert_eq!(
                "2023-Jan-1.png",
                derive_object_key("2023/Jan/1.png", None, Some("-")).unwrap()
            );
            assert_eq!(
                "foobar-2023-Jan-1.png",
                derive_object_key("2023/Jan/1.png", Some("foobar"), Some("-")).unwrap()
            );
            assert_eq!(
                "foobar-2023-Jan-1.png",
                derive_object_key("2023/Jan/1.png", Some("foobar-"), Some("-")).unwrap()
            );
            assert_eq!(
                "foobar--2023-Jan-1.png",
                derive_object_key("2023/Jan/1.png", Some("foobar--"), Some("-")).unwrap()
            );
            assert_eq!(
                "2023/MYLONGDELIMJan/MYLONGDELIM1.png",
                derive_object_key("2023/Jan/1.png", None, Some("/MYLONGDELIM")).unwrap()
            );
            {
                let err = derive_object_key("2023/Jan-1.png", None, Some("-"))
                    .err()
                    .unwrap();
                assert_eq!(
                    "a custom delimiter `-` should not appear in `2023/Jan-1.png`",
                    format!("{}", err.source().unwrap())
                );
            }

            // Should not replace the path separator in prefix with a custom delimiter
            assert_eq!(
                "foo/bar-2023-Jan-1.png",
                derive_object_key("2023/Jan/1.png", Some("foo/bar"), Some("-")).unwrap()
            );

            // Should not fail if the user specifies the default delimiter as a custom delimiter
            assert_eq!(
                "2023/Jan/1.png",
                derive_object_key("2023/Jan/1.png", None, Some(DEFAULT_DELIMITER)).unwrap()
            );
        }

        async fn exercise_list_directory_contents(
            input: UploadObjectsInput,
        ) -> (BTreeMap<String, usize>, Vec<error::Error>) {
            let (list_directory_tx, list_directory_rx) = async_channel::unbounded();

            let join_handle = tokio::spawn(list_directory_contents(input, list_directory_tx));

            let mut successes = BTreeMap::new();
            let mut errors = Vec::new();
            while let Ok(job) = list_directory_rx.recv().await {
                match job {
                    Ok(job) => {
                        successes.insert(job.key, job.object.size_hint().upper().unwrap() as usize);
                    }
                    Err(e) => errors.push(e),
                }
            }

            let _ = join_handle.await.unwrap();

            (successes, errors)
        }

        #[tokio::test]
        async fn test_list_directory_contents_should_send_upload_object_jobs_from_traversed_path_entries(
        ) {
            let recursion_root = "test";
            let files = vec![
                ("sample.jpg", 1),
                ("photos/2022/January/sample.jpg", 1),
                ("photos/2022/February/sample1.jpg", 1),
                ("photos/2022/February/sample2.jpg", 1),
                ("photos/2022/February/sample3.jpg", 1),
            ];
            let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

            // Test with only required input fields (no recursion)
            {
                let input = UploadObjectsInputBuilder::default()
                    .bucket("doesnotmatter")
                    .source(test_dir.path())
                    .build()
                    .unwrap();

                let (actual, errors) = exercise_list_directory_contents(input).await;

                let expected = files
                    .iter()
                    .take(1)
                    .map(|(entry_path, value)| ((*entry_path).to_owned(), *value))
                    .collect::<BTreeMap<_, _>>();

                assert_eq!(expected, actual);
                assert!(errors.is_empty());
            }

            // Test with recursion
            {
                let input = UploadObjectsInputBuilder::default()
                    .bucket("doesnotmatter")
                    .source(test_dir.path())
                    .recursive(true)
                    .build()
                    .unwrap();

                let (actual, errors) = exercise_list_directory_contents(input).await;

                let expected = files
                    .iter()
                    .map(|(entry_path, value)| ((*entry_path).to_owned(), *value))
                    .collect::<BTreeMap<_, _>>();

                assert_eq!(expected, actual);
                assert!(errors.is_empty());
            }

            // Test with recursion, a custom key prefix, and a custom delimiter
            {
                let key_prefix = "test";
                let delimiter = "-";
                let input = UploadObjectsInputBuilder::default()
                    .bucket("doesnotmatter")
                    .source(test_dir.path())
                    .recursive(true)
                    .key_prefix(key_prefix)
                    .delimiter(delimiter)
                    .build()
                    .unwrap();

                let (actual, errors) = exercise_list_directory_contents(input).await;

                let expected = files
                    .iter()
                    .map(|(entry_path, value)| {
                        (
                            // For verification purporses, we manually derive object key without using `derive_object_key`
                            format!(
                                "{key_prefix}{delimiter}{key_suffix}",
                                key_suffix = entry_path.replace('/', delimiter)
                            ),
                            *value,
                        )
                    })
                    .collect::<BTreeMap<_, _>>();

                assert_eq!(expected, actual);
                assert!(errors.is_empty());
            }
        }

        #[tokio::test]
        async fn test_list_directory_contents_with_symlinks() {
            let files1 = vec![("sample.jpg", 1)];
            let temp_dir1 = create_test_dir(Some("temp1"), files1.clone(), &[]);

            let files2 = vec![
                ("sample.txt", 1),
                ("docs/2022/January/sample.txt", 1),
                ("docs/2022/February/sample1.txt", 1),
                ("docs/2022/February/sample2.txt", 1),
                ("docs/2022/February/sample3.txt", 1),
            ];
            let temp_dir2 = create_test_dir(Some("temp2"), files2.clone(), &[]);

            let files3 = vec![("sample3.png", 1)];
            let temp_dir3 = create_test_dir(Some("temp3"), files3.clone(), &[]);

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

            // Test with following symlinks
            {
                let input = UploadObjectsInputBuilder::default()
                    .bucket("doesnotmatter")
                    .source(temp_dir1.path())
                    .recursive(true)
                    .follow_symlinks(true)
                    .build()
                    .unwrap();

                let (actual, errors) = exercise_list_directory_contents(input).await;

                let expected = files1
                    .iter()
                    .map(|(entry_path, size)| ((*entry_path).to_owned(), *size))
                    .chain(
                        files2
                            .iter()
                            .map(|(entry_path, size)| ("symlink/".to_owned() + *entry_path, *size)),
                    )
                    .chain(
                        files3
                            .iter()
                            .map(|(_, size)| ("symlink2".to_owned(), *size)),
                    )
                    .collect::<BTreeMap<_, _>>();

                assert_eq!(expected, actual);
                assert!(errors.is_empty());
            }

            // Test without following symlinks
            {
                let input = UploadObjectsInputBuilder::default()
                    .bucket("doesnotmatter")
                    .source(temp_dir1.path())
                    .recursive(true)
                    .build()
                    .unwrap();

                let (actual, errors) = exercise_list_directory_contents(input).await;

                let expected = files1
                    .iter()
                    .map(|(entry_path, size)| ((*entry_path).to_owned(), *size))
                    .collect::<BTreeMap<_, _>>();

                assert_eq!(expected, actual);
                assert!(errors.is_empty());
            }
        }

        #[tokio::test]
        async fn test_list_directory_contents_should_send_both_upload_object_jobs_and_errors() {
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

            let input = UploadObjectsInputBuilder::default()
                .bucket("doesnotmatter")
                .source(test_dir.path())
                .recursive(true)
                .build()
                .unwrap();

            let (actual, errors) = exercise_list_directory_contents(input).await;

            let expected = files
                .iter()
                .filter(|(entry_path, _)| !entry_path.starts_with(inaccessible_dir_relative_path))
                .map(|(entry_path, value)| ((*entry_path).to_owned(), *value))
                .collect::<BTreeMap<_, _>>();

            assert_eq!(expected, actual);
            assert_eq!(1, errors.len());
            let walkdir_error = errors[0]
                .source()
                .unwrap()
                .downcast_ref::<walkdir::Error>()
                .expect("should downcast to `walkdir::Error`");
            assert!(walkdir_error
                .path()
                .unwrap()
                .ends_with(inaccessible_dir_relative_path));
            assert_eq!(
                std::io::ErrorKind::PermissionDenied,
                walkdir_error.io_error().unwrap().kind()
            );
        }

        #[tokio::test]
        async fn test_upload_filter() {
            let recursion_root = "test";
            let files = vec![
                ("sample.jpg", 1),
                ("photos/2022/January/sample.jpg", 1),
                ("photos/2022/February/sample1.jpg", 1),
                ("photos/2022/February/sample2.jpg", 1),
                ("photos/2022/February/sample3.jpg", 1),
            ];
            let test_dir = create_test_dir(Some(recursion_root), files.clone(), &[]);

            let input = UploadObjectsInputBuilder::default()
                .bucket("doesnotmatter")
                .source(test_dir.path())
                .recursive(true)
                .filter(|item: &UploadFilterItem<'_>| {
                    !item.path().to_str().unwrap().contains("February") && item.metadata().is_file()
                })
                .build()
                .unwrap();

            let (actual, errors) = exercise_list_directory_contents(input).await;

            let expected = files
                .iter()
                .filter(|(entry_path, _)| !entry_path.contains("February"))
                .map(|(entry_path, value)| ((*entry_path).to_owned(), *value))
                .collect::<BTreeMap<_, _>>();

            assert_eq!(expected, actual);
            assert!(errors.is_empty());
        }
    }

    #[cfg(target_family = "windows")]
    mod window_tests {
        use crate::operation::upload_objects::worker::*;

        #[test]
        fn test_derive_object_key() {
            assert_eq!(
                "2023/Jan/1.png",
                derive_object_key("2023\\Jan\\1.png", None, None).unwrap()
            );
        }
    }
}
