// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! ListObjectsV2: pagination with and without a delimiter, and the attributes of listed objects.

mod common;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, OptionalObjectAttributes, StorageClass};
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::RestoreStatus;

/// The keys and common prefixes of every page of a listing.
async fn list_pages(
    f: &Fixture,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: i32,
    start_after: Option<&str>,
) -> Result<Vec<(Vec<String>, Vec<String>)>> {
    let mut pages = Vec::new();
    let mut token = None;
    loop {
        let out =
            f.s3.list_objects_v2()
                .bucket(&f.bucket)
                .set_prefix(prefix.map(str::to_owned))
                .set_delimiter(delimiter.map(str::to_owned))
                .set_start_after(start_after.map(str::to_owned))
                .max_keys(max_keys)
                .set_continuation_token(token)
                .send()
                .await?;
        let keys: Vec<_> = out
            .contents()
            .iter()
            .map(|o| o.key.clone().unwrap())
            .collect();
        let prefixes: Vec<_> = out
            .common_prefixes()
            .iter()
            .map(|p| p.prefix.clone().unwrap())
            .collect();
        assert!(keys.len() + prefixes.len() <= max_keys as usize);
        assert_eq!(out.key_count, Some((keys.len() + prefixes.len()) as i32));
        pages.push((keys, prefixes));
        if out.is_truncated == Some(true) {
            token = Some(out.next_continuation_token.expect("continuation token"));
        } else {
            assert!(out.next_continuation_token.is_none());
            break;
        }
        assert!(pages.len() < 1000, "listing does not terminate");
    }
    Ok(pages)
}

/// Flatten pages into one sequence of entries, with common prefixes marked by a trailing `/`.
fn entries(pages: &[(Vec<String>, Vec<String>)]) -> Vec<String> {
    let mut all = Vec::new();
    for (keys, prefixes) in pages {
        let mut page: Vec<_> = keys.iter().chain(prefixes).cloned().collect();
        page.sort();
        all.extend(page);
    }
    all
}

const LIST_KEYS: &[&str] = &[
    "a/1", "a/2", "a/3", "b", "c/1", "c/2", "c/x/1", "d", "e/x/1", "e/y", "f",
];

async fn seed_list_keys(f: &Fixture) -> Result {
    for key in LIST_KEYS {
        f.server.add_object(&f.bucket, key, "x", None).await?;
    }
    Ok(())
}

/// An empty `delimiter` lists like no delimiter: every key, and no common prefixes.
#[tokio::test]
async fn test_list_empty_delimiter_is_no_delimiter() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    let out =
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .delimiter("")
            .send()
            .await?;
    let keys: Vec<_> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, LIST_KEYS);
    assert!(out.common_prefixes().is_empty());
    f.shutdown().await;
    Ok(())
}

/// With a delimiter, every page holds at most `max-keys` entries, where all the keys of a
/// common prefix count as one entry, and no common prefix is split across pages or repeated, at
/// the top level and within a prefix. Fails if pages split or repeat a common prefix.
#[tokio::test]
async fn test_list_pagination_with_delimiter() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    let expected = ["a/", "b", "c/", "d", "e/", "f"];
    for max_keys in 1..=7 {
        let pages = list_pages(&f, None, Some("/"), max_keys, None).await?;
        assert_eq!(entries(&pages), expected, "max_keys={max_keys}");
        assert_eq!(
            pages.len(),
            expected.len().div_ceil(max_keys as usize),
            "max_keys={max_keys}"
        );
    }

    // Within a prefix.
    for max_keys in 1..=4 {
        let pages = list_pages(&f, Some("c/"), Some("/"), max_keys, None).await?;
        assert_eq!(
            entries(&pages),
            ["c/1", "c/2", "c/x/"],
            "max_keys={max_keys}"
        );
        let pages = list_pages(&f, Some("e"), Some("/"), max_keys, None).await?;
        assert_eq!(entries(&pages), ["e/"], "max_keys={max_keys}");
    }
    f.shutdown().await;
    Ok(())
}

/// Without a delimiter, the pages list every key once, in order.
#[tokio::test]
async fn test_list_pagination_without_delimiter() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    for max_keys in [1, 2, 3, 5, 1000] {
        let pages = list_pages(&f, None, None, max_keys, None).await?;
        assert_eq!(entries(&pages), LIST_KEYS, "max_keys={max_keys}");
    }
    let pages = list_pages(&f, Some("c/"), None, 2, None).await?;
    assert_eq!(entries(&pages), ["c/1", "c/2", "c/x/1"]);
    f.shutdown().await;
    Ok(())
}

/// `start-after` lists the entries after it: a common prefix that sorts before it is skipped
/// with all its keys, even those that sort after it, as in S3.
#[tokio::test]
async fn test_list_start_after() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    for max_keys in [1, 2, 1000] {
        let pages = list_pages(&f, None, None, max_keys, Some("c/2")).await?;
        assert_eq!(
            entries(&pages),
            ["c/x/1", "d", "e/x/1", "e/y", "f"],
            "max_keys={max_keys}"
        );
        let pages = list_pages(&f, None, Some("/"), max_keys, Some("b")).await?;
        assert_eq!(
            entries(&pages),
            ["c/", "d", "e/", "f"],
            "max_keys={max_keys}"
        );
        // `a/` sorts before `a/1`, so the keys of `a/` after `a/1` are skipped with it.
        let pages = list_pages(&f, None, Some("/"), max_keys, Some("a/1")).await?;
        assert_eq!(
            entries(&pages),
            ["b", "c/", "d", "e/", "f"],
            "max_keys={max_keys}"
        );
        let pages = list_pages(&f, None, Some("/"), max_keys, Some("a")).await?;
        assert_eq!(
            entries(&pages),
            ["a/", "b", "c/", "d", "e/", "f"],
            "max_keys={max_keys}"
        );
        let pages = list_pages(&f, None, None, max_keys, Some("z")).await?;
        assert_eq!(entries(&pages), Vec::<String>::new());
    }
    f.shutdown().await;
    Ok(())
}

/// A listing of a missing bucket fails with NoSuchBucket, and an invalid continuation token with
/// InvalidArgument.
#[tokio::test]
async fn test_list_missing_bucket_and_bad_token() -> Result {
    let f = Fixture::new().await;
    let fail = failure(f.s3.list_objects_v2().bucket("missing-bucket").send().await);
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .continuation_token("%%%not-base64")
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidArgument");
    f.shutdown().await;
    Ok(())
}

/// Listed objects report their checksum algorithms and storage class, and their restore status
/// only when `OptionalObjectAttributes` requests it, as in S3.
#[tokio::test]
async fn test_list_object_attributes() -> Result {
    let f = Fixture::new().await;
    f.s3.put_object()
        .bucket(&f.bucket)
        .key("crc32c")
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await?;
    f.server.add_object(&f.bucket, "default", "x", None).await?;
    for key in ["in-progress", "restored"] {
        f.s3.put_object()
            .bucket(&f.bucket)
            .key(key)
            .storage_class(StorageClass::Glacier)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await?;
    }
    let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000);
    f.server
        .set_restore_status(&f.bucket, "in-progress", Some(RestoreStatus::InProgress))
        .await?;
    f.server
        .set_restore_status(
            &f.bucket,
            "restored",
            Some(RestoreStatus::Restored { expiry }),
        )
        .await?;

    let out = f.s3.list_objects_v2().bucket(&f.bucket).send().await?;
    let listed: HashMap<_, _> = out
        .contents()
        .iter()
        .map(|o| {
            (
                o.key().unwrap().to_owned(),
                (o.checksum_algorithm().to_vec(), o.storage_class.clone()),
            )
        })
        .collect();
    // An object without a storage class is listed as STANDARD, as S3 lists it.
    let standard = Some(aws_sdk_s3::types::ObjectStorageClass::Standard);
    assert_eq!(
        listed["crc32c"],
        (vec![ChecksumAlgorithm::Crc32C], standard.clone())
    );
    assert_eq!(
        listed["default"],
        (vec![ChecksumAlgorithm::Crc64Nvme], standard)
    );
    assert_eq!(
        listed["restored"].1,
        Some(aws_sdk_s3::types::ObjectStorageClass::Glacier)
    );
    assert!(out.contents().iter().all(|o| o.restore_status.is_none()));

    let out =
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .optional_object_attributes(OptionalObjectAttributes::RestoreStatus)
            .send()
            .await?;
    let statuses: HashMap<_, _> = out
        .contents()
        .iter()
        .map(|o| {
            (
                o.key().unwrap().to_owned(),
                o.restore_status.as_ref().map(|r| {
                    (
                        r.is_restore_in_progress,
                        r.restore_expiry_date.map(|d| d.secs()),
                    )
                }),
            )
        })
        .collect();
    assert_eq!(
        statuses,
        HashMap::from([
            ("crc32c".to_owned(), None),
            ("default".to_owned(), None),
            ("in-progress".to_owned(), Some((Some(true), None))),
            (
                "restored".to_owned(),
                Some((Some(false), Some(2_000_000_000)))
            ),
        ])
    );
    f.shutdown().await;
    Ok(())
}

/// `max-keys=0` returns an empty, final page.
#[tokio::test]
async fn test_list_max_keys_zero() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    let out =
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .max_keys(0)
            .send()
            .await?;
    assert!(out.contents().is_empty());
    assert_eq!(out.key_count, Some(0));
    assert_eq!(out.is_truncated, Some(false));
    assert!(out.next_continuation_token.is_none());
    f.shutdown().await;
    Ok(())
}

/// With a continuation token, `start-after` is ignored, as in S3, even when it sorts after the
/// token; the response echoes the token and, as S3's does, not `start-after`.
#[tokio::test]
async fn test_list_start_after_ignored_with_token() -> Result {
    let f = Fixture::new().await;
    seed_list_keys(&f).await?;
    let first =
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .max_keys(1)
            .send()
            .await?;
    let token = first.next_continuation_token.unwrap();
    let out =
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .max_keys(1)
            .continuation_token(&token)
            .start_after("e")
            .send()
            .await?;
    assert_eq!(out.contents()[0].key(), Some("a/2"));
    assert_eq!(out.continuation_token.as_deref(), Some(token.as_str()));
    assert_eq!(out.start_after, None);
    f.shutdown().await;
    Ok(())
}

/// A delimiter of several characters rolls keys up at its first occurrence after the prefix.
#[tokio::test]
async fn test_list_multi_character_delimiter() -> Result {
    let f = Fixture::new().await;
    for key in ["a--b--c", "a--d", "a-e", "f"] {
        f.server.add_object(&f.bucket, key, "x", None).await?;
    }
    for max_keys in [1, 2, 1000] {
        let pages = list_pages(&f, None, Some("--"), max_keys, None).await?;
        assert_eq!(entries(&pages), ["a--", "a-e", "f"], "max_keys={max_keys}");
    }
    f.shutdown().await;
    Ok(())
}
