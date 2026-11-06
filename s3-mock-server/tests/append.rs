// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Appends: PutObject with `x-amz-write-offset-bytes`, served in directory buckets.

mod common;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::RequestChecksumCalculation;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
};
use common::{assert_failure, crc32, crc32c, failure, md5_etag, Fixture, Result};
use s3_mock_server::{FaultType, Occurrence, Operation};

#[allow(clippy::result_large_err)]
async fn append(
    f: &Fixture,
    key: &str,
    offset: i64,
    content: &'static [u8],
) -> std::result::Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    SdkError<aws_sdk_s3::operation::put_object::PutObjectError, HttpResponse>,
> {
    f.s3.put_object()
        .bucket(&f.bucket)
        .key(key)
        .write_offset_bytes(offset)
        .body(ByteStream::from_static(content))
        .send()
        .await
}

/// An append to a general purpose bucket fails with 501 NotImplemented, as in S3, and changes
/// nothing; the request is still counted.
#[tokio::test]
async fn test_append_not_implemented_in_general_purpose_buckets() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    let fail = failure(append(&f, "a", 5, b" world").await);
    assert_failure(&fail, 501, "NotImplemented");
    let fail = failure(append(&f, "b", 0, b"new").await);
    assert_failure(&fail, 501, "NotImplemented");
    assert_eq!(f.content("a").await, b"hello");
    assert!(!f.exists("b").await);
    assert_eq!(f.server.request_count(Operation::PutObject), 3);
    f.shutdown().await;
    Ok(())
}

/// An append at offset 0 creates the object; an append at its size extends it. The ETag and the
/// full-object checksum cover the combined content, and an object written by PutObject can be
/// appended to.
#[tokio::test]
async fn test_append_create_and_extend() -> Result {
    let f = Fixture::directory().await;

    // Offset 0 on a missing key creates the object.
    let out = append(&f, "a", 0, b"hello").await?;
    assert_eq!(out.e_tag.as_deref(), Some(md5_etag(b"hello").as_str()));
    assert_eq!(f.content("a").await, b"hello");

    let out = append(&f, "a", 5, b" world").await?;
    let expected = b"hello world";
    assert_eq!(f.content("a").await, expected);
    assert_eq!(out.e_tag.as_deref(), Some(md5_etag(expected).as_str()));
    // The SDK sends a CRC32 by default; the full-object checksum covers the combined content.
    assert_eq!(
        out.checksum_crc32.as_deref(),
        Some(crc32(expected).as_str())
    );

    let head =
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("a")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
    assert_eq!(head.content_length, Some(expected.len() as i64));
    assert_eq!(head.e_tag.as_deref(), Some(md5_etag(expected).as_str()));
    assert_eq!(
        head.checksum_crc32.as_deref(),
        Some(crc32(expected).as_str())
    );

    // A GET validates the stored checksum against the content.
    let body =
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("a")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
    assert_eq!(&body[..], expected);

    // Appending to an object written by a regular PUT.
    f.put("b", "abc").await;
    append(&f, "b", 3, b"def").await?;
    assert_eq!(f.content("b").await, b"abcdef");
    f.shutdown().await;
    Ok(())
}

/// An append at an offset other than the object's size fails with InvalidWriteOffset, one whose
/// checksum does not match its data fails with BadDigest, and one at a non-zero offset to a missing
/// key fails with NoSuchKey; none of them changes storage.
#[tokio::test]
async fn test_append_invalid_offset() -> Result {
    let f = Fixture::directory().await;
    append(&f, "a", 0, b"hello").await?;

    for offset in [0, 4, 6, 100] {
        let fail = failure(append(&f, "a", offset, b"x").await);
        assert_failure(&fail, 400, "InvalidWriteOffset");
    }
    assert_eq!(f.content("a").await, b"hello");

    // A checksum that does not match the appended data.
    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .write_offset_bytes(5)
            .checksum_crc32("AAAAAA==")
            .body(ByteStream::from_static(b" world"))
            .send()
            .await,
    );
    assert_failure(&fail, 400, "BadDigest");
    assert_eq!(f.content("a").await, b"hello");

    // A non-zero offset on a missing key.
    let fail = failure(append(&f, "missing", 5, b"x").await);
    assert_failure(&fail, 404, "NoSuchKey");
    assert!(!f.exists("missing").await);
    f.shutdown().await;
    Ok(())
}

/// Under `WithholdResponseHeaders`, an append is applied but never answered, so a client that
/// retries it after a timeout gets InvalidWriteOffset.
#[tokio::test]
async fn test_append_withhold_response_headers() -> Result {
    let f = Fixture::directory().await;
    append(&f, "a", 0, b"hello").await?;
    f.server.insert_fault(
        &f.bucket,
        "a",
        FaultType::WithholdResponseHeaders,
        0,
        Occurrence::Once,
    );
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        append(&f, "a", 5, b" world"),
    )
    .await;
    assert!(outcome.is_err(), "the append must not be answered");
    assert_eq!(f.content("a").await, b"hello world");
    let fail = failure(append(&f, "a", 5, b" world").await);
    assert_failure(&fail, 400, "InvalidWriteOffset");
    f.shutdown().await;
    Ok(())
}

/// An empty append to a non-empty object fails with InvalidArgument and S3's message; an empty
/// append at offset 0 creates an empty object.
#[tokio::test]
async fn test_append_empty_body() -> Result {
    let f = Fixture::directory().await;
    append(&f, "a", 0, b"hello").await?;
    let fail = failure(append(&f, "a", 5, b"").await);
    assert_failure(&fail, 400, "InvalidArgument");
    assert_eq!(
        fail.message.as_deref(),
        Some("Request body cannot be empty when 'write offset' is specified")
    );
    assert_eq!(f.content("a").await, b"hello");

    // Creating an empty object with an append is allowed.
    append(&f, "empty", 0, b"").await?;
    assert_eq!(f.content("empty").await, b"");
    f.shutdown().await;
    Ok(())
}

/// An append whose checksum algorithm the object does not store fails with InvalidRequest and the
/// message clients match on; an append with the object's algorithm updates its checksum.
#[tokio::test]
async fn test_append_checksum_algorithm_mismatch() -> Result {
    let f = Fixture::directory().await;
    f.s3.put_object()
        .bucket(&f.bucket)
        .key("a")
        .write_offset_bytes(0)
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await?;

    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .write_offset_bytes(5)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from_static(b" world"))
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidRequest");
    assert!(
        fail.message
            .as_deref()
            .unwrap_or_default()
            .contains("Checksum Type mismatch"),
        "{fail:?}"
    );
    assert_eq!(f.content("a").await, b"hello");

    let out =
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .write_offset_bytes(5)
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .body(ByteStream::from_static(b" world"))
            .send()
            .await?;
    assert_eq!(
        out.checksum_crc32_c.as_deref(),
        Some(crc32c(b"hello world").as_str())
    );
    f.shutdown().await;
    Ok(())
}

/// An append with `If-Match` extends the object only while its ETag matches, and on a missing
/// key fails with NoSuchKey without creating it, as in S3.
#[tokio::test]
async fn test_append_if_match() -> Result {
    let f = Fixture::directory().await;
    let etag = append(&f, "a", 0, b"hello").await?.e_tag.unwrap();

    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .write_offset_bytes(5)
            .if_match("\"nope\"")
            .body(ByteStream::from_static(b" world"))
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    assert_eq!(f.content("a").await, b"hello");

    f.s3.put_object()
        .bucket(&f.bucket)
        .key("a")
        .write_offset_bytes(5)
        .if_match(&etag)
        .body(ByteStream::from_static(b" world"))
        .send()
        .await?;
    assert_eq!(f.content("a").await, b"hello world");

    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("missing")
            .write_offset_bytes(0)
            .if_match(&etag)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    assert!(!f.server.object_exists(&f.bucket, "missing").await?);
    f.shutdown().await;
    Ok(())
}

/// An append without a checksum is checked as a CRC64NVME one, S3's default: it fails with the
/// checksum type mismatch, in S3's words, on an object that stores another algorithm, and extends
/// an object that stores CRC64NVME.
#[tokio::test]
async fn test_append_without_checksum() -> Result {
    let f = Fixture::directory().await;
    let no_checksum = aws_sdk_s3::Client::from_conf(
        f.s3.config()
            .to_builder()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build(),
    );
    f.s3.put_object()
        .bucket(&f.bucket)
        .key("crc32")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await?;
    let fail = failure(
        no_checksum
            .put_object()
            .bucket(&f.bucket)
            .key("crc32")
            .write_offset_bytes(5)
            .body(ByteStream::from_static(b" world"))
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidRequest");
    assert_eq!(
        fail.message.as_deref(),
        Some("Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: crc64nvme.")
    );
    assert_eq!(f.content("crc32").await, b"hello");

    f.server
        .add_object(&f.bucket, "default", "hello", None)
        .await?;
    no_checksum
        .put_object()
        .bucket(&f.bucket)
        .key("default")
        .write_offset_bytes(5)
        .body(ByteStream::from_static(b" world"))
        .send()
        .await?;
    assert_eq!(f.content("default").await, b"hello world");
    f.shutdown().await;
    Ok(())
}

/// An append with `If-None-Match: *` creates a missing key at offset 0, fails with NoSuchKey for
/// a missing key at another offset, and fails with 412 for an existing key, leaving it
/// unchanged, as in S3.
#[tokio::test]
async fn test_append_if_none_match() -> Result {
    let f = Fixture::directory().await;
    let append_if_none_match = |key: &'static str, offset: i64| {
        f.s3.put_object()
            .bucket(&f.bucket)
            .key(key)
            .write_offset_bytes(offset)
            .if_none_match("*")
            .body(ByteStream::from_static(b"hello"))
            .send()
    };
    append_if_none_match("new", 0).await?;
    assert_eq!(f.content("new").await, b"hello");

    let fail = failure(append_if_none_match("missing", 5).await);
    assert_failure(&fail, 404, "NoSuchKey");
    assert!(!f.exists("missing").await);

    let fail = failure(append_if_none_match("new", 5).await);
    assert_failure(&fail, 412, "PreconditionFailed");
    assert_eq!(f.content("new").await, b"hello");
    f.shutdown().await;
    Ok(())
}

/// Upload `parts` to `key` in a multipart upload with `algorithm` and `checksum_type`.
async fn multipart(
    f: &Fixture,
    key: &str,
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
    parts: &[Vec<u8>],
) -> Result {
    let upload_id =
        f.s3.create_multipart_upload()
            .bucket(&f.bucket)
            .key(key)
            .checksum_algorithm(algorithm.clone())
            .checksum_type(checksum_type)
            .send()
            .await?
            .upload_id
            .unwrap();
    let mut completed = Vec::new();
    for (index, content) in parts.iter().enumerate() {
        let part_number = index as i32 + 1;
        let out =
            f.s3.upload_part()
                .bucket(&f.bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .checksum_algorithm(algorithm.clone())
                .body(ByteStream::from(content.clone()))
                .send()
                .await?;
        completed.push(
            CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(out.e_tag)
                .set_checksum_crc32(out.checksum_crc32)
                .set_checksum_sha1(out.checksum_sha1)
                .set_checksum_sha256(out.checksum_sha256)
                .build(),
        );
    }
    f.s3.complete_multipart_upload()
        .bucket(&f.bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

/// After an append, an object has no parts (part 1 is the whole object) and reports the checksum
/// S3 reports: a full-object CRC
/// of the combined content, also for a multipart object with a composite CRC, and for SHA-1 and
/// SHA-256 the composite of the checksums of the object's segments (its parts or the data it was
/// created with, then each append). The expected values are those S3 returned for the same
/// requests.
#[tokio::test]
async fn test_append_checksums() -> Result {
    let f = Fixture::directory().await;
    let five_mib = 5 * 1024 * 1024;
    let parts = [vec![b'b'; five_mib], vec![b'c'; 1000]];
    let cases = [
        (
            "mpu-crc32",
            Some((ChecksumAlgorithm::Crc32, ChecksumType::Composite)),
            ChecksumAlgorithm::Crc32,
        ),
        (
            "mpu-sha256",
            Some((ChecksumAlgorithm::Sha256, ChecksumType::Composite)),
            ChecksumAlgorithm::Sha256,
        ),
        ("put-sha256", None, ChecksumAlgorithm::Sha256),
        ("put-sha1", None, ChecksumAlgorithm::Sha1),
    ];
    for (key, upload, algorithm) in &cases {
        match upload {
            Some((upload_algorithm, checksum_type)) => {
                multipart(
                    &f,
                    key,
                    upload_algorithm.clone(),
                    checksum_type.clone(),
                    &parts,
                )
                .await?
            }
            None => {
                f.s3.put_object()
                    .bucket(&f.bucket)
                    .key(*key)
                    .checksum_algorithm(algorithm.clone())
                    .body(ByteStream::from(vec![b'a'; 1000]))
                    .send()
                    .await?;
            }
        }
        let size =
            f.s3.head_object()
                .bucket(&f.bucket)
                .key(*key)
                .send()
                .await?
                .content_length
                .unwrap();
        f.s3.put_object()
            .bucket(&f.bucket)
            .key(*key)
            .write_offset_bytes(size)
            .checksum_algorithm(algorithm.clone())
            .body(ByteStream::from(vec![b'z'; 100]))
            .send()
            .await?;
    }
    let head = |key: &'static str| {
        f.s3.head_object()
            .bucket(&f.bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
    };
    let crc = head("mpu-crc32").await?;
    assert_eq!(crc.checksum_type, Some(ChecksumType::FullObject));
    assert_eq!(crc.checksum_crc32.as_deref(), Some("ciHECQ=="));
    let sha256 = head("mpu-sha256").await?;
    assert_eq!(sha256.checksum_type, Some(ChecksumType::Composite));
    assert_eq!(
        sha256.checksum_sha256.as_deref(),
        Some("b7HLUo8QAnwFK4DIXnlLKFFPnu+afz5HS/dHK7c58MA=-3")
    );
    let sha256 = head("put-sha256").await?;
    assert_eq!(sha256.checksum_type, Some(ChecksumType::Composite));
    assert_eq!(
        sha256.checksum_sha256.as_deref(),
        Some("HSc94g9LKsl2XRemNpgrLusycjCkTreJOe8jrUPXLIw=-2")
    );
    assert_eq!(
        head("put-sha1").await?.checksum_sha1.as_deref(),
        Some("piHrXKDKBdtKVL/+fJpnoFbRB/Y=-2")
    );
    for (key, _, _) in &cases {
        let whole =
            f.s3.head_object()
                .bucket(&f.bucket)
                .key(*key)
                .send()
                .await?;
        let part =
            f.s3.head_object()
                .bucket(&f.bucket)
                .key(*key)
                .part_number(1)
                .send()
                .await?;
        assert_eq!(part.content_length, whole.content_length, "{key}");
        assert_eq!(part.parts_count, None, "{key}");
    }

    // A further append adds a segment.
    let size = head("put-sha256").await?.content_length.unwrap();
    f.s3.put_object()
        .bucket(&f.bucket)
        .key("put-sha256")
        .write_offset_bytes(size)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from_static(b"more"))
        .send()
        .await?;
    use sha2::Digest;
    let segments: Vec<String> = [&[b'a'; 1000][..], &[b'z'; 100][..], b"more"]
        .iter()
        .map(|segment| {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                sha2::Sha256::digest(segment),
            )
        })
        .collect();
    assert_eq!(
        head("put-sha256").await?.checksum_sha256,
        Some(s3_mock_server::composite_checksum(
            &segments,
            ChecksumAlgorithm::Sha256
        ))
    );
    f.shutdown().await;
    Ok(())
}

/// An object written in one request has one part, and each append adds one. An append past 10,000
/// parts fails with 400 TooManyParts and leaves the object as it was.
#[tokio::test]
async fn test_append_part_limit() -> Result {
    let f = Fixture::directory().await;
    for offset in 0..10_000 {
        append(&f, "limit", offset, b"x").await?;
    }
    let fail = failure(append(&f, "limit", 10_000, b"x").await);
    assert_failure(&fail, 400, "TooManyParts");
    assert_eq!(f.content("limit").await, vec![b'x'; 10_000]);
    f.shutdown().await;
    Ok(())
}
