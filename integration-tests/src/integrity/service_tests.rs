/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! What S3 stores and returns as checksums, asserted the same way on the mock and on real S3.
//!
//! The case tables and the transfer manager's validation depend on these answers: which checksum
//! an upload stores, which checksum a HeadObject reports, and which checksum a ranged GET carries.
//! Each test makes its requests with the raw SDK client in checksum mode, and runs on `mock_gp`,
//! `mock_express`, `real_gp` and `real_express`. A mock variant failing where the real one passes
//! means the mock no longer answers as S3 does.

use aws_sdk_s3::types::{ChecksumMode, ChecksumType};

use crate::assertions::{assert_same_content, describe_sdk_error};
use crate::harness::{target_tests, BucketKind, Target, TmTestClient};
use crate::test_util::data::object_bytes;
use crate::test_util::object::{
    checksum_of, stored_checksum, Alg, ChecksumValues, Layout, Object, StoredChecksum, MIB,
};

/// The part size of every multipart fixture, and the range the ranged GETs request.
const PART: u64 = 8 * MIB;

/// The `Range` header value of the object's first [`PART`] bytes.
fn first_part() -> String {
    format!("bytes=0-{}", PART - 1)
}

/// The bytes, and the first checksum algorithm and value, that a GET of `range` of `key` in
/// checksum mode returns.
async fn get_range(t: &TmTestClient, key: &str, range: &str) -> (Vec<u8>, Option<(Alg, String)>) {
    let out = t
        .s3_client()
        .await
        .get_object()
        .bucket(t.bucket())
        .key(t.key(key))
        .range(range)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {range} of {key}: {}", describe_sdk_error(&e)));
    let checksum = out
        .first_checksum()
        .map(|(alg, value)| (alg, value.to_owned()));
    let bytes = out
        .body
        .collect()
        .await
        .unwrap_or_else(|e| panic!("body of GET {range} of {key}: {e:?}"))
        .into_bytes()
        .to_vec();
    (bytes, checksum)
}

/// A PutObject and a multipart upload that send no checksum are each stored with a CRC64NVME
/// FULL_OBJECT checksum of their bytes.
async fn upload_without_checksum_stores_crc64nvme(target: Target) {
    let t = target.connect().await;
    for (key, object, seed) in [
        ("put", Object::single_put(Alg::NoChecksum), 1),
        ("mpu", Object::mpu_without_checksum(), 2),
    ] {
        object.upload(&t, key, seed).await;
        let stored = stored_checksum(&t, key).await;
        let expected = StoredChecksum {
            algorithm: Alg::Crc64Nvme,
            value: checksum_of(Alg::Crc64Nvme, seed, object.len),
            checksum_type: Some(ChecksumType::FullObject),
        };
        assert_eq!(stored, Some(expected), "{key}");
    }
    t.shutdown().await;
}

/// A `Range` GET of the first part of a FULL_OBJECT multipart upload carries no checksum in a
/// general purpose bucket, and part 1's checksum in a directory bucket.
async fn full_object_mpu_first_range_checksum_by_bucket_type(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (
        Object::mpu(Layout::FullObject, Alg::Crc64Nvme).parts(PART),
        3,
    );
    object.upload(&t, "obj", seed).await;

    let (bytes, checksum) = get_range(&t, "obj", &first_part()).await;
    assert_same_content(&object_bytes(seed, PART as usize), &bytes);
    let expected = match target.bucket_kind {
        BucketKind::GeneralPurpose => None,
        BucketKind::Express => Some((Alg::Crc64Nvme, checksum_of(Alg::Crc64Nvme, seed, PART))),
    };
    assert_eq!(checksum, expected);
    t.shutdown().await;
}

/// A HeadObject of a FULL_OBJECT multipart upload reports the CRC64NVME of the object's bytes,
/// with type FULL_OBJECT.
async fn full_object_mpu_head_returns_object_crc(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (Object::mpu(Layout::FullObject, Alg::Crc64Nvme), 4);
    object.upload(&t, "obj", seed).await;

    let stored = stored_checksum(&t, "obj").await;
    assert_eq!(stored, object.expected_checksum(seed));
    let checksum_type = stored.and_then(|s| s.checksum_type);
    assert_eq!(checksum_type, Some(ChecksumType::FullObject));
    t.shutdown().await;
}

/// A `Range` GET of part of a single PUT carries no checksum.
async fn single_put_sub_range_has_no_checksum(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (Object::single_put(Alg::Crc32).len(12 * MIB), 5);
    object.upload(&t, "obj", seed).await;

    let (bytes, checksum) = get_range(&t, "obj", &first_part()).await;
    assert_same_content(&object_bytes(seed, PART as usize), &bytes);
    assert_eq!(checksum, None);
    t.shutdown().await;
}

/// A HeadObject of a single PUT reports the CRC32 of the object's bytes, with type FULL_OBJECT.
async fn single_put_head_returns_object_crc(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (Object::single_put(Alg::Crc32), 6);
    object.upload(&t, "obj", seed).await;

    let stored = stored_checksum(&t, "obj").await;
    assert_eq!(stored, object.expected_checksum(seed));
    let checksum_type = stored.and_then(|s| s.checksum_type);
    assert_eq!(checksum_type, Some(ChecksumType::FullObject));
    t.shutdown().await;
}

/// A `Range` GET, not a `partNumber` GET, whose range is exactly part 1 of a COMPOSITE multipart
/// upload carries part 1's checksum.
async fn composite_aligned_range_returns_part_checksum(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (Object::mpu(Layout::Composite, Alg::Crc32).parts(PART), 7);
    object.upload(&t, "obj", seed).await;

    let (bytes, checksum) = get_range(&t, "obj", &first_part()).await;
    assert_same_content(&object_bytes(seed, PART as usize), &bytes);
    assert_eq!(
        checksum,
        Some((Alg::Crc32, checksum_of(Alg::Crc32, seed, PART)))
    );
    t.shutdown().await;
}

/// A HeadObject of a COMPOSITE multipart upload reports the checksum of its part checksums,
/// suffixed `-<part count>`, with type COMPOSITE.
async fn composite_head_returns_composite_value(target: Target) {
    let t = target.connect().await;
    let (object, seed) = (Object::mpu(Layout::Composite, Alg::Crc32), 8);
    object.upload(&t, "obj", seed).await;

    let stored = stored_checksum(&t, "obj").await.expect("a checksum");
    let parts = object.stored_part_sizes().len();
    assert!(stored.value.ends_with(&format!("-{parts}")), "{stored:?}");
    assert_eq!(stored.checksum_type, Some(ChecksumType::Composite));
    assert_eq!(Some(stored), object.expected_checksum(seed));
    t.shutdown().await;
}

/// A `Range` GET of the first [`PART`] bytes of a 7 MiB multipart upload returns the whole object, with the
/// object's CRC64NVME.
async fn small_mpu_whole_range_returns_object_crc(target: Target) {
    let t = target.connect().await;
    let object = Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
        .len(7 * MIB)
        .parts(5 * MIB);
    let seed = 9;
    object.upload(&t, "obj", seed).await;

    let (bytes, checksum) = get_range(&t, "obj", &first_part()).await;
    assert_same_content(&object_bytes(seed, object.len as usize), &bytes);
    let expected = object.expected_checksum(seed).expect("a checksum");
    assert_eq!(checksum, Some((Alg::Crc64Nvme, expected.value)));
    t.shutdown().await;
}

/// Under a role allowed only what a GET of an SSE-KMS object needs (`s3:GetObject` on the bucket's
/// objects and `kms:Decrypt` on the object's key), a HeadObject in checksum mode succeeds and
/// reports the stored checksum, and a GET in checksum mode returns the object: reading an object's
/// checksum needs no permission beyond reading the object.
///
/// Needs `S3_TM_TEST_KMS_KEY_ARN`, a KMS key the default credentials can encrypt with, and
/// `S3_TM_TEST_GET_ONLY_ROLE_ARN`, a role the default credentials can assume that holds only those
/// two permissions.
#[cfg(e2e_test)]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs KMS CI setup"]
async fn kms_head_needs_no_grant_beyond_get() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{ChecksumAlgorithm, ServerSideEncryption};

    let kms_key = std::env::var("S3_TM_TEST_KMS_KEY_ARN").expect("S3_TM_TEST_KMS_KEY_ARN");
    let role = std::env::var("S3_TM_TEST_GET_ONLY_ROLE_ARN").expect("S3_TM_TEST_GET_ONLY_ROLE_ARN");
    let t = Target::real_gp().connect().await;
    let (key, object, seed) = (t.key("kms"), Object::single_put(Alg::Crc32), 10);
    let data = object_bytes(seed, object.len as usize);
    t.s3()
        .put_object()
        .bucket(t.bucket())
        .key(&key)
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(kms_key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .expect("PutObject with SSE-KMS");

    let credentials = aws_config::sts::AssumeRoleProvider::builder(role)
        .session_name("s3-tm-kms-head")
        .build()
        .await;
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(credentials)
        .load()
        .await;
    let get_only = aws_sdk_s3::Client::new(&config);

    let head = get_only
        .head_object()
        .bucket(t.bucket())
        .key(&key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "HeadObject in checksum mode under the GET-only role: {}",
                describe_sdk_error(&e)
            )
        });
    assert_eq!(
        StoredChecksum::from_head(&head),
        object.expected_checksum(seed)
    );
    let get = get_only
        .get_object()
        .bucket(t.bucket())
        .key(&key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("GetObject in checksum mode under the GET-only role");
    let bytes = get
        .body
        .collect()
        .await
        .expect("GetObject body")
        .into_bytes();
    assert_same_content(&data, &bytes);
}

target_tests! {
    #![cfg_attr(
        s3_tm_tsan,
        ignore = "service behavior cases repeat download_tests' interleavings; see the integrity module"
    )]
    upload_without_checksum_stores_crc64nvme: [mock_gp, mock_express, real_gp, real_express];
    full_object_mpu_first_range_checksum_by_bucket_type: [mock_gp, mock_express, real_gp, real_express];
    full_object_mpu_head_returns_object_crc: [mock_gp, mock_express, real_gp, real_express];
    single_put_sub_range_has_no_checksum: [mock_gp, mock_express, real_gp, real_express];
    single_put_head_returns_object_crc: [mock_gp, mock_express, real_gp, real_express];
    composite_aligned_range_returns_part_checksum: [mock_gp, mock_express, real_gp, real_express];
    composite_head_returns_composite_value: [mock_gp, mock_express, real_gp, real_express];
    small_mpu_whole_range_returns_object_crc: [mock_gp, mock_express, real_gp, real_express];
}
