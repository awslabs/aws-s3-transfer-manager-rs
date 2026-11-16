/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integrity: what S3 stores for an upload, and what an upload S3 rejects leaves behind.
//!
//! The intact tests check that the stored bytes and checksum equal the source's, for a calculated
//! checksum and for a full-object checksum the caller provides. The rejection tests send a part
//! or full-object checksum that doesn't match the bytes, and check that S3's `BadDigest` fails the
//! upload from the request S3 rejected, that the transfer manager aborts a multipart upload once,
//! and that nothing is stored for the key.
//!
//! The checksums are supplied by the client, so every test runs the same assertions on the mock
//! and on real S3, in a general purpose and a directory bucket.

use aws_sdk_s3::types::ChecksumType;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData};
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;

use crate::assertions::describe_error;
use crate::harness::{target_tests, ClientOptions, Target, TmTestClient};
use crate::test_util::data::{fill_object_bytes, object_bytes};
use crate::test_util::object::{
    assert_nothing_stored, assert_stored_bytes, checksum_of, stored_checksum, write_object_file,
    Alg, Layout, Object, PrebuiltParts, MIB,
};
use crate::test_util::recorder::OperationRecorder;

/// The part size and multipart threshold of [`recorded_client`].
const PART_SIZE: u64 = 8 * MIB;

/// The logical key every test uploads to.
const KEY: &str = "obj";

/// A transfer manager client on `t`'s backend that uploads in [`PART_SIZE`] parts above
/// [`PART_SIZE`], and the recorder of every request attempt its S3 client sends.
fn recorded_client(t: &TmTestClient) -> (aws_sdk_s3_transfer_manager::Client, OperationRecorder) {
    let recorder = OperationRecorder::default();
    let client = t.client_with(ClientOptions {
        part_size: Some(PART_SIZE),
        multipart_threshold: Some(PART_SIZE),
        recorder: Some(recorder.clone()),
        ..Default::default()
    });
    (client, recorder)
}

/// Object `seed`'s first `count * part_size` bytes, as parts of `part_size`.
fn parts_of(seed: u32, part_size: u64, count: u64) -> Vec<PartData> {
    (0..count)
        .map(|i| {
            let mut buf = vec![0; part_size as usize];
            fill_object_bytes(seed, i * part_size, &mut buf);
            PartData::new(i + 1, buf)
        })
        .collect()
}

/// Panics unless `result` is S3's `BadDigest` error for `operation`.
fn assert_bad_digest<T: std::fmt::Debug>(
    result: Result<T, aws_sdk_s3_transfer_manager::error::Error>,
    operation: &str,
) {
    let err = result.expect_err("an upload with a mismatched checksum must fail");
    assert_eq!(
        err.operation_name(),
        Some(operation),
        "{}",
        describe_error(&err)
    );
    assert_eq!(err.code(), Some("BadDigest"), "{}", describe_error(&err));
    assert_eq!(err.upload_id(), None, "the upload was left open in S3");
}

/// A multipart upload with a CRC64NVME checksum the transfer manager calculates stores the
/// source's bytes, with the source's CRC64NVME FULL_OBJECT checksum.
async fn mpu_intact(target: Target) {
    const SEED: u32 = 0x7570_0001;
    let t = target.connect().await;
    let object = Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
        .len(20 * MIB + 3)
        .parts(8 * MIB);
    object.upload(&t, KEY, SEED).await;
    assert_stored_bytes(&t, KEY, SEED, object.len).await;
    t.shutdown().await;
}

/// An upload from a file of many parts with the default checksum strategy stores the source's
/// bytes in one part per [`PART_SIZE`], with the source's CRC64NVME FULL_OBJECT checksum.
async fn path_many_parts_intact(target: Target) {
    const SEED: u32 = 0x7570_0002;
    let t = target.connect().await;
    let object = Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
        .len(40 * PART_SIZE + 17)
        .parts(PART_SIZE);
    let source = tempfile::NamedTempFile::new().expect("create upload source");
    write_object_file(source.path(), SEED, object.len);

    let (client, recorder) = recorded_client(&t);
    client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .body(InputStream::from_path(source.path()).expect("open upload source"))
        .initiate()
        .expect("initiate upload")
        .join()
        .await
        .expect("upload");

    let parts = object.stored_part_sizes().len();
    assert_eq!(recorder.count("UploadPart"), parts);
    assert_eq!(t.head_part(KEY, 1).await.parts_count(), Some(parts as i32));
    assert_eq!(
        stored_checksum(&t, KEY).await,
        object.expected_checksum(SEED)
    );
    assert_stored_bytes(&t, KEY, SEED, object.len).await;
    t.shutdown().await;
}

/// A single PUT with a correct caller-provided CRC32 stores that value as the object's CRC32
/// FULL_OBJECT checksum.
async fn provided_full_object_put(target: Target) {
    const SEED: u32 = 0x7570_0003;
    let t = target.connect().await;
    let object = Object::single_put(Alg::Crc32).len(MIB);
    let provided = object.expected_checksum(SEED).expect("a CRC32");

    let (client, recorder) = recorded_client(&t);
    client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .checksum_strategy(ChecksumStrategy::with_crc32(provided.value.clone()))
        .body(InputStream::from(object_bytes(SEED, object.len as usize)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await
        .expect("upload");

    assert_eq!(recorder.count("PutObject"), 1);
    assert_eq!(recorder.count("CreateMultipartUpload"), 0);
    assert_eq!(stored_checksum(&t, KEY).await, Some(provided));
    assert_stored_bytes(&t, KEY, SEED, object.len).await;
    t.shutdown().await;
}

/// A multipart upload with a correct caller-provided CRC32 stores that value as the object's
/// CRC32 FULL_OBJECT checksum.
async fn provided_full_object_mpu(target: Target) {
    const SEED: u32 = 0x7570_0004;
    let t = target.connect().await;
    let object = Object::mpu(Layout::FullObject, Alg::Crc32)
        .len(20 * MIB)
        .parts(PART_SIZE);
    let provided = object.expected_checksum(SEED).expect("a CRC32");
    assert_eq!(provided.checksum_type, Some(ChecksumType::FullObject));

    let (client, recorder) = recorded_client(&t);
    client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .checksum_strategy(ChecksumStrategy::with_crc32(provided.value.clone()))
        .body(InputStream::from(object_bytes(SEED, object.len as usize)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await
        .expect("upload");

    assert_eq!(recorder.count("CreateMultipartUpload"), 1);
    assert_eq!(recorder.count("CompleteMultipartUpload"), 1);
    assert_eq!(stored_checksum(&t, KEY).await, Some(provided));
    assert_stored_bytes(&t, KEY, SEED, object.len).await;
    t.shutdown().await;
}

/// In a six-part upload, a part whose CRC32 was calculated over other bytes fails the upload with
/// `BadDigest` from UploadPart. The upload is never completed, is aborted once, and stores
/// nothing.
async fn part_checksum_mismatch_aborts_upload(target: Target) {
    const SEED: u32 = 0x7570_0005;
    const OTHER_SEED: u32 = 0x7570_1005;
    const PART: u64 = 5 * MIB;
    let t = target.connect().await;
    let mut parts = parts_of(SEED, PART, 6);
    let part_4 = parts
        .remove(3)
        .with_checksum(checksum_of(Alg::Crc32, OTHER_SEED, PART));
    parts.insert(3, part_4);

    let (client, recorder) = recorded_client(&t);
    let result = client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from_part_stream(PrebuiltParts::new(
            parts,
            6 * PART,
        )))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert_bad_digest(result, "UploadPart");
    assert_eq!(recorder.count("CompleteMultipartUpload"), 0);
    assert_eq!(recorder.count("AbortMultipartUpload"), 1);
    assert_nothing_stored(&t, KEY).await;
    t.shutdown().await;
}

/// A `PartStream` whose full-object checksum doesn't match its parts fails the upload with
/// `BadDigest` from CompleteMultipartUpload. The upload is aborted once and stores nothing.
async fn part_stream_full_object_mismatch(target: Target) {
    const SEED: u32 = 0x7570_0006;
    const OTHER_SEED: u32 = 0x7570_1006;
    const PART: u64 = 5 * MIB;
    let t = target.connect().await;
    // The default strategy: CRC64NVME, FULL_OBJECT.
    let stream = PrebuiltParts::new(parts_of(SEED, PART, 3), 3 * PART)
        .full_object_checksum(checksum_of(Alg::Crc64Nvme, OTHER_SEED, 3 * PART));

    let (client, recorder) = recorded_client(&t);
    let result = client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert_bad_digest(result, "CompleteMultipartUpload");
    assert_eq!(recorder.count("UploadPart"), 3);
    assert_eq!(recorder.count("AbortMultipartUpload"), 1);
    assert_nothing_stored(&t, KEY).await;
    t.shutdown().await;
}

/// A single PUT whose caller-provided CRC32 doesn't match its bytes fails with `BadDigest` from
/// PutObject and stores nothing.
async fn provided_full_object_mismatch_put(target: Target) {
    const SEED: u32 = 0x7570_0007;
    const OTHER_SEED: u32 = 0x7570_1007;
    let t = target.connect().await;
    let len = MIB;

    let (client, recorder) = recorded_client(&t);
    let result = client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .checksum_strategy(ChecksumStrategy::with_crc32(checksum_of(
            Alg::Crc32,
            OTHER_SEED,
            len,
        )))
        .body(InputStream::from(object_bytes(SEED, len as usize)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert_bad_digest(result, "PutObject");
    assert_eq!(recorder.count("CreateMultipartUpload"), 0);
    assert_nothing_stored(&t, KEY).await;
    t.shutdown().await;
}

/// A multipart upload whose caller-provided CRC32 doesn't match its bytes fails with `BadDigest`
/// from CompleteMultipartUpload. The upload is aborted once and stores nothing.
async fn provided_full_object_mismatch_mpu(target: Target) {
    const SEED: u32 = 0x7570_0008;
    const OTHER_SEED: u32 = 0x7570_1008;
    let t = target.connect().await;
    let len = 20 * MIB;

    let (client, recorder) = recorded_client(&t);
    let result = client
        .upload()
        .bucket(t.bucket())
        .key(t.key(KEY))
        .checksum_strategy(ChecksumStrategy::with_crc32(checksum_of(
            Alg::Crc32,
            OTHER_SEED,
            len,
        )))
        .body(InputStream::from(object_bytes(SEED, len as usize)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert_bad_digest(result, "CompleteMultipartUpload");
    assert_eq!(recorder.count("CreateMultipartUpload"), 1);
    assert_eq!(recorder.count("AbortMultipartUpload"), 1);
    assert_nothing_stored(&t, KEY).await;
    t.shutdown().await;
}

target_tests! {
    mpu_intact: [mock_gp, mock_express, real_gp, real_express];
    path_many_parts_intact: [mock_gp, mock_express, real_gp, real_express];
    provided_full_object_put: [mock_gp, mock_express, real_gp, real_express];
    provided_full_object_mpu: [mock_gp, mock_express, real_gp, real_express];
    part_checksum_mismatch_aborts_upload: [mock_gp, mock_express, real_gp, real_express];
    part_stream_full_object_mismatch: [mock_gp, mock_express, real_gp, real_express];
    provided_full_object_mismatch_put: [mock_gp, mock_express, real_gp, real_express];
    provided_full_object_mismatch_mpu: [mock_gp, mock_express, real_gp, real_express];
}
