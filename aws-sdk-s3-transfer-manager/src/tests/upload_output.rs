/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::model::builders::UploadOutputBuilder;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::types::{ChecksumType, RequestCharged, ServerSideEncryption};

#[test]
fn test_update_from_complete_mpu() {
    let complete_mpu_resp = CompleteMultipartUploadOutput::builder()
        .bucket_key_enabled(true)
        .checksum_crc32("AAAAAA==")
        .checksum_type(ChecksumType::FullObject)
        .e_tag("test-etag")
        .expiration("test-expiration")
        .request_charged(RequestCharged::Requester)
        .ssekms_key_id("test-kms-key")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .version_id("test-version")
        .build();

    let sut = crate::sdk_v1::update_upload_output_from_complete_multipart_upload(
        UploadOutputBuilder::default(),
        &complete_mpu_resp,
    );

    assert_eq!(complete_mpu_resp.bucket_key_enabled, sut.bucket_key_enabled);
    assert_eq!(complete_mpu_resp.checksum_crc32, sut.checksum_crc32);
    assert_eq!(complete_mpu_resp.checksum_crc32_c, sut.checksum_crc32_c);
    assert_eq!(
        complete_mpu_resp.checksum_crc64_nvme,
        sut.checksum_crc64_nvme
    );
    assert_eq!(complete_mpu_resp.checksum_sha1, sut.checksum_sha1);
    assert_eq!(complete_mpu_resp.checksum_sha256, sut.checksum_sha256);
    assert_eq!(
        complete_mpu_resp.checksum_type.as_ref().map(|v| v.as_str()),
        sut.checksum_type.as_ref().map(|v| v.as_str())
    );
    assert_eq!(complete_mpu_resp.e_tag, sut.e_tag);
    assert_eq!(complete_mpu_resp.expiration, sut.expiration);
    assert_eq!(
        complete_mpu_resp
            .request_charged
            .as_ref()
            .map(|v| v.as_str()),
        sut.request_charged.as_ref().map(|v| v.as_str())
    );
    assert_eq!(complete_mpu_resp.ssekms_key_id, sut.sse_kms_key_id);
    assert_eq!(
        complete_mpu_resp
            .server_side_encryption
            .as_ref()
            .map(|v| v.as_str()),
        sut.server_side_encryption.as_ref().map(|v| v.as_str())
    );
    assert_eq!(complete_mpu_resp.version_id, sut.version_id);
}

#[test]
fn test_from_put_object_output() {
    let put_object_output = PutObjectOutput::builder()
        .bucket_key_enabled(true)
        .checksum_crc32("AAAAAA==")
        .checksum_type(ChecksumType::FullObject)
        .e_tag("test-etag")
        .expiration("test-expiration")
        .request_charged(RequestCharged::Requester)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .sse_customer_algorithm("AES256")
        .sse_customer_key_md5("test-md5")
        .ssekms_encryption_context("test-context")
        .ssekms_key_id("test-kms-key")
        .version_id("test-version")
        .build();

    let sut = crate::sdk_v1::upload_output_from_put_object(&put_object_output);

    assert_eq!(put_object_output.bucket_key_enabled, sut.bucket_key_enabled);
    assert_eq!(put_object_output.checksum_crc32, sut.checksum_crc32);
    assert_eq!(put_object_output.checksum_crc32_c, sut.checksum_crc32_c);
    assert_eq!(
        put_object_output.checksum_crc64_nvme,
        sut.checksum_crc64_nvme
    );
    assert_eq!(put_object_output.checksum_sha1, sut.checksum_sha1);
    assert_eq!(put_object_output.checksum_sha256, sut.checksum_sha256);
    assert_eq!(
        put_object_output.checksum_type.as_ref().map(|v| v.as_str()),
        sut.checksum_type.as_ref().map(|v| v.as_str())
    );
    assert_eq!(put_object_output.e_tag, sut.e_tag);
    assert_eq!(put_object_output.expiration, sut.expiration);
    assert_eq!(
        put_object_output
            .request_charged
            .as_ref()
            .map(|v| v.as_str()),
        sut.request_charged.as_ref().map(|v| v.as_str())
    );
    assert_eq!(
        put_object_output
            .server_side_encryption
            .as_ref()
            .map(|v| v.as_str()),
        sut.server_side_encryption.as_ref().map(|v| v.as_str())
    );
    assert_eq!(
        put_object_output.sse_customer_algorithm,
        sut.sse_customer_algorithm
    );
    assert_eq!(
        put_object_output.sse_customer_key_md5,
        sut.sse_customer_key_md5
    );
    assert_eq!(
        put_object_output.ssekms_encryption_context,
        sut.sse_kms_encryption_context
    );
    assert_eq!(put_object_output.ssekms_key_id, sut.sse_kms_key_id);
    assert_eq!(None, sut.upload_id);
    assert_eq!(put_object_output.version_id, sut.version_id);
}
