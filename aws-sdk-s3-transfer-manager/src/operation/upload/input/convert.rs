/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::UploadInput;
use aws_sdk_s3::operation::{
    abort_multipart_upload::builders::AbortMultipartUploadFluentBuilder,
    complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder,
    create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
    put_object::builders::PutObjectFluentBuilder, upload_part::builders::UploadPartFluentBuilder,
};
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};

/// Copy fields from UploadInput to `PutObjectFluentBuilder`
pub(crate) fn copy_fields_to_put_object_request(
    upload_input: &UploadInput,
    put_object_builder: PutObjectFluentBuilder,
) -> PutObjectFluentBuilder {
    let mut put_object_builder = put_object_builder
        .set_acl(upload_input.acl.clone())
        .set_bucket(upload_input.bucket.clone())
        .set_bucket_key_enabled(upload_input.bucket_key_enabled)
        .set_cache_control(upload_input.cache_control.clone())
        .set_content_disposition(upload_input.content_disposition.clone())
        .set_content_encoding(upload_input.content_encoding.clone())
        .set_content_language(upload_input.content_language.clone())
        .set_content_md5(upload_input.content_md5.clone())
        .set_content_type(upload_input.content_type.clone())
        .set_expected_bucket_owner(upload_input.expected_bucket_owner.clone())
        .set_expires(upload_input.expires)
        .set_grant_full_control(upload_input.grant_full_control.clone())
        .set_grant_read(upload_input.grant_read.clone())
        .set_grant_read_acp(upload_input.grant_read_acp.clone())
        .set_grant_write_acp(upload_input.grant_write_acp.clone())
        .set_key(upload_input.key.clone())
        .set_metadata(upload_input.metadata.clone())
        .set_object_lock_legal_hold_status(upload_input.object_lock_legal_hold_status.clone())
        .set_object_lock_mode(upload_input.object_lock_mode.clone())
        .set_object_lock_retain_until_date(upload_input.object_lock_retain_until_date)
        .set_request_payer(upload_input.request_payer.clone())
        .set_server_side_encryption(upload_input.server_side_encryption.clone())
        .set_sse_customer_algorithm(upload_input.sse_customer_algorithm.clone())
        .set_sse_customer_key(upload_input.sse_customer_key.clone())
        .set_sse_customer_key_md5(upload_input.sse_customer_key_md5.clone())
        .set_ssekms_encryption_context(upload_input.sse_kms_encryption_context.clone())
        .set_ssekms_key_id(upload_input.sse_kms_key_id.clone())
        .set_storage_class(upload_input.storage_class.clone())
        .set_tagging(upload_input.tagging.clone())
        .set_website_redirect_location(upload_input.website_redirect_location.clone());

    if let Some(checksum_strategy) = &upload_input.checksum_strategy {
        if let Some(value) = checksum_strategy.full_object_checksum() {
            // We have the full-object checksum value, so set it
            put_object_builder = match checksum_strategy.algorithm() {
                ChecksumAlgorithm::Crc32 => put_object_builder.checksum_crc32(value),
                ChecksumAlgorithm::Crc32C => put_object_builder.checksum_crc32_c(value),
                ChecksumAlgorithm::Crc64Nvme => put_object_builder.checksum_crc64_nvme(value),
                ChecksumAlgorithm::Sha1 => put_object_builder.checksum_sha1(value),
                ChecksumAlgorithm::Sha256 => put_object_builder.checksum_sha256(value),
                algo => unreachable!("unexpected algorithm `{algo}` for full object checksum"),
            }
        } else {
            // Set checksum algorithm, which tells SDK to calculate and add checksum value
            put_object_builder =
                put_object_builder.checksum_algorithm(checksum_strategy.algorithm().clone())
        }
    }

    put_object_builder
}

// Copy fields from `UploadInput` to `CreateMultipartUploadFluentBuilder`
pub(crate) fn copy_fields_to_mpu_request(
    upload_input: &UploadInput,
    mpu_builder: CreateMultipartUploadFluentBuilder,
) -> CreateMultipartUploadFluentBuilder {
    let mut mpu_builder = mpu_builder
        .set_acl(upload_input.acl.clone())
        .set_bucket(upload_input.bucket.clone())
        .set_bucket_key_enabled(upload_input.bucket_key_enabled)
        .set_cache_control(upload_input.cache_control.clone())
        .set_content_disposition(upload_input.content_disposition.clone())
        .set_content_encoding(upload_input.content_encoding.clone())
        .set_content_language(upload_input.content_language.clone())
        .set_content_type(upload_input.content_type.clone())
        .set_expected_bucket_owner(upload_input.expected_bucket_owner.clone())
        .set_expires(upload_input.expires)
        .set_grant_full_control(upload_input.grant_full_control.clone())
        .set_grant_read(upload_input.grant_read.clone())
        .set_grant_read_acp(upload_input.grant_read_acp.clone())
        .set_grant_write_acp(upload_input.grant_write_acp.clone())
        .set_key(upload_input.key.clone())
        .set_metadata(upload_input.metadata.clone())
        .set_object_lock_legal_hold_status(upload_input.object_lock_legal_hold_status.clone())
        .set_object_lock_mode(upload_input.object_lock_mode.clone())
        .set_object_lock_retain_until_date(upload_input.object_lock_retain_until_date)
        .set_request_payer(upload_input.request_payer.clone())
        .set_server_side_encryption(upload_input.server_side_encryption.clone())
        .set_sse_customer_algorithm(upload_input.sse_customer_algorithm.clone())
        .set_sse_customer_key(upload_input.sse_customer_key.clone())
        .set_sse_customer_key_md5(upload_input.sse_customer_key_md5.clone())
        .set_ssekms_encryption_context(upload_input.sse_kms_encryption_context.clone())
        .set_ssekms_key_id(upload_input.sse_kms_key_id.clone())
        .set_storage_class(upload_input.storage_class.clone())
        .set_tagging(upload_input.tagging.clone())
        .set_website_redirect_location(upload_input.website_redirect_location.clone());

    if let Some(checksum_strategy) = &upload_input.checksum_strategy {
        mpu_builder = mpu_builder
            .checksum_algorithm(checksum_strategy.algorithm().clone())
            .checksum_type(checksum_strategy.type_if_multipart().clone());
    }

    mpu_builder
}

// Copy fields from UploadInput to `UploadPartFluentBuilder`
//
// Takes an optional checksum value passed by user via `PartStream`.
pub(crate) fn copy_fields_to_upload_part_request(
    upload_input: &UploadInput,
    upload_part_builder: UploadPartFluentBuilder,
    checksum_value: Option<&String>,
) -> UploadPartFluentBuilder {
    let mut upload_part_builder = upload_part_builder
        .set_bucket(upload_input.bucket.clone())
        .set_expected_bucket_owner(upload_input.expected_bucket_owner.clone())
        .set_key(upload_input.key.clone())
        .set_request_payer(upload_input.request_payer.clone())
        .set_sse_customer_algorithm(upload_input.sse_customer_algorithm.clone())
        .set_sse_customer_key(upload_input.sse_customer_key.clone())
        .set_sse_customer_key_md5(upload_input.sse_customer_key_md5.clone());

    if let Some(checksum_strategy) = &upload_input.checksum_strategy {
        // If user passed checksum value via PartStream, add it to request
        if let Some(checksum_value) = checksum_value {
            upload_part_builder = match checksum_strategy.algorithm() {
                ChecksumAlgorithm::Crc32 => upload_part_builder.checksum_crc32(checksum_value),
                ChecksumAlgorithm::Crc32C => upload_part_builder.checksum_crc32_c(checksum_value),
                ChecksumAlgorithm::Crc64Nvme => {
                    upload_part_builder.checksum_crc64_nvme(checksum_value)
                }
                ChecksumAlgorithm::Sha1 => upload_part_builder.checksum_sha1(checksum_value),
                ChecksumAlgorithm::Sha256 => upload_part_builder.checksum_sha256(checksum_value),
                algo => unreachable!("unexpected checksum algorithm `{algo}`"),
            };
        } else {
            // Otherwise, set checksum algorithm, which tells SDK to calculate and add checksum value
            upload_part_builder =
                upload_part_builder.checksum_algorithm(checksum_strategy.algorithm().clone());
        }
    } else {
        // Warn if user is passing a checksum value, but the upload isn't doing checksums.
        // We can't just set a checksum header, because we don't know what algorithm to use.
        if checksum_value.is_some() {
            tracing::warn!("Ignoring part checksum provided during upload, because no ChecksumStrategy is specified");
        }
    }

    upload_part_builder
}

// Copy fields from UploadInput to `CompleteMultipartUploadFluentBuilder`
//
// Takes a closure that produces the full object checksum via `PartStream` as a fallback,
// which makes this function asynchronous.
pub(crate) async fn copy_fields_to_complete_mpu_request<F, Fut>(
    upload_input: &UploadInput,
    complete_mpu_builder: CompleteMultipartUploadFluentBuilder,
    part_stream_full_object_checksum: F,
) -> CompleteMultipartUploadFluentBuilder
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    let mut complete_mpu_builder = complete_mpu_builder
        .set_bucket(upload_input.bucket.clone())
        .set_key(upload_input.key.clone())
        .set_request_payer(upload_input.request_payer.clone())
        .set_expected_bucket_owner(upload_input.expected_bucket_owner.clone())
        .set_sse_customer_algorithm(upload_input.sse_customer_algorithm.clone())
        .set_sse_customer_key(upload_input.sse_customer_key.clone())
        .set_sse_customer_key_md5(upload_input.sse_customer_key_md5.clone());

    if let Some(checksum_strategy) = &upload_input.checksum_strategy {
        complete_mpu_builder =
            complete_mpu_builder.checksum_type(checksum_strategy.type_if_multipart().clone());

        // check for user-provided full-object checksum...
        if checksum_strategy.type_if_multipart() == &ChecksumType::FullObject {
            // it might have been passed via ChecksumStrategy or PartStream
            let full_object_checksum = match checksum_strategy.full_object_checksum() {
                Some(checksum) => Some(checksum.into()),
                None => part_stream_full_object_checksum().await,
            };

            // if we got one, set the proper request field
            if let Some(value) = full_object_checksum {
                complete_mpu_builder = match checksum_strategy.algorithm() {
                    ChecksumAlgorithm::Crc32 => complete_mpu_builder.checksum_crc32(value),
                    ChecksumAlgorithm::Crc32C => complete_mpu_builder.checksum_crc32_c(value),
                    ChecksumAlgorithm::Crc64Nvme => complete_mpu_builder.checksum_crc64_nvme(value),
                    algo => {
                        unreachable!("unexpected algorithm `{algo}` for full object checksum")
                    }
                };
            }
        }
    }

    complete_mpu_builder
}

// Copy fields from UploadInput to `AbortMultipartUploadFluentBuilder`
pub(crate) fn copy_fields_to_abort_mpu_request(
    upload_input: &UploadInput,
    abort_mpu_builder: AbortMultipartUploadFluentBuilder,
) -> AbortMultipartUploadFluentBuilder {
    abort_mpu_builder
        .set_bucket(upload_input.bucket.clone())
        .set_key(upload_input.key.clone())
        .set_request_payer(upload_input.request_payer.clone())
        .set_expected_bucket_owner(upload_input.expected_bucket_owner.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::upload::ChecksumStrategy;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_sdk_s3::types::{
        ObjectCannedAcl, ObjectLockLegalHoldStatus, ObjectLockMode, RequestPayer,
        ServerSideEncryption, StorageClass,
    };
    use aws_sdk_s3::{Client, Config};
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_types::DateTime;
    use std::sync::Arc;

    fn upload_request_for_tests() -> UploadInput {
        UploadInput::builder()
            .acl(ObjectCannedAcl::PublicRead)
            .bucket("test-bucket")
            .bucket_key_enabled(true)
            .cache_control("max-age=3600")
            .checksum_strategy(ChecksumStrategy::with_crc32("0xebe6c6e6"))
            .content_disposition("attachment; filename=test.txt")
            .content_encoding("gzip")
            .content_language("en-US")
            .content_type("text/plain")
            .expected_bucket_owner("owner123")
            .expires(DateTime::from_secs(1234567890))
            .grant_full_control("test-full-control")
            .grant_read("test-read")
            .grant_read_acp("test-read-acp")
            .grant_write_acp("test-write-acp")
            .key("test-key")
            .metadata("key", "value")
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(DateTime::from_secs(1234567890))
            .request_payer(RequestPayer::Requester)
            .server_side_encryption(ServerSideEncryption::Aes256)
            .sse_customer_algorithm("AES256")
            .sse_customer_key("test-key")
            .sse_customer_key_md5("test-md5")
            .sse_kms_encryption_context("test-context")
            .sse_kms_key_id("test-kms-key")
            .storage_class(StorageClass::StandardIa)
            .tagging("key1=value1")
            .website_redirect_location("https://example.com")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_all_fields_copied_to_put_object_request() {
        let upload_req = Arc::new(upload_request_for_tests());

        // Using a mock client to verify field value copying,
        // since `PutObjectInputBuilder` does not implement `Clone`.
        // This prevents us from doing:
        // `put_object_builder.as_input().clone().build().unwrap();`
        let put_object_mock = mock!(aws_sdk_s3::Client::put_object)
            .match_requests({
                let upload_req = upload_req.clone();
                move |put_object_req| {
                    assert_eq!(upload_req.bucket(), put_object_req.bucket());
                    assert_eq!(None, put_object_req.checksum_algorithm());
                    assert_eq!(Some("0xebe6c6e6"), put_object_req.checksum_crc32(),);
                    assert_eq!(None, put_object_req.checksum_crc32_c());
                    assert_eq!(None, put_object_req.checksum_crc64_nvme(),);
                    assert_eq!(None, put_object_req.checksum_sha1());
                    assert_eq!(None, put_object_req.checksum_sha256(),);
                    assert_eq!(
                        upload_req.expected_bucket_owner(),
                        put_object_req.expected_bucket_owner()
                    );
                    assert_eq!(upload_req.key(), put_object_req.key());
                    assert_eq!(upload_req.request_payer(), put_object_req.request_payer());
                    assert_eq!(
                        upload_req.sse_customer_algorithm(),
                        put_object_req.sse_customer_algorithm()
                    );
                    assert_eq!(
                        upload_req.sse_customer_key(),
                        put_object_req.sse_customer_key()
                    );
                    assert_eq!(
                        upload_req.sse_customer_key_md5(),
                        put_object_req.sse_customer_key_md5()
                    );
                    true
                }
            })
            .then_output(|| PutObjectOutput::builder().build());

        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&put_object_mock]);

        let put_object_builder = client
            .put_object()
            .body(aws_sdk_s3::primitives::ByteStream::from(
                bytes::Bytes::from("test"),
            ))
            .content_length(4);

        let put_object_builder =
            copy_fields_to_put_object_request(upload_req.as_ref(), put_object_builder);

        let _ = put_object_builder.send().await.unwrap();
    }

    #[test]
    fn test_all_fields_copied_to_mpu_request() {
        let upload_req = upload_request_for_tests();

        let config = aws_sdk_s3::Config::builder().build();
        let client = aws_sdk_s3::Client::from_conf(config);
        let mpu_builder = copy_fields_to_mpu_request(&upload_req, client.create_multipart_upload());
        let mpu_req = mpu_builder.as_input().clone().build().unwrap();

        assert_eq!(upload_req.acl(), mpu_req.acl());
        assert_eq!(upload_req.bucket(), mpu_req.bucket());
        assert_eq!(
            upload_req.bucket_key_enabled(),
            mpu_req.bucket_key_enabled()
        );
        assert_eq!(upload_req.cache_control(), mpu_req.cache_control());
        assert_eq!(
            upload_req.checksum_strategy().map(|c| c.algorithm()),
            mpu_req.checksum_algorithm()
        );
        assert_eq!(
            upload_req.content_disposition(),
            mpu_req.content_disposition()
        );
        assert_eq!(upload_req.content_encoding(), mpu_req.content_encoding());
        assert_eq!(upload_req.content_language(), mpu_req.content_language());
        assert_eq!(upload_req.content_type(), mpu_req.content_type());
        assert_eq!(
            upload_req.expected_bucket_owner(),
            mpu_req.expected_bucket_owner()
        );
        assert_eq!(upload_req.expires(), mpu_req.expires());
        assert_eq!(
            upload_req.grant_full_control(),
            mpu_req.grant_full_control()
        );
        assert_eq!(upload_req.grant_read(), mpu_req.grant_read());
        assert_eq!(upload_req.grant_read_acp(), mpu_req.grant_read_acp());
        assert_eq!(upload_req.grant_write_acp(), mpu_req.grant_write_acp());
        assert_eq!(upload_req.key(), mpu_req.key());
        assert_eq!(upload_req.metadata(), mpu_req.metadata());
        assert_eq!(
            upload_req.object_lock_legal_hold_status(),
            mpu_req.object_lock_legal_hold_status()
        );
        assert_eq!(upload_req.object_lock_mode(), mpu_req.object_lock_mode());
        assert_eq!(
            upload_req.object_lock_retain_until_date(),
            mpu_req.object_lock_retain_until_date()
        );
        assert_eq!(upload_req.request_payer(), mpu_req.request_payer());
        assert_eq!(
            upload_req.sse_customer_algorithm(),
            mpu_req.sse_customer_algorithm()
        );
        assert_eq!(upload_req.sse_customer_key(), mpu_req.sse_customer_key());
        assert_eq!(
            upload_req.sse_customer_key_md5(),
            mpu_req.sse_customer_key_md5()
        );
        assert_eq!(
            upload_req.sse_kms_encryption_context(),
            mpu_req.ssekms_encryption_context()
        );
        assert_eq!(upload_req.sse_kms_key_id(), mpu_req.ssekms_key_id());
        assert_eq!(
            upload_req.server_side_encryption(),
            mpu_req.server_side_encryption()
        );
        assert_eq!(upload_req.storage_class(), mpu_req.storage_class());
        assert_eq!(upload_req.tagging(), mpu_req.tagging());
        assert_eq!(
            upload_req.website_redirect_location(),
            mpu_req.website_redirect_location()
        );
    }

    #[tokio::test]
    async fn test_all_fields_copied_to_upload_part_request() {
        let upload_req = Arc::new(upload_request_for_tests());

        // Using a mock client to verify field value copying,
        // since `UploadPartInputBuilder` does not implement `Clone`.
        // This prevents us from doing:
        // `upload_part_builder.as_input().clone().build().unwrap();`
        let upload_part_mock = mock!(aws_sdk_s3::Client::upload_part)
            .match_requests({
                let upload_req = upload_req.clone();
                move |upload_part_req| {
                    assert_eq!(upload_req.bucket(), upload_part_req.bucket());
                    assert_eq!(
                        Some(&aws_sdk_s3::types::ChecksumAlgorithm::Crc32),
                        upload_part_req.checksum_algorithm()
                    );
                    assert_eq!(
                        upload_req.expected_bucket_owner(),
                        upload_part_req.expected_bucket_owner()
                    );
                    assert_eq!(upload_req.key(), upload_part_req.key());
                    assert_eq!(upload_req.request_payer(), upload_part_req.request_payer());
                    assert_eq!(
                        upload_req.sse_customer_algorithm(),
                        upload_part_req.sse_customer_algorithm()
                    );
                    assert_eq!(
                        upload_req.sse_customer_key(),
                        upload_part_req.sse_customer_key()
                    );
                    assert_eq!(
                        upload_req.sse_customer_key_md5(),
                        upload_part_req.sse_customer_key_md5()
                    );
                    true
                }
            })
            .then_output(|| UploadPartOutput::builder().build());

        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&upload_part_mock]);
        let upload_part_builder = client.upload_part().upload_id("test-id").part_number(1);
        let upload_part_builder =
            copy_fields_to_upload_part_request(upload_req.as_ref(), upload_part_builder, None);
        let _ = upload_part_builder.send().await.unwrap();
    }

    #[tokio::test]
    async fn test_copy_fields_to_complete_mpu_request() {
        let config = Config::builder().build();
        let client = Client::from_conf(config);

        let upload_req = upload_request_for_tests();

        let complete_mpu_builder = copy_fields_to_complete_mpu_request(
            &upload_req,
            client.complete_multipart_upload(),
            || async move { None },
        )
        .await;
        let complete_mpu_req = complete_mpu_builder.as_input().clone().build().unwrap();

        assert_eq!(upload_req.bucket(), complete_mpu_req.bucket());
        assert_eq!(Some("0xebe6c6e6"), complete_mpu_req.checksum_crc32());
        assert_eq!(None, complete_mpu_req.checksum_crc32_c());
        assert_eq!(None, complete_mpu_req.checksum_crc64_nvme());
        assert_eq!(None, complete_mpu_req.checksum_sha1());
        assert_eq!(None, complete_mpu_req.checksum_sha256());
        assert_eq!(
            upload_req.expected_bucket_owner(),
            complete_mpu_req.expected_bucket_owner()
        );
        // TODO(https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/117): Enable these assertions
        /*
        assert_eq!(
            upload_req.if_match(),
            complete_mpu_req.if_match()
        );
        assert_eq!(
            upload_req.if_none_match(),
            complete_mpu_req.if_none_match()
        );
        */
        assert_eq!(upload_req.key(), complete_mpu_req.key());
        assert_eq!(upload_req.request_payer(), complete_mpu_req.request_payer());
        assert_eq!(
            upload_req.sse_customer_algorithm(),
            complete_mpu_req.sse_customer_algorithm()
        );
        assert_eq!(
            upload_req.sse_customer_key(),
            complete_mpu_req.sse_customer_key()
        );
        assert_eq!(
            upload_req.sse_customer_key_md5(),
            complete_mpu_req.sse_customer_key_md5()
        );
    }

    #[test]
    fn test_copy_fields_to_abort_mpu_request() {
        let config = Config::builder().build();
        let client = Client::from_conf(config);

        let upload_req = upload_request_for_tests();

        let abort_mpu_builder =
            copy_fields_to_abort_mpu_request(&upload_req, client.abort_multipart_upload());
        let abort_mpu_req = abort_mpu_builder.as_input().clone().build().unwrap();

        assert_eq!(upload_req.bucket(), abort_mpu_req.bucket());
        assert_eq!(
            upload_req.expected_bucket_owner(),
            abort_mpu_req.expected_bucket_owner()
        );
        assert_eq!(upload_req.key(), abort_mpu_req.key());
        assert_eq!(upload_req.request_payer(), abort_mpu_req.request_payer());
    }
}
