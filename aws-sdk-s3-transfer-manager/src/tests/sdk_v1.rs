/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{model, sdk_v1::*};
use aws_smithy_types::DateTime;

fn upload() -> model::UploadInput {
    model::UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .request_payer(model::RequestPayer::Requester)
        .expected_bucket_owner("owner")
        .sse_customer_algorithm("AES256")
        .sse_customer_key("customer-key")
        .sse_customer_key_md5("customer-md5")
        .content_md5("whole-object-md5")
        .content_type("text/plain")
        .metadata("name", "value")
        .bucket_key_enabled(false)
        .expires(DateTime::from_secs(123))
        .if_match("etag")
        .if_none_match("*")
        .build()
        .unwrap()
}

#[test]
fn put_copies_values_without_replacing_runtime_body_length_or_checksum() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let request = copy_upload_input_fields_to_put_object(
        &upload(),
        client
            .put_object()
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"runtime"))
            .content_length(7)
            .checksum_crc32("runtime-checksum"),
    );
    let request = request.as_input();
    assert_eq!(
        request.get_body().as_ref().unwrap().bytes(),
        Some(&b"runtime"[..])
    );
    assert_eq!(request.get_content_length(), &Some(7));
    assert_eq!(
        request.get_checksum_crc32().as_deref(),
        Some("runtime-checksum")
    );
    assert_eq!(
        request.get_content_md5().as_deref(),
        Some("whole-object-md5")
    );
    assert_eq!(request.get_bucket_key_enabled(), &Some(false));
    assert_eq!(
        request
            .get_metadata()
            .as_ref()
            .unwrap()
            .get("name")
            .unwrap(),
        "value"
    );
    assert_eq!(request.get_expires(), &Some(DateTime::from_secs(123)));
}

#[test]
fn create_copies_object_setup_without_replacing_checksum_policy() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let request = copy_upload_input_fields_to_create_multipart_upload(
        &upload(),
        client
            .create_multipart_upload()
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32),
    );
    let request = request.as_input().clone().build().unwrap();
    assert_eq!(request.content_type(), Some("text/plain"));
    assert_eq!(request.bucket_key_enabled(), Some(false));
    assert_eq!(request.metadata().unwrap().get("name").unwrap(), "value");
    assert_eq!(request.checksum_algorithm().unwrap().as_str(), "CRC32");
}

#[test]
fn part_does_not_copy_whole_object_md5_or_replace_part_specific_values() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let input = upload();
    let empty = copy_upload_input_fields_to_upload_part(&input, client.upload_part());
    assert_eq!(empty.as_input().get_content_md5(), &None);
    let request = copy_upload_input_fields_to_upload_part(
        &input,
        client
            .upload_part()
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"part"))
            .content_length(4)
            .content_md5("part-md5")
            .part_number(3)
            .upload_id("upload")
            .checksum_sha256("part-checksum"),
    );
    let request = request.as_input();
    assert_eq!(request.get_content_md5().as_deref(), Some("part-md5"));
    assert_eq!(
        request.get_body().as_ref().unwrap().bytes(),
        Some(&b"part"[..])
    );
    assert_eq!(request.get_content_length(), &Some(4));
    assert_eq!(request.get_part_number(), &Some(3));
    assert_eq!(request.get_upload_id().as_deref(), Some("upload"));
    assert_eq!(
        request.get_checksum_sha256().as_deref(),
        Some("part-checksum")
    );
    assert_eq!(
        request.get_sse_customer_key().as_deref(),
        Some("customer-key")
    );
    assert_eq!(
        request.get_request_payer().as_ref().unwrap().as_str(),
        "requester"
    );
}

#[test]
fn complete_preserves_runtime_parts_size_and_upload_id() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let request = copy_upload_input_fields_to_complete_multipart_upload(
        &upload(),
        client
            .complete_multipart_upload()
            .upload_id("upload")
            .mpu_object_size(4)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .parts(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(1)
                            .e_tag("part-etag")
                            .build(),
                    )
                    .build(),
            ),
    );
    let request = request.as_input().clone().build().unwrap();
    assert_eq!(request.upload_id(), Some("upload"));
    assert_eq!(request.mpu_object_size(), Some(4));
    assert_eq!(
        request.multipart_upload().unwrap().parts()[0].e_tag(),
        Some("part-etag")
    );
    assert_eq!(request.if_match(), Some("etag"));
    assert_eq!(request.if_none_match(), Some("*"));
}

#[test]
fn abort_omits_condition_and_preserves_an_explicit_sdk_condition() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let input = upload();
    let request =
        copy_upload_input_fields_to_abort_multipart_upload(&input, client.abort_multipart_upload());
    assert_eq!(request.as_input().get_if_match_initiated_time(), &None);
    let time = DateTime::from_secs(42);
    let request = copy_upload_input_fields_to_abort_multipart_upload(
        &input,
        client
            .abort_multipart_upload()
            .upload_id("upload")
            .if_match_initiated_time(time),
    );
    assert_eq!(
        request.as_input().get_if_match_initiated_time(),
        &Some(time)
    );
    assert_eq!(
        request.as_input().get_upload_id().as_deref(),
        Some("upload")
    );
    assert_eq!(request.as_input().get_bucket().as_deref(), Some("bucket"));
}

#[test]
fn download_copies_conditions_and_preserves_runtime_part_number() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let input = model::DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .range("bytes=0-3")
        .if_modified_since(DateTime::from_secs(42))
        .checksum_mode(model::ChecksumMode::Enabled)
        .build()
        .unwrap();
    let get = copy_download_input_fields_to_get_object(&input, client.get_object().part_number(2));
    let head =
        copy_download_input_fields_to_head_object(&input, client.head_object().part_number(2));
    let get = get.as_input().clone().build().unwrap();
    let head = head.as_input().clone().build().unwrap();
    for (range, modified, mode, part) in [
        (
            get.range(),
            get.if_modified_since(),
            get.checksum_mode(),
            get.part_number(),
        ),
        (
            head.range(),
            head.if_modified_since(),
            head.checksum_mode(),
            head.part_number(),
        ),
    ] {
        assert_eq!(range, Some("bytes=0-3"));
        assert_eq!(modified, Some(&DateTime::from_secs(42)));
        assert_eq!(mode.unwrap().as_str(), "ENABLED");
        assert_eq!(part, Some(2));
    }
}

#[test]
fn download_copies_ordinary_fields_and_keeps_runtime_controls_out_of_requests() {
    let input = model::DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .if_match("etag")
        .if_none_match("*")
        .if_modified_since(DateTime::from_secs(123))
        .if_unmodified_since(DateTime::from_secs(456))
        .range("bytes=1-3")
        .response_cache_control("no-cache")
        .response_content_disposition("attachment")
        .response_content_encoding("gzip")
        .response_content_language("en")
        .response_content_type("text/plain")
        .response_expires(DateTime::from_secs(789))
        .version_id("version")
        .sse_customer_algorithm("AES256")
        .sse_customer_key("key")
        .sse_customer_key_md5("md5")
        .request_payer(model::RequestPayer::from("FUTURE_PAYER"))
        .expected_bucket_owner("owner")
        .checksum_mode(model::ChecksumMode::from("FUTURE_MODE"))
        .part_number(99)
        .read_ahead(crate::types::ReadAhead::Parts(2))
        .build()
        .unwrap();
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let get = copy_download_input_fields_to_get_object(&input, client.get_object().part_number(3));
    let head =
        copy_download_input_fields_to_head_object(&input, client.head_object().part_number(3));
    macro_rules! shared {
        ($request:expr) => {{
            let request = $request;
            assert_eq!(&input.bucket, request.get_bucket());
            assert_eq!(&input.key, request.get_key());
            assert_eq!(&input.if_match, request.get_if_match());
            assert_eq!(&input.if_none_match, request.get_if_none_match());
            assert_eq!(&input.if_modified_since, request.get_if_modified_since());
            assert_eq!(
                &input.if_unmodified_since,
                request.get_if_unmodified_since()
            );
            assert_eq!(&input.range, request.get_range());
            assert_eq!(&input.version_id, request.get_version_id());
            assert_eq!(
                &input.sse_customer_algorithm,
                request.get_sse_customer_algorithm()
            );
            assert_eq!(&input.sse_customer_key, request.get_sse_customer_key());
            assert_eq!(
                &input.sse_customer_key_md5,
                request.get_sse_customer_key_md5()
            );
            assert_eq!(
                &input.expected_bucket_owner,
                request.get_expected_bucket_owner()
            );
            assert_eq!(
                request.get_request_payer().as_ref().unwrap().as_str(),
                "FUTURE_PAYER"
            );
            assert_eq!(
                request.get_checksum_mode().as_ref().unwrap().as_str(),
                "FUTURE_MODE"
            );
            assert_eq!(request.get_part_number(), &Some(3));
        }};
    }
    shared!(get.as_input());
    shared!(head.as_input());
    let get = get.as_input();
    assert_eq!(
        &input.response_cache_control,
        get.get_response_cache_control()
    );
    assert_eq!(
        &input.response_content_disposition,
        get.get_response_content_disposition()
    );
    assert_eq!(
        &input.response_content_encoding,
        get.get_response_content_encoding()
    );
    assert_eq!(
        &input.response_content_language,
        get.get_response_content_language()
    );
    assert_eq!(
        &input.response_content_type,
        get.get_response_content_type()
    );
    assert_eq!(&input.response_expires, get.get_response_expires());
    assert_eq!(input.part_number(), Some(99));
    assert_eq!(input.read_ahead(), Some(&crate::types::ReadAhead::Parts(2)));
}

#[test]
fn absent_download_options_clear_stale_headers_without_clearing_runtime_parts() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let input = model::DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .build()
        .unwrap();
    let get = copy_download_input_fields_to_get_object(
        &input,
        client
            .get_object()
            .if_match("stale")
            .sse_customer_key("stale")
            .response_content_type("stale")
            .part_number(3),
    );
    let head = copy_download_input_fields_to_head_object(
        &input,
        client
            .head_object()
            .if_match("stale")
            .sse_customer_key("stale")
            .part_number(3),
    );
    assert_eq!(get.as_input().get_if_match(), &None);
    assert_eq!(head.as_input().get_if_match(), &None);
    assert_eq!(get.as_input().get_sse_customer_key(), &None);
    assert_eq!(head.as_input().get_sse_customer_key(), &None);
    assert_eq!(get.as_input().get_response_content_type(), &None);
    assert_eq!(get.as_input().get_part_number(), &Some(3));
    assert_eq!(head.as_input().get_part_number(), &Some(3));
}

#[test]
fn head_metadata_preserves_union_only_fields_and_optional_storage() {
    let sdk = aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
        .archive_status(aws_sdk_s3::types::ArchiveStatus::from("FUTURE_ARCHIVE"))
        .content_length(0)
        .delete_marker(false)
        .bucket_key_enabled(false)
        .missing_meta(0)
        .parts_count(0)
        .last_modified(DateTime::from_secs(123))
        .expires_string("not a date")
        .set_metadata(Some(Default::default()))
        .checksum_sha512("sha512")
        .checksum_xxhash3("xxhash3")
        .storage_class(aws_sdk_s3::types::StorageClass::from("FUTURE_STORAGE"))
        .build();
    let metadata = object_metadata_from_head_object(&sdk);
    assert_eq!(
        metadata.archive_status().unwrap().as_str(),
        "FUTURE_ARCHIVE"
    );
    assert_eq!(metadata.content_length(), Some(0));
    assert_eq!(metadata.content_range(), None);
    assert_eq!(metadata.delete_marker(), Some(false));
    assert_eq!(metadata.bucket_key_enabled(), Some(false));
    assert_eq!(metadata.missing_meta(), Some(0));
    assert_eq!(metadata.parts_count(), Some(0));
    assert_eq!(metadata.last_modified(), Some(&DateTime::from_secs(123)));
    assert_eq!(metadata.expires_string(), Some("not a date"));
    assert_eq!(metadata.metadata, Some(Default::default()));
    assert_eq!(metadata.checksum_sha512(), Some("sha512"));
    assert_eq!(metadata.checksum_xxhash3(), Some("xxhash3"));
    assert_eq!(metadata.storage_class().unwrap().as_str(), "FUTURE_STORAGE");
    let get = aws_sdk_s3::operation::get_object::GetObjectOutput::builder().build();
    let metadata = object_metadata_from_get_object(&get);
    assert_eq!(metadata.archive_status(), None);
    assert_eq!(metadata.content_length(), None);
    assert_eq!(metadata.delete_marker(), None);
    assert_eq!(metadata.bucket_key_enabled(), None);
    assert_eq!(metadata.missing_meta(), None);
    assert_eq!(metadata.parts_count(), None);
    assert_eq!(metadata.metadata, None);
}

#[test]
fn metadata_preserves_raw_expires_absence_false_zero_and_new_checksums() {
    let output = aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
        .expires_string("not a date")
        .bucket_key_enabled(false)
        .content_length(0)
        .checksum_xxhash128("checksum")
        .set_metadata(Some(Default::default()))
        .build();
    let object = object_metadata_from_get_object(&output);
    let chunk = chunk_metadata_from_get_object(&output);
    assert_eq!(object.expires_string(), Some("not a date"));
    assert_eq!(chunk.expires_string(), Some("not a date"));
    assert_eq!(object.bucket_key_enabled(), Some(false));
    assert_eq!(chunk.content_length(), Some(0));
    assert_eq!(chunk.checksum_xxhash128(), Some("checksum"));
    assert_eq!(object.metadata, Some(Default::default()));
    assert_eq!(object.delete_marker(), None);
    let head = aws_sdk_s3::operation::head_object::HeadObjectOutput::builder().build();
    let object = object_metadata_from_head_object(&head);
    assert_eq!(object.content_length(), None);
    assert_eq!(object.metadata, None);
    assert_eq!(object.request_id(), None);
}

#[test]
fn upload_updates_preserve_create_only_fields_and_do_not_fabricate_metrics() {
    let create =
        aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput::builder()
            .upload_id("upload")
            .abort_rule_id("rule")
            .ssekms_encryption_context("context")
            .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
            .build();
    let builder = upload_output_from_create_multipart_upload(&create);
    assert!(builder.clone().build().is_err());
    let complete =
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput::builder()
            .e_tag("etag")
            .checksum_md5("checksum")
            .build();
    let builder = update_upload_output_from_complete_multipart_upload(builder, &complete);
    assert_eq!(builder.get_upload_id().as_deref(), Some("upload"));
    assert_eq!(builder.get_abort_rule_id().as_deref(), Some("rule"));
    assert_eq!(
        builder.get_sse_kms_encryption_context().as_deref(),
        Some("context")
    );
    assert_eq!(builder.get_server_side_encryption(), &None);
    assert_eq!(builder.get_checksum_md5().as_deref(), Some("checksum"));
    let put = aws_sdk_s3::operation::put_object::PutObjectOutput::builder()
        .size(0)
        .build();
    assert_eq!(upload_output_from_put_object(&put).get_size(), &Some(0));
}

#[test]
fn upload_ordinary_fields_preserve_wire_values_and_tm_spellings() {
    let input = model::UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .acl(model::ObjectCannedAcl::from("FUTURE_ACL"))
        .cache_control("no-cache")
        .content_disposition("attachment")
        .content_encoding("gzip")
        .content_language("en")
        .content_length(999)
        .content_md5("whole-object-md5")
        .content_type("text/plain")
        .expires(DateTime::from_secs(123))
        .grant_full_control("full")
        .grant_read("read")
        .grant_read_acp("read-acp")
        .grant_write_acp("write-acp")
        .if_match("etag")
        .if_none_match("*")
        .metadata("name", "value")
        .server_side_encryption(model::ServerSideEncryption::from("FUTURE_ENCRYPTION"))
        .storage_class(model::StorageClass::from("FUTURE_STORAGE"))
        .website_redirect_location("/location")
        .sse_customer_algorithm("AES256")
        .sse_customer_key("customer-key")
        .sse_customer_key_md5("customer-md5")
        .sse_kms_key_id("kms-key")
        .sse_kms_encryption_context("kms-context")
        .bucket_key_enabled(false)
        .request_payer(model::RequestPayer::from("FUTURE_PAYER"))
        .tagging("tag=value")
        .object_lock_mode(model::ObjectLockMode::from("FUTURE_LOCK"))
        .object_lock_retain_until_date(DateTime::from_secs(456))
        .object_lock_legal_hold_status(model::ObjectLockLegalHoldStatus::from("FUTURE_HOLD"))
        .expected_bucket_owner("owner")
        .build()
        .unwrap();
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let request =
        copy_upload_input_fields_to_put_object(&input, client.put_object().content_length(7));
    let request = request.as_input();
    macro_rules! same {
        ($(($field:ident, $getter:ident)),* $(,)?) => {
            $(assert_eq!(&input.$field, request.$getter(), stringify!($field));)*
        };
    }
    same!(
        (bucket, get_bucket),
        (key, get_key),
        (cache_control, get_cache_control),
        (content_disposition, get_content_disposition),
        (content_encoding, get_content_encoding),
        (content_language, get_content_language),
        (content_md5, get_content_md5),
        (content_type, get_content_type),
        (expires, get_expires),
        (grant_full_control, get_grant_full_control),
        (grant_read, get_grant_read),
        (grant_read_acp, get_grant_read_acp),
        (grant_write_acp, get_grant_write_acp),
        (if_match, get_if_match),
        (if_none_match, get_if_none_match),
        (metadata, get_metadata),
        (website_redirect_location, get_website_redirect_location),
        (sse_customer_algorithm, get_sse_customer_algorithm),
        (sse_customer_key, get_sse_customer_key),
        (sse_customer_key_md5, get_sse_customer_key_md5),
        (bucket_key_enabled, get_bucket_key_enabled),
        (tagging, get_tagging),
        (
            object_lock_retain_until_date,
            get_object_lock_retain_until_date
        ),
        (expected_bucket_owner, get_expected_bucket_owner),
    );
    macro_rules! same_wire {
        ($(($field:ident, $getter:ident)),* $(,)?) => {
            $(assert_eq!(
                input.$field().map(|value| value.as_str()),
                request.$getter().as_ref().map(|value| value.as_str()),
                stringify!($field),
            );)*
        };
    }
    same_wire!(
        (acl, get_acl),
        (server_side_encryption, get_server_side_encryption),
        (storage_class, get_storage_class),
        (request_payer, get_request_payer),
        (object_lock_mode, get_object_lock_mode),
        (
            object_lock_legal_hold_status,
            get_object_lock_legal_hold_status
        ),
    );
    assert_eq!(
        input.sse_kms_key_id(),
        request.get_ssekms_key_id().as_deref()
    );
    assert_eq!(
        input.sse_kms_encryption_context(),
        request.get_ssekms_encryption_context().as_deref()
    );
    assert_eq!(request.get_content_length(), &Some(7));
    assert_eq!(request.get_checksum_algorithm(), &None);
}

#[test]
fn upload_field_copy_distinguishes_absent_values_from_explicit_empty_and_false() {
    let client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let absent = model::UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .build()
        .unwrap();
    let empty = model::UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .bucket_key_enabled(false)
        .set_metadata(Some(Default::default()))
        .tagging("")
        .build()
        .unwrap();
    for (input, metadata, enabled, tagging) in [
        (&absent, None, None, None),
        (&empty, Some(Default::default()), Some(false), Some("")),
    ] {
        let request = copy_upload_input_fields_to_put_object(
            input,
            client
                .put_object()
                .metadata("stale", "value")
                .bucket_key_enabled(true)
                .tagging("stale"),
        );
        let request = request.as_input();
        assert_eq!(request.get_metadata(), &metadata);
        assert_eq!(request.get_bucket_key_enabled(), &enabled);
        assert_eq!(request.get_tagging().as_deref(), tagging);
    }
}

#[test]
fn multipart_output_updates_retain_real_metrics_and_create_only_members() {
    let metrics = crate::types::TransferMetrics {
        network_tx: 42,
        network_rx: 1,
        disk_read: 42,
        disk_write: 0,
        total_bytes: Some(42),
        started_at: std::time::Instant::now(),
        finished_at: None,
    };
    let create =
        aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput::builder()
            .upload_id("upload")
            .abort_date(DateTime::from_secs(456))
            .abort_rule_id("rule")
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::from(
                "FUTURE_CHECKSUM",
            ))
            .ssekms_encryption_context("context")
            .bucket_key_enabled(true)
            .build();
    let builder = upload_output_from_create_multipart_upload(&create).metrics(metrics);
    let complete =
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput::builder()
            .e_tag("etag")
            .location("location")
            .checksum_md5("md5")
            .checksum_sha512("sha512")
            .checksum_xxhash64("xxhash64")
            .checksum_xxhash3("xxhash3")
            .checksum_xxhash128("xxhash128")
            .checksum_type(aws_sdk_s3::types::ChecksumType::from(
                "FUTURE_CHECKSUM_TYPE",
            ))
            .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::from(
                "FUTURE_ENCRYPTION",
            ))
            .ssekms_key_id("key")
            .build();
    let builder = update_upload_output_from_complete_multipart_upload(builder, &complete);
    assert!(builder.clone().set_metrics(None).build().is_err());
    let output = builder.build().unwrap();
    assert_eq!(output.metrics().network_tx, 42);
    assert_eq!(output.metrics().network_rx, 1);
    assert_eq!(output.metrics().total_bytes, Some(42));
    assert_eq!(output.upload_id(), Some("upload"));
    assert_eq!(output.abort_date(), Some(&DateTime::from_secs(456)));
    assert_eq!(output.abort_rule_id(), Some("rule"));
    assert_eq!(
        output.checksum_algorithm().unwrap().as_str(),
        "FUTURE_CHECKSUM"
    );
    assert_eq!(output.sse_kms_encryption_context(), Some("context"));
    assert_eq!(output.bucket_key_enabled(), None);
    assert_eq!(output.e_tag(), Some("etag"));
    assert_eq!(output.location(), Some("location"));
    assert_eq!(output.checksum_md5(), Some("md5"));
    assert_eq!(output.checksum_sha512(), Some("sha512"));
    assert_eq!(output.checksum_xxhash64(), Some("xxhash64"));
    assert_eq!(output.checksum_xxhash3(), Some("xxhash3"));
    assert_eq!(output.checksum_xxhash128(), Some("xxhash128"));
    assert_eq!(
        output.checksum_type().unwrap().as_str(),
        "FUTURE_CHECKSUM_TYPE"
    );
    assert_eq!(
        output.server_side_encryption().unwrap().as_str(),
        "FUTURE_ENCRYPTION"
    );
    assert_eq!(output.sse_kms_key_id(), Some("key"));
}

#[test]
fn nested_converters_keep_absent_and_empty_collections_distinct() {
    let absent = model::Object::builder().build();
    let empty = model::Object::builder()
        .size(0)
        .set_checksum_algorithm(Some(Vec::new()))
        .restore_status(
            model::RestoreStatus::builder()
                .is_restore_in_progress(false)
                .build(),
        )
        .owner(model::Owner::builder().id("owner").build())
        .build();
    let absent = object_from_sdk(&object_to_sdk(&absent));
    let empty = object_from_sdk(&object_to_sdk(&empty));
    assert_eq!(absent.checksum_algorithm, None);
    assert_eq!(empty.checksum_algorithm, Some(Vec::new()));
    assert_eq!(empty.size(), Some(0));
    assert_eq!(
        empty.restore_status().unwrap().is_restore_in_progress(),
        Some(false)
    );
    assert_eq!(empty.owner().unwrap().id(), Some("owner"));
}

#[test]
fn modeled_checksum_values_do_not_enable_unsupported_execution_algorithms() {
    for name in [
        "MD5",
        "SHA512",
        "XXHASH64",
        "XXHASH3",
        "XXHASH128",
        "FUTURE",
    ] {
        let algorithm = model::ChecksumAlgorithm::from(name);
        assert_eq!(
            checksum_algorithm_from_sdk(&checksum_algorithm_to_sdk(&algorithm)),
            algorithm
        );
        assert!(crate::operation::upload::ChecksumStrategy::builder()
            .algorithm(algorithm)
            .build()
            .is_err());
    }
    for name in ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"] {
        assert!(crate::operation::upload::ChecksumStrategy::builder()
            .algorithm(model::ChecksumAlgorithm::from(name))
            .build()
            .is_ok());
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn response_ids_are_taken_from_decoded_sdk_headers() {
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    let http = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder()
            .uri("https://unused")
            .body(SdkBody::empty())
            .unwrap(),
        http::Response::builder()
            .status(200)
            .header("x-amz-request-id", "request")
            .header("x-amz-id-2", "extended")
            .header("content-length", "7")
            .body(SdkBody::from("payload"))
            .unwrap(),
    )]);
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .with_test_defaults()
            .region(aws_sdk_s3::config::Region::from_static("us-west-2"))
            .http_client(http)
            .build(),
    );
    let output = client
        .get_object()
        .bucket("bucket")
        .key("key")
        .send()
        .await
        .unwrap();
    let object = object_metadata_from_get_object(&output);
    let chunk = chunk_metadata_from_get_object(&output);
    assert_eq!(object.request_id(), Some("request"));
    assert_eq!(object.extended_request_id(), Some("extended"));
    assert_eq!(chunk.request_id(), Some("request"));
    assert_eq!(chunk.extended_request_id(), Some("extended"));
    #[cfg(feature = "sdk-v1")]
    {
        let object = model::ObjectMetadata::from(&output);
        let chunk = model::ChunkMetadata::from(&output);
        assert_eq!(
            aws_sdk_s3::operation::RequestId::request_id(&object),
            Some("request")
        );
        assert_eq!(
            aws_sdk_s3::operation::RequestIdExt::extended_request_id(&object),
            Some("extended")
        );
        assert_eq!(
            aws_sdk_s3::operation::RequestId::request_id(&chunk),
            Some("request")
        );
        assert_eq!(
            aws_sdk_s3::operation::RequestIdExt::extended_request_id(&chunk),
            Some("extended")
        );
    }
    assert_eq!(
        output.body.collect().await.unwrap().into_bytes().as_ref(),
        b"payload"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn head_response_identifiers_are_preserved_by_private_and_gated_conversions() {
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    let http = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder()
            .uri("https://unused")
            .body(SdkBody::empty())
            .unwrap(),
        http::Response::builder()
            .status(200)
            .header("x-amz-request-id", "head-request")
            .header("x-amz-id-2", "head-extended")
            .header("content-length", "0")
            .body(SdkBody::empty())
            .unwrap(),
    )]);
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .with_test_defaults()
            .region(aws_sdk_s3::config::Region::from_static("us-west-2"))
            .http_client(http)
            .build(),
    );
    let sdk = client
        .head_object()
        .bucket("bucket")
        .key("key")
        .send()
        .await
        .unwrap();
    let metadata = object_metadata_from_head_object(&sdk);
    assert_eq!(metadata.content_length(), Some(0));
    assert_eq!(metadata.request_id(), Some("head-request"));
    assert_eq!(metadata.extended_request_id(), Some("head-extended"));
    #[cfg(feature = "sdk-v1")]
    {
        let borrowed = model::ObjectMetadata::from(&sdk);
        let owned = model::ObjectMetadata::from(sdk);
        for metadata in [borrowed, owned] {
            assert_eq!(
                aws_sdk_s3::operation::RequestId::request_id(&metadata),
                Some("head-request")
            );
            assert_eq!(
                aws_sdk_s3::operation::RequestIdExt::extended_request_id(&metadata),
                Some("head-extended")
            );
        }
    }
}
