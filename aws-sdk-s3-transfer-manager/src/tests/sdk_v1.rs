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
}
