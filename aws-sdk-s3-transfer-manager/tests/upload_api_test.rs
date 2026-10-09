/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod common;

#[cfg(feature = "sdk-v1")]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "sdk-v1")]
use aws_sdk_s3::operation::{
    complete_multipart_upload::CompleteMultipartUploadOutput,
    create_multipart_upload::CreateMultipartUploadOutput, put_object::PutObjectOutput,
    upload_part::UploadPartOutput,
};
#[cfg(feature = "sdk-v1")]
use aws_sdk_s3_transfer_manager::io::{PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::{
    io::InputStream,
    model,
    operation::upload::{self, ChecksumStrategy},
    Client, Config,
};
#[cfg(feature = "sdk-v1")]
use aws_smithy_mocks::{mock, mock_client, RuleMode};
#[cfg(feature = "sdk-v1")]
use aws_smithy_types::DateTime;
#[cfg(feature = "sdk-v1")]
use bytes::Bytes;

#[test]
fn operation_exports_are_the_canonical_modeled_types_and_builders() {
    let builder: upload::UploadInputBuilder = model::UploadInput::builder();
    let builder: model::builders::UploadInputBuilder = builder.bucket("bucket").key("key");
    assert_eq!(builder.get_bucket(), &Some("bucket".to_owned()));
    let input: upload::UploadInput = builder.build().unwrap();
    let input: model::UploadInput = input;
    assert_eq!(input.bucket(), Some("bucket"));
    assert_eq!(input.body().size_hint().upper(), Some(0));
    let builder: upload::UploadOutputBuilder = model::UploadOutput::builder();
    let builder: model::builders::UploadOutputBuilder = builder;
    assert!(builder.build().is_err());
    fn canonical_output(value: upload::UploadOutput) -> model::UploadOutput {
        value
    }
    let _: fn(upload::UploadOutput) -> model::UploadOutput = canonical_output;
}

#[test]
fn fluent_fields_use_upstream_getters_and_preserve_collection_and_sensitive_semantics() {
    let tm = Client::new(
        Config::builder()
            .s3_config(common::s3_config(
                aws_smithy_mocks::create_mock_http_client(),
            ))
            .build(),
    );
    let builder = tm
        .upload()
        .bucket("bucket")
        .key("key")
        .metadata("one", "1")
        .metadata("two", "2")
        .sse_kms_key_id("sensitive-upload-kms-sentinel")
        .body(InputStream::from_static(b"payload"))
        .checksum_strategy(ChecksumStrategy::default());
    assert_eq!(builder.get_bucket(), &Some("bucket".to_owned()));
    assert_eq!(builder.get_key().as_deref(), Some("key"));
    assert_eq!(builder.get_metadata().as_ref().unwrap().len(), 2);
    assert_eq!(
        builder.get_body().as_ref().unwrap().size_hint().upper(),
        Some(7)
    );
    assert!(builder.get_checksum_strategy().is_some());
    assert!(!format!("{builder:?}").contains("sensitive-upload-kms-sentinel"));
    let builder = builder
        .set_metadata(None)
        .set_body(None)
        .set_checksum_strategy(None);
    assert!(builder.get_metadata().is_none());
    assert!(builder.get_body().is_none());
    assert!(builder.get_checksum_strategy().is_none());
    assert!(tm.upload().key("key").initiate().is_err());
    assert!(tm.upload().bucket("bucket").initiate().is_err());
}

#[cfg(feature = "sdk-v1")]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn fluent_and_modeled_builder_initiation_return_canonical_metadata_and_transfer_snapshot() {
    for fluent in [true, false] {
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(|request| {
                assert_eq!(request.bucket(), Some("bucket"));
                assert_eq!(request.key(), Some("key"));
                // The SDK supplies PUT Content-Length from the native body during serialization.
                assert_eq!(request.content_length(), None);
                assert_eq!(request.body().size_hint(), (7, Some(7)));
                true
            })
            .then_output(|| {
                PutObjectOutput::builder()
                    .e_tag("etag")
                    .bucket_key_enabled(false)
                    .checksum_sha512("server-sha512")
                    .build()
            });
        let sdk = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&put]);
        let tm = Client::new(
            Config::builder()
                .s3_config(test_common::s3_config_with_test_http(
                    sdk.config().to_builder(),
                ))
                .build(),
        );
        let handle = if fluent {
            tm.upload()
                .bucket("bucket")
                .key("key")
                .body(InputStream::from_static(b"payload"))
                .initiate()
                .unwrap()
        } else {
            model::UploadInput::builder()
                .bucket("bucket")
                .key("key")
                .body(InputStream::from_static(b"payload"))
                .initiate_with(&tm)
                .unwrap()
        };
        let output: model::UploadOutput = handle.join().await.unwrap();
        assert_eq!(output.e_tag(), Some("etag"));
        assert_eq!(output.bucket_key_enabled(), Some(false));
        assert_eq!(output.checksum_sha512(), Some("server-sha512"));
        assert_eq!(output.bucket(), None);
        assert_eq!(output.key(), None);
        assert_eq!(output.size(), None);
        assert_eq!(output.metrics().total_bytes, Some(7));
        assert_eq!(output.metrics().network_tx, 0);
    }
}

#[derive(Debug)]
#[cfg(feature = "sdk-v1")]
struct SinglePart(Option<Bytes>);

#[cfg(feature = "sdk-v1")]
impl PartStream for SinglePart {
    fn poll_part(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(PartData::new(1, bytes))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(7)
    }
}

#[cfg(feature = "sdk-v1")]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn multipart_runtime_merges_response_metadata_without_synthesizing_absent_values() {
    for complete_identity in [true, false] {
        let create = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("upload")
                .bucket("initiation-bucket")
                .key("initiation-key")
                .abort_date(DateTime::from_secs(123))
                .abort_rule_id("lifecycle-rule")
                .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
                .bucket_key_enabled(true)
                .ssekms_encryption_context("initiation-context")
                .build()
        });
        let part = mock!(aws_sdk_s3::Client::upload_part)
            .match_requests(|request| {
                request.upload_id() == Some("upload") && request.content_length() == Some(7)
            })
            .then_output(|| UploadPartOutput::builder().e_tag("part-etag").build());
        let complete = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .match_requests(|request| {
                request.upload_id() == Some("upload") && request.mpu_object_size() == Some(7)
            })
            .then_output(move || {
                CompleteMultipartUploadOutput::builder()
                    .set_bucket(complete_identity.then(|| "completion-bucket".to_owned()))
                    .set_key(complete_identity.then(|| "completion-key".to_owned()))
                    .location("completion-location")
                    .e_tag("completion-etag")
                    .checksum_sha512("server-sha512")
                    .checksum_md5("server-md5")
                    .checksum_xxhash64("server-xxhash64")
                    .checksum_xxhash3("server-xxhash3")
                    .checksum_xxhash128("server-xxhash128")
                    .build()
            });
        let sdk = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&create, &part, &complete]
        );
        let tm = Client::new(
            Config::builder()
                .s3_config(test_common::s3_config_with_test_http(
                    sdk.config().to_builder(),
                ))
                .build(),
        );
        let output: upload::UploadOutput = tm
            .upload()
            .bucket("request-bucket")
            .key("request-key")
            .body(InputStream::from_part_stream(SinglePart(Some(
                Bytes::from_static(b"payload"),
            ))))
            .initiate()
            .unwrap()
            .join()
            .await
            .unwrap();
        assert_eq!(output.abort_date(), Some(&DateTime::from_secs(123)));
        assert_eq!(output.abort_rule_id(), Some("lifecycle-rule"));
        assert_eq!(
            output.checksum_algorithm(),
            Some(&model::ChecksumAlgorithm::Sha256)
        );
        assert_eq!(output.upload_id(), Some("upload"));
        assert_eq!(
            output.sse_kms_encryption_context(),
            Some("initiation-context")
        );
        assert_eq!(
            output.bucket(),
            complete_identity.then_some("completion-bucket")
        );
        assert_eq!(output.key(), complete_identity.then_some("completion-key"));
        assert_eq!(output.bucket_key_enabled(), None);
        assert_eq!(output.location(), Some("completion-location"));
        assert_eq!(output.e_tag(), Some("completion-etag"));
        assert_eq!(output.checksum_sha512(), Some("server-sha512"));
        assert_eq!(output.checksum_md5(), Some("server-md5"));
        assert_eq!(output.checksum_xxhash64(), Some("server-xxhash64"));
        assert_eq!(output.checksum_xxhash3(), Some("server-xxhash3"));
        assert_eq!(output.checksum_xxhash128(), Some("server-xxhash128"));
        assert_eq!(output.size(), None);
        assert_eq!(output.metrics().network_tx, 7);
        assert_eq!(create.num_calls(), 1);
        assert_eq!(part.num_calls(), 1);
        assert_eq!(complete.num_calls(), 1);
    }
}
