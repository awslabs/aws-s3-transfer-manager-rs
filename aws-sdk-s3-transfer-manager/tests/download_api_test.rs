/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::{get_object::GetObjectOutput, head_object::HeadObjectOutput};
use aws_sdk_s3_transfer_manager::{
    model,
    operation::download,
    types::{ChecksumValidation, NotValidatedReason, ReadAhead},
    Client, Config,
};
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_types::{byte_stream::ByteStream, DateTime};
use bytes::Buf;

#[test]
fn operation_exports_are_canonical_and_preserve_construction_and_empty_metadata() {
    let builder: download::DownloadInputBuilder = model::DownloadInput::builder();
    let builder: model::builders::DownloadInputBuilder = builder
        .bucket("bucket")
        .key("key")
        .read_ahead(ReadAhead::Parts(3));
    assert_eq!(builder.get_bucket(), &Some("bucket".to_owned()));
    let input: download::DownloadInput = builder.build().unwrap();
    let input: model::DownloadInput = input;
    let builder: download::DownloadInputBuilder = input.clone().into();
    assert_eq!(input, builder.build().unwrap());
    assert_eq!(input.part_number(), None);
    assert_eq!(input.read_ahead(), Some(&ReadAhead::Parts(3)));
    assert!(model::DownloadInput::builder().key("key").build().is_err());
    assert!(model::DownloadInput::builder()
        .bucket("bucket")
        .build()
        .is_err());
    let object: download::ObjectMetadata = model::ObjectMetadata::default();
    let chunk: download::ChunkMetadata = model::ChunkMetadata::default();
    assert_eq!(object, model::ObjectMetadata::builder().build());
    assert_eq!(chunk, model::ChunkMetadata::builder().build());
    assert_eq!(object.delete_marker(), None);
    assert_eq!(object.missing_meta(), None);
    assert_eq!(object.bucket_key_enabled(), None);
    assert_eq!(object.request_id(), None);
    assert_eq!(object.extended_request_id(), None);
    assert_eq!(chunk.content_length(), None);
    assert_eq!(chunk.request_id(), None);
}

#[test]
fn fluent_fields_use_upstream_getters_and_preserve_conditions_read_ahead_and_redaction() {
    let tm = Client::new(
        Config::builder()
            .client(mock_client!(aws_sdk_s3, []))
            .build(),
    );
    let since = DateTime::from_secs(123);
    let builder = tm
        .download()
        .bucket("bucket")
        .key("key")
        .if_modified_since(since)
        .range("bytes=1-3")
        .response_content_type("application/octet-stream")
        .sse_customer_key("sensitive-download-key-sentinel")
        .checksum_mode(model::ChecksumMode::Enabled)
        .read_ahead(ReadAhead::Parts(3));
    assert_eq!(builder.get_bucket(), &Some("bucket".to_owned()));
    assert_eq!(builder.get_key().as_deref(), Some("key"));
    assert_eq!(builder.get_if_modified_since(), &Some(since));
    assert_eq!(builder.get_range().as_deref(), Some("bytes=1-3"));
    assert_eq!(builder.get_read_ahead(), &Some(ReadAhead::Parts(3)));
    assert!(!format!("{builder:?}").contains("sensitive-download-key-sentinel"));
    let builder = builder
        .set_range(None)
        .set_response_content_type(None)
        .set_checksum_mode(None)
        .set_read_ahead(None);
    assert!(builder.get_range().is_none());
    assert!(builder.get_response_content_type().is_none());
    assert!(builder.get_checksum_mode().is_none());
    assert!(builder.get_read_ahead().is_none());
    assert!(tm.download().key("key").initiate().is_err());
    assert!(tm.download().bucket("bucket").initiate().is_err());
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn fluent_and_modeled_initiation_return_canonical_object_and_chunk_metadata() {
    for fluent in [true, false] {
        let get = mock!(aws_sdk_s3::Client::get_object)
            .match_requests(|input| {
                assert_eq!(input.bucket(), Some("bucket"));
                assert_eq!(input.key(), Some("key"));
                assert_eq!(input.version_id(), Some("version"));
                assert_eq!(
                    input.response_content_type(),
                    Some("application/octet-stream")
                );
                assert_eq!(input.part_number(), None);
                true
            })
            .then_output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(b"payload"))
                    .content_length(7)
                    .e_tag("etag")
                    .delete_marker(false)
                    .bucket_key_enabled(false)
                    .missing_meta(0)
                    .tag_count(2)
                    .checksum_sha512("server-sha512")
                    .checksum_md5("server-md5")
                    .expires_string("unparsed-service-date")
                    .request_charged(aws_sdk_s3::types::RequestCharged::Requester)
                    .build()
            });
        let sdk = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&get]);
        let tm = Client::new(Config::builder().client(sdk).build());
        let mut handle = if fluent {
            tm.download()
                .bucket("bucket")
                .key("key")
                .version_id("version")
                .response_content_type("application/octet-stream")
                .read_ahead(ReadAhead::Parts(2))
                .initiate()
                .unwrap()
        } else {
            model::DownloadInput::builder()
                .bucket("bucket")
                .key("key")
                .version_id("version")
                .response_content_type("application/octet-stream")
                .read_ahead(ReadAhead::Parts(2))
                .initiate_with(&tm)
                .unwrap()
        };
        let metadata: &model::ObjectMetadata = handle.object_meta().await.unwrap();
        assert_eq!(metadata.total_object_size(), 7);
        assert_eq!(metadata.e_tag(), Some("etag"));
        assert_eq!(metadata.delete_marker(), Some(false));
        assert_eq!(metadata.bucket_key_enabled(), Some(false));
        assert_eq!(metadata.missing_meta(), Some(0));
        assert_eq!(metadata.tag_count(), Some(2));
        assert_eq!(metadata.archive_status(), None);
        assert_eq!(metadata.checksum_sha512(), Some("server-sha512"));
        assert_eq!(metadata.checksum_md5(), Some("server-md5"));
        assert_eq!(metadata.expires_string(), Some("unparsed-service-date"));
        assert_eq!(
            metadata.request_charged(),
            Some(&model::RequestCharged::Requester)
        );
        let mut chunk = handle.body_mut().next().await.unwrap().unwrap();
        let metadata: &model::ChunkMetadata = &chunk.metadata;
        assert_eq!(metadata.content_length(), Some(7));
        assert_eq!(metadata.checksum_sha512(), Some("server-sha512"));
        assert_eq!(metadata.checksum_md5(), Some("server-md5"));
        assert_eq!(
            chunk.data.copy_to_bytes(chunk.data.remaining()).as_ref(),
            b"payload"
        );
        assert!(handle.body_mut().next().await.is_none());
        let output: download::DownloadOutput = handle.join().await.unwrap();
        assert!(matches!(
            output.integrity_checks().checksum_validation(),
            ChecksumValidation::NotValidated { .. }
        ));
        let metadata: model::ObjectMetadata = output.object_meta;
        assert_eq!(metadata.total_object_size(), 7);
        assert_eq!(metadata.checksum_sha512(), Some("server-sha512"));
        assert_eq!(output.metrics.total_bytes, Some(7));
        assert_eq!(output.metrics.network_rx, 7);
    }
}

#[cfg(any(unix, windows))]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn managed_suffix_download_preserves_head_discovery_metadata_and_absolute_range() {
    let head = mock!(aws_sdk_s3::Client::head_object)
        .match_requests(|input| {
            assert_eq!(input.range(), Some("bytes=-3"));
            assert_eq!(input.version_id(), Some("version"));
            true
        })
        .then_output(|| {
            HeadObjectOutput::builder()
                .content_length(3)
                .content_range("bytes 4-6/7")
                .e_tag("etag")
                .checksum_sha512("head-sha512")
                .archive_status(aws_sdk_s3::types::ArchiveStatus::ArchiveAccess)
                .build()
        });
    let get = mock!(aws_sdk_s3::Client::get_object)
        .match_requests(|input| {
            assert_eq!(input.range(), Some("bytes=4-6"));
            assert_eq!(input.if_match(), Some("etag"));
            assert_eq!(input.version_id(), Some("version"));
            true
        })
        .then_output(|| {
            GetObjectOutput::builder()
                .content_length(3)
                .content_range("bytes 4-6/7")
                .e_tag("etag")
                .checksum_sha512("get-sha512")
                .body(ByteStream::from_static(b"oad"))
                .build()
        });
    let sdk = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&head, &get]);
    let tm = Client::new(Config::builder().client(sdk).build());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("download");
    let handle = tm
        .download()
        .bucket("bucket")
        .key("key")
        .version_id("version")
        .range("bytes=-3")
        .read_ahead(ReadAhead::Parts(2))
        .write_to_path(&path)
        .await
        .unwrap();
    let metadata: &model::ObjectMetadata = handle.object_meta().await.unwrap();
    assert_eq!(metadata.total_object_size(), 7);
    assert_eq!(
        metadata.archive_status(),
        Some(&model::ArchiveStatus::ArchiveAccess)
    );
    assert_eq!(metadata.checksum_sha512(), Some("head-sha512"));
    let output = handle.join().await.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"oad");
    assert_eq!(output.object_meta.total_object_size(), 7);
    assert_eq!(output.object_meta.checksum_sha512(), Some("head-sha512"));
    assert_eq!(output.metrics.total_bytes, Some(3));
    assert_eq!(output.metrics.network_rx, 3);
    assert!(matches!(
        output.integrity_checks().checksum_validation(),
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Disabled | NotValidatedReason::Unavailable,
            ..
        }
    ));
}

#[cfg(feature = "sdk-v1")]
#[test]
fn sdk_interoperability_uses_the_operation_reexports() {
    let response = GetObjectOutput::builder()
        .content_length(0)
        .delete_marker(false)
        .build();
    let object: download::ObjectMetadata = (&response).into();
    let chunk: download::ChunkMetadata = (&response).into();
    assert_eq!(object.total_object_size(), 0);
    assert_eq!(chunk.content_length(), Some(0));
    assert_eq!(object.delete_marker(), Some(false));
    fn sdk_identifier<T: aws_sdk_s3::operation::RequestId + aws_sdk_s3::operation::RequestIdExt>(
        value: &T,
    ) {
        assert_eq!(value.request_id(), None);
        assert_eq!(value.extended_request_id(), None);
    }
    sdk_identifier(&object);
    sdk_identifier(&chunk);
}
