// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

#[path = "../../test-model/model/src/model/mod.rs"]
pub mod model;

// These doubles intentionally reproduce the runtime types' relevant trait constraints.
pub mod io {
    #[derive(Debug, Default)]
    pub struct InputStream(pub Vec<u8>);
}
pub mod types {
    #[derive(Debug, Clone, PartialEq)]
    pub enum ReadAhead {
        Parts(usize),
    }
    #[derive(Debug, Clone)]
    pub enum FailedMultipartUploadPolicy {
        Abort,
    }
    #[derive(Debug, Clone)]
    pub struct TransferMetrics(pub u64);
}
pub mod operation {
    pub mod upload {
        #[derive(Debug, Clone)]
        pub struct ChecksumStrategy;
    }
}

#[cfg(test)]
mod tests {
    use super::{io::InputStream, model::*, operation::upload::ChecksumStrategy, types::*};

    #[test]
    fn tm_fields_set_unset_get_construct_and_debug_in_both_input_builders() {
        let expires = aws_smithy_types::DateTime::from_secs(123);
        let download = DownloadInput::builder()
            .bucket("bucket")
            .key("key")
            .read_ahead(ReadAhead::Parts(3))
            .set_read_ahead(None)
            .build()
            .unwrap();
        assert!(download.read_ahead().is_none());
        let builder = DownloadInput::builder().read_ahead(ReadAhead::Parts(2));
        assert_eq!(builder.get_read_ahead(), &Some(ReadAhead::Parts(2)));
        assert!(format!("{builder:?}").contains("read_ahead"));
        let mut upload = UploadInput::builder()
            .bucket("bucket")
            .key("key")
            .body(InputStream(vec![1, 2, 3]))
            .expires(expires)
            .checksum_strategy(ChecksumStrategy)
            .failed_multipart_upload_policy(FailedMultipartUploadPolicy::Abort)
            .build()
            .unwrap();
        assert_eq!(upload.body().0, vec![1, 2, 3]);
        assert_eq!(upload.expires(), Some(&expires));
        assert!(upload.checksum_strategy().is_some());
        assert!(upload.failed_multipart_upload_policy().is_some());
        assert!(format!("{upload:?}").contains("checksum_strategy"));
        assert_eq!(upload.take_body().0, vec![1, 2, 3]);
        assert!(upload.body().0.is_empty());
        let builder = UploadInput::builder()
            .set_body(None)
            .set_checksum_strategy(None)
            .set_failed_multipart_upload_policy(None);
        assert!(builder.get_body().is_none());
        assert!(builder.get_checksum_strategy().is_none());
        assert!(builder.get_failed_multipart_upload_policy().is_none());
        assert!(builder
            .bucket("bucket")
            .key("key")
            .build()
            .unwrap()
            .body()
            .0
            .is_empty());
    }

    #[test]
    fn metrics_are_required_and_missing_metrics_return_a_build_error() {
        assert!(UploadOutput::builder().build().is_err());
        let builder = UploadOutput::builder().metrics(TransferMetrics(42));
        assert_eq!(builder.get_metrics().as_ref().unwrap().0, 42);
        assert!(builder.clone().set_metrics(None).build().is_err());
        let output = builder.e_tag("etag").build().unwrap();
        assert_eq!(output.metrics().0, 42);
        assert_eq!(output.clone().e_tag(), Some("etag"));
    }

    #[test]
    fn object_lengths_and_request_identifiers_keep_their_internal_construction() {
        let object = ObjectMetadata::builder()
            .content_length(42)
            .request_id("request")
            .extended_request_id("extended")
            .build();
        assert_eq!(object.content_length(), Some(42));
        assert_eq!(object.content_length, Some(42));
        assert_eq!(object.request_id(), Some("request"));
        assert_eq!(object.extended_request_id(), Some("extended"));
        let chunk = ChunkMetadata::builder()
            .set_request_id(Some("chunk".into()))
            .build();
        assert_eq!(chunk.request_id(), Some("chunk"));
        let builder = ChunkMetadata::builder()
            .request_id("chunk")
            .extended_request_id("extended chunk")
            .set_request_id(None);
        assert_eq!(builder.get_request_id(), &None);
        assert_eq!(
            builder.get_extended_request_id().as_deref(),
            Some("extended chunk")
        );
        let chunk = builder.set_extended_request_id(None).build();
        assert_eq!(chunk.request_id(), None);
        assert_eq!(chunk.extended_request_id(), None);
        assert_eq!(
            ObjectMetadata::builder()
                .expires_string("invalid date")
                .build()
                .expires_string(),
            Some("invalid date")
        );
    }
}
