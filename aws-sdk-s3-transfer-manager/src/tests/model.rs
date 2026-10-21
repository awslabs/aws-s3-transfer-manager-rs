/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::InputStream;
use crate::model::{ChunkMetadata, ObjectMetadata, UploadInput, UploadOutput};
use crate::types::TransferMetrics;

#[test]
fn modeled_upload_uses_the_real_stream_and_take_body_leaves_an_empty_stream() {
    let mut input = UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .body(InputStream::from_static(b"payload"))
        .build()
        .unwrap();
    assert_eq!(input.body().size_hint().upper(), Some(7));
    let body = input.take_body();
    assert_eq!(body.size_hint().upper(), Some(7));
    assert_eq!(input.body().size_hint().upper(), Some(0));
}

#[test]
fn aggregate_metadata_has_internal_lengths_and_request_identifier_construction() {
    let object = ObjectMetadata::builder()
        .content_length(42)
        .content_range("bytes 0-41/42")
        .request_id("request")
        .extended_request_id("extended")
        .expires_string("invalid HTTP date")
        .build();
    assert_eq!(object.content_length(), Some(42));
    assert_eq!(object.content_range(), Some("bytes 0-41/42"));
    assert_eq!(object.request_id(), Some("request"));
    assert_eq!(object.extended_request_id(), Some("extended"));
    assert_eq!(object.expires_string(), Some("invalid HTTP date"));
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
}

#[test]
fn upload_output_validates_and_preserves_real_transfer_metrics() {
    assert!(UploadOutput::builder().build().is_err());
    let metrics = TransferMetrics {
        network_tx: 42,
        network_rx: 0,
        disk_read: 42,
        disk_write: 0,
        total_bytes: Some(42),
        started_at: std::time::Instant::now(),
        finished_at: None,
    };
    let builder = UploadOutput::builder().metrics(metrics);
    assert_eq!(builder.get_metrics().as_ref().unwrap().network_tx, 42);
    assert!(builder.clone().set_metrics(None).build().is_err());
    let output = builder
        .e_tag("tag")
        .upload_id("upload")
        .location("location")
        .build()
        .unwrap();
    assert_eq!(output.metrics().network_tx, 42);
    assert_eq!(output.clone().upload_id(), Some("upload"));
    assert_eq!(output.location(), Some("location"));
}
