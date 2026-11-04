// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Checksum mode: GetObject and HeadObject return checksums only when asked.

mod common;

use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::Client;
use common::{crc64nvme, Fixture, Result};

/// GetObject and HeadObject return checksums only when the request sets
/// `x-amz-checksum-mode: ENABLED`, as S3 does.
#[tokio::test]
async fn test_checksum_mode() -> Result {
    let f = Fixture::new().await;
    f.server.add_object(&f.bucket, "a", "hello", None).await?;
    let expected = crc64nvme(b"hello");

    let head = f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(head.checksum_crc64_nvme, None);
    let head =
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("a")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
    assert_eq!(head.checksum_crc64_nvme.as_deref(), Some(expected.as_str()));

    // The SDK enables checksum mode on GET by default, unless response validation is only
    // performed when required.
    let get = f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(get.checksum_crc64_nvme.as_deref(), Some(expected.as_str()));
    let when_required = Client::from_conf(
        f.s3.config()
            .to_builder()
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build(),
    );
    let get = when_required
        .get_object()
        .bucket(&f.bucket)
        .key("a")
        .send()
        .await?;
    assert_eq!(get.checksum_crc64_nvme, None);
    let get = when_required
        .get_object()
        .bucket(&f.bucket)
        .key("a")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(get.checksum_crc64_nvme.as_deref(), Some(expected.as_str()));

    f.shutdown().await;
    Ok(())
}
