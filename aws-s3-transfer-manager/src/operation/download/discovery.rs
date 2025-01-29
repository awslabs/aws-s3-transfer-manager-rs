/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use std::cmp::min;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::{cmp, mem};
use tracing::Instrument;

use super::chunk_meta::ChunkMetadata;
use super::object_meta::ObjectMetadata;
use super::DownloadContext;
use super::DownloadInput;
use crate::error;
use crate::http::header::{self, ByteRange};

#[derive(Debug, Clone, PartialEq)]
enum ObjectDiscoveryStrategy {
    // Send a `HeadObject` request.
    // The overall transfer is optionally constrained to the given range.
    HeadObject(Option<ByteRange>),
    // Send `GetObject` request using a ranged get.
    // The overall transfer is optionally constrained to the given range.
    RangedGet(Option<RangeInclusive<u64>>),
}

/// Discovered object metadata (optionally with first chunk of data)
#[derive(Debug)]
pub(super) struct ObjectDiscovery {
    /// range of data remaining to be fetched
    pub(super) remaining: Option<RangeInclusive<u64>>,

    /// the discovered metadata
    pub(super) chunk_meta: Option<ChunkMetadata>,
    pub(super) object_meta: ObjectMetadata,

    /// the first chunk of data if fetched during discovery
    pub(super) initial_chunk: Option<ByteStream>,
}

impl ObjectDiscoveryStrategy {
    fn from_request(input: &DownloadInput) -> Result<ObjectDiscoveryStrategy, crate::error::Error> {
        let strategy = match input.range() {
            Some(h) => {
                let byte_range = header::Range::from_str(h)?.0;
                match byte_range {
                    ByteRange::Inclusive(start, end) => {
                        ObjectDiscoveryStrategy::RangedGet(Some(start..=end))
                    }
                    // TODO(aws-sdk-rust#1159): explore when given a start range what it would like to just start
                    // sending requests from [start, start+part_size]
                    _ => ObjectDiscoveryStrategy::HeadObject(Some(byte_range)),
                }
            }
            None => ObjectDiscoveryStrategy::RangedGet(None),
        };

        Ok(strategy)
    }
}

/// Discover metadata about an object.
///
/// Returns object metadata, the remaining range of data
/// to be fetched, and _(if available)_ the first chunk of data.
pub(super) async fn discover_obj(
    ctx: &DownloadContext,
    input: &DownloadInput,
) -> Result<ObjectDiscovery, crate::error::Error> {
    let strategy = ObjectDiscoveryStrategy::from_request(input)?;
    tracing::trace!("discovering object with strategy {:?}", strategy);
    let discovery = match strategy {
        ObjectDiscoveryStrategy::HeadObject(byte_range) => {
            discover_obj_with_head(ctx, input, byte_range)
                .instrument(tracing::debug_span!("send-head-object-for-discovery"))
                .await
        }
        ObjectDiscoveryStrategy::RangedGet(range) => {
            discover_obj_with_get(ctx, input, range)
                .instrument(tracing::debug_span!("send-ranged-get-for-discovery"))
                .await
        }
    }?;

    tracing::trace!(
        "discovered object, remaining: {:?}; initial chunk set: {}",
        discovery.remaining,
        discovery.initial_chunk.is_some()
    );

    Ok(discovery)
}

async fn discover_obj_with_get_first_part(
    ctx: &DownloadContext,
    input: &DownloadInput,
) -> Result<ObjectDiscovery, error::Error> {
    // Get object first part.
    let builder: GetObjectInputBuilder = input.clone().into();
    // S3 index starts with 1
    let resp = builder
        .set_range(None)
        .set_part_number(Some(1))
        .send_with(ctx.client())
        .await
        .map_err(error::discovery_failed)?;
    first_chunk_response_handler(resp, None)
}

async fn discover_obj_with_head(
    ctx: &DownloadContext,
    input: &DownloadInput,
    _range_from_user: Option<ByteRange>,
) -> Result<ObjectDiscovery, crate::error::Error> {
    let resp = ctx
        .client()
        .head_object()
        .set_range(input.range.clone())
        .set_bucket(input.bucket().map(str::to_string))
        .set_key(input.key().map(str::to_string))
        .send()
        .await
        .map_err(error::discovery_failed)?;
    let object_meta: ObjectMetadata = resp.into();

    Ok(ObjectDiscovery {
        remaining: object_meta.range(),
        chunk_meta: None,
        object_meta,
        initial_chunk: None,
    })
}

async fn discover_obj_with_get(
    ctx: &DownloadContext,
    input: &DownloadInput,
    range_from_user: Option<RangeInclusive<u64>>,
) -> Result<ObjectDiscovery, error::Error> {
    // Convert input to builder and set the range properly as the first range get.
    let byte_range = match range_from_user.as_ref() {
        Some(r) => ByteRange::Inclusive(
            *r.start(),
            cmp::min(*r.start() + ctx.target_part_size_bytes() - 1, *r.end()),
        ),
        None => ByteRange::Inclusive(0, ctx.target_part_size_bytes() - 1),
    };
    let builder: GetObjectInputBuilder = input.clone().into();
    let resp = builder
        .range(header::Range::bytes(byte_range))
        .send_with(ctx.client())
        .await;
    match resp {
        Err(error) => {
            match error.as_service_error() {
                Some(service_error)
                    if service_error.meta().code() == Some("InvalidRange")
                        && range_from_user.is_none() =>
                {
                    // Invalid Range Error found and no Range passed in it's an empty object.
                    // discover the object with the first part instead for empty object.
                    discover_obj_with_get_first_part(ctx, input).await
                }
                _ => Err(error::discovery_failed(error)),
            }
        }
        Ok(response) => first_chunk_response_handler(response, range_from_user),
    }
}

fn first_chunk_response_handler(
    mut resp: GetObjectOutput,
    range_from_user: Option<RangeInclusive<u64>>,
) -> Result<ObjectDiscovery, error::Error> {
    let empty_stream = ByteStream::new(SdkBody::empty());
    let body = mem::replace(&mut resp.body, empty_stream);
    let object_meta: ObjectMetadata = (&resp).into();
    let chunk_meta: ChunkMetadata = resp.into();
    let chunk_content_len = chunk_meta
        .content_length
        .expect("expected content_length in chunk") as u64;
    let remaining = object_meta
        .content_length()
        .checked_sub(1)
        .map(|object_end| match range_from_user {
            Some(range_from_user) => {
                (*range_from_user.start() + chunk_content_len)
                    ..=min(object_end, *range_from_user.end())
            }
            // If no range is provided, the range is from the end of the chunk to the end of the object.
            // When the chunk is the last part of the object, this result in empty range.
            None => chunk_content_len..=object_end,
        });
    let initial_chunk = match chunk_content_len == 0 {
        true => None,
        false => Some(body),
    };

    Ok(ObjectDiscovery {
        remaining,
        chunk_meta: Some(chunk_meta),
        object_meta,
        initial_chunk,
    })
}

#[cfg(test)]
mod tests {
    use crate::http::header::ByteRange;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::download::discovery::{
        discover_obj, discover_obj_with_head, ObjectDiscoveryStrategy,
    };
    use crate::operation::download::DownloadContext;
    use crate::operation::download::DownloadInput;
    use crate::types::PartSize;
    use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::Client;
    use aws_smithy_mocks_experimental::mock;
    use aws_smithy_types::byte_stream::ByteStream;
    use aws_smithy_types::error::ErrorMetadata;
    use bytes::Buf;
    use std::sync::Arc;
    use test_common::mock_client_with_stubbed_http_client;

    use super::ObjectDiscovery;

    fn strategy_from_range(range: Option<&str>) -> ObjectDiscoveryStrategy {
        let input = DownloadInput::builder()
            .set_range(range.map(|r| r.to_string()))
            .build()
            .unwrap();

        ObjectDiscoveryStrategy::from_request(&input).unwrap()
    }

    fn test_handle(
        client: aws_sdk_s3::Client,
        target_part_size: u64,
    ) -> Arc<crate::client::Handle> {
        let tm_config = crate::Config::builder()
            .client(client)
            .set_target_part_size(PartSize::Target(target_part_size))
            .build();
        let tm = crate::Client::new(tm_config);
        tm.handle.clone()
    }

    #[test]
    fn test_strategy_from_req() {
        assert_eq!(
            ObjectDiscoveryStrategy::RangedGet(None),
            strategy_from_range(None)
        );

        assert_eq!(
            ObjectDiscoveryStrategy::RangedGet(Some(100..=200)),
            strategy_from_range(Some("bytes=100-200"))
        );
        assert_eq!(
            ObjectDiscoveryStrategy::HeadObject(Some(ByteRange::AllFrom(100))),
            strategy_from_range(Some("bytes=100-"))
        );
        assert_eq!(
            ObjectDiscoveryStrategy::HeadObject(Some(ByteRange::Last(500))),
            strategy_from_range(Some("bytes=-500"))
        );
    }

    async fn get_discovery_from_head(range: Option<ByteRange>) -> ObjectDiscovery {
        let head_obj_rule = mock!(Client::head_object)
            .then_output(|| HeadObjectOutput::builder().content_length(500).build());
        let client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&head_obj_rule]);

        let ctx = DownloadContext::new(test_handle(client, 5 * ByteUnit::Mebibyte.as_bytes_u64()));

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        discover_obj_with_head(&ctx, &input, range).await.unwrap()
    }

    #[tokio::test]
    async fn test_discover_obj_with_head() {
        assert_eq!(
            0..=499,
            get_discovery_from_head(None).await.remaining.unwrap()
        );
        assert_eq!(
            10..=100,
            get_discovery_from_head(Some(ByteRange::Inclusive(10, 100)))
                .await
                .remaining
                .unwrap()
        );
        assert_eq!(
            10..=499,
            get_discovery_from_head(Some(ByteRange::Inclusive(10, 10000)))
                .await
                .remaining
                .unwrap()
        );
        assert_eq!(
            100..=499,
            get_discovery_from_head(Some(ByteRange::AllFrom(100)))
                .await
                .remaining
                .unwrap()
        );
        assert_eq!(
            400..=499,
            get_discovery_from_head(Some(ByteRange::Last(100)))
                .await
                .remaining
                .unwrap()
        );
        assert_eq!(
            0..=499,
            get_discovery_from_head(Some(ByteRange::Last(500)))
                .await
                .remaining
                .unwrap()
        );
        assert_eq!(
            0..=499,
            get_discovery_from_head(Some(ByteRange::Last(5000)))
                .await
                .remaining
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_discover_obj_with_get_full_range() {
        let target_part_size = 500;
        let bytes = &[0u8; 500];
        let get_obj_rule = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=0-499"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .content_length(500)
                    .content_range("0-499/700")
                    .body(ByteStream::from_static(bytes))
                    .build()
            });
        let client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&get_obj_rule]);

        let ctx = DownloadContext::new(test_handle(client, target_part_size));

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let discovery = discover_obj(&ctx, &request).await.unwrap();
        let remaining = discovery.remaining.unwrap();
        assert_eq!(200, remaining.clone().count());
        assert_eq!(500..=699, remaining);

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(500, initial_chunk.remaining());
    }

    #[tokio::test]
    async fn test_discover_obj_with_get_single_part() {
        let target_part_size = 500;
        let bytes = &[0u8; 400];
        let get_obj_rule = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=0-499"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .content_length(400)
                    .content_range("0-399/400")
                    .body(ByteStream::from_static(bytes))
                    .build()
            });
        let client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&get_obj_rule]);

        let ctx = DownloadContext::new(test_handle(client, target_part_size));

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let discovery = discover_obj(&ctx, &request).await.unwrap();

        let remaining = discovery.remaining.unwrap();
        assert_eq!(0, remaining.clone().count());

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(400, initial_chunk.remaining());
    }

    #[tokio::test]
    async fn test_discover_obj_with_get_partial_range() {
        let target_part_size = 100;
        let bytes = &[0u8; 100];
        let get_obj_rule = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=200-299"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .content_length(100)
                    .content_range("200-299/700")
                    .body(ByteStream::from_static(bytes))
                    .build()
            });
        let client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&get_obj_rule]);

        let ctx = DownloadContext::new(test_handle(client, target_part_size));

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=200-499")
            .build()
            .unwrap();

        let discovery = discover_obj(&ctx, &request).await.unwrap();
        let remaining = discovery.remaining.unwrap();
        assert_eq!(200, remaining.clone().count());
        assert_eq!(300..=499, remaining);

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(100, initial_chunk.remaining());
    }

    #[tokio::test]
    async fn test_discover_obj_with_get_over_range() {
        let target_part_size = 100;
        let bytes = &[0u8; 50];
        let get_obj_rule = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=200-299"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .content_length(50)
                    .content_range("200-249/250")
                    .body(ByteStream::from_static(bytes))
                    .build()
            });
        let client = mock_client_with_stubbed_http_client!(aws_sdk_s3, &[&get_obj_rule]);

        let ctx = DownloadContext::new(test_handle(client, target_part_size));

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=200-123456")
            .build()
            .unwrap();

        let discovery = discover_obj(&ctx, &request).await.unwrap();
        assert!(discovery.remaining.is_none());

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(50, initial_chunk.remaining());
    }

    #[tokio::test]
    async fn test_discover_obj_with_empty_object() {
        let target_part_size = 500;
        let get_range_rule = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=0-499"))
            .then_error(|| {
                GetObjectError::generic(ErrorMetadata::builder().code("InvalidRange").build())
            });
        let get_first_part_rule = mock!(Client::get_object)
            .match_requests(|r| r.part_number() == Some(1))
            .then_output(|| GetObjectOutput::builder().content_length(0).build());
        let client = mock_client_with_stubbed_http_client!(
            aws_sdk_s3,
            &[&get_range_rule, &get_first_part_rule]
        );

        let ctx = DownloadContext::new(test_handle(client, target_part_size));

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let discovery = discover_obj(&ctx, &request).await.unwrap();
        assert!(discovery.remaining.is_none());
        assert!(discovery.initial_chunk.is_none());
    }
}
