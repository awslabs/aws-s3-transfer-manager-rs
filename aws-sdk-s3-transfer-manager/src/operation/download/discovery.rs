/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::{cmp, mem};
use tracing::Instrument;

use super::chunk_meta::ChunkMetadata;
use super::input::copy_fields_to_get_object_request;
use super::object_meta::ObjectMetadata;
use super::transfer::DownloadTransfer;
use super::DownloadInput;
use crate::error;
use crate::http::header::{self, ByteRange};

#[derive(Debug, Clone, PartialEq)]
enum ObjectDiscoveryStrategy {
    // Send a `HeadObject` request.
    // The overall transfer is optionally constrained to the given range.
    HeadObject,
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

    /// Per-chunk size the transfer should slice remaining ranges at. Normally the
    /// configured download part size; for a validated multipart object it is the
    /// object's stored part size so every range aligns to a stored part boundary
    /// (the precondition for per-part download validation).
    pub(super) effective_part_size: u64,
}

/// Parse the stored part count from an MPU ETag of the form `"<hash>-<N>"`.
/// Returns `None` for a single-part ETag (no `-N` suffix) or unparseable input.
fn parts_from_etag(etag: &str) -> Option<u32> {
    let trimmed = etag.trim_matches('"');
    let (_, n) = trimmed.rsplit_once('-')?;
    n.parse::<u32>().ok().filter(|&n| n > 1)
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
                    _ => ObjectDiscoveryStrategy::HeadObject,
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
    transfer: &DownloadTransfer,
    input: &DownloadInput,
    validation_enabled: bool,
) -> Result<ObjectDiscovery, crate::error::Error> {
    let configured_part_size = transfer.ctx().handle.download_part_size_bytes();
    let strategy = ObjectDiscoveryStrategy::from_request(input)?;
    tracing::trace!("discovering object with strategy {:?}", strategy);
    let user_explicit_part_size = transfer.ctx().handle.user_set_part_size();
    let mut discovery = match strategy {
        ObjectDiscoveryStrategy::HeadObject => {
            discover_obj_with_head(transfer, input)
                .instrument(tracing::debug_span!("send-head-object-for-discovery"))
                .await
        }
        ObjectDiscoveryStrategy::RangedGet(range) => {
            discover_obj_with_get(transfer, input, range)
                .instrument(tracing::debug_span!("send-ranged-get-for-discovery"))
                .await
        }
    }?;
    // Default: slice at the configured download part size. The alignment path
    // below overrides this with the stored part size when validating a multipart
    // object.
    discovery.effective_part_size = configured_part_size;

    // Align download ranges to the object's stored part size so each ranged GET
    // matches a stored part boundary and S3 returns the part's checksum for the
    // SDK to validate. Only for: validation on, no user range, a multipart object
    // (ETag carries `-N`), and the user did not pin an explicit part size.
    //
    // The initial ranged discovery fetched `[0, configured)`, whose chunk length
    // is the CONFIGURED size, not the stored part size, so it cannot tell us the
    // stored layout. Re-issue via partNumber=1, whose response reports the exact
    // first-part (= stored part) size via Content-Range; slice every range at that
    // size so all chunks align to stored boundaries (incl. the ragged tail). One
    // extra request, only on this path.
    //
    // TODO(vnext): a wire checksum over arbitrary response bytes removes the need
    // to align, so this partNumber re-issue can go away once that exists.
    let is_multipart = discovery
        .object_meta
        .e_tag
        .as_deref()
        .and_then(parts_from_etag)
        .is_some();
    if validation_enabled && input.range().is_none() && is_multipart && !user_explicit_part_size {
        let mut aligned = discover_obj_with_get_first_part(transfer, input).await?;
        let stored_part_size = aligned
            .chunk_meta
            .as_ref()
            .and_then(|m| m.content_length)
            .map(|len| len as u64)
            .filter(|&len| len != 0)
            .unwrap_or(configured_part_size);
        tracing::debug!(
            configured_part_size,
            stored_part_size,
            "realigned multipart download to stored part size for validation"
        );
        aligned.effective_part_size = stored_part_size;
        discovery = aligned;
    }

    tracing::trace!(
        remaining = ?discovery.remaining,
        initial_chunk = discovery.initial_chunk.is_some(),
        effective_part_size = discovery.effective_part_size,
        "discovered object",
    );

    Ok(discovery)
}

async fn discover_obj_with_get_first_part(
    transfer: &DownloadTransfer,
    input: &DownloadInput,
) -> Result<ObjectDiscovery, error::Error> {
    let builder = copy_fields_to_get_object_request(input, transfer.ctx().s3_client().get_object());
    // S3 index starts with 1
    let resp = builder
        .set_range(None)
        .set_part_number(Some(1))
        .customize()
        .config_override(crate::retry::bucket_partition_override(input.bucket()))
        .send()
        .await
        .map_err(error::Error::from)?;
    first_chunk_response_handler(resp, None)
}

async fn discover_obj_with_head(
    transfer: &DownloadTransfer,
    input: &DownloadInput,
) -> Result<ObjectDiscovery, crate::error::Error> {
    let resp = transfer
        .ctx()
        .s3_client()
        .head_object()
        .set_range(input.range.clone())
        .set_bucket(input.bucket().map(str::to_string))
        .set_key(input.key().map(str::to_string))
        .customize()
        .config_override(crate::retry::bucket_partition_override(input.bucket()))
        .send()
        .await
        .map_err(error::Error::from)?;
    let object_meta: ObjectMetadata = resp.into();

    Ok(ObjectDiscovery {
        remaining: object_meta.range_from_content_range(),
        chunk_meta: None,
        object_meta,
        initial_chunk: None,
        // Filled in by discover_obj (configured size, or stored size when aligning).
        effective_part_size: 0,
    })
}

async fn discover_obj_with_get(
    transfer: &DownloadTransfer,
    input: &DownloadInput,
    range_from_user: Option<RangeInclusive<u64>>,
) -> Result<ObjectDiscovery, error::Error> {
    let target_part_size = transfer.ctx().handle.download_part_size_bytes();
    // Convert input to builder and set the range properly as the first range get.
    let byte_range = match range_from_user.as_ref() {
        Some(r) => ByteRange::Inclusive(
            *r.start(),
            cmp::min(*r.start() + target_part_size - 1, *r.end()),
        ),
        None => ByteRange::Inclusive(0, target_part_size - 1),
    };
    let builder = copy_fields_to_get_object_request(input, transfer.ctx().s3_client().get_object());
    let resp = builder
        .range(header::Range::bytes(byte_range))
        .customize()
        .config_override(crate::retry::bucket_partition_override(input.bucket()))
        .send()
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
                    discover_obj_with_get_first_part(transfer, input).await
                }
                _ => Err(error::Error::from(error)),
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
        .ok_or_else(|| error::discovery_failed("response missing content-length"))?
        as u64;
    let remaining = object_meta
        .total_object_size()
        .checked_sub(1)
        .and_then(|object_end| {
            // Calculate start and end based on user range (if any)
            let (start, end) = range_from_user
                .map(|r| (*r.start() + chunk_content_len, (*r.end()).min(object_end)))
                .unwrap_or((chunk_content_len, object_end));

            // Only return a range if it's non-empty
            (start <= end).then_some(start..=end)
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
        // Filled in by discover_obj (configured size, or stored size when aligning).
        effective_part_size: 0,
    })
}

#[cfg(test)]
mod tests {
    use crate::metrics::unit::ByteUnit;
    use crate::operation::download::discovery::{
        discover_obj, discover_obj_with_head, ObjectDiscoveryStrategy,
    };
    use crate::operation::download::transfer::DownloadTransfer;
    use crate::operation::download::DownloadInput;
    use crate::transfer::TransferContext;
    use crate::types::BucketType;
    use crate::types::PartSize;
    use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::Client;
    use aws_smithy_mocks::{mock, mock_client};
    use aws_smithy_types::byte_stream::ByteStream;
    use aws_smithy_types::error::ErrorMetadata;
    use bytes::Buf;
    use std::sync::Arc;

    fn strategy_from_range(range: Option<&str>) -> ObjectDiscoveryStrategy {
        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
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

    // Handle with an Auto part size (download Auto = 5 MiB), so the multipart
    // alignment branch (gated on `!user_set_part_size`) can be exercised.
    fn test_handle_auto(client: aws_sdk_s3::Client) -> Arc<crate::client::Handle> {
        let tm_config = crate::Config::builder().client(client).build();
        let tm = crate::Client::new(tm_config);
        tm.handle.clone()
    }

    fn test_transfer(
        handle: Arc<crate::client::Handle>,
        input: &DownloadInput,
    ) -> DownloadTransfer {
        use crate::operation::download::body;
        let (writer, _consumer) = body::new_slot_body(body::DEFAULT_BODY_SLOT_CAPACITY);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        DownloadTransfer::new(ctx, BucketType::Standard, input.clone(), writer)
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
            ObjectDiscoveryStrategy::HeadObject,
            strategy_from_range(Some("bytes=100-"))
        );
        assert_eq!(
            ObjectDiscoveryStrategy::HeadObject,
            strategy_from_range(Some("bytes=-500"))
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_discover_obj_with_head() {
        // Returns the first 500 bytes from a 10MB object
        let head_obj_rule = mock!(Client::head_object).then_output(|| {
            HeadObjectOutput::builder()
                .content_length(500)
                .content_range("bytes 0-499/10485760")
                .build()
        });
        let client = mock_client!(aws_sdk_s3, &[&head_obj_rule]);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let transfer = test_transfer(
            test_handle(client, 5 * ByteUnit::Mebibyte.as_bytes_u64()),
            &input,
        );

        let discovery = discover_obj_with_head(&transfer, &input).await.unwrap();
        let remaining = discovery.remaining.unwrap();
        assert_eq!(500, remaining.clone().count());
        assert_eq!(0..=499, remaining);
    }

    #[cfg_attr(miri, ignore)]
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
        let client = mock_client!(aws_sdk_s3, &[&get_obj_rule]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let transfer = test_transfer(test_handle(client, target_part_size), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();
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

    #[cfg_attr(miri, ignore)]
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
        let client = mock_client!(aws_sdk_s3, &[&get_obj_rule]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let transfer = test_transfer(test_handle(client, target_part_size), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();
        assert!(discovery.remaining.is_none());

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(400, initial_chunk.remaining());
    }

    // A validating download of a multipart object (ETag `-N`) with an Auto part
    // size re-issues discovery via partNumber=1 to learn the exact stored part
    // size and aligns subsequent ranges to it. Download Auto = 5 MiB, but the
    // stored part is 8 MiB; effective_part_size must become 8 MiB.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_discover_obj_aligns_multipart_when_validating() {
        const MIB: u64 = 1024 * 1024;
        let download_default = 5 * MIB; // Auto download part size
        let stored_part = 8 * MIB;
        let total = 20 * MIB; // 8 + 8 + 4 -> 3 parts
        let ranged = mock!(Client::get_object)
            .match_requests(move |r| {
                r.range() == Some(format!("bytes=0-{}", download_default - 1).as_str())
                    && r.part_number().is_none()
            })
            .then_output(move || {
                GetObjectOutput::builder()
                    .content_length(download_default as i64)
                    .content_range(format!("0-{}/{}", download_default - 1, total))
                    .e_tag("\"abc-3\"")
                    .body(ByteStream::from_static(&[0u8; 8]))
                    .build()
            });
        let part1 = mock!(Client::get_object)
            .match_requests(|r| r.part_number() == Some(1))
            .then_output(move || {
                GetObjectOutput::builder()
                    .content_length(stored_part as i64)
                    .content_range(format!("0-{}/{}", stored_part - 1, total))
                    .e_tag("\"abc-3\"")
                    .parts_count(3)
                    .body(ByteStream::from_static(&[0u8; 8]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, &[&ranged, &part1]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let transfer = test_transfer(test_handle_auto(client), &request);

        let discovery = discover_obj(&transfer, &request, true).await.unwrap();

        // Aligned to the stored part size (8 MiB), not the Auto default (5 MiB).
        assert_eq!(discovery.effective_part_size, stored_part);
        // remaining starts at the first stored boundary (after part 1).
        assert_eq!(discovery.remaining, Some(stored_part..=total - 1));
    }

    // Without validation, no partNumber re-issue: the slice size stays the
    // configured part size even for a multipart object.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_discover_obj_no_align_when_validation_disabled() {
        let configured = 500;
        let ranged = mock!(Client::get_object)
            .match_requests(|r| r.range() == Some("bytes=0-499"))
            .then_output(|| {
                GetObjectOutput::builder()
                    .content_length(500)
                    .content_range("0-499/700")
                    .e_tag("\"abc-2\"")
                    .body(ByteStream::from_static(&[0u8; 500]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, &[&ranged]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let transfer = test_transfer(test_handle(client, configured), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();

        assert_eq!(discovery.effective_part_size, configured);
        assert_eq!(discovery.remaining, Some(500..=699));
    }

    #[cfg_attr(miri, ignore)]
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
        let client = mock_client!(aws_sdk_s3, &[&get_obj_rule]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=200-499")
            .build()
            .unwrap();

        let transfer = test_transfer(test_handle(client, target_part_size), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();
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

    #[cfg_attr(miri, ignore)]
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
        let client = mock_client!(aws_sdk_s3, &[&get_obj_rule]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=200-123456")
            .build()
            .unwrap();

        let transfer = test_transfer(test_handle(client, target_part_size), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();
        assert!(discovery.remaining.is_none());

        let initial_chunk = discovery
            .initial_chunk
            .expect("initial chunk")
            .collect()
            .await
            .expect("valid body");
        assert_eq!(50, initial_chunk.remaining());
    }

    #[cfg_attr(miri, ignore)]
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
        let client = mock_client!(aws_sdk_s3, &[&get_range_rule, &get_first_part_rule]);

        let request = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let transfer = test_transfer(test_handle(client, target_part_size), &request);

        let discovery = discover_obj(&transfer, &request, false).await.unwrap();
        assert!(discovery.remaining.is_none());
        assert!(discovery.initial_chunk.is_none());
    }
}
