/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use crate::bucket::BucketType;
use crate::error::Error;
use crate::storage::models::{ObjectMetadata, RestoreStatus};
use crate::storage::StorageBackend;
use crate::Operation;
use async_trait::async_trait;
use aws_sdk_s3::types::ChecksumType;
use aws_smithy_checksums::ChecksumAlgorithm;
use base64::Engine;
use bytes::BytesMut;
use futures_util::StreamExt;
use s3s::dto::Timestamp;
use s3s::dto::{ETag, HeadBucketInput, HeadBucketOutput, StreamingBlob};
use s3s::{S3Request, S3Response, S3Result};
use std::str::FromStr;

/// The `Content-Range` header for a range answered out of an object of `total` bytes.
/// `range` is half-open; the header's end is inclusive.
fn content_range_header(range: &std::ops::Range<u64>, total: u64) -> String {
    format!("bytes {}-{}/{}", range.start, range.end - 1, total)
}

/// Convert a quoted etag string (e.g. `"\"abc123\""`) to `ETag::Strong("abc123")`.
fn etag_from_quoted(s: &str) -> ETag {
    ETag::Strong(s.trim_matches('"').to_owned())
}

/// Convert a quoted etag string to `Option<ETag::Strong>`.
fn opt_etag_from_quoted(s: Option<String>) -> Option<ETag> {
    s.map(|v| etag_from_quoted(&v))
}

/// Apply defaults to a HeadObject response matching post-2023 prod S3
/// behavior on a fresh bucket: SSE-S3 (AES256) default encryption.
fn apply_response_defaults_head(output: &mut s3s::dto::HeadObjectOutput) {
    if output.server_side_encryption.is_none() {
        output.server_side_encryption = Some(s3s::dto::ServerSideEncryption::from_static(
            s3s::dto::ServerSideEncryption::AES256,
        ));
    }
}

fn apply_response_defaults_get(output: &mut s3s::dto::GetObjectOutput) {
    if output.server_side_encryption.is_none() {
        output.server_side_encryption = Some(s3s::dto::ServerSideEncryption::from_static(
            s3s::dto::ServerSideEncryption::AES256,
        ));
    }
}

fn has_any_checksum_head(o: &s3s::dto::HeadObjectOutput) -> bool {
    o.checksum_crc32.is_some()
        || o.checksum_crc32c.is_some()
        || o.checksum_crc64nvme.is_some()
        || o.checksum_sha1.is_some()
        || o.checksum_sha256.is_some()
}

fn has_any_checksum_get(o: &s3s::dto::GetObjectOutput) -> bool {
    o.checksum_crc32.is_some()
        || o.checksum_crc32c.is_some()
        || o.checksum_crc64nvme.is_some()
        || o.checksum_sha1.is_some()
        || o.checksum_sha256.is_some()
}

/// Deterministically corrupt the first checksum header present on a GET response:
/// decode it, XOR its first byte with 0xFF, re-encode. The result is valid-format
/// but guaranteed ≠ the real value, and is recomputable from the object.
fn xor_first_byte_of_checksum(output: &mut s3s::dto::GetObjectOutput) {
    for value in [
        &mut output.checksum_crc32,
        &mut output.checksum_crc32c,
        &mut output.checksum_crc64nvme,
        &mut output.checksum_sha1,
        &mut output.checksum_sha256,
    ]
    .into_iter()
    .flatten()
    {
        if let Ok(mut raw) = base64::engine::general_purpose::STANDARD.decode(value.as_bytes()) {
            if let Some(b) = raw.first_mut() {
                *b ^= 0xFF;
                *value = base64::engine::general_purpose::STANDARD.encode(&raw);
                return;
            }
        }
    }
}

/// Server state shared by the server handle and every request it serves.
#[derive(Debug, Default)]
pub(crate) struct ServerState {
    /// Target-scoped fault injection registry.
    pub(crate) faults: crate::faults::FaultRegistry,
    /// Server-wide load-driven throttle. `None` until one is installed.
    pub(crate) throttle: std::sync::Mutex<Option<std::sync::Arc<crate::throttle::RateThrottle>>>,
    /// Requests received per operation.
    pub(crate) requests: crate::operation::RequestCounters,
}

impl ServerState {
    /// Admit a request: count it, apply the throttle, then draw its fault.
    ///
    /// Called first in every S3 handler. The throttle applies to the operations
    /// [`Operation::is_throttled`] names; a shed request returns 503 `SlowDown` and draws no fault.
    /// A fault that fails the request before it is served (`FaultType::Error`) is
    /// returned as the error; any other fault is returned for
    /// the handler to apply, and has no effect on an operation it does not shape.
    pub(crate) fn admit(
        &self,
        operation: Operation,
        bucket: Option<&str>,
        key: Option<&str>,
    ) -> crate::Result<Option<crate::faults::FaultType>> {
        self.requests.record(operation);
        if operation.is_throttled() {
            self.check_throttle()?;
        }
        match self.faults.next_fault(operation, bucket, key) {
            Some(crate::faults::FaultType::Error(error)) => Err(error),
            fault => Ok(fault),
        }
    }

    /// Decide whether to serve or shed this request under the installed throttle;
    /// a shed request returns 503 `SlowDown`. The throttle lock is released before
    /// consulting the rate limiter, so it is never held across the handler body.
    fn check_throttle(&self) -> crate::Result<()> {
        let throttle = self.throttle.lock().unwrap().clone();
        match throttle {
            Some(rt) if !rt.try_take(std::time::Instant::now()) => Err(Error::SlowDown),
            _ => Ok(()),
        }
    }
}

/// Inner implementation of the s3s::S3 trait.
///
/// This struct implements the s3s::S3 trait, delegating all operations to the storage backend.
/// It is stateless and focuses purely on S3 API semantics and validation.
#[derive(Debug, Clone)]
pub(crate) struct Inner<S: StorageBackend + 'static> {
    /// The storage backend.
    storage: S,
    /// Faults, throttle and request counters, shared with the owning server.
    state: std::sync::Arc<ServerState>,
}

impl<S: StorageBackend + 'static> Inner<S> {
    /// Create a new Inner with the given storage backend (no faults, no throttle).
    #[cfg(test)]
    pub(crate) fn new(storage: S) -> Self {
        Self::with_state(storage, Default::default())
    }

    /// Create a new Inner sharing the state of an owning server.
    pub(crate) fn with_state(storage: S, state: std::sync::Arc<ServerState>) -> Self {
        Self { storage, state }
    }

    /// Apply the fault drawn for a GET request to its response. Effects are
    /// deterministic transforms (XOR) so a faulted value is derivable.
    async fn apply_get_fault(
        &self,
        fault: Option<crate::faults::FaultType>,
        conn_fault: Option<&std::sync::Arc<crate::socket_fault::ConnectionFault>>,
        output: &mut s3s::dto::GetObjectOutput,
    ) -> S3Result<()> {
        match fault {
            Some(crate::faults::FaultType::WrongStoredChecksum) => {
                xor_first_byte_of_checksum(output);
            }
            Some(crate::faults::FaultType::CorruptBody) => {
                if let Some(body) = output.body.take() {
                    let mut bytes = BytesMut::new();
                    let mut stream = body;
                    while let Some(chunk) = stream.next().await {
                        let chunk =
                            chunk.map_err(|e| Error::Internal(format!("Stream error: {}", e)))?;
                        bytes.extend_from_slice(&chunk);
                    }
                    if let Some(b) = bytes.first_mut() {
                        *b ^= 0xFF;
                    }
                    let bytes = bytes.freeze();
                    output.body = Some(StreamingBlob::wrap(futures_util::stream::once(
                        async move { Ok::<_, std::io::Error>(bytes) },
                    )));
                }
            }
            Some(crate::faults::FaultType::TruncateBody { after_bytes }) => {
                if let Some(body) = output.body.take() {
                    // Yield body bytes up to `after_bytes`, then error the stream.
                    // hyper aborts the connection mid-response.
                    //
                    // State: (inner stream, remaining passthrough bytes, errored?).
                    let faulted = futures_util::stream::unfold(
                        (body, after_bytes, false),
                        |(mut stream, remaining, errored)| async move {
                            if errored {
                                return None;
                            }
                            if remaining == 0 {
                                // Already delivered the allotted bytes: error now.
                                return Some((
                                    Err(std::io::Error::new(
                                        std::io::ErrorKind::ConnectionReset,
                                        "injected mid-stream truncation",
                                    )),
                                    (stream, 0, true),
                                ));
                            }
                            match stream.next().await {
                                Some(Ok(chunk)) => {
                                    if (chunk.len() as u64) <= remaining {
                                        let rem = remaining - chunk.len() as u64;
                                        Some((Ok(chunk), (stream, rem, false)))
                                    } else {
                                        // Emit the partial head; error on next poll.
                                        let head = chunk.slice(0..remaining as usize);
                                        Some((Ok(head), (stream, 0, false)))
                                    }
                                }
                                Some(Err(e)) => {
                                    Some((Err(std::io::Error::other(e)), (stream, 0, true)))
                                }
                                // Real EOF before the truncation point: nothing to fault.
                                None => None,
                            }
                        },
                    );
                    output.body = Some(StreamingBlob::wrap(faulted));
                }
            }
            Some(crate::faults::FaultType::PaceBody {
                piece_bytes,
                cadence,
            }) => {
                if let Some(body) = output.body.take() {
                    // Re-chunk the body into `piece_bytes`-sized pieces delivered
                    // on the given cadence.
                    //
                    // State: (inner stream, buffer of pending bytes from the
                    // underlying stream, whether the first piece has been emitted,
                    // bytes remaining in the current piece).
                    let faulted = futures_util::stream::unfold(
                        (body, bytes::BytesMut::new(), false, piece_bytes),
                        move |(mut stream, mut buf, first_emitted, piece_size)| async move {
                            // Fill the buffer until we have a full piece or EOF.
                            while (buf.len() as u64) < piece_size {
                                match stream.next().await {
                                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                                    Some(Err(e)) => {
                                        return Some((
                                            Err(std::io::Error::other(e)),
                                            (stream, buf, first_emitted, piece_size),
                                        ));
                                    }
                                    None => break,
                                }
                            }

                            if buf.is_empty() {
                                // Underlying body exhausted and buffer drained: EOF.
                                return None;
                            }

                            // Apply cadence before delivering subsequent pieces.
                            if first_emitted {
                                match cadence {
                                    crate::faults::BodyCadence::Slow(delay) => {
                                        tokio::time::sleep(delay).await;
                                    }
                                    crate::faults::BodyCadence::Stall => {
                                        std::future::pending::<()>().await;
                                        unreachable!("pending future never resolves");
                                    }
                                }
                            }

                            let take = std::cmp::min(buf.len() as u64, piece_size) as usize;
                            let piece = buf.split_to(take).freeze();
                            Some((Ok(piece), (stream, buf, true, piece_size)))
                        },
                    );
                    output.body = Some(StreamingBlob::wrap(faulted));
                }
            }
            Some(crate::faults::FaultType::ShortBody { actual_bytes }) => {
                if let Some(body) = output.body.take() {
                    // Yield body bytes up to `actual_bytes`, then end the stream
                    // cleanly. Content-Length still advertises the full size, so
                    // the client sees a short read.
                    //
                    // State: (inner stream, remaining bytes to deliver).
                    let faulted = futures_util::stream::unfold(
                        (body, actual_bytes),
                        |(mut stream, remaining)| async move {
                            if remaining == 0 {
                                return None;
                            }
                            match stream.next().await {
                                Some(Ok(chunk)) => {
                                    if (chunk.len() as u64) <= remaining {
                                        let rem = remaining - chunk.len() as u64;
                                        Some((Ok(chunk), (stream, rem)))
                                    } else {
                                        // Emit the partial head, then end.
                                        let head = chunk.slice(0..remaining as usize);
                                        Some((Ok(head), (stream, 0)))
                                    }
                                }
                                Some(Err(e)) => Some((Err(std::io::Error::other(e)), (stream, 0))),
                                None => None,
                            }
                        },
                    );
                    output.body = Some(StreamingBlob::wrap(faulted));
                }
            }
            Some(crate::faults::FaultType::ConnectionReset { after_bytes }) => {
                // Arm the per-connection fault; the socket wrapper aborts the
                // connection (RST) after `after_bytes` further bytes are written.
                if let Some(fault) = conn_fault {
                    fault.arm(after_bytes);
                }
            }
            // Send-time errors are returned by `ServerState::admit`; upload-only faults are
            // no-ops on the GET path.
            Some(
                crate::faults::FaultType::Error(_)
                | crate::faults::FaultType::StallRequestRead { .. },
            ) => {}
            Some(crate::faults::FaultType::WithholdResponseHeaders) => {
                // Never return, so s3s never writes the status line or headers.
                std::future::pending::<()>().await;
                unreachable!("pending future never resolves");
            }
            None => {}
        }
        Ok(())
    }
}

#[async_trait]
impl<S: StorageBackend + 'static> s3s::S3 for Inner<S> {
    async fn get_object(
        &self,
        req: S3Request<s3s::dto::GetObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectOutput>> {
        let fault = self.state.admit(
            Operation::GetObject,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let conn_fault = req
            .extensions
            .get::<std::sync::Arc<crate::socket_fault::ConnectionFault>>()
            .cloned();
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        tracing::trace!(%bucket, %key, "GetObject");
        check_bucket(&self.storage, bucket).await?;
        check_part_number_request(input.part_number, input.range.is_some())?;

        // Get metadata first to validate range
        let metadata = match self.storage.head_object(bucket, key).await? {
            Some(metadata) => metadata,
            None => return Err(Error::NoSuchKey.into()),
        };
        check_if_match(input.if_match.as_ref(), &metadata)?;
        if metadata.is_archived() {
            return Err(Error::InvalidObjectState { restore: false }.into());
        }

        // Convert s3s Range to std Range if present
        let mut range = input
            .range
            .as_ref()
            .map(|range_dto| {
                range_dto
                    .check(metadata.content_length)
                    .map_err(|_| Error::InvalidRange)
            })
            .transpose()?;

        // A partNumber GET returns exactly that part's bytes: the range path below then
        // returns the part's data and Content-Range, and `apply_part_checksums` reports its
        // checksum. parts_count is set on the response so callers learn the stored part count.
        let parts_count = metadata.parts_count();
        if let Some(part_number) = input.part_number {
            range = part_number_range(&metadata, part_number)?;
        }

        // Get object stream with validated range
        let request = crate::storage::GetObjectRequest {
            bucket,
            key,
            range: range.clone(),
        };
        let response = match self.storage.get_object(request).await? {
            Some(response) => response,
            None => return Err(Error::NoSuchKey.into()),
        };

        let (stream, mut stream_metadata) = (response.stream, response.metadata);
        // The object can be replaced between the lookup above and this read: the condition must
        // also hold for the object whose data is returned.
        check_if_match(input.if_match.as_ref(), &stream_metadata)?;
        if let Some(part_number) = input.part_number {
            stream_metadata.apply_part_checksums(part_number);
        }
        if omits_part_checksums(&metadata, input.part_number, range.as_ref()) {
            stream_metadata.clear_checksums();
        }

        let mut output = s3s::dto::GetObjectOutput::default();

        // Set content length and range headers
        if let Some(range) = range {
            // For range requests, content_length is the size of the range
            let range_size = range.end - range.start;
            output.content_length = Some(range_size as i64);
            // Set content_range header to indicate what range is being returned
            output.content_range = Some(content_range_header(&range, metadata.content_length));
        } else {
            // For full object requests, content_length is the full object size
            output.content_length = Some(metadata.content_length as i64);
        }

        // Use the stream directly
        output.body = Some(StreamingBlob::wrap(stream));

        output.e_tag = Some(etag_from_quoted(&stream_metadata.etag));

        let timestamp = Timestamp::from(stream_metadata.last_modified);
        output.last_modified = Some(timestamp);

        if let Some(content_type) = stream_metadata.content_type {
            let mime = content_type
                .parse()
                .expect("stored content_type should be valid");
            output.content_type = Some(mime);
        }

        output.metadata = Some(stream_metadata.user_metadata);

        // Add checksum headers from stored metadata
        output.checksum_crc32 = stream_metadata.crc32;
        output.checksum_crc32c = stream_metadata.crc32c;
        output.checksum_sha1 = stream_metadata.sha1;
        output.checksum_sha256 = stream_metadata.sha256;
        output.checksum_crc64nvme = stream_metadata.crc64nvme;

        // A partNumber GET reports the object's stored part count.
        if input.part_number.is_some() && parts_count > 0 {
            output.parts_count = Some(parts_count as i32);
        }

        // Return stored HTTP headers
        output.storage_class = storage_class_header(stream_metadata.storage_class);
        output.cache_control = stream_metadata.cache_control.map(|s| s.parse().unwrap());
        output.content_encoding = stream_metadata.content_encoding.map(|s| s.parse().unwrap());
        output.content_disposition = stream_metadata
            .content_disposition
            .map(|s| s.parse().unwrap());
        output.content_language = stream_metadata.content_language.map(|s| s.parse().unwrap());
        output.restore = stream_metadata.restore_status.map(restore_header);

        // As in S3, checksums are returned only in checksum mode.
        if !checksum_mode_enabled(input.checksum_mode.as_ref()) {
            output.checksum_crc32 = None;
            output.checksum_crc32c = None;
            output.checksum_sha1 = None;
            output.checksum_sha256 = None;
            output.checksum_crc64nvme = None;
        }
        // The type is the object's, also for a range that returns one part's checksum.
        if has_any_checksum_get(&output) {
            output.checksum_type = stored_checksum_type(&metadata);
        }
        apply_response_defaults_get(&mut output);
        self.apply_get_fault(fault, conn_fault.as_ref(), &mut output)
            .await?;
        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<s3s::dto::HeadObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::HeadObjectOutput>> {
        self.state.admit(
            Operation::HeadObject,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        tracing::trace!(%bucket, %key, "HeadObject");
        check_bucket(&self.storage, bucket).await?;
        check_part_number_request(input.part_number, input.range.is_some())?;

        // Get object metadata from storage
        let mut metadata = match self.storage.head_object(bucket, key).await? {
            Some(metadata) => metadata,
            None => return Err(Error::NoSuchKey.into()),
        };
        check_if_match(input.if_match.as_ref(), &metadata)?;

        // A HEAD carrying a `Range` is answered 206: `Content-Length` is the range's
        // length and `Content-Range` names the offsets, which is the only thing that
        // tells a caller WHERE a suffix or open-ended range landed. Checksums follow
        // the same rule as a ranged GET.
        let mut range = input
            .range
            .as_ref()
            .map(|range_dto| {
                range_dto
                    .check(metadata.content_length)
                    .map_err(|_| Error::InvalidRange)
            })
            .transpose()?;
        // A partNumber HEAD describes the part that a partNumber GET returns.
        let parts_count = metadata.parts_count();
        if let Some(part_number) = input.part_number {
            range = part_number_range(&metadata, part_number)?;
        }
        let content_range = range
            .as_ref()
            .map(|r| content_range_header(r, metadata.content_length));
        let checksum_type = stored_checksum_type(&metadata);
        let omit_checksums = omits_part_checksums(&metadata, input.part_number, range.as_ref());
        match (input.part_number, range.as_ref()) {
            (Some(part_number), _) => metadata.apply_part_checksums(part_number),
            (None, Some(r)) => metadata.apply_range_checksums(r.start, r.end),
            (None, None) => {}
        }
        if omit_checksums {
            metadata.clear_checksums();
        }

        // Build response
        let content_type = metadata.content_type.and_then(|ct| ct.parse().ok());
        let content_length = range
            .as_ref()
            .map(|r| r.end - r.start)
            .unwrap_or(metadata.content_length);
        let mut output = s3s::dto::HeadObjectOutput {
            content_length: Some(content_length as i64),
            content_range,
            e_tag: Some(etag_from_quoted(&metadata.etag)),
            last_modified: Some(Timestamp::from(metadata.last_modified)),
            content_type,
            metadata: Some(metadata.user_metadata),
            ..Default::default()
        };

        // Add checksum headers from stored metadata
        output.checksum_crc32 = metadata.crc32;
        output.checksum_crc32c = metadata.crc32c;
        output.checksum_sha1 = metadata.sha1;
        output.checksum_sha256 = metadata.sha256;
        output.checksum_crc64nvme = metadata.crc64nvme;

        // Return stored HTTP headers
        output.storage_class = storage_class_header(metadata.storage_class);
        output.cache_control = metadata.cache_control.map(|s| s.parse().unwrap());
        output.content_encoding = metadata.content_encoding.map(|s| s.parse().unwrap());
        output.content_disposition = metadata.content_disposition.map(|s| s.parse().unwrap());
        output.content_language = metadata.content_language.map(|s| s.parse().unwrap());
        output.restore = metadata.restore_status.map(restore_header);
        if input.part_number.is_some() && parts_count > 0 {
            output.parts_count = Some(parts_count as i32);
        }
        // As in S3, checksums are returned only in checksum mode.
        if !checksum_mode_enabled(input.checksum_mode.as_ref()) {
            output.checksum_crc32 = None;
            output.checksum_crc32c = None;
            output.checksum_sha1 = None;
            output.checksum_sha256 = None;
            output.checksum_crc64nvme = None;
        }

        if has_any_checksum_head(&output) {
            output.checksum_type = checksum_type;
        }
        apply_response_defaults_head(&mut output);
        Ok(S3Response::new(output))
    }

    async fn put_object(
        &self,
        req: S3Request<s3s::dto::PutObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let fault = self.state.admit(
            Operation::PutObject,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, "PutObject");
        check_bucket(&self.storage, &input.bucket).await?;
        if input.key.is_empty() {
            return Err(
                Error::InvalidArgument("User key must have a length greater than 0.").into(),
            );
        }

        let client_checksums = crate::types::ClientChecksums::from(&input);
        validate_digest_headers(&client_checksums)?;
        let integrity_checks = crate::types::ObjectIntegrityChecks::from(&client_checksums);

        // Create storage request with configured checksums
        let mut request = crate::storage::StoreObjectRequest::from(input);
        request.integrity_checks = integrity_checks;
        // Storage rejects a body that does not match them before it replaces the object.
        request.client_checksums = Some(client_checksums);

        // StallRequestRead: wrap the body stream to stall after `after_bytes`.
        if let Some(crate::faults::FaultType::StallRequestRead { after_bytes }) = fault {
            let inner_body =
                std::mem::replace(&mut request.body, Box::pin(futures_util::stream::empty()));
            let stalled = futures_util::stream::unfold(
                (inner_body, after_bytes),
                |(mut stream, remaining)| async move {
                    if remaining == 0 {
                        std::future::pending::<()>().await;
                        unreachable!("pending future never resolves");
                    }
                    match stream.next().await {
                        Some(Ok(chunk)) => {
                            if (chunk.len() as u64) <= remaining {
                                let rem = remaining - chunk.len() as u64;
                                Some((Ok(chunk), (stream, rem)))
                            } else {
                                let head = chunk.slice(0..remaining as usize);
                                Some((Ok(head), (stream, 0)))
                            }
                        }
                        Some(Err(e)) => Some((Err(e), (stream, 0))),
                        None => None,
                    }
                },
            );
            request.body = Box::pin(stalled);
        }

        let stored_meta = self.storage.put_object(request).await?;

        // WithholdResponseHeaders: body is consumed and stored; never respond.
        if let Some(crate::faults::FaultType::WithholdResponseHeaders) = fault {
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves");
        }

        let output = s3s::dto::PutObjectOutput {
            e_tag: opt_etag_from_quoted(stored_meta.object_integrity.etag()),
            checksum_type: integrity_checksum_type(&stored_meta.object_integrity),
            checksum_crc32: stored_meta.object_integrity.crc32,
            checksum_crc32c: stored_meta.object_integrity.crc32c,
            checksum_sha1: stored_meta.object_integrity.sha1,
            checksum_sha256: stored_meta.object_integrity.sha256,
            checksum_crc64nvme: stored_meta.object_integrity.crc64nvme,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        self.state.admit(
            Operation::CreateMultipartUpload,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, "CreateMultipartUpload");
        check_bucket(&self.storage, &input.bucket).await?;
        let key = &input.key;

        // Extract checksum algorithm if provided, default to CRC64NVME
        let checksum_algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|alg| {
                if alg
                    .as_str()
                    .eq_ignore_ascii_case(aws_smithy_checksums::MD5_NAME)
                {
                    // Handle MD5 specially since it might not parse correctly due to deprecation
                    #[allow(deprecated)]
                    Ok(ChecksumAlgorithm::Md5)
                } else {
                    ChecksumAlgorithm::from_str(alg.as_str())
                }
            })
            .transpose()
            .map_err(|_| Error::InvalidRequest("Unsupported checksum algorithm"))?
            .unwrap_or(ChecksumAlgorithm::Crc64Nvme);

        let checksum_type = match input.checksum_type {
            // S3 accepts the checksum type in any case, e.g. `full_object`, and rejects any
            // other value.
            Some(ct) => {
                let checksum_type = ChecksumType::from(ct.as_str().to_ascii_uppercase().as_str());
                if !ChecksumType::values().contains(&checksum_type.as_str()) {
                    return Err(Error::InvalidRequest(
                        "Value for x-amz-checksum-type header is invalid.",
                    )
                    .into());
                }
                checksum_type
            }
            // Default checksum type based on algorithm
            None => match checksum_algorithm {
                ChecksumAlgorithm::Crc64Nvme => ChecksumType::FullObject,
                _ => ChecksumType::Composite,
            },
        };

        // Extract checksum type (full object vs composite) and validate requested checksum
        validate_checksum_and_type(&checksum_algorithm, Some(&checksum_type))?;

        // Generate a unique upload ID
        let upload_id = format!("{}", uuid::Uuid::new_v4());

        // Extract content type and metadata
        let content_type = input.content_type.map(|mime| mime.to_string());
        let user_metadata = input.metadata.unwrap_or_default();
        let storage_class = input
            .storage_class
            .map(|s| std::borrow::Cow::<str>::from(s).into_owned());

        // Create metadata for the multipart upload
        let metadata = ObjectMetadata {
            content_type,
            content_length: 0,   // Will be updated when the upload is completed
            etag: String::new(), // Will be updated when the upload is completed
            last_modified: std::time::SystemTime::now(),
            user_metadata,
            storage_class,
            checksum_algorithm: Some(checksum_algorithm),
            ..Default::default()
        };
        let request = crate::storage::CreateMultipartUploadRequest {
            bucket: &input.bucket,
            key,
            upload_id: &upload_id,
            metadata,
            checksum_type: checksum_type.clone(),
        };
        self.storage.create_multipart_upload(request).await?;

        let output = s3s::dto::CreateMultipartUploadOutput {
            upload_id: Some(upload_id),
            key: Some(key.to_string()),
            bucket: Some(input.bucket.to_string()),
            checksum_algorithm: Some(s3s::dto::ChecksumAlgorithm::from(
                checksum_algorithm.as_str().to_string(),
            )),
            checksum_type: Some(s3s::dto::ChecksumType::from(
                checksum_type.as_str().to_string(),
            )),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        let fault = self.state.admit(
            Operation::UploadPart,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        let upload_id = &input.upload_id;
        let part_number = input.part_number;
        tracing::trace!(%upload_id, part_number, "UploadPart");
        check_bucket(&self.storage, &input.bucket).await?;

        let client_checksums = crate::types::ClientChecksums::from(&input);
        validate_digest_headers(&client_checksums)?;
        let mut integrity_checks = crate::types::ObjectIntegrityChecks::from(&client_checksums);

        // Read the part content and calculate checksums.
        // StallRequestRead: after consuming `after_bytes`, pend forever.
        let stall_after = match fault {
            Some(crate::faults::FaultType::StallRequestRead { after_bytes }) => Some(after_bytes),
            _ => None,
        };
        let mut bytes_consumed: u64 = 0;
        let mut content = BytesMut::new();
        if let Some(mut body) = input.body {
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => {
                        if let Some(limit) = stall_after {
                            if bytes_consumed >= limit {
                                // Stall: stop reading, pend forever.
                                std::future::pending::<()>().await;
                                unreachable!("pending future never resolves");
                            }
                            let new_total = bytes_consumed + chunk.len() as u64;
                            if new_total > limit {
                                // Consume up to the limit, then stall.
                                let allowed = (limit - bytes_consumed) as usize;
                                integrity_checks.update(&chunk[..allowed]);
                                content.extend_from_slice(&chunk[..allowed]);
                                std::future::pending::<()>().await;
                                unreachable!("pending future never resolves");
                            }
                            bytes_consumed = new_total;
                        }
                        integrity_checks.update(&chunk);
                        content.extend_from_slice(&chunk);
                    }
                    Err(e) => {
                        return Err(Error::Internal(format!("Failed to read body: {e}")).into())
                    }
                }
            }
        }
        let content = content.freeze();
        let calculated_integrity = integrity_checks.finalize();

        // Validate client-provided checksums
        if let Err(algorithm) = calculated_integrity.validate(&client_checksums) {
            return Err(Error::BadDigest {
                algorithm,
                completion: false,
            }
            .into());
        }

        // Store the part and get its ETag
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number,
            content,
        };
        let response = self.storage.upload_part(request).await?;

        // WithholdResponseHeaders: part is consumed and stored; never respond.
        if let Some(crate::faults::FaultType::WithholdResponseHeaders) = fault {
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves");
        }

        // Build response with checksums
        let output = s3s::dto::UploadPartOutput {
            e_tag: Some(etag_from_quoted(&response.etag)),
            checksum_crc32: calculated_integrity.crc32,
            checksum_crc32c: calculated_integrity.crc32c,
            checksum_sha1: calculated_integrity.sha1,
            checksum_sha256: calculated_integrity.sha256,
            checksum_crc64nvme: calculated_integrity.crc64nvme,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CompleteMultipartUploadOutput>> {
        self.state.admit(
            Operation::CompleteMultipartUpload,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, upload_id = %input.upload_id, "CompleteMultipartUpload");
        check_bucket(&self.storage, &input.bucket).await?;
        let bucket = input.bucket.clone();
        let upload_id = &input.upload_id;

        // Extract client checksums before consuming input
        let client_checksums = crate::types::ClientChecksums::from(&input);

        // Extract part information
        let mut parts = Vec::new();
        if let Some(completed_parts) = input.multipart_upload.and_then(|u| u.parts) {
            for part in completed_parts {
                let part_number = part.part_number.ok_or(Error::MalformedXml)?;
                let etag = part.e_tag.ok_or(Error::MalformedXml)?;
                parts.push((part_number, format!("\"{}\"", etag.value())));
            }
        }

        // General-purpose S3 requires the parts list to be in ascending order by
        // part number and returns InvalidPartOrder otherwise. Gaps are allowed in
        // the general case (e.g. 1, 2, 5) — part numbers need not be contiguous.
        //
        // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
        let bucket_type = BucketType::of(&bucket);
        let mut previous_part_number = 0;
        for (part_number, _) in &parts {
            if *part_number <= previous_part_number {
                return Err(Error::InvalidPartOrder { bucket_type }.into());
            }
            previous_part_number = *part_number;
        }

        // Some conditions impose the STRICTER rule that part numbers must be
        // CONSECUTIVE and begin with 1 (no gaps), not merely ascending. S3 requires
        // this for two independent reasons, which return DIFFERENT errors on a gap:
        //
        //   * Composite (part-level) checksums — the checksum type fixed by
        //     CreateMultipartUpload. Per-part checksums on the completion are not a
        //     signal: a FULL_OBJECT upload may carry them too. A gap here is an
        //     HTTP 500 Internal Server error.
        //     <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html>
        //
        //   * Directory buckets (S3 Express One Zone). A gap here is an HTTP 400
        //     Bad Request error.
        //     <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html>
        let composite_checksum =
            self.storage.multipart_checksum_type(upload_id).await? == Some(ChecksumType::Composite);

        if composite_checksum || bucket_type == BucketType::Directory {
            let contiguous = parts
                .iter()
                .enumerate()
                .all(|(i, (part_number, _))| *part_number == (i + 1) as i32);
            if !contiguous {
                // Directory bucket takes precedence as a 400; composite-only is a 500.
                if bucket_type == BucketType::Directory {
                    return Err(Error::InvalidPartOrder { bucket_type }.into());
                }
                return Err(Error::Internal(
                    "composite checksum multipart upload requires consecutive part numbers \
                     beginning with 1"
                        .to_string(),
                )
                .into());
            }
        }

        // Complete the multipart upload
        let request = crate::storage::CompleteMultipartUploadRequest {
            bucket: &bucket,
            upload_id,
            parts,
            client_checksums: if client_checksums.has_any() {
                Some(&client_checksums)
            } else {
                None
            },
        };
        let response = self.storage.complete_multipart_upload(request).await?;

        let output = s3s::dto::CompleteMultipartUploadOutput {
            key: Some(response.key),
            e_tag: Some(etag_from_quoted(&response.etag)),
            bucket: Some(bucket),
            checksum_type: integrity_checksum_type(&response.object_integrity),
            checksum_crc32: response.object_integrity.crc32,
            checksum_crc32c: response.object_integrity.crc32c,
            checksum_sha1: response.object_integrity.sha1,
            checksum_sha256: response.object_integrity.sha256,
            checksum_crc64nvme: response.object_integrity.crc64nvme,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        self.state.admit(
            Operation::AbortMultipartUpload,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, upload_id = %input.upload_id, "AbortMultipartUpload");
        check_bucket(&self.storage, &input.bucket).await?;
        let upload_id = &input.upload_id;

        // Abort the multipart upload
        self.storage.abort_multipart_upload(upload_id).await?;

        Ok(S3Response::new(
            s3s::dto::AbortMultipartUploadOutput::default(),
        ))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<s3s::dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectsV2Output>> {
        self.state.admit(
            Operation::ListObjectsV2,
            Some(&req.input.bucket),
            req.input.prefix.as_deref(),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, prefix = ?input.prefix, delimiter = ?input.delimiter, "ListObjectsV2");

        check_bucket(&self.storage, &input.bucket).await?;

        let prefix = input.prefix.as_deref().unwrap_or("");
        let max_keys = input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;
        let include_restore_status = input
            .optional_object_attributes
            .as_ref()
            .is_some_and(|attrs| attrs.iter().any(|a| a.as_str() == "RestoreStatus"));

        let request = crate::storage::ListObjectsRequest {
            bucket: &input.bucket,
            prefix: Some(prefix),
        };
        let objects = self.storage.list_objects(request).await?.objects;
        let list_request = crate::list::ListRequest {
            prefix,
            delimiter: input.delimiter.as_deref(),
            max_keys,
            continuation_token: input.continuation_token.as_deref(),
            start_after: input.start_after.as_deref(),
        };
        let page = crate::list::list_page(objects, &list_request).map_err(|error| match error {
            crate::list::ListError::InvalidContinuationToken => {
                Error::InvalidArgument("The continuation token provided is incorrect")
            }
        })?;

        let contents: Vec<s3s::dto::Object> = page
            .objects
            .into_iter()
            .map(|obj| list_entry(obj, include_restore_status))
            .collect();
        let common_prefixes: Vec<s3s::dto::CommonPrefix> = page
            .common_prefixes
            .into_iter()
            .map(|prefix| s3s::dto::CommonPrefix {
                prefix: Some(prefix),
            })
            .collect();

        let output = s3s::dto::ListObjectsV2Output {
            key_count: Some((contents.len() + common_prefixes.len()) as i32),
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            is_truncated: Some(page.next_continuation_token.is_some()),
            max_keys: Some(max_keys as i32),
            prefix: input.prefix,
            delimiter: input.delimiter,
            name: Some(input.bucket.to_string()),
            // S3 echoes `start-after` only when it applies, without a continuation token.
            start_after: input
                .start_after
                .filter(|_| input.continuation_token.is_none()),
            continuation_token: input.continuation_token,
            next_continuation_token: page.next_continuation_token,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn restore_object(
        &self,
        req: S3Request<s3s::dto::RestoreObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::RestoreObjectOutput>> {
        self.state.admit(
            Operation::RestoreObject,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, "RestoreObject");
        check_bucket(&self.storage, &input.bucket).await?;
        let metadata = self
            .storage
            .head_object(&input.bucket, &input.key)
            .await?
            .ok_or(Error::NoSuchKey)?;
        if !metadata.is_archival_class() {
            return Err(Error::InvalidObjectState { restore: true }.into());
        }
        if metadata.restore_status == Some(RestoreStatus::InProgress) {
            return Err(Error::RestoreAlreadyInProgress.into());
        }
        let days = input
            .restore_request
            .as_ref()
            .and_then(|request| request.days)
            .unwrap_or(1)
            .max(1) as u64;
        // A restore completes as soon as it is requested. As in S3, the copy expires at the
        // first midnight UTC at least `days` days later.
        let expiry = std::time::SystemTime::now() + std::time::Duration::from_secs(days * 86_400);
        let since_epoch = expiry
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_secs();
        let expiry = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs(since_epoch.div_ceil(86_400) * 86_400);
        self.storage
            .update_metadata(
                &input.bucket,
                &input.key,
                Box::new(move |metadata| {
                    metadata.restore_status = Some(RestoreStatus::Restored { expiry })
                }),
            )
            .await?;
        Ok(S3Response::new(s3s::dto::RestoreObjectOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.state
            .admit(Operation::HeadBucket, Some(&req.input.bucket), None)?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, "HeadBucket");
        if self.storage.head_bucket(&input.bucket).await? {
            Ok(S3Response::new(HeadBucketOutput::default()))
        } else {
            Err(Error::NoSuchBucket.into())
        }
    }

    async fn delete_object(
        &self,
        req: S3Request<s3s::dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteObjectOutput>> {
        self.state.admit(
            Operation::DeleteObject,
            Some(&req.input.bucket),
            Some(&req.input.key),
        )?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, key = %input.key, "DeleteObject");
        check_bucket(&self.storage, &input.bucket).await?;
        match &input.if_match {
            Some(condition) => {
                self.storage
                    .delete_object_if_match(&input.bucket, &input.key, condition)
                    .await?
            }
            None => {
                self.storage
                    .delete_object(&input.bucket, &input.key)
                    .await?
            }
        }
        Ok(S3Response::new(s3s::dto::DeleteObjectOutput::default()))
    }

    async fn create_bucket(
        &self,
        req: S3Request<s3s::dto::CreateBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateBucketOutput>> {
        self.state
            .admit(Operation::CreateBucket, Some(&req.input.bucket), None)?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, "CreateBucket");
        self.storage.create_bucket(&input.bucket).await?;
        let output = s3s::dto::CreateBucketOutput {
            location: Some(format!("/{}", input.bucket)),
        };
        Ok(S3Response::new(output))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOutput>> {
        self.state
            .admit(Operation::DeleteBucket, Some(&req.input.bucket), None)?;
        let input = req.input;
        tracing::trace!(bucket = %input.bucket, "DeleteBucket");
        self.storage.delete_bucket(&input.bucket).await?;
        Ok(S3Response::new(s3s::dto::DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketsOutput>> {
        self.state.admit(Operation::ListBuckets, None, None)?;
        tracing::trace!("ListBuckets");
        let buckets = self.storage.list_buckets().await?;
        let bucket_list: Vec<s3s::dto::Bucket> = buckets
            .into_iter()
            .map(|b| s3s::dto::Bucket {
                name: Some(b.name),
                creation_date: Some(Timestamp::from(b.creation_date)),
                ..Default::default()
            })
            .collect();
        let output = s3s::dto::ListBucketsOutput {
            buckets: Some(bucket_list),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

/// Fail with NoSuchBucket if `bucket` does not exist in `storage`, as S3 does for requests on
/// the objects of a missing bucket.
pub(crate) async fn check_bucket(
    storage: &(impl StorageBackend + ?Sized),
    bucket: &str,
) -> S3Result<()> {
    if storage.head_bucket(bucket).await? {
        Ok(())
    } else {
        Err(Error::NoSuchBucket.into())
    }
}

/// The ListObjectsV2 entry of an object. The restore status is included only when requested, as
/// in S3.
fn list_entry(obj: crate::storage::ObjectInfo, include_restore_status: bool) -> s3s::dto::Object {
    let algorithms = obj.metadata.checksum_algorithms();
    s3s::dto::Object {
        size: Some(obj.metadata.content_length as i64),
        e_tag: Some(etag_from_quoted(&obj.metadata.etag)),
        last_modified: Some(Timestamp::from(obj.metadata.last_modified)),
        storage_class: Some(listed_storage_class(&obj.metadata.storage_class).into()),
        checksum_algorithm: (!algorithms.is_empty()).then(|| {
            algorithms
                .iter()
                .map(|a| s3s::dto::ChecksumAlgorithm::from(a.as_str().to_ascii_uppercase()))
                .collect()
        }),
        checksum_type: stored_checksum_type(&obj.metadata),
        restore_status: obj
            .metadata
            .restore_status
            .filter(|_| include_restore_status)
            .map(|status| match status {
                RestoreStatus::InProgress => s3s::dto::RestoreStatus {
                    is_restore_in_progress: Some(true),
                    restore_expiry_date: None,
                },
                RestoreStatus::Restored { expiry } => s3s::dto::RestoreStatus {
                    is_restore_in_progress: Some(false),
                    restore_expiry_date: Some(Timestamp::from(expiry)),
                },
            }),
        key: Some(obj.key),
        ..Default::default()
    }
}

/// Fail a GetObject or HeadObject whose `partNumber` S3 rejects before looking up the object:
/// with a `Range` (400 InvalidRequest), or outside 1 to 10,000 (400 InvalidArgument).
fn check_part_number_request(part_number: Option<i32>, has_range: bool) -> crate::Result<()> {
    match part_number {
        Some(_) if has_range => Err(Error::InvalidRequest(
            "Cannot specify both Range header and partNumber query parameter",
        )),
        Some(part_number) if !(1..=10_000).contains(&part_number) => Err(Error::InvalidArgument(
            "Part number must be an integer between 1 and 10000, inclusive",
        )),
        _ => Ok(()),
    }
}

/// The byte range of part `part_number` of an object, for a partNumber GetObject or HeadObject.
/// S3 treats an object uploaded in one request as a single part, so partNumber=1 selects the
/// whole object as a range, except for an empty object, which has no range. A part number the
/// object does not have fails with 416 InvalidPartNumber.
fn part_number_range(
    metadata: &ObjectMetadata,
    part_number: i32,
) -> crate::Result<Option<std::ops::Range<u64>>> {
    if metadata.parts_count() == 0 && part_number == 1 {
        Ok((metadata.content_length > 0).then_some(0..metadata.content_length))
    } else {
        metadata
            .part_range(part_number)
            .map(Some)
            .ok_or(Error::InvalidPartNumber)
    }
}

/// Whether a GetObject or HeadObject of part of `metadata`'s object (a `part_number`, or a
/// `range` other than the whole object) returns no checksum: S3 returns none for the parts of a
/// multipart object with a full-object checksum.
fn omits_part_checksums(
    metadata: &ObjectMetadata,
    part_number: Option<i32>,
    range: Option<&std::ops::Range<u64>>,
) -> bool {
    let partial = part_number.is_some()
        || range.is_some_and(|range| range.start != 0 || range.end != metadata.content_length);
    partial
        && !metadata.parts.is_empty()
        && stored_checksum_type(metadata)
            .is_some_and(|t| t.as_str() == s3s::dto::ChecksumType::FULL_OBJECT)
}

/// Fail a request whose digest headers are malformed before its body is read, as S3 does: a
/// checksum that is not the base64 encoding of a digest of its algorithm's size fails with 400
/// InvalidRequest, and a malformed `Content-MD5` with 400 InvalidDigest.
fn validate_digest_headers(checksums: &crate::types::ClientChecksums) -> crate::Result<()> {
    let decoded_len = |value: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .ok()
            .map(|digest| digest.len())
    };
    for (checksum, len, message) in [
        (
            &checksums.crc32,
            4,
            "Value for x-amz-checksum-crc32 header is invalid.",
        ),
        (
            &checksums.crc32c,
            4,
            "Value for x-amz-checksum-crc32c header is invalid.",
        ),
        (
            &checksums.crc64nvme,
            8,
            "Value for x-amz-checksum-crc64nvme header is invalid.",
        ),
        (
            &checksums.sha1,
            20,
            "Value for x-amz-checksum-sha1 header is invalid.",
        ),
        (
            &checksums.sha256,
            32,
            "Value for x-amz-checksum-sha256 header is invalid.",
        ),
    ] {
        if checksum
            .as_deref()
            .is_some_and(|value| decoded_len(value) != Some(len))
        {
            return Err(Error::InvalidRequest(message));
        }
    }
    if checksums
        .md5
        .as_deref()
        .is_some_and(|value| decoded_len(value) != Some(16))
    {
        return Err(Error::InvalidDigest);
    }
    Ok(())
}

/// The checksum type of a stored object: COMPOSITE when its checksum combines part checksums,
/// which S3 writes with a `-<part count>` suffix, FULL_OBJECT otherwise, and `None` when it has
/// no checksum.
fn stored_checksum_type(metadata: &ObjectMetadata) -> Option<s3s::dto::ChecksumType> {
    checksum_type_of([
        &metadata.crc32,
        &metadata.crc32c,
        &metadata.crc64nvme,
        &metadata.sha1,
        &metadata.sha256,
    ])
}

/// The checksum type of an object just written with `integrity`, as [`stored_checksum_type`].
fn integrity_checksum_type(
    integrity: &crate::types::ObjectIntegrity,
) -> Option<s3s::dto::ChecksumType> {
    checksum_type_of([
        &integrity.crc32,
        &integrity.crc32c,
        &integrity.crc64nvme,
        &integrity.sha1,
        &integrity.sha256,
    ])
}

/// The checksum type of an object with `checksums`, from the first one it has: an object's
/// checksums all have the same type, since a multipart upload stores the checksum of one
/// algorithm and other objects have only full-object checksums.
fn checksum_type_of<'a>(
    checksums: impl IntoIterator<Item = &'a Option<String>>,
) -> Option<s3s::dto::ChecksumType> {
    let checksum = checksums.into_iter().flatten().next()?;
    Some(s3s::dto::ChecksumType::from_static(
        if checksum.contains('-') {
            s3s::dto::ChecksumType::COMPOSITE
        } else {
            s3s::dto::ChecksumType::FULL_OBJECT
        },
    ))
}

/// The `x-amz-storage-class` header of a GetObject or HeadObject response: S3 omits it for
/// `STANDARD` objects, also when `STANDARD` was requested explicitly.
fn storage_class_header(storage_class: Option<String>) -> Option<s3s::dto::StorageClass> {
    storage_class
        .filter(|class| class != s3s::dto::StorageClass::STANDARD)
        .map(s3s::dto::StorageClass::from)
}

/// The storage class of an object in ListObjectsV2 entries, which S3 always reports: `STANDARD`
/// when none was set.
fn listed_storage_class(storage_class: &Option<String>) -> String {
    storage_class
        .clone()
        .unwrap_or_else(|| s3s::dto::StorageClass::STANDARD.to_owned())
}

/// Whether a request set `x-amz-checksum-mode: ENABLED`.
fn checksum_mode_enabled(mode: Option<&s3s::dto::ChecksumMode>) -> bool {
    mode.is_some_and(|mode| mode.as_str().eq_ignore_ascii_case("ENABLED"))
}

/// The `x-amz-restore` header for a restore status.
fn restore_header(status: RestoreStatus) -> String {
    match status {
        RestoreStatus::InProgress => r#"ongoing-request="true""#.to_string(),
        RestoreStatus::Restored { expiry } => {
            let mut date = Vec::new();
            Timestamp::from(expiry)
                .format(s3s::dto::TimestampFormat::HttpDate, &mut date)
                .expect("a timestamp formats as an HTTP date");
            format!(
                r#"ongoing-request="false", expiry-date="{}""#,
                String::from_utf8(date).expect("an HTTP date is ASCII")
            )
        }
    }
}

/// Fail with `PreconditionFailed` if an `If-Match` condition does not hold for the object.
fn check_if_match(
    condition: Option<&s3s::dto::ETagCondition>,
    metadata: &ObjectMetadata,
) -> S3Result<()> {
    if condition
        .is_some_and(|condition| !crate::storage::etag_condition_holds(condition, &metadata.etag))
    {
        return Err(Error::PreconditionFailed.into());
    }
    Ok(())
}

fn validate_checksum_and_type(
    checksum_algorithm: &ChecksumAlgorithm,
    checksum_type: Option<&aws_sdk_s3::types::ChecksumType>,
) -> crate::Result<()> {
    // Validate algorithm restrictions for multipart uploads
    match checksum_algorithm {
        #[allow(deprecated)]
        ChecksumAlgorithm::Md5 => {
            // MD5 doesn't support full object checksums in multipart uploads
            if checksum_type == Some(&ChecksumType::FullObject) {
                return Err(Error::InvalidRequest(
                    "MD5 algorithm does not support full object checksum type for multipart uploads",
                ));
            }
        }
        ChecksumAlgorithm::Crc64Nvme => {
            // CRC64NVME only supports full object checksums
            if checksum_type == Some(&ChecksumType::Composite) {
                return Err(Error::InvalidRequest(
                    "CRC64NVME algorithm only supports full object checksum type",
                ));
            }
        }
        ChecksumAlgorithm::Crc32
        | ChecksumAlgorithm::Crc32c
        | ChecksumAlgorithm::Sha1
        | ChecksumAlgorithm::Sha256 => {
            // These support both full object and composite checksums
        }
        _ => {
            // Handle any future algorithms or unknown variants
            return Err(Error::InvalidRequest("Unsupported checksum algorithm"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;
    use futures::stream;
    use s3s::S3;

    fn s3_request<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: http::Method::GET,
            uri: http::Uri::default(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn create_get_object_input(bucket: &str, key: &str) -> s3s::dto::GetObjectInput {
        s3s::dto::GetObjectInput::builder()
            .bucket(s3s::dto::BucketName::from(bucket))
            .key(s3s::dto::ObjectKey::from(key))
            .build()
            .unwrap()
    }

    // helper function to create an object
    async fn create_object(
        inner: &Inner<InMemoryStorage>,
        key: &str,
        content: impl Into<bytes::Bytes>,
        _metadata_opt: Option<ObjectMetadata>,
    ) -> S3Response<s3s::dto::PutObjectOutput> {
        let content_bytes = content.into();
        let content_length = content_bytes.len() as i64;

        let stream = stream::once(async move { Ok::<_, std::io::Error>(content_bytes) });
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .body(Some(StreamingBlob::wrap(stream)))
            .key(s3s::dto::ObjectKey::from(key))
            .content_length(Some(s3s::dto::ContentLength::from(content_length)))
            .build()
            .unwrap();

        let req = s3_request(input);
        inner.put_object(req).await.expect("Failed to put object")
    }

    #[tokio::test]
    async fn test_get_object_range() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);
        let content = "This is a test content that is exactly 100 bytes long to make our range test work properly..........";
        let _ = create_object(&inner, "test-key", content, None).await;

        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=0-49").unwrap());

        let req = s3_request(input);

        // Call the method
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        // For range request "bytes=0-49", we expect:
        // - content_length to be 50 (the size of the range)
        // - content_range to indicate the range being returned
        assert_eq!(output.content_length, Some(50));
        assert_eq!(output.content_range, Some("bytes 0-49/100".to_string()));

        // The etag should be the MD5 hash of the content (calculated during put_object)
        let expected_etag = hex::encode(md5::compute(content).0);
        assert_eq!(output.e_tag.unwrap().value(), &expected_etag);

        let mut body = output.body.unwrap();

        let mut buf = BytesMut::with_capacity(output.content_length.unwrap() as usize);
        while let Some(result) = body.next().await {
            let bytes = result.expect("Failed to read body");
            buf.extend_from_slice(&bytes);
        }
        let range_contents = buf.freeze();

        // Verify we got exactly the requested range (bytes 0-49, which is 50 bytes)
        assert_eq!(range_contents.len(), 50);
        assert_eq!(&range_contents[..], &content.as_bytes()[0..50]);
    }

    #[tokio::test]
    async fn test_get_object_range_edge_cases() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);
        let content = "0123456789"; // 10 bytes
        let _ = create_object(&inner, "test-key", content, None).await;

        // Test case 1: Range at the beginning
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=0-4").unwrap());
        let req = s3_request(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(5));
        assert_eq!(output.content_range, Some("bytes 0-4/10".to_string()));

        // Test case 2: Range at the end
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=5-9").unwrap());
        let req = s3_request(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(5));
        assert_eq!(output.content_range, Some("bytes 5-9/10".to_string()));

        // Test case 3: Single byte range
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=3-3").unwrap());
        let req = s3_request(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(1));
        assert_eq!(output.content_range, Some("bytes 3-3/10".to_string()));
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        let content = b"Hello, checksum world!";

        // Calculate expected CRC32 using our ObjectIntegrityChecks
        let mut checks = crate::types::ObjectIntegrityChecks::new().with_crc32();
        checks.update(content);
        let integrity = checks.finalize();
        let expected_crc32 = integrity.crc32.unwrap();

        // Test with correct checksum - should succeed
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some(expected_crc32.clone()))
            .build()
            .unwrap();

        let result = inner
            .put_object(s3_request(input))
            .await
            .expect("Put should succeed");
        assert_eq!(result.output.checksum_crc32, Some(expected_crc32));

        // Test with wrong checksum - should fail
        let input_wrong = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-wrong"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            // Well-formed, but the CRC32 of other data.
            .checksum_crc32(Some("AAAAAA==".to_string()))
            .build()
            .unwrap();

        let error = inner
            .put_object(s3_request(input_wrong))
            .await
            .expect_err("wrong checksum must fail");
        assert_eq!(error.code(), &s3s::S3ErrorCode::BadDigest);
    }

    /// A PutObject with an empty key, which only a POST Object request can send, fails with
    /// 400 InvalidArgument.
    #[tokio::test]
    async fn test_put_object_empty_key() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from(""))
            .build()
            .unwrap();

        let error = inner.put_object(s3_request(input)).await.unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_eq!(
            error.message(),
            Some("User key must have a length greater than 0.")
        );
    }

    #[tokio::test]
    async fn test_default_crc64nvme() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        let content = b"Test default checksum";

        // Test without any client checksums - should default to CRC64NVME
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .build()
            .unwrap();

        let result = inner
            .put_object(s3_request(input))
            .await
            .expect("Put should succeed");

        // Should have CRC64NVME checksum by default
        assert!(result.output.checksum_crc64nvme.is_some());
        // Should not have other checksums
        assert!(result.output.checksum_crc32.is_none());
        assert!(result.output.checksum_sha1.is_none());
    }

    #[tokio::test]
    async fn test_upload_part_checksum_validation() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        // First create a multipart upload
        let create_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .build()
            .unwrap();

        let create_result = inner
            .create_multipart_upload(s3_request(create_input))
            .await
            .expect("Create multipart upload failed");
        let upload_id = create_result.output.upload_id.unwrap();

        let content = b"Part content for checksum test";

        // Calculate expected CRC32 using our ObjectIntegrityChecks
        let mut checks = crate::types::ObjectIntegrityChecks::new().with_crc32();
        checks.update(content);
        let integrity = checks.finalize();
        let expected_crc32 = integrity.crc32.unwrap();

        // Test with correct checksum - should succeed
        let input = s3s::dto::UploadPartInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id.clone())
            .part_number(1)
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some(expected_crc32.clone()))
            .build()
            .unwrap();

        let result = inner
            .upload_part(s3_request(input))
            .await
            .expect("Upload part should succeed");
        assert_eq!(result.output.checksum_crc32, Some(expected_crc32));

        // Test with wrong checksum - should fail
        let input_wrong = s3s::dto::UploadPartInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id)
            .part_number(2)
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            // Well-formed, but the CRC32 of other data.
            .checksum_crc32(Some("AAAAAA==".to_string()))
            .build()
            .unwrap();

        let error = inner
            .upload_part(s3_request(input_wrong))
            .await
            .expect_err("wrong checksum must fail");
        assert_eq!(error.code(), &s3s::S3ErrorCode::BadDigest);
    }

    #[tokio::test]
    async fn test_part_number_validation() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        // Create multipart upload
        let create_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .build()
            .unwrap();

        let create_result = inner
            .create_multipart_upload(s3_request(create_input))
            .await
            .expect("Create multipart upload failed");
        let upload_id = create_result.output.upload_id.unwrap();

        // Upload parts 1 and 2 and collect their ETags
        let mut part_etags = Vec::new();
        for part_num in 1..=2 {
            let content = format!("Part {} content", part_num);
            let input = s3s::dto::UploadPartInput::builder()
                .bucket(s3s::dto::BucketName::from("test-bucket"))
                .key(s3s::dto::ObjectKey::from("test-key"))
                .upload_id(upload_id.clone())
                .part_number(part_num)
                .body(Some(s3s::dto::StreamingBlob::wrap(
                    futures_util::stream::once(async move {
                        Ok::<_, std::io::Error>(bytes::Bytes::from(content))
                    }),
                )))
                .build()
                .unwrap();

            let result = inner
                .upload_part(s3_request(input))
                .await
                .expect("Upload part failed");
            part_etags.push(result.output.e_tag.unwrap());
        }

        // Test valid consecutive parts [1,2] - should succeed
        let valid_parts = vec![
            s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some(part_etags[0].clone()),
                ..Default::default()
            },
            s3s::dto::CompletedPart {
                part_number: Some(2),
                e_tag: Some(part_etags[1].clone()),
                ..Default::default()
            },
        ];
        let valid_input = s3s::dto::CompleteMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id.clone())
            .multipart_upload(Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(valid_parts),
            }))
            .build()
            .unwrap();

        let valid_result = inner
            .complete_multipart_upload(s3_request(valid_input))
            .await;
        assert!(valid_result.is_ok());

        // Create new upload for invalid test
        let create_input2 = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-2"))
            .build()
            .unwrap();

        let create_result2 = inner
            .create_multipart_upload(s3_request(create_input2))
            .await
            .expect("Create multipart upload failed");
        let upload_id2 = create_result2.output.upload_id.unwrap();

        // Test invalid non-consecutive parts [1,3] - should fail
        let invalid_parts = vec![
            s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some(ETag::Strong("etag1".to_string())),
                ..Default::default()
            },
            s3s::dto::CompletedPart {
                part_number: Some(3),
                e_tag: Some(ETag::Strong("etag3".to_string())),
                ..Default::default()
            },
        ];
        let invalid_input = s3s::dto::CompleteMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-2"))
            .upload_id(upload_id2)
            .multipart_upload(Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(invalid_parts),
            }))
            .build()
            .unwrap();

        let invalid_result = inner
            .complete_multipart_upload(s3_request(invalid_input))
            .await;
        assert!(invalid_result.is_err());
    }

    #[tokio::test]
    async fn test_directory_bucket_requires_contiguous_parts() {
        // Drive the handler directly: the AWS SDK client rejects `--x-s3` names
        // client-side (InvalidBucketName) before a request can reach the server,
        // so a directory-bucket gap can only be exercised at the handler level.
        let bucket = "test-dir-bucket--usw2-az1--x-s3";
        let key = "dir-gap-key";

        let storage = InMemoryStorage::new();
        storage.create_bucket(bucket).await.unwrap();
        let inner = Inner::new(storage);

        let create_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from(bucket))
            .key(s3s::dto::ObjectKey::from(key))
            .build()
            .unwrap();
        let upload_id = inner
            .create_multipart_upload(s3_request(create_input))
            .await
            .expect("create mpu")
            .output
            .upload_id
            .unwrap();

        // Upload real parts 1 and 3 (a gap), no checksums.
        let mut etags = Vec::new();
        for part_num in [1, 3] {
            let content = format!("part {part_num}");
            let input = s3s::dto::UploadPartInput::builder()
                .bucket(s3s::dto::BucketName::from(bucket))
                .key(s3s::dto::ObjectKey::from(key))
                .upload_id(upload_id.clone())
                .part_number(part_num)
                .body(Some(s3s::dto::StreamingBlob::wrap(
                    futures_util::stream::once(async move {
                        Ok::<_, std::io::Error>(bytes::Bytes::from(content))
                    }),
                )))
                .build()
                .unwrap();
            etags.push(
                inner
                    .upload_part(s3_request(input))
                    .await
                    .expect("upload part")
                    .output
                    .e_tag
                    .unwrap(),
            );
        }

        let parts = vec![
            s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some(etags[0].clone()),
                ..Default::default()
            },
            s3s::dto::CompletedPart {
                part_number: Some(3),
                e_tag: Some(etags[1].clone()),
                ..Default::default()
            },
        ];
        let complete_input = s3s::dto::CompleteMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from(bucket))
            .key(s3s::dto::ObjectKey::from(key))
            .upload_id(upload_id)
            .multipart_upload(Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(parts),
            }))
            .build()
            .unwrap();

        let err = inner
            .complete_multipart_upload(s3_request(complete_input))
            .await
            .expect_err("a directory-bucket gap must be rejected");

        // Directory buckets return HTTP 400 Bad Request (InvalidPartOrder) for a gap,
        // distinct from the HTTP 500 that composite checksums return.
        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidPartOrder);
        assert!(
            err.message()
                .is_some_and(|m| m.contains("did not have consecutive part numbers")),
            "expected the directory bucket message, got: {:?}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_default_crc64nvme_multipart() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        // Test CreateMultipartUpload without specifying algorithm - should default to CRC64NVME
        let input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .build()
            .unwrap();

        let result = inner.create_multipart_upload(s3_request(input)).await;
        assert!(result.is_ok());

        // The response should include the default CRC64NVME algorithm
        let output = result.unwrap().output;
        assert_eq!(
            output.checksum_algorithm,
            Some(s3s::dto::ChecksumAlgorithm::from("crc64nvme".to_string()))
        );
        assert_eq!(
            output.checksum_type,
            Some(s3s::dto::ChecksumType::from("FULL_OBJECT".to_string()))
        ); // Default checksum_type for CRC64NVME
    }

    #[tokio::test]
    async fn test_checksum_type_validation() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("test-bucket").await.unwrap();
        let inner = Inner::new(storage);

        // Test invalid combination: MD5 with FULL_OBJECT should fail
        let invalid_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .checksum_algorithm(Some(s3s::dto::ChecksumAlgorithm::from("MD5".to_string())))
            .checksum_type(Some(s3s::dto::ChecksumType::from(
                "FULL_OBJECT".to_string(),
            )))
            .build()
            .unwrap();

        let result = inner
            .create_multipart_upload(s3_request(invalid_input))
            .await;
        assert!(result.is_err());

        // Test invalid combination: CRC64NVME with COMPOSITE should fail
        let invalid_input2 = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-2"))
            .checksum_algorithm(Some(s3s::dto::ChecksumAlgorithm::from(
                "CRC64NVME".to_string(),
            )))
            .checksum_type(Some(s3s::dto::ChecksumType::from("COMPOSITE".to_string())))
            .build()
            .unwrap();

        let result2 = inner
            .create_multipart_upload(s3_request(invalid_input2))
            .await;
        assert!(result2.is_err());

        // Test valid combination: CRC32 with COMPOSITE should succeed
        let valid_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-3"))
            .checksum_algorithm(Some(s3s::dto::ChecksumAlgorithm::from("CRC32".to_string())))
            .checksum_type(Some(s3s::dto::ChecksumType::from("COMPOSITE".to_string())))
            .build()
            .unwrap();

        let result3 = inner.create_multipart_upload(s3_request(valid_input)).await;
        assert!(result3.is_ok());
        let output = result3.unwrap().output;
        assert_eq!(
            output.checksum_algorithm,
            Some(s3s::dto::ChecksumAlgorithm::from("crc32".to_string()))
        );
        assert_eq!(
            output.checksum_type,
            Some(s3s::dto::ChecksumType::from("COMPOSITE".to_string()))
        );
    }
}
