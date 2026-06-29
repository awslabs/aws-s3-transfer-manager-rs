/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes_utils::SegmentedBuf;

use super::input::copy_fields_to_get_object_request;

use crate::error::{self, ChunkRef, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::{BodySlot, BodyWriter, ChunkOutput};
use crate::operation::download::read_ahead::ReadAhead;
use crate::operation::download::recv_buffer::FillOutcome;
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::DownloadState;
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::DownloadInput;
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::BucketType;

/// Download-specific work data.
#[derive(Debug)]
pub(crate) enum DownloadWork {
    Discovery,
    GetObjectRange {
        range: std::ops::RangeInclusive<u64>,
        slot: Option<BodySlot>,
        etag: Option<Arc<str>>,
    },
}

/// Early return if transfer is terminal (failed/cancelled by another work item).
macro_rules! bail_if_terminal {
    ($self:expr) => {
        if !$self.inner.ctx.is_active() {
            $self.decrement_in_flight();
            return WorkOutcome::Cancelled;
        }
    };
}

/// Download transfer that generates and executes download work.
///
/// Cheap to clone - all state is behind `Arc`.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    inner: Arc<DownloadTransferInner>,
}

/// Internal state for download transfer.
#[derive(Debug)]
struct DownloadTransferInner {
    /// Common transfer lifecycle management
    ctx: TransferContext,
    /// State machine for work progression
    state: Mutex<DownloadState>,
    /// The original request
    request: Arc<DownloadInput>,
    /// Type of S3 bucket targeted by this operation.
    // TODO(vnext): unify bucket representation (name + kind) across operations.
    #[allow(dead_code)]
    bucket_type: BucketType,
    /// Chunk delivery + disk-write surface.
    writer: BodyWriter,
    /// Read-ahead window (rwnd): bounds resident occupancy by holding
    /// `issued - consumed` under `read_ahead.window()`.
    read_ahead: ReadAhead,
    /// Object metadata from discovery (set once discovery completes)
    object_meta: std::sync::OnceLock<ObjectMetadata>,
    /// Object-integrity result (set once discovery completes)
    integrity_checks: std::sync::OnceLock<crate::types::IntegrityChecks>,
    /// Notified when discovery completes (success or failure)
    discovery_notify: tokio::sync::Notify,
}

impl DownloadTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        bucket_type: BucketType,
        input: DownloadInput,
        writer: BodyWriter,
    ) -> Self {
        let inner = Arc::new(DownloadTransferInner {
            read_ahead: ReadAhead::new(),
            ctx,
            state: Mutex::new(DownloadState::new()),
            request: Arc::new(input),
            bucket_type,
            writer,
            object_meta: std::sync::OnceLock::new(),
            integrity_checks: std::sync::OnceLock::new(),
            discovery_notify: tokio::sync::Notify::new(),
        });
        Self { inner }
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    /// Get the transfer ID.
    pub(crate) fn id(&self) -> TransferId {
        self.inner.ctx.id
    }

    /// Body writer for chunk delivery and disk writes.
    pub(crate) fn writer(&self) -> &BodyWriter {
        &self.inner.writer
    }

    /// The read-ahead window (the receive-window issuance bound). Test-only:
    /// production code reads `self.inner.read_ahead` directly at the gate.
    #[cfg(test)]
    pub(crate) fn read_ahead(&self) -> &ReadAhead {
        &self.inner.read_ahead
    }

    /// Object metadata from discovery.
    pub(crate) fn object_meta(&self) -> Option<&ObjectMetadata> {
        self.inner.object_meta.get()
    }

    /// Object-integrity result from discovery.
    pub(crate) fn integrity_checks(&self) -> Option<&crate::types::IntegrityChecks> {
        self.inner.integrity_checks.get()
    }

    /// Notified when discovery completes.
    pub(crate) fn discovery_notify(&self) -> &tokio::sync::Notify {
        &self.inner.discovery_notify
    }

    /// Whether download checksum validation is in effect, resolved the same way
    /// the SDK's GetObject mutator does: enabled if the request set
    /// `ChecksumMode=Enabled`, or it is unset and the client's
    /// `ResponseChecksumValidation` is not `WhenRequired` (`WhenSupported`, the
    /// default, and unknown values enable it).
    fn validation_enabled(&self, input: &DownloadInput) -> bool {
        match input.checksum_mode {
            Some(aws_sdk_s3::types::ChecksumMode::Enabled) => true,
            _ => !matches!(
                self.ctx()
                    .s3_client()
                    .config()
                    .response_checksum_validation()
                    .copied()
                    .unwrap_or_default(),
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired
            ),
        }
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - waiting for in-flight work to complete
    /// - `PollWork::Done` - transfer complete
    #[tracing::instrument(level = "debug", skip(self), fields(tid = %self.id()))]
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            tracing::debug!("not active, returning Done");
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().unwrap();

        match &mut *state {
            DownloadState::PendingDiscovery => {
                *state = DownloadState::DiscoveryInFlight;
                PollWork::Ready(IoRequest {
                    data: Some(Box::new(DownloadWork::Discovery)),
                })
            }
            DownloadState::DiscoveryInFlight => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            DownloadState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
                part_size,
                issued,
            } => {
                // Read-ahead gate: bound resident occupancy. `issued - consumed` is
                // the count of parts claimed but not yet delivered in order; the gate
                // holds it under the receive window. A slow or blocked consumer is
                // paced for free — occupancy fills, the gate closes, issuance waits.
                let consumed = self.inner.writer.consumed();
                let window = self.inner.read_ahead.window();
                if *issued - consumed >= window {
                    // Gate closed: the issuer is `window` parts ahead of the consumer
                    // and must wait for a delivery (which advances `consumed`) before
                    // claiming more. This is the load-bearing line for diagnosing a
                    // stall: a *wedge* shows up here as a gate that never reopens —
                    // `consumed` frozen, `issued - consumed == window`, recurring every
                    // poll. A healthy slow consumer shows the same fields advancing in
                    // step. Trace, not debug: it fires every gated poll in steady state.
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        issued = *issued,
                        consumed,
                        in_flight = *issued - consumed,
                        window,
                        "read-ahead gate closed: issuance paused until the consumer drains",
                    );
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }

                let slot = self.inner.writer.claim();
                *issued += 1;

                if let Some(range) = remaining.take() {
                    // `part_size` is the stored part size for a validated multipart
                    // object (so each range aligns to a stored part boundary and S3
                    // returns the part's checksum for the SDK to validate), else the
                    // configured download part size. Set at discovery.
                    let part_size = *part_size;
                    let start = *range.start();
                    let end = *range.end();
                    let chunk_end = cmp::min(start + part_size - 1, end);
                    let chunk_range = start..=chunk_end;

                    if chunk_end < end {
                        *remaining = Some((chunk_end + 1)..=end);
                    }

                    *ranges_in_flight += 1;

                    PollWork::Ready(IoRequest {
                        data: Some(Box::new(DownloadWork::GetObjectRange {
                            range: chunk_range,
                            slot: Some(slot),
                            etag: etag.clone(),
                        })),
                    })
                } else if *ranges_in_flight > 0 {
                    // All ranges generated, waiting for in-flight to complete
                    self.inner.ctx.set_pending();
                    PollWork::Pending
                } else {
                    // All done - success
                    self.complete(state);
                    PollWork::Done
                }
            }
            DownloadState::Terminal => PollWork::Done,
        }
    }

    #[tracing::instrument(level = "debug", skip(self, work), fields(tid = %self.id(), work = ?work.data))]
    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::Discovery => self.execute_discovery().await,
            DownloadWork::GetObjectRange { range, slot, etag } => {
                self.execute_get_range(
                    range.clone(),
                    slot.take().expect("slot already consumed"),
                    etag.clone(),
                )
                .await
            }
        }
    }

    async fn execute_discovery(&self) -> WorkOutcome {
        let input = self.inner.request.as_ref();

        // Resolve the effective validation state the same way the SDK's GetObject
        // mutator does: enabled if the request set ChecksumMode=Enabled, or the
        // request left it unset and the client's ResponseChecksumValidation is not
        // WhenRequired (WhenSupported, the default, and unknown values enable it).
        // Computed before discovery because it drives range alignment.
        let validation_enabled = self.validation_enabled(input);

        let discovery = match discover_obj(self, input, validation_enabled).await {
            Ok(d) => d,
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, e);
            }
        };

        let ObjectDiscovery {
            remaining,
            object_meta,
            initial_chunk,
            chunk_meta,
            effective_part_size,
        } = discovery;

        // Store object_meta for object_meta() and join()
        let _ = self.inner.object_meta.set(object_meta.clone());

        // Object checksum is known only when the whole object arrived in this
        // chunk. A larger multipart object's discovery chunk carries part 1's
        // checksum, which is not the object checksum.
        let whole_object_in_chunk = input.range.is_none() && remaining.is_none();
        let integrity = build_integrity_checks(
            chunk_meta.as_ref(),
            whole_object_in_chunk,
            validation_enabled,
        );
        let _ = self.inner.integrity_checks.set(integrity);

        // Notify waiters that discovery completed
        self.inner.discovery_notify.notify_waiters();

        let etag: Option<Arc<str>> = object_meta.e_tag.as_deref().map(Arc::from);

        // Optimization: Preallocate space for the full object/download size if there is a
        // destination/sink and it supports it. e.g. pre-allocate disk space for the full
        // download to avoid per-write metadata updates and late ENOSPC errors.
        let chunk_content_len = chunk_meta
            .as_ref()
            .and_then(|m| m.content_length)
            .unwrap_or(0) as u64;
        let total_size =
            chunk_content_len + remaining.as_ref().map_or(0, |r| r.end() - r.start() + 1);
        self.inner.writer.preallocate(total_size);
        self.inner.ctx.set_total_bytes(total_size);

        // If there's an initial chunk, claim seq BEFORE waking to prevent race
        // where poll_work exhausts the window before we can claim our seq.
        // Invariant: initial_chunk.is_some() == chunk_meta.is_some()
        let initial_work = match (initial_chunk, chunk_meta) {
            (Some(stream), Some(meta)) => {
                let slot = self.inner.writer.claim();
                Some((stream, meta, slot))
            }
            (None, _) => None,
            (Some(_), None) => {
                panic!("invalid discovery state: initial_chunk present without chunk_meta")
            }
        };

        {
            let mut work = self.inner.state.lock().unwrap();
            *work = DownloadState::Transferring {
                remaining,
                ranges_in_flight: if initial_work.is_some() { 1 } else { 0 },
                etag: etag.clone(),
                part_size: effective_part_size,
                issued: if initial_work.is_some() { 1 } else { 0 },
            };
        }

        // State changed from DiscoveryInFlight - try to wake
        self.inner.ctx.try_wake();

        // If discovery returned an initial chunk, process it
        match initial_work {
            Some((stream, chunk_meta, slot)) => {
                self.execute_read_discovery_body(stream, slot, chunk_meta, etag)
                    .await
            }
            None => WorkOutcome::Success { data: None },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
        slot: BodySlot,
        chunk_meta: ChunkMetadata,
        etag: Option<Arc<str>>,
    ) -> WorkOutcome {
        let seq = slot.seq();
        let input = self.inner.request.as_ref();
        // The discovery GET already fetched this chunk's bytes; the first read
        // consumes that stream with no extra request (preserving time-to-first-
        // byte). A retry has no stream to reuse, so it re-issues a ranged GET for
        // exactly the bytes this chunk covers, taken from the discovery response's
        // content-range. A partNumber=1 discovery maps to the same byte range, so
        // the re-issued aligned range returns the same per-part checksum the SDK
        // validates against.
        let reissue_range = chunk_meta
            .content_range
            .as_deref()
            .and_then(crate::http::header::parse_content_range);

        let mut initial = Some(stream);
        let result = crate::retry::retry_guarded(
            &self.inner.ctx.handle.telemetry.recv_latencies,
            crate::retry::classify_body_retry,
            || {
                let pre_issued = initial.take();
                let etag = etag.clone();
                let ctx = self.inner.ctx.clone();
                let reissue_range = reissue_range.clone();
                let mut builder =
                    copy_fields_to_get_object_request(input, ctx.s3_client().get_object());
                if let Some(r) = reissue_range.as_ref() {
                    builder = builder.set_range(Some(format!("bytes={}-{}", r.start(), r.end())));
                }
                if let Some(etag) = etag.as_ref() {
                    builder = builder.if_match(etag.as_ref());
                }
                let req = builder
                    .customize()
                    .config_override(crate::retry::bucket_partition_override(input.bucket()));
                async move {
                    let body = match pre_issued {
                        // First attempt: the body discovery already fetched.
                        Some(s) => s,
                        // Retry: re-fetch the discovery chunk's range.
                        None => {
                            let resp = req.send().await.map_err(crate::error::Error::from)?;
                            resp.body
                        }
                    };
                    Self::read_body_stream(&ctx, body).await
                }
            },
        )
        .await;

        let (segmented, bytes_received) = match result {
            Ok(val) => val,
            // Go terminal before any wake: fail() sets Terminal under the lock, so
            // a woken poll_work cannot observe ranges_in_flight==0 and complete()
            // the transfer over this error. The in-flight count is abandoned with
            // the Transferring state.
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, e.with_chunk(ChunkRef::new(seq, None)));
            }
        };

        let chunk = ChunkOutput {
            seq,
            offset: chunk_meta
                .content_range
                .as_deref()
                .and_then(crate::http::header::parse_content_range)
                .map(|r| *r.start())
                .unwrap_or(0),
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        // Edge-triggered disk write: only the fill that seals a segment drains it.
        if slot.fill(chunk) == FillOutcome::SealedSegment {
            if let Err(e) = self.inner.writer.drain_sealed() {
                // Go terminal before any wake (see fail_range).
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            }
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        self.decrement_in_flight();

        WorkOutcome::Success { data: None }
    }

    /// Drain a chunk body stream into a buffer, returning the bytes and the count.
    ///
    /// Shared by the range-chunk and discovery-chunk paths. A stream error is
    /// classified by [`error::body_read_error`]: a checksum mismatch becomes
    /// [`ErrorKind::IntegrityError`] (which the retry classifier never re-issues,
    /// to avoid masking corruption), any other stream failure becomes
    /// [`ErrorKind::IOError`] (the body-read class the classifier re-issues). A
    /// cancellation observed mid-read maps to [`ErrorKind::OperationCancelled`].
    async fn read_body_stream(
        ctx: &TransferContext,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<(SegmentedBuf<bytes::Bytes>, u64), crate::error::Error> {
        let mut segmented = SegmentedBuf::new();
        let mut bytes_received: u64 = 0;
        let mut body_stream = body;
        while let Some(result) = body_stream.next().await {
            let data = result.map_err(|e| crate::error::body_read_error(e, None))?;
            bytes_received += data.len() as u64;
            segmented.push(data);
            if !ctx.is_active() {
                return Err(crate::error::Error::new(
                    crate::error::ErrorKind::OperationCancelled,
                    "transfer cancelled during body read",
                ));
            }
        }
        Ok((segmented, bytes_received))
    }

    async fn execute_get_range(
        &self,
        range: std::ops::RangeInclusive<u64>,
        slot: BodySlot,
        etag: Option<Arc<str>>,
    ) -> WorkOutcome {
        let seq = slot.seq();
        let input = self.inner.request.as_ref();
        let range_header = format!("bytes={}-{}", range.start(), range.end());

        let result = crate::retry::retry_guarded(
            &self.inner.ctx.handle.telemetry.recv_latencies,
            crate::retry::classify_body_retry,
            || {
                let rh = range_header.clone();
                let etag = etag.clone();
                let ctx = self.inner.ctx.clone();
                // Every chunk GET must carry the same request fields as discovery
                // (checksum_mode, SSE-C key, version_id, ...). Derive from the input
                // conversion, then pin this chunk's range and the discovered etag.
                let mut builder =
                    copy_fields_to_get_object_request(input, ctx.s3_client().get_object());
                builder = builder.set_range(Some(rh.clone()));
                if let Some(etag) = etag.as_ref() {
                    builder = builder.if_match(etag.as_ref());
                }
                let req = builder
                    .customize()
                    .config_override(crate::retry::bucket_partition_override(input.bucket()));

                async move {
                    let resp = req.send().await.map_err(crate::error::Error::from)?;
                    validate_content_range(&rh, resp.content_range())?;
                    let chunk_meta = ChunkMetadata::from(&resp);
                    let (segmented, bytes_received) =
                        Self::read_body_stream(&ctx, resp.body).await?;
                    Ok::<_, crate::error::Error>((chunk_meta, segmented, bytes_received))
                }
            },
        )
        .await;

        let (chunk_meta, segmented, bytes_received) = match result {
            Ok(val) => val,
            Err(e) => {
                return self.fail_range(ChunkRef::new(seq, Some(*range.start()..=*range.end())), e)
            }
        };

        bail_if_terminal!(self);
        let chunk = ChunkOutput {
            seq,
            offset: *range.start(),
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        // Edge-triggered disk write: only the fill that seals a segment drains it
        // (one positioned write per full segment), not every fill. Stream mode's
        // drain_sealed is a no-op.
        if slot.fill(chunk) == FillOutcome::SealedSegment {
            if let Err(e) = self.inner.writer.drain_sealed() {
                // Go terminal before any wake (see fail_range).
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            }
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        self.decrement_in_flight();

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            seq,
            offset = *range.start(),
            "chunk downloaded",
        );

        WorkOutcome::Success { data: None }
    }

    /// Fail a range request with an error, tagging the chunk it belongs to.
    fn fail_range(&self, location: ChunkRef, e: Error) -> WorkOutcome {
        // Go terminal before any wake (see the discovery-body error path). Do not
        // decrement_in_flight first: that wakes poll_work, which would observe
        // ranges_in_flight==0 and complete() over this error.
        let guard = self.inner.state.lock().unwrap();
        self.fail(guard, e.with_chunk(location))
    }

    fn decrement_in_flight(&self) {
        let should_wake = {
            let mut work = self.inner.state.lock().unwrap();
            if let DownloadState::Transferring {
                ranges_in_flight, ..
            } = &mut *work
            {
                *ranges_in_flight = ranges_in_flight.saturating_sub(1);
                *ranges_in_flight == 0
            } else {
                false
            }
        };
        if should_wake {
            self.inner.ctx.try_wake();
        }
    }

    /// Transition to terminal failed state. Requires holding the work lock.
    fn fail(
        &self,
        mut guard: std::sync::MutexGuard<'_, DownloadState>,
        error: Error,
    ) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            "download failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        // Order matters: set status/error before any wakeups
        self.inner.ctx.set_failed(error);
        // Transition to Terminal
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
                     // Wake all waiters
        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
        WorkOutcome::Failed { classification }
    }

    /// Transition to terminal success state. Requires holding the work lock.
    fn complete(&self, mut guard: std::sync::MutexGuard<'_, DownloadState>) {
        if let Err(e) = self.inner.writer.finalize() {
            self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            return;
        }
        self.inner.ctx.set_completed();
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
    }
}

impl Transfer for DownloadTransfer {
    fn ctx(&self) -> &TransferContext {
        DownloadTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        DownloadTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(DownloadTransfer::execute(self, work))
    }

    fn on_terminal(&self) {
        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
    }
}

/// Validate that the response Content-Range matches the requested range.
fn validate_content_range(
    requested_range: &str,
    response_content_range: Option<&str>,
) -> Result<(), Error> {
    let normalized = requested_range
        .strip_prefix("bytes=")
        .unwrap_or(requested_range);

    if response_content_range
        .map(|range| range.contains(normalized))
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(error::Error::new(
            error::ErrorKind::RuntimeError,
            format!(
                "content range mismatch: requested {}, response {:?}",
                requested_range, response_content_range
            ),
        ))
    }
}

/// Build the object-integrity result for a completed download.
///
/// `whole_object_in_chunk` must be true only when the entire object arrived in
/// the discovery chunk, the one case where the chunk checksum is the object
/// checksum. Otherwise value members are left `None`.
///
/// Validation is reported `Disabled` when not requested, else
/// `NotValidated{Unavailable}`. `Validated{algorithm}` requires a per-response
/// validation outcome the Rust SDK does not currently expose.
// TODO(vnext): resolve `Validated{algorithm}` from an SDK-reported per-response
// validation outcome once the SDK exposes one, instead of always reporting
// NotValidated when validation is enabled.
fn build_integrity_checks(
    chunk_meta: Option<&ChunkMetadata>,
    whole_object_in_chunk: bool,
    validation_enabled: bool,
) -> crate::types::IntegrityChecks {
    use crate::types::{ChecksumValidation, NotValidatedReason};

    let checksum_validation = if validation_enabled {
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Unavailable,
        }
    } else {
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Disabled,
        }
    };

    // Surface checksum values only when they describe the whole object.
    let cm = chunk_meta.filter(|_| whole_object_in_chunk);
    crate::types::IntegrityChecks::new(
        cm.and_then(|m| m.checksum_crc32.clone()),
        cm.and_then(|m| m.checksum_crc32_c.clone()),
        cm.and_then(|m| m.checksum_crc64_nvme.clone()),
        cm.and_then(|m| m.checksum_sha1.clone()),
        cm.and_then(|m| m.checksum_sha256.clone()),
        cm.and_then(|m| m.checksum_type.clone()),
        checksum_validation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::DownloadInput;
    use crate::scheduler::test_util::{assert_done, assert_pending, assert_ready};
    use crate::transfer::TransferContext;
    use crate::transfer::{IoRequest, WorkOutcome};
    use crate::types::BucketType;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    const MB: u64 = 1024 * 1024;

    use crate::operation::download::chunk_meta::ChunkMetadata;
    use crate::types::{ChecksumValidation, NotValidatedReason};
    use aws_sdk_s3::types::ChecksumType;

    fn chunk_with_crc32(value: &str) -> ChunkMetadata {
        let mut m = ChunkMetadata::default();
        m.checksum_crc32 = Some(value.to_string());
        m.checksum_type = Some(ChecksumType::FullObject);
        m
    }

    #[test]
    fn integrity_disabled_when_validation_not_enabled() {
        // validation_enabled=false models a WhenRequired client with no request
        // override: no validation is attempted, so the verdict is Disabled.
        let ic = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, false);
        assert_eq!(
            *ic.checksum_validation(),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Disabled
            }
        );
    }

    #[test]
    fn integrity_unavailable_when_enabled_pending_sdk_signal() {
        // TODO(vnext): becomes Validated once the SDK reports a validation outcome.
        let ic = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, true);
        assert_eq!(
            *ic.checksum_validation(),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Unavailable
            }
        );
    }

    #[test]
    fn integrity_surfaces_value_only_for_whole_object_chunk() {
        // Whole object in the discovery chunk -> the chunk checksum IS the object's.
        let whole = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, false);
        assert_eq!(whole.checksum_crc32(), Some("DUoRhQ=="));
        assert_eq!(whole.checksum_type(), Some(&ChecksumType::FullObject));

        // Multipart (object did not fit in the discovery chunk) -> part-1 value is
        // NOT surfaced as the object value.
        let multipart = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), false, false);
        assert_eq!(multipart.checksum_crc32(), None);
        assert_eq!(multipart.checksum_type(), None);
    }

    fn create_download(object_size: u64, part_size: u64) -> DownloadTransfer {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        create_download_with_client(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]),
            part_size,
        )
    }

    fn create_download_with_client(client: aws_sdk_s3::Client, part_size: u64) -> DownloadTransfer {
        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_slot_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);

        DownloadTransfer::new(ctx, BucketType::Standard, input, writer)
    }

    /// Execute work using DownloadTransfer directly.
    async fn execute(transfer: &DownloadTransfer, work: &mut IoRequest) -> WorkOutcome {
        transfer.execute(work).await
    }

    /// Run discovery to completion
    async fn skip_discovery(transfer: &DownloadTransfer) {
        let mut work = assert_ready(transfer.poll_work());
        execute(transfer, &mut work).await;
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_initial_poll_returns_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::Discovery));
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_pending_while_discovery_in_flight() {
        let transfer = create_download(24 * MB, 8 * MB);
        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_generates_ranges_after_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::GetObjectRange { .. }));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_one_with_initial_chunk() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    1,
                    "seq should start at 1 when initial chunk claims seq=0"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_zero_without_initial_chunk() {
        let head_obj = mock!(aws_sdk_s3::Client::head_object).then_output(|| {
            aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
                .content_length(24 * MB as i64)
                .e_tag("test-etag")
                .build()
        });
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(8 * MB as i64)
                .content_range(format!("bytes 0-{}/{}", 8 * MB - 1, 24 * MB))
                .e_tag("test-etag")
                .body(ByteStream::from(vec![0u8; 8 * MB as usize]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[head_obj, get_obj]);

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(8 * MB))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=0-")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_slot_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);

        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    0,
                    "seq should start at 0 when no initial chunk"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_increments_sequentially() {
        let transfer = create_download(32 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut seqs = Vec::new();
        while let PollWork::Ready(mut w) = transfer.poll_work() {
            let data = w.data_mut::<DownloadWork>();
            if let DownloadWork::GetObjectRange { slot, .. } = data {
                seqs.push(slot.as_ref().unwrap().seq());
            }
        }

        assert_eq!(seqs.len(), 3, "expected multiple ranges");
        for i in 1..seqs.len() {
            assert_eq!(seqs[i], seqs[i - 1] + 1, "seqs should be sequential");
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_pending_when_range_in_flight() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        // generate range work but don't complete
        let _range = assert_ready(transfer.poll_work());
        // transfer is considered active and shouldn't transition to done until all in-flight work is complete and handle is joined/dropped
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_done_when_all_complete() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        execute(&transfer, &mut range).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_out_of_order_completion() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range1 = assert_ready(transfer.poll_work()); // seq=1
        let mut range2 = assert_ready(transfer.poll_work()); // seq=2

        // Complete in reverse order
        execute(&transfer, &mut range2).await;
        execute(&transfer, &mut range1).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_failure_transitions_to_failed() {
        let _logs = show_test_logs();
        // Fail seq 1 (first range after discovery)
        let transfer = FailureConfig::new(24 * MB, 8 * MB).fail(1).build();

        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut range).await;

        assert!(matches!(outcome, WorkOutcome::Failed { .. }));
        assert!(transfer.ctx().is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation_transitions_to_cancelled() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        transfer.ctx().set_cancelled();
        transfer.ctx().signal_terminal();

        assert!(transfer.ctx().is_cancelled());
        assert_done(transfer.poll_work());
    }

    fn create_download_for_gate(
        object_size: u64,
        part_size: u64,
    ) -> (
        DownloadTransfer,
        crate::operation::download::body::SlotBodyConsumer,
    ) {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]);

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, consumer) = crate::operation::download::body::new_slot_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);
        (transfer, consumer)
    }

    /// The read-ahead gate bounds issuance at the current window: with the window
    /// forced small (as a slow consumer's pacing would), issuance proceeds up to
    /// `issued − consumed == window()`, then stalls. (The window's *value* is the
    /// controller's job, unit-tested in `read_ahead`; here we test the gate wiring.)
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_read_ahead_gate_bounds_issuance() {
        let (transfer, _consumer) = create_download_for_gate(128 * MB, 8 * MB);
        skip_discovery(&transfer).await;
        // Force a small window to exercise the gate deterministically (the rwnd default
        // opens wide, so a slow-consumer pacing is what makes it bind).
        transfer.read_ahead().force_window(3);
        let w = transfer.read_ahead().window();
        // Discovery claimed seq 0 (issued=1, consumed=0). Drive until the gate stalls.
        let mut issued = 1u64;
        loop {
            match transfer.poll_work() {
                PollWork::Ready(_item) => {
                    issued += 1;
                    assert!(issued <= w, "issuance ran past the window");
                }
                PollWork::Pending => break,
                PollWork::Done => panic!("unexpected Done"),
            }
        }
        assert_eq!(issued, w, "issuance should fill the window then stall");
    }

    /// Consuming a delivered chunk advances `consumed`, reopening the gate for more
    /// issuance (`issued − consumed` drops below the window).
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_consume_reopens_gate() {
        let (transfer, mut consumer) = create_download_for_gate(128 * MB, 8 * MB);
        skip_discovery(&transfer).await;
        transfer.read_ahead().force_window(3);
        // Fill the window.
        while let PollWork::Ready(_item) = transfer.poll_work() {}
        assert_pending(transfer.poll_work());

        // Consume seq 0 (filled by discovery) — consumed advances 0 → 1, freeing a slot.
        assert!(consumer.try_take_next().is_some(), "discovery chunk should deliver");

        // Gate reopens for exactly one more claim.
        assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    use crate::http::header::Range;
    use aws_smithy_mocks::MockResponse;
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use std::collections::HashMap;

    /// Failure behavior for a specific seq
    #[derive(Clone)]
    enum FailureBehavior {
        /// Always fail
        Always,
        /// Fail first N attempts, then succeed
        #[allow(dead_code)] // TODO: re-enable with retry tests
        Times(usize),
    }

    /// Builder for downloads with configurable failure injection
    struct FailureConfig {
        object_size: u64,
        part_size: u64,
        failures: HashMap<u64, FailureBehavior>,
    }

    impl FailureConfig {
        fn new(object_size: u64, part_size: u64) -> Self {
            Self {
                object_size,
                part_size,
                failures: HashMap::new(),
            }
        }

        /// Fail this seq always (retryable 500 error)
        fn fail(mut self, seq: u64) -> Self {
            self.failures.insert(seq, FailureBehavior::Always);
            self
        }

        fn build(self) -> DownloadTransfer {
            let object_size = self.object_size;
            let part_size = self.part_size;
            let failures = Arc::new(self.failures);
            let call_counts: Arc<std::sync::Mutex<HashMap<u64, usize>>> =
                Arc::new(std::sync::Mutex::new(HashMap::new()));

            let get_obj = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
                let seq = req
                    .range()
                    .and_then(|r| r.parse::<Range>().ok())
                    .map(|r| match r.0 {
                        crate::http::header::ByteRange::Inclusive(start, _) => start / part_size,
                        crate::http::header::ByteRange::AllFrom(start) => start / part_size,
                        _ => 0,
                    })
                    .unwrap_or(0);

                // Track calls per seq
                let mut counts = call_counts.lock().unwrap();
                let call_num = *counts.entry(seq).and_modify(|c| *c += 1).or_insert(1);
                drop(counts);

                // Check failure config
                let should_fail = match failures.get(&seq) {
                    Some(FailureBehavior::Always) => true,
                    Some(FailureBehavior::Times(n)) => call_num <= *n,
                    None => false,
                };

                if should_fail {
                    MockResponse::Http(Response::new(
                        StatusCode::try_from(500).unwrap(),
                        SdkBody::from("internal error"),
                    ))
                } else {
                    let start = seq * part_size;
                    let end = std::cmp::min(start + part_size, object_size) - 1;
                    let chunk = vec![0u8; (end - start + 1) as usize];

                    MockResponse::Output(
                        GetObjectOutput::builder()
                            .content_length((end - start + 1) as i64)
                            .content_range(format!("bytes {}-{}/{}", start, end, object_size))
                            .e_tag("test-etag")
                            .body(ByteStream::from(chunk))
                            .build(),
                    )
                }
            });

            create_download_with_client(
                mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj], |conf| {
                    conf.retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                }),
                part_size,
            )
        }
    }

    #[test]
    fn test_validate_content_range_success() {
        assert!(validate_content_range("bytes=1024-2047", Some("bytes 1024-2047/4096")).is_ok());
        assert!(validate_content_range("1024-2047", Some("bytes 1024-2047/4096")).is_ok());
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        assert!(validate_content_range("bytes=1024-2047", Some("bytes 2048-3071/4096")).is_err());
    }

    #[test]
    fn test_validate_content_range_missing() {
        assert!(validate_content_range("bytes=1024-2047", None).is_err());
    }
}
