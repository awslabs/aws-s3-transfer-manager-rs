/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::IoSlice;

use bytes::Buf;

use crate::runtime::buffer_pool::{Reservation, SegmentedBytes};
use crate::runtime::sync::sync::Arc;

use super::chunk_meta::ChunkMetadata;
use super::recv_buffer::{
    DrainMode, FillOutcome, PagedRecvBuffer, RecvBufferConsumer, SegmentWrite, SlotHandle,
};

/// Segment size for the paged buffer. Re-exported from `recv_buffer` for test access.
const SEG_SIZE: usize = super::recv_buffer::DEFAULT_SEG_SIZE;
use super::transfer::DownloadTransfer;

/// Wakeup signal wrapper. Under loom, tokio::sync::Notify is unavailable,
/// but the Notify doesn't participate in safety invariants — it's just a
/// hint to the consumer. A no-op stub is sufficient for loom tests.
#[cfg(not(all(test, s3_tm_loom)))]
use tokio::sync::Notify as WakeNotify;

#[cfg(all(test, s3_tm_loom))]
struct WakeNotify;

#[cfg(all(test, s3_tm_loom))]
impl WakeNotify {
    fn new() -> Self {
        Self
    }
    fn notify_one(&self) {}
    async fn notified(&self) {}
}

/// Borrowed read position over one contiguous run being written to disk.
///
/// The claimed receive-buffer slots retain ownership until the write
/// completes. This cursor presents their bytes without cloning owner metadata.
struct DiskWriteCursor<'a> {
    write: &'a SegmentWrite<ChunkOutput>,
    payload_index: usize,
    payload_offset: usize,
    remaining: usize,
}

impl<'a> DiskWriteCursor<'a> {
    /// Validates one claimed run and returns its object offset and read cursor.
    fn new(write: &'a SegmentWrite<ChunkOutput>) -> std::io::Result<(u64, Self)> {
        let mut object_offset = None;
        let mut expected_offset = None;
        let mut remaining = 0usize;

        for slot in write.payloads() {
            let chunk = slot.get().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "claimed disk-write run contains an unfilled slot",
                )
            })?;
            let chunk_len = chunk.data.remaining();
            if chunk_len == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "claimed disk-write run contains an empty payload",
                ));
            }
            if let Some(expected) = expected_offset {
                if chunk.offset != expected {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "claimed disk-write run has noncontiguous object offsets",
                    ));
                }
            } else {
                object_offset = Some(chunk.offset);
            }

            let chunk_len_u64 = u64::try_from(chunk_len).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "disk-write payload length exceeds object-offset representation",
                )
            })?;
            expected_offset = Some(chunk.offset.checked_add(chunk_len_u64).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "disk-write object offset overflow",
                )
            })?);
            remaining = remaining.checked_add(chunk_len).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "disk-write run length overflow",
                )
            })?;
        }

        let object_offset = object_offset.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "claimed disk-write run contains no payloads",
            )
        })?;
        Ok((
            object_offset,
            Self {
                write,
                payload_index: 0,
                payload_offset: 0,
                remaining,
            },
        ))
    }

    fn current(&self) -> &SegmentedBytes {
        self.write.payloads()[self.payload_index]
            .get()
            .map(|chunk| &chunk.data)
            .expect("validated disk-write payload disappeared")
    }
}

impl Buf for DiskWriteCursor<'_> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }
        self.current().chunk_from(self.payload_offset)
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut written = 0;
        for (index, slot) in self.write.payloads()[self.payload_index..]
            .iter()
            .enumerate()
        {
            if written == dst.len() {
                break;
            }
            let chunk = slot
                .get()
                .expect("validated disk-write payload disappeared");
            let offset = if index == 0 { self.payload_offset } else { 0 };
            written += chunk.data.chunks_vectored_from(offset, &mut dst[written..]);
        }
        written
    }

    fn advance(&mut self, mut count: usize) {
        assert!(
            count <= self.remaining,
            "advanced beyond download write run"
        );
        self.remaining -= count;
        while count != 0 {
            let current_len = self.current().remaining();
            let available = current_len
                .checked_sub(self.payload_offset)
                .expect("disk-write cursor exceeds its payload");
            let advanced = count.min(available);
            self.payload_offset += advanced;
            count -= advanced;
            if self.payload_offset == current_len {
                self.payload_index += 1;
                self.payload_offset = 0;
            }
        }
    }
}

/// Positioned-write target for download-to-file. Abstracts the file so the drain
/// orchestration (run coalescing, offset translation) can be exercised against an
/// in-memory capture, and so an alternative write strategy (e.g. O_DIRECT/io_uring)
/// can replace the file write without touching the buffer or the drain logic.
///
/// `write_all_at` writes the whole buffer at an absolute file position; `preallocate`
/// is a best-effort size hint. Implementations are shared across the issuer and the
/// drain task, hence `Send + Sync`.
trait SinkWrite: Send + Sync + std::fmt::Debug {
    /// Write the entire buffer at `pos` bytes into the target.
    fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()>;

    /// Best-effort preallocation of `len` bytes. Default no-op.
    fn preallocate(&self, _len: u64) {}
}

/// File-backed [`SinkWrite`]: positioned writes via `pwritev`.
struct FileSink {
    file: std::fs::File,
    /// Whether the transfer manager created this file (vs caller-provided). Only an
    /// owned file is preallocated.
    owns_file: bool,
}

impl std::fmt::Debug for FileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSink").finish_non_exhaustive()
    }
}

impl SinkWrite for FileSink {
    fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()> {
        crate::io::fs::write_all_at(&self.file, buf, pos)
    }

    fn preallocate(&self, len: u64) {
        if self.owns_file {
            if let Err(e) = crate::io::fs::preallocate(&self.file, len) {
                tracing::warn!(error = %e, "failed to preallocate file space");
            }
        }
    }
}

/// Stream vs disk discrimination. Mutually exclusive per transfer.
#[derive(Debug)]
enum Mode {
    Stream,
    /// Positioned writes land at `chunk.offset - object_range_start` in the sink.
    Disk {
        sink: Box<dyn SinkWrite>,
        /// Start of the S3 byte range for this transfer.
        object_range_start: u64,
    },
}

/// Producer handle to the download body buffer. Held at `self.inner.writer`
/// in the transfer. Clone shares all state via `Arc`.
#[derive(Clone)]
pub(crate) struct BodyWriter {
    buffer: PagedRecvBuffer<ChunkOutput>,
    notify: Arc<WakeNotify>,
    mode: Arc<Mode>,
}

impl std::fmt::Debug for BodyWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyWriter")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// A claimed slot in the body buffer, produced by [`BodyWriter::claim`].
/// Consuming [`fill`](Self::fill) publishes the payload and notifies the
/// consumer. Drop without fill still wakes the consumer and closes any
/// attached reservation.
pub(crate) struct BodySlot {
    handle: Option<SlotHandle<ChunkOutput>>,
    buffer: PagedRecvBuffer<ChunkOutput>,
    notify: Arc<WakeNotify>,
    /// Memory reservation held from claim through response collection. A
    /// successful fill closes acquisition authority; the immutable payload then
    /// owns every live carrier charge directly. `None` until
    /// [`attach_reservation`], and on paths that do not reserve (tests).
    reservation: Option<Reservation>,
}

impl std::fmt::Debug for BodySlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodySlot")
            .field("seq", &self.handle.as_ref().map(|h| h.seq()))
            .finish_non_exhaustive()
    }
}

impl BodySlot {
    /// The sequence number for this slot.
    pub(crate) fn seq(&self) -> u64 {
        self.handle.as_ref().expect("slot already consumed").seq()
    }

    /// Returns the reservation attached before this slot was dispatched.
    ///
    /// Response attempts borrow this authority while copying transport-owned
    /// frames into pooled carriers. A failed attempt drops its carriers but
    /// leaves the reservation on the slot for the next attempt.
    pub(crate) fn reservation(&self) -> &Reservation {
        self.reservation
            .as_ref()
            .expect("body slot dispatched without a memory reservation")
    }

    /// Attach the memory reservation backing this chunk. It remains open while
    /// response attempts may acquire carriers and closes on successful fill.
    pub(crate) fn attach_reservation(&mut self, reservation: Reservation) {
        self.reservation = Some(reservation);
    }

    /// Publish a completed chunk into this slot. Wakes the stream consumer
    /// and returns the fill outcome (whether a segment was sealed). Immutable
    /// owners retain all acquired carriers, so no direct acquisition authority
    /// is needed after this point.
    pub(crate) fn fill(mut self, chunk: ChunkOutput) -> FillOutcome {
        let handle = self.handle.take().expect("slot already consumed");
        if let Some(reservation) = self.reservation.take() {
            reservation.close_acquisition();
        }
        let outcome = self.buffer.fill(handle, chunk);
        self.notify.notify_one();
        outcome
    }
}

impl Drop for BodySlot {
    fn drop(&mut self) {
        // If the handle is still present the slot was never filled. Notify to
        // prevent the consumer from sleeping forever waiting on this sequence.
        if self.handle.is_some() {
            self.notify.notify_one();
        }
    }
}

impl BodyWriter {
    pub(crate) fn has_sink(&self) -> bool {
        matches!(&*self.mode, Mode::Disk { .. })
    }

    /// Claim the next slot. The read-ahead gate lives in poll_work (tracks
    /// `issued` under the state lock) — this is the unconditional claim.
    pub(crate) fn claim(&self) -> BodySlot {
        let handle = self.buffer.claim();
        BodySlot {
            handle: Some(handle),
            buffer: self.buffer.clone(),
            notify: self.notify.clone(),
            reservation: None,
        }
    }

    /// Disk mode: drain every batch-ready run to disk. Called on the `DrainReady`
    /// edge from execute tasks. A non-terminal drain claims only runs that reach the
    /// drain batch, coalescing each into one positioned write. Stream mode: no-op.
    ///
    /// Returns the number of parts freed across the runs drained by this call.
    /// The download layer releases that read-ahead occupancy under its state
    /// lock.
    pub(crate) fn drain(&self, mode: DrainMode) -> Result<u64, std::io::Error> {
        let mut freed = 0u64;
        if let Mode::Disk {
            sink,
            object_range_start,
        } = &*self.mode
        {
            while let Some(sw) = self.buffer.take_drain_run(mode) {
                write_run(sink.as_ref(), *object_range_start, &sw)?;
                freed = freed.saturating_add(sw.complete());
            }
        }
        Ok(freed)
    }

    /// Terminal drain: flush every remaining filled run, including a partial final
    /// segment below the drain batch. Called once from `complete()` / `on_terminal()`.
    /// Returns the parts freed by this terminal pass (the tail left resident below the
    /// drain batch) so the caller can release the last of the read-ahead occupancy.
    pub(crate) fn finalize(&self) -> Result<u64, std::io::Error> {
        if !matches!(&*self.mode, Mode::Disk { .. }) {
            return Ok(0);
        }
        let terminal_parts = self.drain(DrainMode::Eager)?;
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            terminal_parts,
            "terminal drain complete; tail flushed to disk",
        );
        Ok(terminal_parts)
    }

    /// Whether a forced eager drain would free at least one resident part right now:
    /// the disk surface holds a contiguous filled run at some segment's claim cursor.
    /// Stream mode returns false: its consumer drives release, so it never pins
    /// carrier charges behind the drain batch. This gates memory-pressure relief.
    pub(crate) fn has_drainable_resident(&self) -> bool {
        matches!(&*self.mode, Mode::Disk { .. }) && self.buffer.has_drainable_prefix()
    }

    /// Flush the resident filled prefix to disk now, below the drain batch.
    /// Memory-pressure relief is distinct from terminal `finalize`, but uses
    /// the same eager take. Returns the parts freed so the caller releases their
    /// read-ahead occupancy. Not terminal: the transfer keeps issuing after this.
    pub(crate) fn flush_resident(&self) -> Result<u64, std::io::Error> {
        self.drain(DrainMode::Eager)
    }

    /// Couple the drain batch to the read-ahead window: a window narrower than a
    /// segment drains in smaller runs so the block surface never waits for a run it
    /// cannot accumulate. Capped at the segment size (the coalescing target). Called
    /// at construction and whenever the window changes.
    pub(crate) fn sync_drain_batch(&self, window: u64) {
        let batch = std::cmp::min(SEG_SIZE as u64, window.max(1)) as usize;
        self.buffer.set_drain_batch(batch);
    }

    /// Wake the consumer (terminal transition).
    pub(crate) fn notify_consumer(&self) {
        self.notify.notify_one();
    }

    /// Best-effort pre-allocation of disk space.
    pub(crate) fn preallocate(&self, len: u64) {
        if let Mode::Disk { sink, .. } = &*self.mode {
            sink.preallocate(len);
        }
    }
}

/// Write one claimed, contiguous payload run at its translated file position.
///
/// The cursor borrows payloads in place. `complete()` (called by the caller
/// after this returns) frees their owners.
fn write_run(
    sink: &dyn SinkWrite,
    object_range_start: u64,
    sw: &SegmentWrite<ChunkOutput>,
) -> std::io::Result<()> {
    let (object_offset, mut cursor) = DiskWriteCursor::new(sw)?;
    let file_pos = object_offset
        .checked_sub(object_range_start)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "disk-write payload begins before the requested object range",
            )
        })?;
    sink.write_all_at(&mut cursor, file_pos)
}

/// Consumer wrapper carrying the notify handle alongside the buffer consumer.
/// Handed from constructors to `Body::new`.
pub(crate) struct RecvBodyConsumer {
    consumer: RecvBufferConsumer<ChunkOutput>,
    notify: Arc<WakeNotify>,
}

impl std::fmt::Debug for RecvBodyConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvBodyConsumer").finish_non_exhaustive()
    }
}

impl RecvBodyConsumer {
    /// Try to take the next sequential chunk without waiting.
    #[cfg(test)]
    pub(crate) fn try_take_next(&mut self) -> Option<ChunkOutput> {
        self.consumer.poll_next()
    }
}

/// Create a matched producer/consumer pair for download body delivery (stream mode).
///
/// Issuance backpressure is owned by the per-transfer [`ReadAhead`] controller, not
/// by a fixed buffer capacity — the buffer grows as needed.
///
/// [`ReadAhead`]: super::read_ahead::ReadAhead
pub(crate) fn new_recv_body() -> (BodyWriter, RecvBodyConsumer) {
    let (buffer, consumer) = PagedRecvBuffer::new_with_segment_size(SEG_SIZE);
    let notify = Arc::new(WakeNotify::new());
    let writer = BodyWriter {
        buffer,
        notify: notify.clone(),
        mode: Arc::new(Mode::Stream),
    };
    let slot_consumer = RecvBodyConsumer { consumer, notify };
    (writer, slot_consumer)
}

/// Create a producer/consumer pair writing to an arbitrary [`SinkWrite`] target.
/// Positioned writes land at `chunk.offset - object_range_start` in the sink.
///
/// Issuance backpressure is owned by the per-transfer [`ReadAhead`] controller.
///
/// [`ReadAhead`]: super::read_ahead::ReadAhead
fn new_recv_body_with_disk_mode(
    sink: Box<dyn SinkWrite>,
    object_range_start: u64,
) -> (BodyWriter, RecvBodyConsumer) {
    let (buffer, consumer) = PagedRecvBuffer::new_with_segment_size(SEG_SIZE);
    let notify = Arc::new(WakeNotify::new());
    let writer = BodyWriter {
        buffer,
        notify: notify.clone(),
        mode: Arc::new(Mode::Disk {
            sink,
            object_range_start,
        }),
    };
    let slot_consumer = RecvBodyConsumer { consumer, notify };
    (writer, slot_consumer)
}

/// Create a producer/consumer pair with a file sink for download-to-file.
pub(crate) fn new_recv_body_with_sink(
    file: std::fs::File,
    object_range_start: u64,
    owns_file: bool,
) -> (BodyWriter, RecvBodyConsumer) {
    new_recv_body_with_disk_mode(Box::new(FileSink { file, owns_file }), object_range_start)
}

/// Stream of [ChunkOutput] representing an Amazon S3 Object's contents and metadata.
///
/// Wraps potentially multiple streams of binary data into a single coherent stream.
/// The data on this stream is sequenced into the correct order.
pub struct Body {
    consumer: RecvBufferConsumer<ChunkOutput>,
    notify: Arc<WakeNotify>,
    transfer: DownloadTransfer,
}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Body").finish_non_exhaustive()
    }
}

/// Contains body and metadata for each GetObject call made. This will be delivered sequentially
/// in-order.
#[derive(Clone)]
#[non_exhaustive]
pub struct ChunkOutput {
    // TODO(aws-sdk-rust#1159, design) - consider PartialOrd for ChunkResponse and hiding `seq` as internal only detail
    // the seq number
    pub(crate) seq: u64,
    /// Byte offset in the object where this chunk starts.
    pub(crate) offset: u64,
    /// The content associated with this ranged `GetObject` request.
    pub data: SegmentedBytes,
    /// The metadata associated with this particular ranged GetObject request. This contains all the
    /// metadata returned by the S3 GetObject operation.
    pub metadata: ChunkMetadata,
}

impl std::fmt::Debug for ChunkOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkOutput")
            .field("seq", &self.seq)
            .field("offset", &self.offset)
            .field("data_len", &self.data.remaining())
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl Body {
    pub(crate) fn new(consumer: RecvBodyConsumer, transfer: DownloadTransfer) -> Self {
        Self {
            consumer: consumer.consumer,
            notify: consumer.notify,
            transfer,
        }
    }

    /// Close the body, signaling no more chunks will be consumed.
    pub(crate) fn close(&mut self) {
        // No-op: dropping the consumer is sufficient for cleanup.
    }

    /// Pull the next chunk of data off the stream.
    ///
    /// Returns [None] when there is no more data.
    /// Chunks returned from a [Body] are guaranteed to be sequenced
    /// in the right order.
    ///
    /// On failure, returns `Some(Err(...))` with the error kind from the actual failure.
    /// Call `join()` on the handle to get the full error with source chain.
    pub async fn next(&mut self) -> Option<Result<ChunkOutput, crate::error::Error>> {
        loop {
            // Register interest BEFORE checking state (lost-wake safety).
            let notified = self.notify.notified();
            if let Some(chunk) = self.consumer.poll_next() {
                // Delivery freed one part's payload. Release that read-ahead occupancy
                // and wake the issuer under the state lock, so the release is ordered
                // against the gate's park (see `release_stream_occupancy`).
                self.transfer.release_stream_occupancy();
                return Some(Ok(chunk));
            }
            if !self.transfer.ctx().is_active() {
                // Terminal: drain then report.
                if let Some(chunk) = self.consumer.poll_next() {
                    self.transfer.release_stream_occupancy();
                    return Some(Ok(chunk));
                }
                return self.terminal_result();
            }
            notified.await;
        }
    }

    fn terminal_result(&self) -> Option<Result<ChunkOutput, crate::error::Error>> {
        if self.transfer.ctx().is_cancelled() {
            Some(Err(crate::error::from_kind(
                crate::error::ErrorKind::OperationCancelled,
            )("download cancelled")))
        } else if self.transfer.ctx().is_failed() {
            let kind = self
                .transfer
                .ctx()
                .error_kind()
                .unwrap_or(crate::error::ErrorKind::ChildOperationFailed);
            Some(Err(crate::error::from_kind(kind)("transfer failed")))
        } else {
            None // normal completion
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::SegmentedBytes;
    use crate::operation::download::transfer::DownloadTransfer;
    use crate::{error, operation::download::body::ChunkOutput};
    use bytes::{BufMut, Bytes};
    use std::sync::Arc;

    use super::{new_recv_body, Body, BodyWriter};

    fn chunk_resp(seq: u64, data: SegmentedBytes) -> ChunkOutput {
        ChunkOutput {
            seq,
            offset: 0,
            data,
            metadata: Default::default(),
        }
    }

    fn test_body() -> (Body, DownloadTransfer, BodyWriter) {
        use crate::operation::download::DownloadInput;
        use crate::transfer::TransferContext;
        use crate::types::BucketType;

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .with_test_defaults()
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .build(),
        );
        let config = crate::Config::builder().client(s3_client).build();
        let tm = crate::Client::new(config);
        let input = DownloadInput::builder()
            .bucket("test")
            .key("test")
            .build()
            .unwrap();
        let (ctx, _) = TransferContext::new(tm.handle.clone());
        let (writer, consumer) = new_recv_body();
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer.clone());
        (Body::new(consumer, transfer.clone()), transfer, writer)
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_body_next() {
        let (mut body, transfer, writer) = test_body();
        let ctx_clone = transfer.clone();
        tokio::spawn(async move {
            // Claim 3 seqs, fill out of order
            let s0 = writer.claim();
            let s1 = writer.claim();
            let s2 = writer.claim();

            // Fill out of order: 2, 0, 1
            let seq2 = s2.seq();
            s2.fill(chunk_resp(
                seq2,
                SegmentedBytes::from(Bytes::from("chunk 2")),
            ));

            let seq0 = s0.seq();
            s0.fill(chunk_resp(
                seq0,
                SegmentedBytes::from(Bytes::from("chunk 0")),
            ));

            let seq1 = s1.seq();
            s1.fill(chunk_resp(
                seq1,
                SegmentedBytes::from(Bytes::from("chunk 1")),
            ));

            ctx_clone.ctx().set_completed();
        });

        let mut received = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("chunk ok");
            let data = String::from_utf8(chunk.data.into_contiguous().to_vec()).unwrap();
            received.push(data);
        }

        // Delivered in order despite out-of-order fill
        let expected: Vec<String> = [0, 1, 2].iter().map(|i| format!("chunk {i}")).collect();
        assert_eq!(expected, received);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_body_next_on_failed_transfer() {
        let (mut body, transfer, writer) = test_body();

        // Fill one chunk
        let s0 = writer.claim();
        let seq0 = s0.seq();
        s0.fill(chunk_resp(
            seq0,
            SegmentedBytes::from(Bytes::from("chunk 0")),
        ));

        // Fail the transfer
        transfer.ctx().set_failed(error::Error::new(
            error::ErrorKind::ChildOperationFailed,
            "simulated failure",
        ));

        // First call returns the buffered chunk
        let chunk = body.next().await.unwrap().unwrap();
        assert_eq!(chunk.seq, 0);

        // Second call sees failed status, returns error
        let err = body.next().await.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::ChildOperationFailed));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_body_next_on_cancelled_transfer() {
        let (mut body, transfer, _writer) = test_body();

        // Cancel the transfer
        transfer.ctx().set_cancelled();

        // next() should return cancellation error
        let err = body.next().await.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::OperationCancelled));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_body_next_unblocks_on_cancel() {
        let (mut body, transfer, _writer) = test_body();

        // Sync point: consumer signals it's about to wait
        let waiting = Arc::new(tokio::sync::Notify::new());
        let waiting_clone = waiting.clone();

        let cancel_transfer = transfer.clone();
        let consumer = tokio::spawn(async move {
            // Signal we're about to enter next() (which will park on notified)
            waiting_clone.notify_one();
            body.next().await
        });

        // Wait until consumer is about to park
        waiting.notified().await;
        // Yield to let consumer actually reach notified().await
        tokio::task::yield_now().await;

        // Now cancel and wake
        cancel_transfer.ctx().set_cancelled();
        cancel_transfer.writer().notify_consumer();

        let result = consumer.await.unwrap();
        let err = result.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::OperationCancelled));
    }

    // --- Disk driver tests ---

    use super::{
        new_recv_body_with_disk_mode, new_recv_body_with_sink, BodyWriter as Writer,
        DiskWriteCursor, DrainMode, RecvBodyConsumer, Reservation, SinkWrite,
    };
    use bytes::Buf as _;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;

    fn chunk_at(seq: u64, offset: u64, data: &[u8]) -> ChunkOutput {
        ChunkOutput {
            seq,
            offset,
            data: SegmentedBytes::from(Bytes::copy_from_slice(data)),
            metadata: Default::default(),
        }
    }

    #[test]
    fn disk_write_cursor_vectors_and_advances_across_chunk_boundaries() {
        let (writer, _consumer, _sink) = new_recv_body_with_capture(0);
        writer.claim().fill(chunk_at(0, 0, b"ab"));
        writer.claim().fill(chunk_at(1, 2, b"cdef"));
        let write = writer
            .buffer
            .take_drain_run(DrainMode::Eager)
            .expect("two filled payloads");
        {
            let (object_offset, mut cursor) = DiskWriteCursor::new(&write).unwrap();

            let mut slices = [std::io::IoSlice::new(&[]); 4];
            let count = cursor.chunks_vectored(&mut slices);
            assert_eq!(object_offset, 0);
            assert_eq!(count, 2);
            assert_eq!(slices[0].as_ref(), b"ab");
            assert_eq!(slices[1].as_ref(), b"cdef");

            cursor.advance(3);
            assert_eq!(cursor.remaining(), 3);
            assert_eq!(cursor.chunk(), b"def");
        }
        assert_eq!(write.complete(), 2);
    }

    /// In-memory [`SinkWrite`] that records every positioned write, for asserting the
    /// drain orchestration (offset translation, run coalescing, ordering under
    /// concurrency) without a real file. Reconstructs the destination by laying each
    /// write down at its position, so overlapping or misordered writes surface as
    /// wrong bytes.
    #[derive(Debug, Default)]
    struct CaptureSink {
        // pos -> bytes written there (most recent wins, as a real file would).
        writes: StdMutex<BTreeMap<u64, Vec<u8>>>,
    }

    impl CaptureSink {
        /// Materialize the written bytes into a contiguous buffer, panicking on a gap
        /// (an unwritten position below the high-water mark) so a missed write is loud.
        fn assembled(&self) -> Vec<u8> {
            let writes = self.writes.lock().unwrap();
            let mut out = Vec::new();
            for (&pos, bytes) in writes.iter() {
                assert_eq!(pos as usize, out.len(), "gap or overlap at pos {pos}");
                out.extend_from_slice(bytes);
            }
            out
        }

        fn write_count(&self) -> usize {
            self.writes.lock().unwrap().len()
        }
    }

    impl SinkWrite for CaptureSink {
        fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()> {
            let mut bytes = vec![0u8; buf.remaining()];
            buf.copy_to_slice(&mut bytes);
            self.writes.lock().unwrap().insert(pos, bytes);
            Ok(())
        }
    }

    fn new_recv_body_with_capture(
        object_range_start: u64,
    ) -> (Writer, RecvBodyConsumer, Arc<CaptureSink>) {
        let sink = Arc::new(CaptureSink::default());
        // The Mode holds a Box<dyn SinkWrite>; share state via an Arc the test also
        // holds. A thin forwarder lets the Box and the test handle point at one sink.
        #[derive(Debug)]
        struct Shared(Arc<CaptureSink>);
        impl SinkWrite for Shared {
            fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()> {
                self.0.write_all_at(buf, pos)
            }
        }
        let (writer, consumer) =
            new_recv_body_with_disk_mode(Box::new(Shared(sink.clone())), object_range_start);
        (writer, consumer, sink)
    }

    /// `has_drainable_resident` gates memory-pressure draining. It is true only on the
    /// disk surface with a filled run at the cursor: stream mode returns false (the
    /// consumer drives release there, so an eager drain would be a no-op that spins),
    /// and a resident run only exists once a slot at the cursor is filled.
    #[test]
    fn has_drainable_resident_only_on_disk_with_a_filled_run() {
        // Stream mode: even with a filled slot, never drainable-resident.
        let (stream_writer, _consumer) = new_recv_body();
        stream_writer.claim().fill(chunk_at(0, 0, b"streamed"));
        assert!(
            !stream_writer.has_drainable_resident(),
            "stream mode has no drainable-resident run (consumer drives release)"
        );

        // Disk mode: empty → false; filled at the cursor → true.
        let (disk_writer, _consumer, _sink) = new_recv_body_with_capture(0);
        assert!(
            !disk_writer.has_drainable_resident(),
            "disk mode with nothing filled is not drainable-resident"
        );
        disk_writer.claim().fill(chunk_at(0, 0, b"resident"));
        assert!(
            disk_writer.has_drainable_resident(),
            "disk mode with a filled run at the cursor is drainable-resident"
        );
    }

    #[test]
    fn disk_submits_one_contiguous_run_as_one_positioned_write() {
        let (writer, _consumer, sink) = new_recv_body_with_capture(100);
        writer.claim().fill(chunk_at(0, 100, b"ab"));
        writer.claim().fill(chunk_at(1, 102, b"cdef"));

        assert_eq!(writer.finalize().unwrap(), 2);
        assert_eq!(sink.write_count(), 1);
        assert_eq!(sink.assembled(), b"abcdef");
    }

    #[test]
    fn disk_rejects_gaps_and_overlaps_within_a_claimed_run() {
        for second_offset in [1, 3] {
            let (writer, _consumer, sink) = new_recv_body_with_capture(0);
            writer.claim().fill(chunk_at(0, 0, b"ab"));
            writer.claim().fill(chunk_at(1, second_offset, b"cd"));

            let error = writer.finalize().expect_err("offsets are not contiguous");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(sink.write_count(), 0);
        }
    }

    #[test]
    fn disk_rejects_an_empty_claimed_payload() {
        let (writer, _consumer, sink) = new_recv_body_with_capture(0);
        writer.claim().fill(chunk_at(0, 0, b""));

        let error = writer.finalize().expect_err("disk payload is empty");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(sink.write_count(), 0);
    }

    #[test]
    fn disk_writes_full_segment() {
        use super::SEG_SIZE;
        const CHUNK_LEN: usize = 4;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        let mut expected = Vec::with_capacity(SEG_SIZE * CHUNK_LEN);
        for i in 0..SEG_SIZE as u64 {
            let slot = writer.claim();
            let data = [i as u8; CHUNK_LEN];
            let offset = i * CHUNK_LEN as u64;
            expected.extend_from_slice(&data);
            slot.fill(chunk_at(i, offset, &data));
        }

        writer.drain(DrainMode::Batched).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn disk_eof_partial_tail() {
        use super::SEG_SIZE;
        const CHUNK_LEN: usize = 2;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        let partial = SEG_SIZE / 2;
        let mut expected = Vec::with_capacity(partial * CHUNK_LEN);
        for i in 0..partial as u64 {
            let slot = writer.claim();
            let data = [i as u8; CHUNK_LEN];
            let offset = i * CHUNK_LEN as u64;
            expected.extend_from_slice(&data);
            slot.fill(chunk_at(i, offset, &data));
        }

        writer.finalize().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn disk_finalize_drains_full_and_tail() {
        use super::SEG_SIZE;
        const CHUNK_LEN: usize = 2;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        let total = SEG_SIZE + 2;
        let mut expected = Vec::with_capacity(total * CHUNK_LEN);
        for i in 0..total as u64 {
            let slot = writer.claim();
            let data = [i as u8; CHUNK_LEN];
            let offset = i * CHUNK_LEN as u64;
            expected.extend_from_slice(&data);
            slot.fill(chunk_at(i, offset, &data));
        }

        writer.finalize().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn disk_offset_translation() {
        use super::SEG_SIZE;
        const CHUNK_LEN: usize = 2;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 1000, false);

        let n = SEG_SIZE;
        let mut expected = Vec::with_capacity(n * CHUNK_LEN);
        for i in 0..n as u64 {
            let slot = writer.claim();
            let data = [i as u8; CHUNK_LEN];
            let offset = 1000 + i * CHUNK_LEN as u64;
            expected.extend_from_slice(&data);
            slot.fill(chunk_at(i, offset, &data));
        }

        writer.drain(DrainMode::Batched).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn disk_concurrent_fill() {
        use super::SEG_SIZE;
        const CHUNK_LEN: usize = 4;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        let n = SEG_SIZE;
        let mut handles = Vec::new();
        for _ in 0..n {
            let slot = writer.claim();
            handles.push(tokio::spawn(async move {
                tokio::task::yield_now().await;
                let seq = slot.seq();
                let offset = seq * CHUNK_LEN as u64;
                let data = [seq as u8; CHUNK_LEN];
                slot.fill(chunk_at(seq, offset, &data));
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for seq in 0..n as u64 {
            let offset = seq as usize * CHUNK_LEN;
            assert_eq!(
                &contents[offset..offset + CHUNK_LEN],
                &[seq as u8; CHUNK_LEN],
                "mismatch at seq {seq}"
            );
        }
    }

    /// Draining runs on the disk path must report freed parts to release read-ahead
    /// occupancy.
    ///
    /// The issuance gate bounds `issued - released`, where `released` is the sum of the
    /// freed counts `drain` reports. On the disk path the consumer is `drain`, which
    /// writes and frees runs. If draining does not report freed parts, a download larger
    /// than the read-ahead window wedges: the gate latches shut at the window and never
    /// reopens even though the buffer has drained to empty. This drives that path
    /// directly — no network, no large object — and asserts every part is reported freed.
    #[test]
    fn disk_drain_releases_read_ahead_occupancy() {
        use super::SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        // Fill two full segments, draining on the DrainReady edge after each fill — the
        // disk path's steady-state loop. Bounded iteration, so a regression fails fast.
        let total = (2 * SEG_SIZE) as u64;
        let mut freed_total = 0u64;
        for i in 0..total {
            let slot = writer.claim();
            let outcome = slot.fill(chunk_at(i, i, &[i as u8]));
            // Drain on the batch edge, as execute_get_range does.
            if outcome == super::FillOutcome::DrainReady {
                freed_total += writer.drain(DrainMode::Batched).unwrap();
            }
        }
        // A terminal drain flushes any sub-batch tail so the total accounts for every part.
        freed_total += writer.finalize().unwrap();

        // Resident occupancy is `issued - released` = total - freed_total. Every part has
        // been written to disk and its memory freed, so it must be 0 — otherwise the gate
        // would still count drained parts against the window.
        let occupancy = total - freed_total;
        assert_eq!(
            occupancy, 0,
            "all {total} parts drained to disk, but the drains reported only \
             {freed_total} freed; issuance cannot reopen past the window"
        );
    }

    /// Concurrent, out-of-order fills across several segments must drain to the correct
    /// positions: many parts are claimed in order, then filled and drained from many
    /// threads in a shuffled order, each thread mirroring the execute path (fill, then
    /// drain on the `DrainReady` edge). A non-zero `object_range_start` also exercises
    /// offset translation. The capture sink reconstructs the object; any misordered or
    /// misplaced positioned write surfaces as wrong bytes or a gap.
    ///
    /// This is the property the disk path depends on but that an in-order producer
    /// never exercises: parts arrive and drain out of order, yet positioned writes (no
    /// recomputed checksum) must still reassemble the object byte-for-byte.
    #[test]
    fn disk_concurrent_out_of_order_drain_reassembles() {
        use super::SEG_SIZE;

        // Several segments' worth, with a partial final segment, so the run includes
        // full-segment drains, cross-segment coalescing, and a terminal partial tail.
        let parts = 3 * SEG_SIZE + 5;
        let part_len = 64usize;
        let range_start = 4096u64; // non-zero base: exercises offset translation

        // Deterministic per-part bytes, so the assembled object is checkable.
        let part_bytes = |seq: usize| -> Vec<u8> {
            (0..part_len)
                .map(|b| (seq as u8).wrapping_add(b as u8))
                .collect()
        };
        let mut expected = Vec::with_capacity(parts * part_len);
        for seq in 0..parts {
            expected.extend_from_slice(&part_bytes(seq));
        }

        let (writer, _consumer, sink) = new_recv_body_with_capture(range_start);

        // Claim all slots up front (claim is serialized; seq assignment is in order).
        let slots: Vec<_> = (0..parts).map(|_| writer.claim()).collect();

        // A fixed shuffle of fill order (deterministic — no rng): odd indices first,
        // then even, so later segments seal before earlier ones complete.
        let mut order: Vec<usize> = (0..parts).filter(|i| i % 2 == 1).collect();
        order.extend((0..parts).filter(|i| i % 2 == 0));

        // Map seq -> slot, consumed as each thread fills.
        let mut by_seq: std::collections::HashMap<usize, super::BodySlot> =
            slots.into_iter().map(|s| (s.seq() as usize, s)).collect();

        let writer = Arc::new(writer);
        // Sum every drain's freed count across threads, the way the download layer
        // accumulates `released` under its state lock.
        let freed_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let n_threads = 4;
        let chunks: Vec<Vec<usize>> = (0..n_threads)
            .map(|t| order.iter().copied().skip(t).step_by(n_threads).collect())
            .collect();

        std::thread::scope(|scope| {
            for thread_seqs in &chunks {
                // Move this thread's slots in.
                let mut my_slots = Vec::new();
                for &seq in thread_seqs {
                    my_slots.push((seq, by_seq.remove(&seq).expect("slot for seq")));
                }
                let writer = writer.clone();
                let freed_total = freed_total.clone();
                scope.spawn(move || {
                    for (seq, slot) in my_slots {
                        let offset = range_start + (seq as u64) * (part_len as u64);
                        let outcome = slot.fill(chunk_at(seq as u64, offset, &part_bytes(seq)));
                        // Mirror execute_get_range: drain on the batch edge.
                        if outcome == super::FillOutcome::DrainReady {
                            let freed = writer.drain(DrainMode::Batched).unwrap();
                            freed_total.fetch_add(freed, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert!(by_seq.is_empty(), "every slot dispatched to a thread");

        // Terminal drain flushes the partial final segment and any straggler runs.
        freed_total.fetch_add(
            writer.finalize().unwrap(),
            std::sync::atomic::Ordering::Relaxed,
        );

        // Every part written and freed.
        assert_eq!(
            freed_total.load(std::sync::atomic::Ordering::Relaxed),
            parts as u64,
            "all parts drained"
        );
        // The positioned writes reassemble the object exactly.
        assert_eq!(
            sink.assembled(),
            expected,
            "out-of-order concurrent drain must reassemble the object byte-for-byte"
        );
    }

    // --- Admission and payload ownership tests ---
    //
    // Fill closes acquisition authority. The resulting charge follows immutable
    // ownership: to final consumer drop on the stream surface, or through the
    // positioned write on the disk surface.

    use crate::memory::{BufferPool, MemoryBudgetConfig};

    fn test_reservation_pool() -> (BufferPool, usize) {
        let pool = BufferPool::from_capacity(
            MemoryBudgetConfig::Limit(1024 * 1024),
            None,
            crate::config::MemoryDiagnosticsConfig::default(),
        )
        .expect("test pool");
        let carrier_size = pool.carrier_size();
        (pool, carrier_size)
    }

    fn pooled_data(pool: &BufferPool, reservation: &Reservation, data: &[u8]) -> SegmentedBytes {
        let mut buffer = pool
            .acquire(reservation, data.len())
            .expect("reserved pooled buffer");
        buffer.put_slice(data);
        buffer.freeze()
    }

    #[test]
    fn test_stream_fill_closes_reservation_while_payload_retains_charge() {
        let (pool, chunk_bytes) = test_reservation_pool();
        let (writer, mut consumer) = new_recv_body();

        let reservation = pool
            .try_reserve(chunk_bytes)
            .unwrap()
            .expect("has capacity");
        let data = pooled_data(&pool, &reservation, b"hello");
        let mut slot = writer.claim();
        slot.attach_reservation(reservation);
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            chunk_bytes as u64
        );
        slot.fill(chunk_resp(0, data));

        let metrics = pool.metrics();
        assert_eq!(metrics.active_planned_demand_bytes(), 0);
        assert_eq!(metrics.charged_capacity_bytes(), chunk_bytes as u64);
        assert_eq!(metrics.admission_used_bytes(), chunk_bytes as u64);

        let chunk = consumer.try_take_next().expect("chunk delivered");
        assert_eq!(chunk.data.clone().into_contiguous().as_ref(), b"hello");
        assert_eq!(pool.metrics().charged_capacity_bytes(), chunk_bytes as u64);

        drop(chunk);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        assert_eq!(pool.metrics().admission_used_bytes(), 0);
    }

    #[test]
    fn test_stream_charge_holds_until_last_chunk_clone_drops() {
        let (pool, chunk_bytes) = test_reservation_pool();
        let (writer, mut consumer) = new_recv_body();

        let reservation = pool
            .try_reserve(chunk_bytes)
            .unwrap()
            .expect("has capacity");
        let data = pooled_data(&pool, &reservation, b"shared");
        let mut slot = writer.claim();
        slot.attach_reservation(reservation);
        slot.fill(chunk_resp(0, data));

        let chunk = consumer.try_take_next().expect("chunk delivered");
        let clone = chunk.clone();
        assert_eq!(pool.metrics().active_planned_demand_bytes(), 0);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            chunk_bytes as u64,
            "one physical buffer, charged once"
        );

        drop(chunk);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            chunk_bytes as u64,
            "a surviving clone still holds the resident bytes"
        );

        drop(clone);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        assert_eq!(pool.metrics().admission_used_bytes(), 0);
    }

    #[test]
    fn test_disk_payload_charge_returns_on_drain() {
        let (pool, chunk_bytes) = test_reservation_pool();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        let reservation = pool
            .try_reserve(chunk_bytes)
            .unwrap()
            .expect("has capacity");
        let data = pooled_data(&pool, &reservation, b"flushed");
        let mut slot = writer.claim();
        slot.attach_reservation(reservation);
        slot.fill(ChunkOutput {
            seq: 0,
            offset: 0,
            data,
            metadata: Default::default(),
        });

        assert_eq!(pool.metrics().active_planned_demand_bytes(), 0);
        assert_eq!(pool.metrics().charged_capacity_bytes(), chunk_bytes as u64);

        writer.finalize().unwrap();
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        assert_eq!(pool.metrics().admission_used_bytes(), 0);
        assert_eq!(&std::fs::read(&path).unwrap()[..7], b"flushed");
    }
}
