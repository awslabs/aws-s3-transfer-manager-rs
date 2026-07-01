/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use bytes::Buf;

use crate::io::AggregatedBytes;
use crate::runtime::sync::sync::Arc;

use super::chunk_meta::ChunkMetadata;
use super::recv_buffer::{
    FillOutcome, PagedRecvBuffer, RecvBufferConsumer, SegmentWrite, SlotHandle,
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

/// Positioned-write target for download-to-file. Abstracts the file so the drain
/// orchestration (run coalescing, offset translation) can be exercised against an
/// in-memory capture, and so an alternative write strategy (e.g. O_DIRECT/io_uring)
/// can replace the file write without touching the buffer or the drain logic.
///
/// `write_all_at` writes the whole buffer at an absolute file position; `preallocate`
/// is a best-effort size hint. Implementations are shared across the issuer and the
/// drain task, hence `Send + Sync`.
pub(crate) trait SinkWrite: Send + Sync + std::fmt::Debug {
    /// Write the entire buffer at `pos` bytes into the target.
    fn write_all_at(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        pos: u64,
    ) -> std::io::Result<()>;

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
    fn write_all_at(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        pos: u64,
    ) -> std::io::Result<()> {
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
/// in the transfer. Cheap to clone (all state behind Arc).
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
/// consumer. Drop without fill still wakes the consumer (lost-wake safety).
pub(crate) struct BodySlot {
    handle: Option<SlotHandle<ChunkOutput>>,
    buffer: PagedRecvBuffer<ChunkOutput>,
    notify: Arc<WakeNotify>,
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

    /// Publish a completed chunk into this slot. Wakes the stream consumer
    /// and returns the fill outcome (whether a segment was sealed).
    pub(crate) fn fill(mut self, chunk: ChunkOutput) -> FillOutcome {
        let handle = self.handle.take().expect("slot already consumed");
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
        }
    }

    /// Parts whose memory the consumer has freed (stream delivery or disk drain) —
    /// the read-ahead gate's denominator. See [`PagedRecvBuffer::released`].
    pub(crate) fn released(&self) -> u64 {
        self.buffer.released()
    }

    /// Disk mode: drain every batch-ready run to disk. Called on the `DrainReady`
    /// edge from execute tasks. A non-terminal drain claims only runs that reach the
    /// drain batch, coalescing each into one positioned write. Stream mode: no-op.
    pub(crate) fn drain(&self, terminal: bool) -> Result<(), std::io::Error> {
        if let Mode::Disk {
            sink,
            object_range_start,
        } = &*self.mode
        {
            while let Some(sw) = self.buffer.take_drain_run(terminal) {
                write_run(sink.as_ref(), *object_range_start, &sw)?;
                sw.complete();
            }
        }
        Ok(())
    }

    /// Terminal drain: flush every remaining filled run, including a partial final
    /// segment below the drain batch. Called once from `complete()` / `on_terminal()`.
    pub(crate) fn finalize(&self) -> Result<(), std::io::Error> {
        if !matches!(&*self.mode, Mode::Disk { .. }) {
            return Ok(());
        }
        // Report the terminal flush: how many parts this last pass wrote (those left
        // resident below the drain batch) and the total drained over the transfer.
        // Logged once per disk transfer.
        let before = self.released();
        self.drain(true)?;
        let total = self.released();
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            terminal_parts = total - before,
            parts_total = total,
            "terminal drain complete; tail flushed to disk",
        );
        Ok(())
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

/// Gather a claimed run's filled payloads and write them to the sink at the correct
/// positions, coalescing offset-contiguous payloads into one positioned write. A
/// payload at object offset `o` lands at sink position `o - object_range_start`. Reads
/// payloads in place via `Slot::get()`; `complete()` (called by the caller after this
/// returns) frees them.
fn write_run(
    sink: &dyn SinkWrite,
    object_range_start: u64,
    sw: &SegmentWrite<ChunkOutput>,
) -> std::io::Result<()> {
    let mut run_start_file_pos: Option<u64> = None;
    let mut run_end: u64 = 0;
    let mut combined = bytes_utils::SegmentedBuf::<bytes::Bytes>::new();

    for slot in sw.payloads() {
        let Some(chunk) = slot.get() else {
            // Unfilled slot in a partial tail segment — flush accumulated run
            // and reset.
            if let Some(pos) = run_start_file_pos.take() {
                sink.write_all_at(&mut combined, pos)?;
                combined = bytes_utils::SegmentedBuf::new();
            }
            continue;
        };

        let file_pos = chunk.offset - object_range_start;
        let chunk_len = chunk.data.remaining() as u64;

        if let Some(start) = run_start_file_pos {
            if file_pos != run_end {
                // Gap: flush the accumulated run and start a new one.
                sink.write_all_at(&mut combined, start)?;
                combined = bytes_utils::SegmentedBuf::new();
                run_start_file_pos = Some(file_pos);
                run_end = file_pos + chunk_len;
            } else {
                run_end += chunk_len;
            }
        } else {
            run_start_file_pos = Some(file_pos);
            run_end = file_pos + chunk_len;
        }

        // Clone Bytes segments (refcount bump, zero data copy).
        for seg in chunk.data.clone().into_segments() {
            combined.push(seg);
        }
    }

    // Flush any remaining accumulated run.
    if let Some(pos) = run_start_file_pos {
        sink.write_all_at(&mut combined, pos)?;
    }
    Ok(())
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
    /// The content associated with this particular ranged GetObject request.
    pub data: AggregatedBytes,
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
                // Delivery advances `consumed`, freeing read-ahead occupancy; wake
                // the issuer so the reopened window admits more work.
                self.transfer.ctx().try_wake();
                return Some(Ok(chunk));
            }
            if !self.transfer.ctx().is_active() {
                // Terminal: drain then report.
                if let Some(chunk) = self.consumer.poll_next() {
                    self.transfer.ctx().try_wake();
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
    use crate::operation::download::transfer::DownloadTransfer;
    use crate::{error, operation::download::body::ChunkOutput};
    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;
    use std::sync::Arc;

    use super::{new_recv_body, AggregatedBytes, Body, BodyWriter};

    fn chunk_resp(seq: u64, data: AggregatedBytes) -> ChunkOutput {
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
            let mut seg2 = SegmentedBuf::new();
            seg2.push(Bytes::from("chunk 2"));
            let seq2 = s2.seq();
            s2.fill(chunk_resp(seq2, AggregatedBytes(seg2)));

            let mut seg0 = SegmentedBuf::new();
            seg0.push(Bytes::from("chunk 0"));
            let seq0 = s0.seq();
            s0.fill(chunk_resp(seq0, AggregatedBytes(seg0)));

            let mut seg1 = SegmentedBuf::new();
            seg1.push(Bytes::from("chunk 1"));
            let seq1 = s1.seq();
            s1.fill(chunk_resp(seq1, AggregatedBytes(seg1)));

            ctx_clone.ctx().set_completed();
        });

        let mut received = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("chunk ok");
            let data = String::from_utf8(chunk.data.to_vec()).unwrap();
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
        let mut seg = SegmentedBuf::new();
        seg.push(Bytes::from("chunk 0"));
        let seq0 = s0.seq();
        s0.fill(chunk_resp(seq0, AggregatedBytes(seg)));

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
        new_recv_body_with_disk_mode, new_recv_body_with_sink, BodyWriter as Writer, SinkWrite,
        RecvBodyConsumer,
    };
    use bytes::Buf as _;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;

    fn chunk_at(seq: u64, offset: u64, data: &[u8]) -> ChunkOutput {
        let mut seg = SegmentedBuf::new();
        seg.push(Bytes::copy_from_slice(data));
        ChunkOutput {
            seq,
            offset,
            data: AggregatedBytes(seg),
            metadata: Default::default(),
        }
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
    }

    impl SinkWrite for CaptureSink {
        fn write_all_at(
            &self,
            buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
            pos: u64,
        ) -> std::io::Result<()> {
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
            fn write_all_at(
                &self,
                buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
                pos: u64,
            ) -> std::io::Result<()> {
                self.0.write_all_at(buf, pos)
            }
        }
        let (writer, consumer) =
            new_recv_body_with_disk_mode(Box::new(Shared(sink.clone())), object_range_start);
        (writer, consumer, sink)
    }

    #[test]
    fn disk_writes_full_segment() {
        use super::SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        // Fill a full segment's worth of slots
        for i in 0..SEG_SIZE as u64 {
            let slot = writer.claim();
            let offset = i * 100;
            let data = format!("d{i}xx");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
        }

        // The last fill sealed the segment; drain it.
        writer.drain(false).unwrap();

        let contents = std::fs::read(&path).unwrap();
        for i in 0..SEG_SIZE as u64 {
            let offset = (i * 100) as usize;
            let expected = format!("d{i}xx");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {i}"
            );
        }
    }

    #[test]
    fn disk_eof_partial_tail() {
        use super::SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        // Fill fewer than a segment's worth (partial)
        let partial = SEG_SIZE / 2;
        for i in 0..partial as u64 {
            let slot = writer.claim();
            let offset = i * 100;
            let data = format!("p{i}");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
        }

        // finalize drains the partial tail
        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for i in 0..partial as u64 {
            let offset = (i * 100) as usize;
            let expected = format!("p{i}");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {i}"
            );
        }
    }

    #[test]
    fn disk_finalize_drains_full_and_tail() {
        use super::SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 0, false);

        // Fill SEG_SIZE + 2 parts
        let total = SEG_SIZE + 2;
        for i in 0..total as u64 {
            let slot = writer.claim();
            let offset = i * 50;
            let data = format!("x{i}");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
        }

        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for i in 0..total as u64 {
            let offset = (i * 50) as usize;
            let expected = format!("x{i}");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {i}"
            );
        }
    }

    #[test]
    fn disk_offset_translation() {
        use super::SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_recv_body_with_sink(file, 1000, false);

        let n = SEG_SIZE;
        for i in 0..n as u64 {
            let slot = writer.claim();
            let offset = 1000 + i * 100;
            let data = format!("r{i}");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
        }

        writer.drain(false).unwrap();

        let contents = std::fs::read(&path).unwrap();
        for i in 0..n as u64 {
            let file_pos = (i * 100) as usize;
            let expected = format!("r{i}");
            assert_eq!(
                &contents[file_pos..file_pos + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {i}"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn disk_concurrent_fill() {
        use super::SEG_SIZE;
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
                let offset = seq * 100;
                let data = format!("d{seq}");
                slot.fill(chunk_at(seq, offset, data.as_bytes()));
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for seq in 0..n as u64 {
            let offset = (seq * 100) as usize;
            let expected = format!("d{seq}");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {seq}"
            );
        }
    }

    /// Draining runs on the disk path must release read-ahead occupancy.
    ///
    /// The issuance gate bounds `issued - released()`. On the stream path `released`
    /// advances as the consumer pulls; on the disk path the consumer is `drain`, which
    /// writes and frees runs. If draining does not move the gate's occupancy, a
    /// download larger than the read-ahead window wedges: the gate latches shut at the
    /// window and never reopens even though the buffer has drained to empty. This
    /// drives that path directly — no network, no large object — and asserts occupancy
    /// falls as runs drain.
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
        for i in 0..total {
            let slot = writer.claim();
            let outcome = slot.fill(chunk_at(i, i * 100, format!("d{i}").as_bytes()));
            // Drain on the batch edge, as execute_get_range does.
            if outcome == super::FillOutcome::DrainReady {
                writer.drain(false).unwrap();
            }
        }

        // The gate measures resident occupancy as `issued - released()`. Every part
        // has been written to disk and its memory freed, so occupancy must be 0 —
        // otherwise the gate would still count drained parts against the window.
        let released = writer.released();
        let occupancy = total - released;
        assert_eq!(
            occupancy, 0,
            "all {total} parts drained to disk, but the gate still counts {occupancy} \
             as resident (released={released}); issuance cannot reopen past the window"
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
            (0..part_len).map(|b| (seq as u8).wrapping_add(b as u8)).collect()
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
        let n_threads = 4;
        let chunks: Vec<Vec<usize>> = (0..n_threads)
            .map(|t| {
                order
                    .iter()
                    .copied()
                    .skip(t)
                    .step_by(n_threads)
                    .collect()
            })
            .collect();

        std::thread::scope(|scope| {
            for thread_seqs in &chunks {
                // Move this thread's slots in.
                let mut my_slots = Vec::new();
                for &seq in thread_seqs {
                    my_slots.push((seq, by_seq.remove(&seq).expect("slot for seq")));
                }
                let writer = writer.clone();
                scope.spawn(move || {
                    for (seq, slot) in my_slots {
                        let offset = range_start + (seq as u64) * (part_len as u64);
                        let outcome = slot.fill(chunk_at(seq as u64, offset, &part_bytes(seq)));
                        // Mirror execute_get_range: drain on the batch edge.
                        if outcome == super::FillOutcome::DrainReady {
                            writer.drain(false).unwrap();
                        }
                    }
                });
            }
        });
        assert!(by_seq.is_empty(), "every slot dispatched to a thread");

        // Terminal drain flushes the partial final segment and any straggler runs.
        writer.finalize().unwrap();

        // Every part written and freed.
        assert_eq!(writer.released(), parts as u64, "all parts drained");
        // The positioned writes reassemble the object exactly.
        assert_eq!(
            sink.assembled(),
            expected,
            "out-of-order concurrent drain must reassemble the object byte-for-byte"
        );
    }
}
