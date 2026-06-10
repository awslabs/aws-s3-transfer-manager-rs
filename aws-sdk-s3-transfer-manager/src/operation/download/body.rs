/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::runtime::sync::cell::UnsafeCell;
use crate::runtime::sync::sync::atomic::{
    AtomicBool, AtomicU64, AtomicU8, Ordering as AtomicOrdering,
};
use crate::runtime::sync::sync::Arc;

use bytes::Buf;

use crate::io::AggregatedBytes;

use super::chunk_meta::ChunkMetadata;
use super::transfer::DownloadTransfer;

/// Default slot buffer capacity for download body delivery.
pub(crate) const DEFAULT_BODY_SLOT_CAPACITY: usize = 512;

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

/// Slot has no data and is available for reuse.
const SLOT_EMPTY: u8 = 0;
/// Slot contains a completed chunk awaiting consumption or flush.
const SLOT_FILLED: u8 = 1;
/// Data written to disk, awaiting consumed advancement. Sink path only.
const SLOT_FLUSHED: u8 = 2;

/// Number of fills between automatic flush attempts in the sink path.
const FLUSH_INTERVAL: u64 = 1;

/// File sink for download-to-file writes.
///
/// When registered on a `SlotBuffer`, filled slots are batched and
/// flushed to disk via positioned writes instead of being consumed
/// through `Body::next()`.
struct Sink {
    file: std::fs::File,
    /// Start of the S3 byte range for this transfer.
    /// 0 for full object downloads, user's range start for ranged gets.
    /// Each chunk's file position is `chunk.offset - object_range_start`.
    object_range_start: u64,
    /// Whether the transfer manager created this file (vs caller-provided).
    /// Controls whether preallocate is allowed.
    owns_file: bool,
    /// CAS lock for flush exclusion — only one thread flushes at a time.
    flushing: AtomicBool,
    /// Counts fills; flush triggered every `FLUSH_INTERVAL` fills.
    fill_count: AtomicU64,
    /// DIAGNOSTIC: highest file offset written so far (one past the last byte).
    /// Used to flag flushes that write *ahead* of already-written data, which on
    /// NTFS forces a synchronous zero-fill of the gap. Remove with the
    /// flush-timing diagnostics.
    written_high_water: AtomicU64,
}

impl std::fmt::Debug for Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink").finish_non_exhaustive()
    }
}

/// Fixed-size ring buffer of slots for delivering download chunks.
///
/// Slots are indexed by `seq % capacity`. Each slot transitions through
/// states: `Empty` → `Filled` (→ `Flushed` in sink path). The invariants
/// that make `UnsafeCell` access safe are:
///
/// 1. **Exclusive producer access**: each sequence number is claimed exactly
///    once via `claimed` CAS. The producer that claimed `seq` is the only
///    writer to `slots[seq % capacity]`.
/// 2. **State-gated consumer access**: the consumer only reads a slot's data
///    after observing `FILLED` via acquire load. The release store in `fill()`
///    ensures the data write is visible.
/// 3. **No overlap between producer and consumer**: the seq window invariant
///    (`claimed - consumed < capacity`) guarantees the producer never writes
///    to a slot the consumer is currently reading.
/// 4. **Flush exclusion**: in the sink path, the `flushing` CAS lock ensures
///    only one thread reads FILLED slot data at a time.
struct SlotBuffer {
    slots: Box<[Slot]>,
    capacity: u64,
    /// Next seq to be consumed by the consumer. Only advanced by the consumer.
    consumed: AtomicU64,
    /// Next seq to be claimed by a producer. Advanced via CAS in `try_claim`.
    claimed: AtomicU64,
    /// Wakes the consumer when a producer fills a slot.
    notify: WakeNotify,
    /// Optional file sink. When present, filled slots are batched and
    /// flushed to disk periodically instead of being consumed through
    /// `Body::next()`.
    sink: Option<Sink>,
}

/// A single slot in the ring buffer.
///
/// Holds an optional `ChunkOutput` behind `UnsafeCell`. Access is synchronized
/// through the atomic `state` field: producers write data then store `FILLED`,
/// the consumer reads data only after loading `FILLED`.
struct Slot {
    state: AtomicU8,
    data: UnsafeCell<Option<ChunkOutput>>,
}

// Safety: see SlotBuffer doc comment for the full invariant chain.
// In short: producers and consumer never access the same slot concurrently.
// Producers have exclusive write access (unique seq via CAS), the consumer
// has exclusive read access (sequential, state-gated), and the seq window
// prevents overlap.
unsafe impl Sync for SlotBuffer {}

impl SlotBuffer {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "slot buffer capacity must be > 0");
        let slots: Vec<Slot> = (0..capacity)
            .map(|_| Slot {
                state: AtomicU8::new(SLOT_EMPTY),
                data: UnsafeCell::new(None),
            })
            .collect();
        Self {
            slots: slots.into_boxed_slice(),
            capacity: capacity as u64,
            consumed: AtomicU64::new(0),
            claimed: AtomicU64::new(0),
            notify: WakeNotify::new(),
            sink: None,
        }
    }

    /// Write a completed chunk into the slot for `seq`.
    ///
    /// Pure storage — no I/O, no counting, no flushing. In the sink path,
    /// the caller is responsible for calling `try_flush()` afterward.
    ///
    /// # Requirements
    /// - `seq` must have been claimed via the `claimed` CAS (i.e. through
    ///   `BodyWriter::try_claim`). Filling an unclaimed seq is undefined behavior.
    /// - Each `seq` must be filled exactly once. Double-filling is undefined behavior.
    fn fill(&self, seq: u64, chunk: ChunkOutput) {
        let idx = (seq % self.capacity) as usize;
        // Safety: seq window guarantees exclusive access to this slot index.
        self.slots[idx]
            .data
            .with_mut(|ptr| unsafe { *ptr = Some(chunk) });
        self.slots[idx]
            .state
            .store(SLOT_FILLED, AtomicOrdering::Release);

        if self.sink.is_none() {
            self.notify.notify_one();
        }
    }

    /// Interval-gated batched flush of FILLED slots to disk.
    ///
    /// Increments the fill counter and only performs I/O every `FLUSH_INTERVAL`
    /// fills. Returns the first write error encountered, or `Ok(())`.
    fn try_flush(&self) -> Result<(), std::io::Error> {
        let sink = match &self.sink {
            Some(s) => s,
            None => return Ok(()),
        };

        let count = sink.fill_count.fetch_add(1, AtomicOrdering::Relaxed);
        #[allow(clippy::modulo_one)] // FLUSH_INTERVAL is a tunable constant
        if (count + 1) % FLUSH_INTERVAL != 0 {
            return Ok(());
        }

        self.flush_all()
    }

    /// Flush all FILLED slots to disk unconditionally.
    ///
    /// Bypasses the interval counter. Used by `finalize()` for terminal drain
    /// and by `try_flush()` when the interval fires.
    fn flush_all(&self) -> Result<(), std::io::Error> {
        let sink = match &self.sink {
            Some(s) => s,
            None => return Ok(()),
        };

        // Only one thread flushes at a time
        if sink
            .flushing
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let result = self.flush_locked(sink);

        sink.flushing.store(false, AtomicOrdering::Release);
        result
    }

    /// Core flush logic. Caller must hold the `flushing` CAS lock.
    fn flush_locked(&self, sink: &Sink) -> Result<(), std::io::Error> {
        let consumed = self.consumed.load(AtomicOrdering::Acquire);
        let claimed = self.claimed.load(AtomicOrdering::Acquire);

        // Phase 1: Write FILLED slots to disk, coalescing contiguous runs
        let mut first_err: Option<std::io::Error> = None;
        let mut i = consumed;
        while i < claimed {
            let idx = (i % self.capacity) as usize;
            if self.slots[idx].state.load(AtomicOrdering::Acquire) != SLOT_FILLED {
                i += 1;
                continue;
            }

            // Start a new run with the first FILLED slot
            // Safety: state is FILLED and flush holds exclusive consumer access via CAS lock.
            let first_chunk = self.slots[idx]
                .data
                .with_mut(|ptr| unsafe { (*ptr).take().unwrap() });
            let file_pos = first_chunk.offset - sink.object_range_start;
            let mut run_end_offset = file_pos + first_chunk.data.remaining() as u64;
            let mut combined = bytes_utils::SegmentedBuf::new();
            for seg in first_chunk.data.0.into_inner() {
                combined.push(seg);
            }
            self.slots[idx]
                .state
                .store(SLOT_FLUSHED, AtomicOrdering::Release);
            i += 1;

            // Extend with contiguous FILLED neighbors whose file offsets are adjacent
            while i < claimed {
                let jdx = (i % self.capacity) as usize;
                if self.slots[jdx].state.load(AtomicOrdering::Acquire) != SLOT_FILLED {
                    break;
                }
                // Safety: state is FILLED and flush holds exclusive consumer access via CAS lock.
                let chunk = self.slots[jdx]
                    .data
                    .with_mut(|ptr| unsafe { (*ptr).take().unwrap() });
                let chunk_file_pos = chunk.offset - sink.object_range_start;
                if chunk_file_pos != run_end_offset {
                    // Not file-contiguous — put it back for the next iteration
                    // Safety: same CAS lock exclusion; no other thread accesses this slot.
                    self.slots[jdx]
                        .data
                        .with_mut(|ptr| unsafe { *ptr = Some(chunk) });
                    break;
                }
                run_end_offset += chunk.data.remaining() as u64;
                for seg in chunk.data.0.into_inner() {
                    combined.push(seg);
                }
                self.slots[jdx]
                    .state
                    .store(SLOT_FLUSHED, AtomicOrdering::Release);
                i += 1;
            }

            // DIAGNOSTIC: time each positioned write and flag writes that land
            // ahead of the highest offset written so far. On NTFS a write past
            // the file's valid-data-length forces a synchronous zero-fill of the
            // gap, which would block this (managed) thread's reactor. Remove with
            // the rest of the flush-timing diagnostics once the Windows
            // large-download stall is understood.
            let write_len = combined.remaining() as u64;
            let prev_high = sink.written_high_water.load(AtomicOrdering::Relaxed);
            let ahead_by = file_pos.saturating_sub(prev_high);
            let write_start = std::time::Instant::now();
            if let Err(e) = crate::io::fs::write_all_at(&sink.file, &mut combined, file_pos) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            let write_elapsed = write_start.elapsed();
            let new_high = file_pos + write_len;
            sink.written_high_water
                .fetch_max(new_high, AtomicOrdering::Relaxed);
            tracing::debug!(
                target: "download::sink",
                file_pos,
                write_len,
                ahead_by,
                elapsed_ms = write_elapsed.as_millis() as u64,
                "sink flush write"
            );
        }

        // Phase 2: Advance consumed past leading FLUSHED slots
        let mut seq = consumed;
        loop {
            let idx = (seq % self.capacity) as usize;
            if self.slots[idx].state.load(AtomicOrdering::Acquire) != SLOT_FLUSHED {
                break;
            }
            self.slots[idx]
                .state
                .store(SLOT_EMPTY, AtomicOrdering::Release);
            seq += 1;
        }
        if seq > consumed {
            self.consumed.store(seq, AtomicOrdering::Release);
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Try to take the next sequential chunk. Returns `None` if not ready.
    ///
    /// Must only be called by a single consumer. Concurrent calls from
    /// multiple threads are not safe.
    fn try_take_next(&self) -> Option<ChunkOutput> {
        let seq = self.consumed.load(AtomicOrdering::Acquire);
        let idx = (seq % self.capacity) as usize;
        if self.slots[idx].state.load(AtomicOrdering::Acquire) != SLOT_FILLED {
            return None;
        }
        // Safety: state is FILLED and only one consumer calls this.
        let chunk = self.slots[idx]
            .data
            .with_mut(|ptr| unsafe { (*ptr).take() });
        self.slots[idx]
            .state
            .store(SLOT_EMPTY, AtomicOrdering::Release);
        self.consumed.fetch_add(1, AtomicOrdering::Release);
        chunk
    }
}

impl std::fmt::Debug for SlotBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotBuffer")
            .field("capacity", &self.capacity)
            .field("consumed", &self.consumed.load(AtomicOrdering::Relaxed))
            .field("claimed", &self.claimed.load(AtomicOrdering::Relaxed))
            .finish()
    }
}

/// Producer side of the slot buffer. Held by the download transfer.
///
/// Provides seq window claiming and slot filling. The seq window
/// and slot buffer share the same capacity, enforced by construction.
#[derive(Debug, Clone)]
pub(crate) struct BodyWriter {
    buffer: Arc<SlotBuffer>,
}

/// A claimed slot in the body buffer. Produced by [`BodyWriter::try_claim`],
/// consumed by [`fill`](Self::fill). Cannot be constructed outside this module,
/// preventing writes to unclaimed slots. Consuming `self` on fill prevents
/// double-writes.
#[derive(Debug)]
pub(crate) struct BodySlot {
    seq: u64,
    buffer: Arc<SlotBuffer>,
}

impl BodySlot {
    /// The sequence number for this slot.
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// Write a completed chunk into this slot, consuming the claim.
    pub(crate) fn fill(self, chunk: ChunkOutput) {
        self.buffer.fill(self.seq, chunk);
    }
}

impl Drop for BodySlot {
    fn drop(&mut self) {
        self.buffer.notify.notify_one();
    }
}

impl BodyWriter {
    pub(crate) fn has_sink(&self) -> bool {
        self.buffer.sink.is_some()
    }

    /// Try to claim the next slot for work generation.
    ///
    /// Returns `None` if the seq window is exhausted
    /// (`claimed - consumed >= capacity`).
    pub(crate) fn try_claim(&self) -> Option<BodySlot> {
        loop {
            let consumed = self.buffer.consumed.load(AtomicOrdering::Acquire);
            let claimed = self.buffer.claimed.load(AtomicOrdering::Acquire);
            if claimed >= consumed + self.buffer.capacity {
                return None;
            }
            if self
                .buffer
                .claimed
                .compare_exchange_weak(
                    claimed,
                    claimed + 1,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Some(BodySlot {
                    seq: claimed,
                    buffer: self.buffer.clone(),
                });
            }
        }
    }

    /// Wake the consumer so it can observe a state change (e.g. terminal).
    pub(crate) fn notify_consumer(&self) {
        self.buffer.notify.notify_one();
    }

    /// Attempt an interval-gated flush of filled slots to disk.
    ///
    /// Delegates to the slot buffer's `try_flush`, which increments the fill
    /// counter and only performs I/O every `FLUSH_INTERVAL` fills.
    pub(crate) fn try_flush(&self) -> Result<(), std::io::Error> {
        self.buffer.try_flush()
    }

    /// Finalize the writer after the transfer reaches a terminal state.
    ///
    /// Flushes all remaining filled slots to disk when a sink is present.
    /// Must be called before `notify_consumer` on terminal transitions —
    /// no more fills will arrive after this point.
    pub(crate) fn finalize(&self) -> Result<(), std::io::Error> {
        if self.buffer.sink.is_none() {
            return Ok(());
        }
        loop {
            self.buffer.flush_all()?;
            let consumed = self.buffer.consumed.load(AtomicOrdering::Acquire);
            let claimed = self.buffer.claimed.load(AtomicOrdering::Acquire);
            let any_filled = (consumed..claimed).any(|seq| {
                let idx = (seq % self.buffer.capacity) as usize;
                self.buffer.slots[idx].state.load(AtomicOrdering::Acquire) == SLOT_FILLED
            });
            if !any_filled {
                break;
            }
        }
        Ok(())
    }

    /// Best-effort pre-allocation of disk space for the download file.
    // TODO(vnext): should ENOSPC here fail the transfer immediately instead of
    // continuing? Failing fast avoids wasting bandwidth downloading data we can't
    // store. EOPNOTSUPP/ENOTSUP should remain non-fatal (filesystem doesn't support it).
    pub(crate) fn preallocate(&self, len: u64) {
        // DIAGNOSTIC: allow disabling preallocation to test whether a
        // preallocated (set_len) file is the cause of the Windows large-download
        // stall (NTFS zero-fills writes past valid-data-length). Remove with the
        // flush-timing diagnostics.
        if std::env::var_os("S3TM_DIAG_SKIP_PREALLOCATE").is_some() {
            tracing::debug!(target: "download::sink", "preallocate skipped (diagnostic)");
            return;
        }
        if let Some(sink) = &self.buffer.sink {
            if sink.owns_file {
                if let Err(e) = crate::io::fs::preallocate(&sink.file, len) {
                    tracing::warn!(error = %e, "failed to preallocate file space");
                }
            }
        }
    }
}

/// Consumer side of the slot buffer. Reads chunks sequentially.
#[derive(Debug)]
pub(crate) struct SlotBodyConsumer {
    buffer: Arc<SlotBuffer>,
}

impl SlotBodyConsumer {
    /// Pull the next sequential chunk. Waits if not yet available.
    ///
    /// Returns `None` when the transfer is terminal and all filled slots are drained.
    /// `is_terminal` should return `true` when the transfer has reached a terminal
    /// state (success, failure, or cancellation). The caller must ensure
    /// [`BodyWriter::notify_consumer`] is called when the transfer becomes terminal,
    /// otherwise the consumer may wait indefinitely.
    pub(crate) async fn next(&self, is_terminal: impl Fn() -> bool) -> Option<ChunkOutput> {
        loop {
            // Register interest before checking state
            let notified = self.buffer.notify.notified();
            if let Some(chunk) = self.buffer.try_take_next() {
                return Some(chunk);
            }
            if is_terminal() {
                return self.buffer.try_take_next();
            }
            notified.await;
        }
    }

    /// Try to take the next sequential chunk without waiting.
    /// Returns `None` if the next slot is not yet filled.
    #[cfg(test)]
    pub(crate) fn try_take_next(&self) -> Option<ChunkOutput> {
        self.buffer.try_take_next()
    }
}

/// Create a matched producer/consumer pair for download body delivery.
///
/// The capacity determines both the slot buffer size and the seq window gap.
pub(crate) fn new_slot_body(capacity: usize) -> (BodyWriter, SlotBodyConsumer) {
    let buffer = Arc::new(SlotBuffer::new(capacity));
    (
        BodyWriter {
            buffer: buffer.clone(),
        },
        SlotBodyConsumer { buffer },
    )
}

/// Create a producer/consumer pair with a file sink for download-to-file.
///
/// When a sink is registered, filled slots are batched and flushed to disk
/// periodically (every `FLUSH_INTERVAL` fills) and at transfer completion.
pub(crate) fn new_slot_body_with_sink(
    capacity: usize,
    file: std::fs::File,
    object_range_start: u64,
    owns_file: bool,
) -> (BodyWriter, SlotBodyConsumer) {
    let mut buffer = SlotBuffer::new(capacity);
    buffer.sink = Some(Sink {
        file,
        object_range_start,
        owns_file,
        flushing: AtomicBool::new(false),
        fill_count: AtomicU64::new(0),
        written_high_water: AtomicU64::new(0),
    });
    let buffer = Arc::new(buffer);
    (
        BodyWriter {
            buffer: buffer.clone(),
        },
        SlotBodyConsumer { buffer },
    )
}

/// Stream of [ChunkOutput] representing an Amazon S3 Object's contents and metadata.
///
/// Wraps potentially multiple streams of binary data into a single coherent stream.
/// The data on this stream is sequenced into the correct order.
#[derive(Debug)]
pub struct Body {
    consumer: SlotBodyConsumer,
    transfer: DownloadTransfer,
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
    pub(crate) fn new(consumer: SlotBodyConsumer, transfer: DownloadTransfer) -> Self {
        Self { consumer, transfer }
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
        let transfer = &self.transfer;
        let is_terminal = || !transfer.ctx().is_active();

        match self.consumer.next(is_terminal).await {
            Some(chunk) => {
                // Wake the transfer so it can generate more work now that a slot freed up
                transfer.ctx().try_wake();
                Some(Ok(chunk))
            }
            None => {
                if transfer.ctx().is_cancelled() {
                    Some(Err(crate::error::from_kind(
                        crate::error::ErrorKind::OperationCancelled,
                    )("download cancelled")))
                } else if transfer.ctx().is_failed() {
                    let kind = transfer
                        .ctx()
                        .error_kind()
                        .unwrap_or(crate::error::ErrorKind::ChildOperationFailed);
                    Some(Err(crate::error::from_kind(kind)("transfer failed")))
                } else {
                    None // normal completion
                }
            }
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

    use super::{new_slot_body, AggregatedBytes, Body, BodyWriter};

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
        let (writer, consumer) = new_slot_body(16);
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
            let s0 = writer.try_claim().unwrap();
            let s1 = writer.try_claim().unwrap();
            let s2 = writer.try_claim().unwrap();

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
        let s0 = writer.try_claim().unwrap();
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

    // --- Slot buffer tests ---

    #[test]
    fn test_slot_buffer_fill_and_take() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();
        assert_eq!(s0.seq(), 0);
        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));

        let taken = consumer.try_take_next();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().seq, 0);

        assert!(consumer.try_take_next().is_none());
    }

    #[test]
    fn test_slot_buffer_out_of_order_fill() {
        let (writer, consumer) = new_slot_body(4);

        let seq0 = writer.try_claim().unwrap();
        let seq1 = writer.try_claim().unwrap();
        let seq2 = writer.try_claim().unwrap();

        // Fill out of order: 2, 0, 1
        seq2.fill(chunk_resp(2, AggregatedBytes(SegmentedBuf::new())));
        seq0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        seq1.fill(chunk_resp(1, AggregatedBytes(SegmentedBuf::new())));

        // Consumer reads in order
        assert_eq!(consumer.try_take_next().unwrap().seq, 0);
        assert_eq!(consumer.try_take_next().unwrap().seq, 1);
        assert_eq!(consumer.try_take_next().unwrap().seq, 2);
        assert!(consumer.try_take_next().is_none());
    }

    #[test]
    fn test_slot_buffer_seq_window_exhaustion() {
        let (writer, _consumer) = new_slot_body(3);

        assert_eq!(writer.try_claim().unwrap().seq(), 0);
        assert_eq!(writer.try_claim().unwrap().seq(), 1);
        assert_eq!(writer.try_claim().unwrap().seq(), 2);
        assert!(writer.try_claim().is_none());
    }

    #[test]
    fn test_slot_buffer_consume_opens_window() {
        let (writer, consumer) = new_slot_body(2);

        let s0 = writer.try_claim().unwrap();
        let s1 = writer.try_claim().unwrap();
        assert!(writer.try_claim().is_none());

        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        s1.fill(chunk_resp(1, AggregatedBytes(SegmentedBuf::new())));
        consumer.try_take_next(); // consume 0

        // Window opens for seq 2
        assert_eq!(writer.try_claim().unwrap().seq(), 2);
    }

    #[test]
    fn test_slot_buffer_wraps_around() {
        let (writer, consumer) = new_slot_body(2);

        let s0 = writer.try_claim().unwrap();
        let s1 = writer.try_claim().unwrap();
        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        s1.fill(chunk_resp(1, AggregatedBytes(SegmentedBuf::new())));

        assert_eq!(consumer.try_take_next().unwrap().seq, 0);
        assert_eq!(consumer.try_take_next().unwrap().seq, 1);

        // Wraps around to slot indices 0, 1
        let s2 = writer.try_claim().unwrap();
        let s3 = writer.try_claim().unwrap();
        s2.fill(chunk_resp(2, AggregatedBytes(SegmentedBuf::new())));
        s3.fill(chunk_resp(3, AggregatedBytes(SegmentedBuf::new())));

        assert_eq!(consumer.try_take_next().unwrap().seq, 2);
        assert_eq!(consumer.try_take_next().unwrap().seq, 3);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_slot_body_consumer_next() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();
        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));

        let chunk = consumer.next(|| false).await;
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().seq, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_slot_body_consumer_returns_none_when_complete() {
        let (_writer, consumer) = new_slot_body(4);

        let chunk = consumer.next(|| true).await;
        assert!(chunk.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_slot_body_consumer_waits_for_producer() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        });

        let chunk = consumer.next(|| false).await;
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().seq, 0);
    }

    #[test]
    fn test_slot_buffer_batch_drain() {
        let (writer, consumer) = new_slot_body(8);

        for i in 0..5u64 {
            let slot = writer.try_claim().unwrap();
            slot.fill(chunk_resp(i, AggregatedBytes(SegmentedBuf::new())));
        }

        let mut batch = Vec::new();
        while let Some(chunk) = consumer.try_take_next() {
            batch.push(chunk);
        }
        assert_eq!(batch.len(), 5);
        for (i, chunk) in batch.iter().enumerate() {
            assert_eq!(chunk.seq, i as u64);
        }
    }

    #[test]
    fn test_slot_buffer_capacity_one() {
        let (writer, consumer) = new_slot_body(1);

        let s0 = writer.try_claim().unwrap();
        assert!(writer.try_claim().is_none()); // only 1 slot

        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        assert_eq!(consumer.try_take_next().unwrap().seq, 0);

        // Slot freed, can claim again
        let s1 = writer.try_claim().unwrap();
        assert_eq!(s1.seq(), 1);
        s1.fill(chunk_resp(1, AggregatedBytes(SegmentedBuf::new())));
        assert_eq!(consumer.try_take_next().unwrap().seq, 1);
    }

    #[test]
    fn test_slot_buffer_fill_preserves_data() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();
        let data = Bytes::from("hello world");
        let mut seg = SegmentedBuf::new();
        seg.push(data);
        s0.fill(chunk_resp(0, AggregatedBytes(seg)));

        let chunk = consumer.try_take_next().unwrap();
        assert_eq!(chunk.seq, 0);
        assert_eq!(chunk.data.to_vec(), b"hello world");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_slot_buffer_concurrent_producers() {
        let (writer, consumer) = new_slot_body(64);

        // Claim 32 slots
        let slots: Vec<_> = (0..32).map(|_| writer.try_claim().unwrap()).collect();

        // Spawn producers that fill concurrently
        let mut handles = Vec::new();
        for slot in slots {
            handles.push(tokio::spawn(async move {
                // Small jitter to interleave
                tokio::task::yield_now().await;
                let seq = slot.seq();
                let mut seg = SegmentedBuf::new();
                seg.push(Bytes::from(format!("chunk-{seq}")));
                slot.fill(chunk_resp(seq, AggregatedBytes(seg)));
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Consumer reads all 32 in order
        for expected_seq in 0..32u64 {
            let chunk = consumer.next(|| false).await.unwrap();
            assert_eq!(chunk.seq, expected_seq);
            assert_eq!(
                chunk.data.to_vec(),
                format!("chunk-{expected_seq}").as_bytes()
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_slot_body_consumer_completion_race() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();
        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));

        // is_complete returns true, but there's a filled slot
        let chunk = consumer.next(|| true).await;
        assert!(
            chunk.is_some(),
            "should drain filled slot even when complete"
        );
        assert_eq!(chunk.unwrap().seq, 0);

        // Now truly empty and complete
        let chunk = consumer.next(|| true).await;
        assert!(chunk.is_none());
    }

    // --- File sink tests ---

    use super::new_slot_body_with_sink;

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

    #[test]
    fn test_sink_batched_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        // Capacity 16 so all 8 slots fit; FLUSH_INTERVAL=8 triggers flush on 8th try_flush
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 0, false);

        for i in 0..8u64 {
            let slot = writer.try_claim().unwrap();
            let offset = i * 100;
            let data = format!("d{i}xx");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
            writer.try_flush().unwrap();
        }

        // 8th try_flush triggers flush — all data on disk
        let contents = std::fs::read(&path).unwrap();
        for i in 0..8u64 {
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
    fn test_sink_out_of_order_fill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 0, false);

        // Claim 8 slots, fill out of order
        let mut slots: Vec<Option<super::BodySlot>> =
            (0..8).map(|_| Some(writer.try_claim().unwrap())).collect();

        for &i in &[7usize, 3, 0, 5, 2, 6, 1, 4] {
            let slot = slots[i].take().unwrap();
            let offset = (i as u64) * 100;
            let data = format!("s{i}");
            slot.fill(chunk_at(i as u64, offset, data.as_bytes()));
            writer.try_flush().unwrap();
        }

        // 8th try_flush triggers flush; all 8 are FILLED so the entire run is contiguous
        let contents = std::fs::read(&path).unwrap();
        for i in 0..8u64 {
            let offset = (i * 100) as usize;
            let expected = format!("s{i}");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {i}"
            );
        }
    }

    #[test]
    fn test_sink_advances_consumed_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 0, false);

        // Fill 8 slots and call try_flush after each to trigger a flush on the 8th
        for i in 0..8u64 {
            let slot = writer.try_claim().unwrap();
            slot.fill(chunk_at(i, i * 10, b"xx"));
            writer.try_flush().unwrap();
        }

        // After flush, consumed advanced past all 8 flushed slots — new claims work
        let s8 = writer.try_claim().unwrap();
        assert_eq!(s8.seq(), 8);
    }

    #[test]
    fn test_sink_terminal_flush_via_notify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 0, false);

        // Fill fewer than FLUSH_INTERVAL slots — no automatic flush
        let s0 = writer.try_claim().unwrap();
        let s1 = writer.try_claim().unwrap();
        s0.fill(chunk_at(0, 0, b"xxxx"));
        s1.fill(chunk_at(1, 100, b"yyyy"));

        // Data not yet on disk (no flush triggered)
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty() || contents.iter().all(|&b| b == 0));

        // Terminal flush via finalize writes remaining data
        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 104);
        assert_eq!(&contents[0..4], b"xxxx");
        assert_eq!(&contents[100..104], b"yyyy");
    }

    #[test]
    fn test_sink_object_range_offset_translation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 1000, false);

        // Fill 8 slots and call try_flush after each, with object_range_start=1000
        for i in 0..8u64 {
            let slot = writer.try_claim().unwrap();
            let offset = 1000 + i * 100;
            let data = format!("r{i}");
            slot.fill(chunk_at(i, offset, data.as_bytes()));
            writer.try_flush().unwrap();
        }

        let contents = std::fs::read(&path).unwrap();
        // File positions are chunk.offset - object_range_start
        for i in 0..8u64 {
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
    async fn test_sink_concurrent_fill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) = new_slot_body_with_sink(16, file, 0, false);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let slot = writer.try_claim().unwrap();
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

        // Terminal flush to ensure all data is on disk
        writer.finalize().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for seq in 0..8u64 {
            let offset = (seq * 100) as usize;
            let expected = format!("d{seq}");
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at seq {seq}"
            );
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::sync::thread;

    fn dummy_chunk(seq: u64) -> ChunkOutput {
        ChunkOutput {
            seq,
            offset: seq * 1024,
            data: AggregatedBytes(bytes_utils::SegmentedBuf::new()),
            metadata: Default::default(),
        }
    }

    #[test]
    fn two_producers_claim_unique_seqs() {
        loom::model(|| {
            let (writer, _consumer) = new_slot_body(4);
            let writer2 = writer.clone();

            let h = thread::spawn(move || {
                if let Some(slot) = writer2.try_claim() {
                    let seq = slot.seq();
                    slot.fill(dummy_chunk(seq));
                    seq
                } else {
                    u64::MAX
                }
            });

            let seq1 = if let Some(slot) = writer.try_claim() {
                let seq = slot.seq();
                slot.fill(dummy_chunk(seq));
                seq
            } else {
                u64::MAX
            };

            let seq2 = h.join().unwrap();

            // Both should succeed and get different seqs
            assert_ne!(seq1, seq2);
            assert!(seq1 <= 1);
            assert!(seq2 <= 1);
        });
    }

    #[test]
    fn fill_then_take() {
        loom::model(|| {
            let (writer, consumer) = new_slot_body(4);

            let h = thread::spawn(move || {
                let slot = writer.try_claim().unwrap();
                slot.fill(dummy_chunk(0));
            });

            h.join().unwrap();

            // Consumer should see the filled chunk
            let chunk = consumer.try_take_next().unwrap();
            assert_eq!(chunk.seq, 0);
        });
    }

    #[test]
    fn window_pressure() {
        // capacity=1, so after one claim the window is full
        loom::model(|| {
            let (writer, _consumer) = new_slot_body(1);

            let slot = writer.try_claim().unwrap();
            // Window full — next claim should fail
            assert!(writer.try_claim().is_none());

            slot.fill(dummy_chunk(0));
            // Still full until consumed
            assert!(writer.try_claim().is_none());
        });
    }

    #[test]
    fn concurrent_claim_fill_take() {
        loom::model(|| {
            let (writer, consumer) = new_slot_body(2);
            let writer2 = writer.clone();

            // Producer 1
            let h = thread::spawn(move || {
                if let Some(slot) = writer2.try_claim() {
                    let seq = slot.seq();
                    slot.fill(dummy_chunk(seq));
                }
            });

            // Producer 2
            if let Some(slot) = writer.try_claim() {
                let seq = slot.seq();
                slot.fill(dummy_chunk(seq));
            }

            h.join().unwrap();

            // Consumer takes in order
            let mut taken = 0;
            while let Some(_chunk) = consumer.try_take_next() {
                taken += 1;
            }
            assert_eq!(taken, 2);
        });
    }

    /// Producer fills while consumer concurrently polls try_take_next.
    /// This is the core production pattern.
    #[test]
    fn concurrent_fill_and_take() {
        loom::model(|| {
            let (writer, consumer) = new_slot_body(2);

            // Producer claims and fills on a separate thread
            let h = thread::spawn(move || {
                let slot = writer.try_claim().unwrap();
                slot.fill(dummy_chunk(0));
            });

            // Consumer spins until it gets the chunk
            // (producer must complete since we join)
            h.join().unwrap();
            let chunk = consumer.try_take_next().unwrap();
            assert_eq!(chunk.seq, 0);
        });
    }

    /// Fill, consume, then reclaim the same slot index — tests wrap-around.
    #[test]
    fn claim_fill_take_reclaim() {
        loom::model(|| {
            let (writer, consumer) = new_slot_body(1);

            // Round 1: claim, fill, consume
            let slot = writer.try_claim().unwrap();
            assert_eq!(slot.seq(), 0);
            slot.fill(dummy_chunk(0));
            let chunk = consumer.try_take_next().unwrap();
            assert_eq!(chunk.seq, 0);

            // Round 2: same slot index (seq 1 % capacity 1 == 0)
            let slot = writer.try_claim().unwrap();
            assert_eq!(slot.seq(), 1);
            slot.fill(dummy_chunk(1));
            let chunk = consumer.try_take_next().unwrap();
            assert_eq!(chunk.seq, 1);
        });
    }

    /// Two producers + concurrent consumer — the full production pattern.
    #[test]
    fn two_producers_concurrent_consumer() {
        loom::model(|| {
            let (writer, consumer) = new_slot_body(4);
            let writer2 = writer.clone();
            use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
            let taken = Arc::new(AtomicUsize::new(0));

            let taken2 = Arc::clone(&taken);
            // Consumer thread
            let consumer_handle = thread::spawn(move || loop {
                if let Some(_chunk) = consumer.try_take_next() {
                    taken2.fetch_add(1, Ordering::Relaxed);
                    if taken2.load(Ordering::Relaxed) == 2 {
                        break;
                    }
                }
                loom::thread::yield_now();
            });

            // Producer 1
            let h = thread::spawn(move || {
                let slot = writer2.try_claim().unwrap();
                let seq = slot.seq();
                slot.fill(dummy_chunk(seq));
            });

            // Producer 2
            let slot = writer.try_claim().unwrap();
            let seq = slot.seq();
            slot.fill(dummy_chunk(seq));

            h.join().unwrap();
            consumer_handle.join().unwrap();
            assert_eq!(taken.load(Ordering::Relaxed), 2);
        });
    }
}
