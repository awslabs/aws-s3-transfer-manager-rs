/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::io::AggregatedBytes;

use super::chunk_meta::ChunkMetadata;
use super::transfer::DownloadTransfer;

/// Default slot buffer capacity for download body delivery.
pub(crate) const DEFAULT_BODY_SLOT_CAPACITY: usize = 256;

const SLOT_EMPTY: u8 = 0;
const SLOT_FILLED: u8 = 1;

/// Fixed-size ring buffer of slots for delivering download chunks.
///
/// Slots are indexed by `seq % capacity`. Each slot transitions through
/// two states: `Empty` and `Filled`. The invariants that make `UnsafeCell`
/// access safe are:
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
struct SlotBuffer {
    slots: Box<[Slot]>,
    capacity: u64,
    /// Next seq to be consumed by the consumer. Only advanced by the consumer.
    consumed: AtomicU64,
    /// Next seq to be claimed by a producer. Advanced via CAS in `try_claim`.
    claimed: AtomicU64,
    /// Wakes the consumer when a producer fills a slot.
    notify: tokio::sync::Notify,
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
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Write a completed chunk into the slot for `seq`.
    ///
    /// # Requirements
    /// - `seq` must have been claimed via the `claimed` CAS (i.e. through
    ///   `BodyWriter::try_claim`). Filling an unclaimed seq is undefined behavior.
    /// - Each `seq` must be filled exactly once. Double-filling is undefined behavior.
    fn fill(&self, seq: u64, chunk: ChunkOutput) {
        let idx = (seq % self.capacity) as usize;
        // Safety: seq window guarantees exclusive access to this slot index.
        unsafe {
            *self.slots[idx].data.get() = Some(chunk);
        }
        self.slots[idx]
            .state
            .store(SLOT_FILLED, AtomicOrdering::Release);
        self.notify.notify_one();
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
        let chunk = unsafe { (*self.slots[idx].data.get()).take() };
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

impl BodyWriter {
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

    /// Wake the consumer. Call when the transfer reaches a terminal state
    /// (success, failure, cancellation) so the consumer unblocks from
    /// `notified().await` and observes the terminal condition.
    pub(crate) fn notify_consumer(&self) {
        self.buffer.notify.notify_one();
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
    /// otherwise the consumer may block indefinitely.
    pub(crate) async fn next(&self, is_terminal: impl Fn() -> bool) -> Option<ChunkOutput> {
        loop {
            if let Some(chunk) = self.buffer.try_take_next() {
                return Some(chunk);
            }
            if is_terminal() {
                // Drain any slot filled between our check and here.
                return self.buffer.try_take_next();
            }
            self.buffer.notify.notified().await;
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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChunkOutput {
    // TODO(aws-sdk-rust#1159, design) - consider PartialOrd for ChunkResponse and hiding `seq` as internal only detail
    // the seq number
    pub(crate) seq: u64,
    /// The content associated with this particular ranged GetObject request.
    pub data: AggregatedBytes,
    /// The metadata associated with this particular ranged GetObject request. This contains all the
    /// metadata returned by the S3 GetObject operation.
    pub metadata: ChunkMetadata,
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

    #[tokio::test]
    async fn test_body_next_on_cancelled_transfer() {
        let (mut body, transfer, _writer) = test_body();

        // Cancel the transfer
        transfer.ctx().set_cancelled();

        // next() should return cancellation error
        let err = body.next().await.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::OperationCancelled));
    }

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

    #[tokio::test]
    async fn test_slot_body_consumer_next() {
        let (writer, consumer) = new_slot_body(4);

        let s0 = writer.try_claim().unwrap();
        s0.fill(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));

        let chunk = consumer.next(|| false).await;
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().seq, 0);
    }

    #[tokio::test]
    async fn test_slot_body_consumer_returns_none_when_complete() {
        let (_writer, consumer) = new_slot_body(4);

        let chunk = consumer.next(|| true).await;
        assert!(chunk.is_none());
    }

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
}
