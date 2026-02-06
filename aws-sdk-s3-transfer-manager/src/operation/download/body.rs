/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tokio::sync::mpsc;

use crate::io::AggregatedBytes;

use super::chunk_meta::ChunkMetadata;
use super::transfer::DownloadTransfer;

/// Stream of [ChunkOutput] representing an Amazon S3 Object's contents and metadata.
///
/// Wraps potentially multiple streams of binary data into a single coherent stream.
/// The data on this stream is sequenced into the correct order.
#[derive(Debug)]
pub struct Body {
    inner: UnorderedBody,
    sequencer: Sequencer,
    transfer: DownloadTransfer,
}

type BodyChannel = mpsc::Receiver<Result<ChunkOutput, crate::error::Error>>;

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

// TODO: Do we want to expose something to yield multiple chunks in a single call, like
// recv_many/collect, etc.? We can benchmark to see if we get a significant performance boost once
// we have a better scheduler in place.
impl Body {
    pub(crate) fn new(chunks: BodyChannel, transfer: DownloadTransfer) -> Self {
        Self {
            inner: UnorderedBody::new(chunks),
            sequencer: Sequencer::new(),
            transfer,
        }
    }

    /// Convert this body into an unordered stream of chunks.
    // TODO(aws-sdk-rust#1159) - revisit if we actually need/use unordered data stream.
    // download_objects should utilize this so that it can write in parallel to files.
    #[allow(dead_code)]
    pub(crate) fn unordered(self) -> UnorderedBody {
        self.inner
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
            if self.sequencer.is_ordered() {
                break;
            }

            match self.inner.next().await {
                None => {
                    // Channel closed - check if transfer failed or was cancelled
                    let ctx = self.transfer.ctx().clone();
                    tracing::debug!(transfer = %ctx, "channel closed");

                    if ctx.is_cancelled() {
                        self.close();
                        return Some(Err(crate::error::from_kind(
                            crate::error::ErrorKind::OperationCancelled,
                        )("download cancelled")));
                    } else if ctx.is_failed() {
                        self.close();
                        let kind = ctx
                            .error_kind()
                            .unwrap_or(crate::error::ErrorKind::ChildOperationFailed);
                        return Some(Err(crate::error::from_kind(kind)("transfer failed")));
                    }
                    // Channel closed - should be in terminal state
                    debug_assert!(!ctx.is_active(), "channel closed but transfer still active");
                    break;
                }
                Some(Ok(chunk)) => self.sequencer.push(chunk),
                Some(Err(err)) => {
                    // Legacy path - errors via channel. Don't close, just propagate.
                    // TODO: Remove this path when we fully migrate to status-based errors
                    return Some(Err(err));
                }
            }
        }

        let chunk = self.sequencer.pop();
        if let Some(chunk) = chunk {
            // Advance consumed seq - may enable more work generation
            if self.transfer.seq_window().consume(chunk.seq) {
                self.transfer.ctx().try_wake();
            }
            self.sequencer.advance();
            Some(Ok(chunk))
        } else {
            None
        }
    }

    /// Close the body, no more data will flow from it and all publishers will be notified.
    pub(crate) fn close(&mut self) {
        self.inner.close()
    }
}

#[derive(Debug)]
struct Sequencer {
    /// next expected sequence
    next_seq: u64,
    chunks: BinaryHeap<cmp::Reverse<SequencedChunk>>,
}

impl Sequencer {
    fn new() -> Self {
        Self {
            chunks: BinaryHeap::with_capacity(8),
            next_seq: 0,
        }
    }

    fn push(&mut self, chunk: ChunkOutput) {
        self.chunks.push(cmp::Reverse(SequencedChunk(chunk)))
    }

    fn pop(&mut self) -> Option<ChunkOutput> {
        self.chunks.pop().map(|c| c.0 .0)
    }

    fn is_ordered(&self) -> bool {
        let next = self.peek();
        if next.is_none() {
            return false;
        }

        next.unwrap().seq == self.next_seq
    }

    fn peek(&self) -> Option<&ChunkOutput> {
        self.chunks.peek().map(|c| &c.0 .0)
    }

    fn advance(&mut self) {
        self.next_seq += 1
    }
}

#[derive(Debug)]
struct SequencedChunk(ChunkOutput);

impl Ord for SequencedChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.seq.cmp(&other.0.seq)
    }
}

impl PartialOrd for SequencedChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SequencedChunk {}
impl PartialEq for SequencedChunk {
    fn eq(&self, other: &Self) -> bool {
        self.0.seq == other.0.seq
    }
}

/// A body that returns chunks in whatever order they are received.
#[derive(Debug)]
pub(crate) struct UnorderedBody {
    chunks: BodyChannel,
}

impl UnorderedBody {
    fn new(chunks: BodyChannel) -> Self {
        Self { chunks }
    }

    /// Pull the next chunk of data off the stream.
    ///
    /// Returns [None] when there is no more data.
    /// Chunks returned from an [UnorderedBody] are not guaranteed to be sorted
    /// in the right order. Consumers are expected to sort the data themselves
    /// using the chunk sequence number (starting from zero).
    pub(crate) async fn next(&mut self) -> Option<Result<ChunkOutput, crate::error::Error>> {
        self.chunks.recv().await
    }

    /// Close the body
    pub(crate) fn close(&mut self) {
        self.chunks.close();
    }
}

#[cfg(test)]
mod tests {
    use crate::operation::download::transfer::DownloadTransfer;
    use crate::{error, operation::download::body::ChunkOutput};
    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;
    use tokio::sync::mpsc;

    use super::{AggregatedBytes, Body, Sequencer};

    fn chunk_resp(seq: u64, data: AggregatedBytes) -> ChunkOutput {
        ChunkOutput {
            seq,
            data,
            metadata: Default::default(),
        }
    }

    fn test_body(
        rx: mpsc::Receiver<Result<ChunkOutput, crate::error::Error>>,
    ) -> (Body, DownloadTransfer) {
        use crate::operation::download::DownloadInput;
        use crate::operation::TransferContext;
        use crate::types::BucketType;

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .with_test_defaults()
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .build(),
        );
        let config = crate::Config::builder().client(s3_client).build();
        let tm = crate::Client::new(config);
        let id = crate::scheduler::TransferId {
            id: 0,
            parent: None,
        };
        let input = DownloadInput::builder()
            .bucket("test")
            .key("test")
            .build()
            .unwrap();
        let (tx, _) = mpsc::channel(1);
        let (ctx, _) = TransferContext::new(id, tm.handle.clone());
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, tx);

        (Body::new(rx, transfer.clone()), transfer)
    }

    #[test]
    fn test_sequencer() {
        let mut sequencer = Sequencer::new();
        sequencer.push(chunk_resp(1, AggregatedBytes(SegmentedBuf::new())));
        sequencer.push(chunk_resp(2, AggregatedBytes(SegmentedBuf::new())));
        assert_eq!(sequencer.peek().unwrap().seq, 1);
        sequencer.push(chunk_resp(0, AggregatedBytes(SegmentedBuf::new())));
        assert_eq!(sequencer.pop().unwrap().seq, 0);
    }

    #[tokio::test]
    async fn test_body_next() {
        let (tx, rx) = mpsc::channel(2);
        let (mut body, ctx) = test_body(rx);
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let seq = vec![2, 0, 1];
            for i in seq {
                let data = Bytes::from(format!("chunk {i}"));
                let mut aggregated = SegmentedBuf::new();
                aggregated.push(data);
                let chunk = chunk_resp(i as u64, AggregatedBytes(aggregated));
                tx.send(Ok(chunk)).await.unwrap();
            }
            // Mark completed before channel closes
            ctx_clone.ctx().set_completed();
        });

        let mut received = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("chunk ok");
            let data = String::from_utf8(chunk.data.to_vec()).unwrap();
            received.push(data);
        }

        let expected: Vec<String> = [0, 1, 2].iter().map(|i| format!("chunk {i}")).collect();
        assert_eq!(expected, received);
    }

    // Test: when transfer fails, next() returns error
    #[tokio::test]
    async fn test_body_next_on_failed_transfer() {
        let (tx, rx) = mpsc::channel(2);
        let (mut body, transfer) = test_body(rx);

        // Send one chunk, then fail the transfer
        let data = Bytes::from("chunk 0");
        let mut aggregated = SegmentedBuf::new();
        aggregated.push(data);
        tx.send(Ok(chunk_resp(0, AggregatedBytes(aggregated))))
            .await
            .unwrap();

        // Fail the transfer and close channel
        transfer.ctx().set_failed(error::Error::new(
            error::ErrorKind::ChildOperationFailed,
            "simulated failure",
        ));
        drop(tx);

        // First call returns the buffered chunk
        let chunk = body.next().await.unwrap().unwrap();
        assert_eq!(chunk.seq, 0);

        // Second call sees channel closed + failed status, returns error
        let err = body.next().await.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::ChildOperationFailed));
    }

    // Test: when transfer is cancelled, next() returns error
    #[tokio::test]
    async fn test_body_next_on_cancelled_transfer() {
        let (tx, rx) = mpsc::channel::<Result<ChunkOutput, crate::error::Error>>(2);
        let (mut body, transfer) = test_body(rx);

        // Cancel the transfer and close channel
        transfer.ctx().set_cancelled();
        drop(tx);

        // next() should return cancellation error
        let err = body.next().await.unwrap().unwrap_err();
        assert!(matches!(err.kind(), error::ErrorKind::OperationCancelled));
    }
}
