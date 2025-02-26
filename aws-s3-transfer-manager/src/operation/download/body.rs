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
use super::trailing_meta::TrailingMetadataOnceLock;
use super::{ChecksumValidationLevel, TrailingMetadata};

/// Stream of [ChunkOutput] representing an Amazon S3 Object's contents and metadata.
///
/// Wraps potentially multiple streams of binary data into a single coherent stream.
/// The data on this stream is sequenced into the correct order.
#[derive(Debug)]
pub struct Body {
    inner: UnorderedBody,
    sequencer: Sequencer,
    /// Gathers data needed for TrailingMetadata, and sends it after the full body is delivered
    trailing_meta_builder: Option<TrailingMetadataBuilder>,
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
    /// Create a new empty body
    pub fn empty() -> Self {
        Self::new_from_channel(None, None)
    }

    pub(crate) fn new(
        chunk_rx: BodyChannel,
        trailing_meta_oncelock: TrailingMetadataOnceLock,
    ) -> Self {
        Self::new_from_channel(Some(chunk_rx), Some(trailing_meta_oncelock))
    }

    fn new_from_channel(
        chunk_rx: Option<BodyChannel>,
        trailing_meta_oncelock: Option<TrailingMetadataOnceLock>,
    ) -> Self {
        Self {
            inner: UnorderedBody::new(chunk_rx),
            sequencer: Sequencer::new(),
            trailing_meta_builder: trailing_meta_oncelock.map(|x| TrailingMetadataBuilder::new(x)),
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
    pub async fn next(&mut self) -> Option<Result<ChunkOutput, crate::error::Error>> {
        loop {
            if self.sequencer.is_ordered() {
                break;
            }

            match self.inner.next().await {
                None => break,
                Some(Ok(chunk)) => self.sequencer.push(chunk),
                Some(Err(err)) => {
                    self.close();
                    return Some(Err(err));
                }
            }
        }

        let chunk = self.sequencer.pop();
        if let Some(chunk) = chunk {
            self.trailing_meta_builder
                .as_mut()
                .unwrap()
                .inspect_chunk(&chunk);

            self.sequencer.advance();
            Some(Ok(chunk))
        } else {
            // No more chunks, send the TrailingMetadata if we haven't already
            if let Some(trailing_meta_builder) = self.trailing_meta_builder.take() {
                trailing_meta_builder.send();
            }

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
    chunk_rx: Option<mpsc::Receiver<Result<ChunkOutput, crate::error::Error>>>,
}

impl UnorderedBody {
    fn new(chunk_rx: Option<BodyChannel>) -> Self {
        Self { chunk_rx }
    }

    /// Pull the next chunk of data off the stream.
    ///
    /// Returns [None] when there is no more data.
    /// Chunks returned from an [UnorderedBody] are not guaranteed to be sorted
    /// in the right order. Consumers are expected to sort the data themselves
    /// using the chunk sequence number (starting from zero).
    pub(crate) async fn next(&mut self) -> Option<Result<ChunkOutput, crate::error::Error>> {
        match self.chunk_rx.as_mut() {
            None => None,
            Some(ch) => ch.recv().await,
        }
    }

    /// Close the body
    pub(crate) fn close(&mut self) {
        if let Some(ch) = &mut self.chunk_rx {
            ch.close();
        }
    }
}

#[derive(Debug)]
struct TrailingMetadataBuilder {
    /// Once everything is downloaded and we have all the data, "send" it by setting this OnceLock
    trailing_meta_tx: TrailingMetadataOnceLock,

    num_chunks: u32,
    num_chunks_validated: u32,
}

impl TrailingMetadataBuilder {
    fn new(trailing_meta_tx: TrailingMetadataOnceLock) -> Self {
        Self {
            trailing_meta_tx,
            num_chunks: 0,
            num_chunks_validated: 0,
        }
    }

    fn inspect_chunk(&mut self, chunk: &ChunkOutput) {
        self.num_chunks += 1;

        let meta = &chunk.metadata;

        // TODO: it's more complicated that this
        let checksum = meta
            .checksum_crc32
            .as_ref()
            .or(meta.checksum_crc32_c.as_ref())
            .or(meta.checksum_crc64_nvme.as_ref())
            .or(meta.checksum_sha1.as_ref())
            .or(meta.checksum_sha256.as_ref());
        if let Some(checksum) = checksum {
            if !checksum.contains("-") {
                self.num_chunks_validated += 1;
            }
        }
    }

    fn send(self) {
        // TODO: it's more complicated than this
        let checksum_validation_level =
            if self.num_chunks > 0 && self.num_chunks == self.num_chunks_validated {
                ChecksumValidationLevel::AllChunks
            } else {
                ChecksumValidationLevel::NotValidated
            };

        let trailing_meta = TrailingMetadata {
            checksum_validation_level,
        };

        if self.trailing_meta_tx.set(trailing_meta).is_err() {
            tracing::debug!("TODO: what message");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error,
        operation::download::{body::ChunkOutput, TrailingMetadata},
    };
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
        let trailing_meta_oncelock = TrailingMetadata::new_oncelock();
        let mut body = Body::new(rx, trailing_meta_oncelock);
        tokio::spawn(async move {
            let seq = vec![2, 0, 1];
            for i in seq {
                let data = Bytes::from(format!("chunk {i}"));
                let mut aggregated = SegmentedBuf::new();
                aggregated.push(data);
                let chunk = chunk_resp(i as u64, AggregatedBytes(aggregated));
                tx.send(Ok(chunk)).await.unwrap();
            }
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

    #[tokio::test]
    async fn test_body_next_error() {
        let (tx, rx) = mpsc::channel(2);
        let trailing_meta_oncelock = TrailingMetadata::new_oncelock();
        let mut body = Body::new(rx, trailing_meta_oncelock);
        tokio::spawn(async move {
            let data = Bytes::from("chunk 0".to_string());
            let mut aggregated = SegmentedBuf::new();
            aggregated.push(data);
            let chunk = chunk_resp(0, AggregatedBytes(aggregated));
            tx.send(Ok(chunk)).await.unwrap();
            let err = error::Error::new(error::ErrorKind::InputInvalid, "test errors".to_string());
            tx.send(Err(err)).await.unwrap();
        });

        let mut received = Vec::new();
        while let Some(chunk) = body.next().await {
            received.push(chunk);
        }

        assert_eq!(2, received.len());
        received.pop().unwrap().expect_err("error propagated");
        received.pop().unwrap().expect("chunk 0 successful");
    }
}
