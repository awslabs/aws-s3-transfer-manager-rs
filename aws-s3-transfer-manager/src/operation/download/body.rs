/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_smithy_types::byte_stream::ByteStream;
use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;
use std::cmp;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::IoSlice;
use tokio::sync::mpsc;

use crate::error::ErrorKind;

use super::chunk_meta::ChunkMetadata;

#[derive(Debug, Clone)]

/// Non-contiguous Binary Data Storage
///
/// When data is read from the network, it is read in a sequence of chunks that are not in
/// contiguous memory. [`AggregatedBytes`](crate::byte_stream::AggregatedBytes) provides a view of
/// this data via [`impl Buf`](bytes::Buf) or it can be copied into contiguous storage with
/// [`.into_bytes()`](crate::byte_stream::AggregatedBytes::into_bytes).
pub struct AggregatedBytes(SegmentedBuf<Bytes>);

impl AggregatedBytes {
    /// Convert this buffer into [`Bytes`].
    ///
    /// # Why does this consume `self`?
    /// Technically, [`copy_to_bytes`](bytes::Buf::copy_to_bytes) can be called without ownership of self. However, since this
    /// mutates the underlying buffer such that no data is remaining, it is more misuse resistant to
    /// prevent the caller from attempting to reread the buffer.
    ///
    /// If the caller only holds a mutable reference, they may use [`copy_to_bytes`](bytes::Buf::copy_to_bytes)
    /// directly on `AggregatedBytes`.
    pub fn into_bytes(mut self) -> Bytes {
        self.0.copy_to_bytes(self.0.remaining())
    }

    /// Convert this buffer into an [`Iterator`] of underlying non-contiguous segments of [`Bytes`]
    pub fn into_segments(self) -> impl Iterator<Item = Bytes> {
        self.0.into_inner().into_iter()
    }

    /// Convert this buffer into a `Vec<u8>`
    pub fn to_vec(self) -> Vec<u8> {
        self.0.into_inner().into_iter().flatten().collect()
    }

    /// Make this buffer from a ByteStream 
    pub(crate) async fn from_byte_stream(value: ByteStream) -> Result<Self, crate::error::Error> {
        let mut value = value;
        let mut output = SegmentedBuf::new();
        while let Some(buf) = value.next().await {
            match buf {
                Ok(buf) => output.push(buf),
                Err(err) => return Err(crate::error::from_kind(ErrorKind::IOError)(err)),
            };
        }
        Ok(AggregatedBytes(output))
    }
}

impl Buf for AggregatedBytes {
    // Forward all methods that SegmentedBuf has custom implementations of.
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        self.0.chunks_vectored(dst)
    }

    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt)
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        self.0.copy_to_bytes(len)
    }
}

/// Stream of binary data representing an Amazon S3 Object's contents.
///
/// Wraps potentially multiple streams of binary data into a single coherent stream.
/// The data on this stream is sequenced into the correct order.
#[derive(Debug)]
pub struct Body {
    inner: UnorderedBody,
    sequencer: Sequencer,
}

type BodyChannel = mpsc::Receiver<Result<ChunkResponse, crate::error::Error>>;

#[derive(Debug, Clone)]
/// TODO: DOcs
pub struct ChunkResponse {
    // TODO(aws-sdk-rust#1159, design) - consider PartialOrd for ChunkResponse and hiding `seq` as internal only detail
    // the seq number
    pub(crate) seq: u64,
    /// data: chunk data
    pub data: AggregatedBytes,
    /// metadata
    pub metadata: ChunkMetadata,
}

impl Body {
    /// Create a new empty body
    pub fn empty() -> Self {
        Self::new_from_channel(None)
    }

    pub(crate) fn new(chunks: BodyChannel) -> Self {
        Self::new_from_channel(Some(chunks))
    }

    fn new_from_channel(chunks: Option<BodyChannel>) -> Self {
        Self {
            inner: UnorderedBody::new(chunks),
            sequencer: Sequencer::new(),
        }
    }

    /// Pull the next chunk of data off the stream.
    ///
    /// Returns [None] when there is no more data.
    /// Chunks returned from a [Body] are guaranteed to be sequenced
    /// in the right order.
    pub async fn next(&mut self) -> Option<Result<ChunkResponse, crate::error::Error>> {
        // TODO(aws-sdk-rust#1159, design) - do we want ChunkResponse (or similar) rather than AggregatedBytes? Would
        //  make additional retries of an individual chunk/part more feasible (though theoretically already exhausted retries)
        loop {
            if self.sequencer.is_ordered() {
                break;
            }

            match self.inner.next().await {
                None => break,
                Some(Ok(chunk)) => self.sequencer.push(chunk),
                Some(Err(err)) => return Some(Err(err)),
            }
        }

        let chunk = self.sequencer.pop();
        if let Some(chunk) = chunk {
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

    fn push(&mut self, chunk: ChunkResponse) {
        self.chunks.push(cmp::Reverse(SequencedChunk(chunk)))
    }

    fn pop(&mut self) -> Option<ChunkResponse> {
        self.chunks.pop().map(|c| c.0 .0)
    }

    fn is_ordered(&self) -> bool {
        let next = self.peek();
        if next.is_none() {
            return false;
        }

        next.unwrap().seq == self.next_seq
    }

    fn peek(&self) -> Option<&ChunkResponse> {
        self.chunks.peek().map(|c| &c.0 .0)
    }

    fn advance(&mut self) {
        self.next_seq += 1
    }
}

#[derive(Debug)]
struct SequencedChunk(ChunkResponse);

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
    chunks: Option<mpsc::Receiver<Result<ChunkResponse, crate::error::Error>>>,
}

impl UnorderedBody {
    fn new(chunks: Option<BodyChannel>) -> Self {
        Self { chunks }
    }

    /// Pull the next chunk of data off the stream.
    ///
    /// Returns [None] when there is no more data.
    /// Chunks returned from an [UnorderedBody] are not guaranteed to be sorted
    /// in the right order. Consumers are expected to sort the data themselves
    /// using the chunk sequence number (starting from zero).
    pub(crate) async fn next(&mut self) -> Option<Result<ChunkResponse, crate::error::Error>> {
        match self.chunks.as_mut() {
            None => None,
            Some(ch) => ch.recv().await,
        }
    }

    /// Close the body
    pub(crate) fn close(&mut self) {
        if let Some(ch) = &mut self.chunks {
            ch.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error, operation::download::body::ChunkResponse};
    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;
    use tokio::sync::mpsc;

    use super::{AggregatedBytes, Body, Sequencer};

    fn chunk_resp(seq: u64, data: AggregatedBytes) -> ChunkResponse {
        ChunkResponse {
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
        let mut body = Body::new(rx);
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
        let mut body = Body::new(rx);
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
