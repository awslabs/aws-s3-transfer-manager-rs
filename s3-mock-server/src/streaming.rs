/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Streaming utilities for the S3 Mock Server.
//!
//! This module provides utilities for creating and working with byte streams,
//! particularly for implementing streaming GetObject operations.

use bytes::Bytes;
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A stream that yields bytes from a Vec<Bytes> in memory.
///
/// This is similar to s3s::stream::VecByteStream but adapted for our needs.
/// It's useful for implementing streaming GetObject for in-memory storage.
#[derive(Debug)]
pub(crate) struct VecByteStream {
    queue: VecDeque<Bytes>,
    remaining_bytes: usize,
}

// VecByteStream is safe to share between threads since Bytes is Send + Sync
// and VecDeque<Bytes> operations are atomic at the individual element level
unsafe impl Sync for VecByteStream {}

impl VecByteStream {
    /// Create a new VecByteStream from a single Bytes object.
    pub fn new(data: Bytes) -> Self {
        let len = data.len();
        let mut queue = VecDeque::new();
        queue.push_back(data);

        Self {
            queue,
            remaining_bytes: len,
        }
    }

    /// Create a new VecByteStream from a Vec<Bytes>.
    pub fn from_vec(data: Vec<Bytes>) -> Self {
        let total = data
            .iter()
            .map(Bytes::len)
            .try_fold(0, usize::checked_add)
            .expect("length overflow");

        Self {
            queue: data.into(),
            remaining_bytes: total,
        }
    }

    /// Get the exact remaining length in bytes.
    pub fn exact_remaining_length(&self) -> usize {
        self.remaining_bytes
    }
}

impl Stream for VecByteStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        match this.queue.pop_front() {
            Some(b) => {
                this.remaining_bytes -= b.len();
                Poll::Ready(Some(Ok(b)))
            }
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let cnt = self.queue.len();
        (cnt, Some(cnt))
    }
}

/// Apply a byte range to data, returning the sliced bytes.
///
/// This is used for implementing range requests on in-memory data.
pub(crate) fn apply_range(data: &Bytes, range: std::ops::Range<u64>) -> Bytes {
    let start = range.start as usize;
    let end = std::cmp::min(range.end as usize, data.len());

    if start >= data.len() {
        return Bytes::new();
    }

    data.slice(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObjectIntegrityChecks;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_vec_byte_stream_single_chunk() {
        let data = Bytes::from("hello world");
        let mut stream = VecByteStream::new(data.clone());

        assert_eq!(stream.exact_remaining_length(), 11);

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, data);

        assert_eq!(stream.exact_remaining_length(), 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_vec_byte_stream_multiple_chunks() {
        let chunks = vec![Bytes::from("hello"), Bytes::from(" "), Bytes::from("world")];
        let mut stream = VecByteStream::from_vec(chunks.clone());

        assert_eq!(stream.exact_remaining_length(), 11);

        for expected_chunk in chunks {
            let chunk = stream.next().await.unwrap().unwrap();
            assert_eq!(chunk, expected_chunk);
        }

        assert_eq!(stream.exact_remaining_length(), 0);
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn test_apply_range() {
        let data = Bytes::from("hello world");

        // Full range
        let result = apply_range(&data, 0..11);
        assert_eq!(result, data);

        // Partial range
        let result = apply_range(&data, 0..5);
        assert_eq!(result, Bytes::from("hello"));

        // Middle range
        let result = apply_range(&data, 6..11);
        assert_eq!(result, Bytes::from("world"));

        // Range beyond end
        let result = apply_range(&data, 6..20);
        assert_eq!(result, Bytes::from("world"));

        // Range completely beyond end
        let result = apply_range(&data, 20..30);
        assert_eq!(result, Bytes::new());
    }

    #[tokio::test]
    async fn test_streaming_get_object_integration() {
        use crate::storage::in_memory::InMemoryStorage;
        use crate::storage::models::ObjectMetadata;
        use crate::storage::StorageBackend;
        use std::collections::HashMap;

        let storage = InMemoryStorage::new();
        let test_data = Bytes::from("This is test data for streaming");
        let _metadata = ObjectMetadata {
            content_type: Some("text/plain".to_string()),
            content_length: test_data.len() as u64,
            etag: "test-etag".to_string(),
            last_modified: std::time::SystemTime::now(),
            user_metadata: HashMap::new(),
        };

        // Store the object
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();
        let test_data_clone = test_data.clone();
        let stream = Box::pin(futures::stream::once(async move { Ok(test_data_clone) }));
        let request = crate::storage::StoreObjectRequest::new("test-key", stream, integrity_checks);
        storage.put_object(request).await.unwrap();

        // Get the object as a stream
        let request = crate::storage::GetObjectRequest {
            key: "test-key",
            range: None,
        };
        let response = storage.get_object(request).await.unwrap().unwrap();
        let mut stream = response.stream;
        let returned_metadata = response.metadata;

        // Verify metadata
        assert_eq!(returned_metadata.content_length, test_data.len() as u64);
        // The etag should be the MD5 hash of the content
        let expected_etag = format!("\"{}\"", hex::encode(md5::compute(&test_data).0));
        assert_eq!(returned_metadata.etag, expected_etag);

        // Collect all chunks from the stream
        let mut collected_data = Vec::new();
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.unwrap();
            collected_data.extend_from_slice(&chunk);
        }

        // Verify the streamed data matches the original
        assert_eq!(Bytes::from(collected_data), test_data);
    }
}
