/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! In-memory implementation of the StorageBackend trait.

use std::collections::HashMap;
use std::ops::Range;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::StorageBackend;

/// An in-memory implementation of the StorageBackend trait.
///
/// This implementation stores all objects and multipart uploads in memory,
/// making it suitable for testing and benchmarking.
pub(crate) struct InMemoryStorage {
    // key -> content
    objects: RwLock<HashMap<String, Bytes>>,
    // upload-id -> (part# -> content)
    parts: RwLock<HashMap<String, HashMap<i32, Bytes>>>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage backend.
    pub(crate) fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            parts: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn get_object_data(&self, key: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let objects = self.objects.read().await;
        let data = objects.get(key).ok_or(Error::NoSuchKey)?;

        if let Some(range) = range {
            let start = range.start as usize;
            let end = range.end.min(data.len() as u64) as usize;

            if start >= data.len() || start > end {
                return Err(Error::InvalidRange);
            }

            Ok(data.slice(start..end))
        } else {
            Ok(data.clone())
        }
    }

    async fn put_object_data(&self, key: &str, content: Bytes) -> Result<()> {
        let mut objects = self.objects.write().await;
        objects.insert(key.to_string(), content);
        Ok(())
    }

    async fn delete_object_data(&self, key: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        if objects.remove(key).is_none() {
            return Err(Error::NoSuchKey);
        }
        Ok(())
    }

    async fn store_part_data(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<()> {
        let mut parts = self.parts.write().await;
        let upload_parts = parts
            .entry(upload_id.to_string())
            .or_insert_with(HashMap::new);
        upload_parts.insert(part_number, content);
        Ok(())
    }

    async fn get_part_data(&self, upload_id: &str, part_number: i32) -> Result<Bytes> {
        let parts = self.parts.read().await;
        let upload_parts = parts.get(upload_id).ok_or(Error::NoSuchUpload)?;
        let part_data = upload_parts.get(&part_number).ok_or(Error::NoSuchPart)?;
        Ok(part_data.clone())
    }

    async fn complete_multipart_data(
        &self,
        upload_id: &str,
        key: &str,
        part_numbers: &[i32],
    ) -> Result<()> {
        // Get all parts for this upload
        let mut parts = self.parts.write().await;
        let upload_parts = parts.remove(upload_id).ok_or(Error::NoSuchUpload)?;

        // Verify all required parts are present
        for &part_number in part_numbers {
            if !upload_parts.contains_key(&part_number) {
                return Err(Error::NoSuchPart);
            }
        }

        // Combine all parts in the specified order
        let mut combined = BytesMut::new();
        for &part_number in part_numbers {
            if let Some(part_data) = upload_parts.get(&part_number) {
                combined.extend_from_slice(part_data);
            }
        }

        // Store the combined object under the specified key
        let mut objects = self.objects.write().await;
        objects.insert(key.to_string(), combined.freeze());

        Ok(())
    }

    async fn abort_multipart_data(&self, upload_id: &str) -> Result<()> {
        let mut parts = self.parts.write().await;
        if parts.remove(upload_id).is_none() {
            return Err(Error::NoSuchUpload);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");

        // Put object
        storage.put_object_data(key, content.clone()).await.unwrap();

        // Get object
        let retrieved = storage.get_object_data(key, None).await.unwrap();
        assert_eq!(retrieved, content);
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");

        // Put object
        storage.put_object_data(key, content).await.unwrap();

        // Get object with range
        let range = 0..4;
        let retrieved = storage.get_object_data(key, Some(range)).await.unwrap();
        assert_eq!(retrieved, Bytes::from("test"));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");

        // Put object
        storage.put_object_data(key, content).await.unwrap();

        // Delete object
        storage.delete_object_data(key).await.unwrap();

        // Try to get deleted object
        let result = storage.get_object_data(key, None).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NoSuchKey));
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload";
        let key = "test-key";
        let part_numbers = [1, 2];

        // Store parts
        storage
            .store_part_data(upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        storage
            .store_part_data(upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        // Get parts
        let part1 = storage.get_part_data(upload_id, 1).await.unwrap();
        let part2 = storage.get_part_data(upload_id, 2).await.unwrap();
        assert_eq!(part1, Bytes::from("part1"));
        assert_eq!(part2, Bytes::from("part2"));

        // Complete multipart upload
        storage
            .complete_multipart_data(upload_id, key, &part_numbers)
            .await
            .unwrap();

        // Get the combined object
        let combined = storage.get_object_data(key, None).await.unwrap();
        assert_eq!(combined, Bytes::from("part1part2"));
    }

    #[tokio::test]
    async fn test_multipart_upload_missing_part() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload";
        let key = "test-key";
        let part_numbers = [1, 2, 3]; // Part 3 doesn't exist

        // Store parts
        storage
            .store_part_data(upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        storage
            .store_part_data(upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        // Try to complete multipart upload with missing part
        let result = storage
            .complete_multipart_data(upload_id, key, &part_numbers)
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NoSuchPart));
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload";

        // Store a part
        storage
            .store_part_data(upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();

        // Abort the upload
        storage.abort_multipart_data(upload_id).await.unwrap();

        // Try to get the part
        let result = storage.get_part_data(upload_id, 1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NoSuchUpload));
    }
}
