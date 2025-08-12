/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Filesystem implementation of the StorageBackend trait.

use std::io::SeekFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::error::{Error, Result};
use crate::storage::models::{MultipartUploadMetadata, ObjectMetadata, PartMetadata};
use crate::storage::StorageBackend;
use crate::types::StoredObjectMetadata;

/// A stream wrapper that limits the total number of bytes read from the underlying stream.
#[pin_project]
struct LimitedStream<S> {
    #[pin]
    inner: S,
    remaining: u64,
}

impl<S> LimitedStream<S> {
    fn new(inner: S, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}

impl<S> Stream for LimitedStream<S>
where
    S: Stream<Item = std::result::Result<Bytes, std::io::Error>>,
{
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.remaining == 0 {
            return Poll::Ready(None);
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let bytes_len = bytes.len() as u64;
                if bytes_len <= *this.remaining {
                    *this.remaining -= bytes_len;
                    Poll::Ready(Some(Ok(bytes)))
                } else {
                    // Truncate the bytes to the remaining limit
                    let truncated = bytes.slice(0..*this.remaining as usize);
                    *this.remaining = 0;
                    Poll::Ready(Some(Ok(truncated)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A filesystem implementation of the StorageBackend trait.
///
/// This implementation stores objects and multipart uploads on the local filesystem,
/// making it suitable for testing and benchmarking with larger datasets. The directory
/// structure is:
///
/// ```text
/// root/
/// ├── objects/
/// │   ├── my-file.txt              # Object data
/// │   └── my-file.txt.metadata     # Object metadata (JSON)
/// ├── uploads/
/// │   ├── upload-123/
/// │   │   ├── metadata.json        # Upload metadata
/// │   │   ├── part-1.dat          # Part data
/// │   │   └── part-1.metadata     # Part metadata
/// │   └── ...
/// ```
#[derive(Debug)]
pub(crate) struct FilesystemStorage {
    root_dir: PathBuf,
    objects_dir: PathBuf,
    uploads_dir: PathBuf,
}

impl FilesystemStorage {
    /// Create a new filesystem storage backend.
    ///
    /// # Arguments
    ///
    /// * `root_dir` - The root directory for storing objects and uploads
    ///
    /// # Returns
    ///
    /// A new FilesystemStorage instance
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created
    pub(crate) async fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        let objects_dir = root_dir.join("objects");
        let uploads_dir = root_dir.join("uploads");

        // Create directories if they don't exist
        fs::create_dir_all(&objects_dir).await?;
        fs::create_dir_all(&uploads_dir).await?;

        Ok(Self {
            root_dir,
            objects_dir,
            uploads_dir,
        })
    }

    // Helper method to get the path for an object's data
    fn get_object_path(&self, key: &str) -> PathBuf {
        // Handle empty key as a special case
        if key.is_empty() {
            return self.objects_dir.join("empty_key");
        }
        object_key_to_path(&self.objects_dir, key)
    }

    // Helper method to get the path for an object's metadata
    fn get_object_metadata_path(&self, key: &str) -> PathBuf {
        // Handle empty key as a special case
        if key.is_empty() {
            return self.objects_dir.join("empty_key.metadata");
        }
        object_key_to_path(&self.objects_dir, &format!("{}.metadata", key))
    }

    // Helper method to get the directory for an upload
    fn get_upload_dir(&self, upload_id: &str) -> PathBuf {
        self.uploads_dir.join(upload_id)
    }

    // Helper method to get the path for an upload's metadata
    fn get_upload_metadata_path(&self, upload_id: &str) -> PathBuf {
        self.get_upload_dir(upload_id).join("metadata.json")
    }

    // Helper method to get the path for a part's data
    fn get_part_path(&self, upload_id: &str, part_number: i32) -> PathBuf {
        self.get_upload_dir(upload_id)
            .join(format!("part-{}.dat", part_number))
    }

    // Helper method to get the path for a part's metadata
    fn get_part_metadata_path(&self, upload_id: &str, part_number: i32) -> PathBuf {
        self.get_upload_dir(upload_id)
            .join(format!("part-{}.metadata", part_number))
    }

    // Helper method to save metadata to a file
    async fn save_metadata<T: serde::Serialize>(path: &Path, metadata: &T) -> Result<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Serialize and write metadata
        let json =
            serde_json::to_string(metadata).map_err(|e| Error::Io(std::io::Error::other(e)))?;
        fs::write(path, json).await?;
        Ok(())
    }

    // Helper method to load metadata from a file
    async fn load_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
        match fs::read_to_string(path).await {
            Ok(json) => {
                let metadata =
                    serde_json::from_str(&json).map_err(|e| Error::Io(std::io::Error::other(e)))?;
                Ok(Some(metadata))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    // Helper method to list all objects in a directory
    // Helper method to list all objects in a directory recursively
    fn list_directory<'a>(
        &'a self,
        dir: &'a Path,
        prefix: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<PathBuf>>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut entries = Vec::new();
            let mut read_dir = fs::read_dir(dir).await?;

            while let Some(entry) = read_dir.next_entry().await? {
                let path = entry.path();
                let metadata = fs::metadata(&path).await?;

                if metadata.is_dir() {
                    // Recursively list subdirectories
                    let mut sub_entries = self.list_directory(&path, prefix).await?;
                    entries.append(&mut sub_entries);
                } else if path.extension().is_none_or(|ext| ext != "metadata") {
                    if let Some(key) = path_to_object_key(&self.objects_dir, &path) {
                        if let Some(prefix) = prefix {
                            if !key.starts_with(prefix) {
                                continue;
                            }
                        }
                        entries.push(path);
                    }
                }
            }

            entries.sort();
            Ok(entries)
        })
    }
}

// Helper function to convert an object key to a filesystem path
fn object_key_to_path(base_dir: &Path, key: &str) -> PathBuf {
    // Split the key on '/' and join the parts to create a path
    let parts: Vec<&str> = key.split('/').collect();
    let mut path = base_dir.to_path_buf();
    path.extend(parts);
    path
}

// Helper function to convert a filesystem path back to an object key
fn path_to_object_key(base_dir: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(base_dir)
        .ok()
        .map(|rel_path| rel_path.to_string_lossy().replace('\\', "/"))
}
#[async_trait]
impl StorageBackend for FilesystemStorage {
    async fn put_object(
        &self,
        request: crate::storage::StoreObjectRequest,
    ) -> Result<StoredObjectMetadata> {
        let mut body = request.body;
        let mut integrity_checks = request.integrity_checks;
        let path = self.get_object_path(&request.key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = fs::File::create(&path).await?;
        let mut content_length = 0u64;

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Internal(format!("Stream error: {}", e)))?;
            integrity_checks.update(&chunk);
            content_length += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        let object_integrity = integrity_checks.finalize();
        let last_modified = SystemTime::now();

        let metadata = ObjectMetadata {
            content_type: request.content_type,
            content_length,
            etag: object_integrity.etag().cloned().unwrap_or_default(),
            last_modified,
            user_metadata: request.user_metadata,
        };
        let metadata_path = self.get_object_metadata_path(&request.key);
        Self::save_metadata(&metadata_path, &metadata).await?;

        Ok(StoredObjectMetadata {
            content_length,
            object_integrity,
            last_modified,
        })
    }

    async fn get_object(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<
        Option<(
            Box<
                dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
            >,
            ObjectMetadata,
        )>,
    > {
        let path = self.get_object_path(key);
        let metadata_path = self.get_object_metadata_path(key);

        // Load metadata first to check if object exists
        let metadata: ObjectMetadata = match Self::load_metadata(&metadata_path).await? {
            Some(metadata) => metadata,
            None => return Ok(None),
        };

        // Open the file
        let mut file = match fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };

        // Handle range request
        let (content_length, _seek_position) = if let Some(ref range) = range {
            let start = range.start;
            let end = range.end.min(metadata.content_length);

            if start >= metadata.content_length || start > end {
                return Err(Error::InvalidRange);
            }

            // Seek to start of range
            file.seek(SeekFrom::Start(start)).await?;
            (end - start, start)
        } else {
            (metadata.content_length, 0)
        };

        // Create a reader stream with a reasonable buffer size
        let reader_stream = ReaderStream::with_capacity(file, 8192);

        // If we have a range, we need to limit the stream to only read the specified amount
        let limited_stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
        > = if range.is_some() {
            // Create a stream that limits the total bytes read
            Box::new(LimitedStream::new(reader_stream, content_length))
        } else {
            Box::new(reader_stream)
        };

        Ok(Some((limited_stream, metadata)))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let path = self.get_object_path(key);
        let metadata_path = self.get_object_metadata_path(key);

        // Delete both data and metadata files
        match fs::remove_file(&path).await {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Error::NoSuchKey),
            Err(e) => return Err(Error::Io(e)),
        }

        // Try to delete metadata, but don't error if it's already gone
        let _ = fs::remove_file(metadata_path).await;

        Ok(())
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<(String, ObjectMetadata)>> {
        let mut result = Vec::new();

        // List all objects in the directory
        let entries = self.list_directory(&self.objects_dir, prefix).await?;

        // Load metadata for each object
        for path in entries {
            if let Some(key) = path_to_object_key(&self.objects_dir, &path) {
                let metadata_path = self.get_object_metadata_path(&key);
                if let Some(metadata) = Self::load_metadata(&metadata_path).await? {
                    result.push((key, metadata));
                }
            }
        }

        Ok(result)
    }

    async fn create_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        metadata: ObjectMetadata,
    ) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);
        fs::create_dir_all(&upload_dir).await?;

        let upload_metadata = MultipartUploadMetadata {
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            metadata,
            parts: Default::default(),
        };

        let metadata_path = self.get_upload_metadata_path(upload_id);
        Self::save_metadata(&metadata_path, &upload_metadata).await?;

        Ok(())
    }

    async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<String> {
        // Verify the upload exists
        let metadata_path = self.get_upload_metadata_path(upload_id);
        let mut upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        // Calculate ETag
        let etag = format!("\"{:x}\"", md5::compute(&content));

        // Save the part data
        let part_path = self.get_part_path(upload_id, part_number);
        if let Some(parent) = part_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&part_path, &content).await?;

        // Save the part metadata
        let part_metadata = PartMetadata {
            etag: etag.clone(),
            size: content.len() as u64,
        };
        let part_metadata_path = self.get_part_metadata_path(upload_id, part_number);
        Self::save_metadata(&part_metadata_path, &part_metadata).await?;

        // Update the upload metadata
        upload_metadata.parts.insert(part_number, part_metadata);
        Self::save_metadata(&metadata_path, &upload_metadata).await?;

        Ok(etag)
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(i32, String, u64)>> {
        let metadata_path = self.get_upload_metadata_path(upload_id);
        let upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        let mut result: Vec<_> = upload_metadata
            .parts
            .iter()
            .map(|(&part_number, part_metadata)| {
                (part_number, part_metadata.etag.clone(), part_metadata.size)
            })
            .collect();

        // Sort by part number for consistent ordering
        result.sort_by_key(|&(part_number, _, _)| part_number);
        Ok(result)
    }

    async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(String, ObjectMetadata)> {
        // Load the upload metadata
        let metadata_path = self.get_upload_metadata_path(upload_id);
        let upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        // Verify all parts exist and ETags match
        let mut total_size = 0u64;
        let mut etags = Vec::new();
        let mut combined = BytesMut::new();

        for (part_number, expected_etag) in &parts {
            let part_metadata_path = self.get_part_metadata_path(upload_id, *part_number);
            let part_metadata: PartMetadata = Self::load_metadata(&part_metadata_path)
                .await?
                .ok_or(Error::NoSuchPart)?;

            if part_metadata.etag != *expected_etag {
                return Err(Error::InvalidPart);
            }

            // Read the part data
            let part_path = self.get_part_path(upload_id, *part_number);
            let part_data = fs::read(&part_path).await?;
            combined.extend_from_slice(&part_data);

            total_size += part_metadata.size;
            etags.push(part_metadata.etag.clone());
        }

        // Calculate the final ETag
        let combined_etag = if etags.len() > 1 {
            let etags_concat = etags.join("");
            format!("\"{:x}-{}\"", md5::compute(etags_concat), etags.len())
        } else if !etags.is_empty() {
            etags[0].clone()
        } else {
            format!("\"{:x}\"", md5::compute(""))
        };

        // Update the final metadata
        let mut final_metadata = upload_metadata.metadata;
        final_metadata.content_length = total_size;
        final_metadata.etag = combined_etag;
        final_metadata.last_modified = SystemTime::now();

        // Save the final object directly
        let combined_data = combined.freeze();
        let object_path = self.get_object_path(&upload_metadata.key);
        let metadata_path = self.get_object_metadata_path(&upload_metadata.key);

        // Ensure parent directory exists
        if let Some(parent) = object_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write object data and metadata
        fs::write(&object_path, &combined_data).await?;
        let metadata_json = serde_json::to_string_pretty(&final_metadata)
            .map_err(|e| crate::Error::Internal(e.to_string()))?;
        fs::write(&metadata_path, metadata_json).await?;

        // Clean up the multipart upload
        let _ = fs::remove_dir_all(self.get_upload_dir(upload_id)).await;

        Ok((upload_metadata.key, final_metadata))
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);

        // Verify the upload exists
        let metadata_path = self.get_upload_metadata_path(upload_id);
        if !metadata_path.exists() {
            return Err(Error::NoSuchUpload);
        }

        // Remove the entire upload directory
        fs::remove_dir_all(upload_dir).await?;

        Ok(())
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let metadata_path = self.get_object_metadata_path(key);
        Self::load_metadata(&metadata_path).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObjectIntegrityChecks;
    use futures::StreamExt;
    use std::collections::HashMap;
    use tempfile::tempdir;

    // Helper function to collect stream data into bytes
    async fn collect_stream_data(
        mut stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
        >,
    ) -> Bytes {
        let mut collected_data = Vec::new();
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.unwrap();
            collected_data.extend_from_slice(&chunk);
        }
        Bytes::from(collected_data)
    }

    fn create_test_metadata(content_length: u64) -> ObjectMetadata {
        ObjectMetadata {
            content_type: Some("text/plain".to_string()),
            content_length,
            etag: format!("\"{:x}\"", md5::compute("test")),
            last_modified: SystemTime::now(),
            user_metadata: HashMap::new(),
        }
    }

    // Helper function to convert Bytes to a stream for testing
    fn bytes_to_stream(
        data: Bytes,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>> {
        Box::pin(futures::stream::once(async move { Ok(data) }))
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get object
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, retrieved_metadata) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, content);
        assert_eq!(retrieved_metadata.content_length, content.len() as u64);
        // Content type is not preserved in the new streaming API
        assert_eq!(retrieved_metadata.content_type, None);
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "test-key";
        let content = Bytes::from("0123456789");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get range
        let range = Some(2..5);
        let result = storage.get_object(key, range).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, _) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, Bytes::from("234"));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Verify it exists
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());

        // Delete object
        storage.delete_object(key).await.unwrap();

        // Verify it's gone
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let content = Bytes::from("test content");

        // Put multiple objects
        for i in 0..3 {
            let key = format!("test-key-{}", i);
            let integrity_checks = ObjectIntegrityChecks::new().with_md5();
            let stream = bytes_to_stream(content.clone());
            storage
                .put_object(crate::storage::StoreObjectRequest::new(
                    &key,
                    stream,
                    integrity_checks,
                ))
                .await
                .unwrap();
        }

        // List all objects
        let objects = storage.list_objects(None).await.unwrap();
        assert_eq!(objects.len(), 3);

        // List with prefix
        let objects = storage.list_objects(Some("test-key-1")).await.unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].0, "test-key-1");
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0); // Will be updated on completion

        // Create multipart upload
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload parts
        let part1 = Bytes::from("part1");
        let part2 = Bytes::from("part2");

        let etag1 = storage
            .upload_part(upload_id, 1, part1.clone())
            .await
            .unwrap();
        let etag2 = storage
            .upload_part(upload_id, 2, part2.clone())
            .await
            .unwrap();

        // List parts
        let parts = storage.list_parts(upload_id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].0, 1); // part number
        assert_eq!(parts[0].1, etag1); // etag
        assert_eq!(parts[0].2, part1.len() as u64); // size

        // Complete multipart upload
        let parts_to_complete = vec![(1, etag1), (2, etag2)];
        let (final_key, final_metadata) = storage
            .complete_multipart_upload(upload_id, parts_to_complete)
            .await
            .unwrap();

        assert_eq!(final_key, key);
        assert_eq!(
            final_metadata.content_length,
            (part1.len() + part2.len()) as u64
        );

        // Verify the final object exists
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (final_stream, _) = result.unwrap();
        let final_content = collect_stream_data(final_stream).await;
        assert_eq!(final_content, Bytes::from("part1part2"));
    }

    #[tokio::test]
    async fn test_multipart_upload_missing_part() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        // Create multipart upload
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload only one part
        let part1 = Bytes::from("part1");
        let etag1 = storage.upload_part(upload_id, 1, part1).await.unwrap();

        // Try to complete with a missing part
        let parts_to_complete = vec![(1, etag1), (2, "missing-etag".to_string())];
        let result = storage
            .complete_multipart_upload(upload_id, parts_to_complete)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        // Create multipart upload
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload a part
        let part1 = Bytes::from("part1");
        storage.upload_part(upload_id, 1, part1).await.unwrap();

        // Abort the upload
        storage.abort_multipart_upload(upload_id).await.unwrap();

        // Verify we can't list parts anymore
        let result = storage.list_parts(upload_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nested_object_keys() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "nested/path/to/test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get object
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, _) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, content);

        // List objects
        let objects = storage.list_objects(Some("nested/")).await.unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].0, key);
    }

    #[tokio::test]
    async fn test_empty_key() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get object
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, _) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, content);
    }
}
