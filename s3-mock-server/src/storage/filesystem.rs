/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Filesystem implementation of the StorageBackend trait.

use std::io::SeekFrom;

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

// Helper function to extract part checksum based on algorithm
fn get_part_checksum<'a>(
    part_metadata: &'a PartMetadata,
    algorithm: &aws_smithy_checksums::ChecksumAlgorithm,
) -> Option<&'a String> {
    match algorithm {
        aws_smithy_checksums::ChecksumAlgorithm::Crc32 => part_metadata.crc32.as_ref(),
        aws_smithy_checksums::ChecksumAlgorithm::Crc32c => part_metadata.crc32c.as_ref(),
        aws_smithy_checksums::ChecksumAlgorithm::Sha1 => part_metadata.sha1.as_ref(),
        aws_smithy_checksums::ChecksumAlgorithm::Sha256 => part_metadata.sha256.as_ref(),
        _ => None,
    }
}

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
/// ├── my-bucket/
/// │   ├── objects/
/// │   │   ├── my-file.txt              # Object data
/// │   │   └── my-file.txt.metadata     # Object metadata (JSON)
/// │   └── uploads/
/// │       ├── upload-123/
/// │       │   ├── metadata.json        # Upload metadata
/// │       │   ├── part-1.dat          # Part data
/// │       │   └── part-1.metadata     # Part metadata
/// │       └── ...
/// ```
#[derive(Debug)]
pub(crate) struct FilesystemStorage {
    root_dir: PathBuf,
}

impl FilesystemStorage {
    /// Create a new filesystem storage backend.
    pub(crate) async fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir).await?;
        Ok(Self { root_dir })
    }

    fn objects_dir(&self, bucket: &str) -> PathBuf {
        self.root_dir.join(bucket).join("objects")
    }

    /// Ensure bucket directories exist (auto-create for backward compat).
    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        fs::create_dir_all(self.objects_dir(bucket)).await?;
        fs::create_dir_all(self.root_dir.join("uploads")).await?;
        Ok(())
    }

    fn get_object_path(&self, bucket: &str, key: &str) -> PathBuf {
        if key.is_empty() {
            return self.objects_dir(bucket).join("empty_key");
        }
        object_key_to_path(&self.objects_dir(bucket), key)
    }

    fn get_object_metadata_path(&self, bucket: &str, key: &str) -> PathBuf {
        if key.is_empty() {
            return self.objects_dir(bucket).join("empty_key.metadata");
        }
        object_key_to_path(&self.objects_dir(bucket), &format!("{}.metadata", key))
    }

    fn get_upload_dir(&self, upload_id: &str) -> PathBuf {
        self.root_dir.join("uploads").join(upload_id)
    }

    fn get_upload_metadata_path(&self, upload_id: &str) -> PathBuf {
        self.get_upload_dir(upload_id).join("metadata.json")
    }

    fn get_part_path(&self, upload_id: &str, part_number: i32) -> PathBuf {
        self.get_upload_dir(upload_id)
            .join(format!("part-{}.dat", part_number))
    }

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

    fn list_directory<'a>(
        &'a self,
        dir: &'a Path,
        objects_dir: &'a Path,
        prefix: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<PathBuf>>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut entries = Vec::new();
            let mut read_dir = match fs::read_dir(dir).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = read_dir.next_entry().await? {
                let path = entry.path();
                let metadata = fs::metadata(&path).await?;

                if metadata.is_dir() {
                    let mut sub_entries = self.list_directory(&path, objects_dir, prefix).await?;
                    entries.append(&mut sub_entries);
                } else if path.extension().is_none_or(|ext| ext != "metadata") {
                    if let Some(key) = path_to_object_key(objects_dir, &path) {
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
        // Auto-create bucket directories
        self.ensure_bucket(&request.bucket).await?;

        let mut body = request.body;
        let mut integrity_checks = request.integrity_checks;
        let path = self.get_object_path(&request.bucket, &request.key);
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
        let last_modified = request.last_modified.unwrap_or_else(SystemTime::now);

        let metadata = ObjectMetadata {
            content_type: request.content_type,
            content_length,
            etag: object_integrity.etag().unwrap_or_default(),
            last_modified,
            user_metadata: request.user_metadata,
            checksum_algorithm: None,
            crc32: object_integrity.crc32.clone(),
            crc32c: object_integrity.crc32c.clone(),
            crc64nvme: object_integrity.crc64nvme.clone(),
            sha1: object_integrity.sha1.clone(),
            sha256: object_integrity.sha256.clone(),
            storage_class: request.storage_class,
            server_side_encryption: request.server_side_encryption,
            cache_control: request.cache_control,
            content_encoding: request.content_encoding,
            content_disposition: request.content_disposition,
            content_language: request.content_language,
        };
        let metadata_path = self.get_object_metadata_path(&request.bucket, &request.key);
        Self::save_metadata(&metadata_path, &metadata).await?;

        Ok(StoredObjectMetadata { object_integrity })
    }

    async fn get_object(
        &self,
        request: crate::storage::GetObjectRequest<'_>,
    ) -> Result<Option<crate::storage::GetObjectResponse>> {
        let path = self.get_object_path(request.bucket, request.key);
        let metadata_path = self.get_object_metadata_path(request.bucket, request.key);

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
        let (content_length, _seek_position) = if let Some(ref range) = request.range {
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
        > = if request.range.is_some() {
            // Create a stream that limits the total bytes read
            Box::new(LimitedStream::new(reader_stream, content_length))
        } else {
            Box::new(reader_stream)
        };

        // Clear checksums for range requests since they apply to full object
        let mut response_metadata = metadata;
        if request.range.is_some() {
            response_metadata.clear_checksums();
        }

        Ok(Some(crate::storage::GetObjectResponse {
            stream: limited_stream,
            metadata: response_metadata,
        }))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let path = self.get_object_path(bucket, key);
        let metadata_path = self.get_object_metadata_path(bucket, key);

        // DeleteObject is idempotent: deleting a non-existent key is not an error.
        match fs::remove_file(&path).await {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(Error::Io(e)),
        }

        // Try to delete metadata, but don't error if it's already gone
        let _ = fs::remove_file(metadata_path).await;

        Ok(())
    }

    async fn list_objects(
        &self,
        request: crate::storage::ListObjectsRequest<'_>,
    ) -> Result<crate::storage::ListObjectsResponse> {
        let mut matching_objects = Vec::new();
        let objects_dir = self.objects_dir(request.bucket);

        let entries = self
            .list_directory(&objects_dir, &objects_dir, request.prefix)
            .await?;

        for path in entries {
            if let Some(key) = path_to_object_key(&objects_dir, &path) {
                let metadata_path = self.get_object_metadata_path(request.bucket, &key);
                if let Some(metadata) = Self::load_metadata(&metadata_path).await? {
                    matching_objects.push(crate::storage::ObjectInfo { key, metadata });
                }
            }
        }

        Ok(crate::storage::ListObjectsResponse {
            objects: matching_objects,
        })
    }

    async fn create_multipart_upload(
        &self,
        request: crate::storage::CreateMultipartUploadRequest<'_>,
    ) -> Result<()> {
        self.ensure_bucket(request.bucket).await?;
        let upload_dir = self.get_upload_dir(request.upload_id);
        fs::create_dir_all(&upload_dir).await?;

        let upload_metadata = MultipartUploadMetadata {
            key: request.key.to_string(),
            upload_id: request.upload_id.to_string(),
            metadata: request.metadata,
            parts: Default::default(),
            checksum_type: Some(request.checksum_type),
            bucket: Some(request.bucket.to_string()),
        };

        let metadata_path = self.get_upload_metadata_path(request.upload_id);
        Self::save_metadata(&metadata_path, &upload_metadata).await?;

        Ok(())
    }

    async fn upload_part(
        &self,
        request: crate::storage::UploadPartRequest<'_>,
    ) -> Result<crate::storage::UploadPartResponse> {
        let metadata_path = self.get_upload_metadata_path(request.upload_id);
        let mut upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        let checksum_algorithm = upload_metadata.metadata.checksum_algorithm;

        // Calculate ETag
        let etag = format!("\"{:x}\"", md5::compute(&request.content));

        // Calculate part checksums if algorithm is specified
        let mut part_metadata = PartMetadata {
            etag: etag.clone(),
            size: request.content.len() as u64,
            ..Default::default()
        };

        if let Some(algorithm) = checksum_algorithm {
            use crate::types::ObjectIntegrityChecks;

            // Calculate checksum for the specified algorithm
            let mut integrity_checks =
                ObjectIntegrityChecks::new().with_checksum_algorithm(algorithm);
            integrity_checks.update(&request.content);
            let calculated_integrity = integrity_checks.finalize();

            // Store the calculated checksum in part metadata
            match algorithm {
                aws_smithy_checksums::ChecksumAlgorithm::Crc32 => {
                    part_metadata.crc32 = calculated_integrity.crc32;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Crc32c => {
                    part_metadata.crc32c = calculated_integrity.crc32c;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Crc64Nvme => {
                    part_metadata.crc64nvme = calculated_integrity.crc64nvme;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Sha1 => {
                    part_metadata.sha1 = calculated_integrity.sha1;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Sha256 => {
                    part_metadata.sha256 = calculated_integrity.sha256;
                }
                _ => {} // Ignore unsupported algorithms
            }
        }

        // Save the part data
        let part_path = self.get_part_path(request.upload_id, request.part_number);
        if let Some(parent) = part_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&part_path, &request.content).await?;

        // Save the part metadata
        let part_metadata_path =
            self.get_part_metadata_path(request.upload_id, request.part_number);
        Self::save_metadata(&part_metadata_path, &part_metadata).await?;

        // Update the upload metadata
        upload_metadata
            .parts
            .insert(request.part_number, part_metadata);
        Self::save_metadata(&metadata_path, &upload_metadata).await?;

        Ok(crate::storage::UploadPartResponse { etag })
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<crate::storage::PartInfo>> {
        let metadata_path = self.get_upload_metadata_path(upload_id);
        let upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        let mut result: Vec<_> = upload_metadata
            .parts
            .iter()
            .map(|(&part_number, _part_metadata)| crate::storage::PartInfo { part_number })
            .collect();

        // Sort by part number for consistent ordering
        result.sort_by_key(|part| part.part_number);
        Ok(result)
    }

    async fn complete_multipart_upload(
        &self,
        request: crate::storage::CompleteMultipartUploadRequest<'_>,
    ) -> Result<crate::storage::CompleteMultipartUploadResponse> {
        // Load the upload metadata
        let metadata_path = self.get_upload_metadata_path(request.upload_id);
        let upload_metadata: MultipartUploadMetadata = Self::load_metadata(&metadata_path)
            .await?
            .ok_or(Error::NoSuchUpload)?;

        let bucket = upload_metadata.bucket.as_deref().unwrap_or(request.bucket);
        let checksum_algorithm = upload_metadata.metadata.checksum_algorithm;
        let checksum_type = upload_metadata.checksum_type;

        // Verify all parts exist and ETags match, collect metadata
        let mut total_size = 0u64;
        let mut etags = Vec::new();
        let mut part_metadata_list = Vec::new();

        for (part_number, expected_etag) in &request.parts {
            let part_metadata_path = self.get_part_metadata_path(request.upload_id, *part_number);
            let part_metadata: PartMetadata = Self::load_metadata(&part_metadata_path)
                .await?
                .ok_or(Error::NoSuchPart)?;

            if part_metadata.etag != *expected_etag {
                return Err(Error::InvalidPart);
            }

            total_size += part_metadata.size;
            etags.push(part_metadata.etag.clone());
            part_metadata_list.push((*part_number, part_metadata));
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

        // Calculate checksums if algorithm is specified
        let mut combined = BytesMut::new();
        let mut integrity_checks = checksum_algorithm
            .as_ref()
            .map(|algorithm| {
                crate::types::ObjectIntegrityChecks::new().with_checksum_algorithm(*algorithm)
            })
            .unwrap_or_default();

        for (part_number, part_metadata) in &part_metadata_list {
            let part_path = self.get_part_path(request.upload_id, *part_number);
            let part_data = fs::read(&part_path).await?;

            // Always assemble the final object
            combined.extend_from_slice(&part_data);

            // Update checksums based on type
            match checksum_type {
                Some(aws_sdk_s3::types::ChecksumType::FullObject) => {
                    integrity_checks.update(&part_data);
                }
                Some(aws_sdk_s3::types::ChecksumType::Composite) => {
                    if let Some(algorithm) = checksum_algorithm.as_ref() {
                        if let Some(part_checksum) = get_part_checksum(part_metadata, algorithm) {
                            integrity_checks.update(part_checksum.as_bytes());
                        }
                    }
                }
                None => {} // No checksum calculation
                Some(_) => {
                    // Future checksum types - default to composite behavior
                    if let Some(algorithm) = checksum_algorithm.as_ref() {
                        if let Some(part_checksum) = get_part_checksum(part_metadata, algorithm) {
                            integrity_checks.update(part_checksum.as_bytes());
                        }
                    }
                }
            }
        }

        // Finalize checksum calculation
        let object_integrity = integrity_checks.finalize();

        // Validate against client-provided checksum if present
        if let Some(client_checksums) = request.client_checksums {
            if let Err(error_msg) = object_integrity.validate(client_checksums) {
                return Err(Error::ChecksumMismatch(error_msg.to_string()));
            }
        }

        // Update the final metadata
        let mut final_metadata = upload_metadata.metadata;
        final_metadata.content_length = total_size;
        final_metadata.etag = combined_etag.clone();
        final_metadata.last_modified = SystemTime::now();

        // Store calculated checksums in metadata
        final_metadata.crc32 = object_integrity.crc32.clone();
        final_metadata.crc32c = object_integrity.crc32c.clone();
        final_metadata.sha1 = object_integrity.sha1.clone();
        final_metadata.sha256 = object_integrity.sha256.clone();
        final_metadata.crc64nvme = object_integrity.crc64nvme.clone();

        // Save the final object
        let combined_data = combined.freeze();
        let object_path = self.get_object_path(bucket, &upload_metadata.key);
        let obj_metadata_path = self.get_object_metadata_path(bucket, &upload_metadata.key);

        // Ensure parent directory exists
        if let Some(parent) = object_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write object data and metadata
        fs::write(&object_path, &combined_data).await?;
        let metadata_json = serde_json::to_string_pretty(&final_metadata)
            .map_err(|e| Error::Internal(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&obj_metadata_path, metadata_json).await?;

        // Clean up the multipart upload
        let _ = fs::remove_dir_all(self.get_upload_dir(request.upload_id)).await;

        Ok(crate::storage::CompleteMultipartUploadResponse {
            key: upload_metadata.key.clone(),
            etag: combined_etag,
            object_integrity,
        })
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMetadata>> {
        let metadata_path = self.get_object_metadata_path(bucket, key);
        Self::load_metadata(&metadata_path).await
    }

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.ensure_bucket(bucket).await
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let bucket_dir = self.root_dir.join(bucket);
        if !bucket_dir.exists() {
            return Err(Error::NoSuchBucket);
        }
        // Check if objects dir has any entries
        let objects_dir = self.objects_dir(bucket);
        if objects_dir.exists() {
            let mut rd = fs::read_dir(&objects_dir).await?;
            if rd.next_entry().await?.is_some() {
                return Err(Error::Internal("bucket is not empty".to_string()));
            }
        }
        fs::remove_dir_all(&bucket_dir).await?;
        Ok(())
    }

    async fn head_bucket(&self, bucket: &str) -> Result<bool> {
        Ok(self.root_dir.join(bucket).exists())
    }

    async fn list_buckets(&self) -> Result<Vec<crate::storage::BucketInfo>> {
        let mut result = Vec::new();
        let mut rd = fs::read_dir(&self.root_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                // Skip the global uploads directory
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name != "uploads" {
                        let creation_date = fs::metadata(&path)
                            .await
                            .and_then(|m| m.created().or_else(|_| m.modified()))
                            .unwrap_or(SystemTime::UNIX_EPOCH);
                        result.push(crate::storage::BucketInfo {
                            name: name.to_string(),
                            creation_date,
                        });
                    }
                }
            }
        }
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);

        let metadata_path = self.get_upload_metadata_path(upload_id);
        if !metadata_path.exists() {
            return Err(Error::NoSuchUpload);
        }

        // Remove the entire upload directory
        fs::remove_dir_all(upload_dir).await?;

        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        // Remove everything under root and recreate it
        fs::remove_dir_all(&self.root_dir).await?;
        fs::create_dir_all(&self.root_dir).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObjectIntegrityChecks;
    use futures::StreamExt;
    use std::collections::HashMap;
    use tempfile::tempdir;

    const TEST_BUCKET: &str = "test-bucket";

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
            ..Default::default()
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

        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                TEST_BUCKET,
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_content = collect_stream_data(response.stream).await;
        assert_eq!(retrieved_content, content);
        assert_eq!(response.metadata.content_length, content.len() as u64);
        assert_eq!(response.metadata.content_type, None);
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "test-key";
        let content = Bytes::from("0123456789");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                TEST_BUCKET,
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: Some(2..5),
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_content = collect_stream_data(response.stream).await;
        assert_eq!(retrieved_content, Bytes::from("234"));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                TEST_BUCKET,
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());

        storage.delete_object(TEST_BUCKET, key).await.unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let content = Bytes::from("test content");

        for i in 0..3 {
            let key = format!("test-key-{}", i);
            let integrity_checks = ObjectIntegrityChecks::new().with_md5();
            let stream = bytes_to_stream(content.clone());
            storage
                .put_object(crate::storage::StoreObjectRequest::new(
                    TEST_BUCKET,
                    &key,
                    stream,
                    integrity_checks,
                ))
                .await
                .unwrap();
        }

        let request = crate::storage::ListObjectsRequest {
            bucket: TEST_BUCKET,
            prefix: None,
        };
        let objects = storage.list_objects(request).await.unwrap();
        assert_eq!(objects.objects.len(), 3);

        let request = crate::storage::ListObjectsRequest {
            bucket: TEST_BUCKET,
            prefix: Some("test-key-1"),
        };
        let objects = storage.list_objects(request).await.unwrap();
        assert_eq!(objects.objects.len(), 1);
        assert_eq!(objects.objects[0].key, "test-key-1");
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        let request = crate::storage::CreateMultipartUploadRequest {
            bucket: TEST_BUCKET,
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        let part1 = Bytes::from("part1");
        let part2 = Bytes::from("part2");

        let request1 = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1.clone(),
        };
        let etag1 = storage.upload_part(request1).await.unwrap();
        let request2 = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 2,
            content: part2.clone(),
        };
        let etag2 = storage.upload_part(request2).await.unwrap();

        let parts = storage.list_parts(upload_id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);

        let parts_to_complete = vec![(1, etag1.etag), (2, etag2.etag)];
        let request = crate::storage::CompleteMultipartUploadRequest {
            bucket: TEST_BUCKET,
            upload_id,
            parts: parts_to_complete,
            client_checksums: None,
        };
        let response = storage.complete_multipart_upload(request).await.unwrap();

        assert_eq!(response.key, key);

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        assert_eq!(
            response.metadata.content_length,
            (part1.len() + part2.len()) as u64
        );
        let final_content = collect_stream_data(response.stream).await;
        assert_eq!(final_content, Bytes::from("part1part2"));
    }

    #[tokio::test]
    async fn test_multipart_upload_missing_part() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        let request = crate::storage::CreateMultipartUploadRequest {
            bucket: TEST_BUCKET,
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        let part1 = Bytes::from("part1");
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1,
        };
        let etag1 = storage.upload_part(request).await.unwrap();

        let parts_to_complete = vec![(1, etag1.etag), (2, "missing-etag".to_string())];
        let request = crate::storage::CompleteMultipartUploadRequest {
            bucket: TEST_BUCKET,
            upload_id,
            parts: parts_to_complete,
            client_checksums: None,
        };
        let result = storage.complete_multipart_upload(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        let request = crate::storage::CreateMultipartUploadRequest {
            bucket: TEST_BUCKET,
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        let part1 = Bytes::from("part1");
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1,
        };
        storage.upload_part(request).await.unwrap();

        storage.abort_multipart_upload(upload_id).await.unwrap();

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

        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                TEST_BUCKET,
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_content = collect_stream_data(response.stream).await;
        assert_eq!(retrieved_content, content);

        let request = crate::storage::ListObjectsRequest {
            bucket: TEST_BUCKET,
            prefix: Some("nested/"),
        };
        let objects = storage.list_objects(request).await.unwrap();
        assert_eq!(objects.objects.len(), 1);
        assert_eq!(objects.objects[0].key, key);
    }

    #[tokio::test]
    async fn test_empty_key() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                TEST_BUCKET,
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        let request = crate::storage::GetObjectRequest {
            bucket: TEST_BUCKET,
            key,
            range: None,
        };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_content = collect_stream_data(response.stream).await;
        assert_eq!(retrieved_content, content);
    }
}
