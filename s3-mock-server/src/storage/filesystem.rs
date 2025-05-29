/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Filesystem implementation of the StorageBackend trait.

use std::io::SeekFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::storage::StorageBackend;

/// A filesystem implementation of the StorageBackend trait.
///
/// This implementation stores objects and multipart uploads on the local filesystem,
/// making it suitable for testing and benchmarking with larger datasets.
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

    // Helper method to get the path for an object
    fn get_object_path(&self, key: &str) -> PathBuf {
        object_key_to_path(&self.objects_dir, key)
    }

    // Helper method to get the path for an upload
    fn get_upload_dir(&self, upload_id: &str) -> PathBuf {
        self.uploads_dir.join(upload_id)
    }

    // Helper method to get the path for a part
    fn get_part_path(&self, upload_id: &str, part_number: i32) -> PathBuf {
        self.get_upload_dir(upload_id)
            .join(format!("{}", part_number))
    }
}

#[async_trait]
impl StorageBackend for FilesystemStorage {
    async fn get_object_data(&self, key: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let path = self.get_object_path(key);

        // Check if the file exists
        if !path.exists() {
            return Err(Error::NoSuchKey);
        }

        // Open the file
        let mut file = fs::File::open(&path).await?;

        // Get file metadata to determine size
        let metadata = file.metadata().await?;
        let file_size = metadata.len();

        if let Some(range) = range {
            // Validate range
            if range.start >= file_size {
                return Err(Error::InvalidRange);
            }

            let end = range.end.min(file_size);
            if range.start > end {
                return Err(Error::InvalidRange);
            }

            // Seek to the start position
            file.seek(SeekFrom::Start(range.start)).await?;

            // Read the specified range
            let mut buffer = BytesMut::with_capacity((end - range.start) as usize);
            let mut chunk = vec![0; 8192]; // 8KB read buffer
            let mut bytes_read = 0;

            while bytes_read < (end - range.start) {
                let bytes_to_read =
                    ((end - range.start) - bytes_read).min(chunk.len() as u64) as usize;
                let n = file.read(&mut chunk[..bytes_to_read]).await?;
                if n == 0 {
                    break; // End of file
                }
                buffer.extend_from_slice(&chunk[..n]);
                bytes_read += n as u64;
            }

            Ok(buffer.freeze())
        } else {
            // Read the entire file
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await?;
            Ok(Bytes::from(buffer))
        }
    }

    async fn put_object_data(&self, key: &str, content: Bytes) -> Result<()> {
        let path = self.get_object_path(key);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write the content to the file
        let mut file = fs::File::create(&path).await?;
        file.write_all(&content).await?;
        file.flush().await?;

        Ok(())
    }

    async fn delete_object_data(&self, key: &str) -> Result<()> {
        let path = self.get_object_path(key);

        // Check if the file exists
        if !path.exists() {
            return Err(Error::NoSuchKey);
        }

        // Delete the file
        fs::remove_file(path).await?;

        Ok(())
    }

    async fn store_part_data(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);
        let part_path = self.get_part_path(upload_id, part_number);

        // Create upload directory if it doesn't exist
        fs::create_dir_all(&upload_dir).await?;

        // Write the part content to the file
        let mut file = fs::File::create(&part_path).await?;
        file.write_all(&content).await?;
        file.flush().await?;

        Ok(())
    }

    async fn get_part_data(&self, upload_id: &str, part_number: i32) -> Result<Bytes> {
        let part_path = self.get_part_path(upload_id, part_number);

        // Check if the upload directory exists
        let upload_dir = self.get_upload_dir(upload_id);
        if !upload_dir.exists() {
            return Err(Error::NoSuchUpload);
        }

        // Check if the part file exists
        if !part_path.exists() {
            return Err(Error::NoSuchPart);
        }

        // Read the part content
        let mut file = fs::File::open(&part_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        Ok(Bytes::from(buffer))
    }

    async fn complete_multipart_data(
        &self,
        upload_id: &str,
        key: &str,
        part_numbers: &[i32],
    ) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);

        // Check if the upload directory exists
        if !upload_dir.exists() {
            return Err(Error::NoSuchUpload);
        }

        // Create the destination file
        let dest_path = self.get_object_path(key);

        // Create parent directories if they don't exist
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut dest_file = fs::File::create(&dest_path).await?;

        // Combine all parts in the specified order
        for &part_number in part_numbers {
            let part_path = self.get_part_path(upload_id, part_number);

            // Check if the part file exists
            if !part_path.exists() {
                return Err(Error::NoSuchPart);
            }

            // Read the part content and append to the destination file
            let mut part_file = fs::File::open(&part_path).await?;
            let mut buffer = vec![0; 8192]; // 8KB buffer

            loop {
                let bytes_read = part_file.read(&mut buffer).await?;
                if bytes_read == 0 {
                    break;
                }
                dest_file.write_all(&buffer[..bytes_read]).await?;
            }
        }

        // Ensure all data is written to disk
        dest_file.flush().await?;

        // Clean up the upload directory
        fs::remove_dir_all(upload_dir).await?;

        Ok(())
    }

    async fn abort_multipart_data(&self, upload_id: &str) -> Result<()> {
        let upload_dir = self.get_upload_dir(upload_id);

        // Check if the upload directory exists
        if !upload_dir.exists() {
            return Err(Error::NoSuchUpload);
        }

        // Remove the upload directory and all its contents
        fs::remove_dir_all(upload_dir).await?;

        Ok(())
    }
}

/// Convert an S3 object key to a filesystem path.
///
/// This function handles special characters and ensures the path is valid
/// on the local filesystem.
///
/// # Arguments
///
/// * `base_dir` - The base directory for objects
/// * `key` - The S3 object key
///
/// # Returns
///
/// A PathBuf representing the local filesystem path for the object
fn object_key_to_path(base_dir: impl AsRef<Path>, key: &str) -> PathBuf {
    // Handle empty keys as a special case
    if key.is_empty() {
        return base_dir.as_ref().join("empty-key");
    }

    // Handle keys with leading slashes by removing them
    let key = key.trim_start_matches('/');

    // Join the key to the base directory
    base_dir.as_ref().join(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_object_path_formation() {
        let temp_dir = tempdir().unwrap();
        let base_dir = temp_dir.path().join("objects");

        // Test basic key
        let path = object_key_to_path(&base_dir, "simple-key");
        assert_eq!(path, base_dir.join("simple-key"));

        // Test nested key with slashes
        let path = object_key_to_path(&base_dir, "folder/subfolder/key");
        assert_eq!(path, base_dir.join("folder/subfolder/key"));

        // Test key with leading slash
        let path = object_key_to_path(&base_dir, "/leading-slash-key");
        assert_eq!(path, base_dir.join("leading-slash-key"));

        // Test empty key
        let path = object_key_to_path(&base_dir, "");
        assert_eq!(path, base_dir.join("empty-key"));

        // Test key with special characters
        let path = object_key_to_path(&base_dir, "key-with-special-chars!@#$%^&*()");
        assert_eq!(path, base_dir.join("key-with-special-chars!@#$%^&*()"));

        // Test key with spaces
        let path = object_key_to_path(&base_dir, "key with spaces");
        assert_eq!(path, base_dir.join("key with spaces"));

        // Test key with unicode characters
        let path = object_key_to_path(&base_dir, "unicode-key-😀-🚀");
        assert_eq!(path, base_dir.join("unicode-key-😀-🚀"));
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
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
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
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
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
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
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
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

        // Verify upload directory is removed
        let upload_dir = storage.get_upload_dir(upload_id);
        assert!(!upload_dir.exists());
    }

    #[tokio::test]
    async fn test_multipart_upload_missing_part() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
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
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let upload_id = "test-upload";

        // Store a part
        storage
            .store_part_data(upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();

        // Verify part exists
        let part_path = storage.get_part_path(upload_id, 1);
        assert!(part_path.exists());

        // Abort the upload
        storage.abort_multipart_data(upload_id).await.unwrap();

        // Verify upload directory is removed
        let upload_dir = storage.get_upload_dir(upload_id);
        assert!(!upload_dir.exists());

        // Try to get the part
        let result = storage.get_part_data(upload_id, 1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NoSuchUpload));
    }

    #[tokio::test]
    async fn test_nested_object_keys() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "folder/subfolder/test-key";
        let content = Bytes::from("test content");

        // Put object
        storage.put_object_data(key, content.clone()).await.unwrap();

        // Get object
        let retrieved = storage.get_object_data(key, None).await.unwrap();
        assert_eq!(retrieved, content);

        // Verify directory structure was created
        let object_path = storage.get_object_path(key);
        assert!(object_path.exists());
        assert!(object_path.parent().unwrap().exists());
    }

    #[tokio::test]
    async fn test_empty_key() {
        let temp_dir = tempdir().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
        let key = "";
        let content = Bytes::from("empty key content");

        // Put object with empty key
        storage.put_object_data(key, content.clone()).await.unwrap();

        // Get object with empty key
        let retrieved = storage.get_object_data(key, None).await.unwrap();
        assert_eq!(retrieved, content);

        // Verify file exists
        let object_path = storage.get_object_path(key);
        assert!(object_path.exists());
        assert_eq!(object_path, storage.objects_dir.join("empty-key"));
    }
}
