/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Storage backends for the S3 Mock Server.
//!
//! This module provides the `StorageBackend` trait and its implementations.
//! The trait defines the interface for storing and retrieving both object data
//! and metadata, as well as managing multipart upload operations.

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures_util::TryStreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::pin::Pin;
use std::time::SystemTime;

use crate::bucket::BucketType;
use crate::storage::models::{MultipartUploadMetadata, ObjectMetadata, PartMetadata};
use crate::types::{ClientChecksums, ObjectIntegrity, ObjectIntegrityChecks, StoredObjectMetadata};
use crate::Result;

pub(crate) mod filesystem;
pub(crate) mod in_memory;
pub(crate) mod models;
#[cfg(test)]
mod tests;

/// Request for storing an object with all necessary metadata and options.
pub struct StoreObjectRequest {
    pub bucket: String,
    pub key: String,
    pub body: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>>,
    pub integrity_checks: ObjectIntegrityChecks,
    pub content_type: Option<String>,
    pub user_metadata: HashMap<String, String>,
    /// Override the stored last-modified timestamp. When `None`, backends use
    /// `SystemTime::now()`. This is only useful for the direct (non-S3-protocol)
    /// seeding API — the S3 PutObject operation never sets this.
    pub last_modified: Option<SystemTime>,
    pub storage_class: Option<String>,
    pub server_side_encryption: Option<String>,
    pub cache_control: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    /// Checksums the client sent with the body. A body that does not match them fails with
    /// [`Error::BadDigest`](crate::Error::BadDigest), and the key is left
    /// unchanged.
    pub client_checksums: Option<ClientChecksums>,
    /// Condition on the current state of the key, checked atomically with the write by
    /// backends that support it.
    pub precondition: WritePrecondition,
    /// When set, the quoted ETag stored instead of the MD5 of the body.
    pub etag: Option<String>,
    /// Restore status to store, for an object in an archival storage class.
    pub restore_status: Option<crate::storage::models::RestoreStatus>,
}

/// Condition on the current state of a key for a write to succeed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum WritePrecondition {
    /// Unconditional write.
    #[default]
    None,
    /// `If-Match`: the key must exist with this ETag, compared without quotes. A missing key
    /// fails with [`Error::NoSuchKey`](crate::Error::NoSuchKey).
    IfMatch(String),
    /// `If-None-Match: *`: the key must not exist.
    IfNoneMatchAny,
}

impl WritePrecondition {
    /// Check the precondition against the current metadata of the key.
    pub(crate) fn check(&self, current: Option<&ObjectMetadata>) -> Result<()> {
        match (self, current) {
            (WritePrecondition::None, _) | (WritePrecondition::IfNoneMatchAny, None) => Ok(()),
            (WritePrecondition::IfMatch(_), None) => Err(crate::Error::NoSuchKey),
            (WritePrecondition::IfMatch(etag), Some(metadata))
                if etags_match(etag, &metadata.etag) =>
            {
                Ok(())
            }
            (WritePrecondition::IfMatch(_), Some(_))
            | (WritePrecondition::IfNoneMatchAny, Some(_)) => Err(crate::Error::PreconditionFailed),
        }
    }
}

/// Check DeleteObject's `If-Match` against the current metadata of the key.
pub(crate) fn check_delete_if_match(
    if_match: &s3s::dto::ETagCondition,
    current: Option<&ObjectMetadata>,
) -> Result<()> {
    match current {
        None => Err(crate::Error::NoSuchKey),
        Some(metadata) if etag_condition_holds(if_match, &metadata.etag) => Ok(()),
        Some(_) => Err(crate::Error::PreconditionFailed),
    }
}

/// Whether two ETags are equal, ignoring surrounding quotes.
pub(crate) fn etags_match(a: &str, b: &str) -> bool {
    a.trim_matches('"') == b.trim_matches('"')
}

/// Whether an `If-Match` condition holds for an object with `etag`: `*` matches any object.
pub(crate) fn etag_condition_holds(condition: &s3s::dto::ETagCondition, etag: &str) -> bool {
    match condition {
        s3s::dto::ETagCondition::ETag(expected) => etags_match(expected.value(), etag),
        s3s::dto::ETagCondition::Any => true,
    }
}

/// The most parts an object can have, counting each append as one.
pub(crate) const MAX_PARTS: u32 = 10_000;

/// Request for appending to an existing object (PutObject with `x-amz-write-offset-bytes`).
pub(crate) struct AppendObjectRequest {
    pub bucket: String,
    pub key: String,
    /// Where the appended data must start: the current size of the object.
    pub offset: u64,
    /// The data to append. Its checksums have been validated against the request's.
    pub content: Bytes,
    /// `If-Match` (the ETag the object must have) or `If-None-Match: *` (the key must not exist).
    pub precondition: WritePrecondition,
    /// The algorithm of the request's checksum, if it has one.
    pub checksum_algorithm: Option<aws_smithy_checksums::ChecksumAlgorithm>,
}

/// An in-place change to the stored metadata of an object.
pub(crate) type MetadataUpdate = Box<dyn FnOnce(&mut ObjectMetadata) + Send>;

/// Conditions on a RenameObject request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RenamePreconditions {
    /// `If-Match` on the destination.
    pub if_match: Option<String>,
    /// `If-None-Match: *` on the destination.
    pub if_none_match_any: bool,
    /// `x-amz-rename-source-if-match` on the source.
    pub source_if_match: Option<String>,
}

/// Request for renaming an object (RenameObject).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenameRequest {
    pub src_key: String,
    pub dst_key: String,
    pub preconditions: RenamePreconditions,
    /// The `x-amz-client-token` that makes a retry of the rename idempotent.
    pub client_token: Option<String>,
}

impl RenamePreconditions {
    /// Check the conditions against the current source and destination. A missing source fails
    /// with `NoSuchKey` before any condition is evaluated. The ETag matches are checked next, the
    /// destination's `If-Match` and then the source's `x-amz-rename-source-if-match`, both
    /// reported as `If-Match` as S3 reports them, and `If-None-Match` last. `If-Match` fails on a
    /// missing destination.
    pub(crate) fn check(
        &self,
        source: Option<&ObjectMetadata>,
        destination: Option<&ObjectMetadata>,
    ) -> Result<()> {
        let source = source.ok_or(crate::Error::NoSuchKey)?;
        if let Some(etag) = &self.if_match {
            if !destination.is_some_and(|destination| etags_match(etag, &destination.etag)) {
                return Err(crate::Error::RenamePreconditionFailed("If-Match"));
            }
        }
        if let Some(etag) = &self.source_if_match {
            if !etags_match(etag, &source.etag) {
                return Err(crate::Error::RenamePreconditionFailed("If-Match"));
            }
        }
        if self.if_none_match_any && destination.is_some() {
            return Err(crate::Error::RenamePreconditionFailed("If-None-Match"));
        }
        Ok(())
    }
}

/// Request for retrieving an object.
pub(crate) struct GetObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub range: Option<Range<u64>>,
}

/// Response for retrieving an object.
pub(crate) struct GetObjectResponse {
    pub stream:
        Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin>,
    pub metadata: ObjectMetadata,
}

/// Request for listing objects.
pub(crate) struct ListObjectsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
}

/// Response for listing objects.
pub(crate) struct ListObjectsResponse {
    pub objects: Vec<ObjectInfo>,
}

/// Information about an object.
#[derive(Debug)]
pub(crate) struct ObjectInfo {
    pub key: String,
    pub metadata: ObjectMetadata,
}

/// An in-progress multipart upload.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MultipartUploadInfo {
    pub key: String,
    pub upload_id: String,
}

/// Information about a bucket.
pub(crate) struct BucketInfo {
    pub name: String,
    pub creation_date: SystemTime,
}

/// Request for creating a multipart upload.
pub(crate) struct CreateMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub metadata: ObjectMetadata,
    pub checksum_type: aws_sdk_s3::types::ChecksumType,
}

/// Request for uploading a part.
pub(crate) struct UploadPartRequest<'a> {
    pub upload_id: &'a str,
    pub part_number: i32,
    pub content: Bytes,
    /// The algorithms of the checksums the request carried.
    pub client_checksum_algorithms: Vec<aws_smithy_checksums::ChecksumAlgorithm>,
}

impl UploadPartRequest<'_> {
    /// Check that an UploadPart to an upload created with a checksum algorithm carries a checksum
    /// of that algorithm. It fails with 400 InvalidRequest:
    /// - with a checksum of another algorithm only, in either bucket type;
    /// - without a checksum, in a directory bucket, and in a general purpose bucket for the
    ///   COMPOSITE checksum type.
    pub(crate) fn check_checksum(&self, upload: &MultipartUploadMetadata) -> Result<()> {
        let Some(algorithm) = upload
            .metadata
            .checksum_algorithm
            .filter(|_| upload.metadata.part_checksums)
        else {
            return Ok(());
        };
        let directory = upload_in_directory_bucket(upload);
        let composite = upload.checksum_type == Some(aws_sdk_s3::types::ChecksumType::Composite);
        let actual = match self.client_checksum_algorithms.first() {
            _ if self.client_checksum_algorithms.contains(&algorithm) => return Ok(()),
            Some(other) => other.as_str(),
            None if directory => "None",
            None if composite => "null",
            None => return Ok(()),
        };
        Err(crate::Error::ChecksumTypeMismatch {
            expected: algorithm.as_str().to_owned(),
            actual: actual.to_owned(),
            bucket_type: if directory {
                BucketType::Directory
            } else {
                BucketType::GeneralPurpose
            },
        })
    }
}

/// Whether `upload` is to a directory bucket.
fn upload_in_directory_bucket(upload: &MultipartUploadMetadata) -> bool {
    upload
        .bucket
        .as_deref()
        .is_some_and(|bucket| BucketType::of(bucket) == BucketType::Directory)
}

/// Response for uploading a part.
#[derive(Debug)]
pub(crate) struct UploadPartResponse {
    pub etag: String,
    /// The part as stored, with its checksum in the algorithm of its upload, if the upload was
    /// created with a checksum algorithm.
    pub checksums: Option<PartMetadata>,
}

/// Information about a part.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct PartInfo {
    pub part_number: i32,
}

/// Request for completing a multipart upload.
pub(crate) struct CompleteMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub upload_id: &'a str,
    pub parts: Vec<(i32, String)>,
    pub client_checksums: Option<&'a ClientChecksums>,
    /// Lowercase name of the algorithm of the first part checksum the request lists, if it lists
    /// any.
    pub listed_part_checksum: Option<&'static str>,
}

impl CompleteMultipartUploadRequest<'_> {
    /// Check the part checksums a CompleteMultipartUpload lists. Listing one for an upload created
    /// without a checksum algorithm fails with 400 InvalidRequest in a directory bucket, and with
    /// 400 InvalidPart in a general purpose bucket.
    pub(crate) fn check_listed_part_checksums(
        &self,
        upload: &MultipartUploadMetadata,
    ) -> Result<()> {
        match self.listed_part_checksum {
            Some(algorithm) if !upload.metadata.part_checksums => {
                if upload_in_directory_bucket(upload) {
                    Err(crate::Error::ChecksumTypeMismatch {
                        expected: "None".to_owned(),
                        actual: algorithm.to_owned(),
                        bucket_type: BucketType::Directory,
                    })
                } else {
                    Err(crate::Error::InvalidPart)
                }
            }
            _ => Ok(()),
        }
    }
}

/// Response for completing a multipart upload.
#[derive(Debug)]
pub(crate) struct CompleteMultipartUploadResponse {
    pub key: String,
    pub etag: String,
    pub object_integrity: ObjectIntegrity,
}

impl StoreObjectRequest {
    pub(crate) fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        body: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>>,
        integrity_checks: ObjectIntegrityChecks,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            body,
            integrity_checks,
            content_type: None,
            user_metadata: HashMap::new(),
            last_modified: None,
            storage_class: None,
            server_side_encryption: None,
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            client_checksums: None,
            precondition: WritePrecondition::None,
            etag: None,
            restore_status: None,
        }
    }

    pub(crate) fn with_user_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.user_metadata = metadata;
        self
    }
}

impl From<s3s::dto::PutObjectInput> for StoreObjectRequest {
    fn from(input: s3s::dto::PutObjectInput) -> Self {
        // S3 supports only `If-Match` with an ETag and `If-None-Match: *` on writes; other
        // conditions are ignored.
        let precondition = match (&input.if_match, &input.if_none_match) {
            (Some(s3s::dto::ETagCondition::ETag(etag)), None) => {
                WritePrecondition::IfMatch(etag.value().to_owned())
            }
            (None, Some(s3s::dto::ETagCondition::Any)) => WritePrecondition::IfNoneMatchAny,
            _ => WritePrecondition::None,
        };
        let stream = input
            .body
            .unwrap_or_else(|| {
                let empty_stream = futures_util::stream::empty::<
                    std::result::Result<bytes::Bytes, std::io::Error>,
                >();
                s3s::dto::StreamingBlob::wrap(empty_stream)
            })
            .map_err(std::io::Error::other);

        Self {
            bucket: input.bucket,
            key: input.key,
            body: Box::pin(stream),
            integrity_checks: ObjectIntegrityChecks::new().with_md5(),
            content_type: input.content_type.map(|mime| mime.to_string()),
            user_metadata: input.metadata.unwrap_or_default(),
            last_modified: None,
            storage_class: input
                .storage_class
                .map(|s| Cow::<str>::from(s).into_owned()),
            server_side_encryption: input
                .server_side_encryption
                .map(|s| Cow::<str>::from(s).into_owned()),
            cache_control: input
                .cache_control
                .map(|s| Cow::<str>::from(s).into_owned()),
            content_encoding: input
                .content_encoding
                .map(|s| Cow::<str>::from(s).into_owned()),
            content_disposition: input
                .content_disposition
                .map(|s| Cow::<str>::from(s).into_owned()),
            content_language: input
                .content_language
                .map(|s| Cow::<str>::from(s).into_owned()),
            client_checksums: None,
            precondition,
            etag: None,
            restore_status: None,
        }
    }
}

/// A storage backend for the S3 Mock Server.
///
/// This trait defines the interface for storing and retrieving both object data
/// and metadata, as well as managing multipart upload operations. Storage backends
/// are responsible for all persistence concerns, allowing the S3 API layer to
/// remain stateless and focused on S3 semantics.
///
/// # Design Philosophy
///
/// The trait combines data and metadata operations to ensure consistency and
/// enable storage backends to manage their own persistence strategy. For example:
/// - In-memory storage can keep everything in RAM
/// - File-based storage can persist metadata alongside data files
///
/// # Object Operations
///
/// Objects are the primary entities in S3, consisting of both data and metadata.
/// All object operations work with complete objects, not just raw data.
///
/// # Multipart Upload Operations
///
/// Multipart uploads allow large objects to be uploaded in parts. The storage
/// backend manages the entire multipart upload lifecycle, including:
/// - Creating uploads with initial metadata
/// - Storing individual parts with their ETags
/// - Completing uploads by combining parts into final objects
/// - Aborting uploads and cleaning up partial data
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + Debug {
    // Object operations - combine data and metadata

    /// Store an object with integrity checking.
    async fn put_object(&self, request: StoreObjectRequest) -> Result<StoredObjectMetadata>;

    /// Retrieve an object as a stream with metadata, optionally with a byte range.
    ///
    /// This method is preferred for large objects as it avoids loading the entire
    /// object into memory. The stream yields chunks of bytes that can be processed
    /// incrementally.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `range` - Optional byte range to retrieve
    ///
    /// # Returns
    ///
    /// A stream of bytes and the object metadata, or None if the object doesn't exist
    async fn get_object(&self, request: GetObjectRequest<'_>) -> Result<Option<GetObjectResponse>>;

    /// Delete an object and its metadata.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;

    /// Delete an object only if its ETag matches `if_match`, compared without quotes, or
    /// `if_match` is `*` (DeleteObject with `If-Match`). Fails with `NoSuchKey` for a missing key and
    /// `PreconditionFailed` for a different ETag, deleting nothing. The default implementation
    /// checks the ETag and then deletes, which is not atomic.
    async fn delete_object_if_match(
        &self,
        bucket: &str,
        key: &str,
        if_match: &s3s::dto::ETagCondition,
    ) -> Result<()> {
        check_delete_if_match(if_match, self.head_object(bucket, key).await?.as_ref())?;
        self.delete_object(bucket, key).await
    }

    /// List all objects with a given prefix.
    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ListObjectsResponse>;

    /// Get object metadata without fetching the data.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMetadata>>;

    // Bucket operations

    /// Create a new bucket.
    async fn create_bucket(&self, bucket: &str) -> Result<()>;

    /// Delete a bucket. Fails with [`Error::BucketNotEmpty`](crate::Error::BucketNotEmpty) if the
    /// bucket has objects.
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;

    /// Check if a bucket exists.
    async fn head_bucket(&self, bucket: &str) -> Result<bool>;

    /// List all buckets, returning sorted bucket names and creation times.
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;

    // Multipart upload operations

    /// Create a new multipart upload.
    async fn create_multipart_upload(
        &self,
        request: CreateMultipartUploadRequest<'_>,
    ) -> Result<()>;

    /// Upload a part for a multipart upload.
    async fn upload_part(&self, request: UploadPartRequest<'_>) -> Result<UploadPartResponse>;

    /// List all parts for a multipart upload.
    #[allow(dead_code)]
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>>;

    /// Return the checksum type (FULL_OBJECT vs COMPOSITE) recorded when the
    /// multipart upload was created, or `Error::NoSuchUpload` if it does not exist.
    async fn multipart_checksum_type(
        &self,
        upload_id: &str,
    ) -> Result<Option<aws_sdk_s3::types::ChecksumType>>;

    /// Complete a multipart upload by combining parts into a final object.
    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUploadRequest<'_>,
    ) -> Result<CompleteMultipartUploadResponse>;

    /// Abort a multipart upload and clean up all associated data.
    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()>;

    /// Append to an existing object.
    ///
    /// Fails with:
    /// - `NoSuchKey` for a missing key;
    /// - `PreconditionFailed` when `If-Match` does not hold, or with `If-None-Match: *`;
    /// - `InvalidArgument` for an empty append to a non-empty object;
    /// - `InvalidWriteOffset` when the offset is not the object's size;
    /// - `TooManyParts` when the object already has [`MAX_PARTS`] parts;
    /// - `ChecksumTypeMismatch` when the object stores checksums, none of them of the request's
    ///   algorithm (CRC64NVME for a request without a checksum).
    async fn append_object(&self, _request: AppendObjectRequest) -> Result<StoredObjectMetadata> {
        Err(crate::Error::NotImplemented)
    }

    /// Move an object to a new key in the same bucket, replacing any object at the destination.
    ///
    /// A request with the client token of a rename that succeeded succeeds without renaming again
    /// if it is the same request, and fails with `IdempotentParameterMismatch` otherwise. Fails
    /// with `NoSuchKey` when the source does not exist, and with `RenamePreconditionFailed`,
    /// naming the condition header S3 reports, when a condition does not hold.
    async fn rename_object(&self, _bucket: &str, _request: RenameRequest) -> Result<()> {
        Err(crate::Error::NotImplemented)
    }

    /// The multipart uploads in progress in `bucket`, ordered by key and then upload id.
    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUploadInfo>>;

    /// Change the stored metadata of an object in place, leaving its data unchanged.
    ///
    /// Fails with `NoSuchBucket` or `NoSuchKey` when the object does not exist.
    async fn update_metadata(&self, bucket: &str, key: &str, update: MetadataUpdate) -> Result<()>;

    /// Reset all state, removing all buckets, objects, and in-flight uploads.
    async fn reset(&self) -> Result<()>;
}

// Implement the trait for Arc<dyn StorageBackend> to allow for dynamic dispatch
#[async_trait]
impl StorageBackend for std::sync::Arc<dyn StorageBackend + '_> {
    async fn put_object(&self, request: StoreObjectRequest) -> Result<StoredObjectMetadata> {
        (**self).put_object(request).await
    }

    async fn get_object(&self, request: GetObjectRequest<'_>) -> Result<Option<GetObjectResponse>> {
        (**self).get_object(request).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        (**self).delete_object(bucket, key).await
    }

    async fn delete_object_if_match(
        &self,
        bucket: &str,
        key: &str,
        if_match: &s3s::dto::ETagCondition,
    ) -> Result<()> {
        (**self).delete_object_if_match(bucket, key, if_match).await
    }

    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ListObjectsResponse> {
        (**self).list_objects(request).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMetadata>> {
        (**self).head_object(bucket, key).await
    }

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        (**self).create_bucket(bucket).await
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        (**self).delete_bucket(bucket).await
    }

    async fn head_bucket(&self, bucket: &str) -> Result<bool> {
        (**self).head_bucket(bucket).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        (**self).list_buckets().await
    }

    async fn create_multipart_upload(
        &self,
        request: CreateMultipartUploadRequest<'_>,
    ) -> Result<()> {
        (**self).create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPartRequest<'_>) -> Result<UploadPartResponse> {
        (**self).upload_part(request).await
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>> {
        (**self).list_parts(upload_id).await
    }

    async fn multipart_checksum_type(
        &self,
        upload_id: &str,
    ) -> Result<Option<aws_sdk_s3::types::ChecksumType>> {
        (**self).multipart_checksum_type(upload_id).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUploadRequest<'_>,
    ) -> Result<CompleteMultipartUploadResponse> {
        (**self).complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        (**self).abort_multipart_upload(upload_id).await
    }

    async fn append_object(&self, request: AppendObjectRequest) -> Result<StoredObjectMetadata> {
        (**self).append_object(request).await
    }

    async fn rename_object(&self, bucket: &str, request: RenameRequest) -> Result<()> {
        (**self).rename_object(bucket, request).await
    }

    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUploadInfo>> {
        (**self).list_multipart_uploads(bucket).await
    }

    async fn update_metadata(&self, bucket: &str, key: &str, update: MetadataUpdate) -> Result<()> {
        (**self).update_metadata(bucket, key, update).await
    }

    async fn reset(&self) -> Result<()> {
        (**self).reset().await
    }
}
