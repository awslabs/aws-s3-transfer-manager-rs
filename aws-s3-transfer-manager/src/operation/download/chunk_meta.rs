/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::operation::RequestIdExt;

/// Chunk Metadata, other than the body, that will be set from the `GetObject` request.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ChunkMetadata {
    /// <p>Indicates whether the object retrieved was (true) or was not (false) a Delete Marker. If false, this response header does not appear in the response.</p><note>
    /// <ul>
    /// <li>
    /// <p>If the current version of the object is a delete marker, Amazon S3 behaves as if the object was deleted and includes <code>x-amz-delete-marker: true</code> in the response.</p></li>
    /// <li>
    /// <p>If the specified version in the request is a delete marker, the response returns a <code>405 Method Not Allowed</code> error and the <code>Last-Modified: timestamp</code> response header.</p></li>
    /// </ul>
    /// </note>
    pub delete_marker: Option<bool>,
    /// <p>Indicates that a range of bytes was specified in the request.</p>
    pub accept_ranges: Option<String>,
    /// <p>If the object expiration is configured (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html"> <code>PutBucketLifecycleConfiguration</code> </a>), the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs providing object expiration information. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub expiration: Option<String>,
    /// <p>Provides information about object restoration action and expiration time of the restored object copy.</p><note>
    /// <p>This functionality is not supported for directory buckets. Only the S3 Express One Zone storage class is supported by directory buckets to store objects.</p>
    /// </note>
    pub restore: Option<String>,
    /// <p>Date and time when the object was last modified.</p>
    /// <p><b>General purpose buckets </b> - When you specify a <code>versionId</code> of the object in your request, if the specified version in the request is a delete marker, the response returns a <code>405 Method Not Allowed</code> error and the <code>Last-Modified: timestamp</code> response header.</p>
    pub last_modified: Option<::aws_smithy_types::DateTime>,
    /// <p>Size of the body in bytes.</p>
    pub content_length: Option<i64>,
    /// <p>An entity tag (ETag) is an opaque identifier assigned by a web server to a specific version of a resource found at a URL.</p>
    pub e_tag: Option<String>,
    /// <p>The base64-encoded, 32-bit CRC-32 checksum of the object. This will only be present if it was uploaded with the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_crc32: Option<String>,
    /// <p>The base64-encoded, 32-bit CRC-32C checksum of the object. This will only be present if it was uploaded with the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_crc32_c: Option<String>,
    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub checksum_crc64_nvme: Option<String>,
    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_sha1: Option<String>,
    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_sha256: Option<String>,
    /// <p>The checksum type, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. You can use this header response to verify that the checksum type that is received is the same checksum type that was specified in the <code>CreateMultipartUpload</code> request. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
    /// <p>This is set to the number of metadata entries not returned in the headers that are prefixed with <code>x-amz-meta-</code>. This can happen if you create metadata using an API like SOAP that supports more flexible metadata than the REST API. For example, using SOAP, you can create metadata whose values are not legal HTTP headers.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub missing_meta: Option<i32>,
    /// <p>Version ID of the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub version_id: Option<String>,
    /// <p>Specifies caching behavior along the request/reply chain.</p>
    pub cache_control: Option<String>,
    /// <p>Specifies presentational information for the object.</p>
    pub content_disposition: Option<String>,
    /// <p>Indicates what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.</p>
    pub content_encoding: Option<String>,
    /// <p>The language the content is in.</p>
    pub content_language: Option<String>,
    /// <p>The portion of the object returned in the response.</p>
    pub content_range: Option<String>,
    /// <p>A standard MIME type describing the format of the object data.</p>
    pub content_type: Option<String>,
    /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub website_redirect_location: Option<String>,
    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3.</p>
    pub server_side_encryption: Option<aws_sdk_s3::types::ServerSideEncryption>,
    /// <p>A map of metadata to store with the object in S3.</p>
    pub metadata: Option<::std::collections::HashMap<String, String>>,
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_algorithm: Option<String>,
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_key_md5: Option<String>,
    /// <p>If present, indicates the ID of the KMS key that was used for object encryption.</p>
    pub ssekms_key_id: Option<String>,
    /// <p>Indicates whether the object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p>
    pub bucket_key_enabled: Option<bool>,
    /// <p>Provides storage class information of the object. Amazon S3 returns this header for all objects except for S3 Standard storage class objects.</p><note>
    /// <p><b>Directory buckets </b> - Only the S3 Express One Zone storage class is supported by directory buckets to store objects.</p>
    /// </note>
    pub storage_class: Option<aws_sdk_s3::types::StorageClass>,
    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,
    /// <p>Amazon S3 can return this if your request involves a bucket that is either a source or destination in a replication rule.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub replication_status: Option<aws_sdk_s3::types::ReplicationStatus>,
    /// <p>The count of parts this object has. This value is only returned if you specify <code>partNumber</code> in your request and the object was uploaded as a multipart upload.</p>
    pub parts_count: Option<i32>,
    /// <p>The number of tags, if any, on the object, when you have the relevant permission to read object tags.</p>
    /// <p>You can use <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html">GetObjectTagging</a> to retrieve the tag set associated with an object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub tag_count: Option<i32>,
    /// <p>The Object Lock mode that's currently in place for this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub object_lock_mode: Option<aws_sdk_s3::types::ObjectLockMode>,
    /// <p>The date and time when this object's Object Lock will expire.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub object_lock_retain_until_date: Option<::aws_smithy_types::DateTime>,
    /// <p>Indicates whether this object has an active legal hold. This field is only returned if you have permission to view an object's legal hold status.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub object_lock_legal_hold_status: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,
    /// <p>The date and time at which the object is no longer cacheable.</p>
    pub expires_string: Option<String>,
    _request_id: Option<String>,
    _extended_request_id: Option<String>,
}

impl From<GetObjectOutput> for ChunkMetadata {
    fn from(value: GetObjectOutput) -> Self {
        Self {
            _request_id: value.request_id().map(|s| s.to_string()),
            _extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges,
            expiration: value.expiration,
            restore: value.restore,
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_crc64_nvme: value.checksum_crc64_nvme,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            checksum_type: value.checksum_type,
            missing_meta: value.missing_meta,
            version_id: value.version_id,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_range: value.content_range,
            content_type: value.content_type,
            website_redirect_location: value.website_redirect_location,
            server_side_encryption: value.server_side_encryption,
            metadata: value.metadata,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            ssekms_key_id: value.ssekms_key_id,
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class,
            request_charged: value.request_charged,
            replication_status: value.replication_status,
            parts_count: value.parts_count,
            tag_count: value.tag_count,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
            expires_string: value.expires_string,
        }
    }
}

impl RequestIdExt for ChunkMetadata {
    fn extended_request_id(&self) -> Option<&str> {
        self._extended_request_id.as_deref()
    }
}
impl RequestId for ChunkMetadata {
    fn request_id(&self) -> Option<&str> {
        self._request_id.as_deref()
    }
}

impl ::std::fmt::Debug for ChunkMetadata {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        let mut formatter = f.debug_struct("ChunkMetadata");
        formatter.field("delete_marker", &self.delete_marker);
        formatter.field("accept_ranges", &self.accept_ranges);
        formatter.field("expiration", &self.expiration);
        formatter.field("restore", &self.restore);
        formatter.field("last_modified", &self.last_modified);
        formatter.field("content_length", &self.content_length);
        formatter.field("e_tag", &self.e_tag);
        formatter.field("checksum_crc32", &self.checksum_crc32);
        formatter.field("checksum_crc32_c", &self.checksum_crc32_c);
        formatter.field("checksum_crc64_nvme", &self.checksum_crc64_nvme);
        formatter.field("checksum_sha1", &self.checksum_sha1);
        formatter.field("checksum_sha256", &self.checksum_sha256);
        formatter.field("checksum_type", &self.checksum_type);
        formatter.field("missing_meta", &self.missing_meta);
        formatter.field("version_id", &self.version_id);
        formatter.field("cache_control", &self.cache_control);
        formatter.field("content_disposition", &self.content_disposition);
        formatter.field("content_encoding", &self.content_encoding);
        formatter.field("content_language", &self.content_language);
        formatter.field("content_range", &self.content_range);
        formatter.field("content_type", &self.content_type);
        formatter.field("website_redirect_location", &self.website_redirect_location);
        formatter.field("server_side_encryption", &self.server_side_encryption);
        formatter.field("metadata", &self.metadata);
        formatter.field("sse_customer_algorithm", &self.sse_customer_algorithm);
        formatter.field("sse_customer_key_md5", &self.sse_customer_key_md5);
        formatter.field("ssekms_key_id", &"*** Sensitive Data Redacted ***");
        formatter.field("bucket_key_enabled", &self.bucket_key_enabled);
        formatter.field("storage_class", &self.storage_class);
        formatter.field("request_charged", &self.request_charged);
        formatter.field("replication_status", &self.replication_status);
        formatter.field("parts_count", &self.parts_count);
        formatter.field("tag_count", &self.tag_count);
        formatter.field("object_lock_mode", &self.object_lock_mode);
        formatter.field(
            "object_lock_retain_until_date",
            &self.object_lock_retain_until_date,
        );
        formatter.field(
            "object_lock_legal_hold_status",
            &self.object_lock_legal_hold_status,
        );
        formatter.field("expires_string", &self.expires_string);
        formatter.field("_extended_request_id", &self._extended_request_id);
        formatter.field("_request_id", &self._request_id);
        formatter.finish()
    }
}
