/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::{
    complete_multipart_upload::CompleteMultipartUploadOutput,
    create_multipart_upload::CreateMultipartUploadOutput, put_object::PutObjectOutput,
};
use std::fmt::{Debug, Formatter};

/// Common response fields for uploading an object to Amazon S3
#[non_exhaustive]
#[derive(Clone, PartialEq)]
pub struct UploadOutput {
    /// <p>If the expiration is configured for the object (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration</a>) in the <i>Amazon S3 User Guide</i>, the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs that provide information about object expiration. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub expiration: Option<String>,

    /// <p>Entity tag for the uploaded object.</p>
    /// <p><b>General purpose buckets </b> - To ensure that data is not corrupted traversing the network, for objects where the ETag is the MD5 digest of the object, you can calculate the MD5 while putting an object to Amazon S3 and compare the returned ETag to the calculated MD5 value.</p>
    /// <p><b>Directory buckets </b> - The ETag for the object in a directory bucket isn't the MD5 digest of the object.</p>
    pub e_tag: Option<String>,

    /// <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_crc32: Option<String>,

    /// <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_crc32_c: Option<String>,

    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. This header is present if the object was uploaded with the CRC64NVME checksum algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default checksum, CRC64NVME, to the uploaded object). For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub checksum_crc64_nvme: Option<String>,

    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. When you use the API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_sha1: Option<String>,

    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub checksum_sha256: Option<String>,

    /// <p>This header specifies the checksum type of the object, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. For <code>PutObject</code> uploads, the checksum type is always <code>FULL_OBJECT</code>. You can use this header as a data integrity check to verify that the checksum type that is received is the same checksum that was specified. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub checksum_type: Option<aws_sdk_s3::types::ChecksumType>,

    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p><note>
    /// <p>For directory buckets, only server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) is supported.</p>
    /// </note>
    pub server_side_encryption: Option<aws_sdk_s3::types::ServerSideEncryption>,

    /// <p>Version ID of the object.</p>
    /// <p>If you enable versioning for a bucket, Amazon S3 automatically generates a unique version ID for the object being stored. Amazon S3 returns this ID in the response. When you enable versioning for a bucket, if Amazon S3 receives multiple write requests for the same object simultaneously, it stores all of the objects. For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/AddingObjectstoVersioningEnabledBuckets.html">Adding Objects to Versioning-Enabled Buckets</a> in the <i>Amazon S3 User Guide</i>. For information about returning the versioning state of a bucket, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub version_id: Option<String>,

    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_algorithm: Option<String>,

    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_key_md5: Option<String>,

    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header indicates the ID of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_kms_key_id: Option<String>,

    /// <p>If present, indicates the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_kms_encryption_context: Option<String>,

    /// <p>Indicates whether the uploaded object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub bucket_key_enabled: Option<bool>,

    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,

    /// <p>ID for the initiated multipart upload.</p>
    /// This will not be set for requests that are not split into multipart uploads.
    pub upload_id: Option<String>,
}

impl UploadOutput {
    /// <p>If the expiration is configured for the object (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration</a>) in the <i>Amazon S3 User Guide</i>, the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs that provide information about object expiration. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn expiration(&self) -> Option<&str> {
        self.expiration.as_deref()
    }
    /// <p>Entity tag for the uploaded object.</p>
    /// <p><b>General purpose buckets </b> - To ensure that data is not corrupted traversing the network, for objects where the ETag is the MD5 digest of the object, you can calculate the MD5 while putting an object to Amazon S3 and compare the returned ETag to the calculated MD5 value.</p>
    /// <p><b>Directory buckets </b> - The ETag for the object in a directory bucket isn't the MD5 digest of the object.</p>
    pub fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }
    /// <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32(&self) -> Option<&str> {
        self.checksum_crc32.as_deref()
    }
    /// <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32_c(&self) -> Option<&str> {
        self.checksum_crc32_c.as_deref()
    }
    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. This header is present if the object was uploaded with the CRC64NVME checksum algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default checksum, CRC64NVME, to the uploaded object). For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn checksum_crc64_nvme(&self) -> Option<&str> {
        self.checksum_crc64_nvme.as_deref()
    }
    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. When you use the API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha1(&self) -> Option<&str> {
        self.checksum_sha1.as_deref()
    }
    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha256(&self) -> Option<&str> {
        self.checksum_sha256.as_deref()
    }
    /// <p>This header specifies the checksum type of the object, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. For <code>PutObject</code> uploads, the checksum type is always <code>FULL_OBJECT</code>. You can use this header as a data integrity check to verify that the checksum type that is received is the same checksum that was specified. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn checksum_type(&self) -> Option<&aws_sdk_s3::types::ChecksumType> {
        self.checksum_type.as_ref()
    }
    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p><note>
    /// <p>For directory buckets, only server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) is supported.</p>
    /// </note>
    pub fn server_side_encryption(&self) -> Option<&aws_sdk_s3::types::ServerSideEncryption> {
        self.server_side_encryption.as_ref()
    }
    /// <p>Version ID of the object.</p>
    /// <p>If you enable versioning for a bucket, Amazon S3 automatically generates a unique version ID for the object being stored. Amazon S3 returns this ID in the response. When you enable versioning for a bucket, if Amazon S3 receives multiple write requests for the same object simultaneously, it stores all of the objects. For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/AddingObjectstoVersioningEnabledBuckets.html">Adding Objects to Versioning-Enabled Buckets</a> in the <i>Amazon S3 User Guide</i>. For information about returning the versioning state of a bucket, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_algorithm(&self) -> Option<&str> {
        self.sse_customer_algorithm.as_deref()
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key_md5(&self) -> Option<&str> {
        self.sse_customer_key_md5.as_deref()
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header indicates the ID of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_key_id(&self) -> Option<&str> {
        self.sse_kms_key_id.as_deref()
    }
    /// <p>If present, indicates the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_encryption_context(&self) -> Option<&str> {
        self.sse_kms_encryption_context.as_deref()
    }
    /// <p>Indicates whether the uploaded object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn bucket_key_enabled(&self) -> Option<bool> {
        self.bucket_key_enabled
    }
    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn request_charged(&self) -> Option<&aws_sdk_s3::types::RequestCharged> {
        self.request_charged.as_ref()
    }

    /// <p>ID for the initiated multipart upload.</p>
    /// This will not be set for requests that are not split into multipart uploads.
    pub fn upload_id(&self) -> Option<&String> {
        self.upload_id.as_ref()
    }
}

impl Debug for UploadOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("UploadOutput");
        formatter.field("expiration", &self.expiration);
        formatter.field("e_tag", &self.e_tag);
        formatter.field("checksum_crc32", &self.checksum_crc32);
        formatter.field("checksum_crc32_c", &self.checksum_crc32_c);
        formatter.field("checksum_crc64_nvme", &self.checksum_crc64_nvme);
        formatter.field("checksum_sha1", &self.checksum_sha1);
        formatter.field("checksum_sha256", &self.checksum_sha256);
        formatter.field("checksum_type", &self.checksum_type);
        formatter.field("server_side_encryption", &self.server_side_encryption);
        formatter.field("version_id", &self.version_id);
        formatter.field("sse_customer_algorithm", &self.sse_customer_algorithm);
        formatter.field("sse_customer_key_md5", &self.sse_customer_key_md5);
        formatter.field("sse_kms_key_id", &"*** Sensitive Data Redacted ***");
        formatter.field(
            "sse_kms_encryption_context",
            &"*** Sensitive Data Redacted ***",
        );
        formatter.field("bucket_key_enabled", &self.bucket_key_enabled);
        formatter.field("request_charged", &self.request_charged);
        formatter.field("upload_id", &self.upload_id);
        formatter.finish()
    }
}

/// A builder for [`UploadOutput`].
#[non_exhaustive]
#[derive(Default)]
pub struct UploadOutputBuilder {
    pub(crate) expiration: Option<String>,
    pub(crate) e_tag: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32_c: Option<String>,
    pub(crate) checksum_crc64_nvme: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
    pub(crate) server_side_encryption: Option<aws_sdk_s3::types::ServerSideEncryption>,
    pub(crate) version_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) sse_kms_key_id: Option<String>,
    pub(crate) sse_kms_encryption_context: Option<String>,
    pub(crate) bucket_key_enabled: Option<bool>,
    pub(crate) request_charged: Option<aws_sdk_s3::types::RequestCharged>,
    pub(crate) upload_id: Option<String>,
}

impl UploadOutputBuilder {
    /// <p>If the expiration is configured for the object (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration</a>) in the <i>Amazon S3 User Guide</i>, the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs that provide information about object expiration. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn expiration(mut self, input: impl Into<String>) -> Self {
        self.expiration = Some(input.into());
        self
    }
    /// <p>If the expiration is configured for the object (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration</a>) in the <i>Amazon S3 User Guide</i>, the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs that provide information about object expiration. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_expiration(mut self, input: Option<String>) -> Self {
        self.expiration = input;
        self
    }
    /// <p>If the expiration is configured for the object (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration</a>) in the <i>Amazon S3 User Guide</i>, the response includes this header. It includes the <code>expiry-date</code> and <code>rule-id</code> key-value pairs that provide information about object expiration. The value of the <code>rule-id</code> is URL-encoded.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_expiration(&self) -> Option<&str> {
        self.expiration.as_deref()
    }
    /// <p>Entity tag for the uploaded object.</p>
    /// <p><b>General purpose buckets </b> - To ensure that data is not corrupted traversing the network, for objects where the ETag is the MD5 digest of the object, you can calculate the MD5 while putting an object to Amazon S3 and compare the returned ETag to the calculated MD5 value.</p>
    /// <p><b>Directory buckets </b> - The ETag for the object in a directory bucket isn't the MD5 digest of the object.</p>
    pub fn e_tag(mut self, input: impl Into<String>) -> Self {
        self.e_tag = Some(input.into());
        self
    }
    /// <p>Entity tag for the uploaded object.</p>
    /// <p><b>General purpose buckets </b> - To ensure that data is not corrupted traversing the network, for objects where the ETag is the MD5 digest of the object, you can calculate the MD5 while putting an object to Amazon S3 and compare the returned ETag to the calculated MD5 value.</p>
    /// <p><b>Directory buckets </b> - The ETag for the object in a directory bucket isn't the MD5 digest of the object.</p>
    pub fn set_e_tag(mut self, input: Option<String>) -> Self {
        self.e_tag = input;
        self
    }
    /// <p>Entity tag for the uploaded object.</p>
    /// <p><b>General purpose buckets </b> - To ensure that data is not corrupted traversing the network, for objects where the ETag is the MD5 digest of the object, you can calculate the MD5 while putting an object to Amazon S3 and compare the returned ETag to the calculated MD5 value.</p>
    /// <p><b>Directory buckets </b> - The ETag for the object in a directory bucket isn't the MD5 digest of the object.</p>
    pub fn get_e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }
    /// <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32(mut self, input: impl Into<String>) -> Self {
        self.checksum_crc32 = Some(input.into());
        self
    }
    /// <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_crc32(mut self, input: Option<String>) -> Self {
        self.checksum_crc32 = input;
        self
    }
    /// <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_crc32(&self) -> Option<&str> {
        self.checksum_crc32.as_deref()
    }
    /// <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32_c(mut self, input: impl Into<String>) -> Self {
        self.checksum_crc32_c = Some(input.into());
        self
    }
    /// <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_crc32_c(mut self, input: Option<String>) -> Self {
        self.checksum_crc32_c = input;
        self
    }
    /// <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_crc32_c(&self) -> Option<&str> {
        self.checksum_crc32_c.as_deref()
    }
    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. This header is present if the object was uploaded with the CRC64NVME checksum algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default checksum, CRC64NVME, to the uploaded object). For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn checksum_crc64_nvme(mut self, input: impl Into<String>) -> Self {
        self.checksum_crc64_nvme = Some(input.into());
        self
    }
    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. This header is present if the object was uploaded with the CRC64NVME checksum algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default checksum, CRC64NVME, to the uploaded object). For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn set_checksum_crc64_nvme(mut self, input: Option<String>) -> Self {
        self.checksum_crc64_nvme = input;
        self
    }
    /// <p>The Base64 encoded, 64-bit CRC64NVME checksum of the object. This header is present if the object was uploaded with the CRC64NVME checksum algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default checksum, CRC64NVME, to the uploaded object). For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn get_checksum_crc64_nvme(&self) -> Option<&str> {
        self.checksum_crc64_nvme.as_deref()
    }
    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. When you use the API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha1(mut self, input: impl Into<String>) -> Self {
        self.checksum_sha1 = Some(input.into());
        self
    }
    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. When you use the API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_sha1(mut self, input: Option<String>) -> Self {
        self.checksum_sha1 = input;
        self
    }
    /// <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be present if it was uploaded with the object. When you use the API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_sha1(&self) -> Option<&str> {
        self.checksum_sha1.as_deref()
    }
    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha256(mut self, input: impl Into<String>) -> Self {
        self.checksum_sha256 = Some(input.into());
        self
    }
    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_sha256(mut self, input: Option<String>) -> Self {
        self.checksum_sha256 = input;
        self
    }
    /// <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be present if it was uploaded with the object. When you use an API operation on an object that was uploaded using multipart uploads, this value may not be a direct checksum value of the full object. Instead, it's a calculation based on the checksum values of each individual part. For more information about how checksums are calculated with multipart uploads, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums"> Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_sha256(&self) -> Option<&str> {
        self.checksum_sha256.as_deref()
    }
    /// <p>This header specifies the checksum type of the object, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. For <code>PutObject</code> uploads, the checksum type is always <code>FULL_OBJECT</code>. You can use this header as a data integrity check to verify that the checksum type that is received is the same checksum that was specified. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn checksum_type(mut self, input: aws_sdk_s3::types::ChecksumType) -> Self {
        self.checksum_type = Some(input);
        self
    }
    /// <p>This header specifies the checksum type of the object, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. For <code>PutObject</code> uploads, the checksum type is always <code>FULL_OBJECT</code>. You can use this header as a data integrity check to verify that the checksum type that is received is the same checksum that was specified. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn set_checksum_type(mut self, input: Option<aws_sdk_s3::types::ChecksumType>) -> Self {
        self.checksum_type = input;
        self
    }
    /// <p>This header specifies the checksum type of the object, which determines how part-level checksums are combined to create an object-level checksum for multipart objects. For <code>PutObject</code> uploads, the checksum type is always <code>FULL_OBJECT</code>. You can use this header as a data integrity check to verify that the checksum type that is received is the same checksum that was specified. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity in the Amazon S3 User Guide</a>.</p>
    pub fn get_checksum_type(&self) -> &Option<aws_sdk_s3::types::ChecksumType> {
        &self.checksum_type
    }
    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p><note>
    /// <p>For directory buckets, only server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) is supported.</p>
    /// </note>
    pub fn server_side_encryption(
        mut self,
        input: aws_sdk_s3::types::ServerSideEncryption,
    ) -> Self {
        self.server_side_encryption = Some(input);
        self
    }
    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p><note>
    /// <p>For directory buckets, only server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) is supported.</p>
    /// </note>
    pub fn set_server_side_encryption(
        mut self,
        input: Option<aws_sdk_s3::types::ServerSideEncryption>,
    ) -> Self {
        self.server_side_encryption = input;
        self
    }
    /// <p>The server-side encryption algorithm used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p><note>
    /// <p>For directory buckets, only server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) is supported.</p>
    /// </note>
    pub fn get_server_side_encryption(&self) -> &Option<aws_sdk_s3::types::ServerSideEncryption> {
        &self.server_side_encryption
    }
    /// <p>Version ID of the object.</p>
    /// <p>If you enable versioning for a bucket, Amazon S3 automatically generates a unique version ID for the object being stored. Amazon S3 returns this ID in the response. When you enable versioning for a bucket, if Amazon S3 receives multiple write requests for the same object simultaneously, it stores all of the objects. For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/AddingObjectstoVersioningEnabledBuckets.html">Adding Objects to Versioning-Enabled Buckets</a> in the <i>Amazon S3 User Guide</i>. For information about returning the versioning state of a bucket, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn version_id(mut self, input: impl Into<String>) -> Self {
        self.version_id = Some(input.into());
        self
    }
    /// <p>Version ID of the object.</p>
    /// <p>If you enable versioning for a bucket, Amazon S3 automatically generates a unique version ID for the object being stored. Amazon S3 returns this ID in the response. When you enable versioning for a bucket, if Amazon S3 receives multiple write requests for the same object simultaneously, it stores all of the objects. For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/AddingObjectstoVersioningEnabledBuckets.html">Adding Objects to Versioning-Enabled Buckets</a> in the <i>Amazon S3 User Guide</i>. For information about returning the versioning state of a bucket, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_version_id(mut self, input: Option<String>) -> Self {
        self.version_id = input;
        self
    }
    /// <p>Version ID of the object.</p>
    /// <p>If you enable versioning for a bucket, Amazon S3 automatically generates a unique version ID for the object being stored. Amazon S3 returns this ID in the response. When you enable versioning for a bucket, if Amazon S3 receives multiple write requests for the same object simultaneously, it stores all of the objects. For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/AddingObjectstoVersioningEnabledBuckets.html">Adding Objects to Versioning-Enabled Buckets</a> in the <i>Amazon S3 User Guide</i>. For information about returning the versioning state of a bucket, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning</a>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_algorithm(mut self, input: impl Into<String>) -> Self {
        self.sse_customer_algorithm = Some(input.into());
        self
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_algorithm(mut self, input: Option<String>) -> Self {
        self.sse_customer_algorithm = input;
        self
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_algorithm(&self) -> Option<&str> {
        self.sse_customer_algorithm.as_deref()
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key_md5(mut self, input: impl Into<String>) -> Self {
        self.sse_customer_key_md5 = Some(input.into());
        self
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_key_md5(mut self, input: Option<String>) -> Self {
        self.sse_customer_key_md5 = input;
        self
    }
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide the round-trip message integrity verification of the customer-provided encryption key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_key_md5(&self) -> Option<&str> {
        self.sse_customer_key_md5.as_deref()
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header indicates the ID of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_key_id(mut self, input: impl Into<String>) -> Self {
        self.sse_kms_key_id = Some(input.into());
        self
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header indicates the ID of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_kms_key_id(mut self, input: Option<String>) -> Self {
        self.sse_kms_key_id = input;
        self
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header indicates the ID of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_kms_key_id(&self) -> Option<&str> {
        self.sse_kms_key_id.as_deref()
    }
    /// <p>If present, indicates the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_encryption_context(mut self, input: impl Into<String>) -> Self {
        self.sse_kms_encryption_context = Some(input.into());
        self
    }
    /// <p>If present, indicates the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_kms_encryption_context(mut self, input: Option<String>) -> Self {
        self.sse_kms_encryption_context = input;
        self
    }
    /// <p>If present, indicates the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_kms_encryption_context(&self) -> Option<&str> {
        self.sse_kms_encryption_context.as_deref()
    }
    /// <p>Indicates whether the uploaded object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn bucket_key_enabled(mut self, input: bool) -> Self {
        self.bucket_key_enabled = Some(input);
        self
    }
    /// <p>Indicates whether the uploaded object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_bucket_key_enabled(mut self, input: Option<bool>) -> Self {
        self.bucket_key_enabled = input;
        self
    }
    /// <p>Indicates whether the uploaded object uses an S3 Bucket Key for server-side encryption with Key Management Service (KMS) keys (SSE-KMS).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_bucket_key_enabled(&self) -> &Option<bool> {
        &self.bucket_key_enabled
    }
    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn request_charged(mut self, input: aws_sdk_s3::types::RequestCharged) -> Self {
        self.request_charged = Some(input);
        self
    }
    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_request_charged(mut self, input: Option<aws_sdk_s3::types::RequestCharged>) -> Self {
        self.request_charged = input;
        self
    }
    /// <p>If present, indicates that the requester was successfully charged for the request.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_request_charged(&self) -> &Option<aws_sdk_s3::types::RequestCharged> {
        &self.request_charged
    }

    /// <p>ID for the initiated multipart upload.</p>
    pub fn upload_id(mut self, input: impl Into<String>) -> Self {
        self.upload_id = Some(input.into());
        self
    }
    /// <p>ID for the initiated multipart upload.</p>
    pub fn set_upload_id(mut self, input: Option<String>) -> Self {
        self.upload_id = input;
        self
    }
    /// <p>ID for the initiated multipart upload.</p>
    pub fn get_upload_id(&self) -> Option<&str> {
        self.upload_id.as_deref()
    }

    /// Consumes the builder and constructs a [`UploadOutput`]
    pub fn build(self) -> Result<UploadOutput, ::aws_smithy_types::error::operation::BuildError> {
        Ok(UploadOutput {
            expiration: self.expiration,
            e_tag: self.e_tag,
            checksum_crc32: self.checksum_crc32,
            checksum_crc32_c: self.checksum_crc32_c,
            checksum_crc64_nvme: self.checksum_crc64_nvme,
            checksum_sha1: self.checksum_sha1,
            checksum_sha256: self.checksum_sha256,
            checksum_type: self.checksum_type,
            server_side_encryption: self.server_side_encryption,
            version_id: self.version_id,
            sse_customer_algorithm: self.sse_customer_algorithm,
            sse_customer_key_md5: self.sse_customer_key_md5,
            sse_kms_key_id: self.sse_kms_key_id,
            sse_kms_encryption_context: self.sse_kms_encryption_context,
            bucket_key_enabled: self.bucket_key_enabled,
            request_charged: self.request_charged,
            upload_id: self.upload_id,
        })
    }
}

impl From<PutObjectOutput> for UploadOutputBuilder {
    fn from(value: PutObjectOutput) -> Self {
        UploadOutputBuilder {
            bucket_key_enabled: value.bucket_key_enabled,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_crc64_nvme: value.checksum_crc64_nvme,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            checksum_type: value.checksum_type,
            e_tag: value.e_tag,
            expiration: value.expiration,
            request_charged: value.request_charged,
            server_side_encryption: value.server_side_encryption,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            sse_kms_encryption_context: value.ssekms_encryption_context,
            sse_kms_key_id: value.ssekms_key_id,
            upload_id: None,
            version_id: value.version_id,
        }
    }
}

impl Debug for UploadOutputBuilder {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        let mut formatter = f.debug_struct("UploadOutputBuilder");
        formatter.field("expiration", &self.expiration);
        formatter.field("e_tag", &self.e_tag);
        formatter.field("checksum_crc32", &self.checksum_crc32);
        formatter.field("checksum_crc32_c", &self.checksum_crc32_c);
        formatter.field("checksum_crc64_nvme", &self.checksum_crc64_nvme);
        formatter.field("checksum_sha1", &self.checksum_sha1);
        formatter.field("checksum_sha256", &self.checksum_sha256);
        formatter.field("checksum_type", &self.checksum_type);
        formatter.field("server_side_encryption", &self.server_side_encryption);
        formatter.field("version_id", &self.version_id);
        formatter.field("sse_customer_algorithm", &self.sse_customer_algorithm);
        formatter.field("sse_customer_key_md5", &self.sse_customer_key_md5);
        formatter.field("sse_kms_key_id", &"*** Sensitive Data Redacted ***");
        formatter.field(
            "sse_kms_encryption_context",
            &"*** Sensitive Data Redacted ***",
        );
        formatter.field("bucket_key_enabled", &self.bucket_key_enabled);
        formatter.field("request_charged", &self.request_charged);
        formatter.field("upload_id", &self.upload_id);
        formatter.finish()
    }
}

impl UploadOutputBuilder {
    /// Updates the builder with fields from `CompleteMultipartUploadOutput
    pub fn update_from_complete_mpu(
        mut self,
        complete_mpu_resp: &CompleteMultipartUploadOutput,
    ) -> Self {
        self.bucket_key_enabled = complete_mpu_resp.bucket_key_enabled;
        self.checksum_crc32 = complete_mpu_resp.checksum_crc32.clone();
        self.checksum_crc32_c = complete_mpu_resp.checksum_crc32_c.clone();
        self.checksum_crc64_nvme = complete_mpu_resp.checksum_crc64_nvme.clone();
        self.checksum_sha1 = complete_mpu_resp.checksum_sha1.clone();
        self.checksum_sha256 = complete_mpu_resp.checksum_sha256.clone();
        self.checksum_type = complete_mpu_resp.checksum_type.clone();
        self.e_tag = complete_mpu_resp.e_tag.clone();
        self.expiration = complete_mpu_resp.expiration.clone();
        self.request_charged = complete_mpu_resp.request_charged.clone();
        self.sse_kms_key_id = complete_mpu_resp.ssekms_key_id.clone();
        self.server_side_encryption = complete_mpu_resp.server_side_encryption.clone();
        self.version_id = complete_mpu_resp.version_id.clone();
        self
    }
}

impl From<CreateMultipartUploadOutput> for UploadOutputBuilder {
    fn from(value: CreateMultipartUploadOutput) -> Self {
        UploadOutputBuilder {
            upload_id: value.upload_id,
            server_side_encryption: value.server_side_encryption,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            sse_kms_key_id: value.ssekms_key_id,
            sse_kms_encryption_context: value.ssekms_encryption_context,
            bucket_key_enabled: value.bucket_key_enabled,
            request_charged: value.request_charged,
            checksum_type: value.checksum_type,
            // remaining fields will be set later, from CompleteMultipartUploadOutput
            expiration: None,
            e_tag: None,
            checksum_crc32: None,
            checksum_crc32_c: None,
            checksum_crc64_nvme: None,
            checksum_sha1: None,
            checksum_sha256: None,
            version_id: None,
            // TODO(aws-sdk-rust#1159): abort_rule_id and abort_date seem unique to CreateMultipartUploadOutput
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::types::{ChecksumType, RequestCharged, ServerSideEncryption};

    #[test]
    fn test_update_from_complete_mpu() {
        let complete_mpu_resp = CompleteMultipartUploadOutput::builder()
            .bucket_key_enabled(true)
            .checksum_crc32("AAAAAA==")
            .checksum_type(ChecksumType::FullObject)
            .e_tag("test-etag")
            .expiration("test-expiration")
            .request_charged(RequestCharged::Requester)
            .ssekms_key_id("test-kms-key")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .version_id("test-version")
            .build();

        let sut = UploadOutputBuilder::default().update_from_complete_mpu(&complete_mpu_resp);

        assert_eq!(complete_mpu_resp.bucket_key_enabled, sut.bucket_key_enabled);
        assert_eq!(complete_mpu_resp.checksum_crc32, sut.checksum_crc32);
        assert_eq!(complete_mpu_resp.checksum_crc32_c, sut.checksum_crc32_c);
        assert_eq!(
            complete_mpu_resp.checksum_crc64_nvme,
            sut.checksum_crc64_nvme
        );
        assert_eq!(complete_mpu_resp.checksum_sha1, sut.checksum_sha1);
        assert_eq!(complete_mpu_resp.checksum_sha256, sut.checksum_sha256);
        assert_eq!(complete_mpu_resp.checksum_type, sut.checksum_type);
        assert_eq!(complete_mpu_resp.e_tag, sut.e_tag);
        assert_eq!(complete_mpu_resp.expiration, sut.expiration);
        assert_eq!(complete_mpu_resp.request_charged, sut.request_charged);
        assert_eq!(complete_mpu_resp.ssekms_key_id, sut.sse_kms_key_id);
        assert_eq!(
            complete_mpu_resp.server_side_encryption,
            sut.server_side_encryption
        );
        assert_eq!(complete_mpu_resp.version_id, sut.version_id);
    }

    #[test]
    fn test_from_put_object_output() {
        let put_object_output = PutObjectOutput::builder()
            .bucket_key_enabled(true)
            .checksum_crc32("AAAAAA==")
            .checksum_type(ChecksumType::FullObject)
            .e_tag("test-etag")
            .expiration("test-expiration")
            .request_charged(RequestCharged::Requester)
            .server_side_encryption(ServerSideEncryption::Aes256)
            .sse_customer_algorithm("AES256")
            .sse_customer_key_md5("test-md5")
            .ssekms_encryption_context("test-context")
            .ssekms_key_id("test-kms-key")
            .version_id("test-version")
            .build();

        let sut = UploadOutputBuilder::from(put_object_output.clone());

        assert_eq!(put_object_output.bucket_key_enabled, sut.bucket_key_enabled);
        assert_eq!(put_object_output.checksum_crc32, sut.checksum_crc32);
        assert_eq!(put_object_output.checksum_crc32_c, sut.checksum_crc32_c);
        assert_eq!(
            put_object_output.checksum_crc64_nvme,
            sut.checksum_crc64_nvme
        );
        assert_eq!(put_object_output.checksum_sha1, sut.checksum_sha1);
        assert_eq!(put_object_output.checksum_sha256, sut.checksum_sha256);
        assert_eq!(put_object_output.checksum_type, sut.checksum_type);
        assert_eq!(put_object_output.e_tag, sut.e_tag);
        assert_eq!(put_object_output.expiration, sut.expiration);
        assert_eq!(put_object_output.request_charged, sut.request_charged);
        assert_eq!(
            put_object_output.server_side_encryption,
            sut.server_side_encryption
        );
        assert_eq!(
            put_object_output.sse_customer_algorithm,
            sut.sse_customer_algorithm
        );
        assert_eq!(
            put_object_output.sse_customer_key_md5,
            sut.sse_customer_key_md5
        );
        assert_eq!(
            put_object_output.ssekms_encryption_context,
            sut.sse_kms_encryption_context
        );
        assert_eq!(put_object_output.ssekms_key_id, sut.sse_kms_key_id);
        assert_eq!(None, sut.upload_id);
        assert_eq!(put_object_output.version_id, sut.version_id);
    }
}
