/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_smithy_types::error::operation::BuildError;

/// Input type for downloading a single object
#[allow(missing_docs)] // documentation missing in model
#[non_exhaustive]
#[derive(Clone, PartialEq)]
pub struct DownloadInput {
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub bucket: Option<String>,
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub if_match: Option<String>,
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub if_modified_since: Option<::aws_smithy_types::DateTime>,
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub if_none_match: Option<String>,
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub if_unmodified_since: Option<::aws_smithy_types::DateTime>,
    /// <p>Key of the object to get.</p>
    pub key: Option<String>,
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub range: Option<String>,
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub response_cache_control: Option<String>,
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub response_content_disposition: Option<String>,
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub response_content_encoding: Option<String>,
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub response_content_language: Option<String>,
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub response_content_type: Option<String>,
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub response_expires: Option<::aws_smithy_types::DateTime>,
    /// <p>Version ID used to reference a specific version of the object.</p>
    /// <p>By default, the <code>GetObject</code> operation returns the current version of an object. To return a different version, use the <code>versionId</code> subresource.</p><note>
    /// <ul>
    /// <li>
    /// <p>If you include a <code>versionId</code> in your request header, you must have the <code>s3:GetObjectVersion</code> permission to access a specific version of an object. The <code>s3:GetObject</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p>If you request the current version of an object without a specific <code>versionId</code> in the request header, only the <code>s3:GetObject</code> permission is required. The <code>s3:GetObjectVersion</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p><b>Directory buckets</b> - S3 Versioning isn't enabled and supported for directory buckets. For this API operation, only the <code>null</code> value of the version ID is supported by directory buckets. You can only specify <code>null</code> to the <code>versionId</code> query parameter in the request.</p></li>
    /// </ul>
    /// </note>
    /// <p>For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>.</p>
    pub version_id: Option<String>,
    /// <p>Specifies the algorithm to use when decrypting the object (for example, <code>AES256</code>).</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_algorithm: Option<String>,
    /// <p>Specifies the customer-provided encryption key that you originally provided for Amazon S3 to encrypt the data before storing it. This value is used to decrypt the object when recovering it and must match the one used when storing the data. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_key: Option<String>,
    /// <p>Specifies the 128-bit MD5 digest of the customer-provided encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub sse_customer_key_md5: Option<String>,
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub request_payer: Option<aws_sdk_s3::types::RequestPayer>,
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub part_number: Option<i32>,
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub expected_bucket_owner: Option<String>,
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub checksum_mode: Option<aws_sdk_s3::types::ChecksumMode>,
}
impl DownloadInput {
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_modified_since(&self) -> Option<&::aws_smithy_types::DateTime> {
        self.if_modified_since.as_ref()
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_unmodified_since(&self) -> Option<&::aws_smithy_types::DateTime> {
        self.if_unmodified_since.as_ref()
    }
    /// <p>Key of the object to get.</p>
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn range(&self) -> Option<&str> {
        self.range.as_deref()
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn response_cache_control(&self) -> Option<&str> {
        self.response_cache_control.as_deref()
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn response_content_disposition(&self) -> Option<&str> {
        self.response_content_disposition.as_deref()
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn response_content_encoding(&self) -> Option<&str> {
        self.response_content_encoding.as_deref()
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn response_content_language(&self) -> Option<&str> {
        self.response_content_language.as_deref()
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn response_content_type(&self) -> Option<&str> {
        self.response_content_type.as_deref()
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn response_expires(&self) -> Option<&::aws_smithy_types::DateTime> {
        self.response_expires.as_ref()
    }
    /// <p>Version ID used to reference a specific version of the object.</p>
    /// <p>By default, the <code>GetObject</code> operation returns the current version of an object. To return a different version, use the <code>versionId</code> subresource.</p><note>
    /// <ul>
    /// <li>
    /// <p>If you include a <code>versionId</code> in your request header, you must have the <code>s3:GetObjectVersion</code> permission to access a specific version of an object. The <code>s3:GetObject</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p>If you request the current version of an object without a specific <code>versionId</code> in the request header, only the <code>s3:GetObject</code> permission is required. The <code>s3:GetObjectVersion</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p><b>Directory buckets</b> - S3 Versioning isn't enabled and supported for directory buckets. For this API operation, only the <code>null</code> value of the version ID is supported by directory buckets. You can only specify <code>null</code> to the <code>versionId</code> query parameter in the request.</p></li>
    /// </ul>
    /// </note>
    /// <p>For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>.</p>
    pub fn version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
    /// <p>Specifies the algorithm to use when decrypting the object (for example, <code>AES256</code>).</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_algorithm(&self) -> Option<&str> {
        self.sse_customer_algorithm.as_deref()
    }
    /// <p>Specifies the customer-provided encryption key that you originally provided for Amazon S3 to encrypt the data before storing it. This value is used to decrypt the object when recovering it and must match the one used when storing the data. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key(&self) -> Option<&str> {
        self.sse_customer_key.as_deref()
    }
    /// <p>Specifies the 128-bit MD5 digest of the customer-provided encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key_md5(&self) -> Option<&str> {
        self.sse_customer_key_md5.as_deref()
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn request_payer(&self) -> Option<&aws_sdk_s3::types::RequestPayer> {
        self.request_payer.as_ref()
    }
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub fn part_number(&self) -> Option<i32> {
        self.part_number
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn expected_bucket_owner(&self) -> Option<&str> {
        self.expected_bucket_owner.as_deref()
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn checksum_mode(&self) -> Option<&aws_sdk_s3::types::ChecksumMode> {
        self.checksum_mode.as_ref()
    }
}

impl fmt::Debug for DownloadInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("DownloadInput");
        formatter.field("bucket", &self.bucket);
        formatter.field("if_match", &self.if_match);
        formatter.field("if_modified_since", &self.if_modified_since);
        formatter.field("if_none_match", &self.if_none_match);
        formatter.field("if_unmodified_since", &self.if_unmodified_since);
        formatter.field("key", &self.key);
        formatter.field("range", &self.range);
        formatter.field("response_cache_control", &self.response_cache_control);
        formatter.field(
            "response_content_disposition",
            &self.response_content_disposition,
        );
        formatter.field("response_content_encoding", &self.response_content_encoding);
        formatter.field("response_content_language", &self.response_content_language);
        formatter.field("response_content_type", &self.response_content_type);
        formatter.field("response_expires", &self.response_expires);
        formatter.field("version_id", &self.version_id);
        formatter.field("sse_customer_algorithm", &self.sse_customer_algorithm);
        formatter.field("sse_customer_key", &"*** Sensitive Data Redacted ***");
        formatter.field("sse_customer_key_md5", &self.sse_customer_key_md5);
        formatter.field("request_payer", &self.request_payer);
        formatter.field("part_number", &self.part_number);
        formatter.field("expected_bucket_owner", &self.expected_bucket_owner);
        formatter.field("checksum_mode", &self.checksum_mode);
        formatter.finish()
    }
}

impl DownloadInput {
    /// Creates a new builder-style object to manufacture [`DownloadInput`](crate::operation::download::DownloadInput).
    pub fn builder() -> DownloadInputBuilder {
        DownloadInputBuilder::default()
    }
}

/// A builder for [`DownloadInput`].
#[derive(Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct DownloadInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) if_match: Option<String>,
    pub(crate) if_modified_since: Option<::aws_smithy_types::DateTime>,
    pub(crate) if_none_match: Option<String>,
    pub(crate) if_unmodified_since: Option<::aws_smithy_types::DateTime>,
    pub(crate) key: Option<String>,
    pub(crate) range: Option<String>,
    pub(crate) response_cache_control: Option<String>,
    pub(crate) response_content_disposition: Option<String>,
    pub(crate) response_content_encoding: Option<String>,
    pub(crate) response_content_language: Option<String>,
    pub(crate) response_content_type: Option<String>,
    pub(crate) response_expires: Option<::aws_smithy_types::DateTime>,
    pub(crate) version_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) request_payer: Option<aws_sdk_s3::types::RequestPayer>,
    pub(crate) part_number: Option<i32>,
    pub(crate) expected_bucket_owner: Option<String>,
    pub(crate) checksum_mode: Option<aws_sdk_s3::types::ChecksumMode>,
}

impl DownloadInputBuilder {
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// This field is required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.bucket = Option::Some(input.into());
        self
    }
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.bucket = input;
        self
    }
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_match(mut self, input: impl Into<String>) -> Self {
        self.if_match = Option::Some(input.into());
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_match(mut self, input: Option<String>) -> Self {
        self.if_match = input;
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_modified_since(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.if_modified_since = Option::Some(input);
        self
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_modified_since(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.if_modified_since = input;
        self
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_modified_since(&self) -> &Option<::aws_smithy_types::DateTime> {
        &self.if_modified_since
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_none_match(mut self, input: impl Into<String>) -> Self {
        self.if_none_match = Option::Some(input.into());
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_none_match(mut self, input: Option<String>) -> Self {
        self.if_none_match = input;
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_unmodified_since(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.if_unmodified_since = Option::Some(input);
        self
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_unmodified_since(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.if_unmodified_since = input;
        self
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_unmodified_since(&self) -> &Option<::aws_smithy_types::DateTime> {
        &self.if_unmodified_since
    }
    /// <p>Key of the object to get.</p>
    /// This field is required.
    pub fn key(mut self, input: impl Into<String>) -> Self {
        self.key = Option::Some(input.into());
        self
    }
    /// <p>Key of the object to get.</p>
    pub fn set_key(mut self, input: Option<String>) -> Self {
        self.key = input;
        self
    }
    /// <p>Key of the object to get.</p>
    pub fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn range(mut self, input: impl Into<String>) -> Self {
        self.range = Option::Some(input.into());
        self
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn set_range(mut self, input: Option<String>) -> Self {
        self.range = input;
        self
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn get_range(&self) -> Option<&str> {
        self.range.as_deref()
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn response_cache_control(mut self, input: impl Into<String>) -> Self {
        self.response_cache_control = Option::Some(input.into());
        self
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn set_response_cache_control(mut self, input: Option<String>) -> Self {
        self.response_cache_control = input;
        self
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn get_response_cache_control(&self) -> Option<&str> {
        self.response_cache_control.as_deref()
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn response_content_disposition(mut self, input: impl Into<String>) -> Self {
        self.response_content_disposition = Option::Some(input.into());
        self
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn set_response_content_disposition(mut self, input: Option<String>) -> Self {
        self.response_content_disposition = input;
        self
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn get_response_content_disposition(&self) -> Option<&str> {
        self.response_content_disposition.as_deref()
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn response_content_encoding(mut self, input: impl Into<String>) -> Self {
        self.response_content_encoding = Option::Some(input.into());
        self
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn set_response_content_encoding(mut self, input: Option<String>) -> Self {
        self.response_content_encoding = input;
        self
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn get_response_content_encoding(&self) -> Option<&str> {
        self.response_content_encoding.as_deref()
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn response_content_language(mut self, input: impl Into<String>) -> Self {
        self.response_content_language = Option::Some(input.into());
        self
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn set_response_content_language(mut self, input: Option<String>) -> Self {
        self.response_content_language = input;
        self
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn get_response_content_language(&self) -> Option<&str> {
        self.response_content_language.as_deref()
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn response_content_type(mut self, input: impl Into<String>) -> Self {
        self.response_content_type = Option::Some(input.into());
        self
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn set_response_content_type(mut self, input: Option<String>) -> Self {
        self.response_content_type = input;
        self
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn get_response_content_type(&self) -> Option<&str> {
        self.response_content_type.as_deref()
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn response_expires(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.response_expires = Option::Some(input);
        self
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn set_response_expires(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.response_expires = input;
        self
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn get_response_expires(&self) -> &Option<::aws_smithy_types::DateTime> {
        &self.response_expires
    }
    /// <p>Version ID used to reference a specific version of the object.</p>
    /// <p>By default, the <code>GetObject</code> operation returns the current version of an object. To return a different version, use the <code>versionId</code> subresource.</p><note>
    /// <ul>
    /// <li>
    /// <p>If you include a <code>versionId</code> in your request header, you must have the <code>s3:GetObjectVersion</code> permission to access a specific version of an object. The <code>s3:GetObject</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p>If you request the current version of an object without a specific <code>versionId</code> in the request header, only the <code>s3:GetObject</code> permission is required. The <code>s3:GetObjectVersion</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p><b>Directory buckets</b> - S3 Versioning isn't enabled and supported for directory buckets. For this API operation, only the <code>null</code> value of the version ID is supported by directory buckets. You can only specify <code>null</code> to the <code>versionId</code> query parameter in the request.</p></li>
    /// </ul>
    /// </note>
    /// <p>For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>.</p>
    pub fn version_id(mut self, input: impl Into<String>) -> Self {
        self.version_id = Option::Some(input.into());
        self
    }
    /// <p>Version ID used to reference a specific version of the object.</p>
    /// <p>By default, the <code>GetObject</code> operation returns the current version of an object. To return a different version, use the <code>versionId</code> subresource.</p><note>
    /// <ul>
    /// <li>
    /// <p>If you include a <code>versionId</code> in your request header, you must have the <code>s3:GetObjectVersion</code> permission to access a specific version of an object. The <code>s3:GetObject</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p>If you request the current version of an object without a specific <code>versionId</code> in the request header, only the <code>s3:GetObject</code> permission is required. The <code>s3:GetObjectVersion</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p><b>Directory buckets</b> - S3 Versioning isn't enabled and supported for directory buckets. For this API operation, only the <code>null</code> value of the version ID is supported by directory buckets. You can only specify <code>null</code> to the <code>versionId</code> query parameter in the request.</p></li>
    /// </ul>
    /// </note>
    /// <p>For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>.</p>
    pub fn set_version_id(mut self, input: Option<String>) -> Self {
        self.version_id = input;
        self
    }
    /// <p>Version ID used to reference a specific version of the object.</p>
    /// <p>By default, the <code>GetObject</code> operation returns the current version of an object. To return a different version, use the <code>versionId</code> subresource.</p><note>
    /// <ul>
    /// <li>
    /// <p>If you include a <code>versionId</code> in your request header, you must have the <code>s3:GetObjectVersion</code> permission to access a specific version of an object. The <code>s3:GetObject</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p>If you request the current version of an object without a specific <code>versionId</code> in the request header, only the <code>s3:GetObject</code> permission is required. The <code>s3:GetObjectVersion</code> permission is not required in this scenario.</p></li>
    /// <li>
    /// <p><b>Directory buckets</b> - S3 Versioning isn't enabled and supported for directory buckets. For this API operation, only the <code>null</code> value of the version ID is supported by directory buckets. You can only specify <code>null</code> to the <code>versionId</code> query parameter in the request.</p></li>
    /// </ul>
    /// </note>
    /// <p>For more information about versioning, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning</a>.</p>
    pub fn get_version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
    /// <p>Specifies the algorithm to use when decrypting the object (for example, <code>AES256</code>).</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_algorithm(mut self, input: impl Into<String>) -> Self {
        self.sse_customer_algorithm = Option::Some(input.into());
        self
    }
    /// <p>Specifies the algorithm to use when decrypting the object (for example, <code>AES256</code>).</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_algorithm(mut self, input: Option<String>) -> Self {
        self.sse_customer_algorithm = input;
        self
    }
    /// <p>Specifies the algorithm to use when decrypting the object (for example, <code>AES256</code>).</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_algorithm(&self) -> Option<&str> {
        self.sse_customer_algorithm.as_deref()
    }
    /// <p>Specifies the customer-provided encryption key that you originally provided for Amazon S3 to encrypt the data before storing it. This value is used to decrypt the object when recovering it and must match the one used when storing the data. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key(mut self, input: impl Into<String>) -> Self {
        self.sse_customer_key = Option::Some(input.into());
        self
    }
    /// <p>Specifies the customer-provided encryption key that you originally provided for Amazon S3 to encrypt the data before storing it. This value is used to decrypt the object when recovering it and must match the one used when storing the data. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_key(mut self, input: Option<String>) -> Self {
        self.sse_customer_key = input;
        self
    }
    /// <p>Specifies the customer-provided encryption key that you originally provided for Amazon S3 to encrypt the data before storing it. This value is used to decrypt the object when recovering it and must match the one used when storing the data. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_key(&self) -> Option<&str> {
        self.sse_customer_key.as_deref()
    }
    /// <p>Specifies the 128-bit MD5 digest of the customer-provided encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key_md5(mut self, input: impl Into<String>) -> Self {
        self.sse_customer_key_md5 = Option::Some(input.into());
        self
    }
    /// <p>Specifies the 128-bit MD5 digest of the customer-provided encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_key_md5(mut self, input: Option<String>) -> Self {
        self.sse_customer_key_md5 = input;
        self
    }
    /// <p>Specifies the 128-bit MD5 digest of the customer-provided encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    /// <p>If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers:</p>
    /// <ul>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-algorithm</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key</code></p></li>
    /// <li>
    /// <p><code>x-amz-server-side-encryption-customer-key-MD5</code></p></li>
    /// </ul>
    /// <p>For more information about SSE-C, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server-Side Encryption (Using Customer-Provided Encryption Keys)</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_key_md5(&self) -> Option<&str> {
        self.sse_customer_key_md5.as_deref()
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn request_payer(mut self, input: aws_sdk_s3::types::RequestPayer) -> Self {
        self.request_payer = Option::Some(input);
        self
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_request_payer(mut self, input: Option<aws_sdk_s3::types::RequestPayer>) -> Self {
        self.request_payer = input;
        self
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_request_payer(&self) -> &Option<aws_sdk_s3::types::RequestPayer> {
        &self.request_payer
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn expected_bucket_owner(mut self, input: impl Into<String>) -> Self {
        self.expected_bucket_owner = Option::Some(input.into());
        self
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn set_expected_bucket_owner(mut self, input: Option<String>) -> Self {
        self.expected_bucket_owner = input;
        self
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn get_expected_bucket_owner(&self) -> Option<&str> {
        self.expected_bucket_owner.as_deref()
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn checksum_mode(mut self, input: aws_sdk_s3::types::ChecksumMode) -> Self {
        self.checksum_mode = Option::Some(input);
        self
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn set_checksum_mode(mut self, input: Option<aws_sdk_s3::types::ChecksumMode>) -> Self {
        self.checksum_mode = input;
        self
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn get_checksum_mode(&self) -> &Option<aws_sdk_s3::types::ChecksumMode> {
        &self.checksum_mode
    }
    /// Consumes the builder and constructs a [`DownloadInput`].
    pub fn build(self) -> Result<DownloadInput, ::aws_smithy_types::error::operation::BuildError> {
        if self.bucket.is_none() {
            return Err(BuildError::missing_field("bucket", "A bucket is required"));
        }

        if self.key.is_none() {
            return Err(BuildError::missing_field("key", "A key is required"));
        }

        Result::Ok(DownloadInput {
            bucket: self.bucket,
            if_match: self.if_match,
            if_modified_since: self.if_modified_since,
            if_none_match: self.if_none_match,
            if_unmodified_since: self.if_unmodified_since,
            key: self.key,
            range: self.range,
            response_cache_control: self.response_cache_control,
            response_content_disposition: self.response_content_disposition,
            response_content_encoding: self.response_content_encoding,
            response_content_language: self.response_content_language,
            response_content_type: self.response_content_type,
            response_expires: self.response_expires,
            version_id: self.version_id,
            sse_customer_algorithm: self.sse_customer_algorithm,
            sse_customer_key: self.sse_customer_key,
            sse_customer_key_md5: self.sse_customer_key_md5,
            request_payer: self.request_payer,
            part_number: self.part_number,
            expected_bucket_owner: self.expected_bucket_owner,
            checksum_mode: self.checksum_mode,
        })
    }
}

impl fmt::Debug for DownloadInputBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("DownloadInputBuilder");
        formatter.field("bucket", &self.bucket);
        formatter.field("if_match", &self.if_match);
        formatter.field("if_modified_since", &self.if_modified_since);
        formatter.field("if_none_match", &self.if_none_match);
        formatter.field("if_unmodified_since", &self.if_unmodified_since);
        formatter.field("key", &self.key);
        formatter.field("range", &self.range);
        formatter.field("response_cache_control", &self.response_cache_control);
        formatter.field(
            "response_content_disposition",
            &self.response_content_disposition,
        );
        formatter.field("response_content_encoding", &self.response_content_encoding);
        formatter.field("response_content_language", &self.response_content_language);
        formatter.field("response_content_type", &self.response_content_type);
        formatter.field("response_expires", &self.response_expires);
        formatter.field("version_id", &self.version_id);
        formatter.field("sse_customer_algorithm", &self.sse_customer_algorithm);
        formatter.field("sse_customer_key", &"*** Sensitive Data Redacted ***");
        formatter.field("sse_customer_key_md5", &self.sse_customer_key_md5);
        formatter.field("request_payer", &self.request_payer);
        formatter.field("part_number", &self.part_number);
        formatter.field("expected_bucket_owner", &self.expected_bucket_owner);
        formatter.field("checksum_mode", &self.checksum_mode);
        formatter.finish()
    }
}

impl From<DownloadInput> for GetObjectInputBuilder {
    fn from(value: DownloadInput) -> Self {
        GetObjectInputBuilder::default()
            .set_bucket(value.bucket)
            .set_if_match(value.if_match)
            .set_if_modified_since(value.if_modified_since)
            .set_if_none_match(value.if_none_match)
            .set_if_unmodified_since(value.if_unmodified_since)
            .set_key(value.key)
            .set_range(value.range)
            .set_response_cache_control(value.response_cache_control)
            .set_response_content_disposition(value.response_content_disposition)
            .set_response_content_encoding(value.response_content_encoding)
            .set_response_content_language(value.response_content_language)
            .set_response_content_type(value.response_content_type)
            .set_response_expires(value.response_expires)
            .set_version_id(value.version_id)
            .set_sse_customer_algorithm(value.sse_customer_algorithm)
            .set_sse_customer_key(value.sse_customer_key)
            .set_sse_customer_key_md5(value.sse_customer_key_md5)
            .set_request_payer(value.request_payer)
            .set_expected_bucket_owner(value.expected_bucket_owner)
            .set_checksum_mode(value.checksum_mode)
    }
}

impl From<DownloadInput> for DownloadInputBuilder {
    fn from(value: DownloadInput) -> Self {
        DownloadInputBuilder {
            bucket: value.bucket,
            if_match: value.if_match,
            if_modified_since: value.if_modified_since,
            if_none_match: value.if_none_match,
            if_unmodified_since: value.if_unmodified_since,
            key: value.key,
            range: value.range,
            response_cache_control: value.response_cache_control,
            response_content_disposition: value.response_content_disposition,
            response_content_encoding: value.response_content_encoding,
            response_content_language: value.response_content_language,
            response_content_type: value.response_content_type,
            response_expires: value.response_expires,
            version_id: value.version_id,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key: value.sse_customer_key,
            sse_customer_key_md5: value.sse_customer_key_md5,
            request_payer: value.request_payer,
            part_number: value.part_number,
            expected_bucket_owner: value.expected_bucket_owner,
            checksum_mode: value.checksum_mode,
        }
    }
}

impl DownloadInputBuilder {
    /// Create an operation fluent builder for the rust SDK S3 client from this transfer manager
    /// download input and the given client
    pub(crate) fn into_sdk_operation(
        self,
        client: &aws_sdk_s3::Client,
    ) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
        client
            .get_object()
            .set_bucket(self.bucket)
            .set_if_match(self.if_match)
            .set_if_modified_since(self.if_modified_since)
            .set_if_none_match(self.if_none_match)
            .set_if_unmodified_since(self.if_unmodified_since)
            .set_key(self.key)
            .set_range(self.range)
            .set_response_cache_control(self.response_cache_control)
            .set_response_content_disposition(self.response_content_disposition)
            .set_response_content_encoding(self.response_content_encoding)
            .set_response_content_language(self.response_content_language)
            .set_response_content_type(self.response_content_type)
            .set_response_expires(self.response_expires)
            .set_version_id(self.version_id)
            .set_sse_customer_algorithm(self.sse_customer_algorithm)
            .set_sse_customer_key(self.sse_customer_key)
            .set_sse_customer_key_md5(self.sse_customer_key_md5)
            .set_request_payer(self.request_payer)
            .set_expected_bucket_owner(self.expected_bucket_owner)
            .set_checksum_mode(self.checksum_mode)
    }
}

// TODO - implement TryFrom<GetObjectInput> and TryFrom<GetObjectInputBuilder> for DownloadInput and DownloadInputBuilder respectively (TryFrom due to checksums)
