/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use crate::types::FailedMultipartUploadPolicy;

use super::{UploadHandle, UploadInputBuilder};

/// Fluent builder for constructing a single object upload transfer
#[derive(Debug)]
pub struct UploadFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: UploadInputBuilder,
}

impl UploadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
        }
    }

    /// Initiate an upload transfer for a single object
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-upload", fields(
        bucket = self.inner.bucket.as_deref().unwrap_or_default(),
        key = self.inner.key.as_deref().unwrap_or_default(),
    ))]
    pub async fn send(self) -> Result<UploadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::upload::Upload::orchestrate(self.handle, input).await
    }

    /// <p>The canned ACL to apply to the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned ACL</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>When adding a new object, you can use headers to grant ACL-based permissions to individual Amazon Web Services accounts or to predefined groups defined by Amazon S3. These permissions are then added to the ACL on the object. By default, all objects are private. Only the owner has full access control. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html">Access Control List (ACL) Overview</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-using-rest-api.html">Managing ACLs Using the REST API</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the bucket that you're uploading objects to uses the bucket owner enforced setting for S3 Object Ownership, ACLs are disabled and no longer affect permissions. Buckets that use this setting only accept PUT requests that don't specify an ACL or PUT requests that specify bucket owner full control ACLs, such as the <code>bucket-owner-full-control</code> canned ACL or an equivalent form of this ACL expressed in the XML format. PUT requests that contain other ACLs (for example, custom grants to certain Amazon Web Services accounts) fail and return a <code>400</code> error with the error code <code>AccessControlListNotSupported</code>. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html"> Controlling ownership of objects and disabling ACLs</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn acl(mut self, input: aws_sdk_s3::types::ObjectCannedAcl) -> Self {
        self.inner = self.inner.acl(input);
        self
    }
    /// <p>The canned ACL to apply to the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned ACL</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>When adding a new object, you can use headers to grant ACL-based permissions to individual Amazon Web Services accounts or to predefined groups defined by Amazon S3. These permissions are then added to the ACL on the object. By default, all objects are private. Only the owner has full access control. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html">Access Control List (ACL) Overview</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-using-rest-api.html">Managing ACLs Using the REST API</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the bucket that you're uploading objects to uses the bucket owner enforced setting for S3 Object Ownership, ACLs are disabled and no longer affect permissions. Buckets that use this setting only accept PUT requests that don't specify an ACL or PUT requests that specify bucket owner full control ACLs, such as the <code>bucket-owner-full-control</code> canned ACL or an equivalent form of this ACL expressed in the XML format. PUT requests that contain other ACLs (for example, custom grants to certain Amazon Web Services accounts) fail and return a <code>400</code> error with the error code <code>AccessControlListNotSupported</code>. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html"> Controlling ownership of objects and disabling ACLs</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn set_acl(mut self, input: Option<aws_sdk_s3::types::ObjectCannedAcl>) -> Self {
        self.inner = self.inner.set_acl(input);
        self
    }
    /// <p>The canned ACL to apply to the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned ACL</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>When adding a new object, you can use headers to grant ACL-based permissions to individual Amazon Web Services accounts or to predefined groups defined by Amazon S3. These permissions are then added to the ACL on the object. By default, all objects are private. Only the owner has full access control. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html">Access Control List (ACL) Overview</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-using-rest-api.html">Managing ACLs Using the REST API</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the bucket that you're uploading objects to uses the bucket owner enforced setting for S3 Object Ownership, ACLs are disabled and no longer affect permissions. Buckets that use this setting only accept PUT requests that don't specify an ACL or PUT requests that specify bucket owner full control ACLs, such as the <code>bucket-owner-full-control</code> canned ACL or an equivalent form of this ACL expressed in the XML format. PUT requests that contain other ACLs (for example, custom grants to certain Amazon Web Services accounts) fail and return a <code>400</code> error with the error code <code>AccessControlListNotSupported</code>. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html"> Controlling ownership of objects and disabling ACLs</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn get_acl(&self) -> &Option<aws_sdk_s3::types::ObjectCannedAcl> {
        self.inner.get_acl()
    }
    /// <p>Object data.</p>
    pub fn body(mut self, input: crate::io::InputStream) -> Self {
        self.inner = self.inner.body(input);
        self
    }
    /// <p>Object data.</p>
    pub fn set_body(mut self, input: Option<crate::io::InputStream>) -> Self {
        self.inner = self.inner.set_body(input);
        self
    }

    /// <p>Object data.</p>
    pub fn get_body(&self) -> &Option<crate::io::InputStream> {
        self.inner.get_body()
    }

    /// <p>The bucket name to which the PUT action was initiated.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// This field is required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input.into());
        self
    }
    /// <p>The bucket name to which the PUT action was initiated.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_bucket(input);
        self
    }
    /// <p>The bucket name to which the PUT action was initiated.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_bucket(&self) -> &Option<String> {
        self.inner.get_bucket()
    }
    /// <p>Can be used to specify caching behavior along the request/reply chain. For more information, see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
    pub fn cache_control(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.cache_control(input);
        self
    }
    /// <p>Can be used to specify caching behavior along the request/reply chain. For more information, see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
    pub fn set_cache_control(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_cache_control(input);
        self
    }
    /// <p>Can be used to specify caching behavior along the request/reply chain. For more information, see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
    pub fn get_cache_control(&self) -> &Option<String> {
        self.inner.get_cache_control()
    }
    /// <p>Specifies presentational information for the object. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc6266#section-4">https://www.rfc-editor.org/rfc/rfc6266#section-4</a>.</p>
    pub fn content_disposition(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.content_disposition(input);
        self
    }
    /// <p>Specifies presentational information for the object. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc6266#section-4">https://www.rfc-editor.org/rfc/rfc6266#section-4</a>.</p>
    pub fn set_content_disposition(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_content_disposition(input);
        self
    }
    /// <p>Specifies presentational information for the object. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc6266#section-4">https://www.rfc-editor.org/rfc/rfc6266#section-4</a>.</p>
    pub fn get_content_disposition(&self) -> &Option<String> {
        self.inner.get_content_disposition()
    }
    /// <p>Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding">https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding</a>.</p>
    pub fn content_encoding(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.content_encoding(input);
        self
    }
    /// <p>Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding">https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding</a>.</p>
    pub fn set_content_encoding(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_content_encoding(input);
        self
    }
    /// <p>Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding">https://www.rfc-editor.org/rfc/rfc9110.html#field.content-encoding</a>.</p>
    pub fn get_content_encoding(&self) -> &Option<String> {
        self.inner.get_content_encoding()
    }
    /// <p>The language the content is in.</p>
    pub fn content_language(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.content_language(input);
        self
    }
    /// <p>The language the content is in.</p>
    pub fn set_content_language(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_content_language(input);
        self
    }
    /// <p>The language the content is in.</p>
    pub fn get_content_language(&self) -> &Option<String> {
        self.inner.get_content_language()
    }
    /// <p>Size of the body in bytes. This parameter is useful when the size of the body cannot be determined automatically. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length</a>.</p>
    pub fn content_length(mut self, input: i64) -> Self {
        self.inner = self.inner.content_length(input);
        self
    }
    /// <p>Size of the body in bytes. This parameter is useful when the size of the body cannot be determined automatically. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length</a>.</p>
    pub fn set_content_length(mut self, input: Option<i64>) -> Self {
        self.inner = self.inner.set_content_length(input);
        self
    }
    /// <p>Size of the body in bytes. This parameter is useful when the size of the body cannot be determined automatically. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-length</a>.</p>
    pub fn get_content_length(&self) -> &Option<i64> {
        self.inner.get_content_length()
    }
    /// <p>The base64-encoded 128-bit MD5 digest of the message (without the headers) according to RFC 1864. This header can be used as a message integrity check to verify that the data is the same data that was originally sent. Although it is optional, we recommend using the Content-MD5 mechanism as an end-to-end integrity check. For more information about REST request authentication, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST Authentication</a>.</p><note>
    /// <p>The <code>Content-MD5</code> header is required for any request to upload an object with a retention period configured using Amazon S3 Object Lock. For more information about Amazon S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html">Amazon S3 Object Lock Overview</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// </note> <note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn content_md5(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.content_md5(input);
        self
    }
    /// <p>The base64-encoded 128-bit MD5 digest of the message (without the headers) according to RFC 1864. This header can be used as a message integrity check to verify that the data is the same data that was originally sent. Although it is optional, we recommend using the Content-MD5 mechanism as an end-to-end integrity check. For more information about REST request authentication, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST Authentication</a>.</p><note>
    /// <p>The <code>Content-MD5</code> header is required for any request to upload an object with a retention period configured using Amazon S3 Object Lock. For more information about Amazon S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html">Amazon S3 Object Lock Overview</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// </note> <note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_content_md5(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_content_md5(input);
        self
    }
    /// <p>The base64-encoded 128-bit MD5 digest of the message (without the headers) according to RFC 1864. This header can be used as a message integrity check to verify that the data is the same data that was originally sent. Although it is optional, we recommend using the Content-MD5 mechanism as an end-to-end integrity check. For more information about REST request authentication, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST Authentication</a>.</p><note>
    /// <p>The <code>Content-MD5</code> header is required for any request to upload an object with a retention period configured using Amazon S3 Object Lock. For more information about Amazon S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html">Amazon S3 Object Lock Overview</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// </note> <note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_content_md5(&self) -> &Option<String> {
        self.inner.get_content_md5()
    }
    /// <p>A standard MIME type describing the format of the contents. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type</a>.</p>
    pub fn content_type(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.content_type(input);
        self
    }
    /// <p>A standard MIME type describing the format of the contents. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type</a>.</p>
    pub fn set_content_type(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_content_type(input);
        self
    }
    /// <p>A standard MIME type describing the format of the contents. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type">https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type</a>.</p>
    pub fn get_content_type(&self) -> &Option<String> {
        self.inner.get_content_type()
    }
    /// <p>Indicates the algorithm used to create the checksum for the object when you use the SDK. This header will not provide any additional functionality if you don't use the SDK. When you send this header, there must be a corresponding <code>x-amz-checksum-<i>algorithm</i> </code> or <code>x-amz-trailer</code> header sent. Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad Request</code>.</p>
    /// <p>For the <code>x-amz-checksum-<i>algorithm</i> </code> header, replace <code> <i>algorithm</i> </code> with the supported algorithm from the following list:</p>
    /// <ul>
    /// <li>
    /// <p>CRC32</p></li>
    /// <li>
    /// <p>CRC32C</p></li>
    /// <li>
    /// <p>SHA1</p></li>
    /// <li>
    /// <p>SHA256</p></li>
    /// </ul>
    /// <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the individual checksum value you provide through <code>x-amz-checksum-<i>algorithm</i> </code> doesn't match the checksum algorithm you set through <code>x-amz-sdk-checksum-algorithm</code>, Amazon S3 ignores any provided <code>ChecksumAlgorithm</code> parameter and uses the checksum algorithm that matches the provided value in <code>x-amz-checksum-<i>algorithm</i> </code>.</p><note>
    /// <p>For directory buckets, when you use Amazon Web Services SDKs, <code>CRC32</code> is the default checksum algorithm that's used for performance.</p>
    /// </note>
    pub fn checksum_algorithm(mut self, input: aws_sdk_s3::types::ChecksumAlgorithm) -> Self {
        self.inner = self.inner.checksum_algorithm(input);
        self
    }
    /// <p>Indicates the algorithm used to create the checksum for the object when you use the SDK. This header will not provide any additional functionality if you don't use the SDK. When you send this header, there must be a corresponding <code>x-amz-checksum-<i>algorithm</i> </code> or <code>x-amz-trailer</code> header sent. Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad Request</code>.</p>
    /// <p>For the <code>x-amz-checksum-<i>algorithm</i> </code> header, replace <code> <i>algorithm</i> </code> with the supported algorithm from the following list:</p>
    /// <ul>
    /// <li>
    /// <p>CRC32</p></li>
    /// <li>
    /// <p>CRC32C</p></li>
    /// <li>
    /// <p>SHA1</p></li>
    /// <li>
    /// <p>SHA256</p></li>
    /// </ul>
    /// <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the individual checksum value you provide through <code>x-amz-checksum-<i>algorithm</i> </code> doesn't match the checksum algorithm you set through <code>x-amz-sdk-checksum-algorithm</code>, Amazon S3 ignores any provided <code>ChecksumAlgorithm</code> parameter and uses the checksum algorithm that matches the provided value in <code>x-amz-checksum-<i>algorithm</i> </code>.</p><note>
    /// <p>For directory buckets, when you use Amazon Web Services SDKs, <code>CRC32</code> is the default checksum algorithm that's used for performance.</p>
    /// </note>
    pub fn set_checksum_algorithm(
        mut self,
        input: Option<aws_sdk_s3::types::ChecksumAlgorithm>,
    ) -> Self {
        self.inner = self.inner.set_checksum_algorithm(input);
        self
    }
    /// <p>Indicates the algorithm used to create the checksum for the object when you use the SDK. This header will not provide any additional functionality if you don't use the SDK. When you send this header, there must be a corresponding <code>x-amz-checksum-<i>algorithm</i> </code> or <code>x-amz-trailer</code> header sent. Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad Request</code>.</p>
    /// <p>For the <code>x-amz-checksum-<i>algorithm</i> </code> header, replace <code> <i>algorithm</i> </code> with the supported algorithm from the following list:</p>
    /// <ul>
    /// <li>
    /// <p>CRC32</p></li>
    /// <li>
    /// <p>CRC32C</p></li>
    /// <li>
    /// <p>SHA1</p></li>
    /// <li>
    /// <p>SHA256</p></li>
    /// </ul>
    /// <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>If the individual checksum value you provide through <code>x-amz-checksum-<i>algorithm</i> </code> doesn't match the checksum algorithm you set through <code>x-amz-sdk-checksum-algorithm</code>, Amazon S3 ignores any provided <code>ChecksumAlgorithm</code> parameter and uses the checksum algorithm that matches the provided value in <code>x-amz-checksum-<i>algorithm</i> </code>.</p><note>
    /// <p>For directory buckets, when you use Amazon Web Services SDKs, <code>CRC32</code> is the default checksum algorithm that's used for performance.</p>
    /// </note>
    pub fn get_checksum_algorithm(&self) -> &Option<aws_sdk_s3::types::ChecksumAlgorithm> {
        self.inner.get_checksum_algorithm()
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32 checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.checksum_crc32(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32 checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_crc32(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_checksum_crc32(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32 checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_crc32(&self) -> &Option<String> {
        self.inner.get_checksum_crc32()
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32C checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_crc32_c(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.checksum_crc32_c(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32C checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_crc32_c(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_checksum_crc32_c(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 32-bit CRC32C checksum of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_crc32_c(&self) -> &Option<String> {
        self.inner.get_checksum_crc32_c()
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha1(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.checksum_sha1(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_sha1(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_checksum_sha1(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_sha1(&self) -> &Option<String> {
        self.inner.get_checksum_sha1()
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 256-bit SHA-256 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn checksum_sha256(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.checksum_sha256(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 256-bit SHA-256 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn set_checksum_sha256(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_checksum_sha256(input);
        self
    }
    /// <p>This header can be used as a data integrity check to verify that the data received is the same data that was originally sent. This header specifies the base64-encoded, 256-bit SHA-256 digest of the object. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_checksum_sha256(&self) -> &Option<String> {
        self.inner.get_checksum_sha256()
    }
    /// <p>The date and time at which the object is no longer cacheable. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc7234#section-5.3">https://www.rfc-editor.org/rfc/rfc7234#section-5.3</a>.</p>
    pub fn expires(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.inner = self.inner.expires(input);
        self
    }
    /// <p>The date and time at which the object is no longer cacheable. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc7234#section-5.3">https://www.rfc-editor.org/rfc/rfc7234#section-5.3</a>.</p>
    pub fn set_expires(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.inner = self.inner.set_expires(input);
        self
    }
    /// <p>The date and time at which the object is no longer cacheable. For more information, see <a href="https://www.rfc-editor.org/rfc/rfc7234#section-5.3">https://www.rfc-editor.org/rfc/rfc7234#section-5.3</a>.</p>
    pub fn get_expires(&self) -> &Option<::aws_smithy_types::DateTime> {
        self.inner.get_expires()
    }
    /// <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn grant_full_control(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.grant_full_control(input);
        self
    }
    /// <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn set_grant_full_control(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_grant_full_control(input);
        self
    }
    /// <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn get_grant_full_control(&self) -> &Option<String> {
        self.inner.get_grant_full_control()
    }
    /// <p>Allows grantee to read the object data and its metadata.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn grant_read(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.grant_read(input);
        self
    }
    /// <p>Allows grantee to read the object data and its metadata.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn set_grant_read(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_grant_read(input);
        self
    }
    /// <p>Allows grantee to read the object data and its metadata.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn get_grant_read(&self) -> &Option<String> {
        self.inner.get_grant_read()
    }
    /// <p>Allows grantee to read the object ACL.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn grant_read_acp(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.grant_read_acp(input);
        self
    }
    /// <p>Allows grantee to read the object ACL.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn set_grant_read_acp(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_grant_read_acp(input);
        self
    }
    /// <p>Allows grantee to read the object ACL.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn get_grant_read_acp(&self) -> &Option<String> {
        self.inner.get_grant_read_acp()
    }
    /// <p>Allows grantee to write the ACL for the applicable object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn grant_write_acp(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.grant_write_acp(input);
        self
    }
    /// <p>Allows grantee to write the ACL for the applicable object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn set_grant_write_acp(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_grant_write_acp(input);
        self
    }
    /// <p>Allows grantee to write the ACL for the applicable object.</p><note>
    /// <ul>
    /// <li>
    /// <p>This functionality is not supported for directory buckets.</p></li>
    /// <li>
    /// <p>This functionality is not supported for Amazon S3 on Outposts.</p></li>
    /// </ul>
    /// </note>
    pub fn get_grant_write_acp(&self) -> &Option<String> {
        self.inner.get_grant_write_acp()
    }
    /// <p>Object key for which the PUT action was initiated.</p>
    /// This field is required.
    pub fn key(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key(input);
        self
    }
    /// <p>Object key for which the PUT action was initiated.</p>
    pub fn set_key(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key(input);
        self
    }
    /// <p>Object key for which the PUT action was initiated.</p>
    pub fn get_key(&self) -> &Option<String> {
        self.inner.get_key()
    }
    /// Adds a key-value pair to `metadata`.
    ///
    /// To override the contents of this collection use [`set_metadata`](Self::set_metadata).
    ///
    /// <p>A map of metadata to store with the object in S3.</p>
    pub fn metadata(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.inner = self.inner.metadata(k, v);
        self
    }
    /// <p>A map of metadata to store with the object in S3.</p>
    pub fn set_metadata(
        mut self,
        input: Option<::std::collections::HashMap<String, String>>,
    ) -> Self {
        self.inner = self.inner.set_metadata(input);
        self
    }
    /// <p>A map of metadata to store with the object in S3.</p>
    pub fn get_metadata(&self) -> &Option<::std::collections::HashMap<String, String>> {
        self.inner.get_metadata()
    }
    /// <p>The server-side encryption algorithm that was used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p>
    /// <p><b>General purpose buckets </b> - You have four mutually exclusive options to protect data using server-side encryption in Amazon S3, depending on how you choose to manage the encryption keys. Specifically, the encryption key options are Amazon S3 managed keys (SSE-S3), Amazon Web Services KMS keys (SSE-KMS or DSSE-KMS), and customer-provided keys (SSE-C). Amazon S3 encrypts data with server-side encryption by using Amazon S3 managed keys (SSE-S3) by default. You can optionally tell Amazon S3 to encrypt data at rest by using server-side encryption with other key options. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">Using Server-Side Encryption</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Directory buckets </b> - For directory buckets, only the server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) value is supported.</p>
    pub fn server_side_encryption(
        mut self,
        input: aws_sdk_s3::types::ServerSideEncryption,
    ) -> Self {
        self.inner = self.inner.server_side_encryption(input);
        self
    }
    /// <p>The server-side encryption algorithm that was used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p>
    /// <p><b>General purpose buckets </b> - You have four mutually exclusive options to protect data using server-side encryption in Amazon S3, depending on how you choose to manage the encryption keys. Specifically, the encryption key options are Amazon S3 managed keys (SSE-S3), Amazon Web Services KMS keys (SSE-KMS or DSSE-KMS), and customer-provided keys (SSE-C). Amazon S3 encrypts data with server-side encryption by using Amazon S3 managed keys (SSE-S3) by default. You can optionally tell Amazon S3 to encrypt data at rest by using server-side encryption with other key options. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">Using Server-Side Encryption</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Directory buckets </b> - For directory buckets, only the server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) value is supported.</p>
    pub fn set_server_side_encryption(
        mut self,
        input: Option<aws_sdk_s3::types::ServerSideEncryption>,
    ) -> Self {
        self.inner = self.inner.set_server_side_encryption(input);
        self
    }
    /// <p>The server-side encryption algorithm that was used when you store this object in Amazon S3 (for example, <code>AES256</code>, <code>aws:kms</code>, <code>aws:kms:dsse</code>).</p>
    /// <p><b>General purpose buckets </b> - You have four mutually exclusive options to protect data using server-side encryption in Amazon S3, depending on how you choose to manage the encryption keys. Specifically, the encryption key options are Amazon S3 managed keys (SSE-S3), Amazon Web Services KMS keys (SSE-KMS or DSSE-KMS), and customer-provided keys (SSE-C). Amazon S3 encrypts data with server-side encryption by using Amazon S3 managed keys (SSE-S3) by default. You can optionally tell Amazon S3 to encrypt data at rest by using server-side encryption with other key options. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">Using Server-Side Encryption</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Directory buckets </b> - For directory buckets, only the server-side encryption with Amazon S3 managed keys (SSE-S3) (<code>AES256</code>) value is supported.</p>
    pub fn get_server_side_encryption(&self) -> &Option<aws_sdk_s3::types::ServerSideEncryption> {
        self.inner.get_server_side_encryption()
    }
    /// <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects. The STANDARD storage class provides high durability and high availability. Depending on performance needs, you can specify a different Storage Class. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage Classes</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>For directory buckets, only the S3 Express One Zone storage class is supported to store newly created objects.</p></li>
    /// <li>
    /// <p>Amazon S3 on Outposts only uses the OUTPOSTS Storage Class.</p></li>
    /// </ul>
    /// </note>
    pub fn storage_class(mut self, input: aws_sdk_s3::types::StorageClass) -> Self {
        self.inner = self.inner.storage_class(input);
        self
    }
    /// <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects. The STANDARD storage class provides high durability and high availability. Depending on performance needs, you can specify a different Storage Class. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage Classes</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>For directory buckets, only the S3 Express One Zone storage class is supported to store newly created objects.</p></li>
    /// <li>
    /// <p>Amazon S3 on Outposts only uses the OUTPOSTS Storage Class.</p></li>
    /// </ul>
    /// </note>
    pub fn set_storage_class(mut self, input: Option<aws_sdk_s3::types::StorageClass>) -> Self {
        self.inner = self.inner.set_storage_class(input);
        self
    }
    /// <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects. The STANDARD storage class provides high durability and high availability. Depending on performance needs, you can specify a different Storage Class. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage Classes</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <ul>
    /// <li>
    /// <p>For directory buckets, only the S3 Express One Zone storage class is supported to store newly created objects.</p></li>
    /// <li>
    /// <p>Amazon S3 on Outposts only uses the OUTPOSTS Storage Class.</p></li>
    /// </ul>
    /// </note>
    pub fn get_storage_class(&self) -> &Option<aws_sdk_s3::types::StorageClass> {
        self.inner.get_storage_class()
    }
    /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata. For information about object metadata, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object Key and Metadata</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>In the following example, the request header sets the redirect to an object (anotherPage.html) in the same bucket:</p>
    /// <p><code>x-amz-website-redirect-location: /anotherPage.html</code></p>
    /// <p>In the following example, the request header sets the object redirect to another website:</p>
    /// <p><code>x-amz-website-redirect-location: http://www.example.com/</code></p>
    /// <p>For more information about website hosting in Amazon S3, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting Websites on Amazon S3</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How to Configure Website Page Redirects</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn website_redirect_location(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.website_redirect_location(input);
        self
    }
    /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata. For information about object metadata, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object Key and Metadata</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>In the following example, the request header sets the redirect to an object (anotherPage.html) in the same bucket:</p>
    /// <p><code>x-amz-website-redirect-location: /anotherPage.html</code></p>
    /// <p>In the following example, the request header sets the object redirect to another website:</p>
    /// <p><code>x-amz-website-redirect-location: http://www.example.com/</code></p>
    /// <p>For more information about website hosting in Amazon S3, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting Websites on Amazon S3</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How to Configure Website Page Redirects</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_website_redirect_location(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_website_redirect_location(input);
        self
    }
    /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata. For information about object metadata, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object Key and Metadata</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p>In the following example, the request header sets the redirect to an object (anotherPage.html) in the same bucket:</p>
    /// <p><code>x-amz-website-redirect-location: /anotherPage.html</code></p>
    /// <p>In the following example, the request header sets the object redirect to another website:</p>
    /// <p><code>x-amz-website-redirect-location: http://www.example.com/</code></p>
    /// <p>For more information about website hosting in Amazon S3, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting Websites on Amazon S3</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How to Configure Website Page Redirects</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_website_redirect_location(&self) -> &Option<String> {
        self.inner.get_website_redirect_location()
    }
    /// <p>Specifies the algorithm to use when encrypting the object (for example, <code>AES256</code>).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_algorithm(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.sse_customer_algorithm(input);
        self
    }
    /// <p>Specifies the algorithm to use when encrypting the object (for example, <code>AES256</code>).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_algorithm(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_sse_customer_algorithm(input);
        self
    }
    /// <p>Specifies the algorithm to use when encrypting the object (for example, <code>AES256</code>).</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_algorithm(&self) -> &Option<String> {
        self.inner.get_sse_customer_algorithm()
    }
    /// <p>Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.sse_customer_key(input);
        self
    }
    /// <p>Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_key(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_sse_customer_key(input);
        self
    }
    /// <p>Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_key(&self) -> &Option<String> {
        self.inner.get_sse_customer_key()
    }
    /// <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_customer_key_md5(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.sse_customer_key_md5(input);
        self
    }
    /// <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_customer_key_md5(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_sse_customer_key_md5(input);
        self
    }
    /// <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_customer_key_md5(&self) -> &Option<String> {
        self.inner.get_sse_customer_key_md5()
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header specifies the ID (Key ID, Key ARN, or Key Alias) of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object. If you specify <code>x-amz-server-side-encryption:aws:kms</code> or <code>x-amz-server-side-encryption:aws:kms:dsse</code>, but do not provide<code> x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon Web Services managed key (<code>aws/s3</code>) to protect the data. If the KMS key does not exist in the same account that's issuing the command, you must use the full ARN and not just the ID.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_key_id(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.sse_kms_key_id(input);
        self
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header specifies the ID (Key ID, Key ARN, or Key Alias) of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object. If you specify <code>x-amz-server-side-encryption:aws:kms</code> or <code>x-amz-server-side-encryption:aws:kms:dsse</code>, but do not provide<code> x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon Web Services managed key (<code>aws/s3</code>) to protect the data. If the KMS key does not exist in the same account that's issuing the command, you must use the full ARN and not just the ID.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_kms_key_id(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_sse_kms_key_id(input);
        self
    }
    /// <p>If <code>x-amz-server-side-encryption</code> has a valid value of <code>aws:kms</code> or <code>aws:kms:dsse</code>, this header specifies the ID (Key ID, Key ARN, or Key Alias) of the Key Management Service (KMS) symmetric encryption customer managed key that was used for the object. If you specify <code>x-amz-server-side-encryption:aws:kms</code> or <code>x-amz-server-side-encryption:aws:kms:dsse</code>, but do not provide<code> x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon Web Services managed key (<code>aws/s3</code>) to protect the data. If the KMS key does not exist in the same account that's issuing the command, you must use the full ARN and not just the ID.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_kms_key_id(&self) -> &Option<String> {
        self.inner.get_sse_kms_key_id()
    }
    /// <p>Specifies the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object. This value must be explicitly added during <code>CopyObject</code> operations.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn sse_kms_encryption_context(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.sse_kms_encryption_context(input);
        self
    }
    /// <p>Specifies the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object. This value must be explicitly added during <code>CopyObject</code> operations.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_sse_kms_encryption_context(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_sse_kms_encryption_context(input);
        self
    }
    /// <p>Specifies the Amazon Web Services KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs. This value is stored as object metadata and automatically gets passed on to Amazon Web Services KMS for future <code>GetObject</code> or <code>CopyObject</code> operations on this object. This value must be explicitly added during <code>CopyObject</code> operations.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_sse_kms_encryption_context(&self) -> &Option<String> {
        self.inner.get_sse_kms_encryption_context()
    }
    /// <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS). Setting this header to <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS.</p>
    /// <p>Specifying this header with a PUT action doesn’t affect bucket-level settings for S3 Bucket Key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn bucket_key_enabled(mut self, input: bool) -> Self {
        self.inner = self.inner.bucket_key_enabled(input);
        self
    }
    /// <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS). Setting this header to <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS.</p>
    /// <p>Specifying this header with a PUT action doesn’t affect bucket-level settings for S3 Bucket Key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_bucket_key_enabled(mut self, input: Option<bool>) -> Self {
        self.inner = self.inner.set_bucket_key_enabled(input);
        self
    }
    /// <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with server-side encryption using Key Management Service (KMS) keys (SSE-KMS). Setting this header to <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS.</p>
    /// <p>Specifying this header with a PUT action doesn’t affect bucket-level settings for S3 Bucket Key.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_bucket_key_enabled(&self) -> &Option<bool> {
        self.inner.get_bucket_key_enabled()
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn request_payer(mut self, input: aws_sdk_s3::types::RequestPayer) -> Self {
        self.inner = self.inner.request_payer(input);
        self
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_request_payer(mut self, input: Option<aws_sdk_s3::types::RequestPayer>) -> Self {
        self.inner = self.inner.set_request_payer(input);
        self
    }
    /// <p>Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests. If either the source or destination S3 bucket has Requester Pays enabled, the requester will pay for corresponding charges to copy the object. For information about downloading objects from Requester Pays buckets, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html">Downloading Objects in Requester Pays Buckets</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_request_payer(&self) -> &Option<aws_sdk_s3::types::RequestPayer> {
        self.inner.get_request_payer()
    }
    /// <p>The tag-set for the object. The tag-set must be encoded as URL Query parameters. (For example, "Key1=Value1")</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn tagging(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.tagging(input);
        self
    }
    /// <p>The tag-set for the object. The tag-set must be encoded as URL Query parameters. (For example, "Key1=Value1")</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_tagging(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_tagging(input);
        self
    }
    /// <p>The tag-set for the object. The tag-set must be encoded as URL Query parameters. (For example, "Key1=Value1")</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_tagging(&self) -> &Option<String> {
        self.inner.get_tagging()
    }
    /// <p>The Object Lock mode that you want to apply to this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn object_lock_mode(mut self, input: aws_sdk_s3::types::ObjectLockMode) -> Self {
        self.inner = self.inner.object_lock_mode(input);
        self
    }
    /// <p>The Object Lock mode that you want to apply to this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_object_lock_mode(
        mut self,
        input: Option<aws_sdk_s3::types::ObjectLockMode>,
    ) -> Self {
        self.inner = self.inner.set_object_lock_mode(input);
        self
    }
    /// <p>The Object Lock mode that you want to apply to this object.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_object_lock_mode(&self) -> &Option<aws_sdk_s3::types::ObjectLockMode> {
        self.inner.get_object_lock_mode()
    }
    /// <p>The date and time when you want this object's Object Lock to expire. Must be formatted as a timestamp parameter.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn object_lock_retain_until_date(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.inner = self.inner.object_lock_retain_until_date(input);
        self
    }
    /// <p>The date and time when you want this object's Object Lock to expire. Must be formatted as a timestamp parameter.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_object_lock_retain_until_date(
        mut self,
        input: Option<::aws_smithy_types::DateTime>,
    ) -> Self {
        self.inner = self.inner.set_object_lock_retain_until_date(input);
        self
    }
    /// <p>The date and time when you want this object's Object Lock to expire. Must be formatted as a timestamp parameter.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_object_lock_retain_until_date(&self) -> &Option<::aws_smithy_types::DateTime> {
        self.inner.get_object_lock_retain_until_date()
    }
    /// <p>Specifies whether a legal hold will be applied to this object. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn object_lock_legal_hold_status(
        mut self,
        input: aws_sdk_s3::types::ObjectLockLegalHoldStatus,
    ) -> Self {
        self.inner = self.inner.object_lock_legal_hold_status(input);
        self
    }
    /// <p>Specifies whether a legal hold will be applied to this object. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn set_object_lock_legal_hold_status(
        mut self,
        input: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,
    ) -> Self {
        self.inner = self.inner.set_object_lock_legal_hold_status(input);
        self
    }
    /// <p>Specifies whether a legal hold will be applied to this object. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a> in the <i>Amazon S3 User Guide</i>.</p><note>
    /// <p>This functionality is not supported for directory buckets.</p>
    /// </note>
    pub fn get_object_lock_legal_hold_status(
        &self,
    ) -> &Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus> {
        self.inner.get_object_lock_legal_hold_status()
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn expected_bucket_owner(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.expected_bucket_owner(input);
        self
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn set_expected_bucket_owner(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_expected_bucket_owner(input);
        self
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn get_expected_bucket_owner(&self) -> &Option<String> {
        self.inner.get_expected_bucket_owner()
    }

    /// The policy that describes how to handle a failed multipart upload.
    pub fn failed_multipart_upload_policy(mut self, input: FailedMultipartUploadPolicy) -> Self {
        self.inner = self.inner.failed_multipart_upload_policy(input);
        self
    }

    /// The policy that describes how to handle a failed multipart upload.
    pub fn set_failed_multipart_upload_policy(
        mut self,
        input: Option<FailedMultipartUploadPolicy>,
    ) -> Self {
        self.inner = self.inner.set_failed_multipart_upload_policy(input);
        self
    }

    /// The policy that describes how to handle a failed multipart upload.
    pub fn get_failed_multipart_upload_policy(&self) -> &Option<FailedMultipartUploadPolicy> {
        self.inner.get_failed_multipart_upload_policy()
    }
}

impl crate::operation::upload::input::UploadInputBuilder {
    /// Initiate an upload transfer for a single object with this input using the given client.
    pub async fn send_with(
        self,
        client: &crate::Client,
    ) -> Result<UploadHandle, crate::error::Error> {
        let mut fluent_builder = client.upload();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
