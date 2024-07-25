/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::sync::Arc;

use crate::error::TransferError;

use super::{DownloadHandle, DownloadInputBuilder};

/// Fluent builder for constructing a single object upload transfer
#[derive(Debug)]
pub struct DownloadFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: DownloadInputBuilder,
}

impl DownloadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
        }
    }

    /// Initiate an upload transfer for a single object
    pub async fn send(self) -> Result<DownloadHandle, TransferError> {
        // FIXME - need DownloadError to support this conversion to remove expect() in favor of ?
        let input = self.inner.build().expect("valid input");
        crate::operation::download::Download::orchestrate(self.handle, input).await
    }

    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input.into());
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
        self.inner = self.inner.set_bucket(input);
        self
    }
    /// <p>The bucket name containing the object.</p>
    /// <p><b>Directory buckets</b> - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format <code> <i>Bucket_name</i>.s3express-<i>az_id</i>.<i>region</i>.amazonaws.com</code>. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format <code> <i>bucket_base_name</i>--<i>az-id</i>--x-s3</code> (for example, <code> <i>DOC-EXAMPLE-BUCKET</i>--<i>usw2-az1</i>--x-s3</code>). For information about bucket naming restrictions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html">Directory bucket naming rules</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Access points</b> - When you use this action with an access point, you must provide the alias of the access point in place of the bucket name or specify the access point ARN. When using the access point ARN, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this action with an access point through the Amazon Web Services SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using access points</a> in the <i>Amazon S3 User Guide</i>.</p>
    /// <p><b>Object Lambda access points</b> - When you use this action with an Object Lambda access point, you must direct requests to the Object Lambda access point hostname. The Object Lambda access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p><note>
    /// <p>Access points and Object Lambda access points are not supported by directory buckets.</p>
    /// </note>
    /// <p><b>S3 on Outposts</b> - When you use this action with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <code> <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>. When you use this action with S3 on Outposts through the Amazon Web Services SDKs, you provide the Outposts access point ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">What is S3 on Outposts?</a> in the <i>Amazon S3 User Guide</i>.</p>
    pub fn get_bucket(&self) -> &Option<String> {
        self.inner.get_bucket()
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_match(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.if_match(input.into());
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_match(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_if_match(input);
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified in this header; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_match(&self) -> &Option<String> {
        self.inner.get_if_match()
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_modified_since(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.inner = self.inner.if_modified_since(input);
        self
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_modified_since(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.inner = self.inner.set_if_modified_since(input);
        self
    }
    /// <p>Return the object only if it has been modified since the specified time; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_modified_since(&self) -> &Option<::aws_smithy_types::DateTime> {
        self.inner.get_if_modified_since()
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_none_match(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.if_none_match(input.into());
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_none_match(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_if_none_match(input);
        self
    }
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified in this header; otherwise, return a <code>304 Not Modified</code> error.</p>
    /// <p>If both of the <code>If-None-Match</code> and <code>If-Modified-Since</code> headers are present in the request as follows:<code> If-None-Match</code> condition evaluates to <code>false</code>, and; <code>If-Modified-Since</code> condition evaluates to <code>true</code>; then, S3 returns <code>304 Not Modified</code> HTTP status code.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_none_match(&self) -> &Option<String> {
        self.inner.get_if_none_match()
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn if_unmodified_since(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.inner = self.inner.if_unmodified_since(input);
        self
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn set_if_unmodified_since(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.inner = self.inner.set_if_unmodified_since(input);
        self
    }
    /// <p>Return the object only if it has not been modified since the specified time; otherwise, return a <code>412 Precondition Failed</code> error.</p>
    /// <p>If both of the <code>If-Match</code> and <code>If-Unmodified-Since</code> headers are present in the request as follows: <code>If-Match</code> condition evaluates to <code>true</code>, and; <code>If-Unmodified-Since</code> condition evaluates to <code>false</code>; then, S3 returns <code>200 OK</code> and the data requested.</p>
    /// <p>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>.</p>
    pub fn get_if_unmodified_since(&self) -> &Option<::aws_smithy_types::DateTime> {
        self.inner.get_if_unmodified_since()
    }
    /// <p>Key of the object to get.</p>
    pub fn key(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key(input.into());
        self
    }
    /// <p>Key of the object to get.</p>
    pub fn set_key(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key(input);
        self
    }
    /// <p>Key of the object to get.</p>
    pub fn get_key(&self) -> &Option<String> {
        self.inner.get_key()
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn range(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.range(input.into());
        self
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn set_range(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_range(input);
        self
    }
    /// <p>Downloads the specified byte range of an object. For more information about the HTTP Range header, see <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">https://www.rfc-editor.org/rfc/rfc9110.html#name-range</a>.</p><note>
    /// <p>Amazon S3 doesn't support retrieving multiple ranges of data per <code>GET</code> request.</p>
    /// </note>
    pub fn get_range(&self) -> &Option<String> {
        self.inner.get_range()
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn response_cache_control(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.response_cache_control(input.into());
        self
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn set_response_cache_control(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_response_cache_control(input);
        self
    }
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub fn get_response_cache_control(&self) -> &Option<String> {
        self.inner.get_response_cache_control()
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn response_content_disposition(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.response_content_disposition(input.into());
        self
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn set_response_content_disposition(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_response_content_disposition(input);
        self
    }
    /// <p>Sets the <code>Content-Disposition</code> header of the response.</p>
    pub fn get_response_content_disposition(&self) -> &Option<String> {
        self.inner.get_response_content_disposition()
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn response_content_encoding(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.response_content_encoding(input.into());
        self
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn set_response_content_encoding(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_response_content_encoding(input);
        self
    }
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub fn get_response_content_encoding(&self) -> &Option<String> {
        self.inner.get_response_content_encoding()
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn response_content_language(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.response_content_language(input.into());
        self
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn set_response_content_language(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_response_content_language(input);
        self
    }
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub fn get_response_content_language(&self) -> &Option<String> {
        self.inner.get_response_content_language()
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn response_content_type(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.response_content_type(input.into());
        self
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn set_response_content_type(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_response_content_type(input);
        self
    }
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub fn get_response_content_type(&self) -> &Option<String> {
        self.inner.get_response_content_type()
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn response_expires(mut self, input: ::aws_smithy_types::DateTime) -> Self {
        self.inner = self.inner.response_expires(input);
        self
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn set_response_expires(mut self, input: Option<::aws_smithy_types::DateTime>) -> Self {
        self.inner = self.inner.set_response_expires(input);
        self
    }
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub fn get_response_expires(&self) -> &Option<::aws_smithy_types::DateTime> {
        self.inner.get_response_expires()
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
        self.inner = self.inner.version_id(input.into());
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
        self.inner = self.inner.set_version_id(input);
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
    pub fn get_version_id(&self) -> &Option<String> {
        self.inner.get_version_id()
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
        self.inner = self.inner.sse_customer_algorithm(input.into());
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
        self.inner = self.inner.set_sse_customer_algorithm(input);
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
    pub fn get_sse_customer_algorithm(&self) -> &Option<String> {
        self.inner.get_sse_customer_algorithm()
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
        self.inner = self.inner.sse_customer_key(input.into());
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
        self.inner = self.inner.set_sse_customer_key(input);
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
    pub fn get_sse_customer_key(&self) -> &Option<String> {
        self.inner.get_sse_customer_key()
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
        self.inner = self.inner.sse_customer_key_md5(input.into());
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
        self.inner = self.inner.set_sse_customer_key_md5(input);
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
    pub fn get_sse_customer_key_md5(&self) -> &Option<String> {
        self.inner.get_sse_customer_key_md5()
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
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub fn part_number(mut self, input: i32) -> Self {
        self.inner = self.inner.part_number(input);
        self
    }
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub fn set_part_number(mut self, input: Option<i32>) -> Self {
        self.inner = self.inner.set_part_number(input);
        self
    }
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub fn get_part_number(&self) -> &Option<i32> {
        self.inner.get_part_number()
    }
    /// <p>The account ID of the expected bucket owner. If the account ID that you provide does not match the actual owner of the bucket, the request fails with the HTTP status code <code>403 Forbidden</code> (access denied).</p>
    pub fn expected_bucket_owner(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.expected_bucket_owner(input.into());
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
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn checksum_mode(mut self, input: aws_sdk_s3::types::ChecksumMode) -> Self {
        self.inner = self.inner.checksum_mode(input);
        self
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn set_checksum_mode(mut self, input: Option<aws_sdk_s3::types::ChecksumMode>) -> Self {
        self.inner = self.inner.set_checksum_mode(input);
        self
    }
    /// <p>To retrieve the checksum, this mode must be enabled.</p>
    pub fn get_checksum_mode(&self) -> &Option<aws_sdk_s3::types::ChecksumMode> {
        self.inner.get_checksum_mode()
    }
}

impl crate::operation::download::input::DownloadInputBuilder {
    /// Initiate an upload transfer for a single object with this input using the given client.
    pub async fn send_with(self, client: &crate::Client) -> Result<DownloadHandle, TransferError> {
        let mut fluent_builder = client.download();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
