/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    error::TransferError,
    types::{DownloadFilter, FailedTransferPolicy},
};
use std::{path::PathBuf, sync::Arc};

use super::{DownloadObjectsHandle, DownloadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object download transfer
#[derive(Debug)]
pub struct DownloadObjectsFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: DownloadObjectsInputBuilder,
}

impl DownloadObjectsFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
        }
    }

    /// Initiate an upload transfer for a single object
    pub async fn send(self) -> Result<DownloadObjectsHandle, TransferError> {
        // FIXME - need DownloadError to support this conversion to remove expect() in favor of ?
        let input = self.inner.build().expect("valid input");
        crate::operation::download_objects::DownloadObjects::orchestrate(self.handle, input).await
    }

    /// Set the bucket name containing the object(s) to download.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input);
        self
    }

    /// Set the bucket name containing the object(s) to download.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_bucket(input);
        self
    }

    /// The bucket name containing the object(s).
    pub fn get_bucket(&self) -> &Option<String> {
        self.inner.get_bucket()
    }

    /// Set the destination directory to which files should be downloaded
    pub fn destination(mut self, input: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.destination(input);
        self
    }

    /// Set the destination directory to which files should be downloaded
    pub fn set_destination(mut self, input: Option<PathBuf>) -> Self {
        self.inner = self.inner.set_destination(input);
        self
    }

    /// The destination directory to which files should be downloaded
    pub fn get_destination(&self) -> &Option<PathBuf> {
        self.inner.get_destination()
    }

    /// Limit the response to keys that begin with the given prefix
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key_prefix(input);
        self
    }

    /// Limit the response to keys that begin with the given prefix
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key_prefix(input);
        self
    }

    /// The key_prefix name containing the object(s).
    pub fn get_key_prefix(&self) -> &Option<String> {
        self.inner.get_key_prefix()
    }

    /// Character used to group keys
    pub fn delimiter(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.delimiter(input);
        self
    }

    /// Character used to group keys
    pub fn set_delimiter(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_delimiter(input);
        self
    }

    /// Character used to group keys
    pub fn get_delimiter(&self) -> &Option<String> {
        self.inner.get_delimiter()
    }

    /// The failure policy to use when any individual object download fails.
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner = self.inner.failure_policy(input);
        self
    }

    /// The failure policy to use when any individual object download fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.get_failure_policy()
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn filter(mut self, input: impl Fn(&aws_sdk_s3::types::Object) -> bool + 'static) -> Self {
        self.inner = self.inner.filter(input);
        self
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn set_filter(mut self, input: Option<DownloadFilter>) -> Self {
        self.inner = self.inner.set_filter(input);
        self
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn get_filter(&self) -> &Option<DownloadFilter> {
        self.inner.get_filter()
    }
}

impl crate::operation::download_objects::input::DownloadObjectsInputBuilder {
    /// Initiate a download transfer for multiple objects with this input using the given client.
    pub async fn send_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadObjectsHandle, TransferError> {
        let mut fluent_builder = client.download_objects();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
