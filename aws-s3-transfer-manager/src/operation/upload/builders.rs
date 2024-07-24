/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use crate::error::UploadError;

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
    pub async fn send(self) -> Result<UploadHandle, UploadError> {
        // FIXME - need UploadError to support this conversion to remove expect() in favor of ?
        let input = self.inner.build().expect("valid input");
        crate::operation::upload::Upload::orchestrate(self.handle, input).await
    }

    // TODO - all the builder setters and getters, for now use send_with()
}

impl crate::operation::upload::input::UploadInputBuilder {
    /// Initiate an upload transfer for a single object with this input using the given client.
    pub async fn send_with(self, client: &crate::Client) -> Result<UploadHandle, UploadError> {
        let mut fluent_builder = client.upload();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
