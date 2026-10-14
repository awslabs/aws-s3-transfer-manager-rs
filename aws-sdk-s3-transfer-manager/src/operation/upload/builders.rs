/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use super::{UploadHandle, UploadInputBuilder};

/// Fluent builder for constructing a single object upload transfer.
///
/// Field methods delegate to the modeled input builder. Getter methods return
/// references to optional construction state; use `as_deref()` to borrow strings.
#[derive(Debug)]
pub struct UploadFluentBuilder {
    handle: Arc<crate::client::Handle>,
    pub(crate) inner: UploadInputBuilder,
}

impl UploadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: Default::default(),
        }
    }

    /// Initiate an upload transfer for a single object.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-upload", fields(
        bucket = self.inner.get_bucket().as_deref().unwrap_or_default(),
        key = self.inner.get_key().as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<UploadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::upload::Upload::orchestrate(self.handle, input)
    }
}

impl UploadInputBuilder {
    /// Initiate an upload transfer for a single object using the given client.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<UploadHandle, crate::error::Error> {
        let mut fluent_builder = client.upload();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }
}
