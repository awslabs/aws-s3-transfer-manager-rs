/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::sync::Arc;

use super::{DownloadHandle, DownloadInputBuilder, ManagedDownloadHandle};

/// Fluent builder for constructing a single object download transfer.
#[derive(Debug)]
pub struct DownloadFluentBuilder {
    handle: Arc<crate::client::Handle>,
    pub(crate) inner: DownloadInputBuilder,
}

impl DownloadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
        }
    }

    /// Initiate a download transfer for a single object.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-download", fields(
        bucket = self.inner.get_bucket().as_deref().unwrap_or_default(),
        key = self.inner.get_key().as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<DownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate(self.handle, input, false)
    }

    /// Download the object and write it to the given file path.
    ///
    /// Data is written to a temporary file in the same directory, then
    /// atomically renamed to the destination on success. The temporary file
    /// is deleted on failure, cancellation, or drop. The destination has the
    /// exact downloaded length.
    ///
    /// Successful completion means every byte was accepted by the operating
    /// system and the temporary file was renamed. It does not call `sync_data`,
    /// `sync_all`, or synchronize the parent directory, and therefore does not
    /// promise persistence across a system crash or power loss.
    #[cfg(any(unix, windows))]
    pub async fn write_to_path(
        self,
        path: impl Into<std::path::PathBuf>,
    ) -> Result<ManagedDownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate_to_path(
            self.handle,
            input,
            path.into(),
            None,
        )
        .await
    }

    /// Download the object and write it to the given open file.
    ///
    /// The caller is responsible for the file lifecycle (creation, cleanup).
    /// The transfer manager writes to the file using positioned writes at
    /// offsets starting from 0 and resizes it to the exact downloaded length,
    /// removing any previous tail. The file cursor is ignored. Append and
    /// nonzero destination offsets are not supported by this operation.
    ///
    /// On failure or cancellation, the file may contain a noncontiguous mixture
    /// of downloaded and previous data, and positioned writes may have extended
    /// it. Treat its contents as invalid unless [`ManagedDownloadHandle::join`]
    /// succeeds.
    ///
    /// Successful completion does not synchronize file data or metadata. Call
    /// the appropriate synchronization method on another handle when crash
    /// persistence is required.
    #[cfg(any(unix, windows))]
    pub fn write_to_file(
        self,
        file: std::fs::File,
    ) -> Result<ManagedDownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate_to_file(self.handle, input, file)
    }
}

impl DownloadInputBuilder {
    /// Initiate a download transfer for a single object with this input using the given client.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadHandle, crate::error::Error> {
        let mut fluent_builder = client.download();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }
}
