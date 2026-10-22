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
    events: Option<crate::events::TransferEventSink>,
}

impl DownloadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
            events: None,
        }
    }

    /// Report lifecycle events for this transfer to `sink`.
    ///
    /// Calling this twice adds a second consumer rather than replacing the first.
    pub fn events(mut self, sink: crate::events::TransferEventSink) -> Self {
        self.events = Some(match self.events.take() {
            Some(existing) => existing.merge(sink),
            None => sink,
        });
        self
    }

    /// Initiate a download transfer for a single object.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-download", fields(
        bucket = self.inner.get_bucket().as_deref().unwrap_or_default(),
        key = self.inner.get_key().as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<DownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        let events = self
            .events
            .map(|sink| crate::operation::download::EventRegistration {
                sink,
                destination: crate::events::Endpoint::Stream {},
            });
        crate::operation::download::Download::orchestrate(self.handle, input, false, events)
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
        let path = path.into();
        let events = self
            .events
            .map(|sink| crate::operation::download::EventRegistration {
                sink,
                destination: crate::events::Endpoint::Local {
                    path: Some(std::sync::Arc::from(path.as_path())),
                },
            });
        crate::operation::download::Download::orchestrate_to_path(
            self.handle,
            input,
            path,
            None,
            events,
            &crate::operation::download::sink::FileSinkFactory,
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
    /// A file that is open in append mode when this method is called, such as
    /// one opened with [`OpenOptions::append`](std::fs::OpenOptions::append),
    /// is rejected with
    /// [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) on
    /// every platform. No request is sent, and the file is not modified.
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
        // `path: None`: the caller opened the file, and this method is never told its path.
        let events = self
            .events
            .map(|sink| crate::operation::download::EventRegistration {
                sink,
                destination: crate::events::Endpoint::Local { path: None },
            });
        crate::operation::download::Download::orchestrate_to_file(
            self.handle,
            input,
            file,
            events,
            &crate::operation::download::sink::FileSinkFactory,
        )
    }
}

impl DownloadInputBuilder {
    /// Initiate a download transfer for a single object with this input using the given client.
    ///
    /// This entry point reports no [events](crate::events); [`DownloadFluentBuilder::events`]
    /// registers a sink.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadHandle, crate::error::Error> {
        let mut fluent_builder = client.download();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }
}
