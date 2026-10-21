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
    events: Option<crate::events::TransferEventSink>,
}

impl UploadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: Default::default(),
            events: None,
        }
    }

    /// Report lifecycle events for this transfer to `sink`.
    ///
    /// Merged with any sink set on the client, and calling this twice adds a second
    /// consumer rather than replacing the first.
    pub fn events(mut self, sink: crate::events::TransferEventSink) -> Self {
        // Appends rather than replaces, so every registered consumer sees every event: the
        // SEP asks for "a list of progress listeners", and a replacing setter would make a
        // client-level sink and a request-level sink mutually exclusive.
        self.events = Some(match self.events.take() {
            Some(existing) => existing.merge(sink),
            None => sink,
        });
        self
    }

    /// Initiate an upload transfer for a single object.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-upload", fields(
        bucket = self.inner.get_bucket().as_deref().unwrap_or_default(),
        key = self.inner.get_key().as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<UploadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        let events = crate::events::resolve_sink(self.handle.config.events(), self.events);
        crate::operation::upload::Upload::orchestrate(self.handle, input, events)
    }
}

impl UploadInputBuilder {
    /// Initiate an upload transfer for a single object using the given client.
    ///
    /// This entry point reports no [events](crate::events) — an input builder has no sink to
    /// carry. Use [`initiate_with_events`](Self::initiate_with_events) to register one.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<UploadHandle, crate::error::Error> {
        let mut fluent_builder = client.upload();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }

    /// Initiate an upload transfer for a single object, reporting lifecycle
    /// [events](crate::events) to `sink`.
    ///
    /// The events-carrying form of [`initiate_with`](Self::initiate_with). It exists because a
    /// sink is registered on the fluent builder, which this entry point bypasses — without it,
    /// a caller who assembled an input directly has no way to observe the transfer, and the
    /// silence would look like a transfer that never produced events.
    pub fn initiate_with_events(
        self,
        client: &crate::Client,
        sink: crate::events::TransferEventSink,
    ) -> Result<UploadHandle, crate::error::Error> {
        let mut fluent_builder = client.upload();
        fluent_builder.inner = self;
        fluent_builder.events(sink).initiate()
    }
}
