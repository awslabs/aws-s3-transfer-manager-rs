/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::io::walk::S3Walker;
use crate::types::FailedTransferPolicy;

use super::{DownloadObjectsHandle, DownloadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object download.
///
/// Walk behavior (prefix, filter, pagination) is configured by supplying a
/// custom [`S3Walker`] via [`walker`](Self::walker). When not set, a default
/// walker is used that applies [`key_prefix`](Self::key_prefix). A supplied
/// walker replaces that, so it must carry its own prefix. 0-byte folder markers
/// are dropped either way: the operation excludes them, not the walker.
#[derive(Debug)]
pub struct DownloadObjectsFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: DownloadObjectsInputBuilder,
    events: Option<crate::events::TransferEventSink>,
}

impl DownloadObjectsFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: std::default::Default::default(),
            events: None,
        }
    }

    /// observe lifecycle events for this request and its children.
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

    /// Initiate download of multiple objects.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-download-objects", fields(
        bucket = self.inner.bucket.as_deref().unwrap_or_default(),
        destination = self.inner.destination.as_deref().map(|p| p.to_str().unwrap_or_default()).unwrap_or_default(),
        key_prefix = self.inner.key_prefix.as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let input = self.inner.build()?;
        // Resolved before the handle moves into `orchestrate`.
        let events = crate::events::resolve_sink(self.handle.config.events(), self.events);
        crate::operation::download_objects::DownloadObjects::orchestrate(self.handle, input, events)
    }

    /// The S3 bucket name containing the object(s) to download. Required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input);
        self
    }

    /// The S3 bucket name.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_bucket(input);
        self
    }

    /// The S3 bucket name.
    pub fn get_bucket(&self) -> Option<&str> {
        self.inner.get_bucket()
    }

    /// The local directory to download into. Required.
    pub fn destination(mut self, input: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.destination(input);
        self
    }

    /// The local directory to download into.
    pub fn set_destination(mut self, input: Option<PathBuf>) -> Self {
        self.inner = self.inner.set_destination(input);
        self
    }

    /// The local directory to download into.
    pub fn get_destination(&self) -> Option<&Path> {
        self.inner.get_destination()
    }

    /// Limit the response to keys that begin with the given prefix.
    ///
    /// This does two jobs: it scopes the listing, and it is stripped from each key to derive the
    /// local path under the destination. Supplying a custom [`walker`](Self::walker) takes over
    /// the first job only -- the listing is then whatever the walker enumerates, while the prefix
    /// continues to be stripped from local paths.
    ///
    /// Keep the two in agreement. If the walker lists keys outside the prefix, stripping is no
    /// longer uniform and two objects can collide on one local path: under `key_prefix("b/")`, key
    /// `b/a/x` strips to `a/x` while key `a/x` passes through unchanged, and whichever lands second
    /// wins. The destination-escape guard does not catch this -- both paths are inside the
    /// destination.
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key_prefix(input);
        self
    }

    /// Limit the response to keys that begin with the given prefix.
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key_prefix(input);
        self
    }

    /// The key prefix.
    pub fn get_key_prefix(&self) -> Option<&str> {
        self.inner.get_key_prefix()
    }

    /// Character used to group keys.
    pub fn delimiter(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.delimiter(input);
        self
    }

    /// Character used to group keys.
    pub fn set_delimiter(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_delimiter(input);
        self
    }

    /// Character used to group keys.
    pub fn get_delimiter(&self) -> Option<&str> {
        self.inner.get_delimiter()
    }

    /// The failure policy to use when any individual object download fails.
    /// Defaults to [`FailedTransferPolicy::Abort`].
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner = self.inner.failure_policy(input);
        self
    }

    /// The failure policy.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.get_failure_policy()
    }

    /// Walker configuration (prefix, filter, pagination, etc.).
    ///
    /// The walker owns the listing: supplying one replaces the default walker, and with it the
    /// scope that [`key_prefix`](Self::key_prefix) would otherwise set. A walker built without a
    /// prefix lists the whole bucket even when `key_prefix` is set, so set the prefix on the
    /// walker as well:
    ///
    /// ```no_run
    /// # use aws_sdk_s3_transfer_manager::io::walk::S3Walker;
    /// # fn f(client: &aws_sdk_s3_transfer_manager::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// client
    ///     .download_objects()
    ///     .bucket("my-bucket")
    ///     .destination("/tmp/out")
    ///     .key_prefix("logs/") // still strips `logs/` from local paths
    ///     .walker(
    ///         S3Walker::builder()
    ///             .prefix("logs/") // and this is what scopes the listing
    ///             .page_size(100)
    ///             .build(),
    ///     )
    ///     .initiate()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A [`filter`](crate::io::walk::S3WalkerBuilder::filter) set here is yours alone and needs no
    /// allowance for folder markers.
    pub fn walker(mut self, input: S3Walker) -> Self {
        self.inner = self.inner.walker(input);
        self
    }

    /// Walker configuration.
    pub fn set_walker(mut self, input: Option<S3Walker>) -> Self {
        self.inner = self.inner.set_walker(input);
        self
    }

    /// Walker configuration.
    pub fn get_walker(&self) -> Option<&S3Walker> {
        self.inner.get_walker()
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    /// Defaults to 512.
    pub fn max_concurrent_downloads(mut self, input: usize) -> Self {
        self.inner = self.inner.max_concurrent_downloads(input);
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn set_max_concurrent_downloads(mut self, input: Option<usize>) -> Self {
        self.inner = self.inner.set_max_concurrent_downloads(input);
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn get_max_concurrent_downloads(&self) -> Option<usize> {
        self.inner.get_max_concurrent_downloads()
    }
}

impl crate::operation::download_objects::input::DownloadObjectsInputBuilder {
    /// Initiate a download transfer for multiple objects with this input using the given client.
    ///
    /// This entry point reports no [events](crate::events) — an input builder has no sink to
    /// carry. Use [`initiate_with_events`](Self::initiate_with_events) to register one.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let mut fluent_builder = client.download_objects();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }

    /// Initiate a download transfer for multiple objects, reporting per-object lifecycle
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
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let mut fluent_builder = client.download_objects();
        fluent_builder.inner = self;
        fluent_builder.events(sink).initiate()
    }
}
