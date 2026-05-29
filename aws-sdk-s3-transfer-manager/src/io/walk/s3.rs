/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;
use std::sync::Arc;

use aws_sdk_s3::types::Object;

use super::error::{WalkError, WalkErrorKind};
use crate::types::BucketType;

type FilterFn = Arc<dyn Fn(&Object) -> bool + Send + Sync>;

/// Result of listing a single page of S3 objects.
///
/// Contains the objects in the page (after filter application), any common
/// prefixes when a delimiter is set, and a continuation token for the next
/// page.
pub(crate) struct ListPageResult {
    /// Objects returned in this page, after filter application.
    pub(crate) objects: Vec<Object>,
    /// Common prefixes from the response (populated when a delimiter is set).
    pub(crate) common_prefixes: Vec<String>,
    /// Continuation token for the next page, or `None` if this is the last.
    pub(crate) next_token: Option<String>,
}

/// Configuration for walking an S3 bucket by listing objects under a prefix.
///
/// Describes what to list and how (prefix, delimiter, filter, pagination).
/// The S3 client and bucket name are supplied separately via
/// [`S3WalkContext`] when starting a walk.
///
/// Use [`S3Walker::builder`] to construct an instance.
///
/// # Traversal model
///
/// The walker issues `ListObjectsV2` requests under a configured prefix.
/// Each response contributes objects and (when a delimiter is set) common
/// prefixes representing subdirectories. The walker paginates through the
/// full object set of each prefix before recursing into common prefixes.
///
/// # Error handling
///
/// Service errors from `ListObjectsV2` terminate the walk. There are no
/// non-fatal errors; either the page succeeds or the entire walk fails.
// TODO(walker): bucket-kind refuse/warn policy — we detect directory buckets
//   but don't yet have a configurable refuse policy for sync operations.
// TODO(walker): resume token persistence — save/restore continuation state
//   across process restarts.
#[derive(Clone)]
pub struct S3Walker {
    prefix: Option<String>,
    delimiter: Option<String>,
    expected_bucket_owner: Option<String>,
    request_payer: Option<aws_sdk_s3::types::RequestPayer>,
    filter: Option<FilterFn>,
    start_after: Option<String>,
    continuation_token: Option<String>,
    page_size: Option<i32>,
}

impl std::fmt::Debug for S3Walker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Walker")
            .field("prefix", &self.prefix)
            .field("delimiter", &self.delimiter)
            .field("expected_bucket_owner", &self.expected_bucket_owner)
            .field("request_payer", &self.request_payer)
            .field("start_after", &self.start_after)
            .field("continuation_token", &self.continuation_token)
            .field("page_size", &self.page_size)
            .finish()
    }
}

impl Default for S3Walker {
    /// A default walker that filters out 0-byte folder markers (keys ending in '/').
    fn default() -> Self {
        S3Walker::builder()
            .filter(exclude_s3_folder_markers)
            .build()
    }
}

/// Exclude 0-byte objects whose key ends with `/`. These are "folder markers"
/// created by the S3 console and have no meaningful content to download.
pub(crate) fn exclude_s3_folder_markers(obj: &Object) -> bool {
    let dominated_by_slash = obj.key().unwrap_or("").ends_with('/');
    let is_zero_byte = obj.size().unwrap_or(1) == 0;
    !(dominated_by_slash && is_zero_byte)
}

impl S3Walker {
    /// Create a builder for configuring an `S3Walker`.
    #[must_use]
    pub fn builder() -> S3WalkerBuilder {
        S3WalkerBuilder::default()
    }

    /// Start a walk with the given execution context.
    ///
    /// Returns an [`S3Walk`] that yields objects via [`next`](S3Walk::next).
    /// Directory buckets (S3 Express) are detected and logged as a warning
    /// since they may not return lexicographically sorted listing results.
    #[must_use]
    pub fn walk(self, ctx: S3WalkContext) -> S3Walk {
        if ctx.bucket.kind() == BucketType::Express {
            tracing::warn!(
                bucket = %ctx.bucket.name(),
                kind = ?ctx.bucket.kind(),
                "directory bucket detected; listing results may not be lexicographically sorted"
            );
        }

        let initial_prefix = self.prefix.clone().unwrap_or_default();

        tracing::debug!(
            bucket = %ctx.bucket.name(),
            kind = ?ctx.bucket.kind(),
            prefix = %initial_prefix,
            delimiter = ?self.delimiter,
            "s3 walk started",
        );
        let mut pending_prefixes = VecDeque::new();
        pending_prefixes.push_back(initial_prefix);

        S3Walk {
            config: self,
            client: ctx.client,
            bucket: ctx.bucket,
            pending_prefixes,
            ready_objects: VecDeque::new(),
            current_prefix: None,
            next_token: None,
            done: false,
            initial_first_page_pending: true,
        }
    }
}

/// Builder for [`S3Walker`].
///
/// All fields are optional with sensible defaults: no prefix, no delimiter,
/// no filter, no pagination overrides.
#[derive(Default)]
pub struct S3WalkerBuilder {
    prefix: Option<String>,
    delimiter: Option<String>,
    expected_bucket_owner: Option<String>,
    request_payer: Option<aws_sdk_s3::types::RequestPayer>,
    filter: Option<FilterFn>,
    start_after: Option<String>,
    continuation_token: Option<String>,
    page_size: Option<i32>,
}

impl std::fmt::Debug for S3WalkerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3WalkerBuilder")
            .field("prefix", &self.prefix)
            .field("delimiter", &self.delimiter)
            .finish()
    }
}

impl S3WalkerBuilder {
    /// Restrict listing to keys beginning with the given prefix.
    ///
    /// When unset, the walker lists all objects in the bucket.
    #[must_use]
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Group keys by a delimiter, enabling recursive traversal.
    ///
    /// Typically set to `/` to traverse S3 as if it were a directory tree.
    /// When unset (default), all objects are returned in a flat list.
    #[must_use]
    pub fn delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = Some(delimiter.into());
        self
    }

    /// Set the expected bucket owner account ID.
    ///
    /// When set, `ListObjectsV2` requests fail with `AccessDenied` if the
    /// bucket's actual owner doesn't match.
    #[must_use]
    pub fn expected_bucket_owner(mut self, owner: impl Into<String>) -> Self {
        self.expected_bucket_owner = Some(owner.into());
        self
    }

    /// Set the request-payer preference for Requester-Pays buckets.
    #[must_use]
    pub fn request_payer(mut self, payer: aws_sdk_s3::types::RequestPayer) -> Self {
        self.request_payer = Some(payer);
        self
    }

    /// Set a filter predicate applied to each discovered object.
    ///
    /// Returning `false` drops the object silently.
    #[must_use]
    pub fn filter(mut self, f: impl Fn(&Object) -> bool + Send + Sync + 'static) -> Self {
        self.filter = Some(Arc::new(f));
        self
    }

    /// Resume listing after the given key.
    ///
    /// Maps to the `StartAfter` parameter of `ListObjectsV2`. Only applied
    /// to the initial prefix listing (not to recursed common prefixes).
    #[must_use]
    pub fn start_after(mut self, key: impl Into<String>) -> Self {
        self.start_after = Some(key.into());
        self
    }

    /// Resume listing from a specific continuation token.
    ///
    /// Maps to the `ContinuationToken` parameter of `ListObjectsV2`. Only
    /// applied to the first request of the initial prefix.
    #[must_use]
    pub fn continuation_token(mut self, token: impl Into<String>) -> Self {
        self.continuation_token = Some(token.into());
        self
    }

    /// Override the server default page size (max keys per response).
    ///
    /// Maps to the `MaxKeys` parameter of `ListObjectsV2`. Useful for
    /// tuning small-object workloads. Default is the server default (1000).
    #[must_use]
    pub fn page_size(mut self, n: i32) -> Self {
        self.page_size = Some(n);
        self
    }

    /// Build the [`S3Walker`] configuration.
    #[must_use]
    pub fn build(self) -> S3Walker {
        S3Walker {
            prefix: self.prefix,
            delimiter: self.delimiter,
            expected_bucket_owner: self.expected_bucket_owner,
            request_payer: self.request_payer,
            filter: self.filter,
            start_after: self.start_after,
            continuation_token: self.continuation_token,
            page_size: self.page_size,
        }
    }
}

/// The S3 client and bucket for an S3 walk.
pub struct S3WalkContext {
    client: aws_sdk_s3::Client,
    bucket: crate::types::Bucket,
}

impl std::fmt::Debug for S3WalkContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3WalkContext")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl S3WalkContext {
    /// Create a builder for an `S3WalkContext`.
    #[must_use]
    pub fn builder() -> S3WalkContextBuilder {
        S3WalkContextBuilder {
            client: None,
            bucket: None,
        }
    }
}

/// Builder for [`S3WalkContext`].
#[derive(Debug, Default)]
pub struct S3WalkContextBuilder {
    client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
}

impl S3WalkContextBuilder {
    /// Set the S3 client to use for listing.
    ///
    /// This field is required.
    #[must_use]
    pub fn client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the bucket to list.
    ///
    /// This field is required.
    #[must_use]
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Build the [`S3WalkContext`].
    ///
    /// # Panics
    ///
    /// Panics if `client` or `bucket` has not been set.
    #[must_use]
    pub fn build(self) -> S3WalkContext {
        let bucket_name = self.bucket.expect("required field `bucket` should be set");
        S3WalkContext {
            client: self.client.expect("required field `client` should be set"),
            bucket: crate::types::Bucket::new(bucket_name),
        }
    }
}

/// A running S3 walk, yielding objects from a bucket listing.
///
/// Created by [`S3Walker::walk`]. The walk pages through the bucket,
/// buffering objects and yielding them via [`next`](Self::next).
pub struct S3Walk {
    config: S3Walker,
    client: aws_sdk_s3::Client,
    bucket: crate::types::Bucket,
    pending_prefixes: VecDeque<String>,
    ready_objects: VecDeque<Object>,
    current_prefix: Option<String>,
    next_token: Option<String>,
    done: bool,
    /// Whether the next `list_page` call is the first request of the initial
    /// prefix. Used to gate `continuation_token` and `start_after` config
    /// (which only apply to the very first request).
    initial_first_page_pending: bool,
}

impl std::fmt::Debug for S3Walk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Walk")
            .field("bucket", &self.bucket)
            .field("done", &self.done)
            .finish()
    }
}

impl S3Walk {
    /// Return the next object from the walk.
    ///
    /// Returns:
    /// - `Some(Ok(object))` for an object that passed the filter.
    /// - `Some(Err(err))` when a `ListObjectsV2` request fails. The walk
    ///   terminates after an error; subsequent calls return `None`.
    /// - `None` when the walk is complete.
    pub async fn next(&mut self) -> Option<Result<Object, WalkError>> {
        loop {
            if let Some(obj) = self.ready_objects.pop_front() {
                return Some(Ok(obj));
            }
            if self.done {
                return None;
            }

            if let Some(prefix) = &self.current_prefix {
                let prefix = prefix.clone();
                let token = self.next_token.take();
                let first_page = self.initial_first_page_pending;
                match self.list_page(&prefix, token.as_deref(), first_page).await {
                    Ok(page) => {
                        self.initial_first_page_pending = false;

                        tracing::trace!(
                            %prefix,
                            objects = page.objects.len(),
                            common_prefixes = page.common_prefixes.len(),
                            has_next = page.next_token.is_some(),
                            "page listed",
                        );

                        self.ready_objects.extend(page.objects);
                        for cp in page.common_prefixes {
                            self.pending_prefixes.push_back(cp);
                        }
                        if page.next_token.is_some() {
                            self.next_token = page.next_token;
                            self.current_prefix = Some(prefix);
                        } else {
                            self.current_prefix = None;
                        }
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
                continue;
            }

            match self.pending_prefixes.pop_front() {
                Some(prefix) => {
                    self.current_prefix = Some(prefix);
                }
                None => {
                    self.done = true;
                    return None;
                }
            }
        }
    }

    /// Whether the walk has finished (no more objects will be produced).
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Whether the target bucket is a directory bucket (S3 Express).
    ///
    /// Directory buckets may not return lexicographically sorted listing
    /// results, which can affect sync operations that depend on key ordering.
    pub fn is_directory_bucket(&self) -> bool {
        self.bucket.kind() == BucketType::Express
    }

    async fn list_page(
        &self,
        prefix: &str,
        token: Option<&str>,
        first_page_of_initial_prefix: bool,
    ) -> Result<ListPageResult, WalkError> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.name())
            .prefix(prefix);

        // Runtime pagination token for this page. When a caller-supplied
        // continuation_token is configured AND this is the first request
        // of the initial prefix, the configured token takes precedence
        // (resume use case). Otherwise, use the pagination token from the
        // previous page response.
        let effective_token = if first_page_of_initial_prefix {
            self.config.continuation_token.as_deref().or(token)
        } else {
            token
        };
        if let Some(t) = effective_token {
            req = req.continuation_token(t);
        }

        // start_after is only meaningful on the very first request of the
        // initial prefix and is ignored by S3 whenever a continuation token
        // is present.
        if first_page_of_initial_prefix {
            if let Some(start_after) = &self.config.start_after {
                if effective_token.is_none() {
                    req = req.start_after(start_after);
                }
            }
        }

        if let Some(delimiter) = &self.config.delimiter {
            req = req.delimiter(delimiter);
        }
        if let Some(owner) = &self.config.expected_bucket_owner {
            req = req.expected_bucket_owner(owner);
        }
        if let Some(payer) = &self.config.request_payer {
            req = req.request_payer(payer.clone());
        }
        if let Some(page_size) = self.config.page_size {
            req = req.max_keys(page_size);
        }

        let output = req
            .send()
            .await
            .map_err(|e| WalkError::new(None, WalkErrorKind::Service, Box::new(e)))?;

        let mut objects: Vec<Object> = output.contents.unwrap_or_default();
        if let Some(ref filter) = self.config.filter {
            objects.retain(|obj| filter(obj));
        }

        let common_prefixes = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cp| cp.prefix)
            .collect();

        Ok(ListPageResult {
            objects,
            common_prefixes,
            next_token: output.next_continuation_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::types::{CommonPrefix, Object};
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    fn walker() -> S3WalkerBuilder {
        S3Walker::builder()
    }

    fn s3ctx(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> S3WalkContext {
        S3WalkContext::builder()
            .client(client)
            .bucket(bucket)
            .build()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_single_page() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").build(),
                    Object::builder().key("b.txt").build(),
                    Object::builder().key("c.txt").build(),
                ]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .prefix("prefix/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_with_pagination() {
        let page1 = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .is_truncated(true)
                .next_continuation_token("token1")
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").build(),
                    Object::builder().key("b.txt").build(),
                ]))
                .build()
        });
        let page2 = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("c.txt").build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&page1, &page2]);

        let mut walk = walker().build().walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_with_filter() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("file.txt").build(),
                    Object::builder().key("folder/").build(),
                    Object::builder().key("other.txt").build(),
                ]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .filter(|obj| !obj.key().unwrap_or("").ends_with('/'))
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["file.txt", "other.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_empty_prefix() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .prefix("empty/")
            .build()
            .walk(s3ctx(client, "test-bucket"));
        assert!(walk.next().await.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_with_common_prefixes() {
        let root_page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("root.txt").build()]))
                .set_common_prefixes(Some(vec![
                    CommonPrefix::builder().prefix("sub1/").build(),
                    CommonPrefix::builder().prefix("sub2/").build(),
                ]))
                .build()
        });
        let sub1_page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("sub1/a.txt").build()]))
                .build()
        });
        let sub2_page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("sub2/b.txt").build()]))
                .build()
        });
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&root_page, &sub1_page, &sub2_page]
        );

        let mut walk = walker()
            .delimiter("/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["root.txt", "sub1/a.txt", "sub2/b.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_service_error_terminates_walk() {
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
        use aws_smithy_types::error::metadata::ErrorMetadata;

        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            ListObjectsV2Error::generic(ErrorMetadata::builder().code("NoSuchBucket").build())
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker().build().walk(s3ctx(client, "missing-bucket"));

        let first = walk.next().await;
        assert!(matches!(first, Some(Err(_))), "expected service error");
        assert!(walk.next().await.is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_pagination_across_common_prefix() {
        let root_page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("root.txt").build()]))
                .set_common_prefixes(Some(vec![CommonPrefix::builder().prefix("sub/").build()]))
                .build()
        });
        let sub_page1 = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .is_truncated(true)
                .next_continuation_token("tok")
                .set_contents(Some(vec![
                    Object::builder().key("sub/a.txt").build(),
                    Object::builder().key("sub/b.txt").build(),
                ]))
                .build()
        });
        let sub_page2 = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("sub/c.txt").build()]))
                .build()
        });
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&root_page, &sub_page1, &sub_page2]
        );

        let mut walk = walker()
            .delimiter("/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(
            keys,
            vec!["root.txt", "sub/a.txt", "sub/b.txt", "sub/c.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_delimiter_without_common_prefixes() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").build(),
                    Object::builder().key("b.txt").build(),
                ]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .delimiter("/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt", "b.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_list_no_delimiter_no_recursion() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").build(),
                    Object::builder().key("sub/b.txt").build(),
                    Object::builder().key("sub/c.txt").build(),
                ]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker().build().walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt", "sub/b.txt", "sub/c.txt"]);
    }

    // --- NEW TESTS ---

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_start_after() {
        // start_after should propagate to ListObjectsV2; the mock returns
        // only objects "after" the start key.
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("c.txt").build(),
                    Object::builder().key("d.txt").build(),
                ]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .start_after("b.txt")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["c.txt", "d.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_page_size() {
        // page_size propagates as max_keys; mock returns a single-item page
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("only.txt").build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .page_size(1)
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["only.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_expected_bucket_owner() {
        // expected_bucket_owner propagates; mock just returns success
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("a.txt").build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .expected_bucket_owner("123456789012")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_is_directory_bucket() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule, &rule]);

        // Express bucket name ends with --x-s3
        let walk_express = walker()
            .build()
            .walk(s3ctx(client.clone(), "my-bucket--usw2-az1--x-s3"));
        assert!(walk_express.is_directory_bucket());

        // Standard bucket
        let walk_standard = walker().build().walk(s3ctx(client, "my-normal-bucket"));
        assert!(!walk_standard.is_directory_bucket());
    }

    /// Regression test: configured `continuation_token` must only apply to
    /// the first request. On pagination, subsequent requests must use the
    /// server's continuation token, not the original one (which would cause
    /// an infinite loop).
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_continuation_token_only_on_first_page() {
        // First call: expect the configured token "resume-here"; return a page
        // with server continuation token "server-next".
        let first = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.continuation_token() == Some("resume-here"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .is_truncated(true)
                    .next_continuation_token("server-next")
                    .set_contents(Some(vec![Object::builder().key("a.txt").build()]))
                    .build()
            });
        // Second call: must use "server-next", NOT "resume-here".
        let second = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.continuation_token() == Some("server-next"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("b.txt").build()]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&first, &second]);

        let mut walk = walker()
            .continuation_token("resume-here")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt", "b.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_prefix_propagates() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.prefix() == Some("my-prefix/"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("my-prefix/a.txt").build()]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .prefix("my-prefix/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["my-prefix/a.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_request_payer_propagates() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| {
                req.request_payer() == Some(&aws_sdk_s3::types::RequestPayer::Requester)
            })
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("a.txt").build()]))
                    .build()
            });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker()
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_delimiter_propagates_on_recursed_prefixes() {
        let root = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.delimiter() == Some("/") && req.prefix() == Some(""))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_common_prefixes(Some(vec![CommonPrefix::builder().prefix("a/").build()]))
                    .build()
            });
        let a_page = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.delimiter() == Some("/") && req.prefix() == Some("a/"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("a/file.txt").build()]))
                    .set_common_prefixes(Some(vec![CommonPrefix::builder().prefix("a/b/").build()]))
                    .build()
            });
        let ab_page = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.delimiter() == Some("/") && req.prefix() == Some("a/b/"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("a/b/file.txt").build()]))
                    .set_common_prefixes(Some(vec![CommonPrefix::builder()
                        .prefix("a/b/c/")
                        .build()]))
                    .build()
            });
        let abc_page = mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| req.delimiter() == Some("/") && req.prefix() == Some("a/b/c/"))
            .then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![Object::builder().key("a/b/c/file.txt").build()]))
                    .build()
            });
        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&root, &a_page, &ab_page, &abc_page]
        );

        let mut walk = walker()
            .delimiter("/")
            .build()
            .walk(s3ctx(client, "test-bucket"));

        let mut keys = Vec::new();
        while let Some(result) = walk.next().await {
            keys.push(result.unwrap().key.unwrap());
        }
        assert_eq!(keys, vec!["a/file.txt", "a/b/file.txt", "a/b/c/file.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_s3walker_empty_contents_none() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                // contents is None, not Some(vec![])
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);

        let mut walk = walker().build().walk(s3ctx(client, "test-bucket"));

        let mut count = 0;
        while let Some(result) = walk.next().await {
            result.unwrap();
            count += 1;
        }
        assert_eq!(count, 0, "None contents should yield zero objects");
    }

    #[test]
    fn test_s3walker_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<S3Walker>();
        assert_send_sync::<S3Walk>();
        assert_send_sync::<S3WalkContext>();
        assert_send_sync::<S3WalkerBuilder>();
        assert_send_sync::<S3WalkContextBuilder>();
    }
}
