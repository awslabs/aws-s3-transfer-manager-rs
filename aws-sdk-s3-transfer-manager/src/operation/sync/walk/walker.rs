/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// This module builds the two streams a sync pairs from one local directory and one bucket.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::key::filter::KeyFilter;
use crate::io::key::stream::{
    local_predicate, s3_predicate, AliasedName, Entry, KeyStream, StreamError,
};
use crate::io::walk::{
    FsEntry, FsWalk, FsWalkContext, FsWalker, S3Walk, S3WalkContext, S3Walker, SortOrder,
    WalkErrorKind,
};
use crate::operation::sync::input::DeleteMode;

use super::links::{LinkNamer, LinkScan, Links};
use super::Walk;

// What a run was configured with, independent of the roots it runs against.
#[derive(Debug, Default)]
pub(crate) struct WalkerBuilder {
    filter: Option<Arc<KeyFilter>>,
    follow_symlinks: bool,
}

impl WalkerBuilder {
    // The rules deciding which keys take part. The same set reaches both sides.
    #[must_use]
    pub(crate) fn filter(mut self, filter: Arc<KeyFilter>) -> Self {
        self.filter = Some(filter);
        self
    }

    // Whether the local walk resolves symlinks. Defaults to leaving them alone, and an
    // unresolved symlink still occupies its name.
    #[must_use]
    pub(crate) fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    #[must_use]
    pub(crate) fn build(self) -> Walker {
        Walker {
            filter: self
                .filter
                .unwrap_or_else(|| Arc::new(KeyFilter::new(Vec::new()))),
            follow_symlinks: self.follow_symlinks,
        }
    }
}

// A configured run, ready to be pointed at a pair of roots.
#[derive(Debug)]
pub(crate) struct Walker {
    filter: Arc<KeyFilter>,
    follow_symlinks: bool,
}

impl Walker {
    pub(crate) fn builder() -> WalkerBuilder {
        WalkerBuilder::default()
    }

    // Build both streams against the two roots and start the merge for an upload.
    //
    // TODO(sync): `copying` to follow, once there is a context that can hold two endpoints. Its
    // two roots may sit in different regions and different accounts, so it needs two clients, and
    // the copy request itself goes to the destination while naming the source — which means the
    // destination credentials have to be able to read the source object.
    pub(crate) fn uploading(
        &self,
        ctx: LocalAndBucket,
    ) -> Result<Walk<FsWalk, S3Walk>, crate::error::Error> {
        let root = ctx.local_root.clone();
        let local = self.local_walk(ctx.local_root);
        let remote = self.remote_walk(ctx.client, ctx.bucket, ctx.prefix)?;
        // Only the source walks a filesystem; a listing names its own lost keys.
        Ok(Walk::new(local, remote).with_roots(Some(root), None))
    }

    // The same two roots the other way round: the listing is the source and the local tree is
    // where entries land.
    //
    // The root moves to the destination side, because that is the side whose failures name an
    // absolute path this layer has to turn into a key.
    //
    // The names a followed link reaches exist only to stop deletes. With delete mode off, the
    // destination names none, so a run over a tree with links holds nothing back. With delete mode
    // on and following on, the run also starts a link scan of the destination, and every local
    // delete waits until the scan ends.
    pub(crate) fn downloading(
        &self,
        ctx: LocalAndBucket,
        delete_mode: DeleteMode,
    ) -> Result<Walk<S3Walk, LocalDestination>, crate::error::Error> {
        let root = ctx.local_root.clone();
        let remote = self.remote_walk(ctx.client, ctx.bucket, ctx.prefix)?;
        let names = delete_mode == DeleteMode::On;
        let local =
            LocalDestination::new(self.local_walk(ctx.local_root), &root).naming_aliases(names);
        let mut walk = Walk::new(remote, local).with_roots(None, Some(root.clone()));
        if names && self.follow_symlinks {
            walk.link_scan = Some(LinkScan {
                walk: self.fs_walk(root.clone(), SortOrder::Native),
                namer: LinkNamer::new(&root),
                names: Vec::new(),
                too_many: false,
            });
            walk.links = Links::Scanning { found: Vec::new() };
        }
        Ok(walk)
    }

    // The merge depends on all three of these, so this layer fixes them and a caller never
    // chooses. The walk sorts across the whole tree, because sorting each directory among itself
    // pairs keys that are not the same key. It descends all the way down, because a merge over one
    // level reports every key below it as absent. And it keeps special files, because a name that
    // is taken has to reach the comparison even though nothing can be sent for it.
    pub(super) fn local_walk(&self, root: PathBuf) -> FsWalk {
        self.fs_walk(root, SortOrder::WholeWalk)
    }

    // The link scan reads the same tree with the same rules, in whatever order the filesystem gives.
    // It needs every link and no key order, so it skips the sort.
    fn fs_walk(&self, root: PathBuf, sort: SortOrder) -> FsWalk {
        FsWalker::builder()
            .sort(sort)
            .recursive(true)
            .include_special_files(true)
            .follow_symlinks(self.follow_symlinks)
            .path_filter(local_predicate(Arc::clone(&self.filter)))
            .build()
            .walk(FsWalkContext::builder().root(root).build())
    }

    // A delimiter would turn one listing into many and roll keys up into prefixes, and a
    // continuation token would start the listing part way through, leaving every key
    // before it looking absent. Neither is set.
    pub(super) fn remote_walk(
        &self,
        client: aws_sdk_s3::Client,
        bucket: String,
        prefix: Option<String>,
    ) -> Result<S3Walk, crate::error::Error> {
        // Restore status is asked for here and nowhere else. Without it an object in an archive
        // and one with a restored copy look the same, and a comparison would either refuse every
        // archived key or hand execution a transfer that fails.
        let mut walker = S3Walker::builder()
            .request_restore_status(true)
            .filter(s3_predicate(Arc::clone(&self.filter), prefix.clone()));
        if let Some(prefix) = prefix {
            walker = walker.prefix(prefix);
        }
        let bucket_name = bucket.clone();
        let walk = walker.build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket(bucket)
                .build(),
        );
        // The merge reads absence from position, so a listing arriving out of key order reports
        // a key the other side holds as a key nobody holds, and delete mode removes it. A
        // directory bucket lists in no defined order, and the walk reads that from the bucket's
        // name, so the root stops here. The runtime order check cannot stand in for this: it
        // speaks when the late key arrives, by which time the removal for the key ahead of it
        // has gone out.
        //
        // Refused for this merge and not inside the walk, because sync needs key order and the
        // directory download does not. An operation that reads every key handed to it stays
        // usable against these buckets.
        if walk.is_directory_bucket() {
            return Err(crate::error::invalid_input(format!(
                "the bucket '{bucket_name}' is a directory bucket, which lists keys in no \
                 defined order; sync pairs its two sides by key order",
            )));
        }
        Ok(walk)
    }
}

// The local side of a download, which may name a directory that does not exist yet.
//
// Syncing a bucket into a directory nobody has created is a supported starting state: nothing is
// there, and the directory appears as entries are written. A walk reports a root it cannot open as
// the failure that ends a run, which is the right answer for a side being read from and the wrong
// one here — every listed key is simply missing, and the plan is whole.
//
// Only an absent root is swallowed. A root that exists and is not a directory still ends the run,
// because nothing can be written into it either, and a root that could not be read is a permission
// problem the caller has to hear about.
#[derive(Debug)]
pub(crate) struct LocalDestination {
    walk: FsWalk,
    pub(super) absent: bool,
    // Names what followed links reach. `None` when the run deletes nothing, because only delete
    // mode needs the names.
    pub(super) namer: Option<LinkNamer>,
    // The names followed links aliased wait here until the merge takes them.
    aliased: Vec<AliasedName>,
}

impl LocalDestination {
    // Asked before the walk starts, because a root appearing part way through a run is not the
    // starting state this swallows for. `Ok(false)` is the only answer that means absent: an error
    // leaves the walk to report it.
    pub(super) fn new(walk: FsWalk, root: &Path) -> Self {
        Self {
            walk,
            absent: matches!(root.try_exists(), Ok(false)),
            namer: Some(LinkNamer::new(root)),
            aliased: Vec::new(),
        }
    }

    // Set whether the destination names what followed links reach. A run with delete mode off
    // deletes nothing, so it resolves no link targets.
    #[must_use]
    fn naming_aliases(mut self, names: bool) -> Self {
        if !names {
            self.namer = None;
        }
        self
    }

    fn record_aliases(&mut self) {
        if let Some(namer) = &mut self.namer {
            namer.name(self.walk.followed_links(), &mut self.aliased);
        }
    }
}

impl KeyStream for LocalDestination {
    type Source = FsEntry;

    fn is_done(&self) -> bool {
        self.walk.is_done()
    }

    async fn next_entry(&mut self) -> Option<Result<Entry<FsEntry>, StreamError>> {
        let next = self.walk.next_entry().await;
        // Record the links on every outcome, because a call that returns a failure can also follow a
        // link.
        self.record_aliases();
        match next {
            Some(Err(StreamError::Walk(err)))
                if self.absent
                    && err.kind() == WalkErrorKind::SourceUnreadable
                    && nothing_was_there(&err) =>
            {
                None
            }
            other => other,
        }
    }

    fn take_aliased_names(&mut self) -> Vec<AliasedName> {
        std::mem::take(&mut self.aliased)
    }
}

// Whether the reason a root could not be opened was that it was not there.
//
// The kind cannot answer this. A walk calls anything that stopped it opening its root
// `SourceUnreadable`, so a missing directory and one that refuses to be read arrive the same way,
// and the reason has to come from the error underneath. Reading a refusal as absence would report
// every key as missing from a destination nobody could look at, and send all of them without asking
// the comparison anything.
fn nothing_was_there(err: &crate::io::walk::WalkError) -> bool {
    std::error::Error::source(err)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
}

// One local directory and one bucket, which is what an upload and a download each need. Which of
// the two is the source is the method's business, not this type's.
//
// A copy needs neither field and both endpoints: its two roots may sit in different regions and
// different accounts, so it takes two clients rather than two bucket names.
#[derive(Debug)]
pub(crate) struct LocalAndBucket {
    local_root: PathBuf,
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: Option<String>,
}

impl LocalAndBucket {
    pub(crate) fn builder() -> LocalAndBucketBuilder {
        LocalAndBucketBuilder::default()
    }
}

#[derive(Debug, Default)]
pub(crate) struct LocalAndBucketBuilder {
    local_root: Option<PathBuf>,
    client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
    prefix: Option<String>,
}

impl LocalAndBucketBuilder {
    // The directory the local side walks. Required.
    #[must_use]
    pub(crate) fn local_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.local_root = Some(root.into());
        self
    }

    // The client the listing goes through. Required.
    #[must_use]
    pub(crate) fn client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.client = Some(client);
        self
    }

    // The bucket to list. Required.
    #[must_use]
    pub(crate) fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    // The prefix the remote side is rooted at. Absent lists the whole bucket.
    #[must_use]
    pub(crate) fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    // # Panics
    //
    // Panics if `local_root`, `client` or `bucket` has not been set.
    #[must_use]
    pub(crate) fn build(self) -> LocalAndBucket {
        LocalAndBucket {
            local_root: self
                .local_root
                .expect("required field `local_root` should be set"),
            client: self.client.expect("required field `client` should be set"),
            bucket: self.bucket.expect("required field `bucket` should be set"),
            prefix: self.prefix,
        }
    }
}
