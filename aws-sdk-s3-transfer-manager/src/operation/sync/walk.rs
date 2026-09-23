/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Pairing a local tree against an S3 prefix, one key at a time.
//
// Both sides arrive in key order, which is what lets absence be worked out from position:
// once a side has produced a key sorting after the one being decided, it can never
// produce that one. Nothing else has to be remembered, which is why the merge holds one
// entry per side and no history.
//
// The walk owns both streams because the order it depends on is not what either walker
// does by default, and because one rule set has to reach both sides — a key excluded on
// one side but still listed on the other reads as a key that was deleted.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::key::filter::KeyFilter;
use crate::io::key::stream::{
    key_under_root, local_predicate, s3_predicate, Entry, KeyStream, KeysLost, StreamError,
};
use crate::io::walk::{
    FsWalk, FsWalkContext, FsWalker, S3Walk, S3WalkContext, S3Walker, SortOrder,
};

// What one side holds at the key the merge has reached.
#[derive(Debug, Clone)]
pub(crate) enum SideState<T> {
    // The side produced an entry for this key.
    Present(Entry<T>),
    // The side went past this key without producing it, so nothing is there.
    Absent,
    // The side could not account for this key. How much it lost travels with the state,
    // because a consumer reading absence from position needs to know the position is no
    // longer something to read from.
    //
    // Say the local walk cannot open `photos/2019/` and carries on with `photos/2020/`. The
    // keys under `photos/2019/` never arrive, and position alone reads that as nothing being
    // there, so delete mode would remove `photos/2019/x.jpg` from the bucket while the local
    // file sits there unread. Those keys arrive here.
    Unknown(KeysLost),
}

impl<T> SideState<T> {
    // The entry this side holds, if it holds one.
    pub(crate) fn entry(&self) -> Option<&Entry<T>> {
        match self {
            SideState::Present(entry) => Some(entry),
            SideState::Absent | SideState::Unknown(_) => None,
        }
    }

    // Whether this side failed to account for the key.
    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self, SideState::Unknown(_))
    }
}

// Whether the bytes behind a side's own item can be sent.
//
// One key, and what each side holds at it.
#[derive(Debug)]
pub(crate) struct Pairing<S, D> {
    key: String,
    source: SideState<S>,
    destination: SideState<D>,
}

impl<S, D> Pairing<S, D> {
    // Built by the merge in every other case. A test for whoever consumes a pairing needs to
    // state one directly, without a pair of streams behind it.
    #[cfg(test)]
    pub(crate) fn new(key: String, source: SideState<S>, destination: SideState<D>) -> Self {
        Self {
            key,
            source,
            destination,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn source(&self) -> &SideState<S> {
        &self.source
    }

    pub(crate) fn destination(&self) -> &SideState<D> {
        &self.destination
    }
}

// One side's next entry, held until the key it carries has been paired.
#[derive(Debug)]
enum Head<T> {
    // Nothing read from this side yet.
    Unread,
    // An entry waiting for the other side to reach its key.
    Entry(Entry<T>),
    // The side produced everything it had.
    Finished,
}

impl<T> Head<T> {
    fn entry(&self) -> Option<&Entry<T>> {
        match self {
            Head::Entry(entry) => Some(entry),
            Head::Unread | Head::Finished => None,
        }
    }
}

// A stretch of keys one failure hid, bounded by a prefix taken from the failure's own path.
//
// The bound matters because a failure does not always arrive where the keys it hid would sort. A
// directory nobody could open is reported where that directory sorts, but a symlink loop is noticed
// while reading the parent, so it arrives ahead of every sibling — including siblings sorting
// earlier than the subtree it cost. Releasing the stretch at the side's next key would let such a
// sibling stand in for keys nobody enumerated.
#[derive(Debug, Clone)]
struct Stretch {
    cost: KeysLost,
    // `None` where no prefix could be worked out, which holds the rest of the side: a stretch
    // whose extent is unknown could cover any later key.
    under: Option<String>,
}

impl Stretch {
    // Whether this key is one the failure hid.
    fn covers(&self, key: &str) -> bool {
        match &self.under {
            None => true,
            Some(name) => at_or_under(key, name),
        }
    }

    // Whether the merge reaching this key has passed everything the failure hid.
    fn passed_by(&self, key: &str) -> bool {
        match &self.under {
            None => false,
            Some(name) => !at_or_under(key, name) && key > name.as_str(),
        }
    }
}

// Whether a key is the named one or sits beneath it.
//
// The name counts as hidden along with its subtree, because the walk reports a range for a name
// whose type it could not read — it may have been a file. Matching the subtree alone would leave
// that one name reading absent, and delete mode removes what reads absent.
//
// The boundary check is what keeps a neighbour out: under `link`, the key `link/inner` is covered
// and `linkfoo` is not.
fn at_or_under(key: &str, name: &str) -> bool {
    key.starts_with(name) && (key.len() == name.len() || key.as_bytes()[name.len()] == b'/')
}

// Two key-ordered streams merged into one pairing per key.
pub(crate) struct Walk<S: KeyStream, D: KeyStream> {
    source: S,
    destination: D,
    src: Head<S::Source>,
    dst: Head<D::Source>,
    // What a side's survivable failure cost, held until the merge is past the keys it hid.
    //
    // Anything the other side holds inside that stretch is a key this one could not account for.
    // Reading such a key as absent is what allows it to be deleted, which is the whole reason the
    // answer is carried here.
    src_gap: Option<Stretch>,
    dst_gap: Option<Stretch>,
    // Keys a side named as lost, held until the merge reaches each one.
    //
    // A failure costing one key arrives before that key sorts, so the answer cannot be given
    // where it is heard. Reading a directory already collects every child before any is handed
    // over, and the errors from that read are queued with them, so these keys are a subset of what
    // the walk holds anyway.
    //
    // Ordered by key and not by arrival, because a walk reports the failures from one directory in
    // whatever order the filesystem listed it. Two unreadable children arriving as `z` then `a`
    // would leave `a` unmatched against a queue drained from the front, and an unmatched key reads
    // as absent — a delete for a file sitting on disk whose loss was reported.
    src_lost: BTreeSet<String>,
    dst_lost: BTreeSet<String>,
    // Set when a side lost one key and could not say which.
    //
    // A listing names the key it dropped. A walk names a path, and the failure arrives at the
    // position of the directory holding it while the key sorts later, somewhere inside — so
    // the one key at risk cannot be picked out from here. Holding the side's whole account
    // open is the answer that cannot delete the wrong object, and narrowing it needs the walk
    // root, which arrives with the next change.
    src_lost_unnamed: bool,
    dst_lost_unnamed: bool,
    // The root a side's paths are taken under, when that side walks a filesystem.
    //
    // A walk names the file it could not read by absolute path, and turning that into a key
    // needs the root it sits under. The stream that reported the failure was handed a walker
    // which never says what its root is, so the answer comes from here — which is what makes
    // one lost key nameable at this layer and not a layer down.
    src_root: Option<PathBuf>,
    dst_root: Option<PathBuf>,
    // Set when a key went unaccounted for, so a caller can tell a whole plan from a partial
    // one. A run that lost keys and reports a clean plan is one whose caller cannot know it
    // acted on less than it was asked about.
    incomplete: bool,
    // Set when a side reported a failure that ends it.
    //
    // A transfer needs a source, and a delete needs the source established absent, so a side
    // that has stopped supplies neither. It also answers `None` from here, which position alone
    // reads as "nothing is there" — so carrying on would read every key the other side still
    // holds as absent, and delete mode would remove all of them.
    ended_by_failure: bool,
}

impl<S: KeyStream, D: KeyStream> Walk<S, D> {
    pub(crate) fn new(source: S, destination: D) -> Self {
        Self {
            source,
            destination,
            src: Head::Unread,
            dst: Head::Unread,
            src_gap: None,
            dst_gap: None,
            src_lost: BTreeSet::new(),
            dst_lost: BTreeSet::new(),
            src_lost_unnamed: false,
            dst_lost_unnamed: false,
            src_root: None,
            dst_root: None,
            incomplete: false,
            ended_by_failure: false,
        }
    }

    // The next key either side holds, or the failure that interrupted one of them.
    // `None` once both sides have finished.
    //
    // One call reads at most one entry from each side: a head is filled only when the
    // key it held has been paired, so the side sorting ahead waits where it is.
    pub(crate) async fn next(
        &mut self,
    ) -> Option<Result<Pairing<S::Source, D::Source>, StreamError>> {
        if self.ended_by_failure {
            return None;
        }
        if matches!(self.src, Head::Unread) {
            match self.source.next_entry().await {
                Some(Ok(entry)) => self.src = Head::Entry(entry),
                // The stream says whether that failure ended it. Asking is what the
                // enumeration layer offers for this; classifying the error here would put a
                // second opinion beside the walk's own, and the two could drift apart.
                Some(Err(err)) => {
                    self.incomplete = true;
                    if self.source.is_done() {
                        self.ended_by_failure = true;
                    } else {
                        match cost_of(&err, self.src_root.as_deref()) {
                            Cost::Key(key) => {
                                self.src_lost.insert(key);
                            }
                            Cost::UnnamedKey => self.src_lost_unnamed = true,
                            Cost::Stretch(stretch) => self.src_gap = Some(stretch),
                            Cost::Nothing => {}
                        }
                    }
                    return Some(Err(err));
                }
                None => self.src = Head::Finished,
            }
        }
        if matches!(self.dst, Head::Unread) {
            match self.destination.next_entry().await {
                Some(Ok(entry)) => self.dst = Head::Entry(entry),
                Some(Err(err)) => {
                    self.incomplete = true;
                    if self.destination.is_done() {
                        self.ended_by_failure = true;
                    } else {
                        match cost_of(&err, self.dst_root.as_deref()) {
                            Cost::Key(key) => {
                                self.dst_lost.insert(key);
                            }
                            Cost::UnnamedKey => self.dst_lost_unnamed = true,
                            Cost::Stretch(stretch) => self.dst_gap = Some(stretch),
                            Cost::Nothing => {}
                        }
                    }
                    return Some(Err(err));
                }
                None => self.dst = Head::Finished,
            }
        }

        match (&self.src, &self.dst) {
            (Head::Finished, Head::Finished) => None,
            // A key one side reached while the other has run out is absent there. A side
            // sorting later says the same thing: reaching a key past this one settles
            // that this one never arrives.
            (Head::Entry(_), Head::Finished) => Some(Ok(self.take_source_only())),
            (Head::Finished, Head::Entry(_)) => Some(Ok(self.take_destination_only())),
            (Head::Entry(src), Head::Entry(dst)) => match src.key.cmp(&dst.key) {
                Ordering::Less => Some(Ok(self.take_source_only())),
                Ordering::Greater => Some(Ok(self.take_destination_only())),
                Ordering::Equal => Some(Ok(self.take_both())),
            },
            (Head::Unread, _) | (_, Head::Unread) => {
                unreachable!("both heads are filled before they are compared")
            }
        }
    }

    // The roots the two sides walk, for the sides that walk a filesystem.
    #[must_use]
    pub(crate) fn with_roots(
        mut self,
        source: Option<PathBuf>,
        destination: Option<PathBuf>,
    ) -> Self {
        self.src_root = source;
        self.dst_root = destination;
        self
    }

    // Whether every key both sides hold was accounted for.
    //
    // False once anything went unread, whether the merge carried on past it or stopped. What a
    // caller does with that belongs to whoever reports a run.
    pub(crate) fn is_plan_complete(&self) -> bool {
        !self.incomplete
    }

    // Whether anything further will be paired.
    //
    // A walk that has not been advanced answers `false`, since neither side has said yet
    // whether it holds anything. That matches what the two walkers underneath do.
    pub(crate) fn is_done(&self) -> bool {
        self.ended_by_failure || matches!((&self.src, &self.dst), (Head::Finished, Head::Finished))
    }

    fn take_source_only(&mut self) -> Pairing<S::Source, D::Source> {
        let entry = self.take_src();
        let destination = Self::missing(
            self.dst_gap.as_ref(),
            &mut self.dst_lost,
            self.dst_lost_unnamed,
            &entry.key,
        );
        Pairing {
            key: entry.key.clone(),
            source: SideState::Present(entry),
            destination,
        }
    }

    fn take_destination_only(&mut self) -> Pairing<S::Source, D::Source> {
        let entry = self.take_dst();
        let source = Self::missing(
            self.src_gap.as_ref(),
            &mut self.src_lost,
            self.src_lost_unnamed,
            &entry.key,
        );
        Pairing {
            key: entry.key.clone(),
            source,
            destination: SideState::Present(entry),
        }
    }

    // What a side that did not produce this key is saying. Absent while it is accounting for
    // itself, unknown while a failure has left it unable to, and unknown for a key it named as
    // lost.
    fn missing<T>(
        gap: Option<&Stretch>,
        lost: &mut BTreeSet<String>,
        lost_unnamed: bool,
        key: &str,
    ) -> SideState<T> {
        let_go_before(lost, key);
        if lost.remove(key) {
            return SideState::Unknown(KeysLost::OneKey);
        }
        match gap {
            Some(stretch) if stretch.covers(key) => SideState::Unknown(stretch.cost),
            Some(_) => SideState::Absent,
            // A range, because the conclusion here is that absence cannot be read from
            // position on this side any more. `OneKey` says the keys around it are known,
            // which a consumer would act on by holding back one key and trusting the rest.
            None if lost_unnamed => SideState::Unknown(KeysLost::UnknownRange),
            None => SideState::Absent,
        }
    }

    fn take_both(&mut self) -> Pairing<S::Source, D::Source> {
        // Neither side is missing here, so neither has anything to answer for — but a key held
        // from earlier has now been passed, and keeping it would grow the set for the length of
        // the run.
        let src = self.take_src();
        let dst = self.take_dst();
        let_go_before(&mut self.src_lost, &src.key);
        let_go_before(&mut self.dst_lost, &src.key);
        Pairing {
            key: src.key.clone(),
            source: SideState::Present(src),
            destination: SideState::Present(dst),
        }
    }

    fn take_src(&mut self) -> Entry<S::Source> {
        // Producing a key past the stretch accounts for everything it hid. A key inside it, or
        // one sorting before it, accounts for nothing: the failure was heard early and what it
        // cost is still ahead.
        if self.src.entry().is_some_and(|entry| {
            self.src_gap
                .as_ref()
                .is_some_and(|s| s.passed_by(&entry.key))
        }) {
            self.src_gap = None;
        }
        match std::mem::replace(&mut self.src, Head::Unread) {
            Head::Entry(entry) => entry,
            _ => unreachable!("taken only with an entry at the head"),
        }
    }

    fn take_dst(&mut self) -> Entry<D::Source> {
        if self.dst.entry().is_some_and(|entry| {
            self.dst_gap
                .as_ref()
                .is_some_and(|s| s.passed_by(&entry.key))
        }) {
            self.dst_gap = None;
        }
        match std::mem::replace(&mut self.dst, Head::Unread) {
            Head::Entry(entry) => entry,
            _ => unreachable!("taken only with an entry at the head"),
        }
    }
}

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
    // TODO(sync): `downloading` and `copying` to follow. `Mode` already picks a comparison for
    // each of the three, and the merge is generic over both side types, so a download is this
    // function with the two streams swapped. A copy needs a different context first, because one
    // `bucket` field cannot name a source and a destination bucket.
    pub(crate) fn uploading(&self, ctx: WalkContext) -> Walk<FsWalk, S3Walk> {
        let root = ctx.local_root.clone();
        let local = self.local_walk(ctx.local_root);
        let remote = self.remote_walk(ctx.client, ctx.bucket, ctx.prefix);
        // Only the source walks a filesystem here; a listing names its own lost keys.
        Walk::new(local, remote).with_roots(Some(root), None)
    }

    // The merge depends on all three of these, so this layer fixes them and a caller never
    // chooses. The walk sorts across the whole tree, because sorting each directory among itself
    // pairs keys that are not the same key. It descends all the way down, because a merge over one
    // level reports every key below it as absent. And it keeps special files, because a name that
    // is taken has to reach the comparison even though nothing can be sent for it.
    fn local_walk(&self, root: PathBuf) -> FsWalk {
        FsWalker::builder()
            .sort(SortOrder::WholeWalk)
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
    fn remote_walk(
        &self,
        client: aws_sdk_s3::Client,
        bucket: String,
        prefix: Option<String>,
    ) -> S3Walk {
        // Restore status is asked for here and nowhere else. Without it an object in an archive
        // and one with a restored copy look the same, and a comparison would either refuse every
        // archived key or hand execution a transfer that fails.
        let mut walker = S3Walker::builder()
            .request_restore_status(true)
            .filter(s3_predicate(Arc::clone(&self.filter), prefix.clone()));
        if let Some(prefix) = prefix {
            walker = walker.prefix(prefix);
        }
        let walk = walker.build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket(bucket)
                .build(),
        );
        // Absence is read from position, so a listing that is not in key order breaks the
        // merge without saying anything. A directory bucket is the case that does it, and
        // the walk can tell from the bucket's name. Refusing such a root belongs where a
        // caller names one, so this is a development backstop only — it compiles out of a
        // release build, where a root that got this far still produces an unordered merge.
        debug_assert!(
            !walk.is_directory_bucket(),
            "a directory bucket does not list in key order, which the merge depends on"
        );
        walk
    }
}

// Let go of every held key sorting before this one.
//
// The merge runs in key order, so a key still held below this one was passed without either side
// reaching it, and no decision will ever be owed for it.
fn let_go_before(lost: &mut BTreeSet<String>, key: &str) {
    *lost = lost.split_off(key);
}

// What a failure the side survived costs the merge.
//
// Two of these look alike from `keys_lost()` alone, which reports one key for both. The difference
// is whether a key exists for the merge to answer for: a file nobody could read has one, and naming it is
// a matter of working out which, where a name no key can carry has none at all.
#[derive(Debug)]
enum Cost {
    // One key, and the failure named it. The side holds that key until the merge reaches it,
    // and answers unknown there.
    Key(String),
    // One key, and which one could not be worked out. Every later key on that side has to stay
    // open, since any of them could be the one that went unread.
    UnnamedKey,
    // Every key the failure hid, under the prefix it names where one could be worked out.
    Stretch(Stretch),
    // Nothing for the merge to answer for. The name produced no key on this side, so no pairing will
    // ever ask about it, and holding anything back would hold back a key chosen at random.
    Nothing,
}

// A listing says which key it dropped. A walk says which path it could not read, and a path is not
// a key: turning one into the other needs the walk root, which lives a layer up from the stream
// that reported it.
fn cost_of(err: &StreamError, root: Option<&Path>) -> Cost {
    match err {
        // A listing names the key it dropped, already taken relative to the root.
        StreamError::MalformedListing { key: Some(key), .. } => Cost::Key(key.clone()),
        // A name no key can carry takes no part in the comparison, so there is no key here to
        // answer for. What it cost a run shows up as the plan coming out incomplete.
        StreamError::UnkeyableName(_) => Cost::Nothing,
        // A walk names a path, which is a key only once the root it sits under is known. Three
        // things can stop that: no root was supplied, the error carries no path, or the root and
        // the walk disagree about the path's form — a root left uncanonicalized against a walk
        // that resolved it, say. Each one drops back to holding the side's whole account open,
        // which is safe and much coarser, so it says so.
        StreamError::Walk(walk) if err.keys_lost() == KeysLost::OneKey => {
            match root.and_then(|root| walk.path().and_then(|p| key_under_root(root, p))) {
                Some(key) => Cost::Key(key),
                None => {
                    tracing::warn!(
                        path = ?walk.path(),
                        root = ?root,
                        "could not name the lost key, so this side's account stays open for the run"
                    );
                    Cost::UnnamedKey
                }
            }
        }
        _ if err.keys_lost() == KeysLost::OneKey => Cost::UnnamedKey,
        // A stretch, bounded by the name it cost where the failure named a path and the root is
        // known. The bound is the name itself, which `covers` reads as that name and everything
        // beneath it. Without a bound the rest of the side is answered unknown, since a failure
        // reported before the keys it hid says nothing about where they stop.
        _ => Cost::Stretch(Stretch {
            cost: err.keys_lost(),
            under: match err {
                StreamError::Walk(walk) => {
                    root.and_then(|root| walk.path().and_then(|p| key_under_root(root, p)))
                }
                _ => None,
            },
        }),
    }
}

// The two roots one run compares.
#[derive(Debug)]
pub(crate) struct WalkContext {
    local_root: PathBuf,
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: Option<String>,
}

impl WalkContext {
    pub(crate) fn builder() -> WalkContextBuilder {
        WalkContextBuilder::default()
    }
}

#[derive(Debug, Default)]
pub(crate) struct WalkContextBuilder {
    local_root: Option<PathBuf>,
    client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
    prefix: Option<String>,
}

impl WalkContextBuilder {
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
    pub(crate) fn build(self) -> WalkContext {
        WalkContext {
            local_root: self
                .local_root
                .expect("required field `local_root` should be set"),
            client: self.client.expect("required field `client` should be set"),
            bucket: self.bucket.expect("required field `bucket` should be set"),
            prefix: self.prefix,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use aws_sdk_s3::types::Object;

    use crate::io::key::filter::Rule;
    use crate::io::key::stream::{EntryMeta, Obstruction};
    use crate::io::walk::{WalkError, WalkErrorKind};

    use super::*;

    // A stream that hands back a written-down sequence, counting what was read so a test
    // can show the merge does not read ahead.
    //
    // `stops_on_failure` models a walk that a failure ended: such a walk reports itself done
    // from that moment, where one that survived its failure does not.
    struct Scripted {
        items: VecDeque<Result<Entry<()>, StreamError>>,
        reads: usize,
        stops_on_failure: bool,
        stopped: bool,
    }

    impl Scripted {
        fn of(keys: &[&str]) -> Self {
            Self::build(keys.iter().map(|k| Ok(entry(k))).collect(), false)
        }

        fn from(items: Vec<Result<Entry<()>, StreamError>>) -> Self {
            Self::build(items, false)
        }

        // A side whose failure ends it, the way a walk behaves when it cannot carry on.
        fn that_stops(items: Vec<Result<Entry<()>, StreamError>>) -> Self {
            Self::build(items, true)
        }

        fn build(items: Vec<Result<Entry<()>, StreamError>>, stops_on_failure: bool) -> Self {
            Self {
                items: items.into(),
                reads: 0,
                stops_on_failure,
                stopped: false,
            }
        }
    }

    impl KeyStream for Scripted {
        type Source = ();

        async fn next_entry(&mut self) -> Option<Result<Entry<()>, StreamError>> {
            self.reads += 1;
            let item = self.items.pop_front();
            match &item {
                // Reaching the end stops a walk, as does a failure it could not survive.
                None => self.stopped = true,
                Some(Err(_)) if self.stops_on_failure => self.stopped = true,
                _ => {}
            }
            item
        }

        fn is_done(&self) -> bool {
            self.stopped
        }
    }

    fn entry(key: &str) -> Entry<()> {
        Entry {
            key: key.to_string(),
            meta: EntryMeta {
                size: Some(1),
                last_modified_secs: Some(0),
                obstruction: None,
            },
            source: (),
        }
    }

    // One key lost, and the side carries on.
    fn a_failure() -> StreamError {
        StreamError::MalformedListing {
            key: Some("dropped.txt".to_string()),
            what: "size",
        }
    }

    // A failure that ends the side it came from.
    fn a_fatal_failure() -> StreamError {
        StreamError::Walk(WalkError::new(
            Some(PathBuf::from("/gone")),
            WalkErrorKind::SourceUnreadable,
            Box::from("the root went away"),
        ))
    }

    // Where a key was found, written short so a whole plan fits in one assertion.
    //
    // The two unknowns are kept apart, because a consumer acts on them differently: one names a
    // key it can hold back, the other says absence cannot be read from position here at all. A
    // single `Unknown` would let a change swap one for the other with every plan test still
    // passing.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum At {
        Here,
        Gone,
        UnknownKey,
        UnknownRange,
    }

    fn at<T>(side: &SideState<T>) -> At {
        match side {
            SideState::Present(_) => At::Here,
            SideState::Absent => At::Gone,
            SideState::Unknown(KeysLost::OneKey) => At::UnknownKey,
            SideState::Unknown(KeysLost::UnknownRange) => At::UnknownRange,
        }
    }

    async fn pair_up(src: &[&str], dst: &[&str]) -> Vec<(String, At, At)> {
        let mut walk = Walk::new(Scripted::of(src), Scripted::of(dst));
        let mut plan = Vec::new();
        while let Some(next) = walk.next().await {
            let pairing = next.expect("scripted sides do not fail");
            plan.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
        assert!(
            walk.is_done(),
            "the merge ends only when both sides are done"
        );
        plan
    }

    fn plan(rows: &[(&str, At, At)]) -> Vec<(String, At, At)> {
        rows.iter()
            .map(|(k, s, d)| (k.to_string(), *s, *d))
            .collect()
    }

    #[tokio::test]
    async fn identical_sides_pair_every_key_from_both() {
        assert_eq!(
            pair_up(&["a.txt", "b.txt"], &["a.txt", "b.txt"]).await,
            plan(&[("a.txt", At::Here, At::Here), ("b.txt", At::Here, At::Here)])
        );
    }

    #[tokio::test]
    async fn a_key_only_the_source_has_is_absent_from_the_destination() {
        assert_eq!(
            pair_up(&["a.txt", "b.txt"], &["b.txt"]).await,
            plan(&[("a.txt", At::Here, At::Gone), ("b.txt", At::Here, At::Here)])
        );
    }

    #[tokio::test]
    async fn a_key_only_the_destination_has_is_absent_from_the_source() {
        assert_eq!(
            pair_up(&["b.txt"], &["a.txt", "b.txt"]).await,
            plan(&[("a.txt", At::Gone, At::Here), ("b.txt", At::Here, At::Here)])
        );
    }

    #[tokio::test]
    async fn keys_past_the_end_of_the_source_still_pair() {
        assert_eq!(
            pair_up(&["a.txt"], &["a.txt", "y.txt", "z.txt"]).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("y.txt", At::Gone, At::Here),
                ("z.txt", At::Gone, At::Here),
            ])
        );
    }

    #[tokio::test]
    async fn two_empty_sides_pair_nothing() {
        assert_eq!(pair_up(&[], &[]).await, plan(&[]));
    }

    #[tokio::test]
    async fn one_pairing_reads_one_entry_from_each_side() {
        let mut walk = Walk::new(Scripted::of(&["a.txt", "b.txt"]), Scripted::of(&["a.txt"]));
        let _ = walk.next().await.expect("a pairing for a.txt");
        assert_eq!(walk.source.reads, 1, "the source is read once");
        assert_eq!(walk.destination.reads, 1, "the destination is read once");
    }

    #[tokio::test]
    async fn a_side_sorting_ahead_is_not_read_again_while_it_waits() {
        // The destination holds a key that sorts last, so it should sit at the head while
        // the source is drained past it.
        let mut walk = Walk::new(Scripted::of(&["a.txt", "b.txt"]), Scripted::of(&["z.txt"]));
        let _ = walk.next().await.expect("a.txt");
        let _ = walk.next().await.expect("b.txt");
        assert_eq!(
            walk.destination.reads, 1,
            "the waiting side is read once, not once per pairing"
        );
    }

    #[tokio::test]
    async fn a_failure_costing_one_key_is_reported_and_the_merge_carries_on() {
        let mut walk = Walk::new(
            Scripted::from(vec![Err(a_failure()), Ok(entry("b.txt"))]),
            Scripted::of(&["b.txt"]),
        );
        let reported = walk.next().await.expect("the failure");
        assert!(
            matches!(reported, Err(StreamError::MalformedListing { .. })),
            "the failure reaches the caller as it was reported"
        );
        let after = walk.next().await.expect("b.txt").expect("a pairing");
        assert_eq!(after.key(), "b.txt");
        assert_eq!(at(after.source()), At::Here);
        assert_eq!(at(after.destination()), At::Here);
    }

    #[tokio::test]
    async fn a_failure_does_not_discard_the_other_sides_entry() {
        // `z.txt` sorts last, so it is sitting at the destination head when the source
        // fails. That is the entry a careless implementation would drop.
        let mut walk = Walk::new(
            Scripted::from(vec![Ok(entry("a.txt")), Err(a_failure())]),
            Scripted::of(&["z.txt"]),
        );
        let first = walk.next().await.expect("a.txt").expect("a pairing");
        assert_eq!(first.key(), "a.txt");
        let _ = walk.next().await.expect("the failure");
        let after = walk.next().await.expect("z.txt").expect("a pairing");
        assert_eq!(
            after.key(),
            "z.txt",
            "the entry held at the other head survives the failure"
        );
        assert_eq!(at(after.destination()), At::Here);
    }

    // A directory nobody could open, taken from the design's own example.
    fn a_lost_directory() -> StreamError {
        StreamError::Walk(WalkError::new(
            Some(PathBuf::from("/root/photos/2019")),
            WalkErrorKind::DirectoryUnreadable,
            Box::from("permission denied"),
        ))
    }

    #[tokio::test]
    async fn keys_a_lost_directory_hid_are_unknown_and_not_absent() {
        // The source cannot open `photos/2019/` and carries on at `photos/2020/`. The
        // destination holds a key inside the directory nobody read, and reporting it absent
        // is what would delete it.
        let mut walk = Walk::new(
            Scripted::from(vec![
                Ok(entry("photos/2018/a.jpg")),
                Err(a_lost_directory()),
                Ok(entry("photos/2020/b.jpg")),
            ]),
            Scripted::of(&[
                "photos/2018/a.jpg",
                "photos/2019/x.jpg",
                "photos/2020/b.jpg",
            ]),
        );
        let mut seen = Vec::new();
        while let Some(next) = walk.next().await {
            if let Ok(pairing) = next {
                seen.push((
                    pairing.key().to_string(),
                    at(pairing.source()),
                    at(pairing.destination()),
                ));
            }
        }
        assert_eq!(
            seen,
            plan(&[
                ("photos/2018/a.jpg", At::Here, At::Here),
                ("photos/2019/x.jpg", At::UnknownRange, At::Here),
                ("photos/2020/b.jpg", At::Here, At::Here),
            ]),
            "the key inside the unread directory is unknown, and the sibling after it compares"
        );
    }

    #[tokio::test]
    async fn a_key_outside_the_lost_subtree_is_absent() {
        // The failure cost `photos/2019/` and nothing else, so `z.txt` is absent as usual. What
        // bounds it is the prefix taken from the failure's own path: the side producing a later key
        // settles nothing, because a failure can be heard before the keys it hid.
        let mut walk = Walk::new(
            Scripted::from(vec![Err(a_lost_directory()), Ok(entry("m.txt"))]),
            Scripted::of(&["m.txt", "z.txt"]),
        )
        .with_roots(Some(PathBuf::from("/root")), None);
        let _ = walk.next().await.expect("the failure");
        let m = walk.next().await.expect("m.txt").expect("a pairing");
        assert_eq!(m.key(), "m.txt");
        let z = walk.next().await.expect("z.txt").expect("a pairing");
        assert_eq!(at(z.source()), At::Gone, "the gap closed at m.txt");
    }

    // A listing that dropped one object and named which.
    fn a_lost_key(key: &str) -> StreamError {
        StreamError::MalformedListing {
            key: Some(key.to_string()),
            what: "size",
        }
    }

    async fn drain(mut walk: Walk<Scripted, Scripted>) -> Vec<(String, At, At)> {
        let mut seen = Vec::new();
        while let Some(next) = walk.next().await {
            if let Ok(pairing) = next {
                seen.push((
                    pairing.key().to_string(),
                    at(pairing.source()),
                    at(pairing.destination()),
                ));
            }
        }
        seen
    }

    #[tokio::test]
    async fn a_key_a_side_named_as_lost_is_unknown_where_it_sorts() {
        // The destination drops `m.txt` and says so while handing over `a.txt`, so the answer is
        // due three keys later. Its neighbours are unaffected.
        let walk = Walk::new(
            Scripted::of(&["a.txt", "m.txt", "q.txt", "z.txt"]),
            Scripted::from(vec![
                Ok(entry("a.txt")),
                Err(a_lost_key("m.txt")),
                Ok(entry("q.txt")),
                Ok(entry("z.txt")),
            ]),
        );
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("m.txt", At::Here, At::UnknownKey),
                ("q.txt", At::Here, At::Here),
                ("z.txt", At::Here, At::Here),
            ]),
            "one key is unknown on the side that lost it, and the keys around it compare"
        );
    }

    #[tokio::test]
    async fn a_key_the_source_lost_is_unknown_there() {
        // Downloading makes the source a listing, so the side naming a dropped key can be
        // either one.
        //
        // `q.txt` is the key that shows only `m.txt` is affected. Treating the
        // failure as a stretch would hold every destination-only key until the source spoke
        // again, and `q.txt` sits inside that window.
        let walk = Walk::new(
            Scripted::from(vec![
                Ok(entry("a.txt")),
                Err(a_lost_key("m.txt")),
                Ok(entry("z.txt")),
            ]),
            Scripted::of(&["a.txt", "m.txt", "q.txt", "z.txt"]),
        );
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("m.txt", At::UnknownKey, At::Here),
                ("q.txt", At::Gone, At::Here),
                ("z.txt", At::Here, At::Here),
            ])
        );
    }

    #[tokio::test]
    async fn a_held_key_is_let_go_once_the_merge_is_past_it() {
        // Held keys are bounded by what the walk has open, which only holds while keys nobody
        // reaches are let go. Without that the list grows for the length of the run.
        let mut walk = Walk::new(
            Scripted::of(&["z.txt"]),
            Scripted::from(vec![Err(a_lost_key("m.txt")), Ok(entry("z.txt"))]),
        );
        let _ = walk.next().await.expect("the failure");
        assert_eq!(walk.dst_lost.len(), 1, "the key is held when it is named");
        let _ = walk.next().await.expect("z.txt");
        assert!(
            walk.dst_lost.is_empty(),
            "a key the merge passed without reaching is no longer held"
        );
    }

    #[tokio::test]
    async fn a_lost_key_nobody_reaches_leaves_later_keys_alone() {
        // `m.txt` is named as lost and neither side ever produces it, so the answer is due at a
        // key that never arrives. `z.txt` must not inherit it.
        let walk = Walk::new(
            Scripted::of(&["z.txt"]),
            Scripted::from(vec![Err(a_lost_key("m.txt"))]),
        );
        assert_eq!(
            drain(walk).await,
            plan(&[("z.txt", At::Here, At::Gone)]),
            "a held key that nobody reached says nothing about a later one"
        );
    }

    // One file the walk could not read. The error names a path, not a key, and it arrives at
    // the directory's position rather than where the file sorts inside it.
    fn one_file_unread() -> StreamError {
        StreamError::Walk(WalkError::new(
            Some(PathBuf::from("/root/photos/2019/b.jpg")),
            WalkErrorKind::PermissionDenied,
            Box::from("permission denied"),
        ))
    }

    #[tokio::test]
    async fn a_lost_key_nobody_could_name_holds_the_side_open() {
        // Which key was lost cannot be worked out here, so no later key on that side may be
        // called absent — that is what would delete the object matching the file nobody read.
        let walk = Walk::new(
            Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt", "q.txt", "z.txt"]),
        );
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("q.txt", At::UnknownRange, At::Here),
                ("z.txt", At::UnknownRange, At::Here),
            ]),
            "every later key stays unknown on the side that lost one it could not name"
        );
    }

    // A directory that lists but whose children cannot be stat'd: readable, not searchable.
    // `None` when the mode has no effect, as it has none for root.
    #[cfg(unix)]
    fn unstattable_dir(root: &std::path::Path) -> Option<PathBuf> {
        use std::os::unix::fs::PermissionsExt;

        let locked = root.join("locked");
        std::fs::create_dir(&locked).expect("a directory");
        std::fs::write(locked.join("secret.txt"), "").expect("a file inside it");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o400)).expect("chmod");
        let listable = std::fs::read_dir(&locked).is_ok();
        let stattable = std::fs::metadata(locked.join("secret.txt")).is_ok();
        if listable && !stattable {
            Some(locked)
        } else {
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
            None
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_real_walk_failure_is_named_against_the_root_the_walk_reports() {
        use std::os::unix::fs::PermissionsExt;

        // The root this layer holds and the paths the walk puts in its errors have to agree on
        // form. They do only because the walk is built without canonicalizing its root — turn
        // that on and the root stays `/var/...` while the walk reports `/private/var/...`, the
        // prefix no longer strips, and every key after the failure goes back to unknown. A
        // scripted failure cannot catch that, because the test writes both halves.
        let dir = tempfile::tempdir().expect("a temp dir");
        let Some(locked) = unstattable_dir(dir.path()) else {
            return;
        };

        let walker = Walker::builder().build();
        let mut walk = Walk::new(
            walker.local_walk(dir.path().to_path_buf()),
            Scripted::of(&["locked/secret.txt", "z.txt"]),
        )
        .with_roots(Some(dir.path().to_path_buf()), None);

        let mut seen = Vec::new();
        while let Some(next) = walk.next().await {
            if let Ok(pairing) = next {
                seen.push((
                    pairing.key().to_string(),
                    at(pairing.source()),
                    at(pairing.destination()),
                ));
            }
        }
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        assert_eq!(
            seen,
            plan(&[
                ("locked/secret.txt", At::UnknownKey, At::Here),
                ("z.txt", At::Gone, At::Here),
            ]),
            "the unreadable file's own key is unknown, and the key after it is absent"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn two_keys_lost_from_one_directory_are_both_unknown() {
        use std::os::unix::fs::PermissionsExt;

        // A directory that lists but whose children cannot be stat'd, holding two files. The walk
        // reports both in the order the filesystem hands them over, which is not key order.
        let dir = tempfile::tempdir().expect("a temp dir");
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("a directory");
        std::fs::write(locked.join("z.txt"), "").expect("a file");
        std::fs::write(locked.join("a.txt"), "").expect("a file");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o400)).expect("chmod");
        let usable =
            std::fs::read_dir(&locked).is_ok() && std::fs::metadata(locked.join("a.txt")).is_err();
        assert!(
            usable,
            "this platform does not produce the state under test"
        );

        let walker = Walker::builder().build();
        let mut walk = Walk::new(
            walker.local_walk(dir.path().to_path_buf()),
            Scripted::of(&["locked/a.txt", "locked/z.txt"]),
        )
        .with_roots(Some(dir.path().to_path_buf()), None);

        let mut seen = Vec::new();
        while let Some(next) = walk.next().await {
            if let Ok(pairing) = next {
                seen.push((
                    pairing.key().to_string(),
                    at(pairing.source()),
                    at(pairing.destination()),
                ));
            }
        }
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        assert_eq!(
            seen,
            plan(&[
                ("locked/a.txt", At::UnknownKey, At::Here),
                ("locked/z.txt", At::UnknownKey, At::Here),
            ]),
            "both files exist and both were reported unreadable, so neither may read as absent"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_loss_reported_before_its_own_position_still_covers_the_keys_it_hid() {
        // A symlink pointing at its own ancestor. The walk notices while reading the parent, so the
        // failure arrives ahead of every sibling — including ones sorting earlier than the subtree
        // it hid. A key inside that subtree must not read as absent.
        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::write(dir.path().join("a.txt"), "").expect("a file");
        std::os::unix::fs::symlink(".", dir.path().join("zlink")).expect("a self-referring link");

        let walker = Walker::builder().follow_symlinks(true).build();
        let mut walk = Walk::new(
            walker.local_walk(dir.path().to_path_buf()),
            Scripted::of(&["a.txt", "zlink/deep.txt"]),
        )
        .with_roots(Some(dir.path().to_path_buf()), None);

        let mut seen = Vec::new();
        while let Some(next) = walk.next().await {
            if let Ok(pairing) = next {
                seen.push((
                    pairing.key().to_string(),
                    at(pairing.source()),
                    at(pairing.destination()),
                ));
            }
        }
        assert_eq!(
            seen,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("zlink/deep.txt", At::UnknownRange, At::Here),
            ]),
            "nobody enumerated under the link, so what is there cannot be read from position"
        );
    }

    #[tokio::test]
    async fn a_walk_failure_names_one_key_once_the_root_is_known() {
        // The same unreadable file, with the root supplied. `/root/photos/2019/b.jpg` under
        // `/root` is the key `photos/2019/b.jpg`, so that one key is held and `z.txt` after it
        // is absent as usual — where without a root the whole side would read as unknown.
        let walk = Walk::new(
            Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt", "photos/2019/b.jpg", "z.txt"]),
        )
        .with_roots(Some(PathBuf::from("/root")), None);
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("photos/2019/b.jpg", At::UnknownKey, At::Here),
                ("z.txt", At::Gone, At::Here),
            ]),
            "one key is unknown, and the key after it compares"
        );
    }

    #[tokio::test]
    async fn a_named_lost_key_is_reported_as_one_key() {
        // Naming the key is what lets a consumer act on it alone, which is what `OneKey`
        // promises and `UnknownRange` does not.
        let mut walk = Walk::new(
            Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt", "photos/2019/b.jpg"]),
        )
        .with_roots(Some(PathBuf::from("/root")), None);
        let _ = walk.next().await.expect("the failure");
        let _ = walk.next().await.expect("a.txt");
        let lost = walk.next().await.expect("the lost key").expect("a pairing");
        assert_eq!(lost.key(), "photos/2019/b.jpg");
        assert!(
            matches!(lost.source(), SideState::Unknown(KeysLost::OneKey)),
            "got {:?}",
            lost.source()
        );
    }

    #[tokio::test]
    async fn a_key_lost_without_a_name_is_reported_as_a_range() {
        // What a consumer is told has to match what was concluded. `OneKey` says the keys
        // around it are known, and acting on that means holding one key back and trusting the
        // rest — the opposite of what this side can promise.
        let mut walk = Walk::new(
            Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt", "z.txt"]),
        );
        let _ = walk.next().await.expect("the failure");
        let _ = walk.next().await.expect("a.txt");
        let z = walk.next().await.expect("z.txt").expect("a pairing");
        assert!(
            matches!(z.source(), SideState::Unknown(KeysLost::UnknownRange)),
            "got {:?}",
            z.source()
        );
    }

    // A local name that is not text, so no key could carry it.
    fn a_name_no_key_can_carry() -> StreamError {
        StreamError::UnkeyableName(PathBuf::from("/root/photos/caf\u{FFFD}.txt"))
    }

    #[tokio::test]
    async fn a_name_no_key_can_carry_holds_nothing_back() {
        // Such a name never becomes a key, so no pairing ever asks about it and there is nothing
        // to answer for. Treating the side as unable to account for itself would report every
        // later key as unknown, suppressing every delete in the run over a key the comparison
        // never saw.
        let walk = Walk::new(
            Scripted::from(vec![Err(a_name_no_key_can_carry()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt", "photos/caf\u{FFFD}.txt", "z.txt"]),
        );
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("photos/caf\u{FFFD}.txt", At::Gone, At::Here),
                ("z.txt", At::Gone, At::Here),
            ]),
            "the object at the lossy key is an orphan like any other, and later keys compare"
        );
    }

    #[tokio::test]
    async fn a_name_no_key_can_carry_still_leaves_the_plan_incomplete() {
        // Nothing is held, but a file was not accounted for, so the run cannot claim it covered
        // everything it was pointed at.
        let mut walk = Walk::new(
            Scripted::from(vec![Err(a_name_no_key_can_carry()), Ok(entry("a.txt"))]),
            Scripted::of(&["a.txt"]),
        );
        while walk.next().await.is_some() {}
        assert!(!walk.is_plan_complete());
    }

    #[tokio::test]
    async fn a_run_a_fatal_failure_ended_reports_a_partial_plan() {
        // The merge stopped, so most of the keyspace was never looked at. A caller reading the
        // short plan as a whole one is the mistake this answer exists to prevent.
        let mut walk = Walk::new(
            Scripted::that_stops(vec![Err(a_fatal_failure())]),
            Scripted::of(&["a.txt", "b.txt", "c.txt"]),
        );
        while walk.next().await.is_some() {}
        assert!(walk.is_done());
        assert!(!walk.is_plan_complete());
    }

    #[tokio::test]
    async fn a_clean_run_reports_a_whole_plan() {
        let mut walk = Walk::new(Scripted::of(&["a.txt"]), Scripted::of(&["a.txt"]));
        while walk.next().await.is_some() {}
        assert!(walk.is_plan_complete());
    }

    #[tokio::test]
    async fn a_run_that_lost_a_key_reports_a_partial_plan() {
        let mut walk = Walk::new(
            Scripted::of(&["a.txt"]),
            Scripted::from(vec![Ok(entry("a.txt")), Err(a_lost_key("m.txt"))]),
        );
        while walk.next().await.is_some() {}
        assert!(
            !walk.is_plan_complete(),
            "a caller cannot tell it acted on less than it asked about unless the run says so"
        );
    }

    #[tokio::test]
    async fn a_fatal_failure_on_the_source_ends_the_merge() {
        let mut walk = Walk::new(
            Scripted::that_stops(vec![Err(a_fatal_failure())]),
            Scripted::of(&["a.txt", "b.txt", "c.txt"]),
        );
        let reported = walk.next().await.expect("the failure");
        assert!(reported.is_err(), "the failure reaches the caller");
        assert!(
            walk.next().await.is_none(),
            "a side that has stopped cannot say a key is absent, so nothing more is paired"
        );
        assert!(walk.is_done());
    }

    #[tokio::test]
    async fn a_fatal_failure_on_the_destination_ends_the_merge() {
        let mut walk = Walk::new(
            Scripted::of(&["a.txt", "b.txt", "c.txt"]),
            Scripted::that_stops(vec![Err(a_fatal_failure())]),
        );
        let reported = walk.next().await.expect("the failure");
        assert!(reported.is_err(), "the failure reaches the caller");
        assert!(
            walk.next().await.is_none(),
            "nothing may be written to or removed from a side nobody could read"
        );
        assert!(walk.is_done());
    }

    #[tokio::test]
    async fn keys_past_the_end_of_the_destination_still_pair() {
        assert_eq!(
            pair_up(&["a.txt", "y.txt", "z.txt"], &["a.txt"]).await,
            plan(&[
                ("a.txt", At::Here, At::Here),
                ("y.txt", At::Here, At::Gone),
                ("z.txt", At::Here, At::Gone),
            ])
        );
    }

    fn a_client() -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-west-2"))
            .http_client(aws_smithy_runtime::client::http::test_util::NeverClient::new())
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    // Walk a real tree through the configuration this layer applies, and report each key with
    // whatever stops it being transferred.
    //
    // Driven through the key stream, so the answer read here is the one production sets rather
    // than a rule restated in the test.
    async fn walk_locally(
        root: &std::path::Path,
        filter: KeyFilter,
    ) -> Vec<(String, Option<Obstruction>)> {
        let walker = Walker::builder().filter(Arc::new(filter)).build();
        let mut walk = walker.local_walk(root.to_path_buf());
        let mut seen = Vec::new();
        while let Some(next) = walk.next_entry().await {
            let entry = next.expect("this tree reads cleanly");
            seen.push((entry.key, entry.meta.obstruction));
        }
        // Left in the order the walk produced, because that order is what the merge depends
        // on and a sort here would hide it.
        seen
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_named_pipe_is_walked_and_cannot_have_its_bytes_sent() {
        use nix::sys::stat::Mode;

        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::write(dir.path().join("a.txt"), b"x").expect("a regular file");
        let fifo = dir.path().join("pipe");
        nix::unistd::mkfifo(&fifo, Mode::S_IRWXU).expect("a named pipe");
        assert!(
            std::fs::symlink_metadata(&fifo).is_ok(),
            "the pipe has to exist for this test to mean anything"
        );

        assert_eq!(
            walk_locally(dir.path(), KeyFilter::new(Vec::new())).await,
            vec![
                ("a.txt".to_string(), None),
                ("pipe".to_string(), Some(Obstruction::NothingToRead)),
            ],
            "a special file is walked, and says nothing can be read from it"
        );
        // `a.txt` before `pipe` is alphabetical either way, so this says nothing about
        // ordering; `the_local_side_is_walked_in_key_order` is what pins that.
    }

    #[tokio::test]
    async fn the_local_side_is_walked_in_key_order() {
        // A listing puts `a/b.txt` before `z.txt`, because `a/` sorts before `z`. A walk that
        // orders each directory among itself hands over the root's files first and descends
        // afterwards, so `z.txt` would come out ahead of `a/b.txt` — two keys the merge would
        // then pair against the wrong counterparts.
        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::create_dir(dir.path().join("a")).expect("a directory");
        std::fs::write(dir.path().join("a/b.txt"), b"x").expect("a nested file");
        std::fs::write(dir.path().join("z.txt"), b"x").expect("a root file");

        let seen: Vec<String> = walk_locally(dir.path(), KeyFilter::new(Vec::new()))
            .await
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(seen, vec!["a/b.txt".to_string(), "z.txt".to_string()]);
    }

    #[tokio::test]
    async fn one_rule_set_decides_what_the_local_side_yields() {
        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::create_dir(dir.path().join("logs")).expect("a directory");
        std::fs::write(dir.path().join("keep.txt"), b"x").expect("a kept file");
        std::fs::write(dir.path().join("logs/drop.txt"), b"x").expect("an excluded file");

        let filter = KeyFilter::new(vec![Rule::exclude("logs/*")]);
        assert_eq!(
            walk_locally(dir.path(), filter).await,
            vec![("keep.txt".to_string(), None)],
            "an excluded key never reaches the merge, on either side"
        );
    }

    #[tokio::test]
    async fn the_listing_asks_for_one_unbroken_run_of_keys() {
        // A delimiter rolls keys up into prefixes and a continuation token starts the listing
        // part way through. Either shortens this side, and the merge reads a short side as
        // keys that are not there. The rule matches only a request carrying neither, so a walk
        // that sets one gets no answer and the listing fails.
        let listing = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(vec![
                // A folder marker: zero bytes under a name ending in the delimiter. The
                // walker drops these by default, and attaching a filter must not replace that.
                Object::builder()
                    .key("photos/")
                    .size(0)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                    .build(),
                Object::builder()
                    .key("photos/a.jpg")
                    .size(1)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                    .build(),
                // The rules below exclude this one. A listing side that yields it anyway
                // would pair it against a local key the rules also hid, and the merge would
                // read one of them as deleted.
                Object::builder()
                    .key("photos/thumbs/t.jpg")
                    .size(1)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                    .build(),
            ]))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .match_requests(|req| {
                req.delimiter().is_none()
                    && req.continuation_token().is_none()
                    // Asked for, so an archived object can be told from a restored one. A listing
                    // without it reports no restore status at all, and every archived key would be
                    // skipped for the life of the bucket.
                    && req.optional_object_attributes()
                        == [aws_sdk_s3::types::OptionalObjectAttributes::RestoreStatus]
            })
            .then_output(move || listing.clone());
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[rule]
        );

        let walker = Walker::builder()
            .filter(Arc::new(KeyFilter::new(vec![Rule::exclude("thumbs/*")])))
            .build();
        let mut walk = walker.remote_walk(
            client,
            "amzn-s3-demo-bucket".to_string(),
            Some("photos/".to_string()),
        );
        let mut keys = Vec::new();
        let mut obstructions = Vec::new();
        while let Some(next) = walk.next_entry().await {
            let entry = next.expect("the listing answers");
            obstructions.push(entry.meta.obstruction);
            keys.push(entry.key);
        }
        assert_eq!(
            obstructions,
            vec![None],
            "a listed object can have its bytes read, which is all a listing can say today"
        );
        assert_eq!(
            keys,
            vec!["a.jpg".to_string()],
            "the folder marker and the excluded key are both absent, and the rules reach this \
             side as well as the local one"
        );
    }

    #[tokio::test]
    async fn a_loss_naming_the_root_itself_covers_every_key_under_it() {
        let err = StreamError::Walk(WalkError::new(
            Some(PathBuf::from("/root")),
            WalkErrorKind::DirectoryUnreadable,
            Box::from("permission denied"),
        ));
        let walk = Walk::new(
            Scripted::from(vec![Err(err), Ok(entry("zzz.txt"))]),
            Scripted::of(&["a.txt", "m.txt", "zzz.txt"]),
        )
        .with_roots(Some(PathBuf::from("/root")), None);
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("a.txt", At::UnknownRange, At::Here),
                ("m.txt", At::UnknownRange, At::Here),
                ("zzz.txt", At::Here, At::Here),
            ]),
            "a failure naming the root hid every key under it"
        );
    }

    #[tokio::test]
    async fn a_lost_name_of_unknown_type_is_covered_with_its_subtree() {
        let err = StreamError::Walk(WalkError::new(
            Some(PathBuf::from("/root/link")),
            WalkErrorKind::DirectoryUnreadable,
            Box::from("permission denied"),
        ));
        let walk = Walk::new(
            Scripted::from(vec![Err(err), Ok(entry("zzz.txt"))]),
            Scripted::of(&["link", "link/inner.txt", "zzz.txt"]),
        )
        .with_roots(Some(PathBuf::from("/root")), None);
        assert_eq!(
            drain(walk).await,
            plan(&[
                ("link", At::UnknownRange, At::Here),
                ("link/inner.txt", At::UnknownRange, At::Here),
                ("zzz.txt", At::Here, At::Here),
            ]),
            "the name whose type was unknown is as unaccounted for as its subtree"
        );
    }

    #[tokio::test]
    async fn a_walk_is_built_against_both_roots() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let walk = Walker::builder().build().uploading(
            WalkContext::builder()
                .local_root(dir.path())
                .client(a_client())
                .bucket("amzn-s3-demo-bucket")
                .prefix("photos/")
                .build(),
        );
        assert!(
            !walk.is_done(),
            "a walk that has read nothing has not finished"
        );
        // Without this the local side cannot turn a failure's path into a key, and one
        // unreadable file would hold that side's whole account open.
        assert_eq!(
            walk.src_root.as_deref(),
            Some(dir.path()),
            "the side that walks a filesystem is told the root it walks"
        );
        assert_eq!(
            walk.dst_root, None,
            "a listing names its own lost keys, so it needs no root"
        );
    }
}
