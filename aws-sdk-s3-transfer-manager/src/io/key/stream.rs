/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Ordered key streams for comparing a local directory against an S3 prefix.
//
// Comparing two sides reads absence from position, so both must be keyed the same
// way: relative to their own root, in `ListObjectsV2` order. The walkers yield
// their own item types, so this module derives a key and comparable metadata for
// each.

use std::borrow::Cow;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::types::Object;

use crate::io::FileType;

use super::filter::KeyFilter;
use super::{derive_object_key, DEFAULT_DELIMITER};
use crate::io::walk::{
    exclude_s3_folder_markers, FsEntry, FsWalk, S3Walk, WalkError, WalkErrorKind,
};

// Metadata of an `Entry`, read off the item the walker produced and kept to what a comparison
// needs to tell whether two sides differ.
//
// Whole seconds, because that is the granularity S3 reports last-modified at. Keeping finer
// local precision would make an identical pair differ every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMeta {
    // `None` when the side never read it. A number invented here would be a length nobody
    // observed, and a real zero-byte object would be indistinguishable from an entry the walk
    // could not describe.
    pub(crate) size: Option<u64>,
    // Optional because a side may have no time to report: the walk read no metadata, or the
    // platform does not supply one.
    //
    // Do not compare two of these with `<`. `None` is less than every `Some`, so a source with no
    // time reads as older than a destination that has one: asking "is the destination older?" gives
    // `false`, and the key is skipped.
    //
    // Note that the S3 side always produces `Some`.
    pub(crate) last_modified_secs: Option<i64>,
    // What stops this item taking part in a transfer, where the walk or the listing already
    // shows it. `None` for an ordinary file or object.
    pub(crate) obstruction: Option<Obstruction>,
}

// Why an item cannot take part in a transfer.
//
// The name still has to reach a comparison, because a taken name is what keeps the matching key
// on the other side from being deleted. What a comparison needs beyond that is why nothing can
// be sent, and the causes differ in whether anything can be done about them.
//
// Some obstructions never appear here. A retention lock, a legal hold, and which tier an
// Intelligent-Tiering object currently sits in each need a request per object, so they arrive as
// a failed transfer. Conditions on the action instead of the item arrive the same way — a key no
// filename on the destination platform can hold, a path too long, a full disk — because where the
// destination lacks the key there is no item here to hang them on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Obstruction {
    // A socket, a device, a named pipe, or a symlink the walk was told not to follow: the name is
    // taken and holds nothing a transfer could read, and reading one may never finish.
    NothingToRead,
}

impl Obstruction {
    // Whether this also stops the item being written over.
    //
    // A name with nothing behind it does: replacing a device is not what anyone asked for, and
    // writing to a pipe nobody reads never returns.
    pub(crate) fn blocks_overwrite(&self) -> bool {
        match self {
            Self::NothingToRead => true,
        }
    }
}

// An item a walker produced, carried under the key a comparison pairs it by, with the metadata
// that comparison reads.
//
// `source` travels with the key because the key alone is not enough to act on: a path rebuilt
// from it is not the path that was walked, and on the S3 side the listing's storage class and
// restore status decide whether the object is readable at all.
#[derive(Debug, Clone)]
pub(crate) struct Entry<T> {
    pub(crate) key: String,
    pub(crate) meta: EntryMeta,
    pub(crate) source: T,
}

// What can go wrong while producing keyed entries.
#[derive(Debug)]
pub(crate) enum StreamError {
    // The underlying walk failed. Whether it ends the run is the walk's own answer.
    Walk(WalkError),
    // A local name that is not valid UTF-8, so no S3 key could carry it. One name, and the walk
    // read it fine.
    UnkeyableName(PathBuf),
    // A listed object without a field a comparison needs. The key is named where the listing gave
    // one, so a consumer can hold back the action for that key alone.
    MalformedListing {
        key: Option<String>,
        what: &'static str,
    },
}

impl StreamError {
    // Whether nothing is left to carry on with. Only a walk can say so: a name that cannot be
    // keyed and an object missing a field each cost one key.
    pub(crate) fn is_fatal(&self) -> bool {
        match self {
            StreamError::Walk(err) => err.is_fatal(),
            StreamError::UnkeyableName(_) | StreamError::MalformedListing { .. } => false,
        }
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Walk(err) => write!(f, "{err}"),
            // `OsStr`'s `Debug` escapes invalid bytes, where `Path::display` replaces them and
            // prints two different names identically.
            StreamError::UnkeyableName(path) => {
                write!(f, "name is not valid UTF-8: {:?}", path.as_os_str())
            }
            StreamError::MalformedListing {
                key: Some(key),
                what,
            } => {
                write!(f, "{what}: {key}")
            }
            StreamError::MalformedListing { key: None, what } => write!(f, "{what}"),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StreamError::Walk(err) => Some(err),
            StreamError::UnkeyableName(_) | StreamError::MalformedListing { .. } => None,
        }
    }
}

impl From<WalkError> for StreamError {
    fn from(err: WalkError) -> Self {
        StreamError::Walk(err)
    }
}

// Errors pass through as the walkers report them; what they mean is the caller's
// call, via `keys_lost`. A run may continue past an unreadable directory, but
// not while also deleting keys it never saw.
pub(crate) trait KeyStream {
    type Source;

    // Named to avoid colliding with the walkers' inherent `next`.
    fn next_entry(
        &mut self,
    ) -> impl Future<Output = Option<Result<Entry<Self::Source>, StreamError>>> + Send;

    // Whether the stream has stopped, whether because it reached the end or because a
    // failure ended it.
    //
    // A consumer needs this to tell those apart from the failure it was just handed: a
    // stream that stopped answers `None` from here on, and position alone reads that as
    // "no key is there". Classifying the error instead would put a second opinion in the
    // consumer, and `WalkErrorKind` is `#[non_exhaustive]`, so the two could disagree.
    fn is_done(&self) -> bool;
}

// What a failure cost the side that hit it.
//
// A comparison reads absence from position, so before it can act on "this key is missing from the
// other side" it has to know whether the side it is reading is still able to account for its keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeysLost {
    // One key, and the keys around it are known. Whether a consumer can act on that one key alone
    // depends on which failure produced it.
    //
    // A listing names the key it dropped. A name no key can carry names the file itself, at that
    // file's own position, so it is just as identifiable — what it cannot do is produce a key to
    // match against the other side, which is why nothing can be sent for it even though the name is
    // taken. A walk failure is the weak one: it carries an absolute path nothing here turns into a
    // key, and it arrives at the position of the directory holding it, so all a consumer learns is
    // that some key inside that directory is gone.
    OneKey,
    // An unknown range. A subtree went unenumerated, or the side stopped before its end, so
    // absence cannot be read from position at all.
    UnknownRange,
}

impl StreamError {
    // Matched exhaustively rather than tested against one kind, so a kind added later has to be
    // placed deliberately instead of defaulting to the answer that permits a delete.
    pub(crate) fn keys_lost(&self) -> KeysLost {
        match self {
            StreamError::Walk(err) => match err.kind() {
                // The walk never got going, or gave up part way.
                WalkErrorKind::SourceUnreadable
                | WalkErrorKind::NotADirectory
                | WalkErrorKind::Service => KeysLost::UnknownRange,
                // A directory nobody could read, and a cycle that stopped a descent, both leave a
                // subtree unenumerated. No single key stands for a subtree.
                WalkErrorKind::DirectoryUnreadable | WalkErrorKind::SymlinkCycle => {
                    KeysLost::UnknownRange
                }
                // One entry that should have been readable.
                WalkErrorKind::Io
                | WalkErrorKind::PermissionDenied
                | WalkErrorKind::BrokenSymlink => KeysLost::OneKey,
            },
            // One entry, like the devices and sockets it is grouped with, and not a range — losing a
            // whole directory is a separate case. What it costs is this: the name is taken, and no key
            // can carry it, so the upload path's lossy derivation may already have put an object
            // where a walk cannot look. Not `Nothing`, which would license deleting that object.
            //
            // The truthful state is narrower than either: the key is occupied and nothing can be
            // sent for it. Saying so needs an answer beside transfer and skip, which the comparison
            // owns; until then this is the conservative half of it.
            StreamError::UnkeyableName(_) => KeysLost::OneKey,
            // The object was listed and then dropped, so its key is absent from this side while
            // the keys around it arrived.
            StreamError::MalformedListing { key: Some(_), .. } => KeysLost::OneKey,
            // No key to name, so a consumer cannot hold back the action for it. One key is gone and
            // which one is unknowable, which is a range of one.
            StreamError::MalformedListing { key: None, .. } => KeysLost::UnknownRange,
        }
    }
}

// Seconds from the epoch, negative for a time before it.
//
// A file dated before 1970 is a real time a run can compare, so it is reported as it stands.
// Reading it as unknown instead would leave the pair failing the time test on every run, and a
// restored backup would transfer its whole tree every time.
//
// `None` covers the two cases with no time to report: the filesystem gave none, or the value is
// too large to hold in seconds. The second cannot happen where a clock is no wider than `i64`
// seconds, and the conversion is checked anyway, because a cast on a wider platform would wrap
// and hand a comparison a time nobody recorded.
fn secs_since_epoch(modified: std::io::Result<SystemTime>) -> Option<i64> {
    let modified = modified.ok()?;
    match modified.duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_secs()).ok(),
        // A time before the epoch counts down, so a fraction of a second puts it in the second
        // below. `stat` reports 1.9 seconds before the epoch as -2, and a run that called it -1
        // would disagree with every other tool reading the same file.
        Err(before) => {
            let before = before.duration();
            let secs = i64::try_from(before.as_secs()).ok()?;
            if before.subsec_nanos() == 0 {
                secs.checked_neg()
            } else {
                secs.checked_neg()?.checked_sub(1)
            }
        }
    }
}

// Keys are compared relative to their own root, so no prefix is applied here. The
// derivation only fails on a caller-configured delimiter appearing in a filename,
// which cannot happen while comparison fixes the delimiter at `/`.
//
// An S3 object key is Unicode encoded as UTF-8, so a name that is not valid UTF-8
// has no key it could take. Converting it lossily would give two names that differ
// only in their invalid bytes the same key, and whatever consumed the stream would
// then treat two files as one.
//
// Case and Unicode form pass through untouched. Folding `README` onto `readme`, or
// rewriting a name into a different normal form, would make a key match an object
// that is not the same object.
fn local_key(entry: &FsEntry) -> Result<String, StreamError> {
    key_for_relative_path(entry.relative_path())
        .map(Cow::into_owned)
        .ok_or_else(|| StreamError::UnkeyableName(entry.path().to_path_buf()))
}

// `None` when the name cannot be a key at all.
//
// Borrowed where the separator is already `/`, which is every Unix name, because the path filter
// reads the key and drops it for every file in the tree.
fn key_for_relative_path(relative: &std::path::Path) -> Option<Cow<'_, str>> {
    let relative = relative.to_str()?;
    Some(
        derive_object_key(relative, None, None)
            .expect("key derivation cannot fail without a custom delimiter"),
    )
}

// The key a path would take, given the root it sits under.
//
// A walk reports a failure by absolute path, and the rules for turning one into a key live
// here — a name that is not valid UTF-8 has no key, and case and Unicode form pass through
// untouched. The root has to be supplied, because a stream is handed a walker that already
// knows its root and never says what it is.
pub(crate) fn key_under_root(root: &std::path::Path, path: &std::path::Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    // The root itself is not a key under it, the same answer `relative_key` gives for its own
    // root. An empty remainder would otherwise become the empty key, and a caller deriving a
    // bound from that gets one that matches nothing while sorting below every real key — so the
    // range it stands for would be let go at the first name the side produced.
    if relative.as_os_str().is_empty() {
        return None;
    }
    key_for_relative_path(relative).map(Cow::into_owned)
}

// Predicates that apply one rule set to both sides.
//
// Both derive the key the same way the streams do, so a rule cannot decide one thing
// for a local file and another for the object it corresponds to. That symmetry is what
// keeps an excluded key from being read as a missing one: it is absent from both
// sides, so nothing compares it and nothing deletes it.
//
// The destination side is built here too, in one place, because the deferred
// `--delete-excluded` behavior inverts exactly this decision.

// Whether a local walk should yield a file, answered from the key that file would take.
pub(crate) fn local_predicate(
    filter: Arc<KeyFilter>,
) -> impl Fn(&std::path::Path) -> bool + Send + Sync + 'static {
    move |relative| match key_for_relative_path(relative) {
        Some(key) => filter.allows(&key),
        // No key means no rule can match it. Keeping it lets the stream report the
        // name; excluding it here would drop it without a word.
        None => true,
    }
}

// Whether a listing should yield an object, answered from its key taken relative to the root.
//
// Under the root `data/`, a key like `datab/x` starts with the same letters without sitting inside
// the root, so it has no relative key and the rules never see it. Sending the delimiter to
// `ListObjectsV2` keeps such keys out of the listing; this is the guard behind that.
pub(crate) fn s3_predicate(
    filter: Arc<KeyFilter>,
    prefix: Option<String>,
) -> impl Fn(&Object) -> bool + Send + Sync + 'static {
    // Fixed for the whole run, so it is normalised here rather than rebuilt for every object. A
    // prefix written without its delimiter would otherwise allocate once per listed object.
    let root = root_prefix(prefix.as_deref()).into_owned();
    move |obj| {
        // Folder markers are dropped by the walker's own default, which setting a
        // filter would otherwise replace.
        if !exclude_s3_folder_markers(obj) {
            return false;
        }
        // No key means no name to test a rule against, and selecting it leaves the judgement to the
        // code that reports a malformed listing. Substituting an empty key here would have a rule
        // decide the fate of an object nobody can name, and a lost key would go unreported for no
        // reason beyond a filter being installed.
        let Some(key) = obj.key() else {
            return true;
        };
        // A key outside the root is not the filter's business: it should never have been
        // listed, and testing a rule against a key that is not under the root would answer
        // about a name nobody asked for.
        match relative_key(key, &root) {
            Some(relative) => filter.allows(relative),
            None => false,
        }
    }
}

impl KeyStream for FsWalk {
    type Source = FsEntry;

    fn is_done(&self) -> bool {
        FsWalk::is_done(self)
    }

    async fn next_entry(&mut self) -> Option<Result<Entry<FsEntry>, StreamError>> {
        match self.next().await? {
            Ok(entry) => {
                let key = match local_key(&entry) {
                    Ok(key) => key,
                    Err(err) => return Some(Err(err)),
                };
                // An entry the walk could not describe carries neither field. A comparison cannot
                // decide such a key on size or time and has to hold it: the name is taken, which is
                // what keeps a delete off it, but nothing here says whether it matches.
                let meta = EntryMeta {
                    size: entry.metadata().map(|m| m.len()),
                    last_modified_secs: entry
                        .metadata()
                        .and_then(|m| secs_since_epoch(m.modified())),
                    obstruction: match entry.file_type() {
                        FileType::Regular => None,
                        // A special file, or a symlink left unresolved. The walk yields it so the
                        // name counts as taken; nothing can be read from it.
                        _ => Some(Obstruction::NothingToRead),
                    },
                };
                Some(Ok(Entry {
                    key,
                    meta,
                    source: entry,
                }))
            }
            Err(err) => Some(Err(err.into())),
        }
    }
}

impl KeyStream for S3Walk {
    type Source = Object;

    fn is_done(&self) -> bool {
        S3Walk::is_done(self)
    }

    async fn next_entry(&mut self) -> Option<Result<Entry<Object>, StreamError>> {
        loop {
            match self.next().await? {
                Err(err) => return Some(Err(err.into())),
                Ok(obj) => match key_and_meta(&obj, &root_prefix(self.prefix())) {
                    Ok(None) => continue,
                    Ok(Some((key, meta))) => {
                        return Some(Ok(Entry {
                            key,
                            meta,
                            source: obj,
                        }))
                    }
                    // One key that cannot be compared. The listing itself arrived, so the run
                    // carries on with the keys around it.
                    Err(err) => return Some(Err(err)),
                },
            }
        }
    }
}

// The prefix a sync root lists under, which always names a place that holds entries.
//
//     Some("data")   ->  "data/"
//     Some("data/")  ->  "data/"
//     Some("")       ->  ""
//     None           ->  ""
//
// A root holds entries, so `data` means the folder `data/` and never the keys `datab/x` or
// `datafile` that merely start the same way. Sending the delimiter to `ListObjectsV2` keeps
// those out of the listing, which is what lets the keys that do arrive stay in order once the
// prefix comes off them.
//
// A prefix that already names a place is borrowed, so the only case that allocates is the one
// a caller wrote without the delimiter.
pub(crate) fn root_prefix(prefix: Option<&str>) -> Cow<'_, str> {
    match prefix {
        None => Cow::Borrowed(""),
        Some(prefix) if prefix.is_empty() || prefix.ends_with(DEFAULT_DELIMITER) => {
            Cow::Borrowed(prefix)
        }
        Some(prefix) => Cow::Owned(format!("{prefix}{DEFAULT_DELIMITER}")),
    }
}

// The key relative to a sync root, or `None` when the key does not sit under it.
//
// Under the root `data/`:
//
//     "data/a"     ->  Some("a")
//     "data/x/y"   ->  Some("x/y")
//     "data/"      ->  None          the root holds nothing of its own
//     "datab/x"    ->  None          a neighbour, not a child
//     "datafile"   ->  None          a neighbour, not a child
//     "data"       ->  None          an object named like the root is not under it
//
// With no root, every key is already relative:
//
//     "a/b.txt"    ->  Some("a/b.txt")
//
// Every key this accepts shares the same leading run, so taking that run off cannot reorder
// them. The merge reads absence from position, so keeping the order is what keeps a key the
// destination still holds from looking deleted.
//
// `strip_key_prefix` cannot serve here. It strips a prefix that names a span of keys, which is
// right for a download of `s3://bucket/data` and wrong for a sync: with `data` it turns
// `data/z` into `z` and leaves `datab/x` alone, and `z` sorts after `datab/x`.
pub(crate) fn relative_key<'a>(key: &'a str, root_prefix: &str) -> Option<&'a str> {
    let relative = key.strip_prefix(root_prefix)?;
    (!relative.is_empty()).then_some(relative)
}

// `Ok(None)` is the prefix itself, a folder marker, or a key outside the root. `Err` means
// the listing was not what the API documents.
//
// What stops a listed object being read.
//
// Nothing today. An object in Glacier Flexible Retrieval or Deep Archive with no restored copy
// cannot be read either, and a listing carries what says so, but reading that needs a request
// parameter this walk does not set yet. Named so the answer has one home and a test can hold it,
// where an inline `None` leaves nowhere to put either.
fn object_obstruction(obj: &Object) -> Option<Obstruction> {
    let _ = obj;
    None
}

// Markers are dropped here, so they are invisible whether or not a filter is
// configured. The walker's own default filter is replaced by any filter a caller
// sets, which would otherwise make an entry out of a key holding nothing.
// The error names the key where the listing gave one, because that key is absent from this side
// once the object is dropped, and a consumer that reads absence from position needs to know which
// one to hold back.
fn key_and_meta(
    obj: &Object,
    root_prefix: &str,
) -> Result<Option<(String, EntryMeta)>, StreamError> {
    if !exclude_s3_folder_markers(obj) {
        return Ok(None);
    }
    let Some(key) = obj.key() else {
        return Err(StreamError::MalformedListing {
            key: None,
            what: "listing returned an object with no key",
        });
    };
    let Some(relative) = relative_key(key, root_prefix) else {
        return Ok(None);
    };
    let malformed = |what| StreamError::MalformedListing {
        key: Some(relative.to_string()),
        what,
    };
    // A negative length is impossible, so it is a malformed listing rather than a size to cast.
    // Wrapping it would give an enormous length a comparison reads as real.
    let size = obj
        .size()
        .ok_or_else(|| malformed("listing returned no size"))?;
    let size = u64::try_from(size).map_err(|_| malformed("listing returned a negative size"))?;
    let last_modified_secs = obj
        .last_modified()
        .ok_or_else(|| malformed("listing returned no last-modified"))?
        .secs();
    Ok(Some((
        relative.to_string(),
        EntryMeta {
            size: Some(size),
            // Always present: a listing without one is rejected above.
            last_modified_secs: Some(last_modified_secs),
            obstruction: object_obstruction(obj),
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::walk::SortOrder;
    use crate::io::walk::{FsWalkContext, FsWalker, S3WalkContext, S3Walker};
    use aws_sdk_s3::types::{ObjectStorageClass, RestoreStatus};
    use aws_smithy_types::DateTime;
    use std::fs;
    use tempfile::tempdir;

    fn local(root: &std::path::Path) -> FsWalk {
        FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .include_special_files(true)
            .build()
            .walk(FsWalkContext::builder().root(root).build())
    }

    // A sync lists under a root, so the prefix it hands the walker always names a place. Once
    // roots are parsed this normalising happens there; the listing never asks for the sibling
    // keys, and `relative_key` stays the guard that a wrong prefix cannot produce a wrong plan.
    fn s3(client: aws_sdk_s3::Client, prefix: Option<&str>) -> S3Walk {
        let mut builder = S3Walker::builder();
        let root = root_prefix(prefix);
        if !root.is_empty() {
            builder = builder.prefix(root.into_owned());
        }
        builder.build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket("test-bucket")
                .build(),
        )
    }

    async fn keys<S: KeyStream>(s: &mut S) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(next) = s.next_entry().await {
            out.push(next.expect("stream failed").key);
        }
        out
    }

    fn object(key: &str, size: i64) -> Object {
        Object::builder()
            .key(key)
            .size(size)
            .last_modified(DateTime::from_secs(1_700_000_000))
            .build()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn local_keys_are_relative_and_slash_separated() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/c.txt"), "xyz").unwrap();
        fs::write(dir.path().join("top.txt"), "1").unwrap();

        assert_eq!(
            keys(&mut local(dir.path())).await,
            vec!["a/b/c.txt", "top.txt"]
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn local_entries_carry_size_time_and_the_real_path() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("f"), "hello").unwrap();

        let entry = local(dir.path()).next_entry().await.unwrap().unwrap();
        assert_eq!(entry.meta.size, Some(5));
        assert!(entry.meta.last_modified_secs.unwrap() > 1_600_000_000);
        // What a transfer opens, not a path rebuilt from the key.
        assert_eq!(entry.source.path(), dir.path().join("f"));
    }

    // What a stream error is, for asserting on a whole batch at once. Every caller builds its
    // failures from permissions or symlinks, so it follows them behind the gate.
    #[cfg(unix)]
    fn walk_kind(err: &StreamError) -> Option<WalkErrorKind> {
        match err {
            StreamError::Walk(err) => Some(err.kind()),
            _ => None,
        }
    }

    #[test]
    fn case_and_unicode_form_reach_the_key_untouched() {
        // Both sides are compared byte for byte, so folding case or rewriting a name
        // into another normal form here would pair a key with an object that is not
        // the same object. Tested through derivation rather than a real directory,
        // since APFS is insensitive to both and cannot hold the pairs.
        for name in ["README", "readme", "ReadMe"] {
            assert_eq!(derive_object_key(name, None, None).unwrap(), name);
        }

        // "café" precomposed (U+00E9) and decomposed (e + U+0301) are different
        // byte sequences and must stay different keys.
        let nfc = "caf\u{00e9}.txt";
        let nfd = "cafe\u{0301}.txt";
        assert_ne!(nfc.as_bytes(), nfd.as_bytes());
        assert_eq!(derive_object_key(nfc, None, None).unwrap(), nfc);
        assert_eq!(derive_object_key(nfd, None, None).unwrap(), nfd);
    }

    // Key derivation, without a filesystem that has to accept the name. macOS refuses to create
    // one, so the walk-level test below is Linux-only, and this keeps the refusal itself covered
    // everywhere.
    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_utf8_has_no_key() {
        use std::os::unix::ffi::OsStrExt;

        // 0xFF cannot begin a valid UTF-8 sequence.
        let bad = std::path::PathBuf::from(std::ffi::OsStr::from_bytes(b"bad-\xff.txt"));
        assert_eq!(key_for_relative_path(&bad), None);
        assert_eq!(
            key_for_relative_path(std::path::Path::new("fine.txt")).as_deref(),
            Some("fine.txt")
        );
    }

    // Only Linux is guaranteed to accept a filename that is not valid UTF-8; macOS
    // rejects one outright.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_name_that_is_not_utf8_is_reported_and_yields_no_entry() {
        use std::os::unix::ffi::OsStrExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("good.txt"), "ok").unwrap();
        // 0xFF cannot begin a valid UTF-8 sequence.
        let bad = std::ffi::OsStr::from_bytes(b"bad-\xff.txt");
        fs::write(dir.path().join(bad), "x").unwrap();

        let mut stream = local(dir.path());
        let (mut keys, mut errors) = (Vec::new(), Vec::new());
        while let Some(item) = stream.next_entry().await {
            match item {
                Ok(entry) => keys.push(entry.key),
                Err(err) => errors.push(err),
            }
        }

        assert_eq!(keys, ["good.txt"]);
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(errors[0], StreamError::UnkeyableName(_)),
            "got {:?}",
            errors[0]
        );
        // The raw bytes are named, so two names differing only in their invalid
        // bytes stay distinguishable.
        assert!(
            format!("{}", errors[0]).contains("\\xFF")
                || format!("{}", errors[0]).contains("\\xff"),
            "message should name the invalid byte: {}",
            errors[0]
        );
    }

    // Keys chosen so a breadth-first walk would order them differently from S3: '/'
    // is 0x2F, below every alphanumeric, so nested keys interleave with siblings.
    const JOIN_KEYS: &[&str] = &[
        "a.txt",
        "a/c",
        "az.txt",
        "test-123.txt",
        "test.txt",
        "test/inner.txt",
        "test0.txt",
    ];

    #[derive(Debug, PartialEq)]
    enum Action {
        Transfer,
        Delete,
        Skip,
    }

    // Takes whole entries: a comparison that checks a checksum or an ETag needs
    // fields only the walker's own item carries.
    //
    // Uploading, so local is the source: a same-size entry is left alone unless the
    // destination is the older of the two.
    //
    // Size and time are all this reads, so it will answer `Transfer` for a name no transfer can
    // move — a socket holds a key and cannot be uploaded. A real comparison has to reach the
    // source's own item for that, which means a third answer beside transfer and skip: the key is
    // taken, so no delete may touch it, and nothing can be sent for it either.
    fn decide<S, D>(src: &Entry<S>, dest: &Entry<D>) -> Action {
        if src.meta.size != dest.meta.size {
            return Action::Transfer;
        }
        // Absence is answered here rather than by comparing the options, because `None` sorts below
        // every `Some`: a source with no time would read as older than the destination and be
        // skipped, which is the one outcome a side that cannot describe itself must not produce.
        match (src.meta.last_modified_secs, dest.meta.last_modified_secs) {
            (Some(src_secs), Some(dest_secs)) if dest_secs >= src_secs => Action::Skip,
            _ => Action::Transfer,
        }
    }

    // One buffered entry per side. Each decision is emitted as soon as both sides
    // have passed the key it concerns, so neither side is ever held whole.
    async fn merge_join<S, D>(src: &mut S, dest: &mut D) -> Vec<(String, Action)>
    where
        S: KeyStream,
        D: KeyStream,
    {
        let mut plan = Vec::new();
        let mut s = src
            .next_entry()
            .await
            .transpose()
            .expect("local walk failed");
        let mut d = dest.next_entry().await.transpose().expect("listing failed");
        loop {
            match (&s, &d) {
                (None, None) => break,
                (Some(se), None) => {
                    plan.push((se.key.clone(), Action::Transfer));
                    s = src
                        .next_entry()
                        .await
                        .transpose()
                        .expect("local walk failed");
                }
                (None, Some(de)) => {
                    plan.push((de.key.clone(), Action::Delete));
                    d = dest.next_entry().await.transpose().expect("listing failed");
                }
                (Some(se), Some(de)) => match se.key.cmp(&de.key) {
                    std::cmp::Ordering::Less => {
                        plan.push((se.key.clone(), Action::Transfer));
                        s = src
                            .next_entry()
                            .await
                            .transpose()
                            .expect("local walk failed");
                    }
                    std::cmp::Ordering::Greater => {
                        plan.push((de.key.clone(), Action::Delete));
                        d = dest.next_entry().await.transpose().expect("listing failed");
                    }
                    std::cmp::Ordering::Equal => {
                        plan.push((se.key.clone(), decide(se, de)));
                        s = src
                            .next_entry()
                            .await
                            .transpose()
                            .expect("local walk failed");
                        d = dest.next_entry().await.transpose().expect("listing failed");
                    }
                },
            }
        }
        plan
    }

    // Sized to match the local files, timestamped newer than them.
    fn join_listing() -> aws_sdk_s3::Client {
        let mut keys: Vec<&str> = JOIN_KEYS.to_vec();
        keys.sort();
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(format!("data/{k}"))
                    .size(k.len() as i64)
                    .last_modified(aws_smithy_types::DateTime::from_secs(now + 60))
                    .build()
            })
            .collect();
        let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(move || output.clone());
        aws_smithy_mocks::mock_client!(aws_sdk_s3, aws_smithy_mocks::RuleMode::MatchAny, &[rule])
    }

    // A page holding one object missing a field a comparison needs. That one key cannot be
    // compared, and the keys around it are fine, so the stream must report it and carry on. This
    // guards a regression: while these errors borrowed `WalkErrorKind::Service`, which is fatal,
    // one such object ended the whole run.
    #[tokio::test]
    async fn an_object_missing_a_field_costs_only_its_own_key() {
        let contents = vec![
            object("a.txt", 1),
            // No size, so nothing to compare against a local file's length.
            Object::builder()
                .key("b.txt")
                .last_modified(DateTime::from_secs(1_700_000_000))
                .build(),
            object("c.txt", 3),
        ];
        let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(move || output.clone());
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[rule]
        );

        let mut stream = s3(client, None);
        let mut keys = Vec::new();
        let mut errors = Vec::new();
        while let Some(item) = stream.next_entry().await {
            match item {
                Ok(entry) => keys.push(entry.key),
                Err(err) => errors.push(err),
            }
        }

        // The keys on both sides of the bad object arrive, which is what "carry on" means.
        assert_eq!(keys, vec!["a.txt".to_string(), "c.txt".to_string()]);
        assert_eq!(errors.len(), 1, "got {errors:?}");
        // The key is named, because that key is absent from this side now and a consumer that
        // reads absence from position has to hold back the action for it alone.
        assert!(
            matches!(
                &errors[0],
                StreamError::MalformedListing { key: Some(key), .. } if key == "b.txt"
            ),
            "got {:?}",
            errors[0]
        );
        assert!(!errors[0].is_fatal(), "one bad object must not end the run");
        assert_eq!(
            errors[0].keys_lost(),
            KeysLost::OneKey,
            "the object was listed and dropped, so its key is unaccounted for"
        );
    }

    // The key named is the one the stream would have yielded, relative to the root, so a consumer
    // holding back an action can match it against the keys it has already seen. An absolute key
    // here would name something the consumer never saw.
    #[test]
    fn a_malformed_object_names_its_key_relative_to_the_root() {
        let no_size = Object::builder()
            .key("data/a/b.txt")
            .last_modified(DateTime::from_secs(1))
            .build();
        match key_and_meta(&no_size, "data/") {
            Err(StreamError::MalformedListing {
                key: Some(key),
                what,
            }) => {
                assert_eq!(key, "a/b.txt");
                assert!(what.contains("size"), "got {what:?}");
            }
            other => panic!("expected a named malformed listing, got {other:?}"),
        }

        // Whether a filter is installed decides what gets selected, never what gets reported. An
        // object with no key cannot be matched against a rule at all, so the predicate has to pass
        // it on to the one place that can say a key was lost.
        let no_key = Object::builder()
            .size(1)
            .last_modified(DateTime::from_secs(1))
            .build();
        let filtered = s3_predicate(
            Arc::new(KeyFilter::new(vec![
                Rule::exclude("*"),
                Rule::include("data/*"),
            ])),
            Some("data/".to_string()),
        );
        assert!(
            filtered(&no_key),
            "a keyless object must reach the code that reports it, not be dropped by a rule it \
             cannot be tested against"
        );

        // An object the listing gave no key for has none to name, and nothing on the other side
        // could correspond to it.
        let no_key = Object::builder()
            .size(1)
            .last_modified(DateTime::from_secs(1))
            .build();
        assert!(matches!(
            key_and_meta(&no_key, "data/"),
            Err(StreamError::MalformedListing { key: None, .. })
        ));
    }

    // What each failure costs, since a comparison reads absence from position. Every kind is here,
    // so a kind added later has to be placed deliberately rather than defaulting to the answer that
    // permits a delete.
    #[test]
    fn a_failure_says_whether_it_cost_one_key_or_a_range() {
        let walk_err = |kind| StreamError::Walk(WalkError::new(None, kind, Box::from("test")));
        let cases = [
            // The walk never got going, or gave up part way.
            (
                walk_err(WalkErrorKind::SourceUnreadable),
                KeysLost::UnknownRange,
            ),
            (
                walk_err(WalkErrorKind::NotADirectory),
                KeysLost::UnknownRange,
            ),
            (walk_err(WalkErrorKind::Service), KeysLost::UnknownRange),
            // A subtree went unenumerated, and no single key stands for a subtree.
            (
                walk_err(WalkErrorKind::DirectoryUnreadable),
                KeysLost::UnknownRange,
            ),
            (
                walk_err(WalkErrorKind::SymlinkCycle),
                KeysLost::UnknownRange,
            ),
            // One entry that should have been readable.
            (walk_err(WalkErrorKind::Io), KeysLost::OneKey),
            (walk_err(WalkErrorKind::PermissionDenied), KeysLost::OneKey),
            (walk_err(WalkErrorKind::BrokenSymlink), KeysLost::OneKey),
            // Listed, then dropped: the key is gone from this side.
            (
                StreamError::MalformedListing {
                    key: Some("a.txt".to_string()),
                    what: "no size",
                },
                KeysLost::OneKey,
            ),
            // Nothing to name, so a consumer cannot hold back the action for it.
            (
                StreamError::MalformedListing {
                    key: None,
                    what: "no key",
                },
                KeysLost::UnknownRange,
            ),
            // No key can carry a name that is not valid UTF-8, and an object may already sit at the
            // key the upload path would have derived for it. One entry, like a device or a socket.
            (
                StreamError::UnkeyableName(PathBuf::from("/tmp/x")),
                KeysLost::OneKey,
            ),
        ];
        for (err, cost) in cases {
            assert_eq!(err.keys_lost(), cost, "err={err:?}");
        }
    }

    // A link the walk follows to a directory it cannot stat: the target may hold a whole subtree,
    // filed under the link's name, and none of it was seen.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_link_to_an_unstattable_directory_costs_a_range() {
        use std::os::unix::fs::PermissionsExt;

        let outside = tempdir().unwrap();
        let protected = outside.path().join("protected");
        fs::create_dir(&protected).unwrap();
        fs::create_dir(protected.join("data")).unwrap();
        fs::set_permissions(&protected, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::metadata(protected.join("data")).is_ok() {
            fs::set_permissions(&protected, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(protected.join("data"), dir.path().join("link")).unwrap();

        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut costs = Vec::new();
        while let Some(next) = walk.next_entry().await {
            if let Err(err) = next {
                costs.push((walk_kind(&err), err.keys_lost()));
            }
        }
        fs::set_permissions(&protected, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            costs.contains(&(
                Some(WalkErrorKind::DirectoryUnreadable),
                KeysLost::UnknownRange
            )),
            "a link the walk could not stat must read as a lost range, got {costs:?}"
        );
    }

    // A filter excluding a name says nothing about the keys under it: `exclude("link")` matches
    // `link` and not `link/a.txt`. So when the walk cannot tell what `link` is, suppressing the
    // failure because the name was excluded loses every key beneath it with nothing said.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_name_of_unknown_type_still_reports_its_range() {
        use std::os::unix::fs::PermissionsExt;

        let outside = tempdir().unwrap();
        let protected = outside.path().join("protected");
        fs::create_dir(&protected).unwrap();
        fs::create_dir(protected.join("data")).unwrap();
        fs::set_permissions(&protected, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::metadata(protected.join("data")).is_ok() {
            fs::set_permissions(&protected, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(protected.join("data"), dir.path().join("link")).unwrap();

        // Excludes the name `link`, which leaves `link/...` included.
        let filter = Arc::new(KeyFilter::new(vec![Rule::exclude("link")]));
        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .path_filter(local_predicate(filter))
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut costs = Vec::new();
        while let Some(next) = walk.next_entry().await {
            if let Err(err) = next {
                costs.push((walk_kind(&err), err.keys_lost()));
            }
        }
        fs::set_permissions(&protected, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            costs.contains(&(
                Some(WalkErrorKind::DirectoryUnreadable),
                KeysLost::UnknownRange
            )),
            "the keys under an excluded name are not themselves excluded, got {costs:?}"
        );
    }

    // The reason this layer exists. A source that could not read one subdirectory must not let the
    // destination's keys under that name be deleted: they may still exist on the source, inside the
    // part nobody could see.
    // One key the source could not describe must not license deleting its counterpart. The name is
    // taken on both sides; the source simply cannot say what is behind it. A range is not the only
    // kind of loss that has to hold a delete back.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn one_lost_key_on_the_source_holds_back_its_delete() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "x").unwrap();
        // Followed, and pointing at nothing, so the walk can name it but not describe it.
        std::os::unix::fs::symlink(dir.path().join("missing"), dir.path().join("b")).unwrap();

        let contents = vec![object("a.txt", 1), object("b", 1), object("z.txt", 1)];
        let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(move || output.clone());
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[rule]
        );

        let mut src = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .include_special_files(true)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());
        let mut dest = s3(client, None);
        let (plan, lost) = merge_respecting_loss(&mut src, &mut dest).await;

        assert!(
            lost,
            "a key the source could not describe is a loss: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|(key, action)| key == "b" && *action == Action::Delete),
            "deleting `b` removes the counterpart of a key the source could not describe: {plan:?}"
        );
        // And the cost of holding the whole run back: `z.txt` really is gone from the source and
        // could be deleted safely. Asserted so the conservatism reads as chosen, and so relaxing it
        // to a per-key rule has to change this line on purpose.
        assert!(
            !plan
                .iter()
                .any(|(key, action)| key == "z.txt" && *action == Action::Delete),
            "a run-wide hold keeps every delete, including ones that were safe: {plan:?}"
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_lost_range_on_the_source_holds_back_every_delete() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "x").unwrap();
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("hidden.txt"), "x").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::read_dir(&locked).is_ok() {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        // The destination holds a key under the directory the source could not read, and one that is
        // genuinely gone from the source.
        let contents = vec![
            object("a.txt", 1),
            object("locked/hidden.txt", 1),
            object("z.txt", 1),
        ];
        let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(contents))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(move || output.clone());
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[rule]
        );

        let mut src = local(dir.path());
        let mut dest = s3(client, None);
        let (plan, lost) = merge_respecting_loss(&mut src, &mut dest).await;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            lost,
            "an unreadable subdirectory has to read as a lost range"
        );
        let deletes: Vec<_> = plan
            .iter()
            .filter(|(_, a)| *a == Action::Delete)
            .map(|(k, _)| k.as_str())
            .collect();
        assert!(
            deletes.is_empty(),
            "the source could not see everything, so nothing may be deleted: {deletes:?}"
        );
    }

    // A merge that reads what a failure cost instead of unwrapping it. The source losing a range is
    // the case that matters: a key the destination holds may exist on the source inside the part
    // nobody could read, so its absence cannot be trusted and a delete has to be held back.
    //
    // This is the shape the comparison will take. It lives here to show the stream hands over enough
    // A stream of scripted results, for a side the filesystem cannot be made to produce: a name
    // that is not valid UTF-8 cannot be created on every platform these tests run on.
    struct Scripted(std::collections::VecDeque<Result<Entry<()>, StreamError>>);

    impl Scripted {
        fn new(items: Vec<Result<Entry<()>, StreamError>>) -> Self {
            Self(items.into())
        }
    }

    impl KeyStream for Scripted {
        type Source = ();

        async fn next_entry(&mut self) -> Option<Result<Entry<()>, StreamError>> {
            self.0.pop_front()
        }

        fn is_done(&self) -> bool {
            self.0.is_empty()
        }
    }

    fn keyed(key: &str) -> Result<Entry<()>, StreamError> {
        Ok(Entry {
            key: key.to_string(),
            meta: EntryMeta {
                size: Some(1),
                last_modified_secs: Some(1_700_000_000),
                obstruction: None,
            },
            source: (),
        })
    }

    // A name no key can carry still has an object waiting for it, because the upload path derives
    // keys with `to_string_lossy` — so this crate writes an object at the lossy key and a later walk
    // cannot name the file that produced it. Reading that as costing nothing licenses deleting the
    // object while the file is still there.
    #[tokio::test]
    async fn a_name_no_key_can_carry_does_not_license_a_delete() {
        let mut src = Scripted::new(vec![
            keyed("a.txt"),
            Err(StreamError::UnkeyableName(PathBuf::from(
                "/data/caf\u{FFFD}.txt",
            ))),
        ]);
        let mut dest = Scripted::new(vec![keyed("a.txt"), keyed("caf\u{FFFD}.txt")]);

        let (plan, lost) = merge_respecting_loss(&mut src, &mut dest).await;
        assert!(lost, "a name the walk could not key is a gap: {plan:?}");
        assert!(
            !plan.iter().any(|(_, action)| *action == Action::Delete),
            "the object at the lossy key must not be deleted: {plan:?}"
        );
    }

    // to make that decision.
    async fn merge_respecting_loss<S, D>(src: &mut S, dest: &mut D) -> (Vec<(String, Action)>, bool)
    where
        S: KeyStream,
        D: KeyStream,
    {
        let mut plan = Vec::new();
        let mut source_lost_keys = false;

        // Read one side past any failures, recording whether the view stayed trustworthy.
        async fn advance<K: KeyStream>(k: &mut K, lost: &mut bool) -> Option<Entry<K::Source>> {
            loop {
                match k.next_entry().await {
                    None => return None,
                    Some(Ok(entry)) => return Some(entry),
                    Some(Err(err)) => match err.keys_lost() {
                        // Both answers collapse to the same action here. A range is unnameable by
                        // definition, and one key is unnameable in practice on the local side: the
                        // failure carries an absolute path, nothing here turns that into a key, and
                        // it arrives at the position of the directory holding it rather than its
                        // own. So neither can suppress the delete of just the key that went
                        // missing, and holding every delete back is the safe reading left. The cost
                        // is that a key genuinely absent from the source keeps its counterpart too,
                        // which a per-key mechanism recovers once a failure can name a relative
                        // key.
                        KeysLost::OneKey | KeysLost::UnknownRange => *lost = true,
                    },
                }
            }
        }

        let mut ignored = false;
        let mut s = advance(src, &mut source_lost_keys).await;
        let mut d = advance(dest, &mut ignored).await;
        loop {
            match (&s, &d) {
                (None, None) => break,
                (Some(se), None) => {
                    plan.push((se.key.clone(), Action::Transfer));
                    s = advance(src, &mut source_lost_keys).await;
                }
                (None, Some(de)) => {
                    if !source_lost_keys {
                        plan.push((de.key.clone(), Action::Delete));
                    }
                    d = advance(dest, &mut ignored).await;
                }
                (Some(se), Some(de)) => match se.key.cmp(&de.key) {
                    std::cmp::Ordering::Less => {
                        plan.push((se.key.clone(), Action::Transfer));
                        s = advance(src, &mut source_lost_keys).await;
                    }
                    std::cmp::Ordering::Greater => {
                        if !source_lost_keys {
                            plan.push((de.key.clone(), Action::Delete));
                        }
                        d = advance(dest, &mut ignored).await;
                    }
                    std::cmp::Ordering::Equal => {
                        plan.push((se.key.clone(), decide(se, de)));
                        s = advance(src, &mut source_lost_keys).await;
                        d = advance(dest, &mut ignored).await;
                    }
                },
            }
        }
        (plan, source_lost_keys)
    }

    // The join reads "this key is absent from the other side" from position alone, so
    // it only holds if both sides agree on the key space and on the total order.
    // Pointing it at a directory and a prefix holding the same content must therefore
    // plan nothing.
    #[tokio::test]
    async fn identical_sides_plan_no_transfers_or_deletes() {
        let dir = tempdir().unwrap();
        for key in JOIN_KEYS {
            let path = dir.path().join(key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, *key).unwrap();
        }

        let mut src = local(dir.path());
        let mut dest = s3(join_listing(), Some("data/"));
        let plan = merge_join(&mut src, &mut dest).await;

        let unexpected: Vec<_> = plan.iter().filter(|(_, a)| *a != Action::Skip).collect();
        assert!(
            unexpected.is_empty(),
            "identical sides must plan nothing, got {unexpected:?}"
        );
        assert_eq!(
            plan.len(),
            JOIN_KEYS.len(),
            "every key must be accounted for"
        );
    }

    // An entry has to be available before the listing is finished, or a transfer could
    // not start until the whole side had been enumerated. The second page is served
    // only on request, so reading one entry while the walk is still unfinished shows
    // the stream does not drain the listing first.
    #[tokio::test]
    async fn an_entry_arrives_before_the_listing_is_finished() {
        fn page(
            keys: &[&str],
            next: Option<&str>,
        ) -> aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output {
            let contents = keys
                .iter()
                .map(|k| {
                    Object::builder()
                        .key(format!("data/{k}"))
                        .size(1)
                        .last_modified(DateTime::from_secs(1))
                        .build()
                })
                .collect();
            let mut b = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
                .set_contents(Some(contents));
            if let Some(token) = next {
                b = b.next_continuation_token(token).is_truncated(true);
            }
            b.build()
        }

        let first = page(&["a.txt", "b.txt"], Some("page2"));
        let second = page(&["c.txt"], None);
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .sequence()
            .output(move || first.clone())
            .output(move || second.clone())
            .build();
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[&rule]
        );

        let mut stream = s3(client, Some("data/"));
        let first_entry = stream
            .next_entry()
            .await
            .expect("an entry")
            .expect("no error");
        assert_eq!(first_entry.key, "a.txt");
        assert_eq!(
            rule.num_calls(),
            1,
            "the second page should not be fetched yet"
        );

        assert_eq!(keys(&mut stream).await, vec!["b.txt", "c.txt"]);
        assert_eq!(rule.num_calls(), 2);
    }

    #[test]
    fn object_keys_lose_the_root_prefix() {
        let (key, meta) = key_and_meta(&object("data/a/b.txt", 3), "data/")
            .unwrap()
            .unwrap();
        assert_eq!(key, "a/b.txt");
        assert_eq!(meta.size, Some(3));
        assert_eq!(meta.last_modified_secs, Some(1_700_000_000));
    }

    // A prefix with and without its trailing delimiter must key alike, or the two
    // sides would disagree about every key.
    // A root names a place, so a prefix without the delimiter still lists one: `data` reaches
    // `data/` and nothing else.
    #[test]
    fn a_root_prefix_always_names_a_place() {
        assert_eq!(root_prefix(Some("data")), "data/");
        assert_eq!(root_prefix(Some("data/")), "data/");
        assert_eq!(root_prefix(None), "");
        assert_eq!(root_prefix(Some("")), "");

        // A prefix that already names a place costs no allocation.
        assert!(matches!(root_prefix(Some("data/")), Cow::Borrowed(_)));
        assert!(matches!(root_prefix(None), Cow::Borrowed(_)));
        assert!(matches!(root_prefix(Some("data")), Cow::Owned(_)));
    }

    // The keys a sync lists under a root keep their order once the prefix comes off, which is
    // what the merge reads absence from. Stripping a prefix that names a span of names instead
    // would reorder them: `data/z` becomes `z` while `datab/x` is left alone, and `z` sorts
    // after `datab/x`.
    #[test]
    fn taking_the_root_prefix_off_keeps_the_keys_in_order() {
        let prefix = root_prefix(Some("data"));
        let listed = ["data/a", "data/z", "datab/x", "datafile"];

        let under_the_root: Vec<_> = listed
            .iter()
            .filter_map(|key| relative_key(key, &prefix))
            .collect();

        assert_eq!(
            under_the_root,
            ["a", "z"],
            "only keys under the root arrive"
        );
        assert!(
            under_the_root.windows(2).all(|w| w[0] < w[1]),
            "keys under a root stay sorted after the prefix comes off"
        );
    }

    #[test]
    fn a_key_outside_the_root_has_no_relative_key() {
        let prefix = root_prefix(Some("data"));
        assert_eq!(relative_key("datab/x", &prefix), None);
        assert_eq!(relative_key("datafile", &prefix), None);
        // The root itself holds nothing of its own.
        assert_eq!(relative_key("data/", &prefix), None);
    }

    #[test]
    fn an_unprefixed_listing_keeps_whole_keys() {
        let (key, _) = key_and_meta(&object("a/b.txt", 1), "").unwrap().unwrap();
        assert_eq!(key, "a/b.txt");
    }

    #[test]
    fn the_prefix_itself_is_not_an_entry() {
        assert!(key_and_meta(&object("data/", 0), "data/")
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_object_missing_comparison_metadata_is_rejected() {
        let no_size = Object::builder()
            .key("data/a")
            .last_modified(DateTime::from_secs(1))
            .build();
        assert!(key_and_meta(&no_size, "data/").is_err());

        let no_time = Object::builder().key("data/a").size(1).build();
        assert!(key_and_meta(&no_time, "data/").is_err());
    }

    #[test]
    fn every_cause_known_today_also_stops_an_overwrite() {
        // An archive will not: overwriting never reads what is already there, so an upload over an
        // archived object has to go ahead. Spelled as a match, so adding a cause stops this
        // compiling until someone says which way it goes.
        let cause = Obstruction::NothingToRead;
        match cause {
            Obstruction::NothingToRead => assert!(cause.blocks_overwrite()),
        }
    }

    #[test]
    fn an_ordinary_listed_object_can_be_read() {
        // The listing side's answer has one home, so a cause added there without thinking about
        // the ordinary case shows up here.
        let obj = Object::builder().key("data/a").size(1).build();
        assert_eq!(object_obstruction(&obj), None);
    }

    // Readability is decided from the listing, without a HeadObject per key.
    #[test]
    fn archival_metadata_survives_on_the_source() {
        let obj = Object::builder()
            .key("data/cold")
            .size(1)
            .last_modified(DateTime::from_secs(1))
            .storage_class(ObjectStorageClass::Glacier)
            .restore_status(
                RestoreStatus::builder()
                    .is_restore_in_progress(true)
                    .build(),
            )
            .build();
        let entry = Entry {
            key: "cold".to_string(),
            meta: EntryMeta {
                size: Some(1),
                last_modified_secs: Some(1),
                obstruction: None,
            },
            source: obj,
        };

        assert_eq!(
            entry.source.storage_class(),
            Some(&ObjectStorageClass::Glacier)
        );
        assert_eq!(
            entry
                .source
                .restore_status()
                .unwrap()
                .is_restore_in_progress(),
            Some(true)
        );
    }

    // --- filters ---

    use crate::io::key::filter::Rule;

    fn filtered_local(root: &std::path::Path, rules: Vec<Rule>) -> FsWalk {
        let filter = Arc::new(KeyFilter::new(rules));
        FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            // On, so the socket test proves the filter suppresses a report that would
            // otherwise fire.
            .include_special_files(true)
            .path_filter(local_predicate(filter))
            .build()
            .walk(FsWalkContext::builder().root(root).build())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_file_is_not_yielded() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("logs")).unwrap();
        fs::write(dir.path().join("logs/a.txt"), "").unwrap();
        fs::write(dir.path().join("keep.txt"), "").unwrap();

        let mut walk = filtered_local(dir.path(), vec![Rule::exclude("logs/*")]);
        assert_eq!(keys(&mut walk).await, vec!["keep.txt"]);
    }

    // A symlink the walk follows to a regular file is yielded on a different path from a file
    // reached directly, and an excluded key has to stay excluded on both.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_symlink_to_a_file_is_not_yielded() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("logs")).unwrap();
        fs::write(dir.path().join("target.txt"), "x").unwrap();
        fs::write(dir.path().join("keep.txt"), "").unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("target.txt"),
            dir.path().join("logs/link.txt"),
        )
        .unwrap();

        let filter = Arc::new(KeyFilter::new(vec![Rule::exclude("logs/*")]));
        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .path_filter(local_predicate(filter))
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        assert_eq!(keys(&mut walk).await, vec!["keep.txt", "target.txt"]);
    }

    // An excluded entry must not warn even when it cannot be read, which means the
    // rules have to be consulted before the metadata is.
    // A directory that can be listed but whose children cannot be stat'd: readable,
    // not searchable. Returns `None` when the mode has no effect, as for root.
    #[cfg(unix)]
    fn unstattable_dir(root: &std::path::Path) -> Option<std::path::PathBuf> {
        use std::os::unix::fs::PermissionsExt;

        let locked = root.join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("secret.txt"), "").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o400)).unwrap();

        let listable = fs::read_dir(&locked).is_ok();
        let stattable = fs::metadata(locked.join("secret.txt")).is_ok();
        if listable && !stattable {
            Some(locked)
        } else {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            None
        }
    }

    #[cfg(unix)]
    fn unlock(dir: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_unreadable_file_warns_about_nothing() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("keep.txt"), "").unwrap();
        let Some(locked) = unstattable_dir(dir.path()) else {
            return; // running as root, or a filesystem that ignores the mode
        };

        let mut walk = filtered_local(dir.path(), vec![Rule::exclude("locked/*")]);
        let mut seen = Vec::new();
        let mut errors = Vec::new();
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => errors.push(err),
            }
        }
        unlock(&locked);

        assert_eq!(seen, vec!["keep.txt"]);
        assert!(
            errors.is_empty(),
            "an excluded entry must not warn: {:?}",
            errors.iter().map(walk_kind).collect::<Vec<_>>()
        );
    }

    // The control for the test above: without the rule, the same file does warn, so
    // the silence there comes from the filter and not from swallowing errors.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_unreadable_file_that_is_not_excluded_still_warns() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("keep.txt"), "").unwrap();
        let Some(locked) = unstattable_dir(dir.path()) else {
            return; // running as root, or a filesystem that ignores the mode
        };

        let mut walk = filtered_local(dir.path(), vec![Rule::exclude("nothing/*")]);
        let mut errors = Vec::new();
        while let Some(next) = walk.next_entry().await {
            if let Err(err) = next {
                errors.push(err);
            }
        }
        unlock(&locked);

        assert!(
            errors
                .iter()
                .any(|e| walk_kind(e) == Some(WalkErrorKind::PermissionDenied)),
            "expected a per-entry permission warning, got {:?}",
            errors.iter().map(walk_kind).collect::<Vec<_>>()
        );
    }

    // A named pipe is skipped without a warning whether or not a rule excludes it. Unix-only
    // because a named pipe is, and `nix` is only a dependency there.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_special_file_warns_about_nothing() {
        let dir = tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRWXU).unwrap();
        fs::write(dir.path().join("keep.txt"), "").unwrap();

        let mut walk = filtered_local(dir.path(), vec![Rule::exclude("pipe")]);
        let mut seen = Vec::new();
        let mut errors = Vec::new();
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => errors.push(err),
            }
        }
        assert_eq!(seen, vec!["keep.txt"]);
        assert!(errors.is_empty());
    }

    // One rule set, two sides, same answers — the property that keeps an excluded key
    // from looking like a deleted one.
    #[test]
    fn both_sides_decide_alike_for_the_same_key() {
        let rules = vec![
            Rule::exclude("*"),
            Rule::include("logs/*"),
            Rule::exclude("logs/secret/*"),
        ];
        let filter = Arc::new(KeyFilter::new(rules));
        let local = local_predicate(filter.clone());
        let remote = s3_predicate(filter, Some("data/".to_string()));

        for key in [
            "a.txt",
            "logs/a.txt",
            "logs/secret/a.txt",
            "logs/inner/b.log",
            "other/c",
        ] {
            let local_says = local(std::path::Path::new(key));
            let remote_says = remote(&object(&format!("data/{key}"), 1));
            assert_eq!(local_says, remote_says, "disagreed about {key:?}");
        }
    }

    // Setting a filter replaces the walker's default, so the marker exclusion has to
    // be carried explicitly or folder markers reappear as entries.
    // A marker has to be invisible whether or not rules are configured: it is a
    // key with no content, and treating it as an entry makes the destination look
    // like it holds something the source does not.
    #[tokio::test]
    async fn folder_markers_are_not_entries_without_any_filter() {
        let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
            .set_contents(Some(vec![
                Object::builder()
                    .key("data/dir/")
                    .size(0)
                    .last_modified(DateTime::from_secs(1))
                    .build(),
                Object::builder()
                    .key("data/dir/f.txt")
                    .size(3)
                    .last_modified(DateTime::from_secs(1))
                    .build(),
            ]))
            .build();
        let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(move || output.clone());
        let client = aws_smithy_mocks::mock_client!(
            aws_sdk_s3,
            aws_smithy_mocks::RuleMode::MatchAny,
            &[rule]
        );

        let mut stream = s3(client, Some("data/"));
        assert_eq!(keys(&mut stream).await, vec!["dir/f.txt"]);
    }

    #[test]
    fn folder_markers_stay_excluded_when_a_filter_is_set() {
        let remote = s3_predicate(Arc::new(KeyFilter::default()), None);
        assert!(!remote(&object("data/", 0)));
        assert!(remote(&object("data/a.txt", 1)));
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_unreadable_subdirectory_leaves_a_range_unaccounted_for() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("hidden.txt"), "").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::read_dir(&locked).is_ok() {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root
        }

        let mut walk = local(dir.path());
        let mut seen = Vec::new();
        let mut incomplete = false;
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => incomplete |= err.keys_lost() == KeysLost::UnknownRange,
            }
        }
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(seen.contains(&"a.txt".to_string()));
        assert!(incomplete, "an unread subtree must be an incomplete view");
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_broken_symlink_costs_one_entry_not_the_view() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("broken")).unwrap();

        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut seen = Vec::new();
        let mut errors = Vec::new();
        while let Some(next) = walk.next_entry().await {
            match next {
                Ok(entry) => seen.push(entry.key),
                Err(err) => errors.push(err),
            }
        }

        assert_eq!(seen, vec!["a.txt"]);
        assert_eq!(errors.len(), 1);
        assert_eq!(walk_kind(&errors[0]), Some(WalkErrorKind::BrokenSymlink));
        assert_eq!(errors[0].keys_lost(), KeysLost::OneKey);
    }

    // A cycle stops a descent, so everything under it goes unenumerated. The side has to say so, or
    // a delete removes destination keys nobody ever looked for.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_symlink_cycle_leaves_a_range_unaccounted_for() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/deep.txt"), "").unwrap();
        fs::write(dir.path().join("top.txt"), "").unwrap();
        // b/loop points back at a, so descending it would revisit a directory already on the path.
        std::os::unix::fs::symlink(dir.path().join("a"), dir.path().join("a/b/loop")).unwrap();

        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut costs = Vec::new();
        while let Some(next) = walk.next_entry().await {
            if let Err(err) = next {
                costs.push((walk_kind(&err), err.keys_lost()));
            }
        }

        assert_eq!(
            costs,
            vec![(Some(WalkErrorKind::SymlinkCycle), KeysLost::UnknownRange)],
            "a cycle costs the subtree it stopped at"
        );
    }

    // A directory reached through a symlink that cannot be opened is not descended into either, so
    // it has to report what every other unreadable directory reports.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_symlinked_directory_that_cannot_be_opened_costs_a_range() {
        use std::os::unix::fs::PermissionsExt;

        // The target sits outside the walk root, so the only way in is through the link. Inside the
        // root it would also be read directly, and the direct read reports the right kind already.
        let outside = tempdir().unwrap();
        let hidden = outside.path().join("hidden");
        fs::create_dir(&hidden).unwrap();
        fs::write(hidden.join("under.txt"), "").unwrap();

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(&hidden, dir.path().join("link")).unwrap();
        fs::set_permissions(&hidden, fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::File::open(&hidden).is_ok() {
            fs::set_permissions(&hidden, fs::Permissions::from_mode(0o755)).unwrap();
            return; // running as root, or a filesystem that ignores the mode
        }

        let mut walk = FsWalker::builder()
            .recursive(true)
            .sort(SortOrder::WholeWalk)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let mut costs = Vec::new();
        while let Some(next) = walk.next_entry().await {
            if let Err(err) = next {
                costs.push((walk_kind(&err), err.keys_lost()));
            }
        }
        fs::set_permissions(&hidden, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            costs.contains(&(
                Some(WalkErrorKind::DirectoryUnreadable),
                KeysLost::UnknownRange
            )),
            "an unopenable directory behind a link must read as a lost range, got {costs:?}"
        );
    }

    // A side with no time to compare cannot be shown to match, so the pair has to transfer. Letting
    // `Option`'s own ordering answer gets this backwards: `None` sorts below every `Some`, so
    // "destination older than source" reads as false and the key is skipped.
    #[test]
    fn an_entry_with_no_time_is_not_taken_for_unchanged() {
        let entry = |size, secs| Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size,
                last_modified_secs: secs,
                obstruction: None,
            },
            source: (),
        };

        // Same size on both sides, and the source's time could not be read.
        assert_eq!(
            decide(&entry(Some(5), None), &entry(Some(5), Some(1_700_000_000))),
            Action::Transfer,
            "a source with no time cannot be shown to match the destination"
        );
        // And the same the other way round.
        assert_eq!(
            decide(&entry(Some(5), Some(1_700_000_000)), &entry(Some(5), None)),
            Action::Transfer,
            "a destination with no time cannot be shown to match the source"
        );
    }

    // A walk that read no metadata must not look like a real zero-byte object. Reporting a size of
    // zero makes the two compare equal, and with no time to compare either the pair reads as
    // unchanged — so the key is skipped, which is the one outcome it must never produce.
    #[test]
    fn an_entry_whose_metadata_was_never_read_is_not_taken_for_an_empty_object() {
        let unread = Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size: None,
                last_modified_secs: None,
                obstruction: None,
            },
            source: (),
        };
        let empty_object = Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size: Some(0),
                last_modified_secs: Some(1_700_000_000),
                obstruction: None,
            },
            source: (),
        };
        assert_eq!(decide(&unread, &empty_object), Action::Transfer);
    }

    // A listing that reports a negative size has told us something impossible. Casting it would
    // give an enormous length a comparison reads as real, and anything sizing a buffer or a part
    // plan from it would trust a 16-exabyte object.
    #[test]
    fn a_negative_size_is_reported_rather_than_wrapped() {
        let negative = Object::builder()
            .key("data/a.txt")
            .size(-1)
            .last_modified(DateTime::from_secs(1))
            .build();
        match key_and_meta(&negative, "data/") {
            Err(StreamError::MalformedListing {
                key: Some(key),
                what,
            }) => {
                assert_eq!(key, "a.txt");
                assert!(what.contains("size"), "got {what:?}");
            }
            other => panic!("expected a named malformed listing, got {other:?}"),
        }
    }

    // A time either side of the epoch is a time a run can compare, so both are reported. Only a
    // value with no seconds to give comes back unknown.
    #[test]
    fn a_time_before_the_epoch_is_reported_as_a_negative() {
        use std::time::Duration;

        let day = Duration::from_secs(86_400);
        assert_eq!(secs_since_epoch(Ok(UNIX_EPOCH)), Some(0));
        assert_eq!(secs_since_epoch(Ok(UNIX_EPOCH + day)), Some(86_400));
        assert_eq!(secs_since_epoch(Ok(UNIX_EPOCH - day)), Some(-86_400));

        // Both sides count down to the second below, the way `stat` reports them, so a run
        // agrees with every other tool reading the same file.
        let fraction = Duration::from_millis(1_900);
        assert_eq!(secs_since_epoch(Ok(UNIX_EPOCH + fraction)), Some(1));
        assert_eq!(secs_since_epoch(Ok(UNIX_EPOCH - fraction)), Some(-2));
    }

    #[test]
    fn a_time_with_no_seconds_to_give_is_unknown() {
        assert_eq!(
            secs_since_epoch(Err(std::io::Error::other("no metadata"))),
            None
        );
    }

    // The largest time this platform can hold still converts. The ceiling is searched for rather
    // than assumed, because a clock's range is not the same everywhere: one counts seconds in a
    // width that reaches `i64::MAX`, another counts 100ns ticks from 1601 and runs out long before.
    //
    // The assertion on that ceiling is also the canary. Should a platform arrive whose clock reaches
    // past `i64` seconds, `secs_since_epoch` has a time it cannot report and wants testing against
    // it.
    #[test]
    fn no_time_this_platform_can_hold_overflows_the_conversion() {
        use std::time::Duration;

        let mut lo = 0u64;
        let mut hi = u64::MAX;
        while lo < hi {
            let mid = lo + (hi - lo) / 2 + 1;
            if UNIX_EPOCH.checked_add(Duration::from_secs(mid)).is_some() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        assert!(
            lo <= u64::try_from(i64::MAX).unwrap(),
            "this platform holds a time beyond i64 seconds, so the conversion needs testing"
        );
        let largest = UNIX_EPOCH
            .checked_add(Duration::from_secs(lo))
            .expect("the ceiling the search just found");
        assert_eq!(
            secs_since_epoch(Ok(largest)),
            Some(i64::try_from(lo).unwrap())
        );
    }
}
