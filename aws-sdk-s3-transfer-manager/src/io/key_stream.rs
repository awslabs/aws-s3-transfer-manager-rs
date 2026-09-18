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

use crate::io::key::{derive_object_key, DEFAULT_DELIMITER};
use crate::io::key_filter::KeyFilter;
use crate::io::walk::{
    exclude_s3_folder_markers, FsEntry, FsWalk, S3Walk, WalkError, WalkErrorKind,
};

// Whole seconds, because that is the granularity S3 reports last-modified at.
// Keeping finer local precision would make an identical pair differ every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMeta {
    pub(crate) size: u64,
    // `None` when the platform cannot represent the recorded time. Substituting a
    // value here would hand a comparison a number nobody observed, with nothing to
    // say so.
    pub(crate) last_modified_secs: Option<i64>,
}

// `source` travels with the key because the key alone is not enough to act on: a
// path rebuilt from it is not the path that was walked, and on the S3 side the
// listing's storage class and restore status decide whether the object is readable
// at all.
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
    // A listed object without a field a comparison needs. One key that cannot be compared, and
    // the listing itself arrived.
    MalformedListing(&'static str),
}

impl StreamError {
    // Whether nothing is left to carry on with. Only a walk can say so: a name that cannot be
    // keyed and an object missing a field each cost one key.
    pub(crate) fn is_fatal(&self) -> bool {
        match self {
            StreamError::Walk(err) => err.is_fatal(),
            StreamError::UnkeyableName(_) | StreamError::MalformedListing(_) => false,
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
            StreamError::MalformedListing(what) => write!(f, "{what}"),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StreamError::Walk(err) => Some(err),
            StreamError::UnkeyableName(_) | StreamError::MalformedListing(_) => None,
        }
    }
}

impl From<WalkError> for StreamError {
    fn from(err: WalkError) -> Self {
        StreamError::Walk(err)
    }
}

// Errors pass through as the walkers report them; what they mean is the caller's
// call, via `view_incomplete`. A run may continue past an unreadable directory, but
// not while also deleting keys it never saw.
pub(crate) trait KeyStream {
    type Source;

    // Named to avoid colliding with the walkers' inherent `next`.
    fn next_entry(
        &mut self,
    ) -> impl Future<Output = Option<Result<Entry<Self::Source>, StreamError>>> + Send;
}

// Whether a failure left keys unaccounted for, so a side looks emptier than it is.
//
// An unread subtree is the case that matters: position-based comparison cannot tell it from
// deletion. A name that could not be keyed and an object missing a field each cost exactly one
// key, which the stream reports in place, so the keys around them are still known.
pub(crate) fn view_incomplete(err: &StreamError) -> bool {
    match err {
        StreamError::Walk(err) => {
            err.is_fatal() || err.kind() == WalkErrorKind::DirectoryUnreadable
        }
        StreamError::UnkeyableName(_) | StreamError::MalformedListing(_) => false,
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
        .ok_or_else(|| StreamError::UnkeyableName(entry.path().to_path_buf()))
}

// `None` when the name cannot be a key at all.
fn key_for_relative_path(relative: &std::path::Path) -> Option<String> {
    let relative = relative.to_str()?;
    Some(
        derive_object_key(relative, None, None)
            .expect("key derivation cannot fail without a custom delimiter")
            .into_owned(),
    )
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

pub(crate) fn s3_predicate(
    filter: Arc<KeyFilter>,
    prefix: Option<String>,
) -> impl Fn(&Object) -> bool + Send + Sync + 'static {
    move |obj| {
        // Folder markers are dropped by the walker's own default, which setting a
        // filter would otherwise replace.
        if !exclude_s3_folder_markers(obj) {
            return false;
        }
        let key = obj.key().unwrap_or_default();
        // A key outside the root is not the filter's business: it should never have been
        // listed, and testing a rule against a key that is not under the root would answer
        // about a name nobody asked for.
        match relative_key(key, &root_prefix(prefix.as_deref())) {
            Some(relative) => filter.allows(relative),
            None => false,
        }
    }
}

impl KeyStream for FsWalk {
    type Source = FsEntry;

    async fn next_entry(&mut self) -> Option<Result<Entry<FsEntry>, StreamError>> {
        match self.next().await? {
            Ok(entry) => {
                let key = match local_key(&entry) {
                    Ok(key) => key,
                    Err(err) => return Some(Err(err)),
                };
                let meta = EntryMeta {
                    // A walk that read no metadata gives a zero-sized entry with no time, which
                    // a comparison reads as differing from anything and never skips.
                    size: entry.metadata().map_or(0, |m| m.len()),
                    last_modified_secs: entry
                        .metadata()
                        .and_then(|m| secs_since_epoch(m.modified())),
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
                    Err(reason) => return Some(Err(StreamError::MalformedListing(reason))),
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
// Markers are dropped here, so they are invisible whether or not a filter is
// configured. The walker's own default filter is replaced by any filter a caller
// sets, which would otherwise make an entry out of a key holding nothing.
fn key_and_meta(
    obj: &Object,
    root_prefix: &str,
) -> Result<Option<(String, EntryMeta)>, &'static str> {
    if !exclude_s3_folder_markers(obj) {
        return Ok(None);
    }
    let key = obj.key().ok_or("listing returned an object with no key")?;
    let Some(relative) = relative_key(key, root_prefix) else {
        return Ok(None);
    };
    let size = obj.size().ok_or("listing returned no size")?;
    let last_modified_secs = obj
        .last_modified()
        .ok_or("listing returned no last-modified")?
        .secs();
    Ok(Some((
        relative.to_string(),
        EntryMeta {
            size: size as u64,
            // Always present: a listing without one is rejected above.
            last_modified_secs: Some(last_modified_secs),
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::walk::{FsWalkContext, FsWalker, S3WalkContext, S3Walker};
    use aws_sdk_s3::types::{ObjectStorageClass, RestoreStatus};
    use aws_smithy_types::DateTime;
    use std::fs;
    use tempfile::tempdir;

    fn local(root: &std::path::Path) -> FsWalk {
        FsWalker::builder()
            .recursive(true)
            .key_order(true)
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
        assert_eq!(entry.meta.size, 5);
        assert!(entry.meta.last_modified_secs.unwrap() > 1_600_000_000);
        // What a transfer opens, not a path rebuilt from the key.
        assert_eq!(entry.source.path(), dir.path().join("f"));
    }

    // What a stream error is, for asserting on a whole batch at once.
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
            key_for_relative_path(std::path::Path::new("fine.txt")),
            Some("fine.txt".to_string())
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
    fn decide<S, D>(src: &Entry<S>, dest: &Entry<D>) -> Action {
        if src.meta.size != dest.meta.size
            || dest.meta.last_modified_secs < src.meta.last_modified_secs
        {
            Action::Transfer
        } else {
            Action::Skip
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
        assert!(
            matches!(errors[0], StreamError::MalformedListing(_)),
            "got {:?}",
            errors[0]
        );
        assert!(!errors[0].is_fatal(), "one bad object must not end the run");
        assert!(
            !view_incomplete(&errors[0]),
            "the listing arrived, so no key is unaccounted for"
        );
    }

    // What each failure costs, since a comparison reads absence from position: a side that lost a
    // subtree cannot be trusted to say a key is missing, while a side that lost one key can.
    #[test]
    fn only_a_lost_subtree_makes_a_side_look_emptier_than_it_is() {
        let walk_err = |kind| StreamError::Walk(WalkError::new(None, kind, Box::from("test")));
        let cases = [
            (walk_err(WalkErrorKind::SourceUnreadable), true),
            (walk_err(WalkErrorKind::DirectoryUnreadable), true),
            (walk_err(WalkErrorKind::Service), true),
            // A named file the walk read fine, and one listed object: both are single keys.
            (walk_err(WalkErrorKind::PermissionDenied), false),
            (walk_err(WalkErrorKind::BrokenSymlink), false),
            (StreamError::UnkeyableName(PathBuf::from("/tmp/x")), false),
            (StreamError::MalformedListing("no size"), false),
        ];
        for (err, hides_keys) in cases {
            assert_eq!(view_incomplete(&err), hides_keys, "err={err:?}");
        }
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
        assert_eq!(meta.size, 3);
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
                size: 1,
                last_modified_secs: Some(1),
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

    use crate::io::key_filter::Rule;

    fn filtered_local(root: &std::path::Path, rules: Vec<Rule>) -> FsWalk {
        let filter = Arc::new(KeyFilter::new(rules));
        FsWalker::builder()
            .recursive(true)
            .key_order(true)
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

    // An excluded entry must not warn even when it cannot be read, which means the
    // rules have to be consulted before the metadata is.
    // A directory that can be listed but whose children cannot be stat'd: readable,
    // not searchable. Returns `None` when the mode has no effect, as for root.
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

    fn unlock(dir: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

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

    // A named pipe is skipped without a warning whether or not a rule excludes it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_excluded_special_file_warns_about_nothing() {
        let dir = tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo");
        if !status.success() {
            return;
        }
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_unreadable_subdirectory_leaves_the_view_incomplete() {
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
                Err(err) => incomplete |= view_incomplete(&err),
            }
        }
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(seen.contains(&"a.txt".to_string()));
        assert!(incomplete, "an unread subtree must be an incomplete view");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_broken_symlink_costs_one_entry_not_the_view() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "").unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("broken")).unwrap();

        let mut walk = FsWalker::builder()
            .recursive(true)
            .key_order(true)
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
        assert!(!view_incomplete(&errors[0]));
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

    // The largest time this platform holds still converts, and one second more cannot be built
    // at all, so the checked conversion never fails here. It guards a platform whose clock is
    // wider than `i64` seconds; should this assertion ever fail, that platform has arrived and
    // `secs_since_epoch` wants testing against a time it cannot report.
    #[test]
    fn no_time_this_platform_can_hold_overflows_the_conversion() {
        use std::time::Duration;

        let largest = UNIX_EPOCH
            .checked_add(Duration::from_secs(u64::try_from(i64::MAX).unwrap()))
            .expect("i64::MAX seconds from the epoch");
        assert_eq!(secs_since_epoch(Ok(largest)), Some(i64::MAX));

        let beyond = u64::try_from(i64::MAX).unwrap() + 1;
        assert!(
            UNIX_EPOCH
                .checked_add(Duration::from_secs(beyond))
                .is_none(),
            "this platform holds a time beyond i64 seconds, so the conversion needs testing"
        );
    }
}
