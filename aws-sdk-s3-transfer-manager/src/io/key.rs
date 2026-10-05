/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Reached only by its own tests until the comparison wires it up.
#[allow(dead_code)] // TODO(sync): the comparison applies these rules to both sides
pub(crate) mod filter;
// Nothing outside this module's own tests calls it until the comparison lands.
#[allow(dead_code)] // TODO(sync): the comparison reads both sides through this
pub(crate) mod stream;

// Translating between local relative paths and S3 object keys.
//
// The directory operations and key-ordered comparison both come here, and they do not agree on what
// a prefix is: one names a span of keys to take off, the other names a place that holds entries.
// Both notions live here, and `strip_key_prefix` and `relative_key` are where they part.

use std::borrow::Cow;
use std::path::{Component, Path, PathBuf, MAIN_SEPARATOR, MAIN_SEPARATOR_STR};

use path_clean::PathClean;

use crate::error;

// Default S3 key delimiter.
pub(crate) const DEFAULT_DELIMITER: &str = "/";

// Derive the S3 object key for a file at `relative_filename` inside the walk root.
//
// The key is formed by optionally prepending a prefix and substituting the
// path separator with a custom delimiter if one is configured. When the
// custom delimiter appears inside `relative_filename`, derivation fails with
// an invalid-input error.
pub(crate) fn derive_object_key<'a>(
    relative_filename: &'a str,
    object_key_prefix: Option<&str>,
    object_key_delimiter: Option<&str>,
) -> Result<Cow<'a, str>, error::Error> {
    if let Some(delim) = object_key_delimiter {
        if delim != DEFAULT_DELIMITER && relative_filename.contains(delim) {
            return Err(error::invalid_input(format!(
                "a custom delimiter `{delim}` should not appear in `{relative_filename}`"
            )));
        }
    }

    let delim = object_key_delimiter.unwrap_or(DEFAULT_DELIMITER);

    let relative_filename = if delim == MAIN_SEPARATOR_STR {
        Cow::Borrowed(relative_filename)
    } else {
        Cow::Owned(relative_filename.replace(MAIN_SEPARATOR, delim))
    };

    let object_key = if let Some(prefix) = object_key_prefix {
        if prefix.ends_with(delim) {
            Cow::Owned(format!("{prefix}{relative_filename}"))
        } else {
            Cow::Owned(format!("{prefix}{delim}{relative_filename}"))
        }
    } else {
        relative_filename
    };

    Ok(object_key)
}

// The part of `key` below `prefix`, for building a local path under a download's destination.
//
// A download of `s3://bucket/data` writes `data/a.txt` to `a.txt`, so the prefix here names a span
// of keys: it comes off any key that starts with those letters and holds a delimiter somewhere, so
// `data/z` becomes `z` and `datab/x` becomes `b/x`. A sync needs `relative_key`, which treats the
// prefix as a place, where `datab/x` is not under `data/` and has no relative key at all. The two
// also disagree on order: `data/z` sorts before `datab/x`, while `b/x` sorts before `z`.
pub(crate) fn strip_key_prefix<'a>(
    key: &'a str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> &'a str {
    let prefix = prefix.unwrap_or("");
    let delim = delimiter.unwrap_or(DEFAULT_DELIMITER);

    if key.is_empty() || prefix.is_empty() || !key.starts_with(prefix) || !key.contains(delim) {
        return key;
    }

    let stripped = &key[prefix.len()..];

    if prefix.ends_with(delim) || !stripped.starts_with(delim) {
        return stripped;
    }

    // By the delimiter's own length. A delimiter is caller-supplied and arbitrary, so one byte is
    // right only for a single-byte one: two bytes leave half of it on the key, and slicing inside a
    // multi-byte character panics.
    &stripped[delim.len()..]
}

// `key` with the bucket's delimiter swapped for the platform's path separator, so a key becomes a
// relative path.
//
//     ("a|b|c.txt", Some("|"), "/")   ->  "a/b/c.txt"
//     ("a/b/c.txt", Some("/"), "/")   ->  "a/b/c.txt"    borrowed, the two agree
//     ("a/b/c.txt", None,      "/")   ->  "a/b/c.txt"    borrowed, no delimiter configured
//     ("a/b/c.txt", Some("/"), "\\")  ->  "a\\b\\c.txt"   Windows
//
// Only a caller-configured delimiter that differs from the separator allocates, so every key on
// Unix with the default delimiter is borrowed.
pub(crate) fn replace_delim<'a>(
    key: &'a str,
    delimiter: Option<&str>,
    path_separator: &str,
) -> Cow<'a, str> {
    match delimiter {
        Some(delim) if delim != path_separator => {
            let replaced = key.replace(delim, path_separator);
            Cow::Owned(replaced)
        }
        _ => Cow::Borrowed(key),
    }
}

/// Derive the local filesystem path for a given S3 key.
///
/// Strips the configured prefix, replaces the delimiter with the OS path
/// separator, joins with the destination root, normalizes via `path_clean`,
/// and validates the result stays within the root (path traversal guard).
// TODO(vnext): two keys can end up as the same local file. `path_clean` sends `a//b`, `a/./b` and
// `a/b` to one path, and a case-insensitive filesystem folds `Photos/x` onto `photos/x`, leaving a
// download to write one file and report a success for each key. S3 GetObject keeps the keys apart
// and serves each one by name, but preserving them as they are in a filename may not be allowed by
// the OS, which collapses `//` to `/`. Currently the transfer manager preemptively normalizes them
// to avoid a collision, but that shouldn't happen. If there is a collision, it is a write failure
// and should follow the failure policy. `derive_object_key` breaks the same way in the other
// direction, turning two local names into one key.
// See https://github.com/awslabs/aws-s3-transfer-manager-rs/pull/184#discussion_r4149427294
pub(crate) fn local_key_path(
    root_dir: &Path,
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Result<PathBuf, crate::error::Error> {
    let stripped = strip_key_prefix(key, prefix, delimiter);
    let relative_path = replace_delim(stripped, delimiter, std::path::MAIN_SEPARATOR_STR);

    let local_path = root_dir.join(relative_path.as_ref()).clean();
    validate_path(root_dir, &local_path, key)?;

    Ok(local_path)
}

// The part of a derived path lying below `root`, or `None` when the path lies outside it.
//
// Both sides cleaned, because only one of them arrives that way: the derivation normalises the
// path before it gets here, and the root reaches this as the caller wrote it. A root carrying a `.`
// or a `..` would otherwise make every key inside it look like an escape. A root that cleans to `.` leaves no prefix on
// the path at all — cleaning strips it — so the whole path is the remainder.
//
// Strictly below, not at or below. An empty remainder names the root itself, and a caller building a
// temporary name beside that path builds one beside the root. A remainder opening with anything but
// an ordinary name sits outside: a `..` climbs out, a `.` repeats the directory, and a root or a
// drive prefix never was relative to the root, because `Path::join` drops the left side when the
// right one is absolute.
//
// One answer for two callers. Containment asks whether a remainder exists; the local deleter asks
// whether the remainder still reads as the key it came from. Deriving it separately in each place is
// how one of them came to accept `/etc/passwd` as a key.
pub(crate) fn below_root<'p>(root: &Path, path: &'p Path) -> Option<&'p Path> {
    let root = root.clean();
    let rest = if root == Path::new(".") {
        path
    } else {
        path.strip_prefix(root).ok()?
    };
    matches!(rest.components().next(), Some(Component::Normal(_))).then_some(rest)
}

fn validate_path(root_dir: &Path, local_path: &Path, key: &str) -> Result<(), crate::error::Error> {
    if below_root(root_dir, local_path).is_none() {
        return Err(error::Error::new(
            error::ErrorKind::InputInvalid,
            format!(
                "Unable to download key: '{key}', its relative path resolves \
                 outside the target destination directory"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // A root is whatever a caller passed, and a caller may pass one with a `.` or a `..` in it. The
    // key is cleaned before the comparison, so the root has to be too, or every key looks like it
    // escapes a root that it is in fact inside.
    #[test]
    fn a_root_written_the_long_way_round_still_holds_its_keys() {
        for root in ["/tmp/./root", "/tmp/other/../root", "/tmp/root/"] {
            assert_eq!(
                local_key_path(Path::new(root), "a/b", None, None)
                    .unwrap_or_else(|e| panic!("root {root:?} rejected a key inside it: {e}")),
                Path::new("/tmp/root/a/b"),
            );
        }
    }

    // A destination given as the working directory holds its keys like any other. `aws s3 sync
    // s3://bucket .` is the ordinary way to write it, and cleaning leaves the root as a bare `.`,
    // which no key's cleaned path starts with.
    #[test]
    fn the_working_directory_holds_its_keys() {
        for root in [".", "./", "./."] {
            assert_eq!(
                local_key_path(Path::new(root), "a.txt", None, None)
                    .unwrap_or_else(|e| panic!("root {root:?} rejected a key inside it: {e}")),
                Path::new("a.txt"),
            );
            assert_eq!(
                local_key_path(Path::new(root), "nested/b.txt", None, None)
                    .unwrap_or_else(|e| panic!("root {root:?} rejected a nested key: {e}")),
                Path::new("nested/b.txt"),
            );
        }
        // A key climbing out of the working directory is still refused.
        assert!(local_key_path(Path::new("."), "../escape.txt", None, None).is_err());
    }

    // A key naming an absolute path takes the whole path: `Path::join` discards the left side when
    // the right one is absolute, so the root never appears in the result. Climbing out with `..` is
    // the way a relative key escapes, and asking only about that lets an absolute one through
    // without ever being relative to the root at all.
    #[test]
    fn a_key_that_names_an_absolute_path_escapes_the_working_directory() {
        for key in [
            "/etc/passwd",
            "/tmp/elsewhere.txt",
            "//double/slash",
            "/",
            // Cleaning cannot make this relative either: the key keeps its leading separator
            // through the prefix strip and the delimiter replacement.
            "/nested/under/root.txt",
        ] {
            let got = local_key_path(Path::new("."), key, None, None);
            assert!(
                got.is_err(),
                "key {key:?} resolved to {:?}, outside the working directory",
                got.ok(),
            );
        }
    }

    // The same key against a root that does not clean to `.`, where the containment check refuses
    // it and the working directory's arm never runs.
    #[test]
    fn a_key_that_names_an_absolute_path_escapes_a_named_root() {
        for root in ["/tmp/root", "relative/root"] {
            let got = local_key_path(Path::new(root), "/etc/passwd", None, None);
            assert!(
                got.is_err(),
                "root {root:?} accepted an absolute key, resolving it to {:?}",
                got.ok(),
            );
        }
    }

    // A key that resolves to the root names the directory itself, and there is no file there to
    // write. Accepting it is worse than useless: the caller builds a temporary name beside the
    // path it was given, and beside the root is outside the root — so the one key that passes the
    // containment check is the one that escapes it.
    #[test]
    fn a_key_resolving_to_the_root_itself_has_no_local_path() {
        let root = Path::new("/tmp/root");
        for key in [".", "a/..", "./.", "a/b/../.."] {
            assert!(
                local_key_path(root, key, None, None).is_err(),
                "key {key:?} resolved to the root itself and was accepted"
            );
        }
        // A key that lands inside is still fine.
        assert_eq!(
            local_key_path(root, "a/../b", None, None).expect("inside the root"),
            Path::new("/tmp/root/b")
        );
    }

    use super::*;

    // A key naming a place is the caller's business, not this derivation's. The directory download
    // has shipped accepting such a key: the trailing separator does not survive cleaning, so the
    // object's bytes land in a file named for the key's last component. Refusing it here would turn
    // a download that worked into one that fails, and under that operation's default policy it
    // takes the whole directory with it.
    #[test]
    fn a_key_naming_a_place_still_derives_a_path() {
        assert_eq!(
            local_key_path(Path::new("/tmp/root"), "photos/2019/", None, None)
                .expect("a key naming a place"),
            Path::new("/tmp/root/photos/2019")
        );
    }

    #[test]
    fn test_strip_key_prefix() {
        let cases: &[(&str, Option<&str>, Option<&str>, &str)] = &[
            ("no-delim", None, None, "no-delim"),
            ("no-delim", Some(""), None, "no-delim"),
            (
                "delim/with/separator",
                Some(""),
                None,
                "delim/with/separator",
            ),
            ("", Some("no-delim"), None, ""),
            ("no-delim", Some("no-delim"), None, "no-delim"),
            ("delim/", Some("delim"), None, ""),
            ("not-in-key", Some("prefix"), None, "not-in-key"),
            // A prefix here names a span of keys, so it comes off a key that merely starts with
            // those letters. A sync cannot use this, which is why `relative_key` exists.
            ("datab/x", Some("data"), None, "b/x"),
            // And a key with no delimiter anywhere keeps its whole name.
            ("datafile", Some("data"), None, "datafile"),
            ("notes/2021/1.txt", Some("notes/2021"), None, "1.txt"),
            ("notes/2021/1.txt", Some("notes/2021/"), None, "1.txt"),
            (
                "top-level/sub-folder/1.txt",
                Some("top-"),
                None,
                "level/sub-folder/1.txt",
            ),
            (
                "someInnerFolder/another/file1.txt",
                Some("someInner"),
                None,
                "Folder/another/file1.txt",
            ),
            (
                "someInnerF/another/file1.txt",
                Some("someInner"),
                None,
                "F/another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner"),
                None,
                "another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner/a"),
                None,
                "nother/file1.txt",
            ),
        ];
        for (key, prefix, delim, expected) in cases {
            let actual = strip_key_prefix(key, *prefix, *delim);
            assert_eq!(
                *expected, actual,
                "key={key:?} prefix={prefix:?} delim={delim:?}"
            );
        }
    }

    // FR-Root-4 lets a caller write the root with or without its trailing delimiter, so the prefix
    // reaching here often does not end in one — which is the only shape that gets as far as taking
    // the delimiter off. The sibling test below always passes a prefix that ends in it, so it
    // returns early and never exercises this.
    #[test]
    fn a_prefix_without_its_delimiter_still_strips_one_delimiter() {
        for delim in ["/", "//", "\\", "|", "delim", "§"] {
            let key = format!("notes{delim}2021{delim}1.txt");
            let expected = format!("2021{delim}1.txt");
            assert_eq!(
                strip_key_prefix(&key, Some("notes"), Some(delim)),
                expected,
                "delim={delim:?}"
            );
        }
    }

    #[test]
    fn test_strip_key_prefix_delims() {
        for delim in ["/", "//", "\\", "|", "delim"] {
            let prefix = format!("notes{delim}2021{delim}");
            let key = format!("notes{delim}2021{delim}1.txt");
            let actual = strip_key_prefix(&key, Some(&prefix), Some(delim));
            assert_eq!("1.txt", actual, "delim={delim:?}");
        }
    }

    #[test]
    fn a_root_spelled_with_or_without_a_trailing_separator_gives_the_same_key() {
        // `s3://bucket/notes` and `s3://bucket/notes/` name the same place, so both
        // spellings have to reduce to the same relative key. If they did not, the two
        // sides being compared would disagree about which entries pair up, and every
        // entry would look either missing or extra.
        assert_eq!(
            strip_key_prefix("notes/2021/1.txt", Some("notes/2021"), None),
            strip_key_prefix("notes/2021/1.txt", Some("notes/2021/"), None),
        );
        assert_eq!(
            "1.txt",
            strip_key_prefix("notes/2021/1.txt", Some("notes/2021"), None)
        );

        // Same for the prefix a local file's key is built with.
        assert_eq!(
            derive_object_key("2023/Jan/1.png", Some("foobar"), None).unwrap(),
            derive_object_key("2023/Jan/1.png", Some("foobar/"), None).unwrap(),
        );
        assert_eq!(
            "foobar/2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar"), None).unwrap()
        );
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn test_derive_object_key() {
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", None, None).unwrap()
        );
        assert_eq!(
            "foobar/2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar"), None).unwrap()
        );
        assert_eq!(
            "foobar/2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar/"), None).unwrap()
        );
        assert_eq!(
            "2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", None, Some("-")).unwrap()
        );
        assert_eq!(
            "foobar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar"), Some("-")).unwrap()
        );
        assert_eq!(
            "foobar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar-"), Some("-")).unwrap()
        );
        assert_eq!(
            "foobar--2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar--"), Some("-")).unwrap()
        );
        assert_eq!(
            "2023/MYLONGDELIMJan/MYLONGDELIM1.png",
            derive_object_key("2023/Jan/1.png", None, Some("/MYLONGDELIM")).unwrap()
        );
        {
            use std::error::Error as _;
            let err = derive_object_key("2023/Jan-1.png", None, Some("-"))
                .err()
                .unwrap();
            assert_eq!(
                "a custom delimiter `-` should not appear in `2023/Jan-1.png`",
                format!("{}", err.source().unwrap())
            );
        }

        // Should not replace the path separator in prefix with a custom delimiter
        assert_eq!(
            "foo/bar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foo/bar"), Some("-")).unwrap()
        );

        // Should not fail if the user specifies the default delimiter as a custom delimiter
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", None, Some(DEFAULT_DELIMITER)).unwrap()
        );
    }

    #[cfg(target_family = "windows")]
    #[test]
    fn test_derive_object_key() {
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023\\Jan\\1.png", None, None).unwrap()
        );
    }
}
