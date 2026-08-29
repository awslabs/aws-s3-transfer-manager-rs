/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Translating between local relative paths and S3 object keys.
//!
//! Shared by the directory operations and by key-ordered comparison, which all need
//! the same answer for what key a given file has.

use std::borrow::Cow;
use std::path::{MAIN_SEPARATOR, MAIN_SEPARATOR_STR};

use crate::error;

// Default S3 key delimiter.
pub(crate) const DEFAULT_DELIMITER: &str = "/";

/// Derive the S3 object key for a file at `relative_filename` inside the walk root.
///
/// The key is formed by optionally prepending a prefix and substituting the
/// path separator with a custom delimiter if one is configured. When the
/// custom delimiter appears inside `relative_filename`, derivation fails with
/// an invalid-input error.
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

    &stripped[1..]
}

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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_strip_key_prefix_delims() {
        for delim in ["/", "//", "\\", "|", "delim"] {
            let prefix = format!("notes{delim}2021{delim}");
            let key = format!("notes{delim}2021{delim}1.txt");
            let actual = strip_key_prefix(&key, Some(&prefix), Some(delim));
            assert_eq!("1.txt", actual, "delim={delim:?}");
        }
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
