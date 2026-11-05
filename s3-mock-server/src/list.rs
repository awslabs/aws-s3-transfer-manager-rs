/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! ListObjectsV2 pagination.
//!
//! A page holds up to `max_keys` entries, where an entry is an object or a common prefix: all
//! the keys that roll up into one common prefix count as one entry. In key order, as in general
//! purpose buckets, a common prefix is never split across pages or repeated on a later page.
//! [`ListOrder::Unordered`] lists the entries in a fixed pseudo-random order instead, as directory
//! buckets do, with each common prefix as one entry in that order.

use base64::Engine;

use crate::storage::ObjectInfo;

/// The order in which a listing returns keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListOrder {
    /// Lexicographic order of the keys' UTF-8 bytes. Continuation tokens encode the first key of
    /// the next page.
    Lexicographic,
    /// A pseudo-random order of the entries, keys and common prefixes, determined by their names
    /// and stable across requests and builds. Continuation tokens encode the name of the first
    /// entry of the next page, so a listing continues where it left off when keys are created or
    /// deleted between pages. `start-after` is ignored: no key order defines it, and directory
    /// buckets reject it.
    Unordered,
}

/// The parameters of a ListObjectsV2 request that select a page.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ListRequest<'a> {
    pub prefix: &'a str,
    /// The delimiter; an empty delimiter is no delimiter.
    pub delimiter: Option<&'a str>,
    pub max_keys: usize,
    pub continuation_token: Option<&'a str>,
    /// Ignored when there is a continuation token, as in S3.
    pub start_after: Option<&'a str>,
}

/// One page of a listing.
#[derive(Debug, Default)]
pub(crate) struct ListPage {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    /// The token for the next page, if the listing continues.
    pub next_continuation_token: Option<String>,
}

/// Why a page could not be produced.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ListError {
    /// The continuation token was not issued by a listing.
    InvalidContinuationToken,
}

/// The page of `objects` (every object under the request's prefix, in any order) that `request`
/// selects.
pub(crate) fn list_page(
    mut objects: Vec<ObjectInfo>,
    request: &ListRequest<'_>,
    order: ListOrder,
) -> Result<ListPage, ListError> {
    let delimiter = request.delimiter.filter(|d| !d.is_empty());
    match order {
        ListOrder::Lexicographic => {
            objects.sort_by(|a, b| a.key.cmp(&b.key));
            list_page_lexicographic(objects, request, delimiter)
        }
        ListOrder::Unordered => list_page_unordered(objects, request, delimiter),
    }
}

/// The common prefix `key` rolls up into, if any.
fn common_prefix_of(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter?;
    let rest = key.strip_prefix(prefix)?;
    let index = rest.find(delimiter)?;
    Some(format!("{prefix}{}{delimiter}", &rest[..index]))
}

fn list_page_lexicographic(
    objects: Vec<ObjectInfo>,
    request: &ListRequest<'_>,
    delimiter: Option<&str>,
) -> Result<ListPage, ListError> {
    let start_key = request
        .continuation_token
        .map(|token| {
            base64::engine::general_purpose::STANDARD
                .decode(token)
                .ok()
                .and_then(|decoded| String::from_utf8(decoded).ok())
                .ok_or(ListError::InvalidContinuationToken)
        })
        .transpose()?;

    let mut page = ListPage::default();
    for obj in objects {
        let skip = match (&start_key, request.start_after) {
            (Some(start_key), _) => obj.key < *start_key,
            (None, Some(start_after)) => obj.key.as_str() <= start_after,
            (None, None) => false,
        };
        if skip {
            continue;
        }
        let common_prefix = common_prefix_of(&obj.key, request.prefix, delimiter);
        // Entries are ordered by their own names, so a common prefix that sorts up to
        // `start-after` is skipped with all its keys, even those that sort after it.
        if let (None, Some(start_after), Some(common_prefix)) =
            (&start_key, request.start_after, &common_prefix)
        {
            if common_prefix.as_str() <= start_after {
                continue;
            }
        }
        // A key that rolls up into the page's last common prefix adds no entry.
        if common_prefix.is_some() && common_prefix.as_ref() == page.common_prefixes.last() {
            continue;
        }
        if page.objects.len() + page.common_prefixes.len() >= request.max_keys {
            if request.max_keys > 0 {
                page.next_continuation_token =
                    Some(base64::engine::general_purpose::STANDARD.encode(&obj.key));
            }
            break;
        }
        match common_prefix {
            Some(common_prefix) => page.common_prefixes.push(common_prefix),
            None => page.objects.push(obj),
        }
    }
    Ok(page)
}

fn list_page_unordered(
    objects: Vec<ObjectInfo>,
    request: &ListRequest<'_>,
    delimiter: Option<&str>,
) -> Result<ListPage, ListError> {
    /// Where an entry named `name` is in the order: by hash, then by name.
    fn order_key(name: &str) -> (u64, String) {
        (fnv1a(name.as_bytes()), name.to_owned())
    }

    // Roll the keys up into their common prefixes first, so that each common prefix is a single
    // entry in the order.
    let mut entries: Vec<(String, Option<ObjectInfo>)> = Vec::new();
    let mut prefixes = std::collections::HashSet::new();
    for obj in objects {
        match common_prefix_of(&obj.key, request.prefix, delimiter) {
            Some(common_prefix) => {
                if prefixes.insert(common_prefix.clone()) {
                    entries.push((common_prefix, None));
                }
            }
            None => entries.push((obj.key.clone(), Some(obj))),
        }
    }
    entries.sort_by_cached_key(|(name, _)| order_key(name));

    let start = match request.continuation_token {
        None => 0,
        Some(token) => {
            let start_name = base64::engine::general_purpose::STANDARD
                .decode(token)
                .ok()
                .and_then(|decoded| String::from_utf8(decoded).ok())
                .ok_or(ListError::InvalidContinuationToken)?;
            let start_name = order_key(&start_name);
            entries.partition_point(|(name, _)| order_key(name) < start_name)
        }
    };
    let end = start.saturating_add(request.max_keys).min(entries.len());
    let mut page = ListPage {
        next_continuation_token: (end < entries.len() && request.max_keys > 0)
            .then(|| base64::engine::general_purpose::STANDARD.encode(&entries[end].0)),
        ..Default::default()
    };
    for (name, object) in entries.drain(start..end) {
        match object {
            Some(object) => page.objects.push(object),
            None => page.common_prefixes.push(name),
        }
    }
    Ok(page)
}

/// 64-bit FNV-1a of `bytes`: a hash that is the same in every build, unlike `std`'s
/// `DefaultHasher`.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn objects(keys: &[&str]) -> Vec<ObjectInfo> {
        keys.iter()
            .map(|key| ObjectInfo {
                key: key.to_string(),
                metadata: Default::default(),
            })
            .collect()
    }

    fn request(delimiter: Option<&'static str>, max_keys: usize) -> ListRequest<'static> {
        ListRequest {
            prefix: "",
            delimiter,
            max_keys,
            continuation_token: None,
            start_after: None,
        }
    }

    /// Every page of a listing, as (keys, common prefixes).
    fn pages(
        keys: &[&str],
        request: ListRequest<'_>,
        order: ListOrder,
    ) -> Vec<(Vec<String>, Vec<String>)> {
        let mut pages = Vec::new();
        let mut token = None;
        loop {
            let request = ListRequest {
                continuation_token: token.as_deref(),
                ..request
            };
            let page = list_page(objects(keys), &request, order).unwrap();
            pages.push((
                page.objects.into_iter().map(|o| o.key).collect(),
                page.common_prefixes,
            ));
            match page.next_continuation_token {
                Some(next) => token = Some(next),
                None => return pages,
            }
        }
    }

    const KEYS: &[&str] = &["a/1", "a/2", "a/3", "b", "c/1", "c/2", "d"];

    #[test]
    fn common_prefixes_are_never_split_or_repeated() {
        for max_keys in 1..=5 {
            let pages = pages(KEYS, request(Some("/"), max_keys), ListOrder::Lexicographic);
            let entries: Vec<String> = pages
                .iter()
                .flat_map(|(keys, prefixes)| keys.iter().chain(prefixes))
                .cloned()
                .collect();
            let mut sorted = entries.clone();
            sorted.sort();
            assert_eq!(sorted, ["a/", "b", "c/", "d"], "max_keys {max_keys}");
            assert_eq!(
                pages.len(),
                4_usize.div_ceil(max_keys),
                "max_keys {max_keys}"
            );
            for (keys, prefixes) in &pages {
                assert!(keys.len() + prefixes.len() <= max_keys);
            }
        }
    }

    #[test]
    fn empty_delimiter_is_no_delimiter() {
        let pages = pages(KEYS, request(Some(""), 100), ListOrder::Lexicographic);
        assert_eq!(
            pages,
            [(KEYS.iter().map(|k| k.to_string()).collect(), vec![])]
        );
    }

    #[test]
    fn start_after_skips_entries_up_to_and_including_it() {
        let listed = |start_after| {
            let request = ListRequest {
                start_after: Some(start_after),
                ..request(Some("/"), 100)
            };
            let page = list_page(objects(KEYS), &request, ListOrder::Lexicographic).unwrap();
            (
                page.objects.into_iter().map(|o| o.key).collect::<Vec<_>>(),
                page.common_prefixes,
            )
        };
        // `a/` sorts before `a/2`, so it is skipped although `a/3` sorts after `a/2`.
        assert_eq!(
            listed("a/2"),
            (vec!["b".to_owned(), "d".to_owned()], vec!["c/".to_owned()])
        );
        // `a/` sorts after `a`.
        assert_eq!(
            listed("a"),
            (
                vec!["b".to_owned(), "d".to_owned()],
                vec!["a/".to_owned(), "c/".to_owned()]
            )
        );
        assert_eq!(listed("a/").1, ["c/"]);
    }

    #[test]
    fn max_keys_zero_returns_an_empty_final_page() {
        let page = list_page(objects(KEYS), &request(None, 0), ListOrder::Lexicographic).unwrap();
        assert!(page.objects.is_empty() && page.common_prefixes.is_empty());
        assert_eq!(page.next_continuation_token, None);
    }

    #[test]
    fn invalid_tokens_fail() {
        for order in [ListOrder::Lexicographic, ListOrder::Unordered] {
            let request = ListRequest {
                continuation_token: Some("!not a token!"),
                ..request(None, 10)
            };
            assert_eq!(
                list_page(objects(KEYS), &request, order).unwrap_err(),
                ListError::InvalidContinuationToken
            );
        }
    }

    #[test]
    fn unordered_listing_returns_every_key_once_in_a_fixed_order() {
        let keys: Vec<String> = (0..50).map(|i| format!("key-{i:02}")).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let listed = |max_keys| -> Vec<String> {
            pages(&keys, request(None, max_keys), ListOrder::Unordered)
                .into_iter()
                .flat_map(|(keys, _)| keys)
                .collect()
        };
        let order = listed(50);
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(sorted, keys);
        assert_ne!(order, keys, "the keys should not be in lexicographic order");
        for max_keys in [1, 3, 7] {
            assert_eq!(listed(max_keys), order);
        }
    }

    #[test]
    fn unordered_listing_continues_after_changes_between_pages() {
        let keys = ["k0", "k1", "k2", "k3", "k4"];
        let first = list_page(objects(&keys), &request(None, 2), ListOrder::Unordered).unwrap();
        let mut listed: Vec<String> = first.objects.iter().map(|o| o.key.clone()).collect();
        // Delete the first key listed and create another before the next pages.
        let changed: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|key| *key != listed[0])
            .chain(["k5"])
            .collect();
        let mut token = first.next_continuation_token;
        while let Some(continuation_token) = token {
            let request = ListRequest {
                continuation_token: Some(&continuation_token),
                ..request(None, 2)
            };
            let page = list_page(objects(&changed), &request, ListOrder::Unordered).unwrap();
            listed.extend(page.objects.iter().map(|o| o.key.clone()));
            token = page.next_continuation_token;
        }
        // Every key that existed when the listing started is listed exactly once.
        for key in keys {
            assert_eq!(
                listed.iter().filter(|k| *k == key).count(),
                1,
                "{key} in {listed:?}"
            );
        }
    }

    #[test]
    fn unordered_listing_reports_each_common_prefix_once() {
        for max_keys in [1, 2, 3, 100] {
            let pages = pages(KEYS, request(Some("/"), max_keys), ListOrder::Unordered);
            assert!(pages
                .iter()
                .all(|(keys, prefixes)| keys.len() + prefixes.len() <= max_keys));
            let (mut keys, mut prefixes): (Vec<String>, Vec<String>) = pages.into_iter().fold(
                (Vec::new(), Vec::new()),
                |(mut keys, mut prefixes), (k, p)| {
                    keys.extend(k);
                    prefixes.extend(p);
                    (keys, prefixes)
                },
            );
            keys.sort();
            prefixes.sort();
            assert_eq!(keys, ["b", "d"], "max_keys={max_keys}");
            assert_eq!(prefixes, ["a/", "c/"], "max_keys={max_keys}");
        }
    }
}
