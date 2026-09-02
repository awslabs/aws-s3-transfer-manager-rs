/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Include/exclude rules over derived keys.
//!
//! Rules match a key relative to its own root, so one rule set covers both sides of
//! a comparison: a key excluded on the source is excluded on the destination too,
//! and never reaches a delete decision. The reference implementation gets the same
//! effect by anchoring every pattern at both roots and testing both.
//!
//! Pattern syntax reproduces Python's `fnmatch`, which the reference uses directly:
//! `*` matches across `/`, `?` matches one character, `[abc]` and `[!abc]` are
//! classes, there is no escape character, and a malformed class is literal text.
//! Matching is case-sensitive on every platform — `fnmatch` folds case on Windows,
//! which would have the two sides decide differently for the same rule.
//!
//! Anchoring is not part of the pattern text; see [`Anchor`].

// Whether a pattern matches from the start of the key or at any segment boundary.
//
// Kept out of the pattern text deliberately: in `fnmatch`, `**/logs/*` means the
// same as `*/logs/*`, since `**` is just two stars, and both need a segment before
// `logs`. Spelling "at any depth" with a leading `**/` would silently redefine a
// pattern that already has a meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Anchor {
    Root,
    Anywhere,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Include,
    Exclude,
}

#[derive(Debug, Clone)]
pub(crate) struct Rule {
    action: Action,
    pattern: String,
    anchor: Anchor,
}

impl Rule {
    pub(crate) fn include(pattern: impl Into<String>) -> Self {
        Self {
            action: Action::Include,
            pattern: pattern.into(),
            anchor: Anchor::Root,
        }
    }

    pub(crate) fn exclude(pattern: impl Into<String>) -> Self {
        Self {
            action: Action::Exclude,
            pattern: pattern.into(),
            anchor: Anchor::Root,
        }
    }

    pub(crate) fn anywhere(mut self) -> Self {
        self.anchor = Anchor::Anywhere;
        self
    }
}

// An ordered rule list. Every key starts included, each matching rule overrides the
// one before it, and the last match decides. Ordering is what expresses "all of
// `logs/` except `logs/keep/`", so the sequence is the API, not a set.
#[derive(Debug, Clone, Default)]
pub(crate) struct KeyFilter {
    rules: Vec<Rule>,
}

impl KeyFilter {
    pub(crate) fn new(rules: Vec<Rule>) -> Self {
        Self { rules }
    }

    pub(crate) fn allows(&self, _key: &str) -> bool {
        unimplemented!("S1-T11")
    }

    // Whether any key under `dir_prefix` could survive the rules, used to skip
    // descending into it. Answering `true` when nothing would survive costs a wasted
    // descent; answering `false` when something would survive loses data, so this
    // errs toward `true`.
    pub(crate) fn may_include_under(&self, _dir_prefix: &str) -> bool {
        unimplemented!("S1-T12")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(rules: Vec<Rule>) -> KeyFilter {
        KeyFilter::new(rules)
    }

    // --- Rule combination. Ported from aws-cli
    //     tests/unit/customizations/s3/test_filters.py, which is the only filter
    //     coverage sync has there; its sync functional tests have none. ---

    // test_no_filter
    #[test]
    fn no_rules_includes_everything() {
        let f = filter(vec![]);
        assert!(f.allows("a.txt"));
        assert!(f.allows("logs/deep/a.txt"));
    }

    // test_include. Worth pinning because it surprises people: every key starts
    // included, so an include-only list changes nothing.
    #[test]
    fn include_only_is_a_no_op() {
        let f = filter(vec![Rule::include("*.txt")]);
        assert!(f.allows("a.txt"));
        assert!(f.allows("a.jpg"));
    }

    // test_exclude
    #[test]
    fn exclude_everything_leaves_nothing() {
        let f = filter(vec![Rule::exclude("*")]);
        assert!(!f.allows("a.txt"));
        assert!(!f.allows("logs/deep/a.txt"));
    }

    // test_exclude_include
    #[test]
    fn exclude_then_include_keeps_what_is_re_included() {
        let f = filter(vec![Rule::exclude("*"), Rule::include("*.txt")]);
        assert!(f.allows("a.txt"));
        assert!(!f.allows("a.jpg"));
    }

    // test_include_exclude — the same two rules reversed exclude everything.
    #[test]
    fn include_then_exclude_excludes_everything() {
        let f = filter(vec![Rule::include("*.txt"), Rule::exclude("*")]);
        assert!(!f.allows("a.txt"));
        assert!(!f.allows("a.jpg"));
    }

    #[test]
    fn the_last_matching_rule_decides() {
        let f = filter(vec![
            Rule::exclude("*"),
            Rule::include("logs/*"),
            Rule::exclude("logs/secret/*"),
        ]);
        assert!(!f.allows("a.txt"));
        assert!(f.allows("logs/a.txt"));
        assert!(!f.allows("logs/secret/a.txt"));
    }

    // --- Anchoring and `*` crossing `/`. Expectations generated with Python's
    //     `fnmatch.fnmatchcase`, the reference matcher:
    //     python3 -c "from fnmatch import fnmatchcase; print(fnmatchcase(KEY, PAT))"

    // test_root_dir: with the root at `/foo/bar`, `baz.txt` excludes `baz.txt`; with
    // the root at `/foo`, that same pattern does not match `bar/baz.txt`.
    #[test]
    fn a_bare_name_matches_only_at_the_root() {
        let f = filter(vec![Rule::exclude("a.txt")]);
        assert!(!f.allows("a.txt"));
        assert!(f.allows("bar/a.txt"));
    }

    #[test]
    fn a_wildcard_crosses_slashes() {
        let f = filter(vec![Rule::exclude("*.txt")]);
        assert!(!f.allows("a.txt"));
        assert!(!f.allows("logs/a.txt"));
        assert!(!f.allows("deep/logs/a.txt"));
        assert!(f.allows("a.log"));
    }

    // Because `*` crosses `/`, one rule covers a whole subtree — which is what makes
    // pruning worth doing at all.
    #[test]
    fn a_directory_rule_covers_its_whole_subtree() {
        let f = filter(vec![Rule::exclude("logs/*")]);
        assert!(!f.allows("logs/a.txt"));
        assert!(!f.allows("logs/inner/a.txt"));
        assert!(f.allows("deep/logs/a.txt"));
    }

    // test_bucket_exclude_with_prefix
    #[test]
    fn a_partial_name_under_a_directory_matches_only_that_name() {
        let f = filter(vec![Rule::exclude("dir1/key*")]);
        assert!(!f.allows("dir1/key1.txt"));
        assert!(f.allows("dir1/notkey3.txt"));
    }

    // The trap: a leading `*/` requires a segment before the match, so it misses the
    // top level. `Anchor::Anywhere` is how to say what people usually mean.
    #[test]
    fn a_leading_wildcard_segment_skips_the_root() {
        let f = filter(vec![Rule::exclude("*/logs/*")]);
        assert!(!f.allows("deep/logs/a.txt"));
        assert!(f.allows("logs/a.txt"));
    }

    #[test]
    fn the_anywhere_anchor_matches_at_every_depth() {
        let f = filter(vec![Rule::exclude("logs/*").anywhere()]);
        assert!(!f.allows("logs/a.txt"));
        assert!(!f.allows("deep/logs/a.txt"));
        assert!(!f.allows("a/b/logs/c.txt"));
        assert!(f.allows("a.txt"));
    }

    #[test]
    fn single_character_and_class_patterns() {
        assert!(!filter(vec![Rule::exclude("?.txt")]).allows("a.txt"));
        assert!(filter(vec![Rule::exclude("?.txt")]).allows("ab.txt"));
        assert!(!filter(vec![Rule::exclude("[ab].txt")]).allows("a.txt"));
        assert!(filter(vec![Rule::exclude("[ab].txt")]).allows("c.txt"));
        assert!(!filter(vec![Rule::exclude("[a-c].txt")]).allows("b.txt"));
        // A negated class matches one character; the `*` then crosses the rest.
        assert!(!filter(vec![Rule::exclude("[!a]*.txt")]).allows("logs/a.txt"));
        assert!(filter(vec![Rule::exclude("[!a]*.txt")]).allows("a.txt"));
    }

    // fnmatch has no escape character and treats a malformed class as literal text.
    #[test]
    fn a_malformed_class_is_literal() {
        let f = filter(vec![Rule::exclude("a[b")]);
        assert!(!f.allows("a[b"));
        assert!(f.allows("ab"));
    }

    // S3 keys are case-sensitive, and a case-folding local side would decide
    // differently from the remote one for the same rule.
    #[test]
    fn matching_is_case_sensitive() {
        let f = filter(vec![Rule::exclude("logs/*")]);
        assert!(!f.allows("logs/a.txt"));
        assert!(f.allows("LOGS/a.txt"));
    }

    // --- Pruning ---

    #[test]
    fn nothing_is_pruned_without_rules() {
        assert!(filter(vec![]).may_include_under("logs/"));
    }

    #[test]
    fn a_wholly_excluded_directory_is_pruned() {
        let f = filter(vec![Rule::exclude("logs/*")]);
        assert!(!f.may_include_under("logs/"));
        assert!(f.may_include_under("data/"));
    }

    #[test]
    fn a_later_include_under_the_directory_prevents_pruning() {
        let f = filter(vec![Rule::exclude("logs/*"), Rule::include("logs/keep/*")]);
        assert!(f.may_include_under("logs/"));
    }

    // This include cannot match anything under `logs/`, so the exclude still stands.
    #[test]
    fn a_later_include_elsewhere_does_not_prevent_pruning() {
        let f = filter(vec![Rule::exclude("logs/*"), Rule::include("data/*")]);
        assert!(!f.may_include_under("logs/"));
    }

    // `*.txt` could match at any depth, so the subtree has to be walked.
    #[test]
    fn a_later_wildcard_include_prevents_pruning() {
        let f = filter(vec![Rule::exclude("*"), Rule::include("*.txt")]);
        assert!(f.may_include_under("logs/"));
    }

    // An exclude covering only some names under the directory proves nothing about
    // the rest of it.
    #[test]
    fn a_partial_exclude_does_not_prune() {
        let f = filter(vec![Rule::exclude("logs/key*")]);
        assert!(f.may_include_under("logs/"));
    }
}
