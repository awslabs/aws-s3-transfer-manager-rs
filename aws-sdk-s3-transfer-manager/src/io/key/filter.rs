/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Include/exclude rules over derived keys.
//!
//! Rules match a key relative to its own root, so one rule set covers both sides of
//! a comparison: a key excluded on the source is excluded on the destination too,
//! and never reaches a delete decision.
//!
//! In a pattern, `*` matches any run of characters including `/`, `?` matches exactly
//! one, and `[abc]` or `[!abc]` match one character in or not in a set, ranges
//! included. There is no escape character, so a bracket opening no complete set is
//! literal text, and a literal `*` cannot be matched exactly. Matching is
//! case-sensitive, so both sides decide alike for the same rule.
//!
//! Anchoring is not part of the pattern text; see [`Anchor`].

// Where in a key a `Rule`'s pattern may match: from the start, or at any segment boundary.
//
// Kept out of the pattern text: `**` is just two stars, so `**/logs/*` and `*/logs/*`
// mean the same thing, and both need a segment before `logs`. Spelling "at any depth"
// that way would redefine a pattern that already has a meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Anchor {
    Root,
    Anywhere,
}

// What a matching rule does to the key it matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Include,
    Exclude,
}

// One rule of a `KeyFilter`: a pattern, what it does to a key it matches, and where it may
// match.
#[derive(Debug, Clone)]
pub(crate) struct Rule {
    action: Action,
    pattern: String,
    // The same pattern as characters, so matching a key does not re-parse it. Every entry on both
    // sides is matched against every rule, so the parse would otherwise repeat per entry per rule.
    parsed: Vec<char>,
    anchor: Anchor,
}

impl Rule {
    pub(crate) fn include(pattern: impl Into<String>) -> Self {
        let pattern: String = pattern.into();
        Self {
            action: Action::Include,
            parsed: pattern.chars().collect(),
            pattern,
            anchor: Anchor::Root,
        }
    }

    pub(crate) fn exclude(pattern: impl Into<String>) -> Self {
        let pattern: String = pattern.into();
        Self {
            action: Action::Exclude,
            parsed: pattern.chars().collect(),
            pattern,
            anchor: Anchor::Root,
        }
    }

    pub(crate) fn anywhere(mut self) -> Self {
        self.anchor = Anchor::Anywhere;
        self
    }

    fn matches(&self, key: &str, live: &mut Vec<bool>, next: &mut Vec<bool>) -> bool {
        match self.anchor {
            Anchor::Root => glob_match(&self.parsed, key, live, next),
            // Every segment start, plus the whole key, so a rule written for a
            // subtree applies at any depth including the top.
            Anchor::Anywhere => {
                if glob_match(&self.parsed, key, live, next) {
                    return true;
                }
                key.char_indices()
                    .filter(|(_, c)| *c == '/')
                    .any(|(i, _)| glob_match(&self.parsed, &key[i + 1..], live, next))
            }
        }
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

    pub(crate) fn allows(&self, key: &str) -> bool {
        // Two buffers for the whole call, rotated per character, rather than one allocation per
        // character of the key.
        let (mut live, mut next) = (Vec::new(), Vec::new());
        let mut allowed = true;
        for rule in &self.rules {
            if rule.matches(key, &mut live, &mut next) {
                allowed = rule.action == Action::Include;
            }
        }
        allowed
    }
}

// Tracks every pattern position still live and consumes the key once, so `*a*b*c*`
// against a long key costs one pass. Guessing where each `*` ends and backing up on a
// mismatch costs exponentially many splits instead.
fn glob_match(pat: &[char], key: &str, live: &mut Vec<bool>, next: &mut Vec<bool>) -> bool {
    live.clear();
    live.resize(pat.len() + 1, false);
    live[0] = true;
    advance_stars(pat, live);

    for c in key.chars() {
        next.clear();
        next.resize(pat.len() + 1, false);
        for (pos, _) in live.iter().enumerate().filter(|(_, l)| **l) {
            if pos == pat.len() {
                continue;
            }
            match pat[pos] {
                // A star also stays live, which is what lets it span `/`.
                '*' => {
                    next[pos] = true;
                    next[pos + 1] = true;
                }
                '?' => next[pos + 1] = true,
                '[' => {
                    if let Some((end, matched)) = class_match(pat, pos, c) {
                        if matched {
                            next[end + 1] = true;
                        }
                    } else if c == '[' {
                        next[pos + 1] = true;
                    }
                }
                p if p == c => next[pos + 1] = true,
                _ => {}
            }
        }
        std::mem::swap(live, next);
        advance_stars(pat, live);
        if !live.iter().any(|l| *l) {
            return false;
        }
    }
    live[pat.len()]
}

// A star can consume nothing, so any position on one is also live at the next
// position.
fn advance_stars(pat: &[char], live: &mut [bool]) {
    for pos in 0..pat.len() {
        if live[pos] && pat[pos] == '*' {
            live[pos + 1] = true;
        }
    }
}

// `None` when the `[` opens no complete class, which makes it literal text. Otherwise
// the closing bracket's index and whether `c` is in the class.
fn class_match(pat: &[char], open: usize, c: char) -> Option<(usize, bool)> {
    let mut i = open + 1;
    let negated = pat.get(i) == Some(&'!');
    if negated {
        i += 1;
    }
    // A `]` immediately after the opening bracket is a literal member.
    let first_member = i;
    let mut end = None;
    while i < pat.len() {
        if pat[i] == ']' && i > first_member {
            end = Some(i);
            break;
        }
        i += 1;
    }
    let end = end?;

    let members = &pat[first_member..end];
    let mut hit = false;
    let mut j = 0;
    while j < members.len() {
        // A `-` between two members is a range, but a trailing one is literal.
        if j + 2 < members.len() && members[j + 1] == '-' {
            if members[j] <= c && c <= members[j + 2] {
                hit = true;
            }
            j += 3;
        } else {
            if members[j] == c {
                hit = true;
            }
            j += 1;
        }
    }
    Some((end, hit != negated))
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

    // There is no escape character, and a class that never closes is literal text.
    #[test]
    fn a_malformed_class_is_literal() {
        let f = filter(vec![Rule::exclude("a[b")]);
        assert!(!f.allows("a[b"));
        assert!(f.allows("ab"));
    }

    // The same rules must give the same answers whatever order the keys arrive in, so
    // nothing may accumulate between calls.
    #[test]
    fn the_answer_for_a_key_does_not_depend_on_what_came_before() {
        let keys = [
            "a.txt",
            "logs/a.txt",
            "logs/secret/a.txt",
            "logs/inner/b.log",
            "other/c",
        ];
        let rules = || {
            vec![
                Rule::exclude("*"),
                Rule::include("logs/*"),
                Rule::exclude("logs/secret/*"),
            ]
        };

        let forward = KeyFilter::new(rules());
        let expected: Vec<bool> = keys.iter().map(|k| forward.allows(k)).collect();

        // Reversed, and each key on a filter that has seen nothing else.
        let reversed = KeyFilter::new(rules());
        let mut backward: Vec<bool> = keys.iter().rev().map(|k| reversed.allows(k)).collect();
        backward.reverse();
        assert_eq!(expected, backward, "order changed the answers");

        for (k, want) in keys.iter().zip(&expected) {
            let fresh = KeyFilter::new(rules());
            assert_eq!(fresh.allows(k), *want, "{k:?} differed on a fresh filter");
        }
    }

    // S3 keys are case-sensitive, and a case-folding local side would decide differently from the
    // remote one for the same rule. `aws s3 sync` folds case on Windows through `os.path.normcase`;
    // this deliberately does not.
    #[test]
    fn matching_is_case_sensitive() {
        let f = filter(vec![Rule::exclude("logs/*")]);
        assert!(!f.allows("logs/a.txt"));
        assert!(f.allows("LOGS/a.txt"));
    }

    // Cases the hand-written tests above do not reach: empty patterns, multiple
    // stars, and the malformed-class forms fnmatch treats as literals. Expectations
    // generated with Python's `fnmatch.fnmatchcase`, which the reference calls; the
    // same generator over a 30x26 pattern/key matrix (780 cases) also passes, so this
    // is a readable subset rather than the whole check.
    #[test]
    fn matches_the_reference_matcher_on_awkward_patterns() {
        let cases: &[(&str, &str, bool)] = &[
            ("", "", true),
            ("", "a", false),
            ("*", "", true),
            ("?", "", false),
            ("??", "ab", true),
            ("*a*b*c*", "aabbcc", true),
            ("*a*b*c*", "abc", true),
            ("*a*b*c*", "acb", false),
            ("**", "a/b", true),
            ("***", "a/b", true),
            ("a[b", "a[b", true),
            ("a[b", "ab", false),
            ("a]b", "a]b", true),
            ("[]a]", "]", true),
            ("[]a]", "a", true),
            ("[a-]", "-", true),
            ("[a-]", "a", true),
            ("x[!]]y", "xzy", true),
            ("x[!]]y", "x]y", false),
            ("[!a-c]*", "d.txt", true),
            ("[!a-c]*", "b.txt", false),
            ("*[0-9]*", "a1b", true),
            ("*[0-9]*", "ab", false),
            ("logs/*/x", "logs/inner/x", true),
            ("logs/*/x", "logs/x", false),
            ("*/", "a/", true),
            ("/*", "/a", true),
            ("*.txt", "café/a.txt", true),
        ];
        for (pattern, key, expected) in cases {
            assert_eq!(
                glob_match(
                    &pattern.chars().collect::<Vec<_>>(),
                    key,
                    &mut Vec::new(),
                    &mut Vec::new()
                ),
                *expected,
                "pattern={pattern:?} key={key:?}"
            );
        }
    }
}
