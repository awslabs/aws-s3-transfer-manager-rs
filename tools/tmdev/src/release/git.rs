/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Source identity and immutable local/remote release tags.

use super::workspace::Release;
use anyhow::{Context, Result, bail, ensure};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use time::{Date, format_description};

/// Runs a command in `root` and returns trimmed UTF-8 stdout.
///
/// Failure includes exit status and both captured streams. Successful stderr is
/// retained in verbose diagnostics; subprocess environments are never printed.
pub(super) fn capture(root: &Path, program: &str, args: &[&str]) -> Result<String> {
    log::debug!("Run {program} {args:?} in {}", root.display());
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .with_context(|| format!("invoke {program} in {}", root.display()))?;
    log::debug!(
        "{program} completed ({}), {} stdout bytes, {} stderr bytes",
        output.status,
        output.stdout.len(),
        output.stderr.len()
    );
    ensure!(
        output.status.success(),
        "{program} {args:?} failed ({}) in {}:\nstdout:\n{}\nstderr:\n{}",
        output.status,
        root.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.stderr.is_empty() {
        log::debug!(
            "{program} stderr: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)
        .context("command output is not UTF-8")?
        .trim()
        .to_owned())
}

/// Returns the canonical repository root containing the process working directory.
pub(super) fn root() -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    PathBuf::from(capture(&cwd, "git", &["rev-parse", "--show-toplevel"])?)
        .canonicalize()
        .context("resolve repository root")
}

/// Resolves a ref to its commit, failing for missing refs or non-commit objects.
pub(super) fn revision(root: &Path, source: &str) -> Result<String> {
    capture(
        root,
        "git",
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{source}^{{commit}}"),
        ],
    )
}

/// Requires `source` to resolve to checkout HEAD and returns that commit.
///
/// `require_tag` additionally requires the recorded tag and a clean checkout.
/// `require_clean` enforces cleanliness without restricting the ref name.
pub(super) fn check_source(
    root: &Path,
    release: &Release,
    source: &str,
    require_tag: bool,
    require_clean: bool,
) -> Result<String> {
    let head = revision(root, "HEAD")?;
    ensure!(
        revision(root, source)? == head,
        "requested ref {source} does not resolve to the checkout's HEAD"
    );
    if require_tag {
        ensure!(
            source == release.tag || source == format!("refs/tags/{}", release.tag),
            "publication requires the recorded tag: {}",
            release.tag
        );
        ensure!(
            revision(root, &format!("refs/tags/{}", release.tag))? == head,
            "recorded release tag does not resolve to HEAD"
        );
    }
    if require_clean || require_tag {
        ensure!(
            capture(root, "git", &["status", "--porcelain"])?.is_empty(),
            "publication and tagging require a clean worktree"
        );
    }
    log::debug!(
        "Source {source}: validated checkout commit {head}, require_tag={require_tag}, require_clean={require_clean}"
    );
    Ok(head)
}

/// Parses a real UTC date tag and its numeric same-day ordinal.
///
/// The unsuffixed tag has ordinal one; explicit suffixes start at two and cannot
/// contain leading zeroes.
pub(super) fn validate_tag(tag: &str) -> Result<(Date, u64)> {
    let text = tag
        .strip_prefix("release-")
        .context("release tag must be release-YYYY-MM-DD, optionally followed by .2, .3, ...")?;
    let (date, suffix) = match text.split_once('.') {
        Some((date, suffix)) => {
            ensure!(
                !suffix.starts_with('0') && suffix.bytes().all(|b| b.is_ascii_digit()),
                "invalid release suffix: {tag}"
            );
            let suffix: u64 = suffix.parse().context("invalid release suffix")?;
            ensure!(
                suffix >= 2,
                "additional releases on the same day start at suffix .2"
            );
            (date, suffix)
        }
        None => (text, 1),
    };
    ensure!(
        date.len() == 10
            && date.as_bytes()[4] == b'-'
            && date.as_bytes()[7] == b'-'
            && date
                .bytes()
                .enumerate()
                .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit()),
        "invalid release date: {tag}"
    );
    let format = format_description::parse_borrowed::<2>("[year]-[month]-[day]")?;
    let parsed =
        Date::parse(date, &format).with_context(|| format!("invalid release date: {tag}"))?;
    Ok((parsed, suffix))
}

/// Local and origin release tag names mapped to peeled commit identities.
#[derive(Default)]
pub(super) struct Tags {
    pub local: BTreeMap<String, String>,
    pub remote: BTreeMap<String, String>,
}

impl Tags {
    /// Reads both local tags and origin tags without fetching; remote errors fail.
    pub(super) fn read(root: &Path) -> Result<Self> {
        let mut local = BTreeMap::new();
        for tag in capture(
            root,
            "git",
            &["for-each-ref", "--format=%(refname:strip=2)", "refs/tags/"],
        )?
        .lines()
        {
            if validate_tag(tag).is_ok() {
                local.insert(tag.to_owned(), revision(root, &format!("refs/tags/{tag}"))?);
            }
        }
        let listing = capture(root, "git", &["ls-remote", "--tags", "origin"])?;
        let remote = remote_tags(&listing)?;
        log::debug!(
            "Release tags: {} local, {} on origin",
            local.len(),
            remote.len()
        );
        Ok(Self { local, remote })
    }

    /// Returns whether either namespace occupies the tag name.
    pub(super) fn contains(&self, tag: &str) -> bool {
        self.local.contains_key(tag) || self.remote.contains_key(tag)
    }

    /// Returns the next unused date tag after the highest local or remote ordinal.
    pub(super) fn next(&self, today: Date) -> Result<String> {
        let format = format_description::parse_borrowed::<2>("[year]-[month]-[day]")?;
        let date = today.format(&format)?;
        let max = self
            .local
            .keys()
            .chain(self.remote.keys())
            .filter_map(|tag| validate_tag(tag).ok())
            .filter(|(day, _)| *day == today)
            .map(|(_, suffix)| suffix)
            .max();
        match max {
            None => Ok(format!("release-{date}")),
            Some(suffix) => Ok(format!(
                "release-{date}.{}",
                suffix.checked_add(1).context("release suffix overflow")?
            )),
        }
    }
}

/// Parses remote release refs, preferring peeled commits over annotated objects.
fn remote_tags(listing: &str) -> Result<BTreeMap<String, String>> {
    let mut tags = BTreeMap::new();
    let mut peeled = BTreeMap::new();
    for line in listing.lines() {
        let (revision, reference) = line
            .split_once('\t')
            .context("invalid git ls-remote tag output")?;
        let Some(name) = reference.strip_prefix("refs/tags/") else {
            continue;
        };
        if let Some(name) = name.strip_suffix("^{}") {
            if validate_tag(name).is_ok() {
                peeled.insert(name.to_owned(), revision.to_owned());
            }
        } else if validate_tag(name).is_ok() {
            tags.insert(name.to_owned(), revision.to_owned());
        }
    }
    // Annotated tag object ids are not source commits; the peeled entry is authoritative.
    tags.extend(peeled);
    Ok(tags)
}

/// Creates the recorded annotated tag at `head` and optionally pushes its exact ref.
///
/// Existing local or origin tags must identify the same commit. Conflicts return
/// errors without moving tags; push never forces or includes unrelated refs.
pub(super) fn create_tag(root: &Path, release: &Release, head: &str, push: bool) -> Result<()> {
    log::info!("Checking release tag {} on origin and locally", release.tag);
    let tags = Tags::read(root)?;
    if let Some(remote) = tags.remote.get(&release.tag) {
        ensure!(
            remote == head,
            "remote tag {} already identifies another commit",
            release.tag
        );
    }
    if let Some(local) = tags.local.get(&release.tag) {
        ensure!(
            local == head,
            "local tag {} already identifies another commit",
            release.tag
        );
        println!("Local tag {} already identifies {head}", release.tag);
    } else {
        capture(
            root,
            "git",
            &[
                "tag",
                "-a",
                &release.tag,
                "-m",
                &format!("Release {}", release.tag),
                head,
            ],
        )?;
        println!("Created local tag {} at {head}", release.tag);
    }
    if push {
        if tags.remote.contains_key(&release.tag) {
            println!("Origin tag {} already identifies {head}", release.tag);
        } else {
            let refspec = format!("refs/tags/{0}:refs/tags/{0}", release.tag);
            log::info!("Pushing tag {} to origin", release.tag);
            log::debug!("Run git push origin {refspec} in {}", root.display());
            let status = Command::new("git")
                .args(["push", "origin", &refspec])
                .current_dir(root)
                .status()
                .context("invoke git tag push")?;
            if !status.success() {
                bail!(
                    "push of {} failed ({status}); inspect origin and retry without moving the tag",
                    release.tag
                );
            }
            println!("Pushed {} at {head}", release.tag);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::test_util::repo;
    use super::*;

    #[test]
    fn calendar_and_suffix_validation() {
        for tag in [
            "v0.4.0",
            "release-2026-02-30",
            "release-2026-11-04.1",
            "release-2026-11-04.02",
            "release-2026-11-04x2",
            "release-2026-11-04.",
        ] {
            assert!(validate_tag(tag).is_err(), "{tag}");
        }
        for tag in [
            "release-2026-11-04",
            "release-2026-11-04.2",
            "release-2026-11-04.10",
        ] {
            validate_tag(tag).unwrap();
        }
    }

    #[test]
    fn suffix_selection_uses_remote_tags_and_numeric_order() {
        let mut tags = Tags::default();
        let today = validate_tag("release-2026-11-04").unwrap().0;
        assert_eq!("release-2026-11-04", tags.next(today).unwrap());
        tags.remote.insert("release-2026-11-04".into(), "a".into());
        assert_eq!("release-2026-11-04.2", tags.next(today).unwrap());
        tags.local.insert("release-2026-11-04.9".into(), "a".into());
        tags.remote
            .insert("release-2026-11-03.20".into(), "a".into());
        assert_eq!("release-2026-11-04.10", tags.next(today).unwrap());
    }

    #[test]
    fn source_requires_exact_recorded_tag_and_clean_head() {
        let fixture = repo();
        let release = Release::read(fixture.path()).unwrap();
        let head = revision(fixture.path(), "HEAD").unwrap();
        create_tag(fixture.path(), &release, &head, false).unwrap();
        check_source(fixture.path(), &release, &release.tag, true, false).unwrap();
        assert!(check_source(fixture.path(), &release, "main", true, false).is_err());
        std::fs::write(fixture.path().join("dirty"), "changed").unwrap();
        assert!(check_source(fixture.path(), &release, &release.tag, true, false).is_err());
        check_source(fixture.path(), &release, "HEAD", false, false).unwrap();
        capture(fixture.path(), "git", &["add", "dirty"]).unwrap();
        capture(fixture.path(), "git", &["commit", "-m", "change"]).unwrap();
        assert!(check_source(fixture.path(), &release, &release.tag, true, false).is_err());
    }

    #[test]
    fn tags_are_idempotent_and_push_only_recorded_ref() {
        let fixture = repo();
        let release = Release::read(fixture.path()).unwrap();
        let head = revision(fixture.path(), "HEAD").unwrap();
        capture(fixture.path(), "git", &["tag", "unrelated"]).unwrap();
        create_tag(fixture.path(), &release, &head, true).unwrap();
        create_tag(fixture.path(), &release, &head, true).unwrap();
        let remote = capture(fixture.path(), "git", &["ls-remote", "--tags", "origin"]).unwrap();
        assert!(remote.contains(&release.tag));
        assert!(!remote.contains("unrelated"));
        let tags = Tags::read(fixture.path()).unwrap();
        assert_eq!(head, tags.remote[&release.tag]);
        assert_eq!(
            "tag",
            capture(
                fixture.path(),
                "git",
                &["cat-file", "-t", &format!("refs/tags/{}", release.tag)]
            )
            .unwrap()
        );
        std::fs::write(fixture.path().join("change"), "changed").unwrap();
        capture(fixture.path(), "git", &["add", "change"]).unwrap();
        capture(fixture.path(), "git", &["commit", "-m", "change"]).unwrap();
        assert!(
            create_tag(
                fixture.path(),
                &release,
                &revision(fixture.path(), "HEAD").unwrap(),
                true
            )
            .is_err()
        );
    }

    #[test]
    fn remote_only_release_tags_affect_next_name_without_fetching() {
        let fixture = repo();
        let release = Release::read(fixture.path()).unwrap();
        let head = revision(fixture.path(), "HEAD").unwrap();
        create_tag(fixture.path(), &release, &head, true).unwrap();
        capture(fixture.path(), "git", &["tag", "-d", &release.tag]).unwrap();
        let tags = Tags::read(fixture.path()).unwrap();
        assert!(!tags.local.contains_key(&release.tag));
        assert_eq!(head, tags.remote[&release.tag]);
        assert_eq!(
            "release-2026-11-04.2",
            tags.next(validate_tag(&release.tag).unwrap().0).unwrap()
        );
    }

    #[test]
    fn remote_peeled_commit_overrides_annotated_object_and_lookup_errors_fail() {
        let listing = "object\trefs/tags/release-2026-11-04\ncommit\trefs/tags/release-2026-11-04^{}\nother\trefs/tags/unrelated\n";
        assert_eq!(
            "commit",
            remote_tags(listing).unwrap()["release-2026-11-04"]
        );
        let fixture = repo();
        capture(
            fixture.path(),
            "git",
            &[
                "remote",
                "set-url",
                "origin",
                "/nonexistent-release-test-origin",
            ],
        )
        .unwrap();
        assert!(Tags::read(fixture.path()).is_err());
    }
}
