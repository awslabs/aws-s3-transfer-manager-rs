/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Exact registry versions and checksum-verified Cargo source metadata.

use anyhow::{Context, Result, ensure};
use flate2::read::GzDecoder;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::time::Duration;

/// Cargo dependency section; a missing registry kind denotes a normal dependency.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum DependencyKind {
    #[default]
    Normal,
    Dev,
    Build,
}

impl fmt::Display for DependencyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Normal => "normal",
            Self::Dev => "dev",
            Self::Build => "build",
        })
    }
}

/// Published dependency requirement identified by alias, kind, and target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PublishedDependency {
    /// Manifest dependency key, distinct from the package name when renamed.
    pub alias: String,
    pub package: String,
    pub requirement: String,
    pub kind: DependencyKind,
    pub target: Option<String>,
    pub optional: bool,
}

/// Exact occupied registry version and its archive checksum and dependency metadata.
#[derive(Clone, Debug)]
pub(super) struct PublishedVersion {
    pub version: String,
    /// SHA-256 of the compressed archive in lowercase hexadecimal.
    pub checksum: String,
    /// Occupied but unavailable for new dependency resolution when true.
    pub yanked: bool,
    pub dependencies: Vec<PublishedDependency>,
}

/// Source identity read from checksum-verified `.cargo_vcs_info.json`.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct VcsInfo {
    pub git: VcsGit,
    /// Package directory relative to the repository root, using forward slashes.
    pub path_in_vcs: String,
}

/// Commit and dirty-source marker captured by Cargo during packaging.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct VcsGit {
    pub sha1: String,
    #[serde(default)]
    pub dirty: bool,
}

/// Exact-version registry lookups with verifiable archive source identity.
///
/// Lookup failures must remain errors; only confirmed absence returns `None`.
pub(super) trait Registry {
    /// Returns occupied metadata, including yanked versions, or confirmed absence.
    fn version(&mut self, name: &str, version: &Version) -> Result<Option<PublishedVersion>>;
    /// Verifies the archive checksum and returns its unique Cargo source metadata.
    fn vcs_info(
        &mut self,
        name: &str,
        version: &Version,
        published: &PublishedVersion,
    ) -> Result<VcsInfo>;
    /// Invalidates cached index lookups so subsequent calls observe publication.
    fn refresh(&mut self);
}

/// Blocking crates.io client with per-package indexes and verified archive caches.
///
/// Requests have a 30-second timeout and at most three attempts for transient
/// failures. Index refresh preserves immutable archive metadata.
pub(super) struct CratesIo {
    agent: ureq::Agent,
    index: BTreeMap<String, BTreeMap<Version, PublishedVersion>>,
    vcs: BTreeMap<(String, Version), VcsInfo>,
}

impl CratesIo {
    /// Constructs an empty cache and a TLS-enabled HTTP agent.
    pub(super) fn new() -> Self {
        Self {
            agent: ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(30)))
                .build()
                .into(),
            index: BTreeMap::new(),
            vcs: BTreeMap::new(),
        }
    }

    /// Reads at most 128 MiB, retrying transport errors and transient HTTP statuses.
    ///
    /// HTTP 404 returns `None` only when `missing_ok` is true. All other exhausted
    /// or permanent errors include the requested URL.
    fn get(&self, url: &str, missing_ok: bool) -> Result<Option<Vec<u8>>> {
        for attempt in 0..3 {
            log::debug!("Registry GET {url}, attempt {} of 3", attempt + 1);
            let response = self
                .agent
                .get(url)
                .header("User-Agent", "s3-tm-tmdev")
                .call();
            match response {
                Ok(mut response) => {
                    log::debug!("Registry GET {url}: {}", response.status());
                    return response
                        .body_mut()
                        .with_config()
                        .limit(128 * 1024 * 1024)
                        .read_to_vec()
                        .map(Some)
                        .with_context(|| format!("read registry response: {url}"));
                }
                Err(ureq::Error::StatusCode(404)) if missing_ok => {
                    log::debug!("Registry GET {url}: absent (404)");
                    return Ok(None);
                }
                Err(error) => {
                    let retry = match &error {
                        ureq::Error::StatusCode(status) => {
                            matches!(status, 429 | 500 | 502 | 503 | 504)
                        }
                        _ => true,
                    };
                    if !retry || attempt == 2 {
                        return Err(error).with_context(|| {
                            format!(
                                "registry request failed: {url} after {} attempt(s)",
                                attempt + 1
                            )
                        });
                    }
                    log::warn!(
                        "Registry GET {url} failed: {error}; retrying in {}s (attempt {} of 3)",
                        attempt + 1,
                        attempt + 2
                    );
                }
            }
            std::thread::sleep(Duration::from_secs(attempt + 1));
        }
        unreachable!("each final request returns its result")
    }
}

/// One exact version in the sparse registry index.
#[derive(Deserialize)]
struct IndexVersion {
    vers: Version,
    cksum: String,
    #[serde(default)]
    yanked: bool,
    deps: Vec<IndexDependency>,
}

/// Registry dependency entry with alias and package names kept distinct.
#[derive(Deserialize)]
struct IndexDependency {
    name: String,
    package: Option<String>,
    req: String,
    kind: Option<DependencyKind>,
    target: Option<String>,
    #[serde(default)]
    optional: bool,
}

/// Returns the sparse-index path for a nonempty ASCII Cargo package name.
fn index_path(name: &str) -> String {
    let name = name.to_ascii_lowercase();
    match name.len() {
        1 | 2 => format!("{}/{name}", name.len()),
        3 => format!("3/{}/{name}", &name[..1]),
        _ => format!("{}/{}/{name}", &name[..2], &name[2..4]),
    }
}

/// Parses newline-delimited index entries, rejecting empty or duplicate versions.
fn parse_index(data: &[u8]) -> Result<BTreeMap<Version, PublishedVersion>> {
    let text = std::str::from_utf8(data)?;
    ensure!(!text.trim().is_empty(), "empty registry index response");
    let mut versions = BTreeMap::new();
    for line in text.lines() {
        let entry: IndexVersion = serde_json::from_str(line)?;
        let version = PublishedVersion {
            version: entry.vers.to_string(),
            checksum: entry.cksum,
            yanked: entry.yanked,
            dependencies: entry
                .deps
                .into_iter()
                .map(|d| PublishedDependency {
                    package: d.package.unwrap_or_else(|| d.name.clone()),
                    alias: d.name,
                    requirement: d.req,
                    kind: d.kind.unwrap_or_default(),
                    target: d.target,
                    optional: d.optional,
                })
                .collect(),
        };
        ensure!(
            versions.insert(entry.vers, version).is_none(),
            "duplicate registry version"
        );
    }
    Ok(versions)
}

/// Summarizes occupied versions and the highest non-yanked stable version.
///
/// If no stable version is available, reports the highest non-yanked prerelease
/// or that every occupied version is yanked. Version order is SemVer order.
fn index_summary(index: &BTreeMap<Version, PublishedVersion>) -> String {
    if index.is_empty() {
        return "no published versions found".into();
    }
    let stable = index
        .iter()
        .rev()
        .find(|(version, entry)| version.pre.is_empty() && !entry.yanked);
    let latest = stable.or_else(|| index.iter().rev().find(|(_, entry)| !entry.yanked));
    let available = match latest {
        Some((version, _)) => format!(
            "highest non-yanked {}: {version}",
            if version.pre.is_empty() {
                "stable version"
            } else {
                "prerelease"
            }
        ),
        None => "all versions yanked".into(),
    };
    format!("{} published versions; {available}", index.len())
}

/// Returns the archive SHA-256 in the lowercase hexadecimal registry format.
fn checksum(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Verifies the archive against its index checksum and reads Cargo source metadata.
///
/// Missing, duplicate, malformed, or oversized metadata returns an error.
fn archive_vcs(
    name: &str,
    version: &Version,
    published: &PublishedVersion,
    bytes: &[u8],
) -> Result<VcsInfo> {
    ensure!(
        checksum(bytes) == published.checksum,
        "registry archive checksum mismatch: {name} {version}"
    );
    let mut archive = tar::Archive::new(GzDecoder::new(bytes));
    let expected = format!("{name}-{version}/.cargo_vcs_info.json");
    let mut vcs = None;
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()?.as_ref() == std::path::Path::new(&expected) {
            ensure!(
                vcs.is_none(),
                "duplicate Cargo VCS metadata: {name} {version}"
            );
            ensure!(
                entry.header().size()? <= 64 * 1024,
                "oversized Cargo VCS metadata"
            );
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            vcs = Some(serde_json::from_slice(&data).context("invalid Cargo VCS metadata")?);
        }
    }
    vcs.context("missing Cargo VCS metadata")
}

impl Registry for CratesIo {
    fn version(&mut self, name: &str, version: &Version) -> Result<Option<PublishedVersion>> {
        if !self.index.contains_key(name) {
            log::info!("Reading registry index for {name}");
            let url = format!("https://index.crates.io/{}", index_path(name));
            let entries = match self.get(&url, true)? {
                Some(data) => parse_index(&data)
                    .with_context(|| format!("invalid registry index for {name}"))?,
                None => BTreeMap::new(),
            };
            log::info!("Registry {name}: {}", index_summary(&entries));
            self.index.insert(name.to_owned(), entries);
        }
        let published = self.index[name].get(version).cloned();
        log::info!(
            "Registry {name} {version}: {}",
            match &published {
                Some(entry) if entry.yanked => "published (yanked)",
                Some(_) => "published",
                None => "not published",
            }
        );
        Ok(published)
    }

    fn vcs_info(
        &mut self,
        name: &str,
        version: &Version,
        published: &PublishedVersion,
    ) -> Result<VcsInfo> {
        let key = (name.to_owned(), version.clone());
        if !self.vcs.contains_key(&key) {
            log::info!("Verifying registry archive for {name} {version}");
            let url = format!("https://static.crates.io/crates/{name}/{name}-{version}.crate");
            let data = self.get(&url, false)?.context("missing registry archive")?;
            let info = archive_vcs(name, version, published, &data)
                .with_context(|| format!("unverifiable Cargo VCS metadata: {name} {version}"))?;
            log::debug!(
                "Archive {name} {version}: checksum verified, commit={}, dirty={}, path={:?}",
                info.git.sha1,
                info.git.dirty,
                info.path_in_vcs
            );
            self.vcs.insert(key.clone(), info);
        }
        self.vcs
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing cached VCS metadata"))
    }

    fn refresh(&mut self) {
        self.index.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{archive, published};
    use super::*;

    #[test]
    fn index_keeps_renames_kinds_targets_and_yanked_versions() {
        let data = br#"{"vers":"0.1.0","cksum":"check","yanked":true,"deps":[{"name":"alias","package":"a","req":"^0.1","kind":"build","target":"cfg(unix)","optional":true}]}"#;
        let entries = parse_index(data).unwrap();
        let p = &entries[&Version::new(0, 1, 0)];
        assert!(p.yanked);
        let d = &p.dependencies[0];
        assert_eq!("alias", d.alias);
        assert_eq!("a", d.package);
        assert_eq!(DependencyKind::Build, d.kind);
        assert!(d.optional);
        assert_eq!(Some("cfg(unix)"), d.target.as_deref());
        assert!(parse_index(b"").is_err());
        assert!(parse_index(b"invalid").is_err());
    }

    #[test]
    fn archive_requires_matching_checksum_and_valid_vcs_entry() {
        let bytes = archive(
            "component",
            br#"{"git":{"sha1":"abc"},"path_in_vcs":"component"}"#,
        );
        let mut p = published("0.1.0");
        p.checksum = checksum(&bytes);
        assert_eq!(
            "abc",
            archive_vcs("component", &Version::new(0, 1, 0), &p, &bytes)
                .unwrap()
                .git
                .sha1
        );
        assert!(archive_vcs("other", &Version::new(0, 1, 0), &p, &bytes).is_err());
        p.checksum = "wrong".into();
        assert!(
            archive_vcs("component", &Version::new(0, 1, 0), &p, &bytes)
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn index_404_is_missing_but_forbidden_and_malformed_responses_fail() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        for (status, missing) in [(404, true), (403, false)] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/index", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = [0; 4096];
                assert!(stream.read(&mut request).unwrap() > 0);
                write!(
                    stream,
                    "HTTP/1.1 {status} Failure\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            });
            let response = CratesIo::new().get(&url, true);
            server.join().unwrap();
            if missing {
                assert!(response.unwrap().is_none());
            } else {
                assert!(response.is_err());
            }
        }
        assert!(parse_index(br#"{"vers":"0.1.0"}"#).is_err());
    }

    #[test]
    fn index_summary_distinguishes_absence_stable_prerelease_and_yanked_versions() {
        let mut index = BTreeMap::new();
        assert_eq!("no published versions found", index_summary(&index));
        for text in ["0.1.0-rc.1", "0.1.0-rc.2"] {
            index.insert(Version::parse(text).unwrap(), published(text));
        }
        assert_eq!(
            "2 published versions; highest non-yanked prerelease: 0.1.0-rc.2",
            index_summary(&index)
        );
        for text in ["0.1.0", "0.2.0", "0.3.0-rc.1"] {
            index.insert(Version::parse(text).unwrap(), published(text));
        }
        assert_eq!(
            "5 published versions; highest non-yanked stable version: 0.2.0",
            index_summary(&index)
        );
        index.get_mut(&Version::new(0, 2, 0)).unwrap().yanked = true;
        assert_eq!(
            "5 published versions; highest non-yanked stable version: 0.1.0",
            index_summary(&index)
        );
        for entry in index.values_mut() {
            entry.yanked = true;
        }
        assert_eq!(
            "5 published versions; all versions yanked",
            index_summary(&index)
        );
    }

    #[test]
    fn sparse_index_paths() {
        for (name, path) in [
            ("a", "1/a"),
            ("ab", "2/ab"),
            ("Abc", "3/a/abc"),
            ("component", "co/mp/component"),
        ] {
            assert_eq!(path, index_path(name));
        }
    }
}
