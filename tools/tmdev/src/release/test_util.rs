/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Fixtures for release policy, source checks, and publication failures.

use super::registry::{PublishedVersion, VcsGit, VcsInfo};
use super::*;
use std::cell::RefCell;
use std::rc::Rc;
use tempfile::TempDir;

pub(super) fn temporary() -> TempDir {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release-tests");
    fs::create_dir_all(&path).unwrap();
    tempfile::tempdir_in(path).unwrap()
}

pub(super) fn workspace() -> TempDir {
    let dir = temporary();
    fs::write(
        dir.path().join("Cargo.toml"),
        r#"[workspace]
resolver="3"
members=["a","b","private"]
[workspace.dependencies]
a={path="a",version="0.1.0"}
renamed={path="a",package="a",version="0.1.0"}
"#,
    )
    .unwrap();
    for name in ["a", "b", "private"] {
        fs::create_dir_all(dir.path().join(name).join("src")).unwrap();
        fs::write(
            dir.path().join(name).join("src/lib.rs"),
            "pub fn value() {}\n",
        )
        .unwrap();
        let extra = if name == "b" {
            "[dependencies]\nrenamed={workspace=true,optional=true}\n[build-dependencies]\na={workspace=true}\n[target.'cfg(unix)'.dependencies]\na={workspace=true}\n"
        } else {
            ""
        };
        fs::write(
            dir.path().join(name).join("Cargo.toml"),
            format!(
                "[package]\nname=\"{name}\"\nversion=\"0.1.0\"\nedition=\"2024\"\n{}\n{extra}",
                if name == "private" {
                    "publish=false"
                } else {
                    ""
                }
            ),
        )
        .unwrap();
    }
    fs::write(dir.path().join(".gitignore"), "target/\n").unwrap();
    git::capture(dir.path(), "cargo", &["generate-lockfile", "--offline"]).unwrap();
    dir
}

pub(super) struct Repo {
    root: TempDir,
    _origin: TempDir,
}
impl Repo {
    pub(super) fn path(&self) -> &Path {
        self.root.path()
    }
}

pub(super) fn repo() -> Repo {
    let root = workspace();
    let origin = temporary();
    git::capture(origin.path(), "git", &["init", "--bare"]).unwrap();
    git::capture(root.path(), "git", &["init", "-b", "main"]).unwrap();
    for (key, value) in [
        ("user.name", "Release test"),
        ("user.email", "release-test@example.invalid"),
        ("commit.gpgsign", "false"),
        ("tag.gpgsign", "false"),
    ] {
        git::capture(root.path(), "git", &["config", key, value]).unwrap();
    }
    git::capture(
        root.path(),
        "git",
        &["remote", "add", "origin", origin.path().to_str().unwrap()],
    )
    .unwrap();
    Release {
        tag: "release-2026-11-04".into(),
        crates: BTreeMap::from([
            ("a".into(), semver::Version::new(0, 1, 0)),
            ("b".into(), semver::Version::new(0, 1, 0)),
        ]),
    }
    .write(root.path())
    .unwrap();
    git::capture(root.path(), "git", &["add", "."]).unwrap();
    git::capture(root.path(), "git", &["commit", "-m", "fixture"]).unwrap();
    Repo {
        root,
        _origin: origin,
    }
}

pub(super) fn published(version: &str) -> PublishedVersion {
    PublishedVersion {
        version: version.into(),
        checksum: String::new(),
        yanked: false,
        dependencies: Vec::new(),
    }
}

#[derive(Default)]
pub(super) struct RegistryState {
    pub versions: BTreeMap<(String, semver::Version), PublishedVersion>,
    pub vcs: BTreeMap<(String, semver::Version), VcsInfo>,
    pub refreshes: usize,
    pub fail: bool,
    pub delayed: Vec<(usize, String, String)>,
}

#[derive(Clone, Default)]
pub(super) struct FakeRegistry(pub Rc<RefCell<RegistryState>>);
impl FakeRegistry {
    pub(super) fn complete(&self, name: &str, revision: &str) {
        let key = (name.to_owned(), semver::Version::new(0, 1, 0));
        let mut state = self.0.borrow_mut();
        state.versions.insert(key.clone(), published("0.1.0"));
        state.vcs.insert(
            key,
            VcsInfo {
                git: VcsGit {
                    sha1: revision.into(),
                    dirty: false,
                },
                path_in_vcs: name.into(),
            },
        );
    }
}
impl Registry for FakeRegistry {
    fn version(
        &mut self,
        name: &str,
        version: &semver::Version,
    ) -> Result<Option<PublishedVersion>> {
        let state = self.0.borrow();
        ensure!(!state.fail, "registry lookup failed");
        Ok(state
            .versions
            .get(&(name.to_owned(), version.clone()))
            .cloned())
    }
    fn vcs_info(
        &mut self,
        name: &str,
        version: &semver::Version,
        _: &PublishedVersion,
    ) -> Result<VcsInfo> {
        self.0
            .borrow()
            .vcs
            .get(&(name.to_owned(), version.clone()))
            .cloned()
            .context("missing Cargo VCS metadata")
    }
    fn refresh(&mut self) {
        let ready = {
            let mut state = self.0.borrow_mut();
            state.refreshes += 1;
            let mut ready = Vec::new();
            let count = state.refreshes;
            state.delayed.retain(|(after, name, revision)| {
                if *after <= count {
                    ready.push((name.clone(), revision.clone()));
                    false
                } else {
                    true
                }
            });
            ready
        };
        for (name, revision) in ready {
            self.complete(&name, &revision);
        }
    }
}

pub(super) struct FakeClock {
    pub date: Date,
    pub elapsed: Duration,
}
impl Default for FakeClock {
    fn default() -> Self {
        Self {
            date: git::validate_tag("release-2026-11-04").unwrap().0,
            elapsed: Duration::ZERO,
        }
    }
}
impl Clock for FakeClock {
    fn today(&self) -> Result<Date> {
        Ok(self.date)
    }
    fn elapsed(&self) -> Duration {
        self.elapsed
    }
    fn sleep(&mut self, delay: Duration) {
        self.elapsed += delay;
    }
}

#[derive(Default)]
pub(super) struct FakePublisher {
    pub calls: Vec<Vec<String>>,
    pub registry: FakeRegistry,
    pub revision: String,
    pub fail_after_first: bool,
    pub visibility_delay: usize,
}
impl Publisher for FakePublisher {
    fn publish(&mut self, _: &Path, args: &[String]) -> Result<()> {
        self.calls.push(args.to_vec());
        if args.iter().any(|a| a == "--dry-run") {
            return Ok(());
        }
        for pair in args.windows(2).filter(|p| p[0] == "-p") {
            let name = &pair[1];
            if self.visibility_delay == 0 {
                self.registry.complete(name, &self.revision);
            } else {
                self.registry.0.borrow_mut().delayed.push((
                    self.visibility_delay,
                    name.clone(),
                    self.revision.clone(),
                ));
            }
            ensure!(
                !self.fail_after_first,
                "simulated Cargo failure after one upload"
            );
        }
        Ok(())
    }
}

pub(super) fn archive(name: &str, content: &[u8]) -> Vec<u8> {
    let gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(gzip);
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    archive
        .append_data(
            &mut header,
            format!("{name}-0.1.0/.cargo_vcs_info.json"),
            content,
        )
        .unwrap();
    archive.into_inner().unwrap().finish().unwrap()
}

pub(super) fn options(dry_run: bool) -> PublishOptions {
    PublishOptions {
        dry_run,
        allow_dirty: false,
        output: "target/release/report.json".into(),
    }
}
