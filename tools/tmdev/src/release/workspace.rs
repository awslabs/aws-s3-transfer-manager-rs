/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Resolved Cargo workspace inventory and reviewed release records.

use super::git;
use super::registry::DependencyKind;
use anyhow::{Context, Result, bail, ensure};
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Resolved manifest dependency, identified by its key, section, and target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DependencyEdge {
    /// Manifest key; may differ from the dependency package name.
    pub alias: String,
    pub package: String,
    pub kind: DependencyKind,
    pub target: Option<String>,
    /// Whether the dependency follows a local path rather than a registry-only pin.
    pub has_path: bool,
    pub optional: bool,
}

impl DependencyEdge {
    /// Returns whether a dependency constrains consumers of the published package.
    pub(super) fn propagates_bumps(&self) -> bool {
        self.kind != DependencyKind::Dev
    }
}

/// Workspace package with its canonical source directory and resolved dependencies.
#[derive(Clone, Debug)]
pub(super) struct Package {
    pub name: String,
    pub version: Version,
    pub directory: PathBuf,
    /// Whether Cargo permits publication to crates.io.
    pub publishable: bool,
    pub dependencies: Vec<DependencyEdge>,
}

/// Workspace inventory keyed by unique Cargo package name.
pub(super) type Packages = BTreeMap<String, Package>;

/// Reviewed tag and intended package versions, independent of registry completion.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Release {
    pub tag: String,
    /// Full intended set; completed entries remain present for retries.
    pub crates: BTreeMap<String, Version>,
}

/// TOML wrapper for the required release table.
#[derive(Deserialize, Serialize)]
struct Record {
    release: Release,
}

/// Cargo metadata fields required to identify workspace members.
#[derive(Deserialize)]
struct Metadata {
    workspace_members: Vec<String>,
    packages: Vec<MetadataPackage>,
}

/// Package metadata with Cargo-resolved workspace inheritance.
#[derive(Deserialize)]
struct MetadataPackage {
    id: String,
    name: String,
    version: Version,
    manifest_path: PathBuf,
    publish: Option<Vec<String>>,
    dependencies: Vec<MetadataDependency>,
}

/// Resolved dependency metadata, including renames and target sections.
#[derive(Deserialize)]
struct MetadataDependency {
    name: String,
    rename: Option<String>,
    kind: Option<DependencyKind>,
    target: Option<String>,
    path: Option<PathBuf>,
    optional: bool,
}

/// Loads locked Cargo metadata without resolving transitive dependencies.
///
/// Returns an error for duplicate package names or non-development local
/// dependencies outside the workspace.
pub(super) fn packages(root: &Path) -> Result<Packages> {
    let output = git::capture(
        root,
        "cargo",
        &["metadata", "--format-version", "1", "--no-deps", "--locked"],
    )?;
    from_metadata(&output)
}

/// Builds the inventory from Cargo JSON, retaining workspace members only.
fn from_metadata(input: &str) -> Result<Packages> {
    let metadata: Metadata = serde_json::from_str(input).context("invalid Cargo metadata")?;
    let members: BTreeSet<_> = metadata.workspace_members.into_iter().collect();
    let entries: Vec<_> = metadata
        .packages
        .into_iter()
        .filter(|p| members.contains(&p.id))
        .collect();
    let directories: BTreeMap<_, _> = entries
        .iter()
        .map(|p| {
            let directory = p
                .manifest_path
                .parent()
                .context("Cargo manifest has no parent")?
                .canonicalize()?;
            Ok((directory, p.name.clone()))
        })
        .collect::<Result<_>>()?;
    let mut packages = Packages::new();
    for entry in entries {
        let mut dependencies = Vec::new();
        for dependency in entry.dependencies {
            let kind = dependency.kind.unwrap_or_default();
            if let Some(path) = &dependency.path
                && kind != DependencyKind::Dev
            {
                let canonical = path.canonicalize()?;
                ensure!(
                    directories.get(&canonical) == Some(&dependency.name),
                    "{}: local dependency outside workspace: {}",
                    entry.name,
                    dependency.name
                );
            }
            dependencies.push(DependencyEdge {
                alias: dependency.rename.unwrap_or_else(|| dependency.name.clone()),
                package: dependency.name,
                kind,
                target: dependency.target,
                has_path: dependency.path.is_some(),
                optional: dependency.optional,
            });
        }
        let package = Package {
            name: entry.name.clone(),
            version: entry.version,
            directory: entry
                .manifest_path
                .parent()
                .context("Cargo manifest has no parent")?
                .canonicalize()?,
            publishable: entry
                .publish
                .is_none_or(|registries| registries.iter().any(|r| r == "crates-io")),
            dependencies,
        };
        ensure!(
            packages.insert(entry.name.clone(), package).is_none(),
            "ambiguous workspace package name: {}",
            entry.name
        );
    }
    Ok(packages)
}

impl Release {
    /// Reads a nonempty record with a valid UTC date tag from `release.toml`.
    pub(super) fn read(root: &Path) -> Result<Self> {
        let contents =
            fs::read_to_string(root.join("release.toml")).context("read release.toml")?;
        let record: Record = toml::from_str(&contents).context("invalid release.toml")?;
        git::validate_tag(&record.release.tag)?;
        ensure!(
            !record.release.crates.is_empty(),
            "release.toml requires a nonempty crates table"
        );
        Ok(record.release)
    }

    /// Replaces `release.toml` with the complete intended set.
    pub(super) fn write(&self, root: &Path) -> Result<()> {
        let contents = toml::to_string_pretty(&Record {
            release: self.clone(),
        })?;
        fs::write(root.join("release.toml"), contents).context("write release.toml")
    }

    /// Requires every recorded package to be publishable at its manifest version.
    pub(super) fn validate(&self, packages: &Packages) -> Result<()> {
        for (name, version) in &self.crates {
            let Some(package) = packages.get(name).filter(|p| p.publishable) else {
                bail!("recorded package is not a publishable workspace member: {name}");
            };
            ensure!(
                &package.version == version,
                "{name}: release records {version}, but Cargo.toml declares {}",
                package.version
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::workspace;
    use super::*;

    #[test]
    fn metadata_resolves_inherited_renamed_optional_target_and_build_dependencies() {
        let fixture = workspace();
        let packages = packages(fixture.path()).unwrap();
        let b = &packages["b"];
        assert!(
            b.dependencies
                .iter()
                .any(|d| d.alias == "renamed" && d.package == "a" && d.has_path)
        );
        assert!(
            b.dependencies
                .iter()
                .any(|d| d.kind == DependencyKind::Build && d.has_path)
        );
        assert!(
            b.dependencies
                .iter()
                .any(|d| d.target.is_some() && d.has_path)
        );
        assert!(!packages["private"].publishable);
        assert_eq!(3, packages.len());
    }

    #[test]
    fn record_round_trip_and_validation() {
        let fixture = workspace();
        let release = Release {
            tag: "release-2026-11-04".into(),
            crates: BTreeMap::from([("a".into(), Version::new(0, 1, 0))]),
        };
        release.write(fixture.path()).unwrap();
        let read = Release::read(fixture.path()).unwrap();
        assert_eq!(release.crates, read.crates);
        read.validate(&packages(fixture.path()).unwrap()).unwrap();
        let mut changed = read;
        changed.crates.insert("a".into(), Version::new(0, 2, 0));
        assert!(
            changed
                .validate(&packages(fixture.path()).unwrap())
                .is_err()
        );
        fs::write(
            fixture.path().join("release.toml"),
            "[release]\ntag='release-2026-11-04'\n[release.crates]\n",
        )
        .unwrap();
        assert!(Release::read(fixture.path()).is_err());
    }
}
