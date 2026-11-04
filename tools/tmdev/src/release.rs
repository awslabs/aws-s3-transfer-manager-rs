/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reviewed release subsets, published dependency audits, and resumable publication.
//!
//! Manifests own versions. The release record fixes the intended set. Registry
//! lookups distinguish absent versions from errors, and completed entries must
//! identify the selected clean source commit and package directory.

mod git;
mod registry;
mod requirements;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tests;
mod workspace;

use anyhow::{Context, Result, bail, ensure};
use clap::{Args as ClapArgs, CommandFactory, Parser, Subcommand};
use registry::{CratesIo, Registry};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant};
use time::{Date, OffsetDateTime};
use workspace::{Packages, Release};

/// One release operation; an absent command displays release help.
#[derive(ClapArgs)]
pub(crate) struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

/// Release operations with mutation flags explicit in the parsed arguments.
#[derive(Subcommand)]
enum Command {
    /// Generate release.toml from unpublished workspace versions.
    Prepare {
        /// Override the automatically selected UTC release tag.
        #[arg(long)]
        tag: Option<String>,
    },
    /// Check published dependency requirements throughout the workspace.
    Audit,
    /// Validate the release record and report pending versions.
    Check {
        #[command(flatten)]
        source: Source,
        /// Require the recorded tag and a clean checkout.
        #[arg(long)]
        require_tag: bool,
    },
    /// Create the recorded annotated tag at a clean source commit.
    Tag {
        /// Git ref that must resolve to the checkout commit.
        #[arg(long = "ref", default_value = "HEAD")]
        source: String,
        /// Push only the recorded tag to origin, without force.
        #[arg(long)]
        push: bool,
    },
    /// Verify or upload the recorded package versions through Cargo.
    Publish {
        #[command(flatten)]
        source: Source,
        /// Verify packages without uploading.
        #[arg(long)]
        dry_run: bool,
        /// Allow uncommitted package changes during dry-run only.
        #[arg(long, requires = "dry_run")]
        allow_dirty: bool,
    },
}

/// Source ref and report destination shared by validation and publication.
#[derive(ClapArgs)]
struct Source {
    /// Git ref that must resolve to the checkout commit.
    #[arg(long = "ref", default_value = "main")]
    source: String,
    /// JSON report path, resolved relative to the repository root.
    #[arg(long, default_value = "target/release/plan.json")]
    output: PathBuf,
}

/// Release help parser that requires no repository or registry access.
#[derive(Parser)]
#[command(
    name = "tmdev release",
    about = "Prepare, audit, check, tag, or publish workspace releases"
)]
struct Help {
    #[command(flatten)]
    args: Args,
}

/// Runs the selected operation after auditing published workspace dependencies.
///
/// Errors stop the operation. Help performs no repository or registry access.
pub(crate) fn run(args: Args) -> Result<()> {
    let Some(command) = args.command else {
        Help::command().print_help()?;
        return Ok(());
    };
    log::info!("Discovering the Cargo workspace");
    let root = git::root()?;
    let packages = workspace::packages(&root)?;
    let mut registry = CratesIo::new();
    log::info!(
        "Auditing published dependency requirements for {} workspace packages",
        packages.values().filter(|p| p.publishable).count()
    );
    audit(&packages, &mut registry)?;
    log::info!("Published workspace dependency audit passed");
    match command {
        Command::Audit => println!("Published workspace dependency requirements are current."),
        Command::Prepare { tag } => prepare(
            &root,
            &packages,
            &mut registry,
            tag.as_deref(),
            &SystemClock::new(),
        )?,
        Command::Check {
            source,
            require_tag,
        } => {
            log::info!("Validating release record and source ref {}", source.source);
            let release = Release::read(&root)?;
            let revision = git::check_source(&root, &release, &source.source, require_tag, false)?;
            let report = check(&root, &release, &packages, &mut registry, &revision)?;
            save_report(&root, &report, &source.output)?;
            print_report(&root, &report, &source.output);
        }
        Command::Tag { source, push } => {
            log::info!("Validating release record before tag creation at {source}");
            let release = Release::read(&root)?;
            let revision = git::check_source(&root, &release, &source, false, true)?;
            check(&root, &release, &packages, &mut registry, &revision)?;
            git::create_tag(&root, &release, &revision, push)?;
        }
        Command::Publish {
            source,
            dry_run,
            allow_dirty,
        } => {
            log::info!("Validating release record and source ref {}", source.source);
            let release = Release::read(&root)?;
            let revision = git::check_source(&root, &release, &source.source, !dry_run, false)?;
            publish(
                &root,
                &release,
                &packages,
                &mut registry,
                &revision,
                &PublishOptions {
                    dry_run,
                    allow_dirty,
                    output: source.output,
                },
                &mut Cargo,
                &mut SystemClock::new(),
            )?;
        }
    }
    Ok(())
}

/// Finds all occupied dependent versions whose published requirements reject
/// current local dependency versions. Unpublished and private dependents are skipped.
///
/// Registry and unevaluable-requirement errors fail the audit.
fn audit(packages: &Packages, registry: &mut dyn Registry) -> Result<()> {
    let versions: BTreeMap<_, _> = packages
        .values()
        .filter(|p| p.publishable)
        .map(|p| (p.name.clone(), p.version.clone()))
        .collect();
    let mut errors = Vec::new();
    for package in packages.values().filter(|p| p.publishable) {
        let Some(published) = registry.version(&package.name, &package.version)? else {
            log::debug!(
                "Audit {} {}: version is unpublished",
                package.name,
                package.version
            );
            continue;
        };
        log::debug!(
            "Audit {} {}: checking {} published dependencies",
            package.name,
            package.version,
            published.dependencies.len()
        );
        match requirements::stale_requirements(
            &package.name,
            &published,
            &package.dependencies,
            &versions,
        ) {
            Ok(stale) if stale.is_empty() => {}
            Ok(stale) => errors.push(
                requirements::stale_requirements_error(&package.name, &published, &stale)
                    .to_string(),
            ),
            Err(error) => errors.push(format!("{error:#}")),
        }
    }
    ensure!(
        errors.is_empty(),
        "published workspace dependency audit failed:\n{}",
        errors.join("\n")
    );
    Ok(())
}

/// Registry state for one source revision and the full reviewed release set.
///
/// Pending and completed names partition `intended`; private packages are excluded
/// from `inventory`. Completed entries have verified clean source metadata.
#[derive(Debug, Serialize)]
struct Report {
    tag: String,
    source_revision: String,
    inventory: BTreeMap<String, semver::Version>,
    intended: BTreeMap<String, semver::Version>,
    pending: Vec<String>,
    completed: Vec<String>,
}

/// Validates recorded versions and classifies them against registry state.
///
/// Occupied versions must be unyanked and identify `revision` and the package's
/// repository path. Local normal/build dependencies must be publishable and
/// included in the release or already available at their current version.
fn check(
    root: &Path,
    release: &Release,
    packages: &Packages,
    registry: &mut dyn Registry,
    revision: &str,
) -> Result<Report> {
    release.validate(packages)?;
    let mut pending = Vec::new();
    let mut completed = Vec::new();
    for (name, version) in &release.crates {
        let package = &packages[name];
        if let Some(published) = registry.version(name, version)? {
            ensure!(
                !published.yanked,
                "{name} {version} is yanked; its version cannot be reused"
            );
            let info = registry.vcs_info(name, version, &published)?;
            let path = package
                .directory
                .strip_prefix(root)
                .context("package directory is outside source root")?;
            log::debug!(
                "Verify {name} {version}: archive commit={}, dirty={}, path={:?}; expected commit={revision}, path={:?}",
                info.git.sha1,
                info.git.dirty,
                info.path_in_vcs,
                path
            );
            ensure!(
                info.git.sha1 == revision
                    && !info.git.dirty
                    && info.path_in_vcs == path.to_string_lossy().replace('\\', "/"),
                "{name} {version} is already published, but its Cargo VCS metadata does not identify this clean release commit and package directory"
            );
            log::debug!("Release {name} {version}: source verified, completed");
            completed.push(name.clone());
        } else {
            log::debug!("Release {name} {version}: unpublished, pending");
            pending.push(name.clone());
        }
        for edge in package
            .dependencies
            .iter()
            .filter(|d| d.has_path && d.propagates_bumps())
        {
            let dependency = &packages[&edge.package];
            ensure!(
                dependency.publishable,
                "{name}: required local dependency is private: {}",
                edge.package
            );
            if !release.crates.contains_key(&edge.package) {
                let available = registry.version(&edge.package, &dependency.version)?;
                ensure!(
                    available.is_some_and(|v| !v.yanked),
                    "{name}: include unpublished dependency {} {} in release.toml",
                    edge.package,
                    dependency.version
                );
            }
        }
    }
    Ok(Report {
        tag: release.tag.clone(),
        source_revision: revision.to_owned(),
        inventory: packages
            .values()
            .filter(|p| p.publishable)
            .map(|p| (p.name.clone(), p.version.clone()))
            .collect(),
        intended: release.crates.clone(),
        pending,
        completed,
    })
}

/// Writes absent current package versions under an unused UTC date tag.
///
/// An untagged record is preserved unless a different explicit tag is supplied.
/// A tagged record must be fully published before another is prepared. Remote
/// tag lookup failures and unavailable omitted dependencies prevent writing.
fn prepare(
    root: &Path,
    packages: &Packages,
    registry: &mut dyn Registry,
    explicit_tag: Option<&str>,
    clock: &dyn Clock,
) -> Result<()> {
    log::info!("Checking local and origin release tags");
    if let Some(tag) = explicit_tag {
        git::validate_tag(tag)?;
    }
    let tags = git::Tags::read(root)?;
    if root.join("release.toml").exists() {
        let existing = Release::read(root)?;
        if !tags.contains(&existing.tag) && explicit_tag.is_none_or(|tag| tag == existing.tag) {
            existing.validate(packages)?;
            println!(
                "Keeping the existing {} record; review it before tagging.",
                existing.tag
            );
            return Ok(());
        }
        if tags.contains(&existing.tag) {
            for (name, version) in &existing.crates {
                ensure!(
                    registry.version(name, version)?.is_some_and(|p| !p.yanked),
                    "complete {} before preparing another release",
                    existing.tag
                );
            }
        }
    }
    let tag = explicit_tag
        .map(str::to_owned)
        .map(Ok)
        .unwrap_or_else(|| tags.next(clock.today()?))?;
    ensure!(
        !tags.contains(&tag),
        "release tag already exists: {tag}; choose a new date or suffix"
    );
    let mut crates = BTreeMap::new();
    for package in packages.values().filter(|p| p.publishable) {
        if registry.version(&package.name, &package.version)?.is_none() {
            crates.insert(package.name.clone(), package.version.clone());
        }
    }
    if crates.is_empty() {
        println!(
            "Nothing new to release; prepare package version bumps or resume the existing record."
        );
        return Ok(());
    }
    let release = Release { tag, crates };
    check(
        root,
        &release,
        packages,
        registry,
        &git::revision(root, "HEAD")?,
    )?;
    release.write(root)?;
    println!(
        "Prepared {}: {} package versions. Review release.toml with the version and changelog changes.",
        release.tag,
        release.crates.len()
    );
    Ok(())
}

/// Persists the complete intended set and its verified registry classification.
///
/// Relative paths resolve against `root`; parent directories are created.
fn save_report(root: &Path, report: &Report, output: &Path) -> Result<()> {
    let output = root.join(output);
    fs::create_dir_all(output.parent().context("report path has no parent")?)
        .with_context(|| format!("create report directory for {}", output.display()))?;
    fs::write(
        &output,
        format!("{}\n", serde_json::to_string_pretty(report)?),
    )
    .with_context(|| format!("write release report to {}", output.display()))?;
    log::debug!("Saved release report to {}", output.display());
    Ok(())
}

/// Prints source identity, per-package state, and the persisted report path.
fn print_report(root: &Path, report: &Report, output: &Path) {
    println!("Release {} at {}", report.tag, report.source_revision);
    for (name, version) in &report.intended {
        println!(
            "  {name} {version}: {}",
            if report.completed.contains(name) {
                "completed"
            } else {
                "pending"
            }
        );
    }
    println!("Report: {}", root.join(output).display());
}

/// Builds Cargo's selected-package publication arguments without disabling verification.
///
/// An empty pending set and dirty uploads return errors. Managed HTTP selection
/// enables its client feature for packaged-code verification.
fn cargo_arguments(pending: &[String], dry_run: bool, allow_dirty: bool) -> Result<Vec<String>> {
    ensure!(
        !pending.is_empty(),
        "an empty pending set must not invoke Cargo"
    );
    ensure!(
        !allow_dirty || dry_run,
        "--allow-dirty is only supported with --dry-run"
    );
    let mut args = vec!["publish".into(), "--locked".into()];
    for package in pending {
        args.extend(["-p".into(), package.clone()]);
    }
    if pending.iter().any(|p| p == "aws-smithy-managed-http") {
        args.extend(["--features".into(), "aws-smithy-managed-http/client".into()]);
    }
    if dry_run {
        args.push("--dry-run".into());
    }
    if allow_dirty {
        args.push("--allow-dirty".into());
    }
    Ok(args)
}

/// Publishes selected versions or verifies their packaged source without upload.
///
/// A failure may follow a partial upload; it must not modify the release record.
trait Publisher {
    /// Runs publication in `root` with Cargo-compatible arguments.
    fn publish(&mut self, root: &Path, args: &[String]) -> Result<()>;
}

/// Cargo publisher that inherits subprocess output and preserves verification.
struct Cargo;
impl Publisher for Cargo {
    fn publish(&mut self, root: &Path, args: &[String]) -> Result<()> {
        log::info!("Running: cargo {}", args.join(" "));
        let status = ProcessCommand::new("cargo")
            .args(args)
            .current_dir(root)
            .status()
            .context("invoke cargo publish")?;
        ensure!(
            status.success(),
            "cargo publish failed ({status}); retry the same release tag and record"
        );
        Ok(())
    }
}

/// UTC calendar and monotonic time source for naming and bounded visibility waits.
trait Clock {
    /// Returns the current UTC calendar date.
    fn today(&self) -> Result<Date>;
    /// Returns a nondecreasing duration from a fixed origin.
    fn elapsed(&self) -> Duration;
    /// Waits for `delay`, advancing the monotonic duration.
    fn sleep(&mut self, delay: Duration);
}

/// Wall-clock UTC date paired with an elapsed monotonic timer.
struct SystemClock {
    started: Instant,
}
impl SystemClock {
    /// Starts the monotonic timer at construction.
    fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}
impl Clock for SystemClock {
    fn today(&self) -> Result<Date> {
        Ok(OffsetDateTime::now_utc().date())
    }
    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
    fn sleep(&mut self, delay: Duration) {
        std::thread::sleep(delay);
    }
}

/// Upload mode and report destination; dirty source is valid only for dry-run.
struct PublishOptions {
    dry_run: bool,
    allow_dirty: bool,
    output: PathBuf,
}
/// Maximum monotonic visibility wait after Cargo reports successful uploads.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(180);
/// Delay between registry index refreshes while uploads remain unverified.
const VISIBILITY_POLL: Duration = Duration::from_secs(5);

/// Publishes only pending entries while retaining the full reviewed release set.
///
/// A report is written before invoking the publisher. Successful uploads wait
/// for checksum/source-verified registry visibility; failures leave the record
/// unchanged for retry. Dry-run ends after Cargo verification without polling.
#[allow(clippy::too_many_arguments)]
fn publish(
    root: &Path,
    release: &Release,
    packages: &Packages,
    registry: &mut dyn Registry,
    revision: &str,
    options: &PublishOptions,
    publisher: &mut dyn Publisher,
    clock: &mut dyn Clock,
) -> Result<()> {
    let mut report = check(root, release, packages, registry, revision)?;
    save_report(root, &report, &options.output)?;
    print_report(root, &report, &options.output);
    if report.pending.is_empty() {
        println!("All intended versions are already published; no Cargo invocation is needed.");
        return Ok(());
    }
    publisher
        .publish(
            root,
            &cargo_arguments(&report.pending, options.dry_run, options.allow_dirty)?,
        )
        .with_context(|| {
            format!(
                "release {} did not complete; report: {}",
                release.tag,
                root.join(&options.output).display()
            )
        })?;
    if options.dry_run {
        println!("Dry-run package verification completed; no versions uploaded.");
        return Ok(());
    }
    log::info!(
        "Waiting up to {} seconds for verified registry visibility",
        VISIBILITY_TIMEOUT.as_secs()
    );
    let started = clock.elapsed();
    let deadline = clock.elapsed() + VISIBILITY_TIMEOUT;
    loop {
        registry.refresh();
        report = check(root, release, packages, registry, revision)?;
        save_report(root, &report, &options.output)?;
        if report.pending.is_empty() {
            print_report(root, &report, &options.output);
            println!("Release {} completed.", release.tag);
            return Ok(());
        }
        log::info!(
            "Registry visibility after {}s: {} completed, {} pending ({})",
            clock.elapsed().saturating_sub(started).as_secs(),
            report.completed.len(),
            report.pending.len(),
            report.pending.join(", ")
        );
        if clock.elapsed() >= deadline {
            bail!(
                "published versions are not all visible in the registry after {}s: {}; retry the same release tag (report: {})",
                clock.elapsed().saturating_sub(started).as_secs(),
                report.pending.join(", "),
                root.join(&options.output).display()
            );
        }
        clock.sleep(VISIBILITY_POLL);
    }
}
