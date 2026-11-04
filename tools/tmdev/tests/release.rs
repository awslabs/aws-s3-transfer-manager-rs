/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod test_util;
use test_util::{cli, fixture};

#[test]
fn top_level_and_release_help_work_without_repository_or_registry() {
    let root = fixture();
    for args in [&[][..], &["release"][..]] {
        let result = cli(root.path(), args);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("release")
                || String::from_utf8_lossy(&result.stdout).contains("prepare")
        );
    }
}

#[test]
fn real_metadata_and_audit_exclude_private_packages_and_check_rejects_record() {
    let root = fixture();
    let result = cli(root.path(), &["release", "audit"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("requirements are current"));
    let result = cli(root.path(), &["release", "check", "--ref", "HEAD"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("not a publishable workspace member"));
    assert!(!root.path().join("target/release/plan.json").exists());
}

#[test]
fn dirty_upload_option_is_rejected_before_repository_discovery() {
    let root = fixture();
    let result = cli(root.path(), &["release", "publish", "--allow-dirty"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("--dry-run"));
}

#[test]
fn tag_command_rejects_dirty_source_before_any_tag_mutation() {
    let root = fixture();
    std::fs::write(root.path().join("dirty"), "changed").unwrap();
    let result = cli(root.path(), &["release", "tag", "--ref", "HEAD"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("clean worktree"));
    let tags = std::process::Command::new("git")
        .arg("tag")
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(tags.status.success());
    assert!(tags.stdout.is_empty());
}

#[test]
fn diagnostics_report_phases_and_verbose_commands_on_stderr() {
    let root = fixture();
    for verbose in [false, true] {
        let args = if verbose {
            vec!["release", "audit", "--verbose"]
        } else {
            vec!["release", "audit"]
        };
        let result = cli(root.path(), &args);
        let stderr = String::from_utf8_lossy(&result.stderr);
        let stdout = String::from_utf8_lossy(&result.stdout);
        assert!(result.status.success(), "{stderr}");
        for line in stderr.lines().filter(|line| line.contains("tmdev: ")) {
            let timestamp = line.split_once(" tmdev: ").unwrap().0;
            let parsed = time::OffsetDateTime::parse(
                timestamp,
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap();
            assert_eq!(time::UtcOffset::UTC, parsed.offset());
            assert!(timestamp.ends_with('Z'));
        }
        assert!(stderr.contains("INFO: Discovering the Cargo workspace"));
        assert!(stderr.contains("INFO: Auditing published dependency requirements"));
        assert!(stderr.contains("INFO: Published workspace dependency audit passed"));
        assert_eq!(verbose, stderr.contains("DEBUG:"));
        if verbose {
            assert!(stderr.contains("Run git"));
            assert!(stderr.contains("Run cargo"));
            assert!(stderr.contains("metadata"));
            assert!(stderr.contains("stdout bytes"));
        }
        assert!(!stdout.contains("tmdev:"));
        assert!(stdout.contains("requirements are current"));
    }
}

#[test]
fn command_failure_reports_status_directory_and_captured_error() {
    let root = fixture();
    std::fs::write(root.path().join("Cargo.toml"), "[package\n").unwrap();
    let result = cli(root.path(), &["release", "audit"]);
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("cargo"));
    assert!(stderr.contains("failed (exit status:"));
    assert!(stderr.contains(&root.path().display().to_string()));
    assert!(stderr.contains("stdout:"));
    assert!(stderr.contains("stderr:"));
    assert!(stderr.contains("Cargo.toml"));
    assert!(stderr.contains("tmdev: ERROR:"));
}
