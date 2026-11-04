/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::process::{Command, Output};
use tempfile::TempDir;

pub(super) fn fixture() -> TempDir {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/cli-tests");
    std::fs::create_dir_all(&parent).unwrap();
    let dir = tempfile::tempdir_in(parent).unwrap();
    std::fs::write(dir.path().join("Cargo.toml"),"[package]\nname='private-fixture'\nversion='0.1.0'\nedition='2024'\npublish=false\n[workspace]\n").unwrap();
    std::fs::create_dir(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src/lib.rs"), "").unwrap();
    std::fs::write(dir.path().join(".gitignore"), "target/\n").unwrap();
    std::fs::write(
        dir.path().join("release.toml"),
        "[release]\ntag='release-2026-11-04'\n[release.crates]\nprivate-fixture='0.1.0'\n",
    )
    .unwrap();
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.name", "CLI test"],
        vec!["config", "user.email", "cli-test@example.invalid"],
        vec!["config", "commit.gpgsign", "false"],
    ] {
        assert!(
            Command::new("git")
                .args(args)
                .current_dir(dir.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline"])
            .current_dir(dir.path())
            .output()
            .unwrap()
            .status
            .success()
    );
    for args in [vec!["add", "."], vec!["commit", "-m", "fixture"]] {
        assert!(
            Command::new("git")
                .args(args)
                .current_dir(dir.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    dir
}

pub(super) fn cli(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tmdev"))
        .args(args)
        .current_dir(root)
        .output()
        .unwrap()
}
