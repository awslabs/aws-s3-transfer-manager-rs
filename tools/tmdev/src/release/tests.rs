/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::registry::{DependencyKind, PublishedDependency};
use super::test_util::*;
use super::*;

#[test]
fn private_versions_missing_peers_and_conflicting_sources_fail() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let mut release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    let mut registry = FakeRegistry::default();
    let report = check(root, &release, &packages, &mut registry, &head).unwrap();
    assert_eq!(vec!["a", "b"], report.pending);
    assert!(!report.inventory.contains_key("private"));
    registry.complete("a", &head);
    assert_eq!(
        vec!["a"],
        check(root, &release, &packages, &mut registry, &head)
            .unwrap()
            .completed
    );
    registry.complete("a", "another-commit");
    assert!(
        check(root, &release, &packages, &mut registry, &head)
            .unwrap_err()
            .to_string()
            .contains("already published")
    );
    release.crates.remove("a");
    registry.0.borrow_mut().versions.clear();
    assert!(
        check(root, &release, &packages, &mut registry, &head)
            .unwrap_err()
            .to_string()
            .contains("include unpublished dependency")
    );
    registry.complete("a", "older-release");
    assert_eq!(
        vec!["b"],
        check(root, &release, &packages, &mut registry, &head)
            .unwrap()
            .pending
    );
    let mut private = release.clone();
    private
        .crates
        .insert("private".into(), semver::Version::new(0, 1, 0));
    assert!(private.validate(&packages).is_err());
    let mut packages = packages;
    packages.get_mut("a").unwrap().publishable = false;
    assert!(
        check(root, &release, &packages, &mut registry, &head)
            .unwrap_err()
            .to_string()
            .contains("required local dependency is private")
    );
}

#[test]
fn yanked_dirty_wrong_directory_missing_metadata_and_registry_errors_fail() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    let key = ("a".to_owned(), semver::Version::new(0, 1, 0));
    for defect in ["yanked", "dirty", "path", "missing", "network"] {
        let mut registry = FakeRegistry::default();
        registry.complete("a", &head);
        {
            let mut state = registry.0.borrow_mut();
            match defect {
                "yanked" => state.versions.get_mut(&key).unwrap().yanked = true,
                "dirty" => state.vcs.get_mut(&key).unwrap().git.dirty = true,
                "path" => state.vcs.get_mut(&key).unwrap().path_in_vcs = "wrong".into(),
                "missing" => {
                    state.vcs.clear();
                }
                "network" => state.fail = true,
                _ => unreachable!(),
            }
        }
        assert!(
            check(root, &release, &packages, &mut registry, &head).is_err(),
            "{defect}"
        );
    }
}

#[test]
fn completed_release_skips_cargo_and_native_arguments_preserve_verification() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    let mut registry = FakeRegistry::default();
    registry.complete("a", &head);
    registry.complete("b", &head);
    let mut publisher = FakePublisher::default();
    publish(
        root,
        &release,
        &packages,
        &mut registry,
        &head,
        &options(false),
        &mut publisher,
        &mut FakeClock::default(),
    )
    .unwrap();
    assert!(publisher.calls.is_empty());
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("target/release/report.json")).unwrap())
            .unwrap();
    assert_eq!(serde_json::json!([]), report["pending"]);
    let args = cargo_arguments(
        &["aws-smithy-managed-http".into(), "other".into()],
        true,
        true,
    )
    .unwrap();
    assert_eq!(
        vec![
            "publish",
            "--locked",
            "-p",
            "aws-smithy-managed-http",
            "-p",
            "other",
            "--features",
            "aws-smithy-managed-http/client",
            "--dry-run",
            "--allow-dirty"
        ],
        args
    );
    assert!(
        !cargo_arguments(&["other".into()], false, false)
            .unwrap()
            .iter()
            .any(|v| v == "--no-verify" || v == "--token")
    );
    assert!(cargo_arguments(&[], true, false).is_err());
    assert!(cargo_arguments(&["other".into()], false, true).is_err());
}

#[test]
fn partial_upload_failure_retains_intended_set_and_retry_selects_only_pending() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    let mut registry = FakeRegistry::default();
    let mut publisher = FakePublisher {
        registry: registry.clone(),
        revision: head.clone(),
        fail_after_first: true,
        ..Default::default()
    };
    assert!(
        publish(
            root,
            &release,
            &packages,
            &mut registry,
            &head,
            &options(false),
            &mut publisher,
            &mut FakeClock::default()
        )
        .is_err()
    );
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("target/release/report.json")).unwrap())
            .unwrap();
    assert_eq!(
        serde_json::json!({"a":"0.1.0","b":"0.1.0"}),
        report["intended"]
    );
    publisher.fail_after_first = false;
    publish(
        root,
        &release,
        &packages,
        &mut registry,
        &head,
        &options(false),
        &mut publisher,
        &mut FakeClock::default(),
    )
    .unwrap();
    assert_eq!(vec!["publish", "--locked", "-p", "b"], publisher.calls[1]);
    assert_eq!(release.crates, Release::read(root).unwrap().crates);
}

#[test]
fn publication_waits_for_visibility_and_timeout_keeps_retryable_record() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    for (delay, success) in [(3, true), (usize::MAX, false)] {
        let mut registry = FakeRegistry::default();
        let mut publisher = FakePublisher {
            registry: registry.clone(),
            revision: head.clone(),
            visibility_delay: delay,
            ..Default::default()
        };
        let mut clock = FakeClock::default();
        let result = publish(
            root,
            &release,
            &packages,
            &mut registry,
            &head,
            &options(false),
            &mut publisher,
            &mut clock,
        );
        assert_eq!(success, result.is_ok());
        if success {
            assert_eq!(VISIBILITY_POLL * 2, clock.elapsed);
        } else {
            assert_eq!(VISIBILITY_TIMEOUT, clock.elapsed);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("retry the same release tag")
            );
        }
        assert_eq!(release.crates, Release::read(root).unwrap().crates);
        assert_eq!(1, publisher.calls.len());
    }
}

#[test]
fn preparation_preserves_unpublished_record_across_midnight_and_partial_release_blocks_new_record()
{
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let original = fs::read(root.join("release.toml")).unwrap();
    let mut registry = FakeRegistry::default();
    let mut clock = FakeClock::default();
    clock.date = clock.date.next_day().unwrap();
    registry.complete("a", "previous-commit");
    prepare(root, &packages, &mut registry, None, &clock).unwrap();
    assert_eq!(original, fs::read(root.join("release.toml")).unwrap());
    let head = git::revision(root, "HEAD").unwrap();
    git::create_tag(root, &release, &head, false).unwrap();
    assert!(
        prepare(
            root,
            &packages,
            &mut registry,
            Some("release-2026-11-05"),
            &clock
        )
        .unwrap_err()
        .to_string()
        .contains("complete release-2026-11-04")
    );
    assert_eq!(original, fs::read(root.join("release.toml")).unwrap());
}

#[test]
fn preparation_selects_only_unpublished_versions_and_rejects_remote_collisions() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let mut registry = FakeRegistry::default();
    registry.complete("a", "previous-commit");
    prepare(
        root,
        &packages,
        &mut registry,
        Some("release-2026-11-04.2"),
        &FakeClock::default(),
    )
    .unwrap();
    let record = Release::read(root).unwrap();
    assert_eq!(
        BTreeMap::from([("b".into(), semver::Version::new(0, 1, 0))]),
        record.crates
    );
    let head = git::revision(root, "HEAD").unwrap();
    git::create_tag(root, &record, &head, true).unwrap();
    assert!(
        prepare(
            root,
            &packages,
            &mut registry,
            Some(&record.tag),
            &FakeClock::default()
        )
        .is_err()
    );
    registry.complete("b", &head);
    let before = fs::read(root.join("release.toml")).unwrap();
    prepare(root, &packages, &mut registry, None, &FakeClock::default()).unwrap();
    assert_eq!(before, fs::read(root.join("release.toml")).unwrap());
}

#[test]
fn occupied_dependent_uses_published_requirement_even_after_local_requirement_edit() {
    let fixture = repo();
    let root = fixture.path();
    let mut packages = workspace::packages(root).unwrap();
    packages.get_mut("a").unwrap().version = semver::Version::new(0, 2, 0);
    let mut registry = FakeRegistry::default();
    registry.complete("b", "old-source");
    registry
        .0
        .borrow_mut()
        .versions
        .get_mut(&("b".into(), semver::Version::new(0, 1, 0)))
        .unwrap()
        .dependencies
        .push(PublishedDependency {
            alias: "renamed".into(),
            package: "a".into(),
            requirement: "^0.1".into(),
            kind: DependencyKind::Normal,
            target: None,
            optional: true,
        });
    let error = audit(&packages, &mut registry).unwrap_err().to_string();
    assert!(error.contains("b 0.1.0"), "{error}");
    assert!(error.contains("0.2.0"), "{error}");
    packages.get_mut("b").unwrap().version = semver::Version::new(0, 1, 1);
    audit(&packages, &mut registry).unwrap();
}

#[test]
fn audit_collects_multiple_occupied_dependents_and_ignores_unpublished_dependents() {
    let fixture = repo();
    let mut packages = workspace::packages(fixture.path()).unwrap();
    packages.get_mut("a").unwrap().version = semver::Version::new(0, 2, 0);
    let mut registry = FakeRegistry::default();
    for name in ["b", "c"] {
        if name == "c" {
            let mut c = packages["b"].clone();
            c.name = name.into();
            packages.insert(name.into(), c);
        }
        registry.complete(name, "older-source");
        registry
            .0
            .borrow_mut()
            .versions
            .get_mut(&(name.into(), semver::Version::new(0, 1, 0)))
            .unwrap()
            .dependencies
            .push(PublishedDependency {
                alias: "renamed".into(),
                package: "a".into(),
                requirement: "=0.1.0".into(),
                kind: DependencyKind::Normal,
                target: None,
                optional: false,
            });
    }
    let error = audit(&packages, &mut registry).unwrap_err().to_string();
    assert!(error.contains("b 0.1.0"), "{error}");
    assert!(error.contains("c 0.1.0"), "{error}");
    for name in ["b", "c"] {
        packages.get_mut(name).unwrap().version = semver::Version::new(0, 1, 1);
    }
    audit(&packages, &mut registry).unwrap();
}

#[test]
fn dry_run_verifies_pending_set_without_waiting_for_registry_or_changing_record() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let original = fs::read(root.join("release.toml")).unwrap();
    let mut registry = FakeRegistry::default();
    let mut publisher = FakePublisher::default();
    let mut clock = FakeClock::default();
    let mut options = options(true);
    options.allow_dirty = true;
    publish(
        root,
        &release,
        &packages,
        &mut registry,
        &git::revision(root, "HEAD").unwrap(),
        &options,
        &mut publisher,
        &mut clock,
    )
    .unwrap();
    assert_eq!(1, publisher.calls.len());
    assert!(publisher.calls[0].iter().any(|s| s == "--dry-run"));
    assert_eq!(Duration::ZERO, clock.elapsed);
    assert_eq!(0, registry.0.borrow().refreshes);
    assert_eq!(original, fs::read(root.join("release.toml")).unwrap());
}

#[test]
fn publication_ignores_existing_report_and_rechecks_registry_on_completed_retry() {
    let fixture = repo();
    let root = fixture.path();
    let packages = workspace::packages(root).unwrap();
    let release = Release::read(root).unwrap();
    let head = git::revision(root, "HEAD").unwrap();
    let mut registry = FakeRegistry::default();
    registry.complete("a", &head);
    let original = fs::read(root.join("release.toml")).unwrap();
    let options = options(false);
    fs::create_dir_all(root.join(options.output.parent().unwrap())).unwrap();
    fs::write(root.join(&options.output), "not valid JSON").unwrap();
    let mut publisher = FakePublisher {
        registry: registry.clone(),
        revision: head.clone(),
        ..Default::default()
    };
    publish(
        root,
        &release,
        &packages,
        &mut registry,
        &head,
        &options,
        &mut publisher,
        &mut FakeClock::default(),
    )
    .unwrap();
    assert_eq!(
        vec![vec!["publish", "--locked", "-p", "b"]],
        publisher.calls
    );
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join(&options.output)).unwrap()).unwrap();
    assert_eq!(serde_json::json!(["a", "b"]), report["completed"]);
    assert_eq!(serde_json::json!([]), report["pending"]);
    fs::remove_file(root.join(&options.output)).unwrap();
    publish(
        root,
        &release,
        &packages,
        &mut registry,
        &head,
        &options,
        &mut publisher,
        &mut FakeClock::default(),
    )
    .unwrap();
    assert_eq!(1, publisher.calls.len());
    assert!(root.join(&options.output).exists());
    assert_eq!(original, fs::read(root.join("release.toml")).unwrap());
}
