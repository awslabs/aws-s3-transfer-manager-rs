# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Release-record, publication-selection, and retry contract tests."""

from contextlib import redirect_stdout, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import release


class RegistryFixture:
    def __init__(self, versions=None, vcs=None):
        self.versions = versions or {}
        self.vcs = vcs or {}

    def version(self, name, version):
        return self.versions.get((name, version))

    def vcs_info(self, name, version, published):
        return self.vcs.get((name, version), {})

    def refresh(self):
        pass


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        parent = Path(__file__).resolve().parents[2] / "target/release-tests"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.revision = "a" * 40
        self.tag = "release-2026-11-03"

    def package(self, name="component", version="0.1.0", dependencies=(), publishable=True):
        return release.Package(name, version, self.root / name, publishable, dependencies)

    def published(self, package, *, revision=None, dirty=False, directory=None):
        return RegistryFixture(
            {(package.name, package.version): release.PublishedVersion("checksum")},
            {(package.name, package.version): {
                "git": {"sha1": revision or self.revision, "dirty": dirty},
                "path_in_vcs": directory or package.name,
            }},
        )

    def check(self, packages, registry=None, intended=None):
        intended = intended or {name: package.version for name, package in packages.items()}
        return release.check_release(self.root, release.Release(self.tag, intended), packages,
                                     registry or RegistryFixture(), self.revision)

    def write_record(self, crates):
        lines = ["[release]", f'tag = "{self.tag}"', "", "[release.crates]"]
        lines.extend(f'{name} = "{version}"' for name, version in crates.items())
        (self.root / "release.toml").write_text("\n".join(lines) + "\n")

    def test_unknown_private_and_mismatched_versions_fail(self):
        for packages, intended in [
            ({}, {"missing": "0.1.0"}),
            ({"private": self.package("private", publishable=False)}, {"private": "0.1.0"}),
            ({"component": self.package()}, {"component": "0.2.0"}),
        ]:
            with self.subTest(intended=intended), self.assertRaises(release.ReleaseError):
                self.check(packages, intended=intended)

    def test_partial_publication_preserves_intended_set(self):
        a, b = self.package("a"), self.package("b", dependencies=("a",))
        report = self.check({"a": a, "b": b}, self.published(a))
        self.assertEqual({"a": "0.1.0", "b": "0.1.0"}, report["intended"])
        self.assertEqual(["a"], report["completed"])
        self.assertEqual(["b"], report["pending"])

    def test_conflicting_or_unverifiable_published_versions_fail(self):
        package = self.package()
        registries = [self.published(package, revision="b" * 40), self.published(package, dirty=True),
                      self.published(package, directory="other"), RegistryFixture(
                          {("component", "0.1.0"): release.PublishedVersion("checksum")})]
        for registry in registries:
            with self.subTest(vcs=registry.vcs), self.assertRaisesRegex(release.ReleaseError, "already published"):
                self.check({"component": package}, registry)

    def test_yanked_version_is_occupied(self):
        package = self.package()
        registry = RegistryFixture({("component", "0.1.0"): release.PublishedVersion("checksum", True)})
        with self.assertRaisesRegex(release.ReleaseError, "yanked"):
            self.check({"component": package}, registry)

    def test_missing_unpublished_peer_fails_before_cargo(self):
        a, b = self.package("a"), self.package("b", dependencies=("a",))
        with self.assertRaisesRegex(release.ReleaseError, "include unpublished dependency a"):
            self.check({"a": a, "b": b}, intended={"b": "0.1.0"})

    def test_published_peer_can_remain_outside_release(self):
        a, b = self.package("a"), self.package("b", dependencies=("a",))
        report = self.check({"a": a, "b": b}, self.published(a, revision="b" * 40), {"b": "0.1.0"})
        self.assertEqual(["b"], report["pending"])
        self.assertEqual([], report["completed"])
        self.assertEqual({"a": "0.1.0", "b": "0.1.0"}, report["inventory"])

    def test_private_normal_dependency_fails(self):
        a, b = self.package("a", publishable=False), self.package("b", dependencies=("a",))
        with self.assertRaisesRegex(release.ReleaseError, "required local dependency is private"):
            self.check({"a": a, "b": b}, intended={"b": "0.1.0"})

    def test_registry_failure_is_not_an_absent_version(self):
        registry = RegistryFixture()
        with patch.object(registry, "version", side_effect=release.ReleaseError("lookup failed")):
            with self.assertRaisesRegex(release.ReleaseError, "lookup failed"):
                self.check({"component": self.package()}, registry)

    def test_completed_release_does_not_invoke_cargo(self):
        package = self.package()
        with patch.object(release.subprocess, "run") as cargo, redirect_stdout(io.StringIO()):
            release.publish_release(self.root, release.Release(self.tag, {"component": "0.1.0"}),
                                    {"component": package}, self.published(package), self.revision,
                                    dry_run=False, allow_dirty=False, output=Path("report.json"))
        cargo.assert_not_called()
        report = json.loads((self.root / "report.json").read_text())
        self.assertEqual([], report["pending"])

    def test_failed_cargo_preserves_original_report(self):
        package = self.package()
        error = subprocess.CalledProcessError(1, ["cargo", "publish"])
        with patch.object(release.subprocess, "run", side_effect=error), redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError):
                release.publish_release(self.root, release.Release(self.tag, {"component": "0.1.0"}),
                                        {"component": package}, RegistryFixture(), self.revision,
                                        dry_run=True, allow_dirty=False, output=Path("report.json"))
        report = json.loads((self.root / "report.json").read_text())
        self.assertEqual({"component": "0.1.0"}, report["intended"])
        self.assertEqual(["component"], report["pending"])

    def test_successful_publication_confirms_registry_completion(self):
        package = self.package()
        registry = RegistryFixture()
        def uploaded(args, **kwargs):
            registry.versions[(package.name, package.version)] = release.PublishedVersion("checksum")
            registry.vcs[(package.name, package.version)] = {
                "git": {"sha1": self.revision}, "path_in_vcs": package.name,
            }
        with patch.object(release.subprocess, "run", side_effect=uploaded) as cargo, redirect_stdout(io.StringIO()):
            release.publish_release(self.root, release.Release(self.tag, {"component": "0.1.0"}),
                                    {"component": package}, registry, self.revision,
                                    dry_run=False, allow_dirty=False, output=Path("report.json"))
        cargo.assert_called_once_with(["cargo", "publish", "--locked", "-p", "component"], cwd=self.root, check=True)
        self.assertEqual(["component"], json.loads((self.root / "report.json").read_text())["completed"])

    def test_empty_successful_index_response_is_not_missing(self):
        registry = release.CratesIo()
        with patch.object(registry, "_get", return_value=b""):
            with self.assertRaisesRegex(release.ReleaseError, "invalid registry index"):
                registry.version("component", "0.1.0")

    def test_only_pending_packages_and_client_feature_reach_cargo(self):
        args = release.cargo_arguments(["aws-smithy-managed-http", "other"], dry_run=True, allow_dirty=True)
        self.assertEqual(["cargo", "publish", "--locked", "-p", "aws-smithy-managed-http", "-p", "other",
                          "--features", "aws-smithy-managed-http/client", "--dry-run", "--allow-dirty"], args)
        args = release.cargo_arguments(["other"], dry_run=False)
        self.assertNotIn("--features", args)
        self.assertNotIn("--no-verify", args)
        self.assertNotIn("--token", args)
        with self.assertRaises(release.ReleaseError):
            release.cargo_arguments([], dry_run=True)
        with self.assertRaises(release.ReleaseError):
            release.cargo_arguments(["other"], dry_run=False, allow_dirty=True)

    def test_allow_dirty_publication_fails_before_metadata_or_network(self):
        with patch.object(release, "workspace_packages") as metadata, redirect_stderr(io.StringIO()):
            self.assertEqual(1, release.main(["publish", "--allow-dirty"], root=self.root))
        metadata.assert_not_called()

    def test_preparation_does_not_shrink_existing_record_on_retry(self):
        self.write_record({"component": "0.1.0"})
        original = (self.root / "release.toml").read_bytes()
        with patch.object(release.subprocess, "run") as git, redirect_stdout(io.StringIO()):
            release.prepare_release(self.root, {"component": self.package()}, self.published(self.package()), self.tag)
        git.assert_not_called()
        self.assertEqual(original, (self.root / "release.toml").read_bytes())

    def test_metadata_discovery_uses_workspace_members_and_ignores_dev_paths(self):
        def entry(name, publish, dependencies=()):
            return {"id": name, "name": name, "version": "0.1.0", "manifest_path": str(self.root / name / "Cargo.toml"),
                    "publish": publish, "dependencies": list(dependencies), "features": {}}
        dep = {"name": "a", "rename": "renamed", "kind": None, "path": str(self.root / "a")}
        dev = {"name": "outside", "kind": "dev", "path": "/outside"}
        metadata = {"workspace_members": ["a", "b", "private"], "packages": [
            entry("a", None), entry("b", None, [dep, dev]), entry("private", []), entry("external", None)]}
        packages = release.packages_from_metadata(metadata)
        self.assertEqual({"a", "b", "private"}, set(packages))
        self.assertEqual(("a",), packages["b"].local_dependencies)
        self.assertFalse(packages["private"].publishable)

    def test_date_tag_requires_valid_calendar_date_and_suffix(self):
        for tag in ["v0.4.0", "release-2026-02-30", "release-2026-11-03.1", "release-2026-11-03.02"]:
            with self.subTest(tag=tag), self.assertRaises(release.ReleaseError):
                release.validate_tag(tag)
        release.validate_tag(self.tag)
        release.validate_tag(self.tag + ".2")
        release.validate_tag(self.tag + ".10")

    def test_archive_checksum_and_vcs_metadata(self):
        archive_data = io.BytesIO()
        content = json.dumps({"git": {"sha1": self.revision}, "path_in_vcs": "component"}).encode()
        with tarfile.open(fileobj=archive_data, mode="w:gz") as archive:
            member = tarfile.TarInfo("component-0.1.0/.cargo_vcs_info.json")
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
        data = archive_data.getvalue()
        registry = release.CratesIo()
        published = release.PublishedVersion(hashlib.sha256(data).hexdigest())
        with patch.object(registry, "_get", return_value=data):
            self.assertEqual(self.revision, registry.vcs_info("component", "0.1.0", published)["git"]["sha1"])
            with self.assertRaisesRegex(release.ReleaseError, "checksum mismatch"):
                registry.vcs_info("other", "0.1.0", release.PublishedVersion("wrong"))

    def test_index_404_is_missing_but_other_errors_fail(self):
        def error(status):
            return urllib.error.HTTPError("https://index.crates.io/test", status, "failed", {}, None)
        with patch.object(release.urllib.request, "urlopen", side_effect=error(404)):
            self.assertIsNone(release.CratesIo._get("https://index.crates.io/test", missing_ok=True))
        with patch.object(release.urllib.request, "urlopen", side_effect=error(403)):
            with self.assertRaisesRegex(release.ReleaseError, "HTTP 403"):
                release.CratesIo._get("https://index.crates.io/test", missing_ok=True)


class SourceTests(unittest.TestCase):
    def setUp(self):
        parent = Path(__file__).resolve().parents[2] / "target/release-tests"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.git("init", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        (self.root / "file").write_text("first\n")
        self.git("add", "file")
        self.git("commit", "-m", "fixture")
        self.release = release.Release("release-2026-11-03", {"component": "0.1.0"})
        self.git("tag", "-a", self.release.tag, "-m", "fixture release")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True, stderr=subprocess.DEVNULL).strip()

    def test_actual_publication_requires_tag_even_at_same_commit(self):
        self.assertEqual(self.git("rev-parse", "HEAD"), release.check_source(
            self.root, self.release, self.release.tag, require_tag=True))
        with self.assertRaisesRegex(release.ReleaseError, "requires the recorded tag"):
            release.check_source(self.root, self.release, "main", require_tag=True)
        self.assertEqual(self.git("rev-parse", "HEAD"), release.check_source(
            self.root, self.release, "main", require_tag=False))

    def test_actual_publication_rejects_dirty_worktree(self):
        (self.root / "file").write_text("changed\n")
        with self.assertRaisesRegex(release.ReleaseError, "clean worktree"):
            release.check_source(self.root, self.release, self.release.tag, require_tag=True)
        release.check_source(self.root, self.release, "main", require_tag=False)

    def test_preparation_blocks_incomplete_previous_tag_and_preserves_record(self):
        (self.root / "release.toml").write_text(
            '[release]\ntag = "release-2026-11-03"\n[release.crates]\ncomponent = "0.1.0"\n'
        )
        original = (self.root / "release.toml").read_bytes()
        package = release.Package("component", "0.1.1", self.root / "component", True)
        with self.assertRaisesRegex(release.ReleaseError, "complete release-2026-11-03"):
            release.prepare_release(self.root, {"component": package}, RegistryFixture(), "release-2026-11-03.2")
        self.assertEqual(original, (self.root / "release.toml").read_bytes())

    def test_preparation_writes_only_unpublished_versions(self):
        a = release.Package("a", "0.1.0", self.root / "a", True)
        b = release.Package("b", "0.1.1", self.root / "b", True, ("a",))
        private = release.Package("private", "0.1.0", self.root / "private", False)
        registry = RegistryFixture({("a", "0.1.0"): release.PublishedVersion("checksum")})
        with redirect_stdout(io.StringIO()):
            release.prepare_release(self.root, {"a": a, "b": b, "private": private}, registry, "release-2026-11-03.2")
        prepared = release.read_release(self.root)
        self.assertEqual("release-2026-11-03.2", prepared.tag)
        self.assertEqual({"b": "0.1.1"}, prepared.crates)

    def test_release_tag_must_identify_current_head(self):
        (self.root / "file").write_text("second\n")
        self.git("commit", "-am", "second fixture")
        with self.assertRaisesRegex(release.ReleaseError, "does not resolve"):
            release.check_source(self.root, self.release, self.release.tag, require_tag=True)


if __name__ == "__main__":
    unittest.main()
