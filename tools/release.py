#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Prepare, validate, and publish a recorded workspace release."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time
import tomllib
from typing import Protocol
import urllib.error
import urllib.request


class ReleaseError(Exception):
    """A release contract or registry operation failed."""


@dataclass(frozen=True)
class Package:
    name: str
    version: str
    directory: Path
    publishable: bool
    local_dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class Release:
    tag: str
    crates: dict[str, str]


@dataclass(frozen=True)
class PublishedVersion:
    checksum: str
    yanked: bool = False


class Registry(Protocol):
    """Exact-version availability and Cargo VCS metadata for registry packages."""

    def version(self, name: str, version: str) -> PublishedVersion | None: ...

    def vcs_info(self, name: str, version: str, published: PublishedVersion) -> dict: ...

    def refresh(self) -> None: ...


class CratesIo:
    """Read the crates.io sparse index and checksum-verified package archives."""

    def __init__(self) -> None:
        self._index: dict[str, dict[str, PublishedVersion]] = {}
        self._vcs: dict[tuple[str, str], dict] = {}

    @staticmethod
    def _get(url: str, *, missing_ok: bool = False) -> bytes | None:
        for attempt in range(3):
            try:
                request = urllib.request.Request(
                    url, headers={"User-Agent": "aws-s3-transfer-manager-release"}
                )
                with urllib.request.urlopen(request, timeout=30) as response:
                    return response.read()
            except urllib.error.HTTPError as error:
                if error.code == 404 and missing_ok:
                    return None
                if error.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise ReleaseError(f"registry request failed: {url}: HTTP {error.code}") from error
            except (urllib.error.URLError, TimeoutError) as error:
                if attempt == 2:
                    raise ReleaseError(f"registry request failed: {url}: {error}") from error
            time.sleep(attempt + 1)
        raise AssertionError("request retry loop exhausted")

    def refresh(self) -> None:
        self._index.clear()

    def version(self, name: str, version: str) -> PublishedVersion | None:
        if name not in self._index:
            lower = name.lower()
            if len(lower) <= 2:
                index_path = f"{len(lower)}/{lower}"
            elif len(lower) == 3:
                index_path = f"3/{lower[0]}/{lower}"
            else:
                index_path = f"{lower[:2]}/{lower[2:4]}/{lower}"
            data = self._get(f"https://index.crates.io/{index_path}", missing_ok=True)
            try:
                records = [json.loads(line) for line in data.splitlines()] if data is not None else []
                if data is not None and not records:
                    raise ValueError("empty index response")
                self._index[name] = {
                    entry["vers"]: PublishedVersion(entry["cksum"], entry.get("yanked", False))
                    for entry in records
                }
            except (ValueError, KeyError, TypeError) as error:
                raise ReleaseError(f"invalid registry index for {name}") from error
        return self._index[name].get(version)

    def vcs_info(self, name: str, version: str, published: PublishedVersion) -> dict:
        key = (name, version)
        if key not in self._vcs:
            url = f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
            data = self._get(url)
            assert data is not None
            if hashlib.sha256(data).hexdigest() != published.checksum:
                raise ReleaseError(f"registry archive checksum mismatch: {name} {version}")
            try:
                with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
                    member = archive.extractfile(f"{name}-{version}/.cargo_vcs_info.json")
                    if member is None:
                        raise ReleaseError(f"missing Cargo VCS metadata: {name} {version}")
                    with member:
                        self._vcs[key] = json.load(member)
                if not isinstance(self._vcs[key], dict):
                    raise ValueError("VCS metadata is not an object")
            except (KeyError, ValueError, tarfile.TarError, OSError) as error:
                raise ReleaseError(f"unverifiable Cargo VCS metadata: {name} {version}") from error
        return self._vcs[key]


def capture(root: Path, args: list[str]) -> str:
    result = subprocess.run(args, cwd=root, text=True, capture_output=True)
    if result.returncode:
        raise ReleaseError(f"{' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def packages_from_metadata(metadata: dict) -> dict[str, Package]:
    members = set(metadata["workspace_members"])
    entries = [entry for entry in metadata["packages"] if entry["id"] in members]
    directories = {Path(entry["manifest_path"]).parent.resolve(): entry["name"] for entry in entries}
    packages: dict[str, Package] = {}
    for entry in entries:
        name = entry["name"]
        if name in packages:
            raise ReleaseError(f"ambiguous workspace package name: {name}")
        dependencies = []
        for dependency in entry["dependencies"]:
            if dependency["kind"] == "dev" or not dependency.get("path"):
                continue
            dependency_name = directories.get(Path(dependency["path"]).resolve())
            if dependency_name is None:
                raise ReleaseError(f"{name}: local dependency outside workspace: {dependency['name']}")
            dependencies.append(dependency_name)
        publish = entry["publish"]
        packages[name] = Package(
            name,
            entry["version"],
            Path(entry["manifest_path"]).parent.resolve(),
            publish is None or "crates-io" in publish,
            tuple(sorted(set(dependencies))),
        )
    return packages


def workspace_packages(root: Path) -> dict[str, Package]:
    metadata = json.loads(capture(root, ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"]))
    return packages_from_metadata(metadata)


def validate_tag(tag: str) -> None:
    match = re.fullmatch(r"release-(\d{4}-\d{2}-\d{2})(?:\.([1-9]\d*))?", tag)
    if match is None:
        raise ReleaseError("release tag must be release-YYYY-MM-DD, optionally followed by .2, .3, ...")
    try:
        date.fromisoformat(match[1])
    except ValueError as error:
        raise ReleaseError(f"invalid release date: {tag}") from error
    if match[2] is not None and int(match[2]) < 2:
        raise ReleaseError("additional releases on the same day start at suffix .2")


def read_release(root: Path) -> Release:
    try:
        document = tomllib.loads((root / "release.toml").read_text())
        release = document["release"]
        tag, crates = release["tag"], release["crates"]
        if not isinstance(tag, str) or not isinstance(crates, dict) or not crates:
            raise ValueError("tag and a nonempty crates table are required")
        if any(not isinstance(name, str) or not isinstance(version, str) for name, version in crates.items()):
            raise ValueError("crate names and versions must be strings")
        validate_tag(tag)
        return Release(tag, crates)
    except (OSError, KeyError, ValueError, TypeError) as error:
        raise ReleaseError(f"invalid release.toml: {error}") from error


def validate_record(release: Release, packages: dict[str, Package]) -> None:
    for name, version in release.crates.items():
        package = packages.get(name)
        if package is None or not package.publishable:
            raise ReleaseError(f"recorded package is not a publishable workspace member: {name}")
        if package.version != version:
            raise ReleaseError(f"{name}: release records {version}, but Cargo.toml declares {package.version}")


def check_source(root: Path, release: Release, ref: str, *, require_tag: bool) -> str:
    revision = capture(root, ["git", "rev-parse", "HEAD"])
    requested = capture(root, ["git", "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"])
    if requested != revision:
        raise ReleaseError(f"requested ref {ref} does not resolve to the checkout's HEAD")
    if require_tag:
        if ref not in {release.tag, f"refs/tags/{release.tag}"}:
            raise ReleaseError(f"publication requires the recorded tag: {release.tag}")
        tagged = capture(root, ["git", "rev-parse", "--verify", f"refs/tags/{release.tag}^{{commit}}"])
        if tagged != revision:
            raise ReleaseError("recorded release tag does not resolve to HEAD")
        if capture(root, ["git", "status", "--porcelain"]):
            raise ReleaseError("publication requires a clean worktree")
    return revision


def completed_version(root: Path, package: Package, version: str, published: PublishedVersion,
                      registry: Registry, revision: str) -> None:
    if published.yanked:
        raise ReleaseError(f"{package.name} {version} is yanked; its version cannot be reused")
    info = registry.vcs_info(package.name, version, published)
    git = info.get("git", {})
    path = package.directory.relative_to(root).as_posix()
    if (not isinstance(git, dict) or git.get("sha1") != revision
            or git.get("dirty", False) or info.get("path_in_vcs") != path):
        raise ReleaseError(
            f"{package.name} {version} is already published, but its Cargo VCS metadata "
            "does not identify this clean release commit and package directory"
        )


def check_release(root: Path, release: Release, packages: dict[str, Package], registry: Registry,
                  revision: str) -> dict:
    validate_record(release, packages)
    pending, completed = [], []
    for name, version in sorted(release.crates.items()):
        package = packages[name]
        published = registry.version(name, version)
        if published is None:
            pending.append(name)
        else:
            completed_version(root, package, version, published, registry, revision)
            completed.append(name)
        for dependency_name in package.local_dependencies:
            dependency = packages[dependency_name]
            if not dependency.publishable:
                raise ReleaseError(f"{name}: required local dependency is private: {dependency_name}")
            if dependency_name not in release.crates:
                available = registry.version(dependency_name, dependency.version)
                if available is None or available.yanked:
                    raise ReleaseError(
                        f"{name}: include unpublished dependency {dependency_name} {dependency.version} "
                        "in release.toml"
                    )
    return {
        "tag": release.tag,
        "source_revision": revision,
        "inventory": {name: package.version for name, package in sorted(packages.items()) if package.publishable},
        "intended": dict(sorted(release.crates.items())),
        "pending": pending,
        "completed": completed,
    }


def prepare_release(root: Path, packages: dict[str, Package], registry: Registry, tag: str) -> None:
    validate_tag(tag)
    record = root / "release.toml"
    if record.exists():
        existing = read_release(root)
        if existing.tag == tag:
            validate_record(existing, packages)
            print(f"Keeping the existing {tag} record; use publish to resume it.")
            return
        previous_tag = subprocess.run(
            ["git", "rev-parse", "--verify", f"refs/tags/{existing.tag}^{{commit}}"],
            cwd=root, text=True, capture_output=True,
        )
        if previous_tag.returncode == 0:
            for name, version in existing.crates.items():
                published = registry.version(name, version)
                if published is None:
                    raise ReleaseError(f"complete {existing.tag} before preparing another release")
    tag_exists = subprocess.run(["git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}"], cwd=root)
    if tag_exists.returncode == 0:
        raise ReleaseError(f"release tag already exists: {tag}; choose a new date or suffix")
    crates = {}
    for name, package in sorted(packages.items()):
        if package.publishable and registry.version(name, package.version) is None:
            crates[name] = package.version
    if not crates:
        print("Nothing new to release; prepare package version bumps or resume the existing record.")
        return
    release = Release(tag, crates)
    check_release(root, release, packages, registry, capture(root, ["git", "rev-parse", "HEAD"]))
    lines = ["[release]", f"tag = {json.dumps(tag)}", "", "[release.crates]"]
    lines.extend(f"{name} = {json.dumps(version)}" for name, version in crates.items())
    record.write_text("\n".join(lines) + "\n")
    print(f"Prepared {tag}: {len(crates)} package versions. Review release.toml with the version and changelog changes.")


def cargo_arguments(pending: list[str], *, dry_run: bool, allow_dirty: bool = False) -> list[str]:
    if not pending:
        raise ReleaseError("an empty pending set must not invoke Cargo")
    if allow_dirty and not dry_run:
        raise ReleaseError("--allow-dirty is only supported with --dry-run")
    args = ["cargo", "publish", "--locked"]
    for name in pending:
        args.extend(["-p", name])
    if "aws-smithy-managed-http" in pending:
        args.extend(["--features", "aws-smithy-managed-http/client"])
    if dry_run:
        args.append("--dry-run")
    if allow_dirty:
        args.append("--allow-dirty")
    return args


def save_report(root: Path, report: dict, output: Path) -> None:
    path = output if output.is_absolute() else root / output
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Release {report['tag']} at {report['source_revision']}")
    for name, version in report["intended"].items():
        state = "completed" if name in report["completed"] else "pending"
        print(f"  {name} {version}: {state}")
    print(f"Report: {path}")


def publish_release(root: Path, release: Release, packages: dict[str, Package], registry: Registry,
                    revision: str, *, dry_run: bool, allow_dirty: bool, output: Path) -> None:
    report = check_release(root, release, packages, registry, revision)
    save_report(root, report, output)
    if not report["pending"]:
        print("All intended versions are already published; no Cargo invocation is needed.")
        return
    args = cargo_arguments(report["pending"], dry_run=dry_run, allow_dirty=allow_dirty)
    print("Running: " + " ".join(args), flush=True)
    subprocess.run(args, cwd=root, check=True)
    if not dry_run:
        deadline = time.monotonic() + 180
        while True:
            registry.refresh()
            report = check_release(root, release, packages, registry, revision)
            save_report(root, report, output)
            if not report["pending"]:
                return
            if time.monotonic() >= deadline:
                raise ReleaseError("published versions are not all visible in the registry; retry the same release tag")
            time.sleep(5)


def main(argv: list[str] | None = None, *, root: Path | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command")
    prepare = commands.add_parser("prepare", help="generate release.toml from unpublished workspace versions")
    prepare.add_argument("--tag", required=True)
    check = commands.add_parser("check", help="validate the release record and report pending versions")
    publish = commands.add_parser("publish", help="verify or upload the recorded package versions")
    for command in [check, publish]:
        command.add_argument("--ref", default="main")
        command.add_argument("--output", type=Path, default=Path("target/release/plan.json"))
    check.add_argument("--require-tag", action="store_true")
    publish.add_argument("--dry-run", action="store_true")
    publish.add_argument("--allow-dirty", action="store_true", help="allow uncommitted package changes during dry-run only")
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    root = (root or Path(__file__).resolve().parents[1]).resolve()
    try:
        if args.command == "publish" and args.allow_dirty and not args.dry_run:
            raise ReleaseError("--allow-dirty is only supported with --dry-run")
        packages = workspace_packages(root)
        registry = CratesIo()
        if args.command == "prepare":
            prepare_release(root, packages, registry, args.tag)
        else:
            release = read_release(root)
            require_tag = args.require_tag if args.command == "check" else not args.dry_run
            revision = check_source(root, release, args.ref, require_tag=require_tag)
            if args.command == "check":
                save_report(root, check_release(root, release, packages, registry, revision), args.output)
            else:
                publish_release(root, release, packages, registry, revision, dry_run=args.dry_run,
                                allow_dirty=args.allow_dirty, output=args.output)
        return 0
    except (ReleaseError, subprocess.CalledProcessError, OSError) as error:
        print(f"release: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
