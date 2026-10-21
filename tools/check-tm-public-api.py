#!/usr/bin/env python3
"""Audit ordinary TM, experimental adoption, and the execution adapter separately."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TM = "aws-sdk-s3-transfer-manager"
ADAPTER = "crates/aws-sdk-s3-transfer-manager-ort"


def check(package, config, *features):
    print(f"Checking {package}: {config}", flush=True)
    environment = os.environ.copy()
    environment.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
    subprocess.run(
        ["cargo", "check-external-types", "--config", config, *features],
        cwd=package,
        env=environment,
        check=True,
    )


def check_ordinary():
    # The adapter enables the experimental feature in workspace resolution.
    # An identical-source workspace without that member keeps the ordinary
    # allowlist strict; --no-default-features alone does not undo unification.
    packages = [TM, "crates/aws-ort-execution", "crates/aws-ort-topology"]
    with tempfile.TemporaryDirectory(prefix="tm-public-api-") as directory:
        snapshot = Path(directory)
        for package in packages:
            shutil.copytree(
                ROOT / package,
                snapshot / package,
                ignore=shutil.ignore_patterns("target", ".git"),
            )
        manifest = (ROOT / "Cargo.toml").read_text()
        members = "members = [" + ", ".join(f'"{p}"' for p in packages) + "]"
        manifest, replaced = re.subn(r"members = \[.*?\]", members, manifest, count=1, flags=re.S)
        if replaced != 1:
            raise RuntimeError("expected one workspace member list")

        def path(match):
            value = match.group(1)
            base = snapshot if value in packages else ROOT
            return f'path = "{(base / value).resolve()}"'

        # Preserve versions/features and workspace metadata; only relocate path
        # dependencies and remove the member that opts into experimental APIs.
        manifest = re.sub(r'path = "([^"]+)"', path, manifest)
        (snapshot / "Cargo.toml").write_text(manifest)
        shutil.copyfile(ROOT / "Cargo.lock", snapshot / "Cargo.lock")
        check(snapshot / TM, "external-types.toml", "--no-default-features")


def main():
    check_ordinary()
    check(
        ROOT / TM,
        "external-types-experimental.toml",
        "--features",
        "__exp_ort",
    )
    check(ROOT / ADAPTER, "external-types.toml")


if __name__ == "__main__":
    main()
