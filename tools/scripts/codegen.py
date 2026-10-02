# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project the S3 model, generate Rust values, and preview artifact changes."""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

from model_source import REPOSITORY_ROOT


MODEL_ARTIFACT = Path("s3-tm-dataplane/model/model.json")
RUST_ARTIFACT = Path("model/src/model/mod.rs")


def generated_files(directory: Path) -> dict:
    """Inventory generated inputs/outputs, excluding Cargo's build products."""
    crate = directory / "model"
    files = {}
    for relative in ("Cargo.toml", "member-sources.json"):
        file = crate / relative
        if file.is_file():
            files[relative] = file.read_bytes()
    if (crate / "src").is_dir():
        for file in sorted((crate / "src").rglob("*")):
            if file.is_file():
                files[file.relative_to(crate).as_posix()] = file.read_bytes()
    return files


def report_generated_changes(baseline: Path, candidate: Path) -> None:
    old, new = generated_files(baseline), generated_files(candidate)
    for file in sorted(old.keys() | new.keys()):
        if file not in old:
            print(f"Added: model/{file}")
        elif file not in new:
            print(f"Removed: model/{file}")
        elif old[file] != new[file]:
            print(f"Changed: model/{file}")
    if old == new:
        print("Generated Rust artifact unchanged.")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Project the S3 dataplane model and generate standalone Rust values."
    )
    parser.add_argument(
        "--project-only", action="store_true", help="export and validate the dataplane model"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="generate in temporary storage and compare with existing output",
    )
    parser.add_argument("--model", type=Path, help="explicit local Smithy JSON input")
    parser.add_argument(
        "--output", type=Path, default=Path("target/codegen/projections"),
        help="artifact output directory, relative to the repository root",
    )
    parser.add_argument("--offline", action="store_true", help="forbid dependency/model fetching")
    parser.add_argument("--pinned-only", action="store_true", help="reject local model overrides")
    options = parser.parse_args(argv)
    if options.pinned_only and options.model is not None:
        parser.error("--pinned-only does not accept --model overrides")

    project = REPOSITORY_ROOT / "tools/codegen/s3-tm-model-codegen"
    command = [str(project / "gradlew"), "--project-dir", str(project)]
    if options.offline:
        command.append("--offline")
    output = (REPOSITORY_ROOT / options.output).resolve()
    properties = []
    if options.model is not None:
        properties.append(f"-PmodelFile={(REPOSITORY_ROOT / options.model).resolve()}")
    if options.pinned_only:
        properties.append("-PpinnedOnly=true")

    def generate(destination: Path) -> int:
        return subprocess.run(
            command + [
                "codegen", f"-PprojectionOutput={destination}",
                f"-PprojectOnly={str(options.project_only).lower()}",
            ] + properties,
            cwd=REPOSITORY_ROOT,
        ).returncode

    try:
        if not options.dry_run:
            return generate(output)
        with tempfile.TemporaryDirectory(prefix="s3-tm-codegen-") as temporary:
            scratch = Path(temporary)
            print(f"Dry run destination: {output} (unchanged)", flush=True)
            print(f"Temporary output root: {scratch} (removed after preview)", flush=True)
            result = generate(scratch)
            if result:
                return result
            candidate = scratch / MODEL_ARTIFACT
            if not candidate.is_file():
                print(f"codegen: missing model artifact: {candidate}", file=sys.stderr)
                return 1
            if not options.project_only and not (scratch / RUST_ARTIFACT).is_file():
                print("codegen: missing Rust artifact", file=sys.stderr)
                return 1
            baseline = output / MODEL_ARTIFACT
            print(f"Dry run: destination unchanged: {output}", flush=True)
            if not baseline.exists():
                print(f"Would write: {baseline}")
                if not options.project_only:
                    report_generated_changes(output, scratch)
                return 0
            result = subprocess.run(
                command + [
                    "diffModels", f"-PoldModel={baseline}", f"-PnewModel={candidate}",
                ],
                cwd=REPOSITORY_ROOT,
            ).returncode
            if not options.project_only:
                report_generated_changes(output, scratch)
            return result
    except OSError as error:
        print(f"codegen: {error}", file=sys.stderr)
        return 1
