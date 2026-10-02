# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run model export/validation and scratch previews."""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

from model_source import REPOSITORY_ROOT


MODEL_ARTIFACT = Path("s3-tm-dataplane/model/model.json")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Export and validate the S3 dataplane Smithy model."
    )
    parser.add_argument(
        "--project-only", action="store_true", help="export and validate the dataplane model"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="export to temporary storage and compare with existing output",
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
            command + ["codegen", f"-PprojectionOutput={destination}"] + properties,
            cwd=REPOSITORY_ROOT,
        ).returncode

    try:
        if not options.dry_run:
            return generate(output)
        with tempfile.TemporaryDirectory(prefix="s3-tm-codegen-") as temporary:
            scratch = Path(temporary)
            result = generate(scratch)
            if result:
                return result
            candidate = scratch / MODEL_ARTIFACT
            if not candidate.is_file():
                print(f"codegen: missing model artifact: {candidate}", file=sys.stderr)
                return 1
            baseline = output / MODEL_ARTIFACT
            print(f"Dry run: destination unchanged: {output}", flush=True)
            if not baseline.exists():
                print(f"Would write: {baseline}")
                return 0
            return subprocess.run(
                command + [
                    "diffModels", f"-PoldModel={baseline}", f"-PnewModel={candidate}",
                ],
                cwd=REPOSITORY_ROOT,
            ).returncode
    except OSError as error:
        print(f"codegen: {error}", file=sys.stderr)
        return 1
