# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import codegen


class CodegenTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.destination = self.root / "target/codegen/projections"
        self.calls = []
        self.status = 0
        self.diff_status = 0
        self.emit = True
        self.addCleanup(patch.stopall)
        patch.object(codegen, "REPOSITORY_ROOT", self.root).start()
        patch.object(codegen.subprocess, "run", side_effect=self.run_gradle).start()

    def run_gradle(self, command, cwd):
        self.calls.append(command)
        self.assertEqual(self.root, cwd)
        if "diffModels" in command:
            self.assertTrue(Path(self.property(command, "newModel")).is_file())
            return subprocess.CompletedProcess(command, self.diff_status)
        output = Path(self.property(command, "projectionOutput"))
        if self.emit and not self.status:
            artifact = output / codegen.MODEL_ARTIFACT
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_text("candidate", encoding="utf-8")
            if self.property(command, "projectOnly") == "false":
                rust = output / codegen.RUST_ARTIFACT
                rust.parent.mkdir(parents=True, exist_ok=True)
                rust.write_text("pub struct Candidate;", encoding="utf-8")
                sdk = output / codegen.SDK_ARTIFACT
                sdk.parent.mkdir(parents=True, exist_ok=True)
                sdk.write_text("// SDK candidate", encoding="utf-8")
        return subprocess.CompletedProcess(command, self.status)

    @staticmethod
    def property(command, name):
        return next(value.split("=", 1)[1] for value in command if value.startswith(f"-P{name}="))

    def baseline(self):
        artifact = self.destination / codegen.MODEL_ARTIFACT
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("baseline", encoding="utf-8")
        return artifact

    def test_default_exports_to_conventional_output(self):
        self.assertEqual(0, codegen.main([]))
        self.assertIn("codegen", self.calls[0])
        self.assertEqual(str(self.destination), self.property(self.calls[0], "projectionOutput"))
        self.assertEqual("false", self.property(self.calls[0], "projectOnly"))

    def test_project_only_exports_the_model(self):
        self.assertEqual(0, codegen.main(["--project-only"]))
        self.assertEqual("candidate", (self.destination / codegen.MODEL_ARTIFACT).read_text())
        self.assertEqual("true", self.property(self.calls[0], "projectOnly"))
        self.assertFalse((self.destination / codegen.RUST_ARTIFACT).exists())

    def test_forwards_local_input_output_and_offline_without_shell_splitting(self):
        self.assertEqual(0, codegen.main([
            "--model", "models/local input.json", "--output", "output with spaces", "--offline",
        ]))
        self.assertEqual(
            str(self.root / "models/local input.json"), self.property(self.calls[0], "modelFile")
        )
        self.assertEqual(str(self.root / "output with spaces"), self.property(self.calls[0], "projectionOutput"))
        self.assertIn("--offline", self.calls[0])

    def test_forwards_pin_only_requirement(self):
        self.assertEqual(0, codegen.main(["--pinned-only"]))
        self.assertIn("-PpinnedOnly=true", self.calls[0])

    def test_rejects_local_input_with_pin_only_before_running_gradle(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            codegen.main(["--pinned-only", "--model", "local.json"])
        self.assertEqual(2, error.exception.code)
        self.assertEqual([], self.calls)

    def test_rejects_unknown_options_before_running_gradle(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            codegen.main(["--unknown-option"])
        self.assertEqual([], self.calls)

    def test_propagates_codegen_failure(self):
        self.status = 7
        self.assertEqual(7, codegen.main([]))

    def test_dry_run_preserves_baseline_and_uses_standard_diff(self):
        baseline = self.baseline()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, codegen.main(["--dry-run", "--project-only", "--offline"]))
        self.assertEqual("baseline", baseline.read_text())
        self.assertEqual(str(baseline), self.property(self.calls[1], "oldModel"))
        self.assertIn("diffModels", self.calls[1])
        self.assertIn("--offline", self.calls[1])
        self.assertFalse(Path(self.property(self.calls[0], "projectionOutput")).exists())

    def test_dry_run_reports_new_output_without_creating_destination(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(0, codegen.main(["--dry-run"]))
        scratch = Path(self.property(self.calls[0], "projectionOutput"))
        self.assertIn(f"Dry run destination: {self.destination} (unchanged)", output.getvalue())
        self.assertIn(f"Temporary output root: {scratch} (removed after comparison)", output.getvalue())
        self.assertIn("Would write:", output.getvalue())
        self.assertFalse(scratch.exists())
        self.assertFalse(self.destination.exists())
        self.assertEqual(1, len(self.calls))

    def test_dry_run_failure_skips_diff_and_cleans_scratch(self):
        baseline = self.baseline()
        self.status = 9
        self.assertEqual(9, codegen.main(["--dry-run"]))
        self.assertEqual(1, len(self.calls))
        self.assertFalse(Path(self.property(self.calls[0], "projectionOutput")).exists())
        self.assertEqual("baseline", baseline.read_text())

    def test_dry_run_propagates_compatibility_errors(self):
        self.baseline()
        self.diff_status = 1
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(1, codegen.main(["--dry-run"]))
        self.assertFalse(Path(self.property(self.calls[0], "projectionOutput")).exists())

    def test_dry_run_rejects_missing_artifact(self):
        self.emit = False
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(1, codegen.main(["--dry-run"]))
        self.assertIn("missing model artifact", output.getvalue())

    def test_subprocess_errors_are_actionable_without_traceback(self):
        codegen.subprocess.run.side_effect = OSError("wrapper unavailable")
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(1, codegen.main([]))
        self.assertIn("wrapper unavailable", output.getvalue())
        self.assertNotIn("Traceback", output.getvalue())

    def test_full_preview_reports_entire_rust_subtree_without_modifying_it(self):
        self.baseline()
        rust = self.destination / codegen.RUST_ARTIFACT
        rust.parent.mkdir(parents=True, exist_ok=True)
        rust.write_text("old")
        removed = rust.parent / "removed.rs"
        removed.write_text("old helper")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(0, codegen.main(["--dry-run"]))
        self.assertIn("Changed: model/src/model/mod.rs", output.getvalue())
        self.assertIn("Removed: model/src/model/removed.rs", output.getvalue())
        self.assertEqual("old", rust.read_text())
        self.assertEqual("old helper", removed.read_text())

    def test_generated_inventory_excludes_cargo_build_products(self):
        rust = self.destination / codegen.RUST_ARTIFACT
        rust.parent.mkdir(parents=True, exist_ok=True)
        rust.write_text("model")
        crate = self.destination / "model"
        (crate / "Cargo.lock").write_text("lock")
        (crate / "target").mkdir()
        (crate / "target/output").write_text("binary")
        self.assertEqual({"src/model/mod.rs": b"model"}, codegen.generated_files(self.destination))

    def matching_baseline(self):
        baseline = self.baseline()
        baseline.write_text("candidate")
        rust = self.destination / codegen.RUST_ARTIFACT
        rust.parent.mkdir(parents=True, exist_ok=True)
        rust.write_text("pub struct Candidate;")
        sdk = self.destination / codegen.SDK_ARTIFACT
        sdk.parent.mkdir(parents=True, exist_ok=True)
        sdk.write_text("// SDK candidate")
        return baseline

    def test_check_matching_artifact_is_non_mutating_and_skips_semantic_diff(self):
        baseline = self.matching_baseline()
        before = codegen.artifact_files(self.destination)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, codegen.main(["--check", "--offline"]))
        self.assertEqual(before, codegen.artifact_files(self.destination))
        self.assertEqual("candidate", baseline.read_text())
        self.assertEqual(1, len(self.calls))
        self.assertFalse(Path(self.property(self.calls[0], "projectionOutput")).exists())

    def test_check_ignores_obsolete_ownership_metadata_without_modifying_it(self):
        self.matching_baseline()
        ledger = self.destination / "generated-files.json"
        ledger.write_text("obsolete inventory")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, codegen.main(["--check"]))
        self.assertEqual("obsolete inventory", ledger.read_text())

    def test_check_missing_destination_fails_without_creating_it(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(1, codegen.main(["--check"]))
        self.assertFalse(self.destination.exists())

    def test_check_changed_projection_fails_even_when_smithy_diff_would_succeed(self):
        self.matching_baseline().write_text("changed canonical model")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(1, codegen.main(["--check"]))
        self.assertIn(f"Changed: {codegen.MODEL_ARTIFACT}", output.getvalue())
        self.assertEqual(1, len(self.calls))

    def test_check_changed_or_missing_rust_and_extra_source_are_reported(self):
        self.matching_baseline()
        rust = self.destination / codegen.RUST_ARTIFACT
        rust.write_text("modified Rust")
        extra = rust.parent / "stale.rs"
        extra.write_text("extra helper")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(1, codegen.main(["--check"]))
        self.assertIn("Changed: model/src/model/mod.rs", output.getvalue())
        self.assertIn("Removed: model/src/model/stale.rs", output.getvalue())
        self.assertEqual("extra helper", extra.read_text())
        rust.unlink()
        with contextlib.redirect_stdout(output):
            self.assertEqual(1, codegen.main(["--check"]))
        self.assertIn("Added: model/src/model/mod.rs", output.getvalue())

    def test_check_reports_additional_artifact_metadata(self):
        self.matching_baseline()
        metadata = self.destination / "model/provenance.json"
        metadata.write_text("old provenance")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(1, codegen.main(["--check"]))
        self.assertIn("Removed: model/provenance.json", output.getvalue())

    def test_project_only_check_does_not_compare_an_existing_rust_crate(self):
        self.matching_baseline()
        (self.destination / codegen.RUST_ARTIFACT).write_text("irrelevant Rust")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, codegen.main(["--check", "--project-only"]))

    def test_check_and_dry_run_are_mutually_exclusive(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            codegen.main(["--check", "--dry-run"])
        self.assertEqual([], self.calls)

    def test_inventory_rejects_symlinks_without_reading_external_source(self):
        self.matching_baseline()
        external = self.root / "external.rs"
        external.write_text("hand-maintained")
        (self.destination / "model/src/model/external.rs").symlink_to(external)
        with self.assertRaises(OSError):
            codegen.generated_files(self.destination)

    def test_sdk_changes_are_checked_and_project_only_ignores_them(self):
        self.matching_baseline()
        sdk = self.destination / codegen.SDK_ARTIFACT
        sdk.write_text("changed SDK adapter")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(1, codegen.main(["--check"]))
            self.assertEqual(0, codegen.main(["--check", "--project-only"]))
        self.assertIn("Changed: sdk_v1/mod.rs", output.getvalue())
        self.assertEqual("changed SDK adapter", sdk.read_text())

    def test_sdk_inventory_rejects_symlinks_and_foreign_files(self):
        root = self.destination / "sdk_v1"
        root.mkdir(parents=True)
        foreign = root / "manual.txt"
        foreign.write_text("foreign")
        with self.assertRaises(OSError):
            codegen.sdk_files(self.destination)
        foreign.unlink()
        (root / "mod.rs").symlink_to(self.root / "outside")
        with self.assertRaises(OSError):
            codegen.sdk_files(self.destination)
