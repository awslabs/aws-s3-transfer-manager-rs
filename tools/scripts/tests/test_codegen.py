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

    def test_project_only_exports_the_model(self):
        self.assertEqual(0, codegen.main(["--project-only"]))
        self.assertEqual("candidate", (self.destination / codegen.MODEL_ARTIFACT).read_text())

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
        self.assertIn("Would write:", output.getvalue())
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
