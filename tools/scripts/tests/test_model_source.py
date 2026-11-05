# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import model_source


class ModelSourceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.cache = self.root / "models/s3.json"
        fixture = (
            model_source.REPOSITORY_ROOT
            / "tools/codegen/s3-tm-model-codegen/src/test/resources/s3-minimal.json"
        )
        self.content = fixture.read_bytes()
        self.properties = {
            "model.repository": "aws/api-models-aws",
            "model.revision": "a" * 40,
            "model.path": "models/s3/service/2006-03-01/s3-2006-03-01.json",
            "model.sha256": hashlib.sha256(self.content).hexdigest(),
        }
        self.calls = []

    def download(self, url, destination):
        self.calls.append(url)
        destination.write_bytes(self.content)

    def acquire(self, **kwargs):
        kwargs.setdefault("downloader", self.download)
        return model_source.acquire_model(self.properties, self.cache, **kwargs)

    def seed_cache(self, content=None):
        self.cache.parent.mkdir(parents=True, exist_ok=True)
        self.cache.write_bytes(self.content if content is None else content)

    def test_fetches_the_immutable_raw_url(self):
        self.acquire()
        self.assertEqual(
            [
                "https://raw.githubusercontent.com/aws/api-models-aws/"
                + "a" * 40
                + "/models/s3/service/2006-03-01/s3-2006-03-01.json"
            ],
            self.calls,
        )

    def test_installs_verified_bytes(self):
        self.acquire()
        self.assertEqual(self.content, self.cache.read_bytes())

    def test_records_pinned_provenance(self):
        result = self.acquire()
        self.assertEqual(
            {
                "path": str(self.cache),
                "sha256": self.properties["model.sha256"],
                "origin": "pinned",
                "revision": "a" * 40,
                "repository": "aws/api-models-aws",
                "source_path": self.properties["model.path"],
            },
            result.report(),
        )

    def test_verified_cache_needs_no_fetch(self):
        self.seed_cache()
        self.acquire(offline=True)
        self.assertEqual([], self.calls)

    def test_missing_offline_cache_is_actionable(self):
        with self.assertRaisesRegex(model_source.ModelSourceError, "missing.*--offline"):
            self.acquire(offline=True)

    def test_mismatch_does_not_replace_developer_cache(self):
        self.seed_cache(b"developer version")
        with self.assertRaises(model_source.ModelSourceError):
            self.acquire()
        self.assertEqual(b"developer version", self.cache.read_bytes())

    def test_mismatch_does_not_fetch(self):
        self.seed_cache(b"developer version")
        with self.assertRaises(model_source.ModelSourceError):
            self.acquire()
        self.assertEqual([], self.calls)

    def test_bad_download_is_not_published(self):
        def invalid_download(url, destination):
            destination.write_bytes(b"wrong digest")

        with self.assertRaisesRegex(model_source.ModelSourceError, "SHA-256 mismatch"):
            self.acquire(downloader=invalid_download)
        self.assertFalse(self.cache.exists())

    def test_failed_fetch_cleans_temporary_files_and_lock(self):
        def failed_download(url, destination):
            destination.write_bytes(b"partial")
            raise model_source.ModelSourceError("network failure")

        with self.assertRaises(model_source.ModelSourceError):
            self.acquire(downloader=failed_download)
        self.assertEqual([], list(self.cache.parent.iterdir()))

    def test_fetch_can_retry_after_failure(self):
        with self.assertRaises(model_source.ModelSourceError):
            self.acquire(
                downloader=lambda url, destination: (_ for _ in ()).throw(
                    model_source.ModelSourceError("network failure")
                )
            )
        self.assertEqual("pinned", self.acquire().origin)

    def test_concurrent_fetch_fails_without_removing_others_lock(self):
        self.cache.parent.mkdir(parents=True)
        lock = self.cache.with_name("s3.json.lock")
        lock.write_text("other fetch", encoding="utf-8")
        with self.assertRaisesRegex(model_source.ModelSourceError, "Another fetch"):
            self.acquire()
        self.assertEqual("other fetch", lock.read_text(encoding="utf-8"))

    def test_cache_created_during_fetch_is_preserved(self):
        def concurrent_download(url, destination):
            destination.write_bytes(self.content)
            self.cache.write_bytes(b"developer version")

        with self.assertRaises(model_source.ModelSourceError):
            self.acquire(downloader=concurrent_download)
        self.assertEqual(b"developer version", self.cache.read_bytes())

    def test_matching_cache_created_during_fetch_is_accepted(self):
        def concurrent_download(url, destination):
            destination.write_bytes(self.content)
            self.cache.write_bytes(self.content)

        self.assertEqual("pinned", self.acquire(downloader=concurrent_download).origin)

    def test_rejects_symbolic_link_cache(self):
        local = self.root / "developer.json"
        local.write_bytes(self.content)
        self.cache.parent.mkdir(parents=True)
        self.cache.symlink_to(local)
        with self.assertRaisesRegex(model_source.ModelSourceError, "symbolic link"):
            self.acquire()

    def test_rejects_dangling_symbolic_link_cache(self):
        self.cache.parent.mkdir(parents=True)
        self.cache.symlink_to(self.root / "missing.json")
        with self.assertRaisesRegex(model_source.ModelSourceError, "symbolic link"):
            self.acquire()

    def test_local_override_bypasses_incomplete_pin(self):
        local = self.root / "developer model.json"
        local.write_bytes(self.content)
        result = model_source.acquire_model({}, self.cache, local_model=local, offline=True)
        self.assertEqual("local", result.origin)

    def test_local_override_is_not_reported_as_pinned(self):
        self.seed_cache()
        self.assertIsNone(self.acquire(local_model=self.cache).revision)

    def test_local_override_never_populates_cache(self):
        local = self.root / "developer.json"
        local.write_bytes(self.content)
        self.acquire(local_model=local)
        self.assertFalse(self.cache.exists())

    def test_pinned_only_rejects_local_override(self):
        self.seed_cache()
        with self.assertRaisesRegex(model_source.ModelSourceError, "does not accept"):
            self.acquire(local_model=self.cache, pinned_only=True)

    def test_incomplete_pin_fails_before_creating_cache(self):
        self.properties["model.revision"] = ""
        with self.assertRaisesRegex(model_source.ModelSourceError, "pin is incomplete"):
            self.acquire()
        self.assertFalse(self.cache.parent.exists())

    def test_rejects_mutable_revision(self):
        self.properties["model.revision"] = "main"
        with self.assertRaisesRegex(model_source.ModelSourceError, "full lowercase"):
            self.acquire()

    def test_rejects_malformed_digest(self):
        self.properties["model.sha256"] = "not a digest"
        with self.assertRaisesRegex(model_source.ModelSourceError, "SHA-256 digest"):
            self.acquire()

    def test_rejects_unsafe_repository(self):
        for repository in ("aws/repo?other=value", "../repo", "aws/.."):
            with self.subTest(repository=repository):
                self.properties["model.repository"] = repository
                with self.assertRaisesRegex(model_source.ModelSourceError, "owner/repository"):
                    self.acquire()

    def test_rejects_unsafe_paths(self):
        for path in ("../s3.json", "/s3.json", "models//s3.json", "./s3.json", "s3.json?x=1"):
            with self.subTest(path=path):
                self.properties["model.path"] = path
                with self.assertRaisesRegex(model_source.ModelSourceError, "without traversal"):
                    self.acquire()

    def test_rejects_invalid_json(self):
        self.seed_cache(b"not json")
        with self.assertRaisesRegex(model_source.ModelSourceError, "Invalid model JSON"):
            self.acquire(local_model=self.cache)

    def test_rejects_other_service_model_format(self):
        self.seed_cache(b'{"version":"2.0","metadata":{"protocol":"rest-xml"}}')
        with self.assertRaisesRegex(model_source.ModelSourceError, "Smithy 2.0 JSON AST"):
            self.acquire(local_model=self.cache)

    def test_requires_s3_service(self):
        self.seed_cache(b'{"smithy":"2.0","shapes":{}}')
        with self.assertRaisesRegex(model_source.ModelSourceError, "does not define S3"):
            self.acquire(local_model=self.cache)

    def test_rejects_wrong_service_shape_type(self):
        self.seed_cache(
            b'{"smithy":"2.0","shapes":{"com.amazonaws.s3#AmazonS3":{"type":"structure"}}}'
        )
        with self.assertRaisesRegex(model_source.ModelSourceError, "does not define S3"):
            self.acquire(local_model=self.cache)

    def test_rejects_missing_local_file(self):
        with self.assertRaisesRegex(model_source.ModelSourceError, "Cannot read model"):
            self.acquire(local_model=self.root / "missing.json")

    def test_rejects_oversized_local_input(self):
        self.seed_cache()
        with patch.object(model_source, "MAX_MODEL_BYTES", 10):
            with self.assertRaisesRegex(model_source.ModelSourceError, "byte limit"):
                self.acquire(local_model=self.cache)

    def test_local_input_is_read_only(self):
        self.seed_cache()
        self.acquire(local_model=self.cache)
        self.assertEqual(self.content, self.cache.read_bytes())

    def test_download_limits_size(self):
        destination = self.root / "download"
        with patch.object(model_source, "urlopen", return_value=io.BytesIO(b"large input")):
            with patch.object(model_source, "MAX_MODEL_BYTES", 3):
                with self.assertRaisesRegex(model_source.ModelSourceError, "byte limit"):
                    model_source.download_model("https://example.invalid/model", destination)

    def test_download_converts_network_error(self):
        with patch.object(model_source, "urlopen", side_effect=URLError("offline")):
            with self.assertRaisesRegex(model_source.ModelSourceError, "Cannot fetch pinned"):
                model_source.download_model("https://example.invalid/model", self.root / "download")

    def test_literal_properties_accept_comments_and_empty_values(self):
        config = self.root / "gradle.properties"
        config.write_text("# comment\nmodel.revision=\n model.repository = aws/repo\n")
        self.assertEqual(
            {"model.revision": "", "model.repository": "aws/repo"},
            model_source.read_properties(config),
        )

    def test_rejects_duplicate_properties(self):
        config = self.root / "gradle.properties"
        config.write_text("model.revision=one\nmodel.revision=two\n")
        with self.assertRaisesRegex(model_source.ModelSourceError, "duplicate property"):
            model_source.read_properties(config)

    def test_rejects_unsupported_property_syntax(self):
        config = self.root / "gradle.properties"
        config.write_text("model.revision: value\n")
        with self.assertRaisesRegex(model_source.ModelSourceError, "literal key=value"):
            model_source.read_properties(config)

    def test_cli_handles_spaces_and_reports_local_provenance(self):
        local = self.root / "developer model.json"
        local.write_bytes(self.content)
        command = [
            sys.executable,
            str(model_source.REPOSITORY_ROOT / "tools/scripts/fetch-model"),
            "--model",
            str(local),
            "--offline",
            "--json",
        ]
        result = subprocess.run(command, cwd=self.root, text=True, capture_output=True, check=True)
        self.assertEqual("local", json.loads(result.stdout)["origin"])

    def test_cli_errors_have_no_traceback(self):
        result = subprocess.run(
            [
                sys.executable,
                str(model_source.REPOSITORY_ROOT / "tools/scripts/fetch-model"),
                "--model",
                str(self.root / "missing.json"),
            ],
            text=True,
            capture_output=True,
        )
        self.assertTrue(result.returncode != 0 and "Traceback" not in result.stderr)


if __name__ == "__main__":
    unittest.main()
