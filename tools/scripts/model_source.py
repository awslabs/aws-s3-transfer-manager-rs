# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Acquire and validate S3 model input files."""

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Dict, Optional
from urllib.error import URLError
from urllib.request import urlopen


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = (
    REPOSITORY_ROOT / "tools/codegen/s3-tm-model-codegen/gradle.properties"
)
CACHE_PATH = REPOSITORY_ROOT / "target/codegen/models/s3.json"
SERVICE_ID = "com.amazonaws.s3#AmazonS3"
MAX_MODEL_BYTES = 32 * 1024 * 1024


class ModelSourceError(Exception):
    """A configuration, acquisition, or input validation failure."""


def read_properties(path: Path) -> Dict[str, str]:
    """Read literal key=value configuration entries."""
    result = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ModelSourceError(f"Cannot read model configuration {path}: {error}") from error
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip() or "\\" in line:
            raise ModelSourceError(f"{path}:{number}: expected literal key=value")
        key = key.strip()
        if key in result:
            raise ModelSourceError(f"{path}:{number}: duplicate property {key}")
        result[key] = value.strip()
    return result


@dataclass(frozen=True)
class ModelPin:
    repository: str
    revision: str
    path: str
    sha256: str

    @classmethod
    def from_properties(cls, properties: Dict[str, str]) -> "ModelPin":
        keys = ("model.repository", "model.revision", "model.path", "model.sha256")
        missing = [key for key in keys if not properties.get(key)]
        if missing:
            raise ModelSourceError(
                "Model source pin is incomplete: "
                + ", ".join(missing)
                + f". Set a verified immutable revision/digest in {CONFIG_PATH}, "
                "or select a developer input explicitly with --model <path>."
            )
        pin = cls(*(properties[key] for key in keys))
        if not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*",
            pin.repository,
        ):
            raise ModelSourceError("model.repository must be a GitHub owner/repository")
        if not re.fullmatch(r"[0-9a-f]{40}", pin.revision):
            raise ModelSourceError("model.revision must be a full lowercase Git commit SHA")
        if not re.fullmatch(r"[0-9a-f]{64}", pin.sha256):
            raise ModelSourceError("model.sha256 must be a lowercase SHA-256 digest")
        parts = PurePosixPath(pin.path).parts
        if (
            not re.fullmatch(r"[A-Za-z0-9_./-]+", pin.path)
            or pin.path.startswith("/")
            or any(part in (".", "..") for part in pin.path.split("/"))
            or str(PurePosixPath(pin.path)) != pin.path
            or not parts
            or not pin.path.endswith(".json")
        ):
            raise ModelSourceError("model.path must be a relative JSON path without traversal")
        return pin

    @property
    def url(self) -> str:
        return (
            f"https://raw.githubusercontent.com/{self.repository}/"
            f"{self.revision}/{self.path}"
        )


@dataclass(frozen=True)
class ModelInput:
    path: Path
    sha256: str
    origin: str
    revision: Optional[str] = None
    repository: Optional[str] = None
    source_path: Optional[str] = None

    def report(self) -> dict:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "origin": self.origin,
            "revision": self.revision,
            "repository": self.repository,
            "source_path": self.source_path,
        }


def inspect_model(path: Path, expected_digest: Optional[str] = None) -> str:
    """Check bytes and format only; the JVM loader performs Smithy validation."""
    try:
        with path.open("rb") as stream:
            content = stream.read(MAX_MODEL_BYTES + 1)
    except OSError as error:
        raise ModelSourceError(f"Cannot read model {path}: {error}") from error
    if len(content) > MAX_MODEL_BYTES:
        raise ModelSourceError(f"Model exceeds the {MAX_MODEL_BYTES}-byte limit: {path}")
    digest = hashlib.sha256(content).hexdigest()
    if expected_digest is not None and digest != expected_digest:
        raise ModelSourceError(
            f"SHA-256 mismatch for {path}: expected {expected_digest}, got {digest}. "
            "The existing cache was not replaced. For developer input use "
            "--model <path>; to restore the pin, move the differing cache aside first."
        )
    try:
        model = json.loads(content)
    except (ValueError, UnicodeDecodeError) as error:
        raise ModelSourceError(f"Invalid model JSON in {path}: {error}") from error
    if not isinstance(model, dict) or model.get("smithy") != "2.0":
        raise ModelSourceError(f"Expected a Smithy 2.0 JSON AST in {path}")
    shapes = model.get("shapes")
    service = shapes.get(SERVICE_ID) if isinstance(shapes, dict) else None
    if not isinstance(service, dict) or service.get("type") != "service":
        raise ModelSourceError(f"Model {path} does not define S3 service {SERVICE_ID}")
    return digest


def download_model(url: str, destination: Path) -> None:
    """Fetch a bounded raw model into temporary storage."""
    try:
        with urlopen(url, timeout=30) as response, destination.open("wb") as output:
            size = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_MODEL_BYTES:
                    raise ModelSourceError(
                        f"Downloaded model exceeds the {MAX_MODEL_BYTES}-byte limit"
                    )
                output.write(chunk)
    except (OSError, URLError) as error:
        raise ModelSourceError(
            f"Cannot fetch pinned model from {url}: {error}. "
            f"Populate the verified cache at {CACHE_PATH}, or use --model <path>."
        ) from error


def acquire_model(
    properties: Dict[str, str],
    cache: Path = CACHE_PATH,
    local_model: Optional[Path] = None,
    offline: bool = False,
    pinned_only: bool = False,
    downloader: Callable[[str, Path], None] = download_model,
) -> ModelInput:
    if local_model is not None:
        if pinned_only:
            raise ModelSourceError("--pinned-only does not accept --model overrides")
        path = local_model.resolve()
        return ModelInput(path, inspect_model(path), "local")

    pin = ModelPin.from_properties(properties)
    cache = cache.absolute()
    if cache.is_symlink():
        raise ModelSourceError(f"Refusing a symbolic link as the pinned cache: {cache}")

    def cached_input() -> ModelInput:
        digest = inspect_model(cache, pin.sha256)
        return ModelInput(
            cache, digest, "pinned", pin.revision, pin.repository, pin.path
        )

    if cache.exists():
        return cached_input()
    if offline:
        raise ModelSourceError(
            f"Pinned model is missing at {cache} and --offline forbids fetching. "
            "Run fetch-model online first, or use --model <path> for developer input."
        )

    cache.parent.mkdir(parents=True, exist_ok=True)
    lock = cache.with_name(cache.name + ".lock")
    try:
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ModelSourceError(
            f"Another fetch may be active: {lock}. "
            "If it was interrupted, inspect the lock before removing it."
        ) from error
    os.close(descriptor)
    temporary = None
    try:
        if cache.is_symlink():
            raise ModelSourceError(f"Refusing a symbolic link as the pinned cache: {cache}")
        if cache.exists():
            return cached_input()
        descriptor, name = tempfile.mkstemp(
            prefix=f".{cache.name}.", suffix=".download", dir=cache.parent
        )
        os.close(descriptor)
        temporary = Path(name)
        downloader(pin.url, temporary)
        inspect_model(temporary, pin.sha256)
        # A hard link publishes atomically without overwriting a file created
        # while downloading. Both paths are on the same filesystem.
        try:
            os.link(temporary, cache)
        except FileExistsError:
            if cache.is_symlink():
                raise ModelSourceError(
                    f"Refusing a symbolic link as the pinned cache: {cache}"
                )
        return cached_input()
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        lock.unlink(missing_ok=True)


def main(argv=None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Resolve the pinned S3 Smithy model or an explicit developer input."
    )
    parser.add_argument("--model", type=Path, help="explicit local Smithy JSON input")
    parser.add_argument("--offline", action="store_true", help="never fetch a missing model")
    parser.add_argument(
        "--pinned-only", action="store_true", help="reject developer overrides (CI)"
    )
    parser.add_argument("--json", action="store_true", help="emit path and provenance as JSON")
    args = parser.parse_args(argv)
    try:
        properties = {} if args.model is not None else read_properties(CONFIG_PATH)
        model = acquire_model(
            properties,
            local_model=args.model,
            offline=args.offline,
            pinned_only=args.pinned_only,
        )
    except (ModelSourceError, OSError) as error:
        print(f"fetch-model: {error}", file=sys.stderr)
        return 1
    print(json.dumps(model.report(), sort_keys=True) if args.json else model.path)
    return 0
