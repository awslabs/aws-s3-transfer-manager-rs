from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config

from aws_s3_transfer import TransferManager

MIB = 1024 * 1024

REPO_ROOT = Path(__file__).resolve().parents[2]


def _mock_server_binary() -> str:
    """Returns the mock server binary, building it with Cargo unless ``S3_MOCK_SERVER`` is set."""
    if binary := os.environ.get("S3_MOCK_SERVER"):
        return binary
    build = subprocess.run(
        [
            "cargo",
            "build",
            "-p",
            "s3-mock-server",
            "--bin",
            "s3-mock-server",
            "--message-format=json",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for line in build.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message.get("executable"):
            executable: str = message["executable"]
            return executable
    raise RuntimeError("cargo did not report the s3-mock-server executable")


@pytest.fixture(scope="session")
def endpoint_url() -> Iterator[str]:
    # Exiting closes the server's pipes, which stops it, and waits for it to exit.
    with subprocess.Popen(
        [_mock_server_binary()], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as server:
        assert server.stdout is not None
        yield server.stdout.readline().strip()


@pytest.fixture(scope="session")
def tm_config(endpoint_url: str) -> dict[str, Any]:
    return {
        "region_name": "us-east-1",
        "endpoint_url": endpoint_url,
        "force_path_style": True,
        "aws_access_key_id": "mock-akid",
        "aws_secret_access_key": "mock-secret",
        "part_size": 5 * MIB,
        "multipart_threshold": 5 * MIB,
    }


@pytest.fixture(scope="session")
def s3(tm_config: dict[str, Any]) -> Any:
    return boto3.client(
        "s3",
        region_name=tm_config["region_name"],
        endpoint_url=tm_config["endpoint_url"],
        aws_access_key_id=tm_config["aws_access_key_id"],
        aws_secret_access_key=tm_config["aws_secret_access_key"],
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture
def bucket(s3: Any) -> str:
    name = f"test-{uuid.uuid4().hex[:16]}"
    s3.create_bucket(Bucket=name)
    return name


@pytest.fixture
def tm(tm_config: dict[str, Any]) -> Iterator[TransferManager]:
    with TransferManager(**tm_config) as tm:
        yield tm


@pytest.fixture
def read_object(s3: Any, bucket: str) -> Callable[[str], bytes]:
    def read(key: str) -> bytes:
        body: bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return body

    return read
