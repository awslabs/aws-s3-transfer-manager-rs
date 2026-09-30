from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import aws_s3_transfer
from aws_s3_transfer import AsyncTransferManager, TransferManager


def test_version() -> None:
    assert aws_s3_transfer.__version__.count(".") == 2


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"aws_access_key_id": "akid"}, "must be given together"),
        ({"aws_session_token": "token"}, "must be given together"),
        ({"max_concurrency": 8, "target_throughput_gbps": 10}, "mutually exclusive"),
        ({"part_size": 0}, "part_size must be positive"),
        ({"memory_limit": 0}, "memory_limit must be positive"),
    ],
)
def test_invalid_configuration(config: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        TransferManager(region_name="us-east-1", **config)
    with pytest.raises(ValueError, match=message):
        AsyncTransferManager(region_name="us-east-1", **config)


@pytest.fixture
def empty_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for variable in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.mark.usefixtures("empty_environment")
def test_loads_configuration_eagerly() -> None:
    with pytest.raises(ValueError, match="no AWS region"):
        TransferManager()


@pytest.mark.usefixtures("empty_environment")
def test_async_loads_configuration_on_first_use() -> None:
    async def main() -> None:
        tm = AsyncTransferManager()
        with pytest.raises(ValueError, match="no AWS region"):
            await tm.upload(b"data", "bucket", "key")

    asyncio.run(main())


def test_closed_transfer_manager(tm_config: dict[str, Any], bucket: str) -> None:
    tm = TransferManager(**tm_config)
    tm.close()
    tm.close()

    with pytest.raises(RuntimeError, match="closed"):
        tm.upload(b"data", bucket, "key")


def test_transfers_outlive_close(tm_config: dict[str, Any], bucket: str) -> None:
    with TransferManager(**tm_config) as tm:
        tm.upload(b"data", bucket, "key")
        stream = tm.download(bucket, "key")

    assert stream.read() == b"data"
