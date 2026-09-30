from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from aws_s3_transfer import NotFoundError, TransferManager

MIB = 1024 * 1024


@pytest.fixture
def data(tm: TransferManager, bucket: str) -> bytes:
    data = os.urandom(12 * MIB)
    tm.upload(data, bucket, "key", content_type="application/octet-stream")
    return data


def test_download_stream(tm: TransferManager, bucket: str, data: bytes) -> None:
    with tm.download(bucket, "key") as stream:
        assert stream.metadata.object_size == len(data)
        assert stream.metadata.content_type == "application/octet-stream"
        assert isinstance(stream.metadata.last_modified, datetime)
        assert stream.output is None
        chunks = list(stream)

    assert len(chunks) > 1
    assert b"".join(chunks) == data
    assert stream.output is not None
    assert stream.output.metadata == stream.metadata


def test_download_stream_read(tm: TransferManager, bucket: str, data: bytes) -> None:
    with tm.download(bucket, "key") as stream:
        head = stream.read(10)
        middle = stream.read(6 * MIB)
        rest = next(stream) + stream.read()
        assert stream.read(1) == b""

    assert head == data[:10]
    assert middle == data[10 : 10 + 6 * MIB]
    assert head + middle + rest == data


def test_download_range(tm: TransferManager, bucket: str, data: bytes) -> None:
    with tm.download(bucket, "key", range="bytes=100-199") as stream:
        assert stream.read() == data[100:200]
        assert stream.metadata.object_size == len(data)


def test_download_stream_close_cancels(tm: TransferManager, bucket: str, data: bytes) -> None:
    stream = tm.download(bucket, "key")
    stream.read(1)
    stream.close()

    with pytest.raises(ValueError, match="closed"):
        stream.read()


def test_download_missing_object(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(NotFoundError) as error:
        tm.download(bucket, "missing")

    assert error.value.code == "NoSuchKey"
    assert error.value.operation_name


def test_download_file(tm: TransferManager, bucket: str, data: bytes, tmp_path: Path) -> None:
    reports: list[int] = []

    output = tm.download_file(bucket, "key", tmp_path / "file", progress=reports.append)
    tm.download_file(bucket, "key", str(tmp_path / "file-from-str"))

    assert (tmp_path / "file").read_bytes() == data
    assert (tmp_path / "file-from-str").read_bytes() == data
    assert output.metadata.object_size == len(data)
    assert sum(reports) == len(data)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["file", "file-from-str"]


def test_download_file_errors(
    tm: TransferManager, bucket: str, data: bytes, tmp_path: Path
) -> None:
    with pytest.raises(FileNotFoundError):
        tm.download_file(bucket, "key", tmp_path / "missing" / "file")
    with pytest.raises(IsADirectoryError):
        tm.download_file(bucket, "key", tmp_path)
    with pytest.raises(NotFoundError):
        tm.download_file(bucket, "missing", tmp_path / "file")
    assert list(tmp_path.iterdir()) == []


def test_download_options(tm: TransferManager, bucket: str, data: bytes, s3: Any) -> None:
    etag = s3.head_object(Bucket=bucket, Key="key")["ETag"]

    with tm.download(bucket, "key", if_match=etag, read_ahead=1) as stream:
        assert stream.read() == data

    with pytest.raises(TypeError, match="invalid value for option 'if_modified_since'"):
        tm.download(bucket, "key", if_modified_since="yesterday")  # type: ignore[arg-type]
