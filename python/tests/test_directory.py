from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from aws_s3_transfer import TransferManager

MIB = 1024 * 1024


@pytest.fixture
def source(tmp_path: Path) -> dict[str, bytes]:
    files = {
        "top.txt": b"top",
        "nested/middle.bin": os.urandom(6 * MIB),
        "nested/deeper/bottom.txt": b"",
    }
    for name, data in files.items():
        path = tmp_path / "source" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return files


def list_keys(s3: Any, bucket: str) -> list[str]:
    return sorted(item["Key"] for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []))


def test_upload_directory(
    tm: TransferManager,
    bucket: str,
    s3: Any,
    read_object: Callable[[str], bytes],
    source: dict[str, bytes],
    tmp_path: Path,
) -> None:
    reports: list[int] = []

    output = tm.upload_directory(
        tmp_path / "source", bucket, key_prefix="prefix/", progress=reports.append
    )

    assert output.objects_uploaded == len(source)
    assert output.failures == ()
    assert list_keys(s3, bucket) == sorted(f"prefix/{name}" for name in source)
    assert read_object("prefix/nested/middle.bin") == source["nested/middle.bin"]
    assert sum(reports) == sum(len(data) for data in source.values())


def test_upload_directory_non_recursive(
    tm: TransferManager, bucket: str, s3: Any, source: dict[str, bytes], tmp_path: Path
) -> None:
    output = tm.upload_directory(tmp_path / "source", bucket, recursive=False)

    assert output.objects_uploaded == 1
    assert list_keys(s3, bucket) == ["top.txt"]


def test_download_directory(
    tm: TransferManager, bucket: str, source: dict[str, bytes], tmp_path: Path
) -> None:
    tm.upload_directory(tmp_path / "source", bucket, key_prefix="prefix/")
    destination = tmp_path / "new" / "destination"

    output = tm.download_directory(bucket, destination, key_prefix="prefix/")

    assert output.objects_downloaded == len(source)
    assert output.failures == ()
    for name, data in source.items():
        assert (destination / name).read_bytes() == data


def test_directory_errors(tm: TransferManager, bucket: str, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        tm.upload_directory(tmp_path / "missing", bucket)
    (tmp_path / "file").write_bytes(b"")
    with pytest.raises(NotADirectoryError):
        tm.upload_directory(tmp_path / "file", bucket)
    with pytest.raises(NotADirectoryError):
        tm.download_directory(bucket, tmp_path / "file")
    with pytest.raises(ValueError, match="failure_policy"):
        tm.upload_directory(tmp_path, bucket, failure_policy="ignore")  # type: ignore[arg-type]
