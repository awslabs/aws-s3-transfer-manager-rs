from __future__ import annotations

import _thread
import io
import os
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from aws_s3_transfer import TransferManager

MIB = 1024 * 1024

Reader = Callable[[str], bytes]


@pytest.mark.parametrize("body_type", [bytes, bytearray, memoryview])
def test_upload_bytes_like(
    tm: TransferManager, bucket: str, read_object: Reader, body_type: type[Any]
) -> None:
    output = tm.upload(body_type(b"hello world"), bucket, "key")

    assert output.e_tag
    assert output.upload_id is None
    assert read_object("key") == b"hello world"


def test_upload_multipart(tm: TransferManager, bucket: str, read_object: Reader) -> None:
    data = os.urandom(12 * MIB)

    output = tm.upload(data, bucket, "key")

    assert output.upload_id is not None
    assert read_object("key") == data


@pytest.mark.parametrize("size", [0, 1, 12 * MIB])
def test_upload_seekable_file_object(
    tm: TransferManager, bucket: str, read_object: Reader, size: int
) -> None:
    data = os.urandom(size)
    stream = io.BytesIO(b"skipped" + data)
    stream.seek(len(b"skipped"))

    tm.upload(stream, bucket, "key")

    assert read_object("key") == data


class NonSeekable(io.RawIOBase):
    def __init__(self, data: bytes) -> None:
        self._data = io.BytesIO(data)

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        return self._data.readinto(buffer)


@pytest.mark.parametrize("size", [100, 17 * MIB])
def test_upload_stream_of_unknown_size(
    tm: TransferManager, bucket: str, read_object: Reader, size: int
) -> None:
    data = os.urandom(size)

    output = tm.upload(NonSeekable(data), bucket, "key")

    assert (output.upload_id is not None) == (size >= 5 * MIB)
    assert read_object("key") == data


def test_upload_iterable(tm: TransferManager, bucket: str, read_object: Reader) -> None:
    chunks = [os.urandom(3 * MIB) for _ in range(4)]

    tm.upload(iter([b"", *chunks, b""]), bucket, "key")

    assert read_object("key") == b"".join(chunks)


def test_upload_declared_content_length_mismatch(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(ValueError, match="declared size"):
        tm.upload(iter([b"abc"]), bucket, "key", content_length=4)


def test_upload_propagates_body_errors(tm: TransferManager, bucket: str) -> None:
    def chunks() -> Iterator[bytes]:
        yield os.urandom(6 * MIB)
        raise LookupError("source failed")

    with pytest.raises(LookupError, match="source failed"):
        tm.upload(chunks(), bucket, "key")


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("text", "encode str bodies"),
        (42, "unsupported upload body type"),
        (iter(["text"]), "expected a bytes-like object"),
    ],
)
def test_upload_rejects_invalid_bodies(
    tm: TransferManager, bucket: str, body: Any, message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        tm.upload(body, bucket, "key")


def test_upload_rejects_async_bodies(tm: TransferManager, bucket: str) -> None:
    async def chunks() -> Any:
        yield b"data"

    with pytest.raises(TypeError, match="AsyncTransferManager"):
        tm.upload(chunks(), bucket, "key")


def test_upload_file(tm: TransferManager, bucket: str, read_object: Reader, tmp_path: Path) -> None:
    data = os.urandom(11 * MIB)
    path = tmp_path / "file.bin"
    path.write_bytes(data)

    tm.upload_file(path, bucket, "from-path")
    tm.upload_file(str(path), bucket, "from-str")

    assert read_object("from-path") == data
    assert read_object("from-str") == data


def test_upload_file_errors(tm: TransferManager, bucket: str, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError) as missing:
        tm.upload_file(tmp_path / "missing", bucket, "key")
    assert missing.value.filename == str(tmp_path / "missing")

    with pytest.raises(IsADirectoryError):
        tm.upload_file(tmp_path, bucket, "key")


def test_upload_options(tm: TransferManager, bucket: str, s3: Any) -> None:
    tm.upload(
        b"{}",
        bucket,
        "key",
        content_type="application/json",
        cache_control="no-cache",
        metadata={"owner": "tests"},
        checksum_algorithm="CRC32",
    )

    head = s3.head_object(Bucket=bucket, Key="key")
    assert head["ContentType"] == "application/json"
    assert head["CacheControl"] == "no-cache"
    assert head["Metadata"] == {"owner": "tests"}


def test_upload_rejects_invalid_options(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'colour'"):
        tm.upload(b"", bucket, "key", colour="blue")  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="invalid value for option 'metadata'"):
        tm.upload(b"", bucket, "key", metadata="owner=tests")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="checksum_algorithm"):
        tm.upload(b"", bucket, "key", checksum_type="COMPOSITE")
    with pytest.raises(ValueError, match="does not support"):
        tm.upload(b"", bucket, "key", checksum_algorithm="SHA256", checksum_type="FULL_OBJECT")


def test_upload_reports_progress(tm: TransferManager, bucket: str) -> None:
    data = os.urandom(12 * MIB)
    reports: list[int] = []
    callback_threads: set[int] = set()

    def progress(transferred: int) -> None:
        reports.append(transferred)
        callback_threads.add(threading.get_ident())

    tm.upload(data, bucket, "key", progress=progress)

    assert sum(reports) == len(data)
    assert callback_threads == {threading.get_ident()}


def test_upload_propagates_progress_errors(tm: TransferManager, bucket: str) -> None:
    def progress(transferred: int) -> None:
        raise RuntimeError("progress failed")

    with pytest.raises(RuntimeError, match="progress failed"):
        tm.upload(os.urandom(6 * MIB), bucket, "key", progress=progress)


def test_upload_is_interruptible(tm: TransferManager, bucket: str) -> None:
    def slow_chunks() -> Iterator[bytes]:
        while True:
            time.sleep(0.05)
            yield b"x" * 1024

    threading.Timer(0.3, _thread.interrupt_main).start()
    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        tm.upload(slow_chunks(), bucket, "key")
    assert time.monotonic() - started < 5
