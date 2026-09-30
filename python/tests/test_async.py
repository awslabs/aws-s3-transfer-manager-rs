from __future__ import annotations

import asyncio
import io
import os
import threading
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

from aws_s3_transfer import AsyncTransferManager, NotFoundError

MIB = 1024 * 1024


def run(config: dict[str, Any], test: Callable[[AsyncTransferManager], Any]) -> None:
    async def main() -> None:
        async with AsyncTransferManager(**config) as tm:
            await test(tm)

    asyncio.run(main())


class AsyncReader:
    def __init__(self, data: bytes) -> None:
        self._data = io.BytesIO(data)

    async def read(self, size: int) -> bytes:
        await asyncio.sleep(0)
        return self._data.read(size)


def test_upload_and_download(
    tm_config: dict[str, Any], bucket: str, read_object: Callable[[str], bytes]
) -> None:
    data = os.urandom(12 * MIB)

    async def test(tm: AsyncTransferManager) -> None:
        output = await tm.upload(data, bucket, "key")
        assert output.upload_id is not None

        async with tm.download(bucket, "key") as stream:
            assert stream.metadata.object_size == len(data)
            chunks = [chunk async for chunk in stream]
        assert b"".join(chunks) == data
        assert stream.output is not None

        stream = await tm.download(bucket, "key")
        async with stream:
            assert await stream.read(3) == data[:3]
            assert await stream.read() == data[3:]

    run(tm_config, test)
    assert read_object("key") == data


@pytest.mark.parametrize("size", [10, 12 * MIB])
def test_upload_async_sources(
    tm_config: dict[str, Any], bucket: str, read_object: Callable[[str], bytes], size: int
) -> None:
    data = os.urandom(size)

    async def chunks() -> AsyncIterator[bytes]:
        for offset in range(0, len(data), MIB):
            await asyncio.sleep(0)
            yield data[offset : offset + MIB]

    async def test(tm: AsyncTransferManager) -> None:
        await tm.upload(chunks(), bucket, "generator")
        await tm.upload(AsyncReader(data), bucket, "reader")
        await tm.upload(io.BytesIO(data), bucket, "file")

    run(tm_config, test)
    for key in ("generator", "reader", "file"):
        assert read_object(key) == data


def test_upload_propagates_async_body_errors(tm_config: dict[str, Any], bucket: str) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield os.urandom(6 * MIB)
        raise LookupError("source failed")

    async def test(tm: AsyncTransferManager) -> None:
        with pytest.raises(LookupError, match="source failed"):
            await tm.upload(chunks(), bucket, "key")

    run(tm_config, test)


def test_files_and_directories(tm_config: dict[str, Any], bucket: str, tmp_path: Path) -> None:
    data = os.urandom(6 * MIB)
    (tmp_path / "source").mkdir()
    (tmp_path / "source" / "file").write_bytes(data)
    (tmp_path / "destination").mkdir()

    async def test(tm: AsyncTransferManager) -> None:
        reports: list[int] = []
        loop_thread = threading.get_ident()

        def progress(transferred: int) -> None:
            assert threading.get_ident() == loop_thread
            reports.append(transferred)

        await tm.upload_file(tmp_path / "source" / "file", bucket, "file", progress=progress)
        await tm.download_file(bucket, "file", tmp_path / "file")
        uploaded = await tm.upload_directory(tmp_path / "source", bucket, key_prefix="dir/")
        downloaded = await tm.download_directory(
            bucket, tmp_path / "destination", key_prefix="dir/"
        )

        await asyncio.sleep(0)
        assert sum(reports) == len(data)
        assert uploaded.objects_uploaded == downloaded.objects_downloaded == 1

    run(tm_config, test)
    assert (tmp_path / "file").read_bytes() == data
    assert (tmp_path / "destination" / "file").read_bytes() == data


def test_errors(tm_config: dict[str, Any], bucket: str) -> None:
    async def test(tm: AsyncTransferManager) -> None:
        with pytest.raises(NotFoundError):
            await tm.download(bucket, "missing")
        with pytest.raises(NotFoundError):
            async with tm.download(bucket, "missing"):
                pass

    run(tm_config, test)


def test_cancellation(tm_config: dict[str, Any], bucket: str) -> None:
    async def endless() -> AsyncIterator[bytes]:
        while True:
            await asyncio.sleep(0.01)
            yield b"x" * 1024

    async def test(tm: AsyncTransferManager) -> None:
        upload = asyncio.ensure_future(tm.upload(endless(), bucket, "key"))
        await asyncio.sleep(0.2)
        upload.cancel()
        with pytest.raises(asyncio.CancelledError):
            await upload

        await tm.upload(b"after", bucket, "key")

    run(tm_config, test)
