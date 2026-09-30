from __future__ import annotations

import os
from collections.abc import AsyncIterable, Callable, Coroutine, Generator, Iterable
from types import TracebackType
from typing import Any, ClassVar, Literal, Protocol, Self, TypeAlias, Unpack

from . import _core
from ._models import (
    DownloadDirectoryOutput,
    DownloadOptions,
    DownloadOutput,
    ObjectMetadata,
    UploadDirectoryOutput,
    UploadOptions,
    UploadOutput,
)


class _Reader(Protocol):
    def read(self, size: int, /) -> bytes: ...


class _AsyncReader(Protocol):
    async def read(self, size: int, /) -> bytes: ...


StrPath: TypeAlias = str | os.PathLike[str]
ProgressCallback: TypeAlias = Callable[[int], object]
FailurePolicy: TypeAlias = Literal["abort", "continue"]
UploadBody: TypeAlias = bytes | bytearray | memoryview | _Reader | Iterable[bytes]
AsyncUploadBody: TypeAlias = UploadBody | _AsyncReader | AsyncIterable[bytes]


class _TransferManagerBase:
    __slots__ = ("_core",)

    _load_eagerly: ClassVar[bool]

    def __init__(
        self,
        *,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        force_path_style: bool | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        part_size: int | None = None,
        multipart_threshold: int | None = None,
        max_concurrency: int | None = None,
        target_throughput_gbps: int | None = None,
        memory_limit: int | None = None,
        read_ahead: int | None = None,
    ) -> None:
        """Creates a transfer manager.

        Settings that are not given are resolved from the environment, shared
        AWS config and credentials files, and instance metadata, as in other
        AWS SDKs.

        Args:
            region_name: The AWS Region of the buckets to access.
            profile_name: The named profile to load settings from.
            endpoint_url: A custom endpoint, for example an S3-compatible service.
            force_path_style: Whether to address buckets in the URL path rather
                than the host name.
            aws_access_key_id: The access key for static credentials.
            aws_secret_access_key: The secret key for static credentials.
            aws_session_token: The session token for temporary static credentials.
            part_size: The target size in bytes of each part of a multipart
                transfer. Chosen automatically by default.
            multipart_threshold: The object size in bytes at which uploads are
                split into parts. Chosen automatically by default.
            max_concurrency: The maximum number of concurrent requests across
                all transfers. Chosen from the host's network capacity by default.
            target_throughput_gbps: A throughput target in gigabits per second
                from which to derive concurrency, instead of ``max_concurrency``.
            memory_limit: The maximum number of bytes of transfer data to
                buffer in memory. Derived from available memory by default.
            read_ahead: The default number of parts a download may fetch ahead
                of its consumer.
        """
        self._core = _core.Client(
            region_name=region_name,
            profile_name=profile_name,
            endpoint_url=endpoint_url,
            force_path_style=force_path_style,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            part_size=part_size,
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            target_throughput_gbps=target_throughput_gbps,
            memory_limit=memory_limit,
            read_ahead=read_ahead,
        )
        if self._load_eagerly:
            self._core.load().wait()


class TransferManager(_TransferManagerBase):
    """Transfers objects between Amazon S3 and local storage.

    Each method blocks until its transfer completes. Large objects are split
    into parts that are transferred in parallel. Interrupting a transfer, for
    example with Ctrl-C, cancels it.

    A transfer manager is safe to share between threads. Use it as a context
    manager, or call :meth:`close`, to release its resources.
    """

    __slots__ = ()

    _load_eagerly = True

    def upload(
        self,
        body: UploadBody,
        bucket: str,
        key: str,
        *,
        content_length: int | None = None,
        progress: ProgressCallback | None = None,
        **options: Unpack[UploadOptions],
    ) -> UploadOutput:
        """Uploads an object from memory or a stream.

        Args:
            body: The object's contents: a bytes-like object, a binary file
                object, or an iterable of bytes-like objects. Streams are read
                from a background thread.
            bucket: The bucket to upload to.
            key: The key of the object.
            content_length: The size of a streamed body, if known. Seekable
                file objects are measured automatically.
            progress: Called with the number of bytes transferred since the
                previous call.
            **options: Additional request parameters; see :class:`UploadOptions`.
        """
        operation = self._core.upload(body, bucket, key, options, content_length)
        result: UploadOutput = operation.wait(progress)
        return result

    def upload_file(
        self,
        filename: StrPath,
        bucket: str,
        key: str,
        *,
        progress: ProgressCallback | None = None,
        **options: Unpack[UploadOptions],
    ) -> UploadOutput:
        """Uploads a local file, reading its parts in parallel.

        Args:
            filename: The path of the file to upload.
            bucket: The bucket to upload to.
            key: The key of the object.
            progress: Called with the number of bytes transferred since the
                previous call.
            **options: Additional request parameters; see :class:`UploadOptions`.
        """
        result: UploadOutput = self._core.upload_file(filename, bucket, key, options).wait(progress)
        return result

    def download(self, bucket: str, key: str, **options: Unpack[DownloadOptions]) -> DownloadStream:
        """Starts downloading an object, returning once its metadata is known.

        Iterate over the returned stream to receive the object's contents in
        order. Close the stream, or use it as a context manager, to cancel the
        download if it is not read to the end.

        Args:
            bucket: The bucket to download from.
            key: The key of the object.
            **options: Additional request parameters; see :class:`DownloadOptions`.
        """
        stream: _core.DownloadStream = self._core.download(bucket, key, options).wait()
        return DownloadStream(stream)

    def download_file(
        self,
        bucket: str,
        key: str,
        filename: StrPath,
        *,
        progress: ProgressCallback | None = None,
        **options: Unpack[DownloadOptions],
    ) -> DownloadOutput:
        """Downloads an object to a local file, writing its parts in parallel.

        The object is written to a temporary file in the same directory, which
        replaces ``filename`` once the download succeeds.

        Args:
            bucket: The bucket to download from.
            key: The key of the object.
            filename: The path to write the object to.
            progress: Called with the number of bytes transferred since the
                previous call.
            **options: Additional request parameters; see :class:`DownloadOptions`.
        """
        result: DownloadOutput = self._core.download_file(bucket, key, filename, options).wait(
            progress
        )
        return result

    def upload_directory(
        self,
        directory: StrPath,
        bucket: str,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        recursive: bool = True,
        follow_symlinks: bool = False,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_uploads: int | None = None,
        progress: ProgressCallback | None = None,
    ) -> UploadDirectoryOutput:
        """Uploads the files in a local directory.

        Each file's key is its path relative to ``directory``, joined with
        ``delimiter`` and prefixed with ``key_prefix``.

        Args:
            directory: The directory to upload.
            bucket: The bucket to upload to.
            key_prefix: A prefix for every uploaded key.
            delimiter: The key separator that replaces the local path separator.
                Defaults to ``"/"``.
            recursive: Whether to upload the contents of subdirectories.
            follow_symlinks: Whether to follow symbolic links. Symbolic links
                are skipped by default.
            failure_policy: ``"abort"`` to stop at the first failure and raise
                :exc:`DirectoryTransferError`, or ``"continue"`` to upload the
                remaining files and report failures in the output.
            max_concurrent_uploads: The maximum number of files to upload at once.
            progress: Called with the number of bytes transferred since the
                previous call.
        """
        operation = self._core.upload_directory(
            directory,
            bucket,
            key_prefix=key_prefix,
            delimiter=delimiter,
            recursive=recursive,
            follow_symlinks=follow_symlinks,
            failure_policy=failure_policy,
            max_concurrent_uploads=max_concurrent_uploads,
        )
        result: UploadDirectoryOutput = operation.wait(progress)
        return result

    def download_directory(
        self,
        bucket: str,
        directory: StrPath,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_downloads: int | None = None,
        progress: ProgressCallback | None = None,
    ) -> DownloadDirectoryOutput:
        """Downloads the objects under a key prefix into a local directory.

        Each object is written to its key relative to ``key_prefix``, split on
        ``delimiter`` into subdirectories of ``directory``.

        Args:
            bucket: The bucket to download from.
            directory: The directory to download into, created if it does not exist.
            key_prefix: Only objects whose keys start with this are downloaded.
            delimiter: The key separator that maps to the local path separator.
                Defaults to ``"/"``.
            failure_policy: ``"abort"`` to stop at the first failure and raise
                :exc:`DirectoryTransferError`, or ``"continue"`` to download the
                remaining objects and report failures in the output.
            max_concurrent_downloads: The maximum number of objects to download
                at once.
            progress: Called with the number of bytes transferred since the
                previous call.
        """
        operation = self._core.download_directory(
            bucket,
            directory,
            key_prefix=key_prefix,
            delimiter=delimiter,
            failure_policy=failure_policy,
            max_concurrent_downloads=max_concurrent_downloads,
        )
        result: DownloadDirectoryOutput = operation.wait(progress)
        return result

    def close(self) -> None:
        """Releases the transfer manager's resources. Transfers in progress run to completion."""
        self._core.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncTransferManager(_TransferManagerBase):
    """Transfers objects between Amazon S3 and local storage from ``asyncio`` code.

    Large objects are split into parts that are transferred in parallel.
    Cancelling the task awaiting a transfer cancels the transfer.

    It loads its configuration when first used. Use it as an async
    context manager, or call :meth:`aclose`, to release its resources.
    """

    __slots__ = ()

    _load_eagerly = False

    async def upload(
        self,
        body: AsyncUploadBody,
        bucket: str,
        key: str,
        *,
        content_length: int | None = None,
        progress: ProgressCallback | None = None,
        **options: Unpack[UploadOptions],
    ) -> UploadOutput:
        """Uploads an object from memory or a stream.

        Args:
            body: The object's contents: a bytes-like object, a binary file
                object (with a regular or coroutine ``read`` method), or a
                synchronous or asynchronous iterable of bytes-like objects.
                Synchronous streams are read from a background thread.
            bucket: The bucket to upload to.
            key: The key of the object.
            content_length: The size of a streamed body, if known. Seekable
                file objects are measured automatically.
            progress: Called on the event loop with the number of bytes
                transferred since the previous call.
            **options: Additional request parameters; see :class:`UploadOptions`.
        """
        operation = self._core.upload(body, bucket, key, options, content_length, allow_async=True)
        result: UploadOutput = await operation.wait_async(progress)
        return result

    async def upload_file(
        self,
        filename: StrPath,
        bucket: str,
        key: str,
        *,
        progress: ProgressCallback | None = None,
        **options: Unpack[UploadOptions],
    ) -> UploadOutput:
        """Uploads a local file, reading its parts in parallel.

        Args:
            filename: The path of the file to upload.
            bucket: The bucket to upload to.
            key: The key of the object.
            progress: Called on the event loop with the number of bytes
                transferred since the previous call.
            **options: Additional request parameters; see :class:`UploadOptions`.
        """
        operation = self._core.upload_file(filename, bucket, key, options)
        result: UploadOutput = await operation.wait_async(progress)
        return result

    def download(
        self, bucket: str, key: str, **options: Unpack[DownloadOptions]
    ) -> _AsyncDownloadStarter:
        """Starts downloading an object.

        Await the result, or enter it with ``async with``, to get a stream once
        the object's metadata is known. Iterate over the stream with
        ``async for`` to receive the object's contents in order. Leaving the
        ``async with`` block, or closing the stream, cancels the download if
        it was not read to the end.

        Args:
            bucket: The bucket to download from.
            key: The key of the object.
            **options: Additional request parameters; see :class:`DownloadOptions`.
        """
        return _AsyncDownloadStarter(self._open_download(bucket, key, options))

    async def _open_download(
        self, bucket: str, key: str, options: DownloadOptions
    ) -> AsyncDownloadStream:
        stream: _core.DownloadStream = await self._core.download(bucket, key, options).wait_async()
        return AsyncDownloadStream(stream)

    async def download_file(
        self,
        bucket: str,
        key: str,
        filename: StrPath,
        *,
        progress: ProgressCallback | None = None,
        **options: Unpack[DownloadOptions],
    ) -> DownloadOutput:
        """Downloads an object to a local file, writing its parts in parallel.

        The object is written to a temporary file in the same directory, which
        replaces ``filename`` once the download succeeds.

        Args:
            bucket: The bucket to download from.
            key: The key of the object.
            filename: The path to write the object to.
            progress: Called on the event loop with the number of bytes
                transferred since the previous call.
            **options: Additional request parameters; see :class:`DownloadOptions`.
        """
        operation = self._core.download_file(bucket, key, filename, options)
        result: DownloadOutput = await operation.wait_async(progress)
        return result

    async def upload_directory(
        self,
        directory: StrPath,
        bucket: str,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        recursive: bool = True,
        follow_symlinks: bool = False,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_uploads: int | None = None,
        progress: ProgressCallback | None = None,
    ) -> UploadDirectoryOutput:
        """Uploads the files in a local directory.

        See :meth:`TransferManager.upload_directory` for a description of the arguments.
        """
        operation = self._core.upload_directory(
            directory,
            bucket,
            key_prefix=key_prefix,
            delimiter=delimiter,
            recursive=recursive,
            follow_symlinks=follow_symlinks,
            failure_policy=failure_policy,
            max_concurrent_uploads=max_concurrent_uploads,
        )
        result: UploadDirectoryOutput = await operation.wait_async(progress)
        return result

    async def download_directory(
        self,
        bucket: str,
        directory: StrPath,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_downloads: int | None = None,
        progress: ProgressCallback | None = None,
    ) -> DownloadDirectoryOutput:
        """Downloads the objects under a key prefix into a local directory.

        See :meth:`TransferManager.download_directory` for a description of the arguments.
        """
        operation = self._core.download_directory(
            bucket,
            directory,
            key_prefix=key_prefix,
            delimiter=delimiter,
            failure_policy=failure_policy,
            max_concurrent_downloads=max_concurrent_downloads,
        )
        result: DownloadDirectoryOutput = await operation.wait_async(progress)
        return result

    async def aclose(self) -> None:
        """Releases the transfer manager's resources. Transfers in progress run to completion."""
        self._core.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


class DownloadStream:
    """The contents of an object being downloaded by :meth:`TransferManager.download`.

    Iterating yields the object's contents in order, in chunks of up to the
    configured part size. Reading the stream to the end verifies the object's
    checksum, when one is available.
    """

    __slots__ = ("_core", "_pending")

    def __init__(self, core: _core.DownloadStream) -> None:
        self._core = core
        self._pending = memoryview(b"")

    @property
    def metadata(self) -> ObjectMetadata:
        """The object's metadata."""
        metadata: ObjectMetadata = self._core.metadata
        return metadata

    @property
    def output(self) -> DownloadOutput | None:
        """The download's output, once the stream has been read to the end."""
        output: DownloadOutput | None = self._core.output()
        return output

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        if self._pending:
            chunk = self._pending.tobytes()
            self._pending = memoryview(b"")
            return chunk
        chunk_or_end: bytes | None = self._core.next_chunk()
        if chunk_or_end is None:
            raise StopIteration
        return chunk_or_end

    def read(self, size: int = -1) -> bytes:
        """Reads up to ``size`` bytes, or to the end of the object if ``size`` is negative.

        Returns fewer than ``size`` bytes only at the end of the object.
        """
        if size < 0:
            return b"".join(self)
        data = bytearray()
        while len(data) < size:
            if not self._pending:
                chunk: bytes | None = self._core.next_chunk()
                if chunk is None:
                    break
                self._pending = memoryview(chunk)
            wanted = size - len(data)
            data += self._pending[:wanted]
            self._pending = self._pending[wanted:]
        return bytes(data)

    def close(self) -> None:
        """Cancels the download if it has not finished."""
        self._core.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncDownloadStream:
    """The contents of an object being downloaded by :meth:`AsyncTransferManager.download`.

    Iterating with ``async for`` yields the object's contents in order, in
    chunks of up to the configured part size. Reading the stream to the end
    verifies the object's checksum, when one is available.
    """

    __slots__ = ("_core", "_pending")

    def __init__(self, core: _core.DownloadStream) -> None:
        self._core = core
        self._pending = memoryview(b"")

    @property
    def metadata(self) -> ObjectMetadata:
        """The object's metadata."""
        metadata: ObjectMetadata = self._core.metadata
        return metadata

    @property
    def output(self) -> DownloadOutput | None:
        """The download's output, once the stream has been read to the end."""
        output: DownloadOutput | None = self._core.output()
        return output

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        if self._pending:
            chunk = self._pending.tobytes()
            self._pending = memoryview(b"")
            return chunk
        chunk_or_end: bytes | None = await self._core.next_chunk_async()
        if chunk_or_end is None:
            raise StopAsyncIteration
        return chunk_or_end

    async def read(self, size: int = -1) -> bytes:
        """Reads up to ``size`` bytes, or to the end of the object if ``size`` is negative.

        Returns fewer than ``size`` bytes only at the end of the object.
        """
        if size < 0:
            return b"".join([chunk async for chunk in self])
        data = bytearray()
        while len(data) < size:
            if not self._pending:
                chunk: bytes | None = await self._core.next_chunk_async()
                if chunk is None:
                    break
                self._pending = memoryview(chunk)
            wanted = size - len(data)
            data += self._pending[:wanted]
            self._pending = self._pending[wanted:]
        return bytes(data)

    async def aclose(self) -> None:
        """Cancels the download if it has not finished."""
        await self._core.close_async()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


class _AsyncDownloadStarter:
    """Awaitable, and usable with ``async with``, to open an :class:`AsyncDownloadStream`."""

    __slots__ = ("_opening", "_stream")

    def __init__(self, opening: Coroutine[Any, Any, AsyncDownloadStream]) -> None:
        self._opening = opening
        self._stream: AsyncDownloadStream | None = None

    def __await__(self) -> Generator[Any, None, AsyncDownloadStream]:
        return self._opening.__await__()

    async def __aenter__(self) -> AsyncDownloadStream:
        self._stream = await self._opening
        return self._stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._stream is not None:
            await self._stream.aclose()
