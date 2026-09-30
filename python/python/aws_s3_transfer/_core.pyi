from collections.abc import Callable, Mapping
from os import PathLike
from typing import Any

__version__: str

class Operation:
    def wait(self, progress: Callable[[int], object] | None = None) -> Any: ...
    async def wait_async(self, progress: Callable[[int], object] | None = None) -> Any: ...

class DownloadStream:
    @property
    def metadata(self) -> Any: ...
    def next_chunk(self) -> bytes | None: ...
    async def next_chunk_async(self) -> bytes | None: ...
    def output(self) -> Any: ...
    def close(self) -> None: ...
    async def close_async(self) -> None: ...

class Client:
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
    ) -> None: ...
    def load(self) -> Operation: ...
    def close(self) -> None: ...
    def upload(
        self,
        body: object,
        bucket: str,
        key: str,
        options: Mapping[str, Any] | None = None,
        content_length: int | None = None,
        allow_async: bool = False,
    ) -> Operation: ...
    def upload_file(
        self,
        filename: str | PathLike[str],
        bucket: str,
        key: str,
        options: Mapping[str, Any] | None = None,
    ) -> Operation: ...
    def download(
        self, bucket: str, key: str, options: Mapping[str, Any] | None = None
    ) -> Operation: ...
    def download_file(
        self,
        bucket: str,
        key: str,
        filename: str | PathLike[str],
        options: Mapping[str, Any] | None = None,
    ) -> Operation: ...
    def upload_directory(
        self,
        directory: str | PathLike[str],
        bucket: str,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        recursive: bool = True,
        follow_symlinks: bool = False,
        failure_policy: str = "abort",
        max_concurrent_uploads: int | None = None,
    ) -> Operation: ...
    def download_directory(
        self,
        bucket: str,
        directory: str | PathLike[str],
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        failure_policy: str = "abort",
        max_concurrent_downloads: int | None = None,
    ) -> Operation: ...
