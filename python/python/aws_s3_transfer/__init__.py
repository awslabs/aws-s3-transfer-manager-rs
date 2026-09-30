"""A high-performance transfer manager for Amazon S3.

This package is experimental: its API may change without notice between releases.
"""

from ._core import __version__
from ._errors import (
    ChecksumMismatchError,
    DirectoryTransferError,
    InvalidInputError,
    NotFoundError,
    ServiceError,
    TransferCancelledError,
    TransferError,
)
from ._models import (
    DownloadDirectoryOutput,
    DownloadFailure,
    DownloadOptions,
    DownloadOutput,
    IntegrityChecks,
    ObjectMetadata,
    UploadDirectoryOutput,
    UploadFailure,
    UploadOptions,
    UploadOutput,
)
from ._transfer_manager import (
    AsyncDownloadStream,
    AsyncTransferManager,
    DownloadStream,
    TransferManager,
)

__all__ = [
    "AsyncDownloadStream",
    "AsyncTransferManager",
    "ChecksumMismatchError",
    "DirectoryTransferError",
    "DownloadDirectoryOutput",
    "DownloadFailure",
    "DownloadOptions",
    "DownloadOutput",
    "DownloadStream",
    "IntegrityChecks",
    "InvalidInputError",
    "NotFoundError",
    "ObjectMetadata",
    "ServiceError",
    "TransferCancelledError",
    "TransferError",
    "TransferManager",
    "UploadDirectoryOutput",
    "UploadFailure",
    "UploadOptions",
    "UploadOutput",
    "__version__",
]
