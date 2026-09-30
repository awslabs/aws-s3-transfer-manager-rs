from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import DownloadFailure, UploadFailure


class TransferError(Exception):
    """Base class for errors raised by a transfer."""


class InvalidInputError(TransferError, ValueError):
    """The transfer manager rejected a request as invalid."""


class ServiceError(TransferError):
    """Amazon S3 returned an error."""

    operation_name: str | None = None
    """The S3 operation that failed, for example ``"GetObject"``."""
    code: str | None = None
    """The S3 error code, for example ``"AccessDenied"``."""
    message: str | None = None
    """The error message returned by S3."""
    request_id: str | None = None
    extended_request_id: str | None = None


class NotFoundError(ServiceError):
    """The requested object, bucket, or upload does not exist."""


class ChecksumMismatchError(TransferError):
    """Downloaded data did not match the checksum reported by S3."""

    algorithm: str | None = None
    expected: str | None = None
    computed: str | None = None


class TransferCancelledError(TransferError):
    """The transfer was cancelled."""


class DirectoryTransferError(TransferError):
    """A directory transfer stopped because an object failed to transfer."""

    failures: tuple[UploadFailure | DownloadFailure, ...] = ()
