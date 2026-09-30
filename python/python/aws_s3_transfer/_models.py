from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    from ._errors import TransferError


class UploadOptions(TypedDict, total=False):
    """Optional request parameters for uploading an object.

    Values mirror the corresponding Amazon S3 ``PutObject`` parameters.
    Enumerated values use their S3 wire value, for example
    ``storage_class="STANDARD_IA"`` or ``server_side_encryption="aws:kms"``.
    """

    acl: str
    bucket_key_enabled: bool
    cache_control: str
    checksum_algorithm: Literal["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"]
    """Algorithm used to calculate the object's checksum."""
    checksum_type: Literal["FULL_OBJECT", "COMPOSITE"]
    """How part checksums are combined when the upload is multipart."""
    full_object_checksum: str
    """Precalculated base64-encoded checksum of the whole object."""
    content_disposition: str
    content_encoding: str
    content_language: str
    content_md5: str
    content_type: str
    expected_bucket_owner: str
    expires: datetime
    grant_full_control: str
    grant_read: str
    grant_read_acp: str
    grant_write_acp: str
    if_match: str
    if_none_match: str
    metadata: Mapping[str, str]
    object_lock_legal_hold_status: str
    object_lock_mode: str
    object_lock_retain_until_date: datetime
    request_payer: str
    server_side_encryption: str
    sse_customer_algorithm: str
    sse_customer_key: str
    sse_customer_key_md5: str
    sse_kms_encryption_context: str
    sse_kms_key_id: str
    storage_class: str
    tagging: str
    website_redirect_location: str


class DownloadOptions(TypedDict, total=False):
    """Optional request parameters for downloading an object.

    Values mirror the corresponding Amazon S3 ``GetObject`` parameters.
    """

    checksum_mode: Literal["ENABLED"]
    expected_bucket_owner: str
    if_match: str
    if_modified_since: datetime
    if_none_match: str
    if_unmodified_since: datetime
    range: str
    """Byte range to download, for example ``"bytes=0-1023"``."""
    read_ahead: int
    """Maximum number of parts to fetch ahead of the consumer."""
    request_payer: str
    response_cache_control: str
    response_content_disposition: str
    response_content_encoding: str
    response_content_language: str
    response_content_type: str
    response_expires: datetime
    sse_customer_algorithm: str
    sse_customer_key: str
    sse_customer_key_md5: str
    version_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class UploadOutput:
    """The result of uploading an object."""

    e_tag: str | None = None
    version_id: str | None = None
    expiration: str | None = None
    checksum_crc32: str | None = None
    checksum_crc32c: str | None = None
    checksum_crc64nvme: str | None = None
    checksum_sha1: str | None = None
    checksum_sha256: str | None = None
    checksum_type: str | None = None
    server_side_encryption: str | None = None
    sse_customer_algorithm: str | None = None
    sse_customer_key_md5: str | None = None
    sse_kms_key_id: str | None = None
    sse_kms_encryption_context: str | None = None
    bucket_key_enabled: bool | None = None
    request_charged: str | None = None
    upload_id: str | None = None
    """The multipart upload ID, if the object was uploaded in parts."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ObjectMetadata:
    """Metadata of a downloaded object."""

    object_size: int
    """Size of the whole object in bytes, even when a range was requested."""
    content_type: str | None = None
    content_encoding: str | None = None
    content_language: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None
    e_tag: str | None = None
    last_modified: datetime | None = None
    version_id: str | None = None
    metadata: Mapping[str, str]
    """User-defined metadata."""
    storage_class: str | None = None
    server_side_encryption: str | None = None
    sse_customer_algorithm: str | None = None
    sse_customer_key_md5: str | None = None
    sse_kms_key_id: str | None = None
    bucket_key_enabled: bool | None = None
    expiration: str | None = None
    expires: str | None = None
    restore: str | None = None
    website_redirect_location: str | None = None
    delete_marker: bool | None = None
    missing_meta: int | None = None
    parts_count: int | None = None
    replication_status: str | None = None
    request_charged: str | None = None
    object_lock_mode: str | None = None
    object_lock_retain_until_date: datetime | None = None
    object_lock_legal_hold_status: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class IntegrityChecks:
    """Checksums reported for a downloaded object and whether they were verified."""

    validated: bool
    """Whether every downloaded byte was verified against a checksum."""
    algorithm: str | None = None
    """The algorithm used for verification, when ``validated`` is true."""
    not_validated_reason: (
        Literal["disabled", "composite_checksum", "partial_coverage", "unavailable", "unknown"]
        | None
    ) = None
    """Why the download was not verified, when ``validated`` is false."""
    checksum_crc32: str | None = None
    checksum_crc32c: str | None = None
    checksum_crc64nvme: str | None = None
    checksum_sha1: str | None = None
    checksum_sha256: str | None = None
    checksum_type: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadOutput:
    """The result of downloading an object."""

    metadata: ObjectMetadata
    integrity: IntegrityChecks


@dataclass(frozen=True, slots=True, kw_only=True)
class UploadFailure:
    """A file that failed to upload as part of a directory upload."""

    path: Path | None
    bucket: str | None
    key: str | None
    error: TransferError


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadFailure:
    """An object that failed to download as part of a directory download."""

    bucket: str | None
    key: str | None
    error: TransferError


@dataclass(frozen=True, slots=True, kw_only=True)
class UploadDirectoryOutput:
    """The result of uploading a directory."""

    objects_uploaded: int
    failures: tuple[UploadFailure, ...] = ()
    """Files that failed to upload when ``failure_policy="continue"``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadDirectoryOutput:
    """The result of downloading a directory."""

    objects_downloaded: int
    failures: tuple[DownloadFailure, ...] = ()
    """Objects that failed to download when ``failure_policy="continue"``."""
