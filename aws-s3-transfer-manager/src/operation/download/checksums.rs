/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// The level of checksum validation performed on a download
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChecksumValidationLevel {
    /// Checksum validation was not performed on all downloaded data.
    ///
    /// Note this DOES NOT mean the checksum didn't match. That would have failed the download with a
    /// [ChecksumMismatch] error.
    ///
    /// There are many reasons for `NotValidated`, including:
    /// -   The object had no checksum.
    ///     -   Objects uploaded before 2025 are unlikely to have a checksum.
    ///         In late-2024/early-2025 Amazon S3 began automatically calculating and storing checksums ([blog post]).
    ///         The exact date for this varies by region.
    ///     -   Third parties that mimic the Amazon S3 API may not provide a checksum.
    /// -   The object was downloaded in chunks, and one or more chunks had no checksum.
    ///     -   This happens when a large object with a [FullObject checksum][ChecksumTypes]
    ///         is downloaded in multiple chunks.
    ///     -   This happens when an object with a [Composite checksum][ChecksumTypes]
    ///         is downloaded in chunks that don't align with the [PartSize] it was uploaded with.
    /// -   Checksum validation was disabled in the underlying The S3 client,
    ///     by configuring it with the non-default
    ///     [`aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired`].
    ///
    /// [ChecksumMismatch]: https://docs.rs/aws-smithy-checksums/latest/aws_smithy_checksums/body/validate/enum.Error.html#variant.ChecksumMismatch
    /// [ChecksumTypes]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#ChecksumTypes
    /// [blog post]: https://aws.amazon.com/blogs/aws/introducing-default-data-integrity-protections-for-new-objects-in-amazon-s3/
    /// [PartSize]: crate::types::PartSize
    #[default]
    NotValidated,
    /// The checksum of each downloaded chunk was validated, but the
    /// [FullObject or Composite checksum][ChecksumTypes] for the whole object
    /// was not validated.
    ///
    /// This can happen if:
    /// - A large object is downloaded in multiple chunks.
    /// - You requested a range of the object to download, not the full object.
    ///
    /// [ChecksumTypes]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#ChecksumTypes
    AllChunks,
    /// The full object was downloaded, and its checksum was validated.
    FullObject,
}
