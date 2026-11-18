/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Checksum algorithms a [`ChecksumStrategy`](crate::types::ChecksumStrategy) is built with.
//!
//! [`FullObjectAlgorithm`] and [`CompositeAlgorithm`] list the algorithms S3 supports for each
//! checksum type of a multipart upload.

mod sealed {
    pub trait Sealed {}
}

/// An algorithm the transfer manager can calculate. Implemented only by the types in this module.
pub trait Algorithm: sealed::Sealed + Copy {
    #[doc(hidden)]
    const ALGORITHM: crate::model::ChecksumAlgorithm;
}

/// An algorithm S3 supports as a full object checksum for multipart uploads.
///
/// SHA-1 and SHA-256 have no full object type for multipart uploads; for a checksum of the whole
/// object, use a CRC algorithm. A single-request upload with
/// [`composite(Sha1)`](crate::types::ChecksumStrategy::composite) or `composite(Sha256)` still
/// stores a full object checksum.
///
/// ```no_run
/// use aws_s3_transfer::types::checksum::Crc64Nvme;
/// use aws_s3_transfer::types::ChecksumStrategy;
///
/// let strategy = ChecksumStrategy::full_object(Crc64Nvme);
/// ```
///
/// A SHA algorithm is not a full object algorithm:
///
/// ```compile_fail
/// use aws_s3_transfer::types::checksum::Sha256;
/// use aws_s3_transfer::types::ChecksumStrategy;
///
/// let strategy = ChecksumStrategy::full_object(Sha256);
/// ```
#[diagnostic::on_unimplemented(
    message = "S3 does not support a full object `{Self}` checksum for multipart uploads",
    label = "not a full object algorithm",
    note = "use `ChecksumStrategy::composite`"
)]
pub trait FullObjectAlgorithm: Algorithm {}

/// An algorithm S3 supports as a composite checksum for multipart uploads.
///
/// ```no_run
/// use aws_s3_transfer::types::checksum::Crc32;
/// use aws_s3_transfer::types::ChecksumStrategy;
///
/// let strategy = ChecksumStrategy::composite(Crc32);
/// ```
///
/// CRC64NVME is not a composite algorithm:
///
/// ```compile_fail
/// use aws_s3_transfer::types::checksum::Crc64Nvme;
/// use aws_s3_transfer::types::ChecksumStrategy;
///
/// let strategy = ChecksumStrategy::composite(Crc64Nvme);
/// ```
#[diagnostic::on_unimplemented(
    message = "S3 does not support a composite `{Self}` checksum",
    label = "not a composite algorithm",
    note = "use `ChecksumStrategy::full_object`"
)]
pub trait CompositeAlgorithm: Algorithm {}

macro_rules! algorithm {
    ($(#[doc = $doc:literal] $name:ident => $model:ident;)+) => {
        $(
            #[doc = $doc]
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
            pub struct $name;

            impl sealed::Sealed for $name {}

            impl Algorithm for $name {
                const ALGORITHM: crate::model::ChecksumAlgorithm =
                    crate::model::ChecksumAlgorithm::$model;
            }
        )+
    };
}

algorithm! {
    /// CRC-64/NVME (`CRC64NVME`).
    Crc64Nvme => Crc64Nvme;
    /// CRC-32 (`CRC32`).
    Crc32 => Crc32;
    /// CRC-32C (`CRC32C`).
    Crc32C => Crc32C;
    /// SHA-1 (`SHA1`).
    Sha1 => Sha1;
    /// SHA-256 (`SHA256`).
    Sha256 => Sha256;
}

impl FullObjectAlgorithm for Crc64Nvme {}
impl FullObjectAlgorithm for Crc32 {}
impl FullObjectAlgorithm for Crc32C {}

impl CompositeAlgorithm for Crc32 {}
impl CompositeAlgorithm for Crc32C {}
impl CompositeAlgorithm for Sha1 {}
impl CompositeAlgorithm for Sha256 {}
