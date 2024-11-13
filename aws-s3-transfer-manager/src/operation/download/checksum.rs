/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::GetObjectOutput;

/// The level of checksum validation performed on a download
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChecksumValidationLevel {
    /// TODO: where should this enum live?
    /// TODO: docs
    #[default]
    NotValidated,
    /// TODO: docs
    /// TODO: sounds too much like "partial"? If we cal it "AllParts" then can't apply it to individual part
    AllParts,
    /// TODO: docs
    FullObject,
}

pub(crate) fn get_checksum_validation_level_of_chunk(
    output: &GetObjectOutput,
) -> ChecksumValidationLevel {
    // TODO: test to enforce this list stays up to date

    let checksum = output
        .checksum_crc32()
        .or(output.checksum_crc32_c())
        .or(output.checksum_sha1())
        .or(output.checksum_sha256());

    match checksum {
        // If no checksum found, the SDK couldn't validate it
        None => ChecksumValidationLevel::NotValidated,

        Some(checksum) => {
            if is_multipart(checksum) {
                // If we got the object's multipart checksum, the SDK didn't validate it.
                // We SHOULD NOT get multipart checksums from a ranged-get or part-get,
                // it should only be possible when doing vanilla get on the whole object.
                // But let's check anyway.
                ChecksumValidationLevel::NotValidated
            } else {
                // Now determine if this is the full-object checksum, or just the checksum of 1 part.
                // We can figure out if the object is multipart by checking its ETag.
                match output.e_tag() {
                    // ETag SHOULD NOT be missing, give up and say not validated
                    None => ChecksumValidationLevel::NotValidated,

                    Some(etag) => {
                        if is_multipart(etag) {
                            ChecksumValidationLevel::AllParts
                        } else {
                            ChecksumValidationLevel::FullObject
                        }
                    }
                }
            }
        }
    }
}

/// Return whether this is a multipart checksum or ETag.
/// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums
fn is_multipart(checksum_or_etag: &str) -> bool {
    return checksum_or_etag.contains('-');
}
