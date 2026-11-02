/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bucket types.
//!
//! S3 has general purpose buckets and directory buckets (S3 Express One Zone), which serve
//! different operations and answer some requests differently. As in S3 and in its clients, a
//! bucket's type follows from its name: directory bucket names end in `--x-s3`.

use crate::error::Error;

/// The suffix of directory bucket names.
const DIRECTORY_BUCKET_SUFFIX: &str = "--x-s3";

/// The type of a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketType {
    /// A general purpose bucket.
    GeneralPurpose,
    /// A directory bucket (S3 Express One Zone).
    Directory,
}

impl BucketType {
    /// The type of the bucket named `bucket`.
    pub(crate) fn of(bucket: &str) -> Self {
        if bucket.ends_with(DIRECTORY_BUCKET_SUFFIX) {
            Self::Directory
        } else {
            Self::GeneralPurpose
        }
    }

    /// Fail a ListObjectsV2 request that buckets of this type do not serve: in directory
    /// buckets, a delimiter other than `/` or a prefix that does not end in `/` with 400
    /// InvalidRequest, and `start-after` with 501 NotSupported.
    pub(crate) fn check_listing(
        self,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: bool,
    ) -> crate::Result<()> {
        if self == Self::GeneralPurpose {
            return Ok(());
        }
        if delimiter.is_some_and(|delimiter| !delimiter.is_empty() && delimiter != "/") {
            return Err(Error::InvalidRequest(
                "This bucket only supports '/' as a delimiter",
            ));
        }
        if !prefix.is_empty() && !prefix.ends_with('/') {
            return Err(Error::InvalidRequest(
                "This bucket does not support a prefix that does not end in a delimiter. Specify a \
                 prefix path ending with a delimiter and try again.",
            ));
        }
        if start_after {
            return Err(Error::NotSupported(
                "This bucket does not support start-after query parameter for ListObjectsV2 API.",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_follows_from_the_name() {
        // Directory bucket names end with the `--x-s3` suffix (base name + zone-id).
        for bucket in ["my-bucket--usw2-az1--x-s3", "x--x-s3"] {
            assert_eq!(BucketType::of(bucket), BucketType::Directory, "{bucket}");
        }
        // General purpose bucket names do not end with it.
        for bucket in ["my-bucket", "my-bucket--usw2-az1", "x-s3", ""] {
            assert_eq!(
                BucketType::of(bucket),
                BucketType::GeneralPurpose,
                "{bucket}"
            );
        }
    }
}
