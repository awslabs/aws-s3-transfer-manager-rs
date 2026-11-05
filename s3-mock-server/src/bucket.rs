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

/// The storage class of every object in a directory bucket.
const EXPRESS_ONEZONE: &str = "EXPRESS_ONEZONE";

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

    /// The storage class an object stored without one has: STANDARD or EXPRESS_ONEZONE.
    pub(crate) fn default_storage_class(self) -> &'static str {
        match self {
            Self::GeneralPurpose => s3s::dto::StorageClass::STANDARD,
            Self::Directory => EXPRESS_ONEZONE,
        }
    }

    /// Fail with 400 InvalidStorageClass when a request sets a storage class that buckets of
    /// this type do not have: EXPRESS_ONEZONE in general purpose buckets, any other class in
    /// directory buckets.
    pub(crate) fn check_storage_class(self, storage_class: Option<&str>) -> crate::Result<()> {
        match (self, storage_class) {
            (Self::GeneralPurpose, Some(EXPRESS_ONEZONE)) => Err(Error::InvalidStorageClass),
            (Self::Directory, Some(class)) if class != EXPRESS_ONEZONE => {
                Err(Error::InvalidStorageClass)
            }
            _ => Ok(()),
        }
    }

    /// Fail with 501 NotImplemented when a request sets `Content-MD5` in a directory bucket,
    /// which does not support it.
    pub(crate) fn check_content_md5(self, content_md5: Option<&str>) -> crate::Result<()> {
        if self == Self::Directory && content_md5.is_some() {
            return Err(Error::ContentMd5NotImplemented);
        }
        Ok(())
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

    /// The `x-amz-storage-class` header of a GetObject or HeadObject response for an object
    /// stored with `storage_class`. General purpose buckets omit it for STANDARD objects, also
    /// when STANDARD was requested explicitly; directory buckets always report it.
    pub(crate) fn storage_class_header(
        self,
        storage_class: Option<String>,
    ) -> Option<s3s::dto::StorageClass> {
        match self {
            Self::GeneralPurpose => storage_class
                .filter(|class| class != s3s::dto::StorageClass::STANDARD)
                .map(s3s::dto::StorageClass::from),
            Self::Directory => Some(self.listed_storage_class(&storage_class).into()),
        }
    }

    /// The storage class of an object in ListObjectsV2 entries, which S3 always reports.
    pub(crate) fn listed_storage_class(self, storage_class: &Option<String>) -> String {
        storage_class
            .clone()
            .unwrap_or_else(|| self.default_storage_class().to_owned())
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

    #[test]
    fn storage_classes_per_type() {
        let gp = BucketType::GeneralPurpose;
        let dir = BucketType::Directory;
        assert!(gp.check_storage_class(Some("GLACIER")).is_ok());
        assert!(gp.check_storage_class(Some(EXPRESS_ONEZONE)).is_err());
        assert!(dir.check_storage_class(Some(EXPRESS_ONEZONE)).is_ok());
        assert!(dir.check_storage_class(Some("STANDARD")).is_err());
        assert!(dir.check_storage_class(None).is_ok());
        assert_eq!(gp.storage_class_header(Some("STANDARD".into())), None);
        assert_eq!(
            dir.storage_class_header(None),
            Some(s3s::dto::StorageClass::from(EXPRESS_ONEZONE.to_owned()))
        );
        assert_eq!(dir.listed_storage_class(&None), EXPRESS_ONEZONE);
        assert_eq!(gp.listed_storage_class(&None), "STANDARD");
    }
}
