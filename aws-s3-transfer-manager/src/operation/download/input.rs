/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::builders::{GetObjectFluentBuilder, GetObjectInputBuilder};

// FIXME - replace with our own types like upload

/// Request type for downloading a single object
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadInput {
    pub(crate) input: GetObjectInputBuilder,
}

// FIXME - should probably be TryFrom since checksums may conflict?
impl From<GetObjectFluentBuilder> for DownloadInput {
    fn from(value: GetObjectFluentBuilder) -> Self {
        Self {
            input: value.as_input().clone(),
        }
    }
}

impl From<GetObjectInputBuilder> for DownloadInput {
    fn from(value: GetObjectInputBuilder) -> Self {
        Self { input: value }
    }
}
