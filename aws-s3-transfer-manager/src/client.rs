/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Abstractions for downloading objects from S3
pub mod downloader;
pub use downloader::Downloader;

/// Abstractions for downloading objects from Amazon S3
pub mod uploader;

pub use uploader::Uploader;
