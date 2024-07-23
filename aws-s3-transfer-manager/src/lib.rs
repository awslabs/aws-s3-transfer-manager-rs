/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/* Automatically managed default lints */
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
/* End of automatically managed default lints */

//! AWS S3 Transfer Manager
//!
//! # Crate Features
//!
//! - `test-util`: Enables utilities for unit tests. DO NOT ENABLE IN PRODUCTION.

#![warn(
    missing_debug_implementations,
    missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

pub(crate) const MEBIBYTE: u64 = 1024 * 1024;

pub(crate) const DEFAULT_CONCURRENCY: usize = 8;

/// Error types emitted by `aws-s3-transfer-manager`
pub mod error;

/// Common types used by `aws-s3-transfer-manager`
pub mod types;

/// Types and helpers for I/O
pub mod io;

/// Transfer manager client
pub mod client;

/// Transfer manager operations
pub mod operation;
