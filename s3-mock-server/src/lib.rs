/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! S3 Mock Server
//!
//! A mock S3 server for testing and benchmarking the AWS S3 Transfer Manager.
//! This crate provides a local HTTP server that implements the S3 API operations
//! required by the Transfer Manager, allowing for comprehensive testing of
//! concurrent operations, error handling, and performance characteristics.

mod error;
mod s3s;
mod server;
mod storage;

pub use error::Error;
pub use error::Result;
pub use server::{S3MockServer, S3MockServerBuilder, ServerConfig, ServerHandle};
