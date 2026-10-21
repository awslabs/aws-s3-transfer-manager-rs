/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![warn(
    missing_debug_implementations,
    missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]
#![doc = include_str!("../README.md")]

mod error;
mod http;

pub use error::HttpClientError;

use aws_smithy_execution::{ManagedRuntime, ManagedRuntimeHandle};
use aws_smithy_managed_http::client::ManagedHttpPool;
use aws_smithy_runtime_api::client::http::SharedHttpClient;

/// Transfers runtime ownership into opaque transfer-manager configuration.
///
/// Configure topology, affinity, thread names, and instrumentation before
/// construction. The transfer manager owns shutdown after this conversion;
/// retained execution handles and HTTP clients do not own that lifetime.
/// Dropping an unused managed value or configuration requests shutdown.
///
/// Configure Dial9 on the execution builder. A separate transfer-manager
/// `dial9_handle` setting for an adopted runtime fails client construction.
pub fn managed(runtime: ManagedRuntime) -> aws_sdk_s3_transfer_manager::Managed {
    aws_sdk_s3_transfer_manager::unstable::managed(runtime)
}

/// Creates a Smithy HTTP view over this runtime's existing partition clients.
///
/// Requests made on a managed partition use its client. Requests outside that
/// runtime, including another runtime's partitions, select a random client.
/// Native connection reuse policy still determines which connection serves
/// the request; selecting a client does not guarantee body-polling locality.
///
/// The returned client retains native pool clients and an execution handle.
/// It can outlive the pool wrapper, but the runtime owner must remain alive.
/// It does not borrow the pool or retain runtime shutdown ownership.
///
/// Install it through SDK/S3 HTTP configuration. The transfer manager preserves
/// that transport and offloads file-backed single-PUT body reads. This helper
/// checks the supplied handle against the pool, not against the runtime behind
/// an independently constructed transfer-manager configuration.
///
/// # Errors
///
/// Returns [`HttpClientError`] if the execution handle belongs to another
/// runtime or a native client cannot be resolved for an execution partition.
pub fn http_client(
    execution: ManagedRuntimeHandle,
    pool: &ManagedHttpPool,
) -> Result<SharedHttpClient, HttpClientError> {
    http::client(execution, pool)
}
