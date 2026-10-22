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

use aws_smithy_execution::ManagedRuntime;

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
