/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Version-coupled integrations with managed execution.
//!
//! Available with the non-default `__unstable_ext` feature. This module's contract
//! is experimental and may change independently of the ordinary TM API. Use
//! an adapter release constrained to the supported TM and execution versions.

/// Transfers an existing managed runtime into transfer-manager configuration.
///
/// The runtime retains its topology, partition identities, CPU bindings,
/// thread names, and instrumentation. Client construction takes this owner
/// into managed execution. Dropping an unused managed value or configuration
/// requests runtime shutdown; execution handles do not retain shutdown ownership.
///
/// Configure any Dial9 recorder on the runtime builder before construction.
/// Supplying a separate transfer-manager `dial9_handle` for an adopted runtime
/// causes client construction to panic.
///
/// The transfer manager continues to preserve HTTP configured through the
/// SDK or S3 configuration. With no configured HTTP it builds its default
/// transport against the adopted runtime's partitions.
pub fn managed(runtime: aws_smithy_execution::ManagedRuntime) -> crate::Managed {
    crate::Managed::from_runtime(runtime)
}
