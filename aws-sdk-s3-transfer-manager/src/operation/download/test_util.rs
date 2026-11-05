/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared download test support.
//!
//! [`sink`] scripts faults into a real download's destination writes, and
//! [`fixtures`] builds the objects, S3 clients, handles and transfers those
//! tests run against. [`now`] completes a body-writer operation whose sink
//! completes inline.

pub(crate) mod fixtures;
pub(crate) mod sink;

/// Polls `future` once and returns its output.
///
/// Panics if the future is not ready on its first poll, as every drain,
/// preparation, and finalization through a sink that completes inline is.
pub(crate) fn now<F: std::future::Future>(future: F) -> F::Output {
    futures_util::FutureExt::now_or_never(future)
        .expect("an operation through an inline sink completes on its first poll")
}
