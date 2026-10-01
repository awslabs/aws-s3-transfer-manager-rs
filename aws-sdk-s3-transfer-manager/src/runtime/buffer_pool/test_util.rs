/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared buffer-pool validation support.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::{Reservation, ReserveError, ReserveFuture};

pub(super) mod audit;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
pub(super) use fixtures::*;
#[cfg(test)]
mod hooks;
#[cfg(test)]
pub(super) use hooks::*;

#[cfg(any(all(test, not(s3_tm_loom)), s3_tm_fuzz))]
pub(super) mod model_harness;

#[cfg(any(all(test, not(s3_tm_loom)), s3_tm_fuzz))]
pub(super) mod operation_sequence;

mod pool;
#[cfg(all(test, not(any(miri, s3_tm_loom))))]
pub(super) use pool::test_guarded_pool;
#[cfg(test)]
pub(crate) use pool::test_pool;
#[cfg(all(test, s3_tm_loom))]
pub(super) use pool::test_single_carrier_pool;
#[cfg(all(test, not(s3_tm_loom)))]
pub(super) use pool::{test_pool_with_scan, test_pool_with_scan_and_diagnostics};

/// Polls one reservation future with an explicit waker.
pub(super) fn poll_reserve(
    future: &mut ReserveFuture,
    waker: &Waker,
) -> Poll<Result<Reservation, ReserveError>> {
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}
