/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Monotonic timestamps read from the installed [`WasmRuntime`](super::WasmRuntime).

use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

/// A point in time read from the transfer manager's monotonic clock.
///
/// On `wasm32-unknown-unknown` this stores the duration since the installed
/// [`WasmRuntime`](crate::wasm::WasmRuntime)'s monotonic epoch. It provides the
/// [`std::time::Instant`] operations that apply to a monotonic clock. Ordering
/// and differences are meaningful only within one module.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransferInstant(Duration);

impl TransferInstant {
    /// Returns the current time of the installed runtime's clock.
    ///
    /// # Panics
    ///
    /// Panics if no [`WasmRuntime`](crate::wasm::WasmRuntime) is installed.
    #[track_caller]
    pub fn now() -> Self {
        Self(super::runtime().monotonic_now())
    }

    /// Returns the time elapsed from `earlier` to `self`, or zero if `earlier`
    /// is later than `self`.
    pub fn duration_since(&self, earlier: Self) -> Duration {
        self.saturating_duration_since(earlier)
    }

    /// Returns the time elapsed from `earlier` to `self`, or `None` if
    /// `earlier` is later than `self`.
    pub fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }

    /// Returns the time elapsed from `earlier` to `self`, or zero if `earlier`
    /// is later than `self`.
    pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// Returns the time elapsed since `self`.
    ///
    /// # Panics
    ///
    /// Panics if no [`WasmRuntime`](crate::wasm::WasmRuntime) is installed.
    #[track_caller]
    pub fn elapsed(&self) -> Duration {
        Self::now().saturating_duration_since(*self)
    }

    /// Returns `self + duration`, or `None` if the result is not representable.
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    /// Returns `self - duration`, or `None` if the result precedes the
    /// runtime's epoch.
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        self.0.checked_sub(duration).map(Self)
    }
}

impl Add<Duration> for TransferInstant {
    type Output = Self;

    /// # Panics
    ///
    /// Panics if the result is not representable.
    fn add(self, rhs: Duration) -> Self {
        self.checked_add(rhs)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for TransferInstant {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub<Duration> for TransferInstant {
    type Output = Self;

    /// # Panics
    ///
    /// Panics if the result precedes the runtime's epoch.
    fn sub(self, rhs: Duration) -> Self {
        self.checked_sub(rhs)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for TransferInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Sub<TransferInstant> for TransferInstant {
    type Output = Duration;

    /// Returns the time elapsed from `rhs` to `self`, or zero if `rhs` is
    /// later than `self`.
    fn sub(self, rhs: Self) -> Duration {
        self.saturating_duration_since(rhs)
    }
}
