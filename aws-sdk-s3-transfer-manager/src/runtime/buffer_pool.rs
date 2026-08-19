/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Fixed-size carrier geometry, virtual memory, and physical ownership.

mod block;
mod geometry;
mod virtual_memory;

/// A count of fixed-size carriers.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CarrierCount(usize);

impl CarrierCount {
    /// No carriers.
    const ZERO: Self = Self(0);

    /// Wraps a carrier count.
    fn new(value: usize) -> Self {
        Self(value)
    }

    /// Returns the underlying count.
    fn get(self) -> usize {
        self.0
    }

    /// Adds two counts, returning `None` on overflow.
    fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    /// Subtracts two counts, returning `None` on underflow.
    fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }
}
