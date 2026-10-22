/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic object contents for the integration tests.
//!
//! Byte `i` of an object is `(i + seed) % 251`. The period is prime, so data moved by any distance
//! that is not a multiple of 251 bytes differs from the source at every byte. No power-of-two part
//! size is such a multiple, so a part stored at another part's position, or shifted, no longer
//! matches, unless the two positions are a multiple of 251 parts apart.

/// Length of the repeating pattern, in bytes.
const PERIOD: usize = 251;

/// Returns `size` bytes of the pattern with seed 0: byte `i` is `i % 251`.
pub(crate) fn deterministic_data(size: usize) -> Vec<u8> {
    deterministic_data_seeded(size, 0)
}

/// Returns `size` bytes of the pattern with seed `seed`: byte `i` is `(i + seed) % 251`.
///
/// Seeds that differ modulo 251 give different bytes at every offset, so one object's data, or one
/// of its parts, stored in place of another's no longer matches.
pub(crate) fn deterministic_data_seeded(size: usize, seed: usize) -> Vec<u8> {
    (0..size).map(|i| ((i + seed) % PERIOD) as u8).collect()
}
