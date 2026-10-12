/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic pool construction for tests and fuzzing.

use crate::config::MemoryDiagnosticsConfig;

use super::super::arena::ArenaOptions;
use super::super::geometry::PoolGeometry;
use super::super::virtual_memory::page_size;
use super::super::{BufferPool, CarrierCount};

/// Constructs a pool with a one-word optimistic scan budget.
pub(crate) fn test_pool(block_carriers: usize, configured: usize) -> (BufferPool, usize) {
    test_pool_with_scan(block_carriers, configured, 1)
}

/// Constructs a pool with an explicit carrier size.
#[cfg(all(test, not(s3_tm_loom)))]
pub(in crate::runtime::buffer_pool) fn test_pool_with_carrier_size(
    block_carriers: usize,
    configured: usize,
    carrier_size: usize,
) -> (BufferPool, usize) {
    let runtime_page_size = page_size().expect("runtime page size").get();
    let block_size = carrier_size
        .checked_mul(block_carriers)
        .expect("test block size overflowed");
    let geometry = PoolGeometry::new(runtime_page_size, block_size, carrier_size)
        .expect("valid test carrier geometry");
    let diagnostics = MemoryDiagnosticsConfig::for_test(None, 1);
    let options = ArenaOptions::new(1, diagnostics.enable_detailed_counters());
    let pool = BufferPool::from_parts_with_arena_options(
        geometry,
        CarrierCount::new(configured),
        diagnostics,
        options,
    )
    .expect("valid test pool");
    (pool, carrier_size)
}

/// Constructs a pool whose complete block ranges have native guard pages.
#[cfg(all(test, not(any(miri, s3_tm_loom))))]
pub(in crate::runtime::buffer_pool) fn test_guarded_pool(
    block_carriers: usize,
    configured: usize,
) -> (BufferPool, usize) {
    let page_size = page_size().expect("runtime page size").get();
    let block_size = page_size
        .checked_mul(block_carriers)
        .expect("test block size overflowed");
    let geometry =
        PoolGeometry::new(page_size, block_size, page_size).expect("valid guarded geometry");
    let options = ArenaOptions::new(1, true).enable_guard_pages();
    let pool = BufferPool::from_parts_with_arena_options(
        geometry,
        CarrierCount::new(configured),
        MemoryDiagnosticsConfig::for_test(None, 1),
        options,
    )
    .expect("valid guarded pool");
    (pool, page_size)
}

/// Constructs a pool with explicit block and optimistic-scan geometry.
pub(in crate::runtime::buffer_pool) fn test_pool_with_scan(
    block_carriers: usize,
    configured: usize,
    optimistic_scan_words: usize,
) -> (BufferPool, usize) {
    test_pool_with_scan_and_diagnostics(
        block_carriers,
        configured,
        optimistic_scan_words,
        MemoryDiagnosticsConfig::for_test(None, 1),
    )
}

/// Constructs a pool with explicit scan geometry and diagnostic policy.
pub(in crate::runtime::buffer_pool) fn test_pool_with_scan_and_diagnostics(
    block_carriers: usize,
    configured: usize,
    optimistic_scan_words: usize,
    diagnostics: MemoryDiagnosticsConfig,
) -> (BufferPool, usize) {
    let page_size = page_size().expect("runtime page size").get();
    let block_size = page_size
        .checked_mul(block_carriers)
        .expect("test block size overflowed");
    let geometry =
        PoolGeometry::new(page_size, block_size, page_size).expect("valid test pool geometry");
    let options = ArenaOptions::new(
        optimistic_scan_words,
        diagnostics.enable_detailed_counters(),
    );
    let pool = BufferPool::from_parts_with_arena_options(
        geometry,
        CarrierCount::new(configured),
        diagnostics,
        options,
    )
    .expect("valid test pool");
    (pool, page_size)
}

/// Constructs a pool whose block and carrier are one runtime page.
#[cfg(all(test, s3_tm_loom))]
pub(in crate::runtime::buffer_pool) fn test_single_carrier_pool(
    configured: usize,
) -> (BufferPool, usize) {
    test_pool(1, configured)
}
