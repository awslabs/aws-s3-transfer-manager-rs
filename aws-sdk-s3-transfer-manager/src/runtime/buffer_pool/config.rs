/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Checked capacity policy and storage defaults for one buffer pool.

use std::fmt;

use super::admission::MAX_PACKED_CARRIERS;
use super::geometry::PoolGeometry;
use super::virtual_memory::page_size;
use super::CarrierCount;
use crate::types::MemoryBudgetConfig;

/// Default carrier size before runtime page-size alignment.
const DEFAULT_CARRIER_BYTES: usize = 16 * 1024;

/// Target bytes prepared and reclaimed as one block.
///
/// Pools configured below this value use their complete carrier-rounded
/// capacity as one block.
const DEFAULT_BLOCK_BYTES: usize = 128 * 1024 * 1024;

/// Target byte range represented by one optimistic bitmap scan.
///
/// The actual reach rounds up to a complete bitmap word.
const DEFAULT_OPTIMISTIC_SCAN_BYTES: usize = 32 * 1024 * 1024;

/// Fraction of detected memory selected by automatic capacity.
const AUTO_CAPACITY_DIVISOR: usize = 4;

/// Maximum automatic capacity in bytes.
const AUTO_CAPACITY_MAX_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// Automatic capacity when effective memory cannot be detected.
const AUTO_CAPACITY_FALLBACK_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Explicit fraction bits stored by an IEEE 754 binary64 value.
const F64_FRACTION_BITS: u32 = 52;

/// Exponent bias used by an IEEE 754 binary64 value.
const F64_EXPONENT_BIAS: u32 = 1023;

/// Failure to resolve capacity and geometry before pool publication.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BufferPoolBuildError {
    /// Capacity is invalid or resolves below one carrier.
    InvalidCapacity,
    /// The selected capacity policy requires unavailable memory detection.
    MemoryDetectionUnavailable,
    /// Capacity or geometry exceeds its internal representation.
    CapacityOverflow,
    /// Runtime page geometry cannot produce a supported pool layout.
    UnsupportedPageGeometry,
}

impl fmt::Display for BufferPoolBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => f.write_str("buffer-pool capacity is invalid"),
            Self::MemoryDetectionUnavailable => {
                f.write_str("effective memory could not be detected")
            }
            Self::CapacityOverflow => {
                f.write_str("buffer-pool capacity exceeds its representation")
            }
            Self::UnsupportedPageGeometry => {
                f.write_str("runtime page geometry cannot support the buffer pool")
            }
        }
    }
}

impl std::error::Error for BufferPoolBuildError {}

/// Checked geometry and accounting inputs for constructing an empty pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PoolConfig {
    /// Page, carrier, block, and bitmap dimensions.
    pub(super) geometry: PoolGeometry,
    /// Normal admission ceiling in complete carriers.
    pub(super) configured_capacity: CarrierCount,
    /// Optimistic bitmap words inspected before serialized fallback.
    pub(super) optimistic_scan_words: usize,
}

impl PoolConfig {
    /// Resolves capacity against detected memory and runtime page geometry.
    pub(super) fn resolve(
        capacity: MemoryBudgetConfig,
        detected_memory: Option<usize>,
    ) -> Result<Self, BufferPoolBuildError> {
        let page_size = page_size().map_err(|_| BufferPoolBuildError::UnsupportedPageGeometry)?;
        Self::resolve_for_page(capacity, detected_memory, page_size.get())
    }

    /// Resolves capacity against explicit page geometry.
    pub(super) fn resolve_for_page(
        capacity: MemoryBudgetConfig,
        detected_memory: Option<usize>,
        page_size: usize,
    ) -> Result<Self, BufferPoolBuildError> {
        let carrier_size = DEFAULT_CARRIER_BYTES
            .div_ceil(page_size)
            .checked_mul(page_size)
            .ok_or(BufferPoolBuildError::CapacityOverflow)?;

        let bytes = resolve_capacity_bytes(capacity, detected_memory)?;
        let carriers = bytes / carrier_size;
        if carriers == 0 {
            return Err(BufferPoolBuildError::InvalidCapacity);
        }
        let configured_capacity = CarrierCount::new(carriers);
        if configured_capacity > MAX_PACKED_CARRIERS {
            return Err(BufferPoolBuildError::CapacityOverflow);
        }
        let target_block_carriers = (DEFAULT_BLOCK_BYTES / carrier_size).max(1);
        let block_carriers = target_block_carriers.min(configured_capacity.get());
        let block_size = carrier_size
            .checked_mul(block_carriers)
            .ok_or(BufferPoolBuildError::CapacityOverflow)?;
        let geometry = PoolGeometry::new(page_size, block_size, carrier_size)
            .map_err(|_| BufferPoolBuildError::UnsupportedPageGeometry)?;
        let optimistic_scan_words = DEFAULT_OPTIMISTIC_SCAN_BYTES
            .div_ceil(carrier_size)
            .div_ceil(u64::BITS as usize);

        Ok(Self {
            geometry,
            configured_capacity,
            optimistic_scan_words,
        })
    }
}

/// Resolves one capacity policy to bytes before carrier rounding.
fn resolve_capacity_bytes(
    capacity: MemoryBudgetConfig,
    detected_memory: Option<usize>,
) -> Result<usize, BufferPoolBuildError> {
    match capacity {
        MemoryBudgetConfig::Auto => match detected_memory {
            Some(bytes) => {
                let maximum = usize::try_from(AUTO_CAPACITY_MAX_BYTES).unwrap_or(usize::MAX);
                Ok((bytes / AUTO_CAPACITY_DIVISOR).min(maximum))
            }
            None => usize::try_from(AUTO_CAPACITY_FALLBACK_BYTES)
                .map_err(|_| BufferPoolBuildError::CapacityOverflow),
        },
        MemoryBudgetConfig::Fraction(fraction) => {
            if !(0.0 < fraction && fraction <= 1.0) {
                return Err(BufferPoolBuildError::InvalidCapacity);
            }
            let detected =
                detected_memory.ok_or(BufferPoolBuildError::MemoryDetectionUnavailable)?;
            Ok(floor_fraction_of(detected, fraction))
        }
        MemoryBudgetConfig::Limit(bytes) => Ok(bytes),
    }
}

/// Multiplies an integer by a checked fraction without rounding above it.
///
/// A normal binary64 value is `significand * 2^(exponent - 1023 - 52)`.
/// Multiplying the integer significand in `u128` and shifting right therefore
/// computes the exact floor instead of rounding the intermediate `f64`.
fn floor_fraction_of(total: usize, fraction: f64) -> usize {
    debug_assert!(0.0 < fraction && fraction <= 1.0);

    let bits = fraction.to_bits();
    let exponent = ((bits >> F64_FRACTION_BITS) & 0x7ff) as u32;
    let stored_fraction = bits & ((1_u64 << F64_FRACTION_BITS) - 1);
    let (significand, shift) = if exponent == 0 {
        (
            u128::from(stored_fraction),
            F64_EXPONENT_BIAS + F64_FRACTION_BITS - 1,
        )
    } else {
        (
            u128::from(stored_fraction | (1_u64 << F64_FRACTION_BITS)),
            F64_EXPONENT_BIAS + F64_FRACTION_BITS - exponent,
        )
    };
    let product = (total as u128) * significand;
    if shift >= u128::BITS {
        return 0;
    }
    (product >> shift) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::buffer_pool::BufferPool;

    const GIB: usize = 1024 * 1024 * 1024;

    #[test]
    fn test_auto_uses_quarter_of_detected_memory() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Auto, Some(2 * GIB), 4096).unwrap();
        assert_eq!(
            resolved.configured_capacity.get() * DEFAULT_CARRIER_BYTES,
            GIB / 2
        );
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_auto_caps_large_machines() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Auto, Some(768 * GIB), 4096).unwrap();
        assert_eq!(
            resolved.configured_capacity.get() * DEFAULT_CARRIER_BYTES,
            32 * GIB
        );
    }

    #[test]
    fn test_auto_uses_fallback_without_detection() {
        let resolved = PoolConfig::resolve_for_page(MemoryBudgetConfig::Auto, None, 4096).unwrap();
        assert_eq!(
            resolved.configured_capacity.get() * DEFAULT_CARRIER_BYTES,
            2 * GIB
        );
    }

    #[test]
    fn test_fraction_requires_detection() {
        assert_eq!(
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Fraction(0.5), None, 4096),
            Err(BufferPoolBuildError::MemoryDetectionUnavailable)
        );
    }

    #[test]
    fn test_fraction_rejects_invalid_values() {
        for fraction in [f64::NAN, f64::INFINITY, -0.5, 0.0, 1.5] {
            assert_eq!(
                PoolConfig::resolve_for_page(
                    MemoryBudgetConfig::Fraction(fraction),
                    Some(GIB),
                    4096,
                ),
                Err(BufferPoolBuildError::InvalidCapacity)
            );
        }
    }

    #[test]
    fn test_fraction_rejects_zero_before_capacity_resolution() {
        assert_eq!(
            resolve_capacity_bytes(MemoryBudgetConfig::Fraction(0.0), Some(GIB)),
            Err(BufferPoolBuildError::InvalidCapacity)
        );
    }

    #[test]
    fn test_fraction_accepts_the_inclusive_upper_bound() {
        assert_eq!(
            resolve_capacity_bytes(MemoryBudgetConfig::Fraction(1.0), Some(GIB)),
            Ok(GIB)
        );
    }

    #[test]
    fn test_fraction_rounds_down_to_complete_carriers() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Fraction(0.5), Some(GIB + 1), 4096)
                .unwrap();
        assert_eq!(
            resolved.configured_capacity.get() * DEFAULT_CARRIER_BYTES,
            GIB / 2
        );
    }

    #[test]
    fn test_fraction_does_not_round_above_the_exact_binary_fraction() {
        let resolved = PoolConfig::resolve_for_page(
            MemoryBudgetConfig::Fraction(0.3333333333333333),
            Some(1_074_266_112),
            4096,
        )
        .unwrap();
        assert_eq!(resolved.configured_capacity, CarrierCount::new(21855));
    }

    #[test]
    fn test_tiny_fraction_resolves_below_one_carrier_without_panicking() {
        let capacity = MemoryBudgetConfig::Fraction(1e-30);
        assert_eq!(resolve_capacity_bytes(capacity.clone(), Some(GIB)), Ok(0));
        assert_eq!(
            PoolConfig::resolve_for_page(capacity, Some(GIB), 4096),
            Err(BufferPoolBuildError::InvalidCapacity)
        );
    }

    #[test]
    fn test_limit_rounds_down_to_complete_carriers() {
        let resolved = PoolConfig::resolve_for_page(
            MemoryBudgetConfig::Limit(DEFAULT_CARRIER_BYTES + 123),
            None,
            4096,
        )
        .unwrap();
        assert_eq!(resolved.configured_capacity, CarrierCount::new(1));
    }

    #[test]
    fn test_capacity_below_one_carrier_is_rejected() {
        assert_eq!(
            PoolConfig::resolve_for_page(
                MemoryBudgetConfig::Limit(DEFAULT_CARRIER_BYTES - 1),
                None,
                4096,
            ),
            Err(BufferPoolBuildError::InvalidCapacity)
        );
    }

    #[test]
    fn test_carrier_rounds_up_to_large_runtime_page() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(256 * 1024), None, 128 * 1024)
                .unwrap();
        assert_eq!(resolved.geometry.carrier_size(), 128 * 1024);
        assert_eq!(resolved.geometry.block_size(), 256 * 1024);
        assert_eq!(resolved.configured_capacity, CarrierCount::new(2));
    }

    #[test]
    fn test_block_size_is_independent_of_carrier_bitmap_width() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(GIB), None, 4096).unwrap();

        assert_eq!(resolved.geometry.carrier_size(), 16 * 1024);
        assert_eq!(resolved.geometry.block_size(), 128 * 1024 * 1024);
        assert_eq!(
            resolved.geometry.carriers_per_block(),
            CarrierCount::new(8192)
        );
        assert_eq!(resolved.geometry.bitmap_words(), 128);
    }

    #[test]
    fn test_optimistic_scan_preserves_its_byte_reach_across_page_geometry() {
        for page_size in [4096, 128 * 1024] {
            let resolved =
                PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(GIB), None, page_size)
                    .unwrap();
            let carrier_size = resolved.geometry.carrier_size();
            let scan_bytes = resolved.optimistic_scan_words * u64::BITS as usize * carrier_size;

            assert!(scan_bytes >= DEFAULT_OPTIMISTIC_SCAN_BYTES);
            assert!(scan_bytes - DEFAULT_OPTIMISTIC_SCAN_BYTES < u64::BITS as usize * carrier_size);
        }
    }

    #[test]
    fn test_eight_mibibyte_claim_uses_optimistic_path_after_preparation() {
        const PART_BYTES: usize = 8 * 1024 * 1024;

        let pool = BufferPool::from_capacity(MemoryBudgetConfig::Limit(GIB), None).unwrap();
        let prepared = pool
            .acquire_unreserved(pool.inner.geometry.carrier_size())
            .unwrap();
        drop(prepared);
        let fallbacks_before = pool.diagnostics().serialized_fallbacks;

        let part = pool.acquire_unreserved(PART_BYTES).unwrap();

        assert_eq!(pool.diagnostics().serialized_fallbacks, fallbacks_before);
        drop(part);
    }

    #[test]
    fn test_block_size_is_capped_by_small_configured_capacity() {
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(8 * 1024 * 1024), None, 4096)
                .unwrap();

        assert_eq!(resolved.geometry.block_size(), 8 * 1024 * 1024);
        assert_eq!(
            resolved.geometry.carriers_per_block(),
            resolved.configured_capacity
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_capacity_accepts_the_exact_packed_carrier_limit() {
        let bytes = u32::MAX as usize * DEFAULT_CARRIER_BYTES;
        let resolved =
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(bytes), None, 4096).unwrap();
        assert_eq!(resolved.configured_capacity, MAX_PACKED_CARRIERS);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_capacity_rejects_packed_carrier_overflow() {
        let bytes = (u32::MAX as usize + 1) * DEFAULT_CARRIER_BYTES;
        assert_eq!(
            PoolConfig::resolve_for_page(MemoryBudgetConfig::Limit(bytes), None, 4096),
            Err(BufferPoolBuildError::CapacityOverflow)
        );
    }

    #[test]
    fn test_pool_construction_prepares_no_capacity() {
        let pool =
            BufferPool::from_capacity(MemoryBudgetConfig::Limit(GIB), Some(8 * GIB)).unwrap();
        assert_eq!(pool.inner.geometry.carrier_size(), DEFAULT_CARRIER_BYTES);
        assert_eq!(pool.inner.geometry.block_size(), DEFAULT_BLOCK_BYTES);
        assert_eq!(pool.metrics().configured_capacity_bytes(), GIB as u64);
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);
    }
}
