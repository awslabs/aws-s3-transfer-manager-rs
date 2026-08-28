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

/// Default carrier size before runtime page-size alignment.
const DEFAULT_CARRIER_BYTES: usize = 64 * 1024;

/// Number of carriers prepared and reclaimed as one block.
const DEFAULT_BLOCK_CARRIERS: usize = 64;

/// Maximum bitmap words inspected by one optimistic claim.
const DEFAULT_OPTIMISTIC_SCAN_WORDS: usize = 8;

/// Fraction of detected memory selected by automatic capacity.
const AUTO_CAPACITY_DIVISOR: usize = 4;

/// Maximum automatic capacity in bytes.
const AUTO_CAPACITY_MAX_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// Automatic capacity when effective memory cannot be detected.
const AUTO_CAPACITY_FALLBACK_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Configured memory policy for one buffer pool.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum MemoryCapacity {
    /// Use one quarter of detected effective memory, capped at 32 GiB.
    #[default]
    Auto,
    /// Use a fraction of detected effective memory.
    Fraction(f64),
    /// Use an explicit byte ceiling without memory detection.
    Limit(usize),
}

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

/// Fully checked inputs for constructing an empty pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ResolvedPoolConfig {
    /// Page, carrier, block, and bitmap dimensions.
    pub(super) geometry: PoolGeometry,
    /// Normal admission ceiling in complete carriers.
    pub(super) configured_capacity: CarrierCount,
    /// Optimistic bitmap words inspected before serialized fallback.
    pub(super) optimistic_scan_words: usize,
}

impl ResolvedPoolConfig {
    /// Resolves capacity against detected memory and runtime page geometry.
    pub(super) fn resolve(
        capacity: MemoryCapacity,
        detected_memory: Option<usize>,
    ) -> Result<Self, BufferPoolBuildError> {
        let page_size = page_size().map_err(|_| BufferPoolBuildError::UnsupportedPageGeometry)?;
        Self::resolve_for_page(capacity, detected_memory, page_size.get())
    }

    /// Resolves capacity against explicit page geometry.
    fn resolve_for_page(
        capacity: MemoryCapacity,
        detected_memory: Option<usize>,
        page_size: usize,
    ) -> Result<Self, BufferPoolBuildError> {
        let carrier_size = DEFAULT_CARRIER_BYTES
            .div_ceil(page_size)
            .checked_mul(page_size)
            .ok_or(BufferPoolBuildError::CapacityOverflow)?;
        let block_size = carrier_size
            .checked_mul(DEFAULT_BLOCK_CARRIERS)
            .ok_or(BufferPoolBuildError::CapacityOverflow)?;
        let geometry = PoolGeometry::new(page_size, block_size, carrier_size)
            .map_err(|_| BufferPoolBuildError::UnsupportedPageGeometry)?;

        let bytes = resolve_capacity_bytes(capacity, detected_memory)?;
        let carriers = bytes / carrier_size;
        if carriers == 0 {
            return Err(BufferPoolBuildError::InvalidCapacity);
        }
        let configured_capacity = CarrierCount::new(carriers);
        if configured_capacity > MAX_PACKED_CARRIERS {
            return Err(BufferPoolBuildError::CapacityOverflow);
        }

        Ok(Self {
            geometry,
            configured_capacity,
            optimistic_scan_words: DEFAULT_OPTIMISTIC_SCAN_WORDS,
        })
    }
}

/// Resolves one capacity policy to bytes before carrier rounding.
fn resolve_capacity_bytes(
    capacity: MemoryCapacity,
    detected_memory: Option<usize>,
) -> Result<usize, BufferPoolBuildError> {
    match capacity {
        MemoryCapacity::Auto => match detected_memory {
            Some(bytes) => {
                let maximum = usize::try_from(AUTO_CAPACITY_MAX_BYTES).unwrap_or(usize::MAX);
                Ok((bytes / AUTO_CAPACITY_DIVISOR).min(maximum))
            }
            None => usize::try_from(AUTO_CAPACITY_FALLBACK_BYTES)
                .map_err(|_| BufferPoolBuildError::CapacityOverflow),
        },
        MemoryCapacity::Fraction(fraction) => {
            if !(fraction.is_finite() && 0.0 < fraction && fraction <= 1.0) {
                return Err(BufferPoolBuildError::InvalidCapacity);
            }
            let detected =
                detected_memory.ok_or(BufferPoolBuildError::MemoryDetectionUnavailable)?;
            Ok((detected as f64 * fraction) as usize)
        }
        MemoryCapacity::Limit(bytes) => Ok(bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::buffer_pool::BufferPool;

    const GIB: usize = 1024 * 1024 * 1024;

    #[test]
    fn test_auto_uses_quarter_of_detected_memory() {
        let resolved =
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Auto, Some(2 * GIB), 4096)
                .unwrap();
        assert_eq!(resolved.configured_capacity.get() * 64 * 1024, GIB / 2);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_auto_caps_large_machines() {
        let resolved =
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Auto, Some(768 * GIB), 4096)
                .unwrap();
        assert_eq!(resolved.configured_capacity.get() * 64 * 1024, 32 * GIB);
    }

    #[test]
    fn test_auto_uses_fallback_without_detection() {
        let resolved =
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Auto, None, 4096).unwrap();
        assert_eq!(resolved.configured_capacity.get() * 64 * 1024, 2 * GIB);
    }

    #[test]
    fn test_fraction_requires_detection() {
        assert_eq!(
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Fraction(0.5), None, 4096),
            Err(BufferPoolBuildError::MemoryDetectionUnavailable)
        );
    }

    #[test]
    fn test_fraction_rejects_invalid_values() {
        for fraction in [f64::NAN, f64::INFINITY, -0.5, 0.0, 1.5] {
            assert_eq!(
                ResolvedPoolConfig::resolve_for_page(
                    MemoryCapacity::Fraction(fraction),
                    Some(GIB),
                    4096,
                ),
                Err(BufferPoolBuildError::InvalidCapacity)
            );
        }
    }

    #[test]
    fn test_fraction_rounds_down_to_complete_carriers() {
        let resolved = ResolvedPoolConfig::resolve_for_page(
            MemoryCapacity::Fraction(0.5),
            Some(GIB + 1),
            4096,
        )
        .unwrap();
        assert_eq!(resolved.configured_capacity.get() * 64 * 1024, GIB / 2);
    }

    #[test]
    fn test_limit_rounds_down_to_complete_carriers() {
        let resolved = ResolvedPoolConfig::resolve_for_page(
            MemoryCapacity::Limit(64 * 1024 + 123),
            None,
            4096,
        )
        .unwrap();
        assert_eq!(resolved.configured_capacity, CarrierCount::new(1));
    }

    #[test]
    fn test_capacity_below_one_carrier_is_rejected() {
        assert_eq!(
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Limit(64 * 1024 - 1), None, 4096),
            Err(BufferPoolBuildError::InvalidCapacity)
        );
    }

    #[test]
    fn test_carrier_rounds_up_to_large_runtime_page() {
        let resolved = ResolvedPoolConfig::resolve_for_page(
            MemoryCapacity::Limit(256 * 1024),
            None,
            128 * 1024,
        )
        .unwrap();
        assert_eq!(resolved.geometry.carrier_size(), 128 * 1024);
        assert_eq!(resolved.geometry.block_size(), 8 * 1024 * 1024);
        assert_eq!(resolved.configured_capacity, CarrierCount::new(2));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_capacity_rejects_packed_carrier_overflow() {
        let bytes = (u32::MAX as usize + 1) * 64 * 1024;
        assert_eq!(
            ResolvedPoolConfig::resolve_for_page(MemoryCapacity::Limit(bytes), None, 4096),
            Err(BufferPoolBuildError::CapacityOverflow)
        );
    }

    #[test]
    fn test_pool_construction_prepares_no_capacity() {
        let pool = BufferPool::from_capacity(MemoryCapacity::Limit(GIB), Some(8 * GIB)).unwrap();
        assert_eq!(pool.carrier_size(), 64 * 1024);
        assert_eq!(pool.metrics().configured_capacity_bytes(), GIB as u64);
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);
    }
}
