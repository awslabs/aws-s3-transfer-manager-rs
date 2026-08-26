/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Elastic admission and fixed-size pooled storage.

use crate::runtime::sync::sync::{Arc, Mutex};

mod acquisition;
mod admission;
mod arena;
mod block;
mod geometry;
mod metrics;
mod virtual_memory;

#[cfg(test)]
use acquisition::AcquisitionAllocationFailures;
use admission::{AdmissionState, CoverageState, MAX_PACKED_CARRIERS};
pub(crate) use admission::{Reservation, ReserveError, ReserveFuture};
use arena::{Arena, ArenaError};
use geometry::{GeometryError, PoolGeometry};
use metrics::MemoryMetrics;

/// A cloneable handle to one admission, accounting, and storage domain.
#[derive(Clone)]
pub(crate) struct BufferPool {
    inner: Arc<PoolInner>,
}

/// Shared state retained by pool handles, reservations, and carrier owners.
struct PoolInner {
    /// Planned-demand policy and prepared-capacity serialization.
    admission: Mutex<AdmissionState>,
    /// Aggregate acquisition charges changed at carrier frequency.
    coverage: CoverageState,
    /// Stable virtual ranges and physical carrier ownership.
    arena: Arena,
    /// One-shot acquisition metadata failure injection.
    #[cfg(test)]
    acquisition_allocation_failures: AcquisitionAllocationFailures,
}

impl BufferPool {
    /// Constructs a pool from geometry and a validated configured capacity.
    ///
    /// Callers validate capacity policy before this internal boundary.
    /// Construction reserves no blocks and prepares no physical capacity.
    fn from_validated_parts(
        geometry: PoolGeometry,
        configured_capacity: CarrierCount,
        optimistic_scan_words: usize,
    ) -> Result<Self, ArenaError> {
        assert!(
            configured_capacity != CarrierCount::ZERO,
            "configured capacity must be nonzero"
        );
        assert!(
            configured_capacity <= MAX_PACKED_CARRIERS,
            "configured capacity must fit packed accounting"
        );

        Ok(Self {
            inner: Arc::new(PoolInner {
                admission: Mutex::new(AdmissionState::new(configured_capacity)),
                coverage: CoverageState::new(),
                arena: Arena::new(geometry, optimistic_scan_words)?,
                #[cfg(test)]
                acquisition_allocation_failures: AcquisitionAllocationFailures::new(),
            }),
        })
    }

    /// Attempts one immediate reservation grant.
    ///
    /// `Ok(None)` reports an older FIFO request or current admission pressure.
    /// A successful grant has already prepared storage through its complete
    /// admission floor.
    pub(crate) fn try_reserve(&self, bytes: usize) -> Result<Option<Reservation>, ReserveError> {
        let envelope = self.reservation_envelope(bytes)?;
        PoolInner::try_reserve_count(&self.inner, envelope)
    }

    /// Creates a lazy, cancellation-safe reservation request.
    ///
    /// The first poll either returns an immediate result or enters the
    /// pool-wide FIFO. Invalid requests and physical preparation failures
    /// resolve through the future's `ReserveError`.
    pub(crate) fn reserve(&self, bytes: usize) -> ReserveFuture {
        ReserveFuture::new(self.clone(), bytes)
    }

    /// Returns one coherent sample of this pool's memory state.
    pub(crate) fn metrics(&self) -> MemoryMetrics {
        self.inner.memory_metrics()
    }

    /// Converts one public byte request to its checked carrier envelope.
    fn reservation_envelope(&self, bytes: usize) -> Result<CarrierCount, ReserveError> {
        let envelope = self
            .inner
            .arena
            .carriers_for_bytes(bytes)
            .map_err(map_reservation_geometry_error)?;
        if envelope > MAX_PACKED_CARRIERS {
            return Err(ReserveError::CapacityOverflow);
        }
        Ok(envelope)
    }
}

fn map_reservation_geometry_error(error: GeometryError) -> ReserveError {
    match error {
        GeometryError::ZeroByteRequest => ReserveError::InvalidSize,
        error => panic!("validated geometry returned an impossible byte-conversion error: {error}"),
    }
}

/// A count of fixed-size carriers.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CarrierCount(usize);

impl CarrierCount {
    /// No carriers.
    const ZERO: Self = Self(0);

    /// Wraps a carrier count.
    const fn new(value: usize) -> Self {
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

    /// Converts this count to the packed accounting lane width.
    fn try_as_u32(self) -> Option<u32> {
        u32::try_from(self.0).ok()
    }
}
