/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Executable model for deriving planned per-work memory demand.
//!
//! This is intentionally test-only. Admission limits scheduled demand; it does
//! not escrow physical carriers. Reserved and shared Hyper acquisitions both
//! use the pool's elastic acquisition path.

use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CarrierGeometry {
    bytes: NonZeroU64,
}

impl CarrierGeometry {
    fn new(bytes: u64) -> Self {
        Self {
            bytes: NonZeroU64::new(bytes).expect("carrier size must be non-zero"),
        }
    }

    fn carriers_for(self, bytes: u64) -> Result<usize, ModelError> {
        let carriers = bytes.div_ceil(self.bytes.get());
        usize::try_from(carriers).map_err(|_| ModelError::CarrierCountOverflow)
    }
}

/// Demand charged against the scheduler's admission limit while work, or
/// output produced by that work, can retain the corresponding storage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WorkMemoryRequirement {
    /// Expected maximum simultaneously live pool-sized carriers.
    reservation_size: usize,
    /// Subset acquired directly by TM through the reservation.
    ///
    /// Shared Hyper acquisitions are not attributed to this field.
    direct_acquire_limit: usize,
}

impl WorkMemoryRequirement {
    fn planned_unreserved(reservation_size: usize) -> Self {
        Self {
            reservation_size,
            direct_acquire_limit: 0,
        }
    }

    fn direct(reservation_size: usize) -> Self {
        Self {
            reservation_size,
            direct_acquire_limit: reservation_size,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestStorage {
    /// Storage supplied and retained by the caller before TM admission.
    CallerOwned,
    /// A complete retryable body retained in pool carriers.
    PooledRetained { max_bytes: u64 },
    /// A replayable source streamed through a bounded live-carrier window.
    PooledStreaming { max_live_carriers: NonZeroUsize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PartPlan {
    number: u32,
    offset: u64,
    max_length: u64,
}

impl PartPlan {
    fn indexed(
        number: u32,
        object_length: u64,
        part_size: NonZeroU64,
    ) -> Result<Option<Self>, ModelError> {
        if number == 0 {
            return Err(ModelError::InvalidPartNumber);
        }

        let offset = u64::from(number - 1)
            .checked_mul(part_size.get())
            .ok_or(ModelError::ByteCountOverflow)?;
        if offset >= object_length {
            return Ok(None);
        }

        Ok(Some(Self {
            number,
            offset,
            max_length: (object_length - offset).min(part_size.get()),
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelError {
    InvalidPartNumber,
    ByteCountOverflow,
    CarrierCountOverflow,
}

fn request_body_requirement(
    geometry: CarrierGeometry,
    storage: RequestStorage,
) -> Result<WorkMemoryRequirement, ModelError> {
    let carriers = match storage {
        RequestStorage::CallerOwned => 0,
        RequestStorage::PooledRetained { max_bytes } => geometry.carriers_for(max_bytes)?,
        RequestStorage::PooledStreaming { max_live_carriers } => max_live_carriers.get(),
    };
    Ok(WorkMemoryRequirement::direct(carriers))
}

/// The exact successful range is planned scheduler demand. Hyper acquires its
/// physical carriers through the shared, unreserved path.
fn ranged_get(
    geometry: CarrierGeometry,
    range_length: u64,
) -> Result<WorkMemoryRequirement, ModelError> {
    Ok(WorkMemoryRequirement::planned_unreserved(
        geometry.carriers_for(range_length)?,
    ))
}

/// A metadata probe has no planned retained payload. Any prefetched success
/// bytes or SDK-consumed error body use elastic, unreserved acquisition.
fn metadata_only_discovery() -> WorkMemoryRequirement {
    WorkMemoryRequirement::default()
}

/// Upload admission covers only TM-owned request storage. Response and error
/// bodies are incidental Hyper acquisitions and do not add a physical grant.
fn upload(
    geometry: CarrierGeometry,
    request: RequestStorage,
) -> Result<WorkMemoryRequirement, ModelError> {
    request_body_requirement(geometry, request)
}

/// Control operations rely on scheduler concurrency and elastic transport
/// allocation; they do not reserve a guessed response-body allowance.
fn control() -> WorkMemoryRequirement {
    WorkMemoryRequirement::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn geometry() -> CarrierGeometry {
        CarrierGeometry::new(MIB)
    }

    #[test]
    fn byte_rounding_is_centralized_and_zero_costs_no_carrier() {
        let geometry = geometry();
        assert_eq!(geometry.carriers_for(0), Ok(0));
        assert_eq!(geometry.carriers_for(1), Ok(1));
        assert_eq!(geometry.carriers_for(MIB), Ok(1));
        assert_eq!(geometry.carriers_for(MIB + 1), Ok(2));
    }

    #[test]
    fn ranged_get_charges_only_the_planned_success_payload() {
        assert_eq!(
            ranged_get(geometry(), 8 * MIB).unwrap(),
            WorkMemoryRequirement {
                reservation_size: 8,
                direct_acquire_limit: 0,
            }
        );
    }

    #[test]
    fn metadata_probe_does_not_guess_a_transport_allowance() {
        assert_eq!(metadata_only_discovery(), WorkMemoryRequirement::default());
    }

    #[test]
    fn pooled_upload_body_reserves_its_direct_acquire_limit() {
        assert_eq!(
            upload(
                geometry(),
                RequestStorage::PooledRetained { max_bytes: 8 * MIB },
            )
            .unwrap(),
            WorkMemoryRequirement {
                reservation_size: 8,
                direct_acquire_limit: 8,
            }
        );
    }

    #[test]
    fn caller_owned_upload_memory_is_not_reclassified_as_pool_demand() {
        assert_eq!(
            upload(geometry(), RequestStorage::CallerOwned).unwrap(),
            WorkMemoryRequirement::default()
        );
    }

    #[test]
    fn streaming_put_uses_its_live_window_not_object_length() {
        assert_eq!(
            upload(
                geometry(),
                RequestStorage::PooledStreaming {
                    max_live_carriers: NonZeroUsize::new(4).unwrap(),
                },
            )
            .unwrap(),
            WorkMemoryRequirement {
                reservation_size: 4,
                direct_acquire_limit: 4,
            }
        );
    }

    #[test]
    fn control_operations_do_not_reserve_error_body_capacity() {
        assert_eq!(control(), WorkMemoryRequirement::default());
    }

    #[test]
    fn indexed_part_plan_bounds_the_final_part_before_materialization() {
        let part_size = NonZeroU64::new(8 * MIB).unwrap();
        assert_eq!(
            PartPlan::indexed(1, 18 * MIB, part_size).unwrap(),
            Some(PartPlan {
                number: 1,
                offset: 0,
                max_length: 8 * MIB,
            })
        );
        assert_eq!(
            PartPlan::indexed(3, 18 * MIB, part_size).unwrap(),
            Some(PartPlan {
                number: 3,
                offset: 16 * MIB,
                max_length: 2 * MIB,
            })
        );
        assert_eq!(PartPlan::indexed(4, 18 * MIB, part_size).unwrap(), None);
    }

    #[test]
    fn part_numbers_are_one_based() {
        assert_eq!(
            PartPlan::indexed(0, MIB, NonZeroU64::new(MIB).unwrap()),
            Err(ModelError::InvalidPartNumber)
        );
    }
}
