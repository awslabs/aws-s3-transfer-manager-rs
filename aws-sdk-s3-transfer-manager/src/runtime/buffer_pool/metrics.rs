/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stable memory-pressure metrics derived from one admission sample.

use super::CarrierCount;

/// Point-in-time memory state for one shared pool domain.
///
/// The representation remains private so ledger changes do not become API
/// compatibility constraints. Values are carrier-rounded bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryMetrics {
    configured_capacity_bytes: u64,
    admission_used_bytes: u64,
    active_planned_demand_bytes: u64,
    charged_capacity_bytes: u64,
    admission_overage_bytes: u64,
    prepared_capacity_bytes: u64,
    queued_reservations: usize,
    parked_reservations_total: u64,
}

/// Carrier-count inputs captured under admission serialization.
pub(super) struct MemoryMetricState {
    /// Normal admission ceiling.
    pub(super) configured_capacity: CarrierCount,
    /// Active planned demand plus uncovered charges.
    pub(super) admission_used: CarrierCount,
    /// Complete open envelopes.
    pub(super) active_planned_demand: CarrierCount,
    /// Aggregate owner and in-flight acquisition charges.
    pub(super) charged_capacity: CarrierCount,
    /// Capacity whose physical preparation completed.
    pub(super) prepared_capacity: CarrierCount,
    /// Requests currently retained in the FIFO.
    pub(super) queued_reservations: usize,
    /// Cumulative requests that entered the FIFO.
    pub(super) parked_reservations_total: u64,
}

impl MemoryMetrics {
    /// Constructs one sample from validated carrier counts.
    pub(super) fn from_carriers(carrier_size: usize, state: MemoryMetricState) -> Self {
        let configured_capacity_bytes = to_bytes(state.configured_capacity, carrier_size);
        let admission_used_bytes = to_bytes(state.admission_used, carrier_size);
        Self {
            configured_capacity_bytes,
            admission_used_bytes,
            active_planned_demand_bytes: to_bytes(state.active_planned_demand, carrier_size),
            charged_capacity_bytes: to_bytes(state.charged_capacity, carrier_size),
            admission_overage_bytes: admission_used_bytes.saturating_sub(configured_capacity_bytes),
            prepared_capacity_bytes: to_bytes(state.prepared_capacity, carrier_size),
            queued_reservations: state.queued_reservations,
            parked_reservations_total: state.parked_reservations_total,
        }
    }

    /// Returns the normal admission ceiling after carrier rounding.
    pub(crate) fn configured_capacity_bytes(&self) -> u64 {
        self.configured_capacity_bytes
    }

    /// Returns active planned demand plus ownership outside active demand.
    pub(crate) fn admission_used_bytes(&self) -> u64 {
        self.admission_used_bytes
    }

    /// Returns complete envelopes whose acquisition authority remains open.
    pub(crate) fn active_planned_demand_bytes(&self) -> u64 {
        self.active_planned_demand_bytes
    }

    /// Returns aggregate charges held by owners or in-flight acquisitions.
    pub(crate) fn charged_capacity_bytes(&self) -> u64 {
        self.charged_capacity_bytes
    }

    /// Returns admission use above the normal configured ceiling.
    pub(crate) fn admission_overage_bytes(&self) -> u64 {
        self.admission_overage_bytes
    }

    /// Returns capacity whose physical preparation completed.
    pub(crate) fn prepared_capacity_bytes(&self) -> u64 {
        self.prepared_capacity_bytes
    }

    /// Returns reservation requests currently retained in FIFO order.
    pub(crate) fn queued_reservations(&self) -> usize {
        self.queued_reservations
    }

    /// Returns the saturating count of requests that entered the FIFO.
    pub(crate) fn parked_reservations_total(&self) -> u64 {
        self.parked_reservations_total
    }
}

/// Converts a validated carrier count to its public byte representation.
fn to_bytes(count: CarrierCount, carrier_size: usize) -> u64 {
    let count = u64::try_from(count.get()).expect("carrier count must fit metric representation");
    let carrier_size =
        u64::try_from(carrier_size).expect("carrier size must fit metric representation");
    count
        .checked_mul(carrier_size)
        .expect("carrier capacity must fit metric representation")
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use futures_test::task::new_count_waker;

    use super::super::geometry::PoolGeometry;
    use super::super::virtual_memory::page_size;
    use super::super::{BufferPool, CarrierCount, Reservation, ReserveError, ReserveFuture};

    fn test_pool(block_carriers: usize, configured: usize) -> (BufferPool, usize) {
        let page_size = page_size().unwrap().get();
        let geometry = PoolGeometry::new(
            page_size,
            page_size.checked_mul(block_carriers).unwrap(),
            page_size,
        )
        .unwrap();
        let pool =
            BufferPool::from_validated_parts(geometry, CarrierCount::new(configured), 1).unwrap();
        (pool, page_size)
    }

    fn poll_reserve(
        future: &mut ReserveFuture,
        waker: &Waker,
    ) -> Poll<Result<Reservation, ReserveError>> {
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn test_metrics_report_admission_ownership_and_queue_state() {
        let (pool, carrier_size) = test_pool(1, 1);
        let initial = pool.metrics();
        assert_eq!(initial.configured_capacity_bytes(), carrier_size as u64);
        assert_eq!(initial.admission_used_bytes(), 0);
        assert_eq!(initial.active_planned_demand_bytes(), 0);
        assert_eq!(initial.charged_capacity_bytes(), 0);
        assert_eq!(initial.admission_overage_bytes(), 0);
        assert_eq!(initial.prepared_capacity_bytes(), 0);
        assert_eq!(initial.queued_reservations(), 0);
        assert_eq!(initial.parked_reservations_total(), 0);

        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let (waker, _) = new_count_waker();
        let mut queued = pool.reserve(carrier_size);
        assert!(poll_reserve(&mut queued, &waker).is_pending());

        let parked = pool.metrics();
        assert_eq!(parked.admission_used_bytes(), carrier_size as u64);
        assert_eq!(parked.active_planned_demand_bytes(), carrier_size as u64);
        assert_eq!(parked.charged_capacity_bytes(), 0);
        assert_eq!(parked.prepared_capacity_bytes(), carrier_size as u64);
        assert_eq!(parked.queued_reservations(), 1);
        assert_eq!(parked.parked_reservations_total(), 1);

        drop(queued);
        let acquired = pool.acquire(&reservation, carrier_size).unwrap();
        let charged = pool.metrics();
        assert_eq!(charged.charged_capacity_bytes(), carrier_size as u64);
        assert_eq!(charged.queued_reservations(), 0);
        assert_eq!(charged.parked_reservations_total(), 1);

        reservation.close_acquisition();
        let closed = pool.metrics();
        assert_eq!(closed.active_planned_demand_bytes(), 0);
        assert_eq!(closed.admission_used_bytes(), carrier_size as u64);
        assert_eq!(closed.charged_capacity_bytes(), carrier_size as u64);
        drop(acquired);
    }

    #[test]
    fn test_metrics_report_idle_only_overage_without_exposing_ledger_fields() {
        let (pool, carrier_size) = test_pool(2, 1);

        let acquired = pool.acquire_unreserved(carrier_size * 2).unwrap();
        let metrics = pool.metrics();

        assert_eq!(metrics.configured_capacity_bytes(), carrier_size as u64);
        assert_eq!(metrics.admission_used_bytes(), (carrier_size * 2) as u64);
        assert_eq!(metrics.active_planned_demand_bytes(), 0);
        assert_eq!(metrics.charged_capacity_bytes(), (carrier_size * 2) as u64);
        assert_eq!(metrics.admission_overage_bytes(), carrier_size as u64);
        assert_eq!(metrics.prepared_capacity_bytes(), (carrier_size * 2) as u64);
        drop(acquired);
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::geometry::PoolGeometry;
    use super::super::virtual_memory::page_size;
    use super::super::{BufferPool, CarrierCount};
    use crate::runtime::sync::thread;

    fn test_pool() -> (BufferPool, usize) {
        let page_size = page_size().unwrap().get();
        let geometry = PoolGeometry::new(page_size, page_size, page_size).unwrap();
        let pool = BufferPool::from_validated_parts(geometry, CarrierCount::new(1), 1).unwrap();
        (pool, page_size)
    }

    #[test]
    fn test_metrics_sample_close_and_return_as_complete_states() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool();
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let acquired = pool.acquire(&reservation, carrier_size).unwrap();

            let closing = thread::spawn(move || reservation.close_acquisition());
            let returning = thread::spawn(move || drop(acquired));
            let sample = pool.metrics();
            closing.join().unwrap();
            returning.join().unwrap();

            let carrier_size = carrier_size as u64;
            assert_eq!(sample.configured_capacity_bytes(), carrier_size);
            assert!([0, carrier_size].contains(&sample.admission_used_bytes()));
            assert!([0, carrier_size].contains(&sample.active_planned_demand_bytes()));
            assert!([0, carrier_size].contains(&sample.charged_capacity_bytes()));
            assert!(sample.charged_capacity_bytes() <= sample.admission_used_bytes());
            assert_eq!(sample.admission_overage_bytes(), 0);
            assert_eq!(sample.prepared_capacity_bytes(), carrier_size);

            let final_sample = pool.metrics();
            assert_eq!(final_sample.admission_used_bytes(), 0);
            assert_eq!(final_sample.active_planned_demand_bytes(), 0);
            assert_eq!(final_sample.charged_capacity_bytes(), 0);
        });
    }
}
