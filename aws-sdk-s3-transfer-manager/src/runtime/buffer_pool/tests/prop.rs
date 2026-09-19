/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Generated and deterministic typed operation sequences.

#[cfg(not(miri))]
use proptest::prelude::*;

use super::super::test_util::model_harness::run_sequence;
#[cfg(not(miri))]
use super::super::test_util::operation_sequence::CONFIGURED_CARRIERS;
use super::super::test_util::operation_sequence::{
    Operation, ALL_REMAINING_SELECTOR, GROW_ONE_CARRIER_SELECTOR,
};

#[cfg(not(miri))]
const CARRIER_COUNT_RANGE_END: u8 = CONFIGURED_CARRIERS as u8 + 2;

#[cfg(not(miri))]
/// Generates state transitions with higher weight on ownership-producing operations.
fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
        4 => (0u8..8, 0u8..CARRIER_COUNT_RANGE_END)
            .prop_map(|(slot, envelope)| Operation::TryReserve { slot, envelope }),
        4 => (0u8..8, 0u8..CARRIER_COUNT_RANGE_END)
            .prop_map(|(slot, envelope)| Operation::StartReserve { slot, envelope }),
        5 => (0u8..8, 0u8..8).prop_map(|(request, reservation)| Operation::PollReserve {
            request,
            reservation,
        }),
        2 => (0u8..8).prop_map(|slot| Operation::CancelReserve { slot }),
        3 => (0u8..8).prop_map(|slot| Operation::CloseReservation { slot }),
        6 => (
            0u8..8,
            0u8..8,
            1u8..CARRIER_COUNT_RANGE_END,
            any::<bool>(),
        ).prop_map(
            |(reservation, buffer, carriers, unreserved)| Operation::Acquire {
                reservation,
                buffer,
                carriers,
                unreserved,
            },
        ),
        4 => (0u8..8, any::<u16>())
            .prop_map(|(buffer, writable)| Operation::Grow { buffer, writable }),
        7 => (0u8..8, any::<u16>(), any::<u8>())
            .prop_map(|(buffer, count, byte)| Operation::Write {
                buffer,
                count,
                byte,
            }),
        5 => (0u8..8, 0u8..12, any::<u16>()).prop_map(
            |(buffer, value, count)| Operation::Publish {
                buffer,
                value,
                count,
            },
        ),
        4 => (0u8..8, 0u8..12)
            .prop_map(|(buffer, value)| Operation::Freeze { buffer, value }),
        3 => (0u8..12, 0u8..12)
            .prop_map(|(source, output)| Operation::CloneValue { source, output }),
        2 => (0u8..12, 0u8..12, any::<u16>(), any::<u16>()).prop_map(
            |(source, output, start, len)| Operation::SliceView {
                source,
                output,
                start,
                len,
            },
        ),
        4 => (0u8..12, any::<u16>())
            .prop_map(|(value, count)| Operation::AdvanceValue { value, count }),
        2 => (0u8..12, 0u8..12)
            .prop_map(|(target, source)| Operation::AppendValue { target, source }),
        2 => (0u8..8).prop_map(|slot| Operation::DropBuffer { slot }),
        3 => (0u8..12).prop_map(|slot| Operation::DropValue { slot }),
    ]
}

#[cfg(not(miri))]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        max_shrink_iters: 16_384,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_pool_sequences_preserve_bytes_and_ownership(
        operations in proptest::collection::vec(operation_strategy(), 1..80),
    ) {
        let _ = run_sequence(&operations);
    }
}

#[test]
fn deterministic_property_corpus_covers_queue_publication_and_owner_return() {
    let report = run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 2,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 2,
            unreserved: false,
        },
        Operation::Write {
            buffer: 0,
            count: ALL_REMAINING_SELECTOR,
            byte: b'a',
        },
        Operation::Publish {
            buffer: 0,
            value: 0,
            count: 17,
        },
        Operation::Freeze {
            buffer: 0,
            value: 1,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::StartReserve {
            slot: 0,
            envelope: 3,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloneValue {
            source: 1,
            output: 2,
        },
        Operation::DropValue { slot: 0 },
        Operation::DropValue { slot: 1 },
        Operation::AdvanceValue {
            value: 2,
            count: ALL_REMAINING_SELECTOR,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloseReservation { slot: 1 },
        Operation::DropValue { slot: 2 },
    ]);

    assert_eq!(report.queued_requests, 1);
    assert_eq!(report.granted_queued_requests, 1);
    assert_eq!(report.publications, 1);
    assert_eq!(report.freezes, 1);
    assert_eq!(report.clones, 1);
}

#[test]
fn deterministic_property_corpus_grants_after_final_owner_advance() {
    let report = run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 4,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 4,
            unreserved: false,
        },
        Operation::Write {
            buffer: 0,
            count: 10,
            byte: b'x',
        },
        Operation::Freeze {
            buffer: 0,
            value: 0,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::StartReserve {
            slot: 0,
            envelope: 4,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::AdvanceValue {
            value: 0,
            count: 10,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloseReservation { slot: 1 },
    ]);

    assert_eq!(report.queued_requests, 1);
    assert_eq!(report.granted_queued_requests, 1);
    assert_eq!(report.freezes, 1);
}

#[test]
fn deterministic_property_corpus_preserves_closed_growth_and_view_aliases() {
    let report = run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 3,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 1,
            unreserved: false,
        },
        Operation::Grow {
            buffer: 0,
            writable: GROW_ONE_CARRIER_SELECTOR,
        },
        Operation::Write {
            buffer: 0,
            count: 100,
            byte: b'g',
        },
        Operation::Publish {
            buffer: 0,
            value: 0,
            count: 40,
        },
        Operation::SliceView {
            source: 0,
            output: 1,
            start: 1,
            len: 10,
        },
        Operation::CloneValue {
            source: 1,
            output: 2,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::Grow {
            buffer: 0,
            writable: GROW_ONE_CARRIER_SELECTOR,
        },
        Operation::DropBuffer { slot: 0 },
        Operation::DropValue { slot: 0 },
        Operation::DropValue { slot: 1 },
        Operation::AdvanceValue {
            value: 2,
            count: 11,
        },
    ]);

    assert_eq!(report.successful_growths, 1);
    assert_eq!(report.reservation_closed_growth_rejections, 1);
    assert_eq!(report.publications, 1);
    assert_eq!(report.slices, 1);
    assert_eq!(report.clones, 1);
}

#[test]
fn deterministic_property_corpus_covers_unreserved_queue_bypass_and_growth() {
    let report = run_sequence(&[
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 4,
            unreserved: true,
        },
        Operation::StartReserve {
            slot: 0,
            envelope: 1,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 0,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 1,
            carriers: 1,
            unreserved: true,
        },
        Operation::Grow {
            buffer: 1,
            writable: GROW_ONE_CARRIER_SELECTOR,
        },
        Operation::DropBuffer { slot: 1 },
        Operation::DropBuffer { slot: 0 },
        Operation::PollReserve {
            request: 0,
            reservation: 0,
        },
        Operation::CloseReservation { slot: 0 },
    ]);

    assert_eq!(report.successful_growths, 1);
    assert_eq!(report.queued_requests, 1);
    assert_eq!(report.granted_queued_requests, 1);
}
