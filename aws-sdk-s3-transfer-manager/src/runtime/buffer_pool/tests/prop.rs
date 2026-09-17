/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Generated and deterministic typed operation sequences.

#[cfg(not(miri))]
use proptest::prelude::*;

use super::super::test_util::model_harness::{run_sequence, SequenceReport};
use super::super::test_util::operation_sequence::{
    Operation, ALL_REMAINING_SELECTOR, COMPACT_PROFILE, GROW_ONE_CARRIER_SELECTOR,
    PLACEMENT_ACQUISITION_CARRIERS, PLACEMENT_PROFILE, PLACEMENT_RESERVATION_CARRIERS,
};

#[cfg(not(miri))]
const CARRIER_COUNT_RANGE_END: u8 = COMPACT_PROFILE.configured_carriers() as u8 + 2;

/// Runs one sequence through the compact accounting profile.
fn run_compact_sequence(operations: &[Operation]) -> SequenceReport {
    run_sequence(COMPACT_PROFILE, operations)
}

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
/// Generates the same state machine with sizes around bitmap-word boundaries.
///
/// The profile keeps 63/64/65 adjacent so shrinking cannot silently replace a
/// whole-word request with an unrelated small allocation. A 128-carrier
/// request spans both words in one placement-profile block.
fn placement_operation_strategy() -> impl Strategy<Value = Operation> {
    let envelope = || prop::sample::select(PLACEMENT_RESERVATION_CARRIERS.to_vec());
    let acquisition = || prop::sample::select(PLACEMENT_ACQUISITION_CARRIERS.to_vec());

    prop_oneof![
        4 => (0u8..8, envelope())
            .prop_map(|(slot, envelope)| Operation::TryReserve { slot, envelope }),
        4 => (0u8..8, envelope())
            .prop_map(|(slot, envelope)| Operation::StartReserve { slot, envelope }),
        5 => (0u8..8, 0u8..8).prop_map(|(request, reservation)| Operation::PollReserve {
            request,
            reservation,
        }),
        2 => (0u8..8).prop_map(|slot| Operation::CancelReserve { slot }),
        3 => (0u8..8).prop_map(|slot| Operation::CloseReservation { slot }),
        8 => (0u8..8, 0u8..8, acquisition(), any::<bool>()).prop_map(
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
        let _ = run_compact_sequence(&operations);
    }
}

#[cfg(not(miri))]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        max_shrink_iters: 8_192,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_placement_sequences_preserve_bytes_and_ownership(
        operations in proptest::collection::vec(placement_operation_strategy(), 1..64),
    ) {
        let _ = run_sequence(PLACEMENT_PROFILE, &operations);
    }
}

#[test]
fn placement_profile_skips_a_partial_word_before_a_whole_part() {
    /*
     * One 128-carrier block contains two 64-bit bitmap words:
     *
     *     block 0                     block 1
     *     [x...............................][................................]
     *      ^ one live small carrier
     *
     * A 128-carrier request cannot use block 0 as one run. Contiguous-first
     * must select block 1. Prefix fallback would assemble 127 carriers from
     * block 0 and one from block 1, producing two physical runs.
     */
    let report = run_sequence(
        PLACEMENT_PROFILE,
        &[
            Operation::TryReserve {
                slot: 0,
                envelope: 192,
            },
            Operation::Acquire {
                reservation: 0,
                buffer: 0,
                carriers: 128,
                unreserved: false,
            },
            Operation::DropBuffer { slot: 0 },
            Operation::Acquire {
                reservation: 0,
                buffer: 1,
                carriers: 1,
                unreserved: false,
            },
            Operation::Acquire {
                reservation: 0,
                buffer: 2,
                carriers: 128,
                unreserved: false,
            },
            Operation::CloseReservation { slot: 0 },
            Operation::DropBuffer { slot: 1 },
            Operation::DropBuffer { slot: 2 },
        ],
    );

    assert_eq!(report.whole_word_acquisitions, 2);
    assert_eq!(report.preexisting_contiguous_opportunities, 2);
    assert_eq!(report.contiguous_whole_word_acquisitions, 2);
    assert_eq!(report.segmented_whole_word_acquisitions, 0);
    assert_eq!(report.partial_word_acquisitions, 1);
}

#[test]
fn placement_profile_witnesses_general_large_boundaries() {
    /*
     * These successful claims surround the preferred path without entering it:
     *
     *     65   crosses the first bitmap-word boundary by one carrier
     *     127  stops one carrier before the block boundary
     *     192  is word-aligned but exceeds one 128-carrier block
     *
     * Running each size alone makes the report a witness for that exact
     * boundary rather than an aggregate that another operation could satisfy.
     */
    for carriers in [65, 127, 192] {
        let report = run_sequence(
            PLACEMENT_PROFILE,
            &[
                Operation::TryReserve {
                    slot: 0,
                    envelope: 192,
                },
                Operation::Acquire {
                    reservation: 0,
                    buffer: 0,
                    carriers,
                    unreserved: false,
                },
                Operation::CloseReservation { slot: 0 },
                Operation::DropBuffer { slot: 0 },
            ],
        );

        assert_eq!(
            report.general_large_acquisitions, 1,
            "{carriers}-carrier boundary was not acquired"
        );
        assert_eq!(report.whole_word_acquisitions, 0);
        assert_eq!(report.partial_word_acquisitions, 0);
    }
}

#[test]
fn deterministic_property_corpus_covers_queue_publication_and_owner_return() {
    let report = run_compact_sequence(&[
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
    let report = run_compact_sequence(&[
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
    let report = run_compact_sequence(&[
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
    let report = run_compact_sequence(&[
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
