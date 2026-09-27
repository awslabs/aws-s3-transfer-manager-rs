/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bounded operation vocabulary and byte-record decoding.

pub(in crate::runtime::buffer_pool) const CONFIGURED_CARRIERS: usize = 4;
pub(in crate::runtime::buffer_pool) const FUZZ_RECORD_BYTES: usize = 8;
/// Selects all bytes remaining in the operation's current range.
pub(in crate::runtime::buffer_pool) const ALL_REMAINING_SELECTOR: u16 = u16::from_le_bytes(*b"yy");
/// Selects enough writable capacity to acquire one additional carrier.
pub(in crate::runtime::buffer_pool) const GROW_ONE_CARRIER_SELECTOR: u16 =
    u16::from_le_bytes(*b"zz");
const MAX_FUZZ_OPERATIONS: usize = 128;
const UNRESERVED_ACQUISITION: u8 = 0x80;

/// One state transition attempted through the public pool and buffer interfaces.
///
/// Slot and length fields are selectors normalized against the state present
/// when the operation executes. An inapplicable selector leaves the sequence
/// state unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) enum Operation {
    TryReserve {
        slot: u8,
        envelope: u8,
    },
    StartReserve {
        slot: u8,
        envelope: u8,
    },
    PollReserve {
        request: u8,
        reservation: u8,
    },
    CancelReserve {
        slot: u8,
    },
    CloseReservation {
        slot: u8,
    },
    Acquire {
        reservation: u8,
        buffer: u8,
        carriers: u8,
        unreserved: bool,
    },
    Grow {
        buffer: u8,
        writable: u16,
    },
    Write {
        buffer: u8,
        count: u16,
        byte: u8,
    },
    Publish {
        buffer: u8,
        value: u8,
        count: u16,
    },
    Freeze {
        buffer: u8,
        value: u8,
    },
    CloneValue {
        source: u8,
        output: u8,
    },
    SliceView {
        source: u8,
        output: u8,
        start: u16,
        len: u16,
    },
    AdvanceValue {
        value: u8,
        count: u16,
    },
    AppendValue {
        target: u8,
        source: u8,
    },
    DropBuffer {
        slot: u8,
    },
    DropValue {
        slot: u8,
    },
}

/// Decodes at most 128 fixed-width records.
///
/// A short final record is zero-padded. Selectors retain their full encoded
/// range for state-dependent normalization by the runner.
pub(super) fn decode_fuzz_input(data: &[u8]) -> Vec<Operation> {
    data.chunks(FUZZ_RECORD_BYTES)
        .take(MAX_FUZZ_OPERATIONS)
        .map(decode_fuzz_record)
        .collect()
}

fn decode_fuzz_record(bytes: &[u8]) -> Operation {
    let mut record = [0u8; FUZZ_RECORD_BYTES];
    record[..bytes.len()].copy_from_slice(bytes);
    let first_u16 = u16::from_le_bytes([record[2], record[3]]);

    match record[0] & 0x0f {
        0 => Operation::TryReserve {
            slot: record[1],
            envelope: record[2] % (CONFIGURED_CARRIERS as u8 + 2),
        },
        1 => Operation::StartReserve {
            slot: record[1],
            envelope: record[2] % (CONFIGURED_CARRIERS as u8 + 2),
        },
        2 => Operation::PollReserve {
            request: record[1],
            reservation: record[2],
        },
        3 => Operation::CancelReserve { slot: record[1] },
        4 => Operation::CloseReservation { slot: record[1] },
        5 => Operation::Acquire {
            reservation: record[1],
            buffer: record[2],
            carriers: 1 + record[3] % (CONFIGURED_CARRIERS as u8 + 1),
            unreserved: record[7] & UNRESERVED_ACQUISITION != 0,
        },
        6 => Operation::Grow {
            buffer: record[1],
            writable: first_u16,
        },
        7 => Operation::Write {
            buffer: record[1],
            count: first_u16,
            byte: record[4],
        },
        8 => Operation::Publish {
            buffer: record[1],
            value: record[2],
            count: u16::from_le_bytes([record[3], record[4]]),
        },
        9 => Operation::Freeze {
            buffer: record[1],
            value: record[2],
        },
        10 => Operation::CloneValue {
            source: record[1],
            output: record[2],
        },
        11 => Operation::SliceView {
            source: record[1],
            output: record[2],
            start: u16::from_le_bytes([record[3], record[4]]),
            len: u16::from_le_bytes([record[5], record[6]]),
        },
        12 => Operation::AdvanceValue {
            value: record[1],
            count: first_u16,
        },
        13 => Operation::AppendValue {
            target: record[1],
            source: record[2],
        },
        14 => Operation::DropBuffer { slot: record[1] },
        15 => Operation::DropValue { slot: record[1] },
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_pins_every_operation_tag() {
        let record = [0, 1, 2, 3, 4, 5, 6, 7];
        let expected = [
            Operation::TryReserve {
                slot: 1,
                envelope: 2,
            },
            Operation::StartReserve {
                slot: 1,
                envelope: 2,
            },
            Operation::PollReserve {
                request: 1,
                reservation: 2,
            },
            Operation::CancelReserve { slot: 1 },
            Operation::CloseReservation { slot: 1 },
            Operation::Acquire {
                reservation: 1,
                buffer: 2,
                carriers: 4,
                unreserved: false,
            },
            Operation::Grow {
                buffer: 1,
                writable: 0x0302,
            },
            Operation::Write {
                buffer: 1,
                count: 0x0302,
                byte: 4,
            },
            Operation::Publish {
                buffer: 1,
                value: 2,
                count: 0x0403,
            },
            Operation::Freeze {
                buffer: 1,
                value: 2,
            },
            Operation::CloneValue {
                source: 1,
                output: 2,
            },
            Operation::SliceView {
                source: 1,
                output: 2,
                start: 0x0403,
                len: 0x0605,
            },
            Operation::AdvanceValue {
                value: 1,
                count: 0x0302,
            },
            Operation::AppendValue {
                target: 1,
                source: 2,
            },
            Operation::DropBuffer { slot: 1 },
            Operation::DropValue { slot: 1 },
        ];

        for (tag, expected) in expected.into_iter().enumerate() {
            let mut tagged = record;
            tagged[0] = tag as u8;
            assert_eq!(decode_fuzz_record(&tagged), expected);
        }
    }

    #[test]
    fn decoder_preserves_the_acquisition_tag_when_selecting_unreserved_authority() {
        let mut record = [5, 1, 2, 3, 4, 5, 6, 7];
        record[7] |= UNRESERVED_ACQUISITION;

        assert_eq!(
            decode_fuzz_record(&record),
            Operation::Acquire {
                reservation: 1,
                buffer: 2,
                carriers: 4,
                unreserved: true,
            }
        );
    }

    #[test]
    fn decoder_zero_pads_a_short_final_record() {
        assert_eq!(
            decode_fuzz_input(&[7, 2, 0x34]),
            vec![Operation::Write {
                buffer: 2,
                count: 0x0034,
                byte: 0,
            }]
        );
    }

    #[test]
    fn decoder_limits_each_input_to_128_operations() {
        let input = [15u8; FUZZ_RECORD_BYTES * (MAX_FUZZ_OPERATIONS + 1)];
        let operations = decode_fuzz_input(&input);

        assert_eq!(operations.len(), MAX_FUZZ_OPERATIONS);
        assert!(operations
            .iter()
            .all(|operation| operation == &Operation::DropValue { slot: 15 }));
    }
}
