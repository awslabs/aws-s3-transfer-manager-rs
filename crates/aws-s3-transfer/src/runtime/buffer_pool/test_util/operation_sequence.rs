/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bounded operation vocabulary and byte-record decoding.

/// Fixed pool geometry and byte-decoding policy for one sequence lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) enum SequenceProfile {
    /// Small accounting and ownership state space.
    Compact,
    /// Bitmap-word placement and mixed-tail state space.
    Placement,
}

// Sequence geometry.

/// Carriers in one compact-profile block.
const COMPACT_BLOCK_CARRIERS: u8 = 2;
/// Normal carrier admission ceiling for the compact profile.
const COMPACT_CONFIGURED_CARRIERS: u8 = 4;
/// Carriers represented by one allocator bitmap word.
const BITMAP_WORD_CARRIERS: u8 = u64::BITS as u8;
/// Two complete bitmap words in one placement-profile block.
const PLACEMENT_BLOCK_CARRIERS: u8 = BITMAP_WORD_CARRIERS * 2;
/// One placement block plus one additional bitmap word.
const PLACEMENT_CONFIGURED_CARRIERS: u8 = PLACEMENT_BLOCK_CARRIERS + BITMAP_WORD_CARRIERS;
/// Nontrivial partial-word owner distinct from one and word-minus-one.
const MID_WORD_CARRIERS: u8 = BITMAP_WORD_CARRIERS / 4 + 1;

/// Small accounting profile used by the primary property and fuzz target.
///
/// Two-carrier blocks expose every admission and ownership transition without
/// making each generated operation reserve a production-sized mapping.
pub(in crate::runtime::buffer_pool) const COMPACT_PROFILE: SequenceProfile =
    SequenceProfile::Compact;
/// Whole-word placement profile used by the placement property and fuzz target.
///
/// One bitmap word owns 64 carriers. A 128-carrier block therefore contains
/// exactly two candidate words, while the 192-carrier admission limit permits
/// one 128-carrier part and a live partial-word claim. Reserving 192 carriers
/// prepares two complete blocks and distinguishes contiguous-first placement
/// from fragmented `127 + 1` fallback.
///
/// This is topology-equivalent test geometry, not the production default
/// block size. The arena eligibility parity test prevents this profile from
/// drifting from the production whole-word rule.
pub(in crate::runtime::buffer_pool) const PLACEMENT_PROFILE: SequenceProfile =
    SequenceProfile::Placement;

/// Reservation carrier counts selected by the placement profile.
///
/// The table covers an invalid request, minimum and mid-word requests,
/// values immediately around one bitmap word, one less than a complete
/// placement block, the configured admission ceiling, and one above it.
/// Exact block-size placement is exercised by the acquisition table, where
/// physical run shape is observable.
pub(in crate::runtime::buffer_pool) const PLACEMENT_RESERVATION_CARRIERS: [u8; 9] = [
    0,
    1,
    MID_WORD_CARRIERS,
    BITMAP_WORD_CARRIERS - 1,
    BITMAP_WORD_CARRIERS,
    BITMAP_WORD_CARRIERS + 1,
    PLACEMENT_BLOCK_CARRIERS - 1,
    PLACEMENT_CONFIGURED_CARRIERS,
    PLACEMENT_CONFIGURED_CARRIERS + 1,
];

/// Acquisition carrier counts selected by the placement profile.
///
/// Small values exercise partial-word interference. The remaining values
/// cover immediately below, exactly at, and immediately above one word and
/// one block, plus a legal multi-block acquisition at configured capacity.
pub(in crate::runtime::buffer_pool) const PLACEMENT_ACQUISITION_CARRIERS: [u8; 9] = [
    1,
    2,
    MID_WORD_CARRIERS,
    BITMAP_WORD_CARRIERS - 1,
    BITMAP_WORD_CARRIERS,
    BITMAP_WORD_CARRIERS + 1,
    PLACEMENT_BLOCK_CARRIERS - 1,
    PLACEMENT_BLOCK_CARRIERS,
    PLACEMENT_CONFIGURED_CARRIERS,
];

impl SequenceProfile {
    /// Returns the number of carriers represented by one test block.
    pub(in crate::runtime::buffer_pool) const fn block_carriers(self) -> usize {
        match self {
            Self::Compact => COMPACT_BLOCK_CARRIERS as usize,
            Self::Placement => PLACEMENT_BLOCK_CARRIERS as usize,
        }
    }

    /// Returns the normal reservation-admission ceiling in carriers.
    pub(in crate::runtime::buffer_pool) const fn configured_carriers(self) -> usize {
        match self {
            Self::Compact => COMPACT_CONFIGURED_CARRIERS as usize,
            Self::Placement => PLACEMENT_CONFIGURED_CARRIERS as usize,
        }
    }

    /// Returns whether production should prefer a complete bitmap-word run.
    ///
    /// Eligibility requires a nonzero whole-word claim that fits in one block.
    pub(in crate::runtime::buffer_pool) const fn is_whole_word_claim(
        self,
        carriers: usize,
    ) -> bool {
        carriers != 0
            && carriers <= self.block_carriers()
            && carriers.is_multiple_of(BITMAP_WORD_CARRIERS as usize)
    }

    /// Maps one input byte to a reservation envelope for this profile.
    fn decode_reservation_carriers(self, selector: u8) -> u8 {
        match self {
            Self::Compact => selector % (self.configured_carriers() as u8 + 2),
            Self::Placement => {
                PLACEMENT_RESERVATION_CARRIERS
                    [selector as usize % PLACEMENT_RESERVATION_CARRIERS.len()]
            }
        }
    }

    /// Maps one input byte to a nonzero acquisition size for this profile.
    fn decode_acquisition_carriers(self, selector: u8) -> u8 {
        match self {
            Self::Compact => 1 + selector % (self.configured_carriers() as u8 + 1),
            Self::Placement => {
                PLACEMENT_ACQUISITION_CARRIERS
                    [selector as usize % PLACEMENT_ACQUISITION_CARRIERS.len()]
            }
        }
    }
}

// Fixed-width fuzz record encoding.

/// Number of bytes decoded into one operation.
pub(in crate::runtime::buffer_pool) const FUZZ_RECORD_BYTES: usize = 8;
/// Maximum operations executed from one fuzz input.
const MAX_FUZZ_OPERATIONS: usize = 128;
/// High bit selecting acquisition without reservation-local authority.
const UNRESERVED_ACQUISITION: u8 = 0x80;
/// Selects all bytes remaining in the operation's current range.
pub(in crate::runtime::buffer_pool) const ALL_REMAINING_SELECTOR: u16 = u16::from_le_bytes(*b"yy");
/// Selects enough writable capacity to acquire one additional carrier.
pub(in crate::runtime::buffer_pool) const GROW_ONE_CARRIER_SELECTOR: u16 =
    u16::from_le_bytes(*b"zz");

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
    decode_fuzz_input_for_profile(data, COMPACT_PROFILE)
}

/// Decodes one bounded input using the selected geometry's semantic sizes.
pub(in crate::runtime::buffer_pool) fn decode_fuzz_input_for_profile(
    data: &[u8],
    profile: SequenceProfile,
) -> Vec<Operation> {
    data.chunks(FUZZ_RECORD_BYTES)
        .take(MAX_FUZZ_OPERATIONS)
        .map(|record| decode_fuzz_record(record, profile))
        .collect()
}

fn decode_fuzz_record(bytes: &[u8], profile: SequenceProfile) -> Operation {
    let mut record = [0u8; FUZZ_RECORD_BYTES];
    record[..bytes.len()].copy_from_slice(bytes);
    let first_u16 = u16::from_le_bytes([record[2], record[3]]);

    match record[0] & 0x0f {
        0 => Operation::TryReserve {
            slot: record[1],
            envelope: profile.decode_reservation_carriers(record[2]),
        },
        1 => Operation::StartReserve {
            slot: record[1],
            envelope: profile.decode_reservation_carriers(record[2]),
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
            carriers: profile.decode_acquisition_carriers(record[3]),
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
            assert_eq!(decode_fuzz_record(&tagged, COMPACT_PROFILE), expected);
        }
    }

    #[test]
    fn decoder_preserves_the_acquisition_tag_when_selecting_unreserved_authority() {
        let mut record = [5, 1, 2, 3, 4, 5, 6, 7];
        record[7] |= UNRESERVED_ACQUISITION;

        assert_eq!(
            decode_fuzz_record(&record, COMPACT_PROFILE),
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
    fn placement_profile_decodes_reservation_boundaries() {
        let decoded = (0..PLACEMENT_RESERVATION_CARRIERS.len() as u8)
            .map(|selector| PLACEMENT_PROFILE.decode_reservation_carriers(selector))
            .collect::<Vec<_>>();
        assert_eq!(decoded, PLACEMENT_RESERVATION_CARRIERS);
    }

    #[test]
    fn placement_profile_decodes_acquisition_boundaries() {
        let decoded = (0..PLACEMENT_ACQUISITION_CARRIERS.len() as u8)
            .map(|selector| PLACEMENT_PROFILE.decode_acquisition_carriers(selector))
            .collect::<Vec<_>>();
        assert_eq!(decoded, PLACEMENT_ACQUISITION_CARRIERS);
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
