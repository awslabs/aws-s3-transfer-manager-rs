/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Checked physical layout for pool storage.

use std::fmt;
use std::num::NonZeroUsize;

use super::CarrierCount;

/// Number of carrier bits stored in each bitmap word.
const BITS_PER_BITMAP_WORD: usize = u64::BITS as usize;

/// Invalid or unrepresentable pool geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GeometryError {
    /// Page size is zero.
    ZeroPageSize,
    /// Block size is zero.
    ZeroBlockSize,
    /// Carrier size is zero.
    ZeroCarrierSize,
    /// Carrier size is not a whole number of pages.
    CarrierNotPageMultiple {
        /// Requested carrier size in bytes.
        carrier_size: usize,
        /// Runtime page size in bytes.
        page_size: usize,
    },
    /// Block size is not a whole number of carriers.
    BlockNotCarrierMultiple {
        /// Requested block size in bytes.
        block_size: usize,
        /// Requested carrier size in bytes.
        carrier_size: usize,
    },
    /// A carrier index cannot be represented by the stable identifier.
    CarrierIndexOverflow {
        /// Requested carriers per block.
        carriers_per_block: usize,
    },
    /// Byte-to-carrier conversion received zero.
    ZeroByteRequest,
}

impl fmt::Display for GeometryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPageSize => f.write_str("page size must be nonzero"),
            Self::ZeroBlockSize => f.write_str("block size must be nonzero"),
            Self::ZeroCarrierSize => f.write_str("carrier size must be nonzero"),
            Self::CarrierNotPageMultiple {
                carrier_size,
                page_size,
            } => write!(
                f,
                "carrier size {carrier_size} is not a multiple of page size {page_size}"
            ),
            Self::BlockNotCarrierMultiple {
                block_size,
                carrier_size,
            } => write!(
                f,
                "block size {block_size} is not a multiple of carrier size {carrier_size}"
            ),
            Self::CarrierIndexOverflow { carriers_per_block } => write!(
                f,
                "block carrier count {carriers_per_block} exceeds the carrier index range"
            ),
            Self::ZeroByteRequest => f.write_str("a carrier request must be nonzero"),
        }
    }
}

impl std::error::Error for GeometryError {}

/// Checked page, carrier, block, and bitmap dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PoolGeometry {
    /// Runtime page size in bytes.
    page_size: NonZeroUsize,
    /// Number of bytes prepared and reclaimed together.
    block_size: NonZeroUsize,
    /// Acquisition and ownership unit in bytes.
    carrier_size: NonZeroUsize,
    /// Number of carriers prepared and reclaimed together.
    carriers_per_block: NonZeroUsize,
    /// Bitmap words required to represent every carrier.
    bitmap_words: NonZeroUsize,
    /// Valid carrier bits in the final bitmap word.
    final_word_mask: u64,
}

impl PoolGeometry {
    /// Constructs checked geometry from page, block, and carrier dimensions.
    ///
    /// Carrier size must be page-aligned. Block size must contain a whole
    /// number of carriers, and carrier indices must fit their stored
    /// representation.
    pub(super) fn new(
        page_size: usize,
        block_size: usize,
        carrier_size: usize,
    ) -> Result<Self, GeometryError> {
        let page_size = NonZeroUsize::new(page_size).ok_or(GeometryError::ZeroPageSize)?;
        let block_size = NonZeroUsize::new(block_size).ok_or(GeometryError::ZeroBlockSize)?;
        let carrier_size = NonZeroUsize::new(carrier_size).ok_or(GeometryError::ZeroCarrierSize)?;
        if !carrier_size.get().is_multiple_of(page_size.get()) {
            return Err(GeometryError::CarrierNotPageMultiple {
                carrier_size: carrier_size.get(),
                page_size: page_size.get(),
            });
        }
        if !block_size.get().is_multiple_of(carrier_size.get()) {
            return Err(GeometryError::BlockNotCarrierMultiple {
                block_size: block_size.get(),
                carrier_size: carrier_size.get(),
            });
        }

        let carriers_per_block = NonZeroUsize::new(block_size.get() / carrier_size.get())
            .expect("a nonzero whole-carrier block contains a carrier");
        u32::try_from(carriers_per_block.get() - 1).map_err(|_| {
            GeometryError::CarrierIndexOverflow {
                carriers_per_block: carriers_per_block.get(),
            }
        })?;

        let bitmap_words =
            NonZeroUsize::new(carriers_per_block.get().div_ceil(BITS_PER_BITMAP_WORD))
                .expect("a nonzero carrier count requires a bitmap word");
        let final_bits = carriers_per_block.get() % BITS_PER_BITMAP_WORD;
        let final_word_mask = if final_bits == 0 {
            u64::MAX
        } else {
            (1u64 << final_bits) - 1
        };

        Ok(Self {
            page_size,
            block_size,
            carrier_size,
            carriers_per_block,
            bitmap_words,
            final_word_mask,
        })
    }

    /// Returns the runtime page size in bytes.
    pub(super) fn page_size(self) -> usize {
        self.page_size.get()
    }

    /// Returns the block size in bytes.
    pub(super) fn block_size(self) -> usize {
        self.block_size.get()
    }

    /// Returns the carrier size in bytes.
    pub(super) fn carrier_size(self) -> usize {
        self.carrier_size.get()
    }

    /// Returns the number of carriers in one block.
    pub(super) fn carriers_per_block(self) -> CarrierCount {
        CarrierCount::new(self.carriers_per_block.get())
    }

    /// Returns the number of ownership bitmap words.
    pub(super) fn bitmap_words(self) -> usize {
        self.bitmap_words.get()
    }

    /// Returns valid carrier bits in the final bitmap word.
    ///
    /// A 70-carrier block uses two words. The final mask is `0b11_1111` for
    /// carriers 64 through 69. A block with a whole final word uses
    /// `u64::MAX`.
    pub(super) fn final_word_mask(self) -> u64 {
        self.final_word_mask
    }

    /// Returns valid carrier bits for one bitmap word.
    ///
    /// Complete words use every bit. A partial final word excludes padding
    /// bits. `None` reports a word outside this geometry.
    pub(super) fn bitmap_word_mask(self, word_index: usize) -> Option<u64> {
        if word_index >= self.bitmap_words() {
            return None;
        }
        if word_index + 1 == self.bitmap_words() {
            Some(self.final_word_mask())
        } else {
            Some(u64::MAX)
        }
    }

    /// Rounds a nonzero byte request up to whole carriers.
    pub(super) fn carriers_for_bytes(self, bytes: usize) -> Result<CarrierCount, GeometryError> {
        if bytes == 0 {
            return Err(GeometryError::ZeroByteRequest);
        }
        let count = bytes.div_ceil(self.carrier_size.get());
        Ok(CarrierCount::new(count))
    }

    /// Returns a carrier's byte offset, or `None` when `index` is out of range.
    pub(super) fn carrier_offset(self, index: usize) -> Option<usize> {
        if index >= self.carriers_per_block.get() {
            return None;
        }
        index.checked_mul(self.carrier_size.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_dimensions() {
        assert_eq!(
            PoolGeometry::new(0, 4096, 4096),
            Err(GeometryError::ZeroPageSize)
        );
        assert_eq!(
            PoolGeometry::new(4096, 0, 4096),
            Err(GeometryError::ZeroBlockSize)
        );
        assert_eq!(
            PoolGeometry::new(4096, 4096, 0),
            Err(GeometryError::ZeroCarrierSize)
        );
        assert_eq!(
            PoolGeometry::new(4096, 12288, 6144),
            Err(GeometryError::CarrierNotPageMultiple {
                carrier_size: 6144,
                page_size: 4096,
            })
        );
        assert_eq!(
            PoolGeometry::new(4096, 12288, 8192),
            Err(GeometryError::BlockNotCarrierMultiple {
                block_size: 12288,
                carrier_size: 8192,
            })
        );
        assert_eq!(
            PoolGeometry::new(4096, 4096, 8192),
            Err(GeometryError::BlockNotCarrierMultiple {
                block_size: 4096,
                carrier_size: 8192,
            })
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_carrier_index_overflow() {
        let carriers_per_block = u32::MAX as usize + 2;
        assert_eq!(
            PoolGeometry::new(1, carriers_per_block, 1),
            Err(GeometryError::CarrierIndexOverflow { carriers_per_block })
        );
    }

    #[test]
    fn rounds_byte_requests_to_carriers() {
        let geometry = PoolGeometry::new(4096, 8192 * 8, 8192).unwrap();

        assert_eq!(
            geometry.carriers_for_bytes(1).unwrap(),
            CarrierCount::new(1)
        );
        assert_eq!(
            geometry.carriers_for_bytes(8192).unwrap(),
            CarrierCount::new(1)
        );
        assert_eq!(
            geometry.carriers_for_bytes(8193).unwrap(),
            CarrierCount::new(2)
        );
        assert_eq!(
            geometry.carriers_for_bytes(0),
            Err(GeometryError::ZeroByteRequest)
        );
        assert_eq!(
            geometry.carriers_for_bytes(usize::MAX).unwrap(),
            CarrierCount::new((usize::MAX - 1) / 8192 + 1)
        );
    }

    #[test]
    fn computes_checked_carrier_offsets() {
        let geometry = PoolGeometry::new(4096, 8192 * 3, 8192).unwrap();

        assert_eq!(geometry.carrier_offset(0), Some(0));
        assert_eq!(geometry.carrier_offset(2), Some(16384));
        assert_eq!(geometry.carrier_offset(3), None);
    }

    #[test]
    fn masks_every_partial_bitmap_width() {
        for carriers in 1..=BITS_PER_BITMAP_WORD * 3 {
            let geometry = PoolGeometry::new(1, carriers, 1).unwrap();
            let valid = (0..geometry.bitmap_words())
                .map(|word_index| {
                    let mask = geometry.bitmap_word_mask(word_index).unwrap();
                    let expected = if word_index + 1 == geometry.bitmap_words() {
                        geometry.final_word_mask()
                    } else {
                        u64::MAX
                    };
                    assert_eq!(mask, expected);
                    mask.count_ones() as usize
                })
                .sum::<usize>();

            assert_eq!(
                geometry.bitmap_words(),
                carriers.div_ceil(BITS_PER_BITMAP_WORD)
            );
            assert_eq!(valid, carriers);
            assert_eq!(geometry.bitmap_word_mask(geometry.bitmap_words()), None);
        }
    }

    #[test]
    fn masks_a_seventy_carrier_block() {
        let geometry = PoolGeometry::new(1, 70, 1).unwrap();

        assert_eq!(geometry.bitmap_words(), 2);
        assert_eq!(geometry.final_word_mask(), 0b11_1111);
    }

    #[test]
    fn full_final_word_uses_every_bit() {
        let geometry = PoolGeometry::new(1, 128, 1).unwrap();

        assert_eq!(geometry.bitmap_words(), 2);
        assert_eq!(geometry.final_word_mask(), u64::MAX);
    }

    #[test]
    fn exposes_validated_dimensions() {
        let geometry = PoolGeometry::new(4096, 8192 * 65, 8192).unwrap();

        assert_eq!(geometry.page_size(), 4096);
        assert_eq!(geometry.block_size(), 8192 * 65);
        assert_eq!(geometry.carrier_size(), 8192);
        assert_eq!(geometry.carriers_per_block(), CarrierCount::new(65));
        assert_eq!(geometry.bitmap_words(), 2);
        assert_eq!(geometry.final_word_mask(), 1);
    }
}
