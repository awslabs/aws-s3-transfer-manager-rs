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
    /// Carrier size is zero.
    ZeroCarrierSize,
    /// Carrier size is not a whole number of pages.
    CarrierNotPageMultiple {
        /// Requested carrier size in bytes.
        carrier_size: usize,
        /// Runtime page size in bytes.
        page_size: usize,
    },
    /// Block carrier count is zero.
    ZeroCarriersPerBlock,
    /// A carrier index cannot be represented by the stable identifier.
    CarrierIndexOverflow {
        /// Requested carriers per block.
        carriers_per_block: usize,
    },
    /// Block byte size exceeds `usize`.
    BlockSizeOverflow {
        /// Requested carrier size in bytes.
        carrier_size: usize,
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
            Self::ZeroCarrierSize => f.write_str("carrier size must be nonzero"),
            Self::CarrierNotPageMultiple {
                carrier_size,
                page_size,
            } => write!(
                f,
                "carrier size {carrier_size} is not a multiple of page size {page_size}"
            ),
            Self::ZeroCarriersPerBlock => f.write_str("a block must contain at least one carrier"),
            Self::CarrierIndexOverflow { carriers_per_block } => write!(
                f,
                "block carrier count {carriers_per_block} exceeds the carrier index range"
            ),
            Self::BlockSizeOverflow {
                carrier_size,
                carriers_per_block,
            } => write!(
                f,
                "block size overflows for {carriers_per_block} carriers of {carrier_size} bytes"
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
    /// Acquisition and ownership unit in bytes.
    carrier_size: NonZeroUsize,
    /// Number of carriers prepared and reclaimed together.
    carriers_per_block: NonZeroUsize,
    /// Block size in bytes.
    block_size: NonZeroUsize,
    /// Bitmap words required to represent every carrier.
    bitmap_words: NonZeroUsize,
}

impl PoolGeometry {
    /// Constructs checked geometry from page, carrier, and block dimensions.
    ///
    /// Carrier size must be page-aligned. Block size and carrier indices must
    /// fit their stored representations.
    pub(super) fn new(
        page_size: usize,
        carrier_size: usize,
        carriers_per_block: usize,
    ) -> Result<Self, GeometryError> {
        let page_size = NonZeroUsize::new(page_size).ok_or(GeometryError::ZeroPageSize)?;
        let carrier_size = NonZeroUsize::new(carrier_size).ok_or(GeometryError::ZeroCarrierSize)?;
        if !carrier_size.get().is_multiple_of(page_size.get()) {
            return Err(GeometryError::CarrierNotPageMultiple {
                carrier_size: carrier_size.get(),
                page_size: page_size.get(),
            });
        }

        let carriers_per_block =
            NonZeroUsize::new(carriers_per_block).ok_or(GeometryError::ZeroCarriersPerBlock)?;
        u32::try_from(carriers_per_block.get() - 1).map_err(|_| {
            GeometryError::CarrierIndexOverflow {
                carriers_per_block: carriers_per_block.get(),
            }
        })?;

        let block_size = carrier_size
            .get()
            .checked_mul(carriers_per_block.get())
            .and_then(NonZeroUsize::new)
            .ok_or(GeometryError::BlockSizeOverflow {
                carrier_size: carrier_size.get(),
                carriers_per_block: carriers_per_block.get(),
            })?;
        let bitmap_words =
            NonZeroUsize::new((carriers_per_block.get() - 1) / BITS_PER_BITMAP_WORD + 1)
                .expect("a nonzero carrier count requires a bitmap word");

        Ok(Self {
            page_size,
            carrier_size,
            carriers_per_block,
            block_size,
            bitmap_words,
        })
    }

    /// Returns the runtime page size in bytes.
    pub(super) fn page_size(self) -> usize {
        self.page_size.get()
    }

    /// Returns the carrier size in bytes.
    pub(super) fn carrier_size(self) -> usize {
        self.carrier_size.get()
    }

    /// Returns the number of carriers in one block.
    pub(super) fn carriers_per_block(self) -> CarrierCount {
        CarrierCount::new(self.carriers_per_block.get())
    }

    /// Returns the block size in bytes.
    pub(super) fn block_size(self) -> usize {
        self.block_size.get()
    }

    /// Returns the number of ownership bitmap words.
    pub(super) fn bitmap_words(self) -> usize {
        self.bitmap_words.get()
    }

    /// Rounds a nonzero byte request up to whole carriers.
    pub(super) fn carriers_for_bytes(self, bytes: usize) -> Result<CarrierCount, GeometryError> {
        if bytes == 0 {
            return Err(GeometryError::ZeroByteRequest);
        }
        let count = (bytes - 1) / self.carrier_size.get() + 1;
        Ok(CarrierCount::new(count))
    }

    /// Returns a carrier's byte offset, or `None` when `index` is out of range.
    pub(super) fn carrier_offset(self, index: usize) -> Option<usize> {
        if index >= self.carriers_per_block.get() {
            return None;
        }
        index.checked_mul(self.carrier_size.get())
    }

    /// Returns valid carrier bits, or `None` when the word is out of range.
    ///
    /// Padding bits in the final word are clear.
    ///
    /// A 70-carrier block has two words. Word zero uses `u64::MAX`; word one
    /// uses `0b11_1111` for carriers 64 through 69.
    pub(super) fn valid_mask(self, word_index: usize) -> Option<u64> {
        if word_index >= self.bitmap_words.get() {
            return None;
        }
        if word_index + 1 < self.bitmap_words.get() {
            return Some(u64::MAX);
        }

        let final_bits = self.carriers_per_block.get() % BITS_PER_BITMAP_WORD;
        Some(if final_bits == 0 {
            u64::MAX
        } else {
            (1u64 << final_bits) - 1
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_dimensions() {
        assert_eq!(
            PoolGeometry::new(0, 4096, 1),
            Err(GeometryError::ZeroPageSize)
        );
        assert_eq!(
            PoolGeometry::new(4096, 0, 1),
            Err(GeometryError::ZeroCarrierSize)
        );
        assert_eq!(
            PoolGeometry::new(4096, 6144, 1),
            Err(GeometryError::CarrierNotPageMultiple {
                carrier_size: 6144,
                page_size: 4096,
            })
        );
        assert_eq!(
            PoolGeometry::new(4096, 4096, 0),
            Err(GeometryError::ZeroCarriersPerBlock)
        );
    }

    #[test]
    fn rejects_block_size_overflow() {
        assert_eq!(
            PoolGeometry::new(1, usize::MAX, 2),
            Err(GeometryError::BlockSizeOverflow {
                carrier_size: usize::MAX,
                carriers_per_block: 2,
            })
        );
    }

    #[test]
    fn rounds_byte_requests_to_carriers() {
        let geometry = PoolGeometry::new(4096, 8192, 8).unwrap();

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
        let geometry = PoolGeometry::new(4096, 8192, 3).unwrap();

        assert_eq!(geometry.carrier_offset(0), Some(0));
        assert_eq!(geometry.carrier_offset(2), Some(16384));
        assert_eq!(geometry.carrier_offset(3), None);
    }

    #[test]
    fn masks_every_partial_bitmap_width() {
        for carriers in 1..=BITS_PER_BITMAP_WORD * 3 {
            let geometry = PoolGeometry::new(1, 1, carriers).unwrap();
            let valid = (0..geometry.bitmap_words())
                .map(|word| geometry.valid_mask(word).unwrap().count_ones() as usize)
                .sum::<usize>();

            assert_eq!(
                geometry.bitmap_words(),
                carriers.div_ceil(BITS_PER_BITMAP_WORD)
            );
            assert_eq!(valid, carriers);
            assert_eq!(geometry.valid_mask(geometry.bitmap_words()), None);
        }
    }

    #[test]
    fn masks_a_seventy_carrier_block() {
        let geometry = PoolGeometry::new(1, 1, 70).unwrap();

        assert_eq!(geometry.bitmap_words(), 2);
        assert_eq!(geometry.valid_mask(0), Some(u64::MAX));
        assert_eq!(geometry.valid_mask(1), Some(0b11_1111));
    }

    #[test]
    fn exposes_validated_dimensions() {
        let geometry = PoolGeometry::new(4096, 8192, 65).unwrap();

        assert_eq!(geometry.page_size(), 4096);
        assert_eq!(geometry.carrier_size(), 8192);
        assert_eq!(geometry.carriers_per_block(), CarrierCount::new(65));
        assert_eq!(geometry.block_size(), 8192 * 65);
        assert_eq!(geometry.bitmap_words(), 2);
    }
}
