/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Object contents that differ by position and by object, so a misplaced range can't match.
//!
//! An object is a sequence of eight-byte blocks. Block `n` of the object with seed `seed` is
//! `splitmix64((seed << 32) | n)`, little-endian, so byte `i` is byte `i % 8` of block `i / 8`.
//! For `n < 2^32` (objects up to 32 GiB), the input `(seed << 32) | n` is distinct for every
//! `(seed, n)`, and [`splitmix64`] maps distinct inputs to distinct outputs. So no two blocks are
//! equal, within an object or across objects with different seeds. A range delivered at the
//! wrong offset, a range delivered twice, or a range of another object differs from the expected
//! bytes in every complete block it covers.
//!
//! The bytes are a pure function of `(seed, offset)`, so a test regenerates the expected bytes
//! of any range instead of storing them.

/// The bytes of object `seed`, `len` long.
pub(crate) fn object_bytes(seed: u32, len: usize) -> Vec<u8> {
    let mut buf = vec![0; len];
    fill_object_bytes(seed, 0, &mut buf);
    buf
}

/// Fills `buf` with the bytes of object `seed` at `[offset, offset + buf.len())`, so a large
/// object can be produced in pieces.
///
/// Panics if the range extends past 32 GiB, where block numbers would overlap the seed's bits.
pub(crate) fn fill_object_bytes(seed: u32, offset: u64, buf: &mut [u8]) {
    let mut pos = offset;
    let mut rest = buf;
    while !rest.is_empty() {
        let n = pos / 8;
        assert!(n < 1 << 32, "object byte {pos} is past 32 GiB");
        let block = splitmix64((u64::from(seed) << 32) | n).to_le_bytes();
        let start = (pos % 8) as usize;
        let n = (8 - start).min(rest.len());
        let (head, tail) = rest.split_at_mut(n);
        head.copy_from_slice(&block[start..start + n]);
        rest = tail;
        pos += n as u64;
    }
}

/// The output function of the SplitMix64 generator (Steele, Lea and Flood, "Fast Splittable
/// Pseudorandom Number Generators", OOPSLA 2014), with its published constants.
///
/// Every step is invertible on `u64`: adding a constant, `z ^ (z >> s)` for `s > 0`, and
/// multiplying by an odd constant modulo 2^64. So the function is a bijection, and distinct
/// inputs give distinct outputs; `tests::unsplitmix64` inverts it.
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1 << 20;

    #[test]
    fn parts_differ_at_every_block() {
        for part in [5 * MIB, 8 * MIB, 16 * MIB] {
            // 251 parts apart is where the `i % 251` pattern of `test_data.rs` repeats.
            for apart in [1, 2, 251] {
                let first = object_bytes_at(7, 0, part);
                let other = object_bytes_at(7, (apart * part) as u64, part);
                assert_blocks_differ(&first, &other, &format!("{apart} parts of {part} apart"));
            }
        }
    }

    #[test]
    fn objects_differ_at_the_same_offset() {
        let a = object_bytes_at(1, 3 * MIB as u64, MIB);
        let b = object_bytes_at(2, 3 * MIB as u64, MIB);
        assert_blocks_differ(&a, &b, "seeds 1 and 2");
    }

    #[test]
    fn filling_in_pieces_matches_one_call() {
        let whole = object_bytes(9, 3 * MIB + 5);
        let mut pieces = vec![0; whole.len()];
        let mut offset = 0;
        for len in [1, 7, 4096, MIB + 3].into_iter().cycle() {
            if offset == pieces.len() {
                break;
            }
            let end = (offset + len).min(pieces.len());
            fill_object_bytes(9, offset as u64, &mut pieces[offset..end]);
            offset = end;
        }
        assert!(whole == pieces);
    }

    /// Inverts [`splitmix64`] step by step, in reverse order.
    fn unsplitmix64(z: u64) -> u64 {
        /// Inverts `z ^ (z >> s)`: each pass recovers `s` more of the high bits.
        fn unxorshift(z: u64, s: u32) -> u64 {
            let mut x = z;
            for _ in 0..64 / s {
                x = z ^ (x >> s);
            }
            x
        }
        /// The inverse of odd `c` modulo 2^64, by Newton's iteration: each step doubles the
        /// number of correct low bits, and `c` is its own inverse modulo 8.
        fn inverse(c: u64) -> u64 {
            let mut inv = c;
            for _ in 0..5 {
                inv = inv.wrapping_mul(2u64.wrapping_sub(c.wrapping_mul(inv)));
            }
            inv
        }
        let mut x = unxorshift(z, 31);
        x = unxorshift(x.wrapping_mul(inverse(0x94D0_49BB_1331_11EB)), 27);
        x = unxorshift(x.wrapping_mul(inverse(0xBF58_476D_1CE4_E5B9)), 30);
        x.wrapping_sub(0x9E37_79B9_7F4A_7C15)
    }

    /// `splitmix64` is a bijection: its inverse recovers every input, at the boundaries of the
    /// seed and block fields and across a spread of values.
    #[test]
    fn splitmix64_is_invertible() {
        let edges = [
            0,
            1,
            u64::MAX,
            1 << 32,
            (1 << 32) - 1,
            u64::from(u32::MAX) << 32,
        ];
        let spread = (0..10_000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        for x in edges.into_iter().chain(spread) {
            assert_eq!(unsplitmix64(splitmix64(x)), x, "input {x:#x}");
        }
    }

    #[test]
    #[should_panic(expected = "past 32 GiB")]
    fn bytes_past_32_gib_panic() {
        fill_object_bytes(1, 32 << 30, &mut [0; 1]);
    }

    fn object_bytes_at(seed: u32, offset: u64, len: usize) -> Vec<u8> {
        let mut buf = vec![0; len];
        fill_object_bytes(seed, offset, &mut buf);
        buf
    }

    fn assert_blocks_differ(a: &[u8], b: &[u8], what: &str) {
        let same = a.chunks(8).zip(b.chunks(8)).filter(|(x, y)| x == y).count();
        assert_eq!(same, 0, "{what}: {same} equal eight-byte blocks");
    }
}
