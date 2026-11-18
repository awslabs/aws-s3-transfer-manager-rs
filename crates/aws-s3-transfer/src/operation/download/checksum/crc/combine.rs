/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! CRC combination: the CRC of `A` followed by `B`, from the CRCs of `A` and `B` and
//! the length of `B`.
//!
//! Feeding `n` more zero bytes through a CRC's register multiplies the register by
//! `x^(8n)` in GF(2)\[x\] modulo the CRC polynomial `P`. When a CRC's initial value
//! equals its final XOR, as it does for every algorithm here, their contributions
//! cancel, and
//!
//! ```text
//! crc(A ‖ B) = crc(A) · x^(8·len(B)) mod P  ⊕  crc(B)
//! ```
//!
//! The implementation is the software combine of the AWS Common Runtime's checksum
//! library, [aws-checksums at bbdc308d54], translated to Rust. [`multiply`] is
//! `aws_checksums_multiply_mod_p_reflected` (`source/crc_util.c`), and
//! [`Combiner::combine`] is `aws_checksums_crc32_combine_sw` and
//! `aws_checksums_crc32c_combine_sw` (`source/crc_sw.c`) and
//! `aws_checksums_crc64nvme_combine_sw` (`source/crc64_sw.c`).
//!
//! Each combiner holds a table of `x^(8 · d · 16^i) mod P` for every nonzero hex
//! digit `d` at every position `i` of a 64-bit length, so a combine is one
//! multiplication per nonzero digit of the length, at most 16. aws-checksums ships
//! the tables as literals. Here [`Combiner::new`] computes them at compile time from
//! the polynomial, and a test checks them against those literals.
//!
//! [aws-checksums at bbdc308d54]: https://github.com/awslabs/aws-checksums/tree/bbdc308d54

/// Combination parameters of one reflected CRC.
///
/// Values are reflected, as a reflected CRC stores its register: bit `width - 1`
/// holds the coefficient of `x^0`, and bit 0 that of `x^(width - 1)`.
#[derive(Debug)]
pub(super) struct Combiner {
    /// `P` in the `width + 1`-bit form [`multiply`] reduces by: the reflected
    /// polynomial shifted left one bit, with bit 0 holding the `x^width` term. This is
    /// aws-checksums' `mu_poly[1]` for CRC32 and CRC32C, and `(1 << 64) | mu_poly[1]`
    /// for CRC64NVME.
    poly: u128,
    /// `shift_factors[i][d]` represents `x^(8 · d · 16^i) mod P` in
    /// [`multiply`]'s convention, `x^(8 · d · 16^i - 1)`. Digit 0 is unused.
    shift_factors: [[u64; 16]; 16],
}

impl Combiner {
    /// Builds the combiner for a reflected CRC of `width` bits whose polynomial, in
    /// normal (unreflected) notation without its `x^width` term, is `normal_poly`.
    const fn new(width: u32, normal_poly: u64) -> Self {
        assert!(width >= 8 && width <= 64);
        let reflected = normal_poly.reverse_bits() >> (64 - width);
        let poly = ((reflected as u128) << 1) | 1;
        let mut shift_factors = [[0; 16]; 16];
        // x^8, one zero byte: x^7 in multiply's convention, bit `width - 1 - 7`.
        let mut digit_one = 1u64 << (width - 8);
        let mut position = 0;
        while position < 16 {
            if position > 0 {
                // x^(8 · 16^i) = (x^(8 · 16^(i - 1)))^16: four squarings.
                let mut squarings = 0;
                while squarings < 4 {
                    digit_one = multiply(poly, digit_one, digit_one);
                    squarings += 1;
                }
            }
            shift_factors[position][1] = digit_one;
            let mut digit = 2;
            while digit < 16 {
                shift_factors[position][digit] =
                    multiply(poly, shift_factors[position][digit - 1], digit_one);
                digit += 1;
            }
            position += 1;
        }
        Self {
            poly,
            shift_factors,
        }
    }

    /// The CRC of `A` followed by `B`, from `crc1`, the CRC of `A`, `crc2`, the CRC
    /// of `B`, and `len2`, the byte length of `B`.
    ///
    /// Returns `crc1` for an empty `B`, whose CRC is 0 for these algorithms.
    pub(super) fn combine(&self, mut crc1: u64, crc2: u64, mut len2: u64) -> u64 {
        // TODO(vnext): return `crc1 ^ crc2` here, as the general formula and crc-fast
        // do, so a nonzero `crc2` with a zero length shows up as a mismatch instead of
        // being dropped. The two agree whenever `crc2` is the CRC of an empty `B`. Kept
        // identical to aws-checksums for now.
        if len2 == 0 {
            return crc1;
        }
        let mut position = 0;
        while len2 != 0 {
            let digit = (len2 & 0xf) as usize;
            if digit != 0 {
                crc1 = multiply(self.poly, self.shift_factors[position][digit], crc1);
            }
            position += 1;
            len2 >>= 4;
        }
        crc1 ^ crc2
    }
}

/// `aws_checksums_multiply_mod_p_reflected`: `a · b · x mod P` for reflected
/// `width`-bit `a` and `b`, where `poly` is [`Combiner::poly`].
///
/// Walks one factor's coefficients from `x^0`, bit `width - 1`, toward bit 0. Each
/// step first multiplies the other factor by `x`, a right shift that reduces by
/// `poly` when bit 0 is set, then adds it to the product if the coefficient is set.
/// Multiplying before the first coefficient is the extra factor of `x`, which the
/// shift factors absorb by being stored one power lower. The walk stops after the
/// last set coefficient, so the factor with more trailing zero bits is walked.
const fn multiply(poly: u128, a: u64, b: u64) -> u64 {
    if a == 0 || b == 0 {
        return 0;
    }
    let (a, b) = (a as u128, b as u128);
    let mut hi_bit = (1u128 << (127 - poly.leading_zeros())) >> 1;
    // The factor with the most trailing zero bits ends the loop soonest.
    let (mut x, mut y) = if b.trailing_zeros() > a.trailing_zeros() {
        (b, a)
    } else {
        (a, b)
    };
    let mut product = 0;
    while x != 0 {
        if y & 1 != 0 {
            y ^= poly;
        }
        y >>= 1;
        if x & hi_bit != 0 {
            product ^= y;
            x ^= hi_bit;
        }
        hi_bit >>= 1;
    }
    product as u64
}

/// CRC-32/ISO-HDLC, S3's CRC32: polynomial 0x04C11DB7.
pub(super) static CRC32: Combiner = Combiner::new(32, 0x04C1_1DB7);
/// CRC-32/ISCSI, S3's CRC32C: polynomial 0x1EDC6F41.
pub(super) static CRC32C: Combiner = Combiner::new(32, 0x1EDC_6F41);
/// CRC-64/NVME, S3's CRC64NVME: polynomial 0xAD93D23594C93659.
pub(super) static CRC64_NVME: Combiner = Combiner::new(64, 0xAD93_D235_94C9_3659);

#[cfg(test)]
mod tests {
    //! The combine tests of `aws-checksums` (`tests/crc_test.c` and
    //! `tests/crc64_test.c` at bbdc308d54) and of `aws-c-s3`
    //! (`tests/s3_checksums_combine_tests.c` at a2e3eb9), plus a check of the tables
    //! against `aws-checksums`' literals and of long lengths against `crc-fast`.

    use super::*;
    use aws_smithy_checksums::ChecksumAlgorithm as Sdk;

    /// Each combiner with the SDK algorithm and the matching `crc-fast` algorithm.
    fn algorithms() -> [(&'static Combiner, Sdk, crc_fast::CrcAlgorithm); 3] {
        [
            (&CRC32, Sdk::Crc32, crc_fast::CrcAlgorithm::Crc32IsoHdlc),
            (&CRC32C, Sdk::Crc32c, crc_fast::CrcAlgorithm::Crc32Iscsi),
            (
                &CRC64_NVME,
                Sdk::Crc64Nvme,
                crc_fast::CrcAlgorithm::Crc64Nvme,
            ),
        ]
    }

    /// The SDK's CRC of `bytes` as a number.
    fn crc(algorithm: Sdk, bytes: &[u8]) -> u64 {
        let mut checksum = algorithm.into_impl();
        checksum.update(bytes);
        checksum
            .finalize()
            .iter()
            .fold(0, |value, byte| (value << 8) | u64::from(*byte))
    }

    /// `aws-checksums`' `shift_factors` literals, the `[1]` column, from
    /// `source/crc32.c` (CRC32 and CRC32C, shifted right 32 bits as `crc_sw.c` reads
    /// them) and `source/crc64.c` (CRC64NVME).
    mod crt_tables {
        #[rustfmt::skip]
    pub(super) const CRT_CRC32: [[u64; 16]; 16] = [
        [0x00000000, 0x01000000, 0x00010000, 0x00000100, 0x00000001, 0x77073096, 0x191b3141, 0x01c26a37, 0xb8bc6765, 0x3d6029b0, 0xcb5cd3a5, 0xa6770bb4, 0xccaa009e, 0x177b1443, 0xefc26b3e, 0xc18edfc0],
        [0x00000000, 0x9ba54c6f, 0x01b5fd1d, 0x2a283862, 0xcad38e8f, 0x8e42b13e, 0x101a2331, 0xc64ac0b8, 0x7406fa95, 0x6dd804d9, 0x1d5dce44, 0xf09a54ac, 0xc56d9496, 0x523d48c4, 0xd63a56a6, 0xc4d49c39],
        [0x00000000, 0x03f9f863, 0xc78c44a1, 0x0ac2d904, 0xc84f001c, 0xbdf081c4, 0xa55318bd, 0x231f091c, 0x9a8f75c0, 0x8be6bf49, 0x16335cfe, 0x85dd3539, 0xc68f497a, 0x694ce1d7, 0x27a81a84, 0xb6d9b4e4],
        [0x00000000, 0x13fca91e, 0xdc7b5c5f, 0x121b8a8b, 0x606c5e34, 0xcd028bc2, 0x3f9def1b, 0xaae2f4d7, 0xf6b53986, 0xd7bf8f25, 0x485d4333, 0x84a316e9, 0x0397bf95, 0x04a2a883, 0x1f3e0004, 0x3b59667d],
        [0x00000000, 0x63fd82d2, 0xe4a90215, 0x2d30216a, 0xd91bdb88, 0x4909e378, 0x157e349e, 0x63950184, 0x2bad0e9a, 0xa125c29f, 0x80c9584f, 0xce6c8738, 0x0e8e8c88, 0x335da4cd, 0x2d9daa0f, 0x1e84ee21],
        [0x00000000, 0xbfbcf49c, 0xaec31a2f, 0xf33519ee, 0x5c9cbdde, 0xf5a4ad98, 0xe4ba555a, 0x5df0e43d, 0x9d574428, 0x0bdf6d5a, 0x6d4c1246, 0xcb6f1f1a, 0x64eb408b, 0xaa4a4480, 0x42ef7890, 0x688d9090],
        [0x00000000, 0x8a39e3c1, 0x85352d3c, 0x7d0aaeb0, 0x291a6054, 0x027703b5, 0xcc6652b6, 0xe0dff1b1, 0x53664be1, 0x5b3d5097, 0xe3879e81, 0x90a6d47c, 0xf7a67e01, 0x428ed662, 0x98f7ed21, 0x67e5af6a],
        [0x00000000, 0x52b55e39, 0x80000000, 0xc4e22c3c, 0x40000000, 0x6271161e, 0x20000000, 0x31388b0f, 0x10000000, 0xf524c6a7, 0x08000000, 0x972ae073, 0x04000000, 0xa62df319, 0x02000000, 0xbeae7aac],
        [0x00000000, 0x01000000, 0x00010000, 0x00000100, 0x00000001, 0x77073096, 0x191b3141, 0x01c26a37, 0xb8bc6765, 0x3d6029b0, 0xcb5cd3a5, 0xa6770bb4, 0xccaa009e, 0x177b1443, 0xefc26b3e, 0xc18edfc0],
        [0x00000000, 0x9ba54c6f, 0x01b5fd1d, 0x2a283862, 0xcad38e8f, 0x8e42b13e, 0x101a2331, 0xc64ac0b8, 0x7406fa95, 0x6dd804d9, 0x1d5dce44, 0xf09a54ac, 0xc56d9496, 0x523d48c4, 0xd63a56a6, 0xc4d49c39],
        [0x00000000, 0x03f9f863, 0xc78c44a1, 0x0ac2d904, 0xc84f001c, 0xbdf081c4, 0xa55318bd, 0x231f091c, 0x9a8f75c0, 0x8be6bf49, 0x16335cfe, 0x85dd3539, 0xc68f497a, 0x694ce1d7, 0x27a81a84, 0xb6d9b4e4],
        [0x00000000, 0x13fca91e, 0xdc7b5c5f, 0x121b8a8b, 0x606c5e34, 0xcd028bc2, 0x3f9def1b, 0xaae2f4d7, 0xf6b53986, 0xd7bf8f25, 0x485d4333, 0x84a316e9, 0x0397bf95, 0x04a2a883, 0x1f3e0004, 0x3b59667d],
        [0x00000000, 0x63fd82d2, 0xe4a90215, 0x2d30216a, 0xd91bdb88, 0x4909e378, 0x157e349e, 0x63950184, 0x2bad0e9a, 0xa125c29f, 0x80c9584f, 0xce6c8738, 0x0e8e8c88, 0x335da4cd, 0x2d9daa0f, 0x1e84ee21],
        [0x00000000, 0xbfbcf49c, 0xaec31a2f, 0xf33519ee, 0x5c9cbdde, 0xf5a4ad98, 0xe4ba555a, 0x5df0e43d, 0x9d574428, 0x0bdf6d5a, 0x6d4c1246, 0xcb6f1f1a, 0x64eb408b, 0xaa4a4480, 0x42ef7890, 0x688d9090],
        [0x00000000, 0x8a39e3c1, 0x85352d3c, 0x7d0aaeb0, 0x291a6054, 0x027703b5, 0xcc6652b6, 0xe0dff1b1, 0x53664be1, 0x5b3d5097, 0xe3879e81, 0x90a6d47c, 0xf7a67e01, 0x428ed662, 0x98f7ed21, 0x67e5af6a],
        [0x00000000, 0x52b55e39, 0x80000000, 0xc4e22c3c, 0x40000000, 0x6271161e, 0x20000000, 0x31388b0f, 0x10000000, 0xf524c6a7, 0x08000000, 0x972ae073, 0x04000000, 0xa62df319, 0x02000000, 0xbeae7aac],
    ];
        #[rustfmt::skip]
    pub(super) const CRT_CRC32C: [[u64; 16]; 16] = [
        [0x00000000, 0x01000000, 0x00010000, 0x00000100, 0x00000001, 0xf26b8303, 0x13a29877, 0xa541927e, 0xdd45aab8, 0x38116fac, 0xef306b19, 0x68032cc8, 0x493c7d27, 0xf43ed648, 0xcb567ba5, 0x9771f7c1],
        [0x00000000, 0x3171d430, 0xa2158b34, 0x6051243f, 0x75bba45b, 0x6d883e38, 0x3365346a, 0x963e61cd, 0x7417153f, 0xcf519517, 0x3fc16b86, 0xc54608cd, 0x31c94608, 0x57060022, 0xad327462, 0x048dc5cc],
        [0x00000000, 0x1426a815, 0xe986c148, 0xc63764e6, 0xcdc220dd, 0xe3125636, 0x034a7d63, 0x32d63d5c, 0x1acaec54, 0x5a7c5081, 0x6ea6639f, 0xe30bf1a9, 0xbec12e1e, 0xad361637, 0x74a4ebd4, 0x04663802],
        [0x00000000, 0x6bae74c4, 0x508c2ac8, 0x87ddf93e, 0x7b66d223, 0xbdb9bd72, 0xca0b28a0, 0x9cde8348, 0xc0381349, 0xee9ec095, 0xd2b4990e, 0xe1e4d6a9, 0x3172759d, 0x2dd864dd, 0x8607972e, 0x987a7aca],
        [0x00000000, 0xf902f73d, 0xf760b4e7, 0x44a6484f, 0x78409f1e, 0x81e514d2, 0x6b5e4d64, 0xb10df3bd, 0xa70b0dc6, 0xf1db8206, 0xe40b784d, 0xda83e8cb, 0xd0731164, 0xce5b12b3, 0x1f80c5ea, 0xaa8e81d3],
        [0x00000000, 0xb2e4d22a, 0xe69aa612, 0xb3da10db, 0x7dd9f837, 0x23663f3a, 0xa82979c6, 0xa9e5eef6, 0xfa0e4598, 0x70226ce1, 0x47159656, 0xa1e2eaa7, 0xa8dbf4dc, 0x7eed52d8, 0x42b8047d, 0xe545e261],
        [0x00000000, 0xa0ffe38d, 0xd7752589, 0xd58aa71e, 0x0b6e9e7e, 0x39fa33c1, 0x29bc8f9a, 0x753f60fa, 0x4fd04875, 0xf9437403, 0x071aef18, 0xbfc79e88, 0x6a60ea71, 0x419771c6, 0x9e09f006, 0xace8a6b0],
        [0x00000000, 0x80000000, 0x40000000, 0x20000000, 0x10000000, 0x08000000, 0x04000000, 0x02000000, 0x01000000, 0x00800000, 0x00400000, 0x00200000, 0x00100000, 0x00080000, 0x00040000, 0x00020000],
        [0x00000000, 0x00010000, 0x00000001, 0x13a29877, 0xdd45aab8, 0xef306b19, 0x493c7d27, 0xcb567ba5, 0x3171d430, 0x54075546, 0xf20c0dfe, 0x0f69022b, 0x3743f7bd, 0x5c15eeb4, 0xba4fc28e, 0x2dae840f],
        [0x00000000, 0xa2158b34, 0x75bba45b, 0x3365346a, 0x7417153f, 0x3fc16b86, 0x31c94608, 0xad327462, 0x1426a815, 0x25605e40, 0x258d3fc9, 0x87466f21, 0xcfb65894, 0xf4e995fd, 0xcb65cf95, 0x8857e0fd],
        [0x00000000, 0xe986c148, 0xcdc220dd, 0x034a7d63, 0x1acaec54, 0x6ea6639f, 0xbec12e1e, 0x74a4ebd4, 0x6bae74c4, 0xe9517b5d, 0x18398af9, 0x0c2b8761, 0x9037dc10, 0xb3e90a58, 0xda72e863, 0xb1d4f7ff],
        [0x00000000, 0x508c2ac8, 0x7b66d223, 0xca0b28a0, 0xc0381349, 0xd2b4990e, 0x3172759d, 0x8607972e, 0xf902f73d, 0x537492a5, 0x37415af5, 0xa9584c01, 0xf16af944, 0x6151dbfe, 0x36d7edb3, 0xb231f2a8],
        [0x00000000, 0xf760b4e7, 0x78409f1e, 0x6b5e4d64, 0xa70b0dc6, 0xe40b784d, 0xd0731164, 0x1f80c5ea, 0xb2e4d22a, 0xc8b6adeb, 0x618d2cda, 0x82eaf232, 0xb7d166dc, 0xb628de6c, 0x858ac478, 0xa27c31f9],
        [0x00000000, 0xe69aa612, 0x7dd9f837, 0xa82979c6, 0xfa0e4598, 0x47159656, 0xa8dbf4dc, 0x42b8047d, 0xa0ffe38d, 0xf64a9000, 0x95ef60c9, 0x759bb0a5, 0x4f15effc, 0x00cb412b, 0xfb7bbb4d, 0xbf1d4f29],
        [0x00000000, 0xd7752589, 0x0b6e9e7e, 0x29bc8f9a, 0x4fd04875, 0x071aef18, 0x6a60ea71, 0x9e09f006, 0x80000000, 0xe94ca9bc, 0x05b74f3f, 0x14de47cd, 0xa51e1f42, 0x038d778c, 0xb7c64e40, 0x4f04f803],
        [0x00000000, 0x40000000, 0x10000000, 0x04000000, 0x01000000, 0x00400000, 0x00100000, 0x00040000, 0x00010000, 0x00004000, 0x00001000, 0x00000400, 0x00000100, 0x00000040, 0x00000010, 0x00000004],
    ];
        #[rustfmt::skip]
    pub(super) const CRT_CRC64_NVME: [[u64; 16]; 16] = [
        [0x0000000000000000, 0x0100000000000000, 0x0001000000000000, 0x0000010000000000, 0x0000000100000000, 0x0000000001000000, 0x0000000000010000, 0x0000000000000100, 0x0000000000000001, 0x7f6ef0c830358979, 0x8776a97d73bddf69, 0xff6e4e1f4e4038be, 0x8211147cbaf96306, 0x373d15f784905d1e, 0xe9742a79ef04a5d4, 0xfc5d27f6bf353971],
        [0x0000000000000000, 0x21e9761e252621ac, 0xe1e0bb9d45d7a44c, 0xa3ffdc1fe8e82a8b, 0x62242240ace5045a, 0x03363823e6e791e5, 0x34f5a24e22d66e90, 0x946588403d4adcbc, 0x5f852fb61e8d92dc, 0x3bee332187cc60f7, 0x66650420c4bfb826, 0xf62e65588693c72c, 0x6d2d13de8038b4ca, 0xba7a3407e09207aa, 0x0d1476de2f12000f, 0x224f0e5bd4980292],
        [0x0000000000000000, 0xa043808c0f782663, 0xc2409e2537aa5eb1, 0x9e7c76d179e3fd2e, 0xe09504c2e6ddbb7f, 0x2ac96ab9fb3914d1, 0x951cb5b5cb3c3e58, 0xa53b1866091d47b7, 0xb3996580f54d9368, 0x9d243c1b5ba595d2, 0xe8eafebf51f00d99, 0x93f4f545fca08ce0, 0x6ab65033a0558ae7, 0xd5d9c9ee3cf274e8, 0x5ba822479a292627, 0xbcd080852f93c47e],
        [0x0000000000000000, 0x4166ce950735e4f4, 0x5b1c330db529ab06, 0x2da73f4d18465a70, 0x859be984066ec6ba, 0xa0270010a349b763, 0x74a2e57eab95f420, 0x0a2fc89a5c2984ec, 0x3af0e497e1e4d072, 0x996c3c3a054cb0b5, 0x488fb139897050d0, 0x118906aedee775b4, 0x644e16e93358ceff, 0xbe79336227f3a112, 0x78cec148adfc0b91, 0xfba9372d284d9e59],
        [0x0000000000000000, 0x46002f9207f104c1, 0xba5c3f9a5be147d4, 0x4109a8f396167bb5, 0xaff32afd1ef2efef, 0xd575407640c7bdf1, 0x7b728bb6275cb7d6, 0xf3fae15adce61fc8, 0x477b4484fefad21e, 0x2ff27ce6b1aaede4, 0x6722feaa870051a7, 0x7b8b3c84ef71b19b, 0xd94773f7c45af924, 0xe8ebea1b9742b6bb, 0xc37b4292ad4c9846, 0x78af8dc5920fa5c5],
        [0x0000000000000000, 0x68f52a64646e9e0e, 0x385500f585519dd4, 0x91dd76af8e4d3b67, 0x03fc7c4f716bbbab, 0xe47cab9a57cdd3f1, 0x788a557a0677b83b, 0x4ce9fef3a03036b1, 0x4ae8229113ece164, 0x800f281d14bbc945, 0xedbac14078db4951, 0xdef41794761ee129, 0xb932c785e0889ef0, 0xfa11db1792428921, 0xb6d43ec6a156f54d, 0xdbfa12c4a9690c36],
        [0x0000000000000000, 0x95094d88bc77ea40, 0x166e53d761341c95, 0x4ba2719b7a97d596, 0x548b3bd9ab72b081, 0xb01d2af0752648f6, 0xf8502752c2d20fa9, 0x855fe66535e59e62, 0xaa93b6f3e0530fb7, 0x007bb3e281d680bc, 0x618e8bd4e60bc623, 0x25982a6047b0de18, 0x6a78bb959e21e7ce, 0xcd36a08a5e44f846, 0xb691486a604298cd, 0xf46bce1dd2ffb108],
        [0x0000000000000000, 0x5670c2a4b9829765, 0xa844023bcfc96901, 0xf2b4064f85a523d0, 0xa3b51afc0af8bd66, 0x2098bf45503abbe5, 0x55e1db1d59c9ce22, 0x227c7ee538ba4154, 0x0bbb3614b3ebb8d5, 0xd1f929cea3f79529, 0x567dd80fe98a161b, 0x1be66ab3477a3bbc, 0x0dc64ba3f81413ec, 0x9998ec3533b53ca4, 0x82889ab7a174df10, 0xfdd7f72fedd9ddb0],
        [0x0000000000000000, 0xe7704b67fe3ee39e, 0xbb686d8a80dd76e8, 0xd318af1ad4fa86cf, 0xc02497b480e9c4ff, 0x12028d96d2dedf22, 0xc449d1643fa02c46, 0x89a09b621dd795e0, 0xbd723ba8d4a0ae87, 0xd305d16108b7087d, 0x80d7c8011f328403, 0x36fb1c17ef388d2a, 0xb332083fa1c95a88, 0x44dd26d6f455b51a, 0xb2fc72d9479b525d, 0x050789f6ebb0baf7],
        [0x0000000000000000, 0x7b1e1a90883ce9e1, 0x1b83aba0e2a07689, 0x4891dddc6b657eaa, 0x34157603f0a7dcd6, 0x96102a61fa903b95, 0x0d21de4c2fb52704, 0xdd2f3472ff12f8cf, 0x7e104f86691f042a, 0x5172bc58c615f3e5, 0x10cb8b385c156242, 0xd4e4d62f3634e40f, 0x8a679a8bd16a0c13, 0x9b82fe32e61a4890, 0x3a1637bc77377b26, 0xeff6c4741200464d],
        [0x0000000000000000, 0x9d6d188a0c268c0e, 0x948dedbcbbe69c14, 0x34479bb750628b82, 0x5b0abad438af51ba, 0x440e49a309d099b3, 0xa83d6d501a1f42de, 0xf30eff24f4ba7b93, 0x39c892e29ab5234f, 0x7712c3fcf4997a84, 0x83a4be134ab89db9, 0x0b1d0c7d1f8cc311, 0x5cf94b4fb8c16a96, 0x31a06356eb9010d3, 0xe7e0096d81887c59, 0x2096c9cec17af654],
        [0x0000000000000000, 0xa465e82ff8f8e600, 0x7cc93936965822df, 0x2889293ee4ac8a41, 0x9ef26991b5b059af, 0xc7f557bcba367bb3, 0xead55f1828f1d65a, 0x1bdd74cbb239d65e, 0x2303ef90c5d2844c, 0xdb3ecfa71b2413ad, 0x8fad79b09cb04d7d, 0xb1695ee414d008db, 0x3a5e0b5758cbda67, 0x2f0a5a0075b1a682, 0xb56cf53356b7e1cc, 0x4b907b2fd5e34069],
        [0x0000000000000000, 0x089ebc585c9e0649, 0x03ca03d9ac82486f, 0x92bcb733562fa89e, 0x51587923d484fe95, 0xa3bc73ad62c69f4d, 0xad69344ae204d507, 0x5bd59816120c0a00, 0x5ee2c0aa74bd4030, 0x6feb154228453f78, 0x9c1544387ff8c7d4, 0x3bd3d623c3939cbb, 0xc116574e87db2fe4, 0xf9ad46eaec4431d9, 0x63ed0e605a0ed0c9, 0xc31df4bb3cf09acf],
        [0x0000000000000000, 0x79a6f16b933e4e44, 0x36598f099bd0f3db, 0x2ba39a573d6a3a82, 0x4a35e540a1479edb, 0x08dc8ce10912f553, 0xaf85d12c2c327b77, 0x57b4e8aab69d5dfd, 0x3f92f762000463b5, 0x02968c8cce36c87a, 0x7b9831345f9ce8e4, 0x3b200897594d3224, 0x857fc04a017e47b8, 0x388481ec8819d189, 0xa998abdadfb22862, 0xfc0b631d2a4084f4],
        [0x0000000000000000, 0x69061a237909e68c, 0x185094e576ee8b53, 0xc95a1e2c201b8699, 0x2a080359358eb362, 0x568459818b19cb95, 0xd26e080014835b38, 0xbf827f8c5f10a0d3, 0xe31ac9bb87f78e1f, 0x0652d134d0d85cc3, 0xa7eb4c774d5d9b3a, 0x21ed8a32f45b580d, 0xf9944094fd278e14, 0x7847b2bc52e3f82d, 0xbccd3eab3f0e9eb9, 0x9e0b1672fd087e6d],
        [0x0000000000000000, 0x2910ba4b04c03faf, 0x8000000000000000, 0x8ee4ce0c2e2bd662, 0x4000000000000000, 0x477267061715eb31, 0x2000000000000000, 0xb9d5a0aaa7c13c2d, 0x1000000000000000, 0xc686437cffab57a3, 0x0800000000000000, 0xf92fb297d39e6264, 0x0400000000000000, 0x7c97d94be9cf3132, 0x0200000000000000, 0x3e4beca5f4e79899],
    ];
    }

    #[test]
    fn shift_factors_match_aws_checksums() {
        for (combiner, crt) in [
            (&CRC32, &crt_tables::CRT_CRC32),
            (&CRC32C, &crt_tables::CRT_CRC32C),
            (&CRC64_NVME, &crt_tables::CRT_CRC64_NVME),
        ] {
            assert_eq!(&combiner.shift_factors, crt);
        }
    }

    /// `aws-checksums`' `test_crc32_combine`, `test_crc32c_combine` and
    /// `test_crc64nvme_combine`.
    #[test]
    fn aws_checksums_combine_cases() {
        for (combiner, sdk, _) in algorithms() {
            for (a, b) in [
                (&b"aaaa"[..], &b"bbbb"[..]),
                (&b"cccccccccccccccccccc"[..], &b"dd"[..]),
                (&b""[..], &b"ffff"[..]),
            ] {
                let whole = [a, b].concat();
                assert_eq!(
                    combiner.combine(crc(sdk, a), crc(sdk, b), b.len() as u64),
                    crc(sdk, &whole),
                    "{sdk:?} {a:?} {b:?}"
                );
            }
        }
    }

    /// The input of `aws-c-s3`'s combine tests.
    const AWS_C_S3_INPUT: &[u8] = b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn\
hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";

    /// `aws-c-s3`'s `checksum_combine_crc32`, `checksum_combine_crc32c` and
    /// `checksum_combine_crc64nvme`: every split of the input, including at 0 and at
    /// its length.
    #[test]
    fn aws_c_s3_combine_every_split() {
        let input = if cfg!(miri) {
            &AWS_C_S3_INPUT[..16]
        } else {
            AWS_C_S3_INPUT
        };
        for (combiner, sdk, _) in algorithms() {
            let whole = crc(sdk, input);
            for split in 0..=input.len() {
                let (head, tail) = input.split_at(split);
                assert_eq!(
                    combiner.combine(crc(sdk, head), crc(sdk, tail), tail.len() as u64),
                    whole,
                    "{sdk:?} split at {split}"
                );
            }
        }
    }

    /// `aws-c-s3`'s `checksum_combine_many_blocks`: folding uneven blocks into one
    /// accumulator in order, then the rest as a final block, matches the whole input.
    #[test]
    fn aws_c_s3_combine_many_blocks() {
        let input: Vec<u8> = (0..112u32).map(|i| (i * 31 + 7) as u8).collect();
        for (combiner, sdk, _) in algorithms() {
            let mut accumulator = crc(sdk, b"");
            let mut remaining = &input[..];
            for size in [1, 7, 16, 3, 64, 21] {
                let (block, rest) = remaining.split_at(size.min(remaining.len()));
                accumulator = combiner.combine(accumulator, crc(sdk, block), block.len() as u64);
                remaining = rest;
            }
            if !remaining.is_empty() {
                accumulator =
                    combiner.combine(accumulator, crc(sdk, remaining), remaining.len() as u64);
            }
            assert_eq!(accumulator, crc(sdk, &input), "{sdk:?}");
        }
    }

    /// `aws-c-s3`'s `checksum_combine_identity` and `checksum_combine_empty_tail`: an
    /// empty accumulator is the identity, and an empty block changes nothing.
    #[test]
    fn aws_c_s3_combine_identity_and_empty_tail() {
        for (combiner, sdk, _) in algorithms() {
            let input = crc(sdk, AWS_C_S3_INPUT);
            let len = AWS_C_S3_INPUT.len() as u64;
            assert_eq!(
                combiner.combine(crc(sdk, b""), input, len),
                input,
                "{sdk:?}"
            );
            assert_eq!(combiner.combine(input, crc(sdk, b""), 0), input, "{sdk:?}");
        }
    }

    /// For lengths far beyond what hashing can cover, including every hexadecimal digit
    /// position, the result matches `crc-fast`'s matrix-based `checksum_combine`.
    #[test]
    fn combining_matches_crc_fast() {
        let mut lengths = vec![
            0,
            1,
            15,
            16,
            255,
            4096,
            5 << 20,
            8 << 20,
            (8 << 20) + 1,
            u64::from(u32::MAX),
            1 << 32,
            (1 << 40) + 12_345,
            0x0123_4567_89ab_cdef,
            u64::MAX >> 3,
            u64::MAX,
        ];
        let mut rng = fastrand::Rng::with_seed(0x6372_6331);
        let random = if cfg!(miri) { 4 } else { 200 };
        lengths.extend((0..random).map(|_| rng.u64(..) >> rng.u32(0..64)));
        for (combiner, _, crc_fast) in algorithms() {
            let mask = if std::ptr::eq(combiner, &CRC64_NVME) {
                u64::MAX
            } else {
                u64::from(u32::MAX)
            };
            for &length in &lengths {
                let (crc1, crc2) = (rng.u64(..) & mask, rng.u64(..) & mask);
                let expected = if length == 0 {
                    // crc-fast returns crc1 ^ crc2; aws-checksums, and this combine,
                    // return crc1 (see the TODO in `Combiner::combine`). They agree
                    // when crc2 is the CRC of an empty `B`, which is 0.
                    crc1
                } else {
                    crc_fast::checksum_combine(crc_fast, crc1, crc2, length)
                };
                assert_eq!(
                    combiner.combine(crc1, crc2, length),
                    expected,
                    "{crc_fast:?} {crc1:#x} {crc2:#x} {length}"
                );
            }
        }
    }
}
