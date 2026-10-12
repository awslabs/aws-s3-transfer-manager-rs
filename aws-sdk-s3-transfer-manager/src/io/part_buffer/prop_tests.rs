/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Property tests for `PartBuffer` admission, rollover, and ownership.
//!
//! Each case creates one `PartBuffer` with a generated maximum length. The compact geometry drives
//! admission pressure and short rollover sequences. A second geometry uses two 65-carrier blocks
//! and begins with an envelope crossing both a bitmap-word boundary and a block boundary. Either
//! case may first reserve the complete pool to force the buffer's initial admission poll to return
//! `Pending` and verify that later polls cannot replace the first requested envelope.
//!
//! Each [`WriteStep`] selects an envelope, polls it to readiness, and writes either none, part, or
//! all of the exposed capacity. After every write, the runner compares the buffer with an expected
//! byte vector and checks its lengths, writable bound, reservation queue, and pool accounting.
//!
//! At the end of the sequence, freezing must reproduce the expected payload. Dropping the frozen
//! value must release every ownership charge. Arbitrary selectors are reduced into ranges valid for
//! the current buffer state so generated cases remain productive while shrinking. Deterministic
//! cases exercise the same runner under Miri.

use std::task::{Context, Poll, Waker};

use bytes::BufMut;
#[cfg(not(miri))]
use proptest::prelude::*;

use super::PartBuffer;
use crate::runtime::buffer_pool::test_pool;

const COMPACT_GEOMETRY: PropertyGeometry = PropertyGeometry {
    block_carriers: 2,
    configured_carriers: 4,
    cross_block_prefix: false,
};
#[cfg(not(miri))]
const MULTI_WORD_MULTI_BLOCK_GEOMETRY: PropertyGeometry = PropertyGeometry {
    block_carriers: 65,
    configured_carriers: 130,
    cross_block_prefix: true,
};

/// Pool shape and optional boundary-crossing prefix for one property case.
#[derive(Clone, Copy, Debug)]
struct PropertyGeometry {
    block_carriers: usize,
    configured_carriers: usize,
    cross_block_prefix: bool,
}

impl PropertyGeometry {
    fn boundary_envelope(self, carrier_size: usize) -> Option<usize> {
        self.cross_block_prefix.then(|| {
            self.block_carriers
                .checked_mul(carrier_size)
                .and_then(|bytes| bytes.checked_add(1))
                .expect("property geometry boundary overflowed")
        })
    }

    fn max_len(self, carrier_size: usize, selector: u32) -> usize {
        match self.boundary_envelope(carrier_size) {
            Some(boundary) => boundary
                .checked_add(1 + selector as usize % (carrier_size + 17))
                .expect("property part length overflowed"),
            None => 1 + selector as usize % (carrier_size * 2 + 17),
        }
    }
}

/// One admission-and-write transition in a generated buffer lifetime.
#[derive(Clone, Copy, Debug)]
struct WriteStep {
    /// Selects a nonzero envelope no larger than the buffer's remaining length.
    envelope_selector: u16,
    /// Selects a write length within the writable range when `fill_envelope` is false.
    write_selector: u16,
    /// Writes the complete writable range instead of a selected prefix.
    fill_envelope: bool,
    /// Byte repeated across the selected write length.
    byte: u8,
}

impl WriteStep {
    const fn new(
        envelope_selector: u16,
        write_selector: u16,
        fill_envelope: bool,
        byte: u8,
    ) -> Self {
        Self {
            envelope_selector,
            write_selector,
            fill_envelope,
            byte,
        }
    }

    fn envelope(self, remaining: usize) -> usize {
        1 + self.envelope_selector as usize % remaining
    }

    fn write_len(self, writable: usize) -> usize {
        if self.fill_envelope {
            writable
        } else if self.write_selector.is_multiple_of(5) {
            0
        } else {
            1 + self.write_selector as usize % writable
        }
    }
}

fn poll_acquire(buffer: &mut PartBuffer, envelope: usize) -> Poll<std::io::Result<()>> {
    let mut context = Context::from_waker(Waker::noop());
    buffer.poll_acquire(&mut context, envelope)
}

/// Runs one generated buffer lifetime and verifies every observable transition.
fn run_case(
    geometry: PropertyGeometry,
    max_len_selector: u32,
    block_initial_admission: bool,
    writes: &[WriteStep],
) {
    let (pool, carrier_size) = test_pool(geometry.block_carriers, geometry.configured_carriers);
    let max_len = geometry.max_len(carrier_size, max_len_selector);
    let mut buffer = PartBuffer::new(pool.clone(), max_len);
    let mut expected = Vec::new();
    let mut blocked_envelope = None;

    let blocker = block_initial_admission.then(|| {
        pool.try_reserve(carrier_size * geometry.configured_carriers)
            .expect("blocking reservation attempt")
            .expect("blocking reservation")
    });

    let boundary_envelope = geometry.boundary_envelope(carrier_size);
    let first_envelope =
        boundary_envelope.or_else(|| writes.first().copied().map(|step| step.envelope(max_len)));
    if let Some(envelope) = first_envelope {
        if blocker.is_some() {
            assert!(poll_acquire(&mut buffer, envelope).is_pending());
            let alternate = 1 + envelope % buffer.remaining();
            assert!(poll_acquire(&mut buffer, alternate).is_pending());
            blocked_envelope = Some(envelope);
        }
    }

    drop(blocker);

    if let Some(envelope) = boundary_envelope {
        apply_write_step(
            &mut buffer,
            &pool,
            max_len,
            &mut expected,
            &mut blocked_envelope,
            WriteStep::new(0, 0, true, b'x'),
            Some(envelope),
        );
    }

    for &step in writes {
        if buffer.remaining() == 0 {
            break;
        }

        apply_write_step(
            &mut buffer,
            &pool,
            max_len,
            &mut expected,
            &mut blocked_envelope,
            step,
            None,
        );
    }

    let output = buffer.freeze();
    assert_eq!(output.clone().into_contiguous(), expected);
    pool.validate_quiescent_for_test();
    let frozen = pool.metrics();
    assert_eq!(frozen.active_planned_demand_bytes(), 0);
    assert_eq!(frozen.queued_reservations(), 0);

    drop(output);
    pool.validate_quiescent_for_test();
    let released = pool.metrics();
    assert_eq!(released.active_planned_demand_bytes(), 0);
    assert_eq!(released.admission_used_bytes(), 0);
    assert_eq!(released.charged_capacity_bytes(), 0);
    assert_eq!(released.queued_reservations(), 0);
}

/// Applies one resolved admission-and-write transition and checks its postconditions.
fn apply_write_step(
    buffer: &mut PartBuffer,
    pool: &crate::memory::BufferPool,
    max_len: usize,
    expected: &mut Vec<u8>,
    blocked_envelope: &mut Option<usize>,
    step: WriteStep,
    envelope_override: Option<usize>,
) {
    let envelope = envelope_override.unwrap_or_else(|| step.envelope(buffer.remaining()));
    let Poll::Ready(result) = poll_acquire(buffer, envelope) else {
        panic!("eligible part-buffer envelope remained pending");
    };
    result.expect("part-buffer acquisition");

    if let Some(latched) = blocked_envelope.take() {
        assert_eq!(
            buffer.remaining_mut(),
            latched,
            "pending admission did not retain its first envelope"
        );
    }

    let writable = buffer.remaining_mut().min(buffer.remaining());
    assert!(writable != 0, "ready part buffer exposed no writable bytes");
    let bytes = vec![step.byte; step.write_len(writable)];
    buffer.put_slice(&bytes);
    expected.extend_from_slice(&bytes);

    assert_eq!(buffer.len(), expected.len());
    assert_eq!(buffer.remaining(), max_len - expected.len());
    assert!(buffer.remaining_mut() <= buffer.remaining());
    pool.validate_quiescent_for_test();
    assert!(pool.metrics().queued_reservations() <= 1);
    assert!(pool.metrics().admission_used_bytes() <= pool.metrics().configured_capacity_bytes());
}

#[cfg(not(miri))]
fn write_step_strategy() -> impl Strategy<Value = WriteStep> {
    (any::<u16>(), any::<u16>(), any::<bool>(), any::<u8>()).prop_map(
        |(envelope_selector, write_selector, fill_envelope, byte)| {
            WriteStep::new(envelope_selector, write_selector, fill_envelope, byte)
        },
    )
}

#[cfg(not(miri))]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 96,
        max_shrink_iters: 8_192,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_envelopes_preserve_bytes_and_bounded_ownership(
        max_len_selector in any::<u32>(),
        block_initial_admission in any::<bool>(),
        writes in proptest::collection::vec(write_step_strategy(), 1..40),
    ) {
        run_case(
            COMPACT_GEOMETRY,
            max_len_selector,
            block_initial_admission,
            &writes,
        );
    }

    #[test]
    fn generated_envelopes_cross_bitmap_words_and_blocks(
        max_len_selector in any::<u32>(),
        block_initial_admission in any::<bool>(),
        writes in proptest::collection::vec(write_step_strategy(), 1..40),
    ) {
        run_case(
            MULTI_WORD_MULTI_BLOCK_GEOMETRY,
            max_len_selector,
            block_initial_admission,
            &writes,
        );
    }
}

#[test]
fn deterministic_property_corpus_covers_pending_rollover_and_tail_reuse() {
    run_case(
        COMPACT_GEOMETRY,
        u32::MAX,
        true,
        &[
            WriteStep::new(3, 7, false, b'a'),
            WriteStep::new(5, u16::MAX, true, b'b'),
            WriteStep::new(11, 9, false, b'c'),
            WriteStep::new(17, u16::MAX, true, b'd'),
            WriteStep::new(23, u16::MAX, true, b'e'),
        ],
    );
}

#[test]
fn deterministic_property_corpus_covers_partial_unblocked_envelopes() {
    run_case(
        COMPACT_GEOMETRY,
        97,
        false,
        &[
            WriteStep::new(3, 7, false, b'a'),
            WriteStep::new(5, 0, false, b'b'),
            WriteStep::new(7, u16::MAX, true, b'c'),
            WriteStep::new(11, 13, false, b'd'),
        ],
    );
}
