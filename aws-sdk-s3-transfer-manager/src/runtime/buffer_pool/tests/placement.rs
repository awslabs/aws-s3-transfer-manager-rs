/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Public-lifecycle qualification for whole-word placement.
//!
//! Private block tests prove individual bitmap transitions. These scenarios
//! retain reservations and mutable owners through the same interfaces used by
//! transfer integrations, then reconcile the complete pool after reuse.

use std::sync::Barrier;
use std::thread;

use super::super::test_util::{test_pool, test_pool_with_scan};
use super::super::{BufferPool, CarrierCount, PooledBufMut, Reservation};

const WORD_CARRIERS: usize = u64::BITS as usize;
const REUSE_ROUNDS: usize = 8;

fn acquire(
    pool: &BufferPool,
    reservation: &Reservation,
    carrier_size: usize,
    carriers: usize,
) -> PooledBufMut {
    let buffer = pool
        .acquire(reservation, carrier_size * carriers)
        .expect("placement acquisition");
    assert_eq!(buffer.capacity(), carrier_size * carriers);
    buffer
}

fn assert_one_run(buffer: &PooledBufMut, context: &str) {
    assert_eq!(
        buffer.test_run_count(),
        1,
        "{context}: whole-word acquisition was segmented"
    );
}

fn assert_part_dominant_distribution(parts: &[Option<PooledBufMut>]) {
    let run_counts = parts
        .iter()
        .map(|part| {
            part.as_ref()
                .expect("part-dominant owner is live")
                .test_run_count()
        })
        .collect::<Vec<_>>();
    let segmented = run_counts.iter().filter(|runs| **runs != 1).count();

    assert!(
        segmented <= 1,
        "persistent small owners segmented more than one part: {run_counts:?}"
    );
    assert!(
        run_counts.iter().all(|runs| *runs <= 3),
        "one part required more than three physical runs: {run_counts:?}"
    );
}

fn assert_no_live_ownership(pool: &BufferPool) {
    let audit = pool.audit_quiescent();
    assert_eq!(audit.active_planned_demand, CarrierCount::ZERO);
    assert_eq!(audit.available_coverage, CarrierCount::ZERO);
    assert_eq!(audit.uncovered_charges, CarrierCount::ZERO);
    assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
    assert_eq!(audit.live_carriers, CarrierCount::ZERO);
    assert_eq!(audit.queued_reservations, 0);
    assert_eq!(audit.cleanup_pending_blocks, 0);
}

/// Returns a rotating, non-prefix half-cohort.
///
/// Every fixture part count is coprime with stride three. The walk therefore
/// visits every index before repeating and changes which physical runs are
/// returned without introducing scheduler randomness.
fn release_indices(part_count: usize, round: usize) -> Vec<usize> {
    let release_count = part_count.div_ceil(2);
    (0..release_count)
        .map(|offset| (round + offset * 3) % part_count)
        .collect()
}

#[test]
#[cfg_attr(
    miri,
    ignore = "native suites cover repeated virtual-memory cohorts; focused Miri replay covers the same ownership transitions"
)]
fn whole_word_parts_remain_contiguous_across_capacity_scales() {
    /*
     * Each block holds two equal parts:
     *
     *     one-word part       two-word part
     *     [64][64]            [128][128]
     *
     * The two sizes cover the single-CAS and multi-CAS run paths. One, two,
     * and four blocks make cursor wrap and registry traversal observable while
     * keeping the largest native mapping bounded.
     */
    for part_carriers in [WORD_CARRIERS, WORD_CARRIERS * 2] {
        let block_carriers = part_carriers * 2;
        for blocks in [1, 2, 4] {
            let configured = block_carriers * blocks;
            let (pool, carrier_size) = test_pool(block_carriers, configured);
            let reservation = pool
                .try_reserve(carrier_size * configured)
                .unwrap()
                .expect("whole-pool reservation");
            let part_count = configured / part_carriers;
            let mut parts = (0..part_count)
                .map(|_| {
                    let part = acquire(&pool, &reservation, carrier_size, part_carriers);
                    assert_one_run(&part, "initial placement");
                    Some(part)
                })
                .collect::<Vec<_>>();

            for round in 0..REUSE_ROUNDS {
                let indices = release_indices(part_count, round);
                for &index in &indices {
                    drop(parts[index].take().expect("selected part is live"));
                }
                for index in indices.into_iter().rev() {
                    let part = acquire(&pool, &reservation, carrier_size, part_carriers);
                    assert_one_run(&part, "released-first replacement");
                    parts[index] = Some(part);
                }
            }

            drop(parts);
            drop(reservation);
            assert_no_live_ownership(&pool);
        }
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "threaded placement scheduling is covered by native and TSan suites"
)]
fn persistent_small_claims_bound_fragmentation_during_overlapped_reuse() {
    /*
     * Four 256-carrier blocks contain sixteen bitmap words. The three
     * long-lived small owners consume 81 carriers:
     *
     *     damaged words                     remaining words
     *     [1 + free] [17 + free] [63 + free] [........64........] x 13
     *
     * The reservation prepares all four blocks before acquisition. Rotating
     * optimistic origins place the three small owners in three separate words,
     * leaving thirteen complete words. Six 128-carrier parts remain
     * contiguous; at most one part uses the three damaged regions. The pool is
     * 95% charged, so replacements cannot rely on an unused two-word reserve.
     *
     * The one-word optimistic budget is part of the fixture. It places the
     * small owners at the starts of three consecutive words:
     *
     *     [x..63 free] [17 used..47 free] [63 used][word 3 free]
     *       1..63          81..127             191..255
     *
     * The final two free ranges coalesce. A fragmented part therefore needs
     * at most these three extents; a larger scan budget would define a
     * different placement experiment.
     */
    const BLOCK_CARRIERS: usize = WORD_CARRIERS * 4;
    const BLOCKS: usize = 4;
    const CONFIGURED: usize = BLOCK_CARRIERS * BLOCKS;
    const PART_CARRIERS: usize = WORD_CARRIERS * 2;
    const SMALL_CLAIMS: [usize; 3] = [1, 17, 63];
    const PARTS: usize = 7;
    const OPTIMISTIC_SCAN_WORDS: usize = 1;

    let (pool, carrier_size) =
        test_pool_with_scan(BLOCK_CARRIERS, CONFIGURED, OPTIMISTIC_SCAN_WORDS);
    let reservation = pool
        .try_reserve(carrier_size * CONFIGURED)
        .unwrap()
        .expect("mixed-traffic reservation");
    let small = SMALL_CLAIMS
        .into_iter()
        .map(|carriers| acquire(&pool, &reservation, carrier_size, carriers))
        .collect::<Vec<_>>();
    let mut parts = (0..PARTS)
        .map(|_| {
            let part = acquire(&pool, &reservation, carrier_size, PART_CARRIERS);
            Some(part)
        })
        .collect::<Vec<_>>();
    assert_part_dominant_distribution(&parts);

    for round in 0..REUSE_ROUNDS {
        let indices = release_indices(PARTS, round);
        let released = indices
            .into_iter()
            .map(|index| {
                (
                    index,
                    parts[index].take().expect("selected mixed part is live"),
                )
            })
            .collect::<Vec<_>>();
        let ready = Barrier::new(released.len());
        let replacements = thread::scope(|scope| {
            released
                .into_iter()
                .map(|(index, part)| {
                    let ready = &ready;
                    let pool = &pool;
                    let reservation = &reservation;
                    scope.spawn(move || {
                        ready.wait();
                        drop(part);
                        let replacement = acquire(pool, reservation, carrier_size, PART_CARRIERS);
                        (index, replacement)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().expect("placement worker"))
                .collect::<Vec<_>>()
        });
        for (index, replacement) in replacements {
            parts[index] = Some(replacement);
        }
        assert_part_dominant_distribution(&parts);
    }

    drop(parts);
    drop(small);
    let restored = (0..PARTS)
        .map(|_| {
            let part = acquire(&pool, &reservation, carrier_size, PART_CARRIERS);
            assert_one_run(&part, "whole-part placement after small owners returned");
            part
        })
        .collect::<Vec<_>>();
    drop(restored);
    drop(reservation);
    assert_no_live_ownership(&pool);
}

#[test]
fn partial_word_owner_redirects_a_whole_part_without_segmenting_it() {
    /*
     *     block 0                         block 1
     *     [x...............................][................................]
     *
     * The live one-carrier owner makes block 0 unusable for a two-word claim.
     * The 128-carrier replacement must use block 1 as one run. Returning the
     * small owner restores block 0 for the following replacement.
     */
    const BLOCK_CARRIERS: usize = WORD_CARRIERS * 2;
    const CONFIGURED: usize = WORD_CARRIERS * 3;
    const PART_CARRIERS: usize = WORD_CARRIERS * 2;

    let (pool, carrier_size) = test_pool(BLOCK_CARRIERS, CONFIGURED);
    let reservation = pool
        .try_reserve(carrier_size * CONFIGURED)
        .unwrap()
        .expect("placement reservation");

    let initial = acquire(&pool, &reservation, carrier_size, PART_CARRIERS);
    assert_one_run(&initial, "initial whole part");
    drop(initial);

    let small = acquire(&pool, &reservation, carrier_size, 1);
    let redirected = acquire(&pool, &reservation, carrier_size, PART_CARRIERS);
    assert_one_run(&redirected, "whole part redirected around a partial word");
    drop(redirected);
    drop(small);

    let restored = acquire(&pool, &reservation, carrier_size, PART_CARRIERS);
    assert_one_run(&restored, "whole part after the partial word returned");
    drop(restored);
    drop(reservation);
    assert_no_live_ownership(&pool);
}
