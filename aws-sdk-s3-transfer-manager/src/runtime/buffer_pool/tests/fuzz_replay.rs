/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Curated byte-sequence replay through the model harness.

use super::super::test_util::model_harness::{
    run_fuzz_input, run_fuzz_input_with_carrier_size, run_fuzz_input_with_profile_and_carrier_size,
    run_placement_fuzz_input, SequenceReport,
};
use super::super::test_util::operation_sequence::{FUZZ_RECORD_BYTES, PLACEMENT_PROFILE};
use super::super::virtual_memory::page_size;

const CURATED_FUZZ_CORPUS: &[(&str, &[u8])] = &[
    (
        "lifecycle",
        include_bytes!("corpus/buffer-pool-operations/lifecycle"),
    ),
    (
        "queue_and_return",
        include_bytes!("corpus/buffer-pool-operations/queue_and_return"),
    ),
    (
        "aliases_and_growth",
        include_bytes!("corpus/buffer-pool-operations/aliases_and_growth"),
    ),
];

const PLACEMENT_FUZZ_CORPUS: &[(&str, &[u8])] = &[
    (
        "partial-word-before-part",
        include_bytes!("corpus/buffer-pool-placement/partial-word-before-part"),
    ),
    (
        "fragmented-fallback",
        include_bytes!("corpus/buffer-pool-placement/fragmented-fallback"),
    ),
];

#[test]
fn curated_inputs_replay_through_the_reference_model() {
    for (name, input) in CURATED_FUZZ_CORPUS {
        assert_replay_at_supported_carrier_sizes(
            name,
            input,
            run_fuzz_input,
            run_fuzz_input_with_carrier_size,
            assert_named_milestones,
        );
    }
}

#[test]
fn placement_inputs_replay_through_the_reference_model() {
    for (name, input) in PLACEMENT_FUZZ_CORPUS {
        assert_replay_at_supported_carrier_sizes(
            name,
            input,
            run_placement_fuzz_input,
            |input, carrier_size| {
                run_fuzz_input_with_profile_and_carrier_size(input, PLACEMENT_PROFILE, carrier_size)
            },
            assert_placement_milestones,
        );
    }
}

fn assert_replay_at_supported_carrier_sizes(
    name: &str,
    input: &[u8],
    run_default: impl Fn(&[u8]) -> SequenceReport,
    run_with_carrier_size: impl Fn(&[u8], usize) -> SequenceReport,
    assert_milestones: impl Fn(&str, SequenceReport),
) {
    assert_valid_record_file(name, input);
    assert_milestones(name, run_default(input));

    let runtime_page_size = page_size().expect("runtime page size").get();
    for carrier_size in [4 * 1024, 16 * 1024, 64 * 1024] {
        if carrier_size != runtime_page_size
            && carrier_size >= runtime_page_size
            && carrier_size.is_multiple_of(runtime_page_size)
        {
            assert_milestones(name, run_with_carrier_size(input, carrier_size));
        }
    }
}

fn assert_valid_record_file(name: &str, input: &[u8]) {
    assert!(!input.is_empty(), "{name} is empty");
    assert_eq!(
        input.len() % FUZZ_RECORD_BYTES,
        0,
        "{name} contains a partial operation record"
    );
}

fn assert_named_milestones(name: &str, report: SequenceReport) {
    match name {
        "lifecycle" => {
            assert_eq!(report.publications, 1);
            assert_eq!(report.freezes, 1);
        }
        "queue_and_return" => {
            assert_eq!(report.queued_requests, 1);
            assert_eq!(report.granted_queued_requests, 1);
        }
        "aliases_and_growth" => {
            assert_eq!(report.successful_growths, 1);
            assert_eq!(report.publications, 1);
            assert_eq!(report.slices, 1);
            assert_eq!(report.clones, 1);
        }
        _ => panic!("curated input has no semantic assertions: {name}"),
    }
}

fn assert_placement_milestones(name: &str, report: SequenceReport) {
    match name {
        "partial-word-before-part" => {
            assert_eq!(report.whole_word_acquisitions, 2);
            assert_eq!(report.preexisting_contiguous_opportunities, 2);
            assert_eq!(report.contiguous_whole_word_acquisitions, 2);
            assert_eq!(report.segmented_whole_word_acquisitions, 0);
            assert_eq!(report.partial_word_acquisitions, 1);
            assert_eq!(report.freezes, 1);
        }
        "fragmented-fallback" => {
            assert_eq!(report.whole_word_acquisitions, 1);
            assert_eq!(report.preexisting_contiguous_opportunities, 0);
            assert_eq!(report.contiguous_whole_word_acquisitions, 0);
            assert_eq!(report.segmented_whole_word_acquisitions, 1);
            assert_eq!(report.partial_word_acquisitions, 3);
        }
        _ => panic!("curated placement input has no semantic assertions: {name}"),
    }
}

// cargo-ndk copies the test binary to the device without the package source
// tree. Host CI retains the exact source-directory manifest assertion.
#[cfg(all(not(miri), not(target_os = "android")))]
#[test]
fn manifest_matches_the_checked_in_directory() {
    assert_manifest_matches("buffer-pool-operations", CURATED_FUZZ_CORPUS);
    assert_manifest_matches("buffer-pool-placement", PLACEMENT_FUZZ_CORPUS);
}

#[cfg(all(not(miri), not(target_os = "android")))]
fn assert_manifest_matches(directory: &str, corpus: &[(&str, &[u8])]) {
    let corpus_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/runtime/buffer_pool/tests/corpus")
        .join(directory);
    let mut actual = std::fs::read_dir(&corpus_dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", corpus_dir.display()))
        .map(|entry| {
            let entry = entry.expect("failed to read curated corpus entry");
            assert!(
                entry
                    .file_type()
                    .expect("failed to read curated corpus entry type")
                    .is_file(),
                "curated corpus contains a non-file entry: {}",
                entry.path().display()
            );
            entry
                .file_name()
                .into_string()
                .expect("curated corpus filename is not UTF-8")
        })
        .collect::<Vec<_>>();
    actual.sort();

    let mut expected = corpus
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(actual, expected);
}
