/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Required coverage: what the two case tables must contain, and the rules every case follows.
//!
//! The requirements are written here by hand, separately from the tables, so removing a shape, a
//! group, an algorithm or a fault line from a table fails a test that names what is missing:
//!
//! - [`SHAPES`]: every shape `algorithm_tests` must have, with the algorithms its upload strategy
//!   produces, the targets it must run on, and its platform tier. Each algorithm of a shape needs a
//!   case with no fault and a case with `corrupt(tail)`.
//! - [`GROUPS`]: every group `download_tests` must have, with its algorithms, its targets, and
//!   lists of [`Required`] cases. Each algorithm of a group must meet every requirement with its
//!   own cases, so one algorithm's cases never stand in for another's.
//! - The platform rules in [`missing`]: every algorithm and every validating mechanism has a case
//!   outside the `extended` tier.
//!
//! [`missing`] checks the generated cases against all three and returns one message per unmet
//! requirement. `case_tables_meet_required_coverage` requires it to return none, and
//! `required_coverage_reports_removals` runs it on the generated cases with parts removed or
//! changed, requiring each removal to be reported.

use super::table_cases;
use super::test_util::vocab::*;

use Alg::Crc64Nvme;
use At::{First, Later, Tail};
use CaseTarget::*;
use Sink::{File, Path, Stream};

/// A shape `algorithm_tests` must have.
struct Shape {
    name: &'static str,
    /// The algorithms the shape's upload strategy can produce.
    algorithms: &'static [Alg],
    targets: &'static [CaseTarget],
    extended: bool,
}

/// A group `download_tests` must have.
struct Group {
    name: &'static str,
    algorithms: &'static [Alg],
    targets: &'static [CaseTarget],
    /// The requirement lists each algorithm must meet.
    requirements: &'static [&'static [Required]],
}

/// A case's outcome, without its algorithm: the algorithm is the group's.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Validated,
    IntegrityError,
    NotValidated(NotValidatedReason),
    Undetected(NotValidatedReason),
}
use Outcome::{IntegrityError, NotValidated, Undetected, Validated};

impl From<Expect> for Outcome {
    fn from(expect: Expect) -> Self {
        match expect {
            Expect::Validated(_) => Validated,
            Expect::IntegrityError(_) => IntegrityError,
            Expect::NotValidated(r) => NotValidated(r),
            Expect::Undetected(r) => Undetected(r),
        }
    }
}

/// A case a group must have for each of its algorithms.
#[derive(Clone, Copy, Debug)]
enum Required {
    /// A case with this outcome and fault on this sink, held or not.
    Case {
        outcome: Outcome,
        fault: Option<Fault>,
        sink: Sink,
        held: bool,
    },
    /// A held, validated case with more ranges than its concurrency, so the other requests are
    /// sent in several waves while the first range is held.
    HeldWaves,
    /// A case whose download part size is set and differs from the object's part size, so
    /// ranges don't start at part boundaries.
    OtherPartSize,
}

/// A case with `outcome` and `fault` on `sink`, not held.
const fn case(outcome: Outcome, fault: Option<Fault>, sink: Sink) -> Required {
    Required::Case {
        outcome,
        fault,
        sink,
        held: false,
    }
}

/// A case with `outcome` and `fault` on `sink`, with the first range held.
const fn held(outcome: Outcome, fault: Option<Fault>, sink: Sink) -> Required {
    Required::Case {
        outcome,
        fault,
        sink,
        held: true,
    }
}

impl Required {
    fn met_by(self, c: &Case) -> bool {
        match self {
            Required::Case {
                outcome,
                fault,
                sink,
                held,
            } => {
                Outcome::from(c.expect) == outcome
                    && c.fault == fault
                    && c.sink == sink
                    && c.hold.is_some() == held
            }
            Required::HeldWaves => {
                c.hold.is_some()
                    && Outcome::from(c.expect) == Validated
                    && c.download
                        .concurrency
                        .is_some_and(|n| c.ranges() > n as u64)
            }
            Required::OtherPartSize => c
                .download
                .part_size
                .is_some_and(|p| p != c.object.part_size),
        }
    }
}

const ALL_TARGETS: &[CaseTarget] = &[MockGp, MockExpress, RealGp, RealExpress];
const ALL_CHECKSUMS: &[Alg] = &[Crc32, Crc32c, Crc64Nvme, Sha1, Sha256];
const FULL_OBJECT_CRCS: &[Alg] = &[Crc32, Crc32c, Crc64Nvme];
const COMPOSITE: &[Alg] = &[Crc32, Crc32c, Sha1, Sha256];

/// Every shape `algorithm_tests` must have. A single PUT can use any algorithm; a FULL_OBJECT
/// multipart upload only a CRC; a COMPOSITE one any but CRC64NVME, which S3 doesn't combine.
const SHAPES: &[Shape] = &[
    Shape {
        name: "single_put_one_get",
        algorithms: ALL_CHECKSUMS,
        targets: ALL_TARGETS,
        extended: false,
    },
    Shape {
        name: "single_put_split",
        algorithms: ALL_CHECKSUMS,
        targets: ALL_TARGETS,
        extended: true,
    },
    Shape {
        name: "full_object_mpu",
        algorithms: FULL_OBJECT_CRCS,
        targets: &[MockGp, RealGp],
        extended: true,
    },
    Shape {
        name: "full_object_mpu_small",
        algorithms: FULL_OBJECT_CRCS,
        targets: &[MockGp, RealGp],
        extended: true,
    },
    Shape {
        name: "composite_aligned",
        algorithms: COMPOSITE,
        targets: ALL_TARGETS,
        extended: true,
    },
    Shape {
        name: "composite_realigned",
        algorithms: COMPOSITE,
        targets: ALL_TARGETS,
        extended: true,
    },
    Shape {
        name: "composite_misaligned",
        algorithms: COMPOSITE,
        targets: ALL_TARGETS,
        extended: true,
    },
    Shape {
        name: "client_sent_no_checksum",
        algorithms: &[NoChecksum],
        targets: ALL_TARGETS,
        extended: true,
    },
    // Seeded on the mock: a new upload can't produce an object without a checksum.
    Shape {
        name: "stored_without_checksum",
        algorithms: &[NoChecksum],
        targets: &[MockGp, MockExpress],
        extended: true,
    },
    // Directory buckets: FULL_OBJECT parts carry part checksums there.
    Shape {
        name: "express_full_object_aligned",
        algorithms: FULL_OBJECT_CRCS,
        targets: &[MockExpress, RealExpress],
        extended: false,
    },
    Shape {
        name: "express_full_object_misaligned",
        algorithms: FULL_OBJECT_CRCS,
        targets: &[MockExpress, RealExpress],
        extended: false,
    },
];

/// A validating download succeeds on every sink, fails on a corrupted first byte, and survives a
/// cut-short first range, which discovery fetches.
const EVERY_SINK: &[Required] = &[
    case(Validated, None, Stream),
    case(Validated, None, Path),
    case(Validated, None, File),
    case(IntegrityError, fault::corrupt(First), Stream),
    case(Validated, fault::truncate(First), Stream),
];

/// An object downloaded in one response: corruption also fails the path and file sinks, and with
/// validation off it is delivered.
const WHOLE_RESPONSE: &[Required] = &[
    case(IntegrityError, fault::corrupt(First), Path),
    case(IntegrityError, fault::corrupt(First), File),
    case(Undetected(Disabled), fault::corrupt(First), Stream),
];

/// An object downloaded in several ranges: a later range, which the request planner fetches, is
/// cut short and survives, is corrupted on the path and file sinks, and is corrupted on its retry;
/// and with the first range held until the others finish, the download is validated, and a
/// corrupted later range fails it.
const MULTI_RANGE: &[Required] = &[
    case(Validated, fault::truncate(Later), Stream),
    case(IntegrityError, fault::corrupt(Later), Path),
    case(IntegrityError, fault::corrupt(Later), File),
    case(IntegrityError, fault::corrupt_retry(Later), Stream),
    held(Validated, None, Stream),
    held(IntegrityError, fault::corrupt(Later), Path),
];

/// A full-object CRC combined from ranges: the short final range is covered, corruption is
/// delivered with validation off, and ranges that don't start at part boundaries combine.
const FULL_OBJECT: &[Required] = &[
    case(IntegrityError, fault::corrupt(Tail), Stream),
    case(Undetected(Disabled), fault::corrupt(Later), Stream),
    Required::OtherPartSize,
];

/// Every group `download_tests` must have.
const GROUPS: &[Group] = &[
    Group {
        name: "single_put",
        algorithms: &[Crc64Nvme, Crc32],
        targets: &[MockGp],
        requirements: &[EVERY_SINK, WHOLE_RESPONSE],
    },
    Group {
        name: "composite_mpu",
        algorithms: &[Crc32, Sha256],
        targets: &[MockGp],
        requirements: &[EVERY_SINK, MULTI_RANGE],
    },
    Group {
        name: "full_object_mpu",
        algorithms: &[Crc64Nvme, Crc32],
        targets: &[MockGp, RealGp],
        requirements: &[EVERY_SINK, MULTI_RANGE, FULL_OBJECT],
    },
    Group {
        name: "full_object_large",
        algorithms: &[Crc64Nvme],
        targets: &[MockGp],
        requirements: &[&[Required::HeldWaves]],
    },
    // Only the first range lines up with a part: corruption there is caught, and later isn't.
    Group {
        name: "unequal_parts",
        algorithms: &[Crc32],
        targets: &[MockGp, RealGp],
        requirements: &[&[
            case(NotValidated(PartialCoverage), None, Stream),
            case(IntegrityError, fault::corrupt(First), Stream),
            case(Undetected(PartialCoverage), fault::corrupt(Later), Stream),
        ]],
    },
    // One response carrying the composite `-N` value, which can't be checked against the bytes.
    Group {
        name: "composite_one_get",
        algorithms: &[Crc32],
        targets: &[MockGp, RealGp],
        requirements: &[&[
            case(NotValidated(CompositeChecksum), None, Stream),
            case(Undetected(CompositeChecksum), fault::corrupt(First), Stream),
        ]],
    },
    Group {
        name: "empty_crc",
        algorithms: &[Crc64Nvme],
        targets: &[MockGp],
        requirements: &[&[case(Validated, None, Stream), case(Validated, None, Path)]],
    },
    Group {
        name: "empty_sha",
        algorithms: &[Sha256],
        targets: &[MockGp],
        requirements: &[&[case(NotValidated(Unavailable), None, Stream)]],
    },
];

/// The path segment after `table`: a case's shape or group.
fn segment_after<'a>(c: &'a Case, table: &str) -> Option<&'a str> {
    let mut segments = c.path.split("::");
    segments.find(|s| *s == table)?;
    segments.next()
}

/// One message for each requirement `cases` doesn't meet. Empty when the tables are complete.
fn missing(cases: &[&Case]) -> Vec<String> {
    let mut missing = Vec::new();
    let in_shape = |c: &Case, shape: &str| segment_after(c, "algorithm_tests") == Some(shape);
    let in_group = |c: &Case, group: &str| segment_after(c, "download_tests") == Some(group);

    // A shape or group's cases list its required targets and sit in its required tier.
    let mut check_placement =
        |name: String, mine: Vec<&&Case>, targets: &[CaseTarget], extended| {
            for t in targets {
                if !mine.is_empty() && !mine.iter().any(|c| c.targets.contains(t)) {
                    missing.push(format!("{name}: target {t:?}"));
                }
            }
            if mine.iter().any(|c| c.extended != extended) {
                let tier = if extended {
                    "must be extended"
                } else {
                    "must run on every platform"
                };
                missing.push(format!("{name}: {tier}"));
            }
        };
    for shape in SHAPES {
        let mine = cases.iter().filter(|c| in_shape(c, shape.name)).collect();
        check_placement(
            format!("algorithm_tests::{}", shape.name),
            mine,
            shape.targets,
            shape.extended,
        );
    }
    for group in GROUPS {
        let mine = cases.iter().filter(|c| in_group(c, group.name)).collect();
        check_placement(
            format!("download_tests::{}", group.name),
            mine,
            group.targets,
            false,
        );
    }

    // Each shape's algorithms, intact and with `corrupt(tail)`.
    for shape in SHAPES {
        for alg in shape.algorithms {
            for (fault, word) in [(None, "no fault"), (fault::corrupt(Tail), "corrupt(tail)")] {
                let found = cases
                    .iter()
                    .any(|c| in_shape(c, shape.name) && c.object.alg == *alg && c.fault == fault);
                if !found {
                    missing.push(format!("algorithm_tests::{} {alg:?}: {word}", shape.name));
                }
            }
        }
    }

    // Each group's requirements, for each of its algorithms.
    for group in GROUPS {
        for alg in group.algorithms {
            let mine: Vec<_> = cases
                .iter()
                .filter(|c| in_group(c, group.name) && c.object.alg == *alg)
                .collect();
            for required in group.requirements.iter().copied().flatten() {
                if !mine.iter().any(|c| required.met_by(c)) {
                    missing.push(format!(
                        "download_tests::{} {alg:?}: {required:?}",
                        group.name
                    ));
                }
            }
        }
    }

    // The CRC kernels are architecture-specific, so every algorithm has a case on every platform,
    // and so does every validating mechanism.
    for alg in ALL_CHECKSUMS {
        if !cases.iter().any(|c| !c.extended && c.object.alg == *alg) {
            missing.push(format!("no every-platform case for {alg:?}"));
        }
    }
    for m in [
        Mechanism::WholeResponse,
        Mechanism::PerPart,
        Mechanism::FullObject,
    ] {
        if !cases.iter().any(|c| !c.extended && c.mechanism == m) {
            missing.push(format!("no every-platform case for {m:?}"));
        }
    }
    missing
}

/// The case tables meet every requirement above.
#[test]
fn case_tables_meet_required_coverage() {
    let missing = missing(&table_cases());
    assert!(
        missing.is_empty(),
        "required coverage missing:\n  {}",
        missing.join("\n  ")
    );
}

/// `missing` reports, by name, each kind of removal from the tables: a shape, a group, one
/// algorithm of a group, one fault line, a target, a tier, and the held waves.
#[test]
fn required_coverage_reports_removals() {
    let without = |remove: &dyn Fn(&Case) -> bool| {
        let cases: Vec<_> = table_cases().into_iter().filter(|c| !remove(c)).collect();
        missing(&cases)
    };
    let changed = |change: &dyn Fn(&Case) -> Option<Case>| {
        let cases: Vec<Case> = table_cases()
            .into_iter()
            .map(|c| change(c).unwrap_or(*c))
            .collect();
        missing(&cases.iter().collect::<Vec<_>>())
    };
    let reports = |found: Vec<String>, expected: &str| {
        assert!(
            found.iter().any(|m| m.starts_with(expected)),
            "expected `{expected}` in {found:#?}"
        );
    };
    let in_group = |c: &Case, group: &str| segment_after(c, "download_tests") == Some(group);
    let in_shape = |c: &Case, shape: &str| segment_after(c, "algorithm_tests") == Some(shape);

    reports(
        without(&|c| in_shape(c, "composite_misaligned")),
        "algorithm_tests::composite_misaligned Crc32: no fault",
    );
    reports(
        without(&|c| in_group(c, "unequal_parts")),
        "download_tests::unequal_parts Crc32: Case { outcome: NotValidated(PartialCoverage)",
    );
    // The other algorithm's cases don't hide the removed one's.
    reports(
        without(&|c| in_group(c, "full_object_mpu") && c.object.alg == Crc32),
        "download_tests::full_object_mpu Crc32: Case { outcome: IntegrityError, fault: Some(corrupt_retry(later))",
    );
    reports(
        without(&|c| in_group(c, "composite_mpu") && c.fault == fault::corrupt_retry(Later)),
        "download_tests::composite_mpu Crc32: Case { outcome: IntegrityError, fault: Some(corrupt_retry(later))",
    );
    reports(
        changed(&|c| {
            in_shape(c, "composite_aligned").then_some(Case {
                targets: &[MockGp, RealGp, RealExpress],
                ..*c
            })
        }),
        "algorithm_tests::composite_aligned: target MockExpress",
    );
    reports(
        changed(&|c| {
            in_shape(c, "single_put_one_get").then_some(Case {
                extended: true,
                ..*c
            })
        }),
        "algorithm_tests::single_put_one_get: must run on every platform",
    );
    // The held large download no longer has more ranges than requests in flight.
    reports(
        changed(&|c| {
            in_group(c, "full_object_large").then(|| {
                let mut c = *c;
                c.download.concurrency = Some(c.ranges() as usize);
                c
            })
        }),
        "download_tests::full_object_large Crc64Nvme: HeldWaves",
    );
}

/// The SDK's checksum algorithms that no upload checksum strategy produces.
const NOT_UPLOADABLE: &[&str] = &["MD5", "SHA512", "XXHASH128", "XXHASH3", "XXHASH64"];

/// Every SDK checksum algorithm is either covered by a shape or listed in [`NOT_UPLOADABLE`], so
/// a new SDK algorithm fails this test until it's placed.
#[test]
fn every_sdk_algorithm_is_placed() {
    let covered: Vec<_> = SHAPES
        .iter()
        .flat_map(|shape| shape.algorithms)
        .filter_map(|alg| alg.sdk())
        .collect();
    for name in aws_sdk_s3::types::ChecksumAlgorithm::values() {
        let algorithm = aws_sdk_s3::types::ChecksumAlgorithm::from(*name);
        assert!(
            covered.contains(&algorithm) != NOT_UPLOADABLE.contains(name),
            "{name} must be either covered by a shape or listed in NOT_UPLOADABLE, not both"
        );
    }
}

/// Every table case follows the rules its checks depend on:
///
/// - no two cases share a seed, so no two share an object's bytes;
/// - no case both holds the first range and faults byte 0, since the mock applies one fault or
///   hold per response;
/// - a `later` fault with the automatic part size is on an object whose first part is the
///   default part size, so byte `B` is the second range's first byte.
#[test]
fn table_cases_are_well_formed() {
    let cases = table_cases();
    let seeds: std::collections::HashSet<_> = cases.iter().map(|c| c.seed()).collect();
    assert_eq!(seeds.len(), cases.len(), "two cases share a seed");
    for c in &cases {
        let at = c.fault.map(|f| f.at);
        assert!(
            !(c.hold.is_some() && at == Some(First)),
            "{}: holds and faults byte 0",
            c.path
        );
        if at == Some(Later) && c.download.part_size.is_none() {
            let first_part = c.object.stored_part_sizes().first().copied();
            assert!(
                first_part.is_none_or(|p| p == c.range_size()),
                "{}: `later` with the automatic part size on parts of {first_part:?}",
                c.path
            );
        }
    }
}

/// Which downloads must report the object's stored checksum values, and which must report none.
#[test]
fn reports_object_values_by_case_kind() {
    let cases = table_cases();
    let reports = |path: &str| {
        cases
            .iter()
            .find(|c| c.path.ends_with(path))
            .unwrap_or_else(|| panic!("no case {path}"))
            .reports_object_values()
    };
    // The whole object in one response.
    assert!(reports(
        "download_tests::single_put::crc64nvme::default::validated::path"
    ));
    // One response carrying the COMPOSITE `-N` value.
    assert!(reports(
        "download_tests::composite_one_get::crc32::default::not_validated::stream"
    ));
    // Validated full-object, from the object's metadata.
    assert!(reports(
        "download_tests::full_object_mpu::crc32::default::validated::stream"
    ));
    // An empty object: its one response is the whole object.
    assert!(reports(
        "download_tests::empty_sha::sha256::default::not_validated::stream"
    ));
    // Ranges carrying part checksums: a part's value is never the object's.
    assert!(!reports(
        "download_tests::composite_mpu::crc32::aligned::validated::stream"
    ));
    // Validation off.
    assert!(!reports(
        "download_tests::single_put::crc64nvme::validation_off::not_validated::stream"
    ));
    // A caller range.
    assert!(!reports(
        "download_tests::full_object_mpu::crc32::caller_range::not_validated::stream"
    ));
    // Nothing stored to report.
    assert!(!reports(
        "algorithm_tests::stored_without_checksum::extended::none::not_validated::stream"
    ));
}
