/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Negative controls for the case checks: each test shows that one check fails when the condition
//! it names doesn't hold, so a passing case is evidence of its outcome.
//!
//! Each test executes a real case from the tables on the mock, then checks the execution against a
//! changed copy of the case: one whose expected outcome, mechanism, fault or hold the execution
//! doesn't satisfy. Checks of the request journal are run on their own, so an earlier outcome
//! check can't fail first. Where the condition can't be produced by a download, the test changes
//! the recorded execution instead.

use super::table_cases;
use super::test_util::runner::Execution;
use super::test_util::vocab::*;

/// The case in the tables whose path ends with `suffix`.
fn table_case(suffix: &str) -> &'static Case {
    table_cases()
        .into_iter()
        .find(|c| c.path.ends_with(suffix))
        .unwrap_or_else(|| panic!("no case ends with {suffix}"))
}

/// Executes the table case whose path ends with `suffix` on `mock_gp`.
async fn execute(suffix: &str) -> (&'static Case, Execution) {
    let case = table_case(suffix);
    (case, case.execute(CaseTarget::MockGp).await)
}

/// `case` with `change` applied.
fn changed(case: &Case, change: impl FnOnce(&mut Case)) -> Case {
    let mut case = *case;
    change(&mut case);
    case
}

/// Panics unless `check` failed with a message containing `expected`.
fn assert_fails(check: Result<(), String>, expected: &str) {
    match check {
        Ok(()) => panic!("the check passed; expected it to fail with `{expected}`"),
        Err(message) => assert!(
            message.contains(expected),
            "expected a failure containing `{expected}`, got `{message}`"
        ),
    }
}

const INTACT: &str = "download_tests::single_put::crc32::default::validated::stream";
const INTACT_PATH: &str = "download_tests::single_put::crc32::default::validated::path";
const CORRUPT: &str =
    "download_tests::single_put::crc32::default::integrity_error::corrupt::first::stream";
const CORRUPT_PATH: &str =
    "download_tests::single_put::crc32::default::integrity_error::corrupt::first::path";
const UNVALIDATED: &str =
    "download_tests::single_put::crc32::validation_off::not_validated::stream";
const UNDETECTED: &str =
    "download_tests::single_put::crc32::validation_off::undetected::corrupt::first::stream";
const PER_PART: &str = "download_tests::composite_mpu::crc32::aligned::validated::stream";
const PER_PART_TRUNCATED: &str =
    "download_tests::composite_mpu::crc32::aligned::validated::truncate::later::stream";
const HELD: &str = "download_tests::composite_mpu::crc32::out_of_order::validated::stream";

// Outcome checks.

/// `validated` fails a download that reports `NotValidated`.
#[tokio::test(flavor = "multi_thread")]
async fn validated_rejects_an_unvalidated_download() {
    let (case, run) = execute(UNVALIDATED).await;
    let case = changed(case, |c| c.expect = Expect::Validated(Crc32));
    assert_fails(
        case.check(&run),
        "expected Validated(Crc32), got NotValidated",
    );
}

/// `validated` fails a download validated with another algorithm than the one expected.
#[tokio::test(flavor = "multi_thread")]
async fn validated_rejects_another_algorithm() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.expect = Expect::Validated(Crc64Nvme));
    assert_fails(
        case.check(&run),
        "expected Validated(Crc64Nvme), got Validated",
    );
}

/// `not_validated` fails a download that reports another reason.
#[tokio::test(flavor = "multi_thread")]
async fn not_validated_rejects_another_reason() {
    let (case, run) = execute(UNVALIDATED).await;
    let case = changed(case, |c| c.expect = Expect::NotValidated(Unavailable));
    assert_fails(case.check(&run), "expected NotValidated(Unavailable)");
}

/// A successful outcome fails a download that returned an error.
#[tokio::test(flavor = "multi_thread")]
async fn success_rejects_a_failed_download() {
    let (case, run) = execute(CORRUPT).await;
    let case = changed(case, |c| c.expect = Expect::Validated(Crc32));
    assert_fails(case.check(&run), "expected the download to succeed");
}

/// `integrity_error` fails a download that succeeded.
#[tokio::test(flavor = "multi_thread")]
async fn integrity_error_rejects_a_successful_download() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.expect = Expect::IntegrityError(Crc32));
    assert_fails(
        case.check(&run),
        "expected an integrity error, but the download succeeded",
    );
}

/// `integrity_error` fails an integrity error in another algorithm than the one expected.
#[tokio::test(flavor = "multi_thread")]
async fn integrity_error_rejects_another_algorithm() {
    let (case, run) = execute(CORRUPT).await;
    let case = changed(case, |c| c.expect = Expect::IntegrityError(Sha256));
    assert_fails(
        case.check(&run),
        "integrity error in Some(Crc32), expected Sha256",
    );
}

/// The byte comparison fails a download that delivered a corrupted byte when the outcome says the
/// bytes are intact.
#[tokio::test(flavor = "multi_thread")]
async fn byte_check_rejects_a_corrupted_byte() {
    let (case, run) = execute(UNDETECTED).await;
    let case = changed(case, |c| c.expect = Expect::NotValidated(Disabled));
    assert_fails(
        case.check(&run),
        "differ from the object at [0], expected []",
    );
}

/// `undetected` fails a download whose bytes are intact: the corruption must have reached the
/// caller.
#[tokio::test(flavor = "multi_thread")]
async fn undetected_rejects_intact_bytes() {
    let (case, run) = execute(UNVALIDATED).await;
    let case = changed(case, |c| {
        c.expect = Expect::Undetected(Disabled);
        c.fault = fault::corrupt(At::First);
    });
    assert_fails(
        case.check(&run),
        "differ from the object at [], expected [0]",
    );
}

/// Enabling the object-value requirement rejects an execution that reported none.
#[tokio::test(flavor = "multi_thread")]
async fn object_values_are_required_when_the_rule_says_so() {
    let (case, run) = execute(UNVALIDATED).await;
    let case = changed(case, |c| c.download.validation = true);
    assert!(case.reports_object_values());
    assert_fails(
        case.check(&run),
        "reported Crc32 value None, expected the stored",
    );
}

/// Disabling the object-value requirement rejects an execution that reported values.
#[tokio::test(flavor = "multi_thread")]
async fn object_values_are_absent_when_the_rule_says_so() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.download.validation = false);
    assert!(!case.reports_object_values());
    assert_fails(case.check(&run), "expected none");
}

/// A failed stream download must end with the error as the body's last item.
#[tokio::test(flavor = "multi_thread")]
async fn failed_stream_must_end_in_the_error() {
    let (case, mut run) = execute(CORRUPT).await;
    run.delivered.stream_ended_in_error = false;
    assert_fails(case.check(&run), "the stream's last item must be the error");
}

/// A failed path download must leave the destination's sentinel in place.
#[tokio::test(flavor = "multi_thread")]
async fn failed_path_download_must_leave_the_destination() {
    let (case, mut run) = execute(CORRUPT_PATH).await;
    run.delivered.bytes = b"replaced".to_vec();
    assert_fails(case.check(&run), "must leave the destination unchanged");
}

/// A failed path download must remove its temporary file.
#[tokio::test(flavor = "multi_thread")]
async fn failed_path_download_must_remove_its_temporary_file() {
    let (case, mut run) = execute(CORRUPT_PATH).await;
    run.delivered.temp_file_remains = true;
    assert_fails(case.check(&run), "must remove its temporary file");
}

/// A successful path download must hold exactly the object.
#[tokio::test(flavor = "multi_thread")]
async fn successful_path_download_must_hold_the_object() {
    let (case, mut run) = execute(INTACT_PATH).await;
    run.delivered.bytes.truncate(10);
    assert_fails(case.check(&run), "delivered 10 bytes");
}

// Journal checks, run on their own.

/// Runs only the journal checks of `case` against `run`.
fn check_journal(case: &Case, run: &Execution) -> Result<(), String> {
    let requests = run.requests.as_ref().expect("a mock execution");
    case.check_journal(requests, &run.key)
}

/// A `corrupt` case fails when no GET was corrupted.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_requires_a_corrupted_get() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.fault = fault::corrupt(At::First));
    assert_fails(
        check_journal(&case, &run),
        "no GET covering byte 0 was corrupted",
    );
}

/// A `truncate` case fails when no GET was cut short.
#[tokio::test(flavor = "multi_thread")]
async fn truncate_requires_a_cut_get() {
    let (case, run) = execute(PER_PART).await;
    let case = changed(case, |c| c.fault = fault::truncate(At::Later));
    assert_fails(check_journal(&case, &run), "expected one cut GET");
}

/// A `corrupt_retry` case fails when the retry after the cut was served intact.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_retry_requires_a_corrupted_retry() {
    let (case, run) = execute(PER_PART_TRUNCATED).await;
    let case = changed(case, |c| c.fault = fault::corrupt_retry(At::Later));
    assert_fails(
        check_journal(&case, &run),
        "no GET after the cut covering byte 8388608 was corrupted",
    );
}

/// A held case fails when no GET was held.
#[tokio::test(flavor = "multi_thread")]
async fn hold_requires_a_held_get() {
    let (case, run) = execute(PER_PART).await;
    let case = changed(case, |c| c.hold = Some(Hold::FirstRange));
    assert_fails(check_journal(&case, &run), "no GET was held");
}

/// A held `validated` case fails when the held body didn't end last. The execution's body orders
/// are swapped so the held body ends first.
#[tokio::test(flavor = "multi_thread")]
async fn hold_requires_the_held_body_to_end_last() {
    let (case, mut run) = execute(HELD).await;
    check_journal(case, &run).expect("the held case passes as executed");
    let requests = run.requests.as_mut().expect("a mock execution");
    let held = requests
        .iter()
        .position(|r| matches!(r.fault, Some(s3_mock_server::FaultType::HoldBody { .. })))
        .expect("a held GET");
    let first = requests
        .iter()
        .enumerate()
        .filter_map(|(i, r)| Some((i, r.body?.order)))
        .min_by_key(|(_, order)| *order)
        .map(|(i, _)| i)
        .expect("an ended body");
    let (held_order, first_order) = (
        requests[held].body.expect("ended").order,
        requests[first].body.expect("ended").order,
    );
    requests[held].body.as_mut().unwrap().order = first_order;
    requests[first].body.as_mut().unwrap().order = held_order;
    assert_fails(
        check_journal(case, &run),
        "every other GET must end before the held body",
    );
}

/// `via: per_part` fails a download whose GET carried the object's checksum, not a part's.
#[tokio::test(flavor = "multi_thread")]
async fn per_part_requires_part_checksums() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.mechanism = Mechanism::PerPart);
    assert_fails(check_journal(&case, &run), "without a part checksum");
}

/// `via: whole_response` fails a download whose GETs carried part checksums.
#[tokio::test(flavor = "multi_thread")]
async fn whole_response_requires_the_object_checksum() {
    let (case, run) = execute(PER_PART).await;
    let case = changed(case, |c| c.mechanism = Mechanism::WholeResponse);
    assert_fails(
        check_journal(&case, &run),
        "no GET carried the object's checksum",
    );
}

/// `via: full_object` fails a download that sent no HeadObject reporting a full-object checksum.
#[tokio::test(flavor = "multi_thread")]
async fn full_object_requires_a_head_object() {
    let (case, run) = execute(INTACT).await;
    let case = changed(case, |c| c.mechanism = Mechanism::FullObject);
    assert_fails(check_journal(&case, &run), "no HeadObject reported");
}

// Case derivations.

/// Fault positions resolve to the bytes the tables describe: 0, the download part size (the
/// transfer manager's 8 MiB unless the options set one), and the object's last byte.
#[test]
fn fault_positions_resolve_to_object_bytes() {
    let auto = table_case("download_tests::full_object_mpu::crc32::default::validated::stream");
    assert_eq!(
        [
            auto.byte(At::First),
            auto.byte(At::Later),
            auto.byte(At::Tail)
        ],
        [0, 8 * MIB, 20 * MIB + 2]
    );
    let explicit =
        table_case("download_tests::full_object_mpu::crc32::odd_part_size::validated::path");
    assert_eq!(explicit.byte(At::Later), 7 * MIB);
    assert_eq!(explicit.ranges(), 3);
}
