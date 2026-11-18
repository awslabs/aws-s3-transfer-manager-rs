/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The checks a case's outcome makes: on every target, the delivered bytes, the verdict or error,
//! the destination and the reported object checksum; on mock targets, what the server observed.

use aws_s3_transfer::operation::download::DownloadOutput;
use aws_s3_transfer::types::ChecksumValidation;
use s3_mock_server::{BodyEnding, ChecksumScope, FaultType, HoldEnd, Operation, RequestRecord};

use super::runner::{Check, Delivered, SENTINEL};
use super::vocab::{Action, Alg, At, Case, Expect, Mechanism, NotValidatedReason, Sink};
use crate::assertions::check_integrity_error;
use crate::test_util::data::fill_object_bytes;
use crate::test_util::object::ChecksumValues;
use aws_s3_transfer::error::ErrorKind;

/// Returns `Err(message)` from the enclosing check when `condition` is false.
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

impl Case {
    /// Checks the download's result, delivered bytes, destination and reported object checksum
    /// values against `self.expect`, as each [`Expect`] variant states.
    pub(crate) fn check_outcome(&self, delivered: &Delivered) -> Check {
        match self.expect {
            Expect::Validated(alg) => {
                let output = self.succeeded(delivered)?;
                self.check_bytes(&delivered.bytes, None)?;
                check_validated(output, alg)?;
                self.check_object_values(output)
            }
            Expect::NotValidated(reason) => {
                let output = self.succeeded(delivered)?;
                self.check_bytes(&delivered.bytes, None)?;
                check_not_validated(output, reason)?;
                self.check_object_values(output)
            }
            Expect::Undetected(reason) => {
                let output = self.succeeded(delivered)?;
                let fault = self.fault.ok_or("an undetected case needs a fault")?;
                self.check_bytes(&delivered.bytes, Some(self.byte(fault.at)))?;
                check_not_validated(output, reason)
            }
            Expect::IntegrityError(alg) => self.check_integrity_error(delivered, alg),
        }
    }

    fn succeeded<'a>(&self, delivered: &'a Delivered) -> Result<&'a DownloadOutput, String> {
        delivered
            .result
            .as_ref()
            .map_err(|e| format!("expected the download to succeed, got {e:?}"))
    }

    /// The delivered bytes are the requested bytes, except that the object byte `flipped`, if
    /// given, differs, and nothing else does.
    fn check_bytes(&self, delivered: &[u8], flipped: Option<u64>) -> Check {
        let requested = self.requested();
        ensure!(
            delivered.len() as u64 == requested.end - requested.start,
            "delivered {} bytes, expected {} (bytes {requested:?})",
            delivered.len(),
            requested.end - requested.start
        );
        let mut differ = Vec::new();
        let mut expected = vec![0; 1 << 20];
        for (i, piece) in delivered.chunks(expected.len()).enumerate() {
            let offset = requested.start + (i * expected.len()) as u64;
            let expected = &mut expected[..piece.len()];
            fill_object_bytes(self.seed(), offset, expected);
            if piece != expected {
                differ.extend(
                    piece
                        .iter()
                        .zip(expected.iter())
                        .enumerate()
                        .filter(|(_, (a, b))| a != b)
                        .map(|(j, _)| offset + j as u64)
                        .take(8),
                );
            }
            if differ.len() > 8 {
                break;
            }
        }
        let expected_differ: Vec<u64> = flipped.into_iter().collect();
        ensure!(
            differ == expected_differ,
            "delivered bytes differ from the object at {differ:?}, expected {expected_differ:?}"
        );
        Ok(())
    }

    /// The download reports the object's stored checksum values and type when
    /// [`reports_object_values`](Case::reports_object_values) says so, and none otherwise. For a
    /// composite checksum it reports only the type: the value is a checksum of part checksums,
    /// which the delivered bytes don't reproduce.
    fn check_object_values(&self, output: &DownloadOutput) -> Check {
        let checks = output.integrity_checks();
        let reported = Alg::WITH_CHECKSUM.map(|alg| (alg, checks.checksum(alg)));
        let reported_type = checks.checksum_type().map(|t| t.as_str().to_owned());
        if self.reports_object_values() {
            let stored = self
                .object
                .expected_checksum(self.seed())
                .ok_or("an object reporting checksum values must store one")?;
            let composite = matches!(
                stored.checksum_type,
                Some(aws_sdk_s3::types::ChecksumType::Composite)
            );
            for (alg, value) in reported {
                let expected =
                    (alg == stored.algorithm && !composite).then_some(stored.value.as_str());
                ensure!(
                    value == expected,
                    "reported {alg:?} value {value:?}, expected the stored {expected:?}"
                );
            }
            ensure!(
                reported_type.as_deref() == stored.checksum_type.as_ref().map(|t| t.as_str()),
                "reported checksum type {reported_type:?}, expected {:?}",
                stored.checksum_type
            );
        } else {
            let values: Vec<_> = reported.iter().filter(|(_, v)| v.is_some()).collect();
            ensure!(
                values.is_empty() && reported_type.is_none(),
                "reported object checksum values {values:?} and type {reported_type:?}, expected none"
            );
        }
        Ok(())
    }

    fn check_integrity_error(&self, delivered: &Delivered, alg: Alg) -> Check {
        let error = match &delivered.result {
            Ok(output) => {
                return Err(format!(
                    "expected an integrity error, but the download succeeded: {:?}",
                    output.integrity_checks()
                ))
            }
            Err(e) => e,
        };
        let algorithm = alg.tm().ok_or("an integrity error needs an algorithm")?;
        check_integrity_error(error, &algorithm)?;
        // The mismatch is the injected one: the checksum that failed is the stored checksum of
        // the response containing the faulted byte, as the mechanism checks it.
        if let (Some(fault), ErrorKind::IntegrityError(integrity)) = (self.fault, error.kind()) {
            let byte = self.byte(fault.at);
            let stored = match self.mechanism {
                Mechanism::PerPart => self.object.part_checksum(self.seed(), byte),
                Mechanism::WholeResponse | Mechanism::FullObject => {
                    self.object.expected_checksum(self.seed()).map(|c| c.value)
                }
                Mechanism::None => None,
            };
            ensure!(
                integrity.expected() == stored.as_deref(),
                "integrity error expected {:?}, not the stored {stored:?} the {:?} mechanism \
                 checks for byte {byte}",
                integrity.expected(),
                self.mechanism
            );
        }
        match self.sink {
            Sink::Stream => ensure!(
                delivered.stream_ended_in_error,
                "the stream's last item must be the error"
            ),
            Sink::Path => {
                ensure!(
                    delivered.bytes == SENTINEL,
                    "a failed path download must leave the destination unchanged"
                );
                ensure!(
                    !delivered.temp_file_remains,
                    "a failed path download must remove its temporary file"
                );
            }
            Sink::File => {}
        }
        Ok(())
    }

    /// Checks the mock's records of the requests for `key` for evidence of what the case names:
    /// the fault was applied to a GET covering its byte, and followed as its action says; the
    /// held body ended after every other GET's, or the corrupted GET was sent while it was held;
    /// and for `validated` and `integrity_error` cases, the responses carried the checksum the
    /// mechanism needs. For `full_object` that is a HeadObject reporting the full-object CRC.
    /// `not_validated` and `undetected` cases, and `via: none`, get no mechanism check.
    pub(crate) fn check_journal(&self, records: &[RequestRecord], key: &str) -> Check {
        let records: Vec<_> = records
            .iter()
            .filter(|r| r.key.as_deref() == Some(key))
            .collect();
        let gets: Vec<_> = records
            .iter()
            .copied()
            .filter(|r| r.operation == Operation::GetObject)
            .collect();
        let covers =
            |r: &RequestRecord, byte: u64| r.range.as_ref().is_some_and(|x| x.contains(&byte));
        let sent = |r: &RequestRecord| r.body.is_some_and(|b| b.ending == BodyEnding::Sent);
        let order = |r: &RequestRecord| r.body.map(|b| b.order);
        let is_corrupt = |r: &RequestRecord| matches!(r.fault, Some(FaultType::CorruptByte { .. }));
        let is_cut = |r: &RequestRecord| matches!(r.fault, Some(FaultType::TruncateBody { .. }));

        if let Some(fault) = self.fault {
            let byte = self.byte(fault.at);
            // The GET the fault was applied to holds the range the position names: the first
            // range, the range starting at `B`, or the final range.
            let at_position = |r: &RequestRecord| -> Check {
                let range = r
                    .range
                    .as_ref()
                    .ok_or_else(|| format!("a faulted GET has no range: {r:#?}"))?;
                let named = match fault.at {
                    At::First => range.start == 0,
                    At::Later => range.start == self.range_size(),
                    At::Tail => range.end == self.object.len,
                };
                ensure!(
                    named,
                    "the GET faulted for {:?} has range {range:?}: {r:#?}",
                    fault.at
                );
                Ok(())
            };
            let corrupted_at_byte = |r: &RequestRecord| matches!(r.fault, Some(FaultType::CorruptByte { offset }) if offset == byte);
            ensure!(
                gets.iter().all(|r| r.flipped.is_none_or(|f| f == byte)),
                "a GET's body had another byte than {byte} flipped: {gets:#?}"
            );
            let cut = match fault.action {
                Action::Corrupt => None,
                Action::Truncate | Action::CorruptRetry => {
                    let cuts: Vec<_> = (0..gets.len()).filter(|&i| is_cut(gets[i])).collect();
                    ensure!(cuts.len() == 1, "expected one cut GET: {gets:#?}");
                    let cut = cuts[0];
                    ensure!(
                        covers(gets[cut], byte),
                        "the cut GET doesn't contain byte {byte}: {:#?}",
                        gets[cut]
                    );
                    at_position(gets[cut])?;
                    ensure!(
                        gets[cut].body.map(|b| b.ending) == Some(BodyEnding::CutByFault),
                        "the cut GET's body must end cut short: {:#?}",
                        gets[cut]
                    );
                    Some(cut)
                }
            };
            match (fault.action, cut) {
                (Action::Corrupt, _) => {
                    let corrupted: Vec<_> = gets.iter().filter(|r| corrupted_at_byte(r)).collect();
                    ensure!(
                        !corrupted.is_empty(),
                        "no GET was corrupted at byte {byte}: {gets:#?}"
                    );
                    for r in &corrupted {
                        at_position(r)?;
                    }
                    ensure!(
                        corrupted.iter().any(|r| r.flipped == Some(byte)),
                        "no corrupted GET's body had byte {byte} flipped: {corrupted:#?}"
                    );
                }
                (Action::Truncate, Some(cut)) => ensure!(
                    gets[cut + 1..]
                        .iter()
                        .any(|r| r.range == gets[cut].range && sent(r)),
                    "no GET of the cut GET's range was sent in full after it: {gets:#?}"
                ),
                (Action::CorruptRetry, Some(cut)) => {
                    let retry = gets[cut + 1..]
                        .iter()
                        .find(|r| r.range == gets[cut].range && corrupted_at_byte(r))
                        .ok_or_else(|| {
                            format!("the cut GET's range wasn't corrupted after it: {gets:#?}")
                        })?;
                    ensure!(
                        retry.flipped == Some(byte),
                        "the retry's body didn't have byte {byte} flipped: {retry:#?}"
                    );
                }
                _ => unreachable!("a cut is found for truncate and corrupt_retry"),
            }
        }

        if self.hold.is_some() {
            let held = gets
                .iter()
                .find(|r| matches!(r.fault, Some(FaultType::HoldBody { .. })));
            if let Some(held) = held {
                ensure!(
                    held.hold != Some(HoldEnd::Expired),
                    "the hold expired before the other ranges ended: {held:#?}"
                );
            }
            match self.expect {
                Expect::Validated(_) => {
                    let held = held.ok_or_else(|| format!("no GET was held: {gets:#?}"))?;
                    ensure!(sent(held), "the held body must be sent in full: {held:#?}");
                    let others_ended_first = gets
                        .iter()
                        .filter(|r| !std::ptr::eq(**r, *held))
                        .all(|r| order(r).is_some() && order(r) < order(held));
                    ensure!(
                        others_ended_first,
                        "every other GET must end before the held body: {gets:#?}"
                    );
                }
                Expect::IntegrityError(_) => {
                    // The download fails on the corrupted range and stops its other GETs. It can
                    // fail before the GET covering byte 0 reaches the mock, so that GET isn't
                    // required; if it was sent, its body must not end before the corrupted GET's.
                    let corrupted = gets
                        .iter()
                        .find(|r| is_corrupt(r) && sent(r))
                        .ok_or_else(|| format!("no corrupted GET was sent in full: {gets:#?}"))?;
                    ensure!(
                        held.and_then(|r| order(r))
                            .is_none_or(|held| Some(held) > order(corrupted)),
                        "the corrupted GET must be sent while the held body is held: {gets:#?}"
                    );
                }
                _ => {}
            }
        }

        let algorithm = match self.expect {
            Expect::Validated(alg) | Expect::IntegrityError(alg) => alg.sdk(),
            _ => None,
        };
        let full_object_head = || {
            records.iter().any(|r| {
                r.operation == Operation::HeadObject
                    && r.checksum.as_ref().is_some_and(|c| {
                        c.scope == ChecksumScope::Object
                            && c.checksum_type == Some(aws_sdk_s3::types::ChecksumType::FullObject)
                            && Some(&c.algorithm) == algorithm.as_ref()
                    })
            })
        };
        let scope = |r: &RequestRecord| r.checksum.as_ref().map(|c| c.scope);
        match (self.expect, self.mechanism) {
            (Expect::Validated(_), Mechanism::WholeResponse) => ensure!(
                gets.iter()
                    .any(|r| sent(r) && scope(r) == Some(ChecksumScope::Object)),
                "no GET carried the object's checksum: {gets:#?}"
            ),
            (Expect::Validated(_), Mechanism::PerPart) => {
                let without = gets
                    .iter()
                    .copied()
                    .filter(|r| sent(r) && scope(r) != Some(ChecksumScope::Part))
                    .collect::<Vec<_>>();
                ensure!(
                    without.is_empty(),
                    "GETs sent in full without a part checksum: {without:#?}"
                );
            }
            (Expect::IntegrityError(_), Mechanism::WholeResponse | Mechanism::PerPart) => {
                let byte = self.fault.map(|f| self.byte(f.at)).unwrap_or_default();
                ensure!(
                    gets.iter().any(|r| covers(r, byte) && scope(r).is_some()),
                    "no GET covering byte {byte} carried the object's or a part's checksum: \
                     {gets:#?}"
                );
            }
            (Expect::Validated(_) | Expect::IntegrityError(_), Mechanism::FullObject) => ensure!(
                full_object_head(),
                "no HeadObject reported a full-object {algorithm:?} checksum: {records:#?}"
            ),
            _ => {}
        }
        Ok(())
    }
}

fn check_validated(output: &DownloadOutput, alg: Alg) -> Check {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::Validated { algorithm, .. } if Some(algorithm) == alg.tm().as_ref() => {
            Ok(())
        }
        other => Err(format!("expected Validated({alg:?}), got {other:?}")),
    }
}

fn check_not_validated(output: &DownloadOutput, reason: NotValidatedReason) -> Check {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::NotValidated { reason: got, .. } if *got == reason => Ok(()),
        other => Err(format!("expected NotValidated({reason:?}), got {other:?}")),
    }
}
