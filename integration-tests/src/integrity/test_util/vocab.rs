/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`Case`], one integrity test case, and the words `download_cases!` and `algorithm_cases!`
//! are written in.
//!
//! A case describes an object ([`Object`]), how it is downloaded ([`Download`]), what the mock
//! does to the responses ([`Fault`], [`Hold`]), where the bytes go ([`Sink`]), the outcome the
//! caller must get ([`Expect`]), and where it runs ([`CaseTarget`]). [`Mechanism`] names how the
//! object is validated, and the mock-only checks confirm it.
//!
//! The words are constants and `const fn`s in the lowercase modules (`alg`, `mechanism`, `hold`,
//! `fault`, `at`, `sink`, `expect`), named as the tables spell them. The macros resolve each word
//! by path, such as `sink::$sink`, so a misspelled word fails to compile with the word in the
//! error. Every value here is `const`, so a case can be a `static`.

#![allow(non_upper_case_globals)]

pub(crate) use crate::test_util::object::{Alg, Layout, Object, MIB};
/// Reason words, for `not_validated(<reason>)` and `undetected(<reason>)`: the transfer manager's
/// own `NotValidatedReason` variants.
pub(crate) use aws_s3_transfer::types::NotValidatedReason;
pub(crate) use Alg::*;
pub(crate) use Layout::Composite;
pub(crate) use NotValidatedReason::{CompositeChecksum, Disabled, PartialCoverage, Unavailable};

/// `Layout::FullObject`, under a name that doesn't clash with `Mechanism::FullObject` in a glob
/// import.
pub(crate) const FullObject: Layout = Layout::FullObject;

/// Algorithm words, for `algorithms:` and the `[<algorithms>]` of an algorithm line. Each is the
/// upload checksum algorithm, and the algorithm a `validated` or `integrity_error` case expects
/// unless it names another.
pub(crate) mod alg {
    use super::Alg;
    /// CRC32.
    pub(crate) const crc32: Alg = Alg::Crc32;
    /// CRC32C.
    pub(crate) const crc32c: Alg = Alg::Crc32c;
    /// CRC64NVME, the transfer manager's default.
    pub(crate) const crc64nvme: Alg = Alg::Crc64Nvme;
    /// SHA-1.
    pub(crate) const sha1: Alg = Alg::Sha1;
    /// SHA-256.
    pub(crate) const sha256: Alg = Alg::Sha256;
    /// No upload checksum: the client sends none, or the object is stored without one.
    pub(crate) const none: Alg = Alg::NoChecksum;
}

/// How a case downloads its object: the download client's settings and the request's range.
///
/// Built from [`Download::new`], the transfer manager's defaults, with one method per change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Download {
    /// The download part size. `None`: the transfer manager's automatic part size.
    pub(crate) part_size: Option<u64>,
    /// A caller range, inclusive. `None`: the whole object.
    pub(crate) range: Option<(u64, u64)>,
    /// Whether the client validates responses (`ResponseChecksumValidation::WhenSupported`).
    pub(crate) validation: bool,
    /// At most this many requests in flight (`ConcurrencyMode::Explicit`).
    pub(crate) concurrency: Option<usize>,
}

impl Download {
    /// The transfer manager's defaults: automatic part size, the whole object, validation on.
    pub(crate) const fn new() -> Self {
        Download {
            part_size: None,
            range: None,
            validation: true,
            concurrency: None,
        }
    }

    /// Downloads in ranges of `part_size` bytes (`PartSize::Target`), which also disables the
    /// realignment of ranges to the object's stored parts.
    pub(crate) const fn part_size(mut self, part_size: u64) -> Self {
        self.part_size = Some(part_size);
        self
    }

    /// Requests only the object bytes in `range`, as a caller's `Range: bytes=<start>-<end>`.
    pub(crate) const fn range(mut self, range: core::ops::RangeInclusive<u64>) -> Self {
        self.range = Some((*range.start(), *range.end()));
        self
    }

    /// Downloads with a client whose `ResponseChecksumValidation` is `WhenRequired` and without a
    /// checksum mode, so no response is validated.
    pub(crate) const fn validation_off(mut self) -> Self {
        self.validation = false;
        self
    }

    /// Downloads with at most `n` requests in flight.
    pub(crate) const fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }
}

/// A delivery order only the mock can impose. A case with a hold runs on mock targets only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hold {
    /// The GET covering byte 0 sends its headers, then holds its body until the bodies of the
    /// object's other [`Case::ranges`] − 1 GETs have ended, so the first range completes last.
    /// The hold is released after a deadline regardless, and the case then fails its hold check.
    FirstRange,
}

/// Hold words, for a setup's `hold:`.
pub(crate) mod hold {
    use super::Hold;
    /// [`Hold::FirstRange`].
    pub(crate) const first_range: Option<Hold> = Some(Hold::FirstRange);
    /// No hold: the value when a setup has no `hold:`.
    pub(crate) const no_hold: Option<Hold> = None;
}

/// How the object's bytes are checked against a stored checksum, given its layout and the
/// download's ranges: the `via` of a group or algorithm line.
///
/// The mechanism is part of what a case pins, not a description. On mock targets, a
/// `validated` or `integrity_error` case checks the request journal for the mechanism's evidence
/// (see `Case::check_journal`), and an `integrity_error` case requires the error to name the
/// checksum the mechanism checks. A download that reaches its outcome by another mechanism fails
/// the case, and the table's `via` must change with it; `coverage_tests` pins each group's and
/// shape's mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mechanism {
    /// The whole object comes back in one GET, which carries the object's checksum.
    WholeResponse,
    /// Each range lines up with a stored part, and carries its checksum.
    PerPart,
    /// The ranges are combined and compared with the object's full-object CRC.
    FullObject,
    /// No stored checksum the SDK can use covers the ranges.
    None,
}

/// Mechanism words, for `via`.
pub(crate) mod mechanism {
    use super::Mechanism;
    /// [`Mechanism::WholeResponse`].
    pub(crate) const whole_response: Mechanism = Mechanism::WholeResponse;
    /// [`Mechanism::PerPart`].
    pub(crate) const per_part: Mechanism = Mechanism::PerPart;
    /// [`Mechanism::FullObject`].
    pub(crate) const full_object: Mechanism = Mechanism::FullObject;
    /// [`Mechanism::None`].
    pub(crate) const none: Mechanism = Mechanism::None;
}

/// What the mock does to the GET whose resolved range covers a fault's byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Flips the byte (XOR `0xFF`) in every GET that covers it, leaving the checksum headers
    /// unchanged.
    Corrupt,
    /// Ends the first GET that covers the byte with a connection error after
    /// `TRUNCATE_AFTER` bytes. The transfer manager is expected to retry the range.
    Truncate,
    /// Truncates the first GET that covers the byte, as [`Action::Truncate`] does, then flips the
    /// byte in the next GET that covers it: the retry. The case shows that a retried range is
    /// checked against the bytes of the retry's response.
    CorruptRetry,
}

/// The object byte a fault is positioned at; [`Case::byte`] resolves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum At {
    /// Byte 0: the first byte of the first range.
    First,
    /// Byte `B`, the download part size ([`Case::range_size`]): the first byte of the second
    /// range, which the request planner fetches.
    Later,
    /// Byte `L - 1`, where `L` is the object's length: the last byte of the final range.
    Tail,
}

/// A fault: an action at a position, written `<action>(<position>)` in the tables.
///
/// The mock applies one fault or hold per response, so a case with a hold doesn't fault byte 0,
/// which the held GET covers; `table_cases_are_well_formed` checks this of the tables.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fault {
    pub(crate) action: Action,
    pub(crate) at: At,
}

/// Prints as the tables spell it: `corrupt_retry(later)`.
impl core::fmt::Debug for Fault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let action = match self.action {
            Action::Corrupt => "corrupt",
            Action::Truncate => "truncate",
            Action::CorruptRetry => "corrupt_retry",
        };
        let at = match self.at {
            At::First => "first",
            At::Later => "later",
            At::Tail => "tail",
        };
        write!(f, "{action}({at})")
    }
}

/// Fault action words, applied to a position word: `corrupt(later)`.
pub(crate) mod fault {
    use super::{Action, At, Fault};
    /// [`Action::Corrupt`] at `at`.
    pub(crate) const fn corrupt(at: At) -> Option<Fault> {
        Some(Fault {
            action: Action::Corrupt,
            at,
        })
    }
    /// [`Action::Truncate`] at `at`.
    pub(crate) const fn truncate(at: At) -> Option<Fault> {
        Some(Fault {
            action: Action::Truncate,
            at,
        })
    }
    /// [`Action::CorruptRetry`] at `at`.
    pub(crate) const fn corrupt_retry(at: At) -> Option<Fault> {
        Some(Fault {
            action: Action::CorruptRetry,
            at,
        })
    }
}

/// Fault position words.
pub(crate) mod at {
    use super::At;
    /// [`At::First`].
    pub(crate) const first: At = At::First;
    /// [`At::Later`].
    pub(crate) const later: At = At::Later;
    /// [`At::Tail`].
    pub(crate) const tail: At = At::Tail;
}

/// Where the downloaded bytes go. The sinks differ in what they leave behind on failure: the
/// `integrity_error` checks assert the stream's last item and the path destination's state. A
/// caller-owned file may hold anything after a failure, so a `file` case checks nothing there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sink {
    /// `DownloadHandle::body_mut().next()` until it ends, then `join`.
    Stream,
    /// `write_to_path` to a destination that already holds a sentinel file.
    Path,
    /// `write_to_file` into a temporary file the test opened.
    File,
}

/// Sink words.
pub(crate) mod sink {
    use super::Sink;
    /// [`Sink::Stream`].
    pub(crate) const stream: Sink = Sink::Stream;
    /// [`Sink::Path`].
    pub(crate) const path: Sink = Sink::Path;
    /// [`Sink::File`].
    pub(crate) const file: Sink = Sink::File;
}

/// The outcome a case's download must have, on every target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expect {
    /// The download succeeds, delivers exactly the requested bytes, and reports `Validated` with
    /// the algorithm.
    Validated(Alg),
    /// The download succeeds, delivers exactly the requested bytes, and reports `NotValidated`
    /// with the reason.
    NotValidated(NotValidatedReason),
    /// The download fails with an `IntegrityError` in the algorithm that carries different
    /// expected and computed values.
    IntegrityError(Alg),
    /// The download succeeds and reports `NotValidated` with the reason, and the delivered bytes
    /// differ from the object at the faulted byte and nowhere else: corruption the transfer
    /// manager cannot detect for this object and download.
    Undetected(NotValidatedReason),
}

/// Outcome words, for `expect <outcome>` and an algorithm line's `intact:` and `corrupt:`.
/// `validated` and `integrity_error` written without an argument take the object's algorithm.
pub(crate) mod expect {
    use super::{Alg, Expect, NotValidatedReason};
    /// [`Expect::Validated`].
    pub(crate) const fn validated(a: Alg) -> Expect {
        Expect::Validated(a)
    }
    /// [`Expect::IntegrityError`].
    pub(crate) const fn integrity_error(a: Alg) -> Expect {
        Expect::IntegrityError(a)
    }
    /// [`Expect::NotValidated`].
    pub(crate) const fn not_validated(r: NotValidatedReason) -> Expect {
        Expect::NotValidated(r)
    }
    /// [`Expect::Undetected`].
    pub(crate) const fn undetected(r: NotValidatedReason) -> Expect {
        Expect::Undetected(r)
    }
}

/// Where a case runs. The target words in `targets:` (`mock_gp`, `mock_express`, `real_gp`,
/// `real_express`) are the names of the generated test functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaseTarget {
    /// The mock, with a general purpose bucket.
    MockGp,
    /// The mock, with a directory bucket.
    MockExpress,
    /// Real S3, with a general purpose bucket. Generated only under `--cfg e2e_test`.
    RealGp,
    /// Real S3, with a directory bucket. Generated only under `--cfg e2e_test`.
    RealExpress,
}

impl CaseTarget {
    /// Whether the target is the mock, which can apply faults and holds and keeps a journal.
    pub(crate) fn is_mock(self) -> bool {
        matches!(self, CaseTarget::MockGp | CaseTarget::MockExpress)
    }

    /// The harness target. Panics for a real target unless built with `--cfg e2e_test`.
    pub(crate) fn harness(self) -> crate::harness::Target {
        use crate::harness::Target as H;
        match self {
            CaseTarget::MockGp => H::mock_gp(),
            CaseTarget::MockExpress => H::mock_express(),
            #[cfg(e2e_test)]
            CaseTarget::RealGp => H::real_gp(),
            #[cfg(e2e_test)]
            CaseTarget::RealExpress => H::real_express(),
            #[cfg(not(e2e_test))]
            CaseTarget::RealGp | CaseTarget::RealExpress => {
                unreachable!("real targets are generated only under e2e_test")
            }
        }
    }
}

/// The cases a `cases()` function collects.
pub(crate) type Cases = Vec<&'static Case>;

/// The download part size the transfer manager uses when none is configured
/// (`runtime::sizing::DEFAULT_PART_SIZE`, which the crate does not export).
const DEFAULT_PART_SIZE: u64 = 8 * MIB;

/// One integrity test case: an object, how it is downloaded, what the mock does to the responses,
/// where the bytes go, the outcome the download must have, and where the case runs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Case {
    /// The case's module path, such as
    /// `integration_tests::integrity::download_tests::single_put::crc32::default::validated::path`.
    /// It is the test's name, and the object's key and seed derive from it, so no two cases
    /// share an object.
    pub(crate) path: &'static str,
    /// How the object is validated: the `via` word.
    pub(crate) mechanism: Mechanism,
    /// The object uploaded before the download.
    pub(crate) object: Object,
    /// How the object is downloaded.
    pub(crate) download: Download,
    /// The mock's delivery order, if any.
    pub(crate) hold: Option<Hold>,
    /// The mock's fault, if any.
    pub(crate) fault: Option<Fault>,
    /// Where the downloaded bytes go.
    pub(crate) sink: Sink,
    /// The outcome the download must have.
    pub(crate) expect: Expect,
    /// The reason every test function of the case is `#[ignore]`d, if it is.
    pub(crate) ignore: Option<&'static str>,
    /// Whether the case is in the `extended` platform tier, whose E2E tests run on Linux only.
    pub(crate) extended: bool,
    /// The targets the table lists. Faulted and held cases run only on the mock ones.
    pub(crate) targets: &'static [CaseTarget],
}

impl Case {
    /// The targets a test function is generated for: a case with a fault or a hold runs only on
    /// mock targets. The macros generate test functions by the same rule.
    pub(crate) fn emitted_targets(&self) -> impl Iterator<Item = CaseTarget> + '_ {
        let mock_only = self.fault.is_some() || self.hold.is_some();
        self.targets
            .iter()
            .copied()
            .filter(move |t| !mock_only || t.is_mock())
    }

    /// The download part size `B`: the size in the options, or [`DEFAULT_PART_SIZE`].
    ///
    /// Automatic COMPOSITE sizing uses the first stored part's length. If that differs from
    /// [`DEFAULT_PART_SIZE`], [`At::Later`] does not name the second range's first byte.
    /// Full-object CRC downloads retain `B` regardless of their stored part sizes.
    /// `table_cases_are_well_formed` checks the tables' fault-position assumptions.
    pub(crate) fn range_size(&self) -> u64 {
        self.download.part_size.unwrap_or(DEFAULT_PART_SIZE)
    }

    /// The object byte a fault position names. Panics for [`At::Tail`] of an empty object.
    pub(crate) fn byte(&self, at: At) -> u64 {
        match at {
            At::First => 0,
            At::Later => self.range_size(),
            At::Tail => self.object.len - 1,
        }
    }

    /// The bytes the download is asked for, end exclusive: the caller range or the object.
    pub(crate) fn requested(&self) -> std::ops::Range<u64> {
        match self.download.range {
            Some((first, last)) => first..last + 1,
            None => 0..self.object.len,
        }
    }

    /// Whether the download must report the object's stored checksum values and type
    /// (`integrity_checks()`). When false, it must report none.
    ///
    /// Checksum-mode HEAD reports stored object values on every validated whole-object
    /// download. Caller ranges, validation-off downloads and objects without checksums
    /// must report none.
    pub(crate) const fn reports_object_values(&self) -> bool {
        self.download.validation
            && self.download.range.is_none()
            && !(self.object.seeded && matches!(self.object.alg, Alg::NoChecksum))
    }
}
