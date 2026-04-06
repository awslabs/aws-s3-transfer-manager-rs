/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use core::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;

use crate::error;

/// Parse a Content-Range response header into an inclusive byte range.
///
/// Expects format `bytes START-END/TOTAL` (e.g. `bytes 200-500/1000`).
/// Returns `None` if the header is missing, malformed, or not a bytes range.
pub(crate) fn parse_content_range(header: &str) -> Option<RangeInclusive<u64>> {
    let byte_range_str = header.strip_prefix("bytes ")?.split_once('/')?.0;
    let (start_str, end_str) = byte_range_str.split_once('-')?;
    let start = start_str.parse::<u64>().ok()?;
    let end = end_str.parse::<u64>().ok()?;
    Some(start..=end)
}

/// Representation of `Range` header.
/// NOTE: S3 only supports a single bytes range this is a simplified representation
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Range(pub(crate) ByteRange);

impl Range {
    /// Create a range from the given byte range
    pub(crate) fn bytes(rng: ByteRange) -> Self {
        Self(rng)
    }

    /// Create a range from the inclusive start and end offsets
    pub(crate) fn bytes_inclusive(start: u64, end: u64) -> Self {
        Range::bytes(ByteRange::Inclusive(start, end))
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes={}", self.0)
    }
}

impl From<Range> for String {
    fn from(value: Range) -> Self {
        format!("{}", value)
    }
}

impl FromStr for Range {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, '=');
        match (iter.next(), iter.next()) {
            (Some("bytes"), Some(range)) => {
                if range.contains(',') {
                    Err(error::invalid_input(format!(
                        "multiple byte ranges not supported for range header {}",
                        s
                    )))
                } else {
                    let spec = ByteRange::from_str(range).map_err(|_| {
                        error::invalid_input(format!("invalid range header {}", s))
                    })?;
                    Ok(Range(spec))
                }
            }
            _ => Err(error::invalid_input(format!(
                "unsupported byte range header format `{s}`; see https://www.rfc-editor.org/rfc/rfc9110.html#name-range for valid formats"
            ))),
        }
    }
}

/// Representation of a single [RFC-99110 byte range](https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges)
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ByteRange {
    /// Get all bytes between x and y inclusive ("bytes=x-y")
    Inclusive(u64, u64),

    /// Get all bytes starting from x ("bytes=x-")
    AllFrom(u64),

    /// Get the last n bytes ("bytes=-n")
    Last(u64),
}

impl fmt::Display for ByteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ByteRange::Inclusive(start, end) => write!(f, "{}-{}", start, end),
            ByteRange::AllFrom(from) => write!(f, "{}-", from),
            ByteRange::Last(n) => write!(f, "-{}", n),
        }
    }
}

impl FromStr for ByteRange {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, '-');
        match (iter.next(), iter.next()) {
            (Some(""), Some(end)) => end.parse().map(ByteRange::Last).or(Err(())),
            (Some(start), Some("")) => start.parse().map(ByteRange::AllFrom).or(Err(())),
            (Some(start), Some(end)) => match (start.parse(), end.parse()) {
                (Ok(start), Ok(end)) if start <= end => Ok(ByteRange::Inclusive(start, end)),
                _ => Err(()),
            },
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, Range};
    use crate::error;
    use aws_smithy_types::error::display::DisplayErrorContext;
    use std::str::FromStr;

    #[test]
    fn test_byte_range_from_str() {
        assert_eq!(
            ByteRange::Last(500),
            Range::from_str("bytes=-500").unwrap().0
        );
        assert_eq!(
            ByteRange::AllFrom(200),
            Range::from_str("bytes=200-").unwrap().0
        );
        assert_eq!(
            ByteRange::Inclusive(200, 500),
            Range::from_str("bytes=200-500").unwrap().0
        );
    }

    fn assert_err_contains(r: Result<Range, error::Error>, msg: &str) {
        let err = r.unwrap_err();
        match err.kind() {
            error::ErrorKind::InputInvalid => {
                let m = DisplayErrorContext(err).to_string();
                assert!(m.contains(msg), "'{}' does not contain '{}'", m, msg);
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_parse_content_range() {
        use super::parse_content_range;
        assert_eq!(parse_content_range("bytes 0-499/1000"), Some(0..=499));
        assert_eq!(
            parse_content_range("bytes 10000000-14999999/100000000"),
            Some(10000000..=14999999)
        );
        assert_eq!(parse_content_range("bytes 0-0/1"), Some(0..=0));
        assert_eq!(parse_content_range("invalid"), None);
        assert_eq!(parse_content_range("bytes abc-def/100"), None);
        assert_eq!(parse_content_range("bytes 0-499"), None);
    }

    #[test]
    fn test_invalid_byte_range_from_str() {
        assert_err_contains(Range::from_str("bytes=-"), "invalid range header");
        assert_err_contains(Range::from_str("bytes=500-200"), "invalid range header");
        assert_err_contains(
            Range::from_str("bytes=0-200,400-500"),
            "multiple byte ranges not supported for range header",
        );
    }
}
