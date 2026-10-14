/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::model::ObjectMetadata;
use std::ops::RangeInclusive;

impl ObjectMetadata {
    /// Total size of the S3 object in bytes.
    ///
    /// When a `Content-Range` header is present (e.g. `bytes 0-8388607/34359738368`),
    /// returns the total object size from the range denominator, NOT the response
    /// body length. Falls back to the `Content-Length` header when no range is present.
    pub fn total_object_size(&self) -> u64 {
        match (self.content_length, self.content_range.as_ref()) {
            (_, Some(range)) => {
                let total = range.split_once('/').map(|x| x.1).expect("content range total");
                total.parse().expect("valid range total")
            }
            (Some(length), None) => {
                debug_assert!(length >= 0, "content length invalid");
                length as u64
            },
            (None, None) => panic!("total object size cannot be calculated without either content length or content range headers")
        }
    }

    /// Parse the content-range header to the inclusive range.
    pub(crate) fn range_from_content_range(&self) -> Option<RangeInclusive<u64>> {
        match self.content_range.as_deref() {
            Some(range) => crate::http::header::parse_content_range(range),
            // Without Content-Range, Content-Length describes the complete object.
            None => self
                .content_length
                .and_then(|length| u64::try_from(length).ok())
                .and_then(|length| length.checked_sub(1))
                .map(|end| 0..=end),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectMetadata;

    #[test]
    fn test_inferred_total_size() {
        let meta = ObjectMetadata::builder().content_length(15).build();
        assert_eq!(15, meta.total_object_size());
        let meta = ObjectMetadata::builder()
            .content_range("bytes 0-499/900")
            .content_length(500)
            .build();
        assert_eq!(900, meta.total_object_size());
    }

    #[test]
    fn test_parse_content_range() {
        let cases = vec![
            (Some("bytes 0-499/1234".to_string()), Some(0..=499)),
            (Some("bytes 500-999/1234".to_string()), Some(500..=999)),
            (Some("bytes 0-0/1234".to_string()), Some(0..=0)),
            (None, Some(0..=1233)),
        ];
        for (input, expected) in cases {
            let meta = ObjectMetadata::builder()
                .set_content_range(input)
                .content_length(1234)
                .build();
            assert_eq!(meta.range_from_content_range(), expected);
        }
        let meta = ObjectMetadata::builder().content_length(0).build();
        assert_eq!(meta.range_from_content_range(), None);
    }
}
