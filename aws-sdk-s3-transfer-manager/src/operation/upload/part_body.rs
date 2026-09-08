/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Retryable HTTP bodies for immutable multipart payloads.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body_1x::{Body, Frame, SizeHint};

use crate::memory::SegmentedBytes;

/// Builds the SDK body for one immutable upload part.
///
/// Contiguous values retain Smithy's native in-memory body, including checksum
/// header calculation. Multi-segment values use an exact-length retryable body
/// that emits one owner-backed frame per presentation segment.
pub(crate) fn sdk_body(data: SegmentedBytes) -> SdkBody {
    match data.try_into_contiguous() {
        Ok(contiguous) => SdkBody::from(contiguous),
        Err(segmented) => SdkBody::retryable(move || {
            SdkBody::from_body_1_x(SegmentedBody::new(segmented.clone()))
        }),
    }
}

/// One replay instance of a multi-segment upload part.
#[derive(Debug)]
struct SegmentedBody {
    data: SegmentedBytes,
}

impl SegmentedBody {
    /// Creates a body positioned at the first remaining segment.
    fn new(data: SegmentedBytes) -> Self {
        Self { data }
    }
}

impl Body for SegmentedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let frame = self
            .get_mut()
            .data
            .take_front_segment()
            .map(Frame::data)
            .map(Ok);
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;

    fn segmented(left: Bytes, right: Bytes) -> SegmentedBytes {
        let mut data = SegmentedBytes::from(left);
        data.append(SegmentedBytes::from(right));
        data
    }

    fn poll_frame<B>(body: &mut B) -> Option<Result<Frame<B::Data>, B::Error>>
    where
        B: Body + Unpin,
    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match Pin::new(body).poll_frame(&mut cx) {
            Poll::Ready(frame) => frame,
            Poll::Pending => panic!("immutable in-memory body returned Pending"),
        }
    }

    fn collect(mut body: SdkBody) -> Bytes {
        let mut data = Vec::new();
        while let Some(frame) = poll_frame(&mut body) {
            data.extend_from_slice(&frame.unwrap().into_data().unwrap());
        }
        Bytes::from(data)
    }

    #[test]
    fn segmented_body_emits_owner_backed_frames() {
        let left = Bytes::from_static(b"left");
        let right = Bytes::from_static(b"right");
        let left_ptr = left.as_ptr();
        let right_ptr = right.as_ptr();
        let mut body = SegmentedBody::new(segmented(left, right));

        assert_eq!(body.size_hint().exact(), Some(9));
        let first = poll_frame(&mut body).unwrap().unwrap().into_data().unwrap();
        assert_eq!(first.as_ptr(), left_ptr);
        assert_eq!(first, b"left"[..]);
        assert_eq!(body.size_hint().exact(), Some(5));

        let second = poll_frame(&mut body).unwrap().unwrap().into_data().unwrap();
        assert_eq!(second.as_ptr(), right_ptr);
        assert_eq!(second, b"right"[..]);
        assert!(body.is_end_stream());
        assert!(poll_frame(&mut body).is_none());
    }

    #[test]
    fn segmented_sdk_body_is_exact_length_and_replayable() {
        let body = sdk_body(segmented(
            Bytes::from_static(b"first"),
            Bytes::from_static(b"-second"),
        ));

        assert!(body.bytes().is_none());
        assert_eq!(body.content_length(), Some(12));
        let replay = body.try_clone().expect("segmented body must be retryable");

        assert_eq!(collect(body), b"first-second"[..]);
        assert_eq!(collect(replay), b"first-second"[..]);
    }

    #[test]
    fn contiguous_sdk_body_preserves_native_in_memory_representation() {
        let source = Bytes::from_static(b"contiguous");
        let source_ptr = source.as_ptr();
        let body = sdk_body(SegmentedBytes::from(source));

        assert_eq!(body.bytes().unwrap().as_ptr(), source_ptr);
        assert_eq!(body.bytes().unwrap(), b"contiguous");
        assert_eq!(body.content_length(), Some(10));
        assert!(body.try_clone().is_some());
    }
}
