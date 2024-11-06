/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::stream::StreamContext;
use crate::io::Buffer;
use crate::io::PartStream;
use crate::io::SizeHint;

use crate::io::PartData;

use std::mem;
use std::task::Poll;

use bytes::Bytes;
use pin_project_lite::pin_project;

// TODO - add feature gate for this?
pin_project! {
    /// A wrapper that implements [`PartStream`] trait for an inner type
    /// that implements Tokio's IO traits.
    ///
    /// # Examples
    ///
    /// ```
    /// use aws_s3_transfer_manager::io::adapters::TokioIo;
    /// use aws_s3_transfer_manager::io::{InputStream, SizeHint};
    /// use tokio::io::AsyncRead;
    ///
    /// fn into_input_stream<T: AsyncRead + Send + Sync + 'static>(inner: T, content_length: u64) -> InputStream {
    ///     InputStream::from_part_stream(TokioIo::new(inner, SizeHint::exact(content_length)))
    /// }
    /// ```
    #[derive(Debug)]
    pub struct TokioIo<T> {
        #[pin]
        inner: T,
        size_hint: SizeHint,
        buf: BufState,
        next_part: u64,
    }
}

#[derive(Debug)]
enum BufState {
    /// No buffer acquired
    Unset,
    /// Buffer acquired and possibly partially filled
    Acquired(Buffer),
}

impl BufState {
    /// Take the current buffer if already set or acquire a new one
    fn take_or_acquire(&mut self, stream_cx: &StreamContext) -> Buffer {
        let current = mem::replace(self, BufState::Unset);
        match current {
            BufState::Unset => stream_cx.new_buffer(stream_cx.part_size()),
            BufState::Acquired(buffer) => buffer,
        }
    }
}

impl<T> TokioIo<T> {
    /// Wrap a type implementing Tokio's IO traits.
    pub fn new(inner: T, size_hint: SizeHint) -> Self {
        Self {
            inner,
            size_hint,
            buf: BufState::Unset,
            next_part: 1,
        }
    }

    /// Borrow the inner type
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Mutably borrow the inner type
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> PartStream for TokioIo<T>
where
    T: tokio::io::AsyncRead,
{
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        stream_cx: &StreamContext,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
        use bytes::BufMut;
        let mut this = self.project();
        let mut inner_buf = this.buf.take_or_acquire(stream_cx);

        println!("loop outer");
        loop {
            // SAFETY: `ReadBuf` and `poll_read` promise not to set any uninitialized bytes into `dst`.
            let dst = unsafe { inner_buf.chunk_mut().as_uninit_slice_mut() };
            let mut buf = tokio::io::ReadBuf::uninit(dst);

            match this.inner.as_mut().poll_read(cx, &mut buf) {
                Poll::Ready(result) => match result {
                    Ok(_) => {
                        let n = buf.filled().len();
                        println!("read {} bytes", n);
                        if n > 0 {
                            // SAFETY: we just read that many bytes into the uninitialized part of the buffer
                            unsafe {
                                inner_buf.advance_mut(n);
                            }
                        }

                        // full part available or EOF with partial data already buffered
                        if inner_buf.len() >= stream_cx.part_size()
                            || (n == 0 && inner_buf.len() > 0)
                        {
                            let data: Bytes = inner_buf.into();
                            let part_number = *this.next_part;
                            *this.next_part += 1;
                            let part = PartData { part_number, data };
                            return Poll::Ready(Some(Ok(part)));
                        } else if n == 0 {
                            // EOF
                            return Poll::Ready(None);
                        }
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                Poll::Pending => {
                    // store already acquired, possibly partially filled buffer for next poll
                    *this.buf = BufState::Acquired(inner_buf);
                    return Poll::Pending;
                }
            }
        }
    }

    fn size_hint(&self) -> crate::io::SizeHint {
        self.size_hint
    }
}

#[cfg(test)]
mod tests {
    use super::TokioIo;
    use crate::io::stream::StreamContext;
    use crate::io::PartData;
    use crate::io::PartStream;
    use crate::io::SizeHint;
    use futures_test::task::new_count_waker;
    use std::pin::pin;
    use std::task::Context;
    use tokio_test::io::Builder;
    use tokio_test::{assert_pending, assert_ready};

    #[tokio::test]
    async fn test_tokio_adapter_e2e() {
        let (mock, mut handle) = Builder::new().build_with_handle();
        let part_size = 5;
        let mut io = pin!(TokioIo::new(mock, SizeHint::exact(10)));

        let (waker, awoken_cnt) = new_count_waker();
        let mut task_cx = Context::from_waker(&waker);
        let stream_cx = StreamContext::new(part_size);

        assert_pending!(io.as_mut().poll_part(&stream_cx, &mut task_cx));
        handle.read(b"hello");
        assert_eq!(awoken_cnt.get(), 1);

        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx))
            .unwrap()
            .unwrap();
        let expected = PartData::new(1, "hello");
        assert_eq!(expected, result);

        assert_pending!(io.as_mut().poll_part(&stream_cx, &mut task_cx));
        handle.read(b"world");
        assert_eq!(awoken_cnt.get(), 2);

        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx))
            .unwrap()
            .unwrap();
        let expected = PartData::new(2, "world");
        assert_eq!(expected, result);

        drop(handle);
        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx));
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_tokio_adapter_partial_reads() {
        let (mock, mut handle) = Builder::new().build_with_handle();
        let part_size = 10;
        let mut io = pin!(TokioIo::new(mock, SizeHint::exact(16)));

        let (waker, awoken_cnt) = new_count_waker();
        let mut task_cx = Context::from_waker(&waker);
        let stream_cx = StreamContext::new(part_size);

        // partial read
        handle.read(b"hello");
        assert_pending!(io.as_mut().poll_part(&stream_cx, &mut task_cx));

        handle.read(b"world"); // full part available
        handle.read(b"second"); // last part less than part size
        drop(handle);
        assert_eq!(awoken_cnt.get(), 1);

        // we have a full part available at this point so it should be yielded
        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx))
            .unwrap()
            .unwrap();
        let expected = PartData::new(1, "helloworld");
        assert_eq!(expected, result);

        // should hit EOF and get the last part less than configured part size
        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx))
            .unwrap()
            .unwrap();
        let expected = PartData::new(2, "second");
        assert_eq!(expected, result);

        // one final poll to actual get done signal
        let result = assert_ready!(io.as_mut().poll_part(&stream_cx, &mut task_cx));
        assert!(result.is_none());
    }
}
