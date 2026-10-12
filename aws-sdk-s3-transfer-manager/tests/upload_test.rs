/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp;
use std::sync::{Arc, Mutex};
use std::task::ready;
use std::{task::Poll, time::Duration};

use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3_transfer_manager::error::{Error, ErrorKind};
use aws_sdk_s3_transfer_manager::io::{
    InputStream, PartBuffer, PartData, PartStream, SizeHint, StreamContext,
};
use aws_sdk_s3_transfer_manager::memory::{
    BufferPool, MemoryBudgetConfig, MemoryConfig, SegmentedBytes,
};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_smithy_mocks::{mock, mock_client, Rule, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::{BufMut, Bytes};
use pin_project_lite::pin_project;

use tokio::sync::mpsc;

/// number of simultaneous uploads to create
const MANY_ASYNC_UPLOADS_CNT: usize = 200;
/// number of bytes to upload per transfer
const MANY_ASYNC_UPLOADS_OBJECT_SIZE: usize = 100;
/// bytes per write
const MANY_ASYNC_UPLOADS_BYTES_PER_WRITE: usize = 10;
/// how long to spend before assuming we're deadlocked
const SEND_DATA_TIMEOUT_S: u64 = 10;

use std::sync::atomic::{AtomicUsize, Ordering};

pin_project! {
    #[derive(Debug)]
    struct TestStream {
        next_part_num: u64,
        rx: mpsc::Receiver<Bytes>,
        size_hint: SizeHint,
        observed_part_size: Arc<AtomicUsize>,
    }
}

impl TestStream {
    fn exact(rx: mpsc::Receiver<Bytes>, size: u64) -> Self {
        Self::with_size_hint(rx, SizeHint::exact(size))
    }

    fn with_size_hint(rx: mpsc::Receiver<Bytes>, size_hint: SizeHint) -> Self {
        Self {
            next_part_num: 1,
            rx,
            size_hint,
            observed_part_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn observed_part_size(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.observed_part_size)
    }
}

impl PartStream for TestStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        this.observed_part_size
            .store(stream_cx.part_size(), Ordering::Relaxed);
        let data = ready!(this.rx.poll_recv(cx));
        let part = data.map(|b| {
            let part_num = *this.next_part_num;
            *this.next_part_num += 1;
            Ok(PartData::new(part_num, b))
        });
        Poll::Ready(part)
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }
}

pin_project! {
    /// A `PartStream` whose total size is not known up front: `size_hint` has no upper bound.
    ///
    /// Mirrors a caller like Mountpoint, which writes an object whose final size is only known once
    /// the writer closes. Parts arrive over a channel; closing the sender is end-of-stream.
    #[derive(Debug)]
    struct UnknownLengthStream {
        next_part_num: u64,
        rx: mpsc::Receiver<Bytes>,
    }
}

impl UnknownLengthStream {
    fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            next_part_num: 1,
            rx,
        }
    }
}

impl PartStream for UnknownLengthStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        let data = ready!(this.rx.poll_recv(cx));
        let part = data.map(|b| {
            let part_num = *this.next_part_num;
            *this.next_part_num += 1;
            Ok(PartData::new(part_num, b))
        });
        Poll::Ready(part)
    }

    /// No upper bound — this is what routes the upload down the unknown-length path.
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

pin_project! {
    /// Collects caller writes into one complete part while preserving source backpressure.
    #[derive(Debug)]
    struct AccumulatingPartStream {
        rx: mpsc::Receiver<Bytes>,
        expected_len: usize,
        buffered: Vec<u8>,
        emitted: bool,
    }
}

impl AccumulatingPartStream {
    fn new(rx: mpsc::Receiver<Bytes>, expected_len: usize) -> Self {
        Self {
            rx,
            expected_len,
            buffered: Vec::with_capacity(expected_len),
            emitted: false,
        }
    }
}

impl PartStream for AccumulatingPartStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        if *this.emitted {
            return Poll::Ready(None);
        }

        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Some(bytes)) => {
                    this.buffered.extend_from_slice(&bytes);
                    if this.buffered.len() > *this.expected_len {
                        return Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "test stream received more bytes than declared",
                        ))));
                    }
                    if this.buffered.len() == *this.expected_len {
                        *this.emitted = true;
                        return Poll::Ready(Some(Ok(PartData::new(
                            1,
                            std::mem::take(this.buffered),
                        ))));
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "test stream ended before the complete part arrived",
                    ))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.expected_len as u64)
    }
}

#[derive(Debug)]
struct WakeBeforePendingStream {
    polls: Arc<AtomicUsize>,
}

impl PartStream for WakeBeforePendingStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        match self.polls.fetch_add(1, Ordering::Relaxed) {
            0 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            1 => Poll::Ready(Some(Ok(PartData::new(1, Bytes::from_static(b"ready"))))),
            2 => Poll::Ready(None),
            _ => panic!("single-part stream was polled after end-of-stream"),
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(5)
    }
}

#[derive(Debug)]
struct SegmentedPartStream {
    part: Option<PartData>,
    size: u64,
}

#[derive(Debug)]
struct PooledPartStream {
    data: Option<Bytes>,
    buffer: Option<PartBuffer>,
}

impl PartStream for PooledPartStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let Some(data_len) = self.data.as_ref().map(Bytes::len) else {
            return Poll::Ready(None);
        };
        let buffer = self.buffer.get_or_insert_with(|| stream_cx.part_buffer());
        match buffer.poll_acquire(cx, data_len) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(())) => {}
        }

        let data = self.data.take().expect("pooled stream data disappeared");
        self.buffer
            .as_mut()
            .expect("pooled stream buffer disappeared")
            .put_slice(&data);
        let data = self
            .buffer
            .take()
            .expect("pooled stream buffer disappeared")
            .freeze();
        Poll::Ready(Some(Ok(PartData::from_segmented(1, data))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.data.as_ref().map_or(0, |data| data.len() as u64))
    }
}

impl PartStream for SegmentedPartStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        Poll::Ready(self.part.take().map(Ok))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.size)
    }
}

/// Builds a transfer manager with default config that sends its S3 requests to `client`.
fn tm_with(client: aws_sdk_s3::Client) -> aws_sdk_s3_transfer_manager::Client {
    aws_sdk_s3_transfer_manager::Client::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .client(client)
            .build(),
    )
}

/// The upload ID a [`MultipartMock`]'s CreateMultipartUpload returns.
const MOCK_UPLOAD_ID: &str = "test-upload-id";

/// The multipart requests a [`MultipartMock`]'s default rules answered, in arrival order.
///
/// A request answered by an override rule is not recorded; that rule's `num_calls` counts it.
#[derive(Debug, Default)]
struct MultipartRecord {
    /// `(part number, content length)` of each UploadPart.
    upload_parts: Mutex<Vec<(i32, i64)>>,
    /// Each CompleteMultipartUpload request, as received.
    completions: Mutex<Vec<CompleteMultipartUploadInput>>,
}

impl MultipartRecord {
    /// `(part number, content length)` of each recorded UploadPart.
    fn upload_parts(&self) -> Vec<(i32, i64)> {
        self.upload_parts.lock().unwrap().clone()
    }

    /// How many UploadPart requests were recorded.
    fn upload_part_calls(&self) -> usize {
        self.upload_parts.lock().unwrap().len()
    }

    /// Each recorded CompleteMultipartUpload request.
    fn completions(&self) -> Vec<CompleteMultipartUploadInput> {
        self.completions.lock().unwrap().clone()
    }

    /// How many CompleteMultipartUpload requests were recorded.
    fn complete_calls(&self) -> usize {
        self.completions.lock().unwrap().len()
    }

    /// The part numbers each recorded CompleteMultipartUpload listed, in the order it listed them.
    fn completed_part_numbers(&self) -> Vec<Vec<i32>> {
        self.completions()
            .iter()
            .map(|complete| {
                complete
                    .multipart_upload()
                    .map(|upload| {
                        upload
                            .parts()
                            .iter()
                            .filter_map(|part| part.part_number())
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    /// The `MpuObjectSize` each recorded CompleteMultipartUpload carried.
    fn mpu_object_sizes(&self) -> Vec<Option<i64>> {
        self.completions()
            .iter()
            .map(CompleteMultipartUploadInput::mpu_object_size)
            .collect()
    }
}

/// A mock multipart service. CreateMultipartUpload, UploadPart and CompleteMultipartUpload succeed
/// and are recorded in a [`MultipartRecord`], unless a test overrides one with its own rule.
///
/// CreateMultipartUpload returns [`MOCK_UPLOAD_ID`]. The default UploadPart and
/// CompleteMultipartUpload rules match only requests carrying that upload ID, so a request carrying
/// any other ID matches no rule and the mock panics. The default UploadPart answer carries the ETag
/// `etag-{part number}`.
struct MultipartMock {
    record: Arc<MultipartRecord>,
    /// Consulted in the order they were added, before the default rules.
    overrides: Vec<Rule>,
}

impl MultipartMock {
    /// A mock with no override rules and an empty record.
    fn new() -> Self {
        Self {
            record: Arc::default(),
            overrides: Vec::new(),
        }
    }

    /// Answers the requests `rule` matches with `rule` instead of the default rules.
    ///
    /// `rule` applies to the operation its `mock!` names. The requests it answers are not recorded;
    /// `rule.num_calls()` counts them.
    fn with_override(mut self, rule: &Rule) -> Self {
        self.overrides.push(rule.clone());
        self
    }

    /// Builds a client that answers with the override rules, then the default rules.
    ///
    /// `RuleMode::MatchAny` answers a request with the first rule, in list order, that matches it,
    /// which is what lets an override take precedence.
    fn client(&self) -> aws_sdk_s3::Client {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id(MOCK_UPLOAD_ID)
                .build()
        });
        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .match_requests(|req| req.upload_id() == Some(MOCK_UPLOAD_ID))
            .then_compute_output({
                let record = Arc::clone(&self.record);
                move |req| {
                    let part_number = req.part_number().expect("UploadPart carries a part number");
                    let content_length = req
                        .content_length()
                        .expect("UploadPart carries a content length");
                    record
                        .upload_parts
                        .lock()
                        .unwrap()
                        .push((part_number, content_length));
                    UploadPartOutput::builder()
                        .e_tag(format!("etag-{part_number}"))
                        .build()
                }
            });
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .match_requests(|req| req.upload_id() == Some(MOCK_UPLOAD_ID))
            .then_compute_output({
                let record = Arc::clone(&self.record);
                move |req| {
                    record.completions.lock().unwrap().push(req.clone());
                    CompleteMultipartUploadOutput::builder().build()
                }
            });

        let rules: Vec<Rule> = self
            .overrides
            .iter()
            .cloned()
            .chain([create_mpu, upload_part, complete_mpu])
            .collect();
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &rules)
    }

    /// Builds a transfer manager with default config over [`Self::client`].
    fn tm(&self) -> aws_sdk_s3_transfer_manager::Client {
        tm_with(self.client())
    }

    /// The requests the default rules have answered so far.
    fn record(&self) -> &MultipartRecord {
        &self.record
    }
}

/// A `PartStream` that yields one part per scripted part number, in script order, then
/// end-of-stream.
///
/// The part at script position `i` holds `i + 1` bytes, so a recorded UploadPart's content length
/// identifies which scripted part it carried.
#[derive(Debug)]
struct NumberedPartStream {
    part_numbers: std::collections::VecDeque<u64>,
    yielded: usize,
}

impl NumberedPartStream {
    fn new(part_numbers: &[u64]) -> Self {
        Self {
            part_numbers: part_numbers.iter().copied().collect(),
            yielded: 0,
        }
    }
}

impl PartStream for NumberedPartStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let Some(part_number) = self.part_numbers.pop_front() else {
            return Poll::Ready(None);
        };
        self.yielded += 1;
        let data = vec![0u8; self.yielded];
        Poll::Ready(Some(Ok(PartData::new(part_number, data))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Uploads a `NumberedPartStream` for `part_numbers` and returns the outcome with the mock it ran
/// against.
async fn upload_numbered_parts(part_numbers: &[u64]) -> (Result<(), Error>, MultipartMock) {
    let mock = MultipartMock::new();
    let tm = mock.tm();
    let result = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(NumberedPartStream::new(
            part_numbers,
        )))
        .initiate()
        .unwrap()
        .join()
        .await
        .map(|_| ());
    (result, mock)
}

/// Runs one valid zero-byte multipart source and verifies that the transfer sends exactly one empty
/// part before completing a zero-byte object.
async fn assert_single_empty_multipart_upload(stream: InputStream) {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    tm.upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(stream)
        .initiate()
        .unwrap()
        .join()
        .await
        .expect("a valid empty stream must complete as a zero-byte object");

    assert_eq!(
        vec![(1, 0)],
        mock.record().upload_parts(),
        "an empty multipart source must upload exactly one empty part"
    );
    assert_eq!(vec![Some(0)], mock.record().mpu_object_sizes());
}

/// Runs one nonexact size declaration and verifies that CompleteMPU receives the validated bytes
/// emitted by the source rather than either declared bound.
async fn assert_ranged_mpu_object_size(size_hint: SizeHint, actual: usize) {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::with_size_hint(
            rx, size_hint,
        )))
        .initiate()
        .unwrap();
    tx.send(Bytes::from(vec![0u8; actual])).await.unwrap();
    drop(tx);

    handle
        .join()
        .await
        .expect("source output within its declared bounds must upload");
    assert_eq!(vec![Some(actual as i64)], mock.record().mpu_object_sizes());
}

#[tokio::test]
async fn test_custom_stream_uploads_segmented_part_data() {
    let mut data = SegmentedBytes::from(Bytes::from_static(b"left"));
    data.append(SegmentedBytes::from(Bytes::from_static(b"-right")));
    let size = data.len() as u64;
    let stream = SegmentedPartStream {
        part: Some(PartData::from_segmented(1, data)),
        size,
    };
    let tm = MultipartMock::new().tm();

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("segmented-part")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();

    handle.join().await.unwrap();
}

#[tokio::test]
async fn test_custom_stream_resumes_after_part_buffer_admission() {
    const CAPACITY: usize = 1024 * 1024;

    let pool = BufferPool::builder()
        .memory_budget(MemoryBudgetConfig::Limit(CAPACITY))
        .build()
        .unwrap();
    let holder = pool.try_reserve(CAPACITY).unwrap().unwrap();
    let held = pool.acquire(&holder, CAPACITY).unwrap();

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(MultipartMock::new().client())
        .memory(MemoryConfig::Explicit(pool.clone()))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("pooled-part")
        .body(InputStream::from_part_stream(PooledPartStream {
            data: Some(Bytes::from_static(b"pooled payload")),
            buffer: None,
        }))
        .initiate()
        .unwrap();
    let join = tokio::spawn(async move { handle.join().await });

    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.metrics().queued_reservations() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("custom stream did not wait for pool admission");

    drop(held);
    holder.close_acquisition();

    tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("custom stream did not resume after pool admission")
        .expect("upload task panicked")
        .unwrap();
    assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
}

#[tokio::test]
async fn test_source_wake_before_read_parking_is_retained() {
    let tm = MultipartMock::new().tm();
    let polls = Arc::new(AtomicUsize::new(0));

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("wake-before-park")
        .body(InputStream::from_part_stream(WakeBeforePendingStream {
            polls: Arc::clone(&polls),
        }))
        .initiate()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("source wake was lost before the retained read was parked")
        .unwrap();
    assert_eq!(polls.load(Ordering::Relaxed), 3);
}

// Regression test for deadlock discovered by a user of Mountpoint
// The user opens MANY files at once. The user wrote data to some of the later files they opened,
// and waited for those writes to complete.
//
// If we wait on data from the first few files then both sides
// are waiting on each other causing deadlock.
//
// This test starts N uploads then only processes them starting from the last one created.
// If the test times out, then we suffer from deadlock.
//
// See https://github.com/awslabs/aws-c-s3/blob/5d8d4205e7de4e152bf26bb27d86f3acf8cd5d2/tests/s3_many_async_uploads_without_data_test.c
// Each source retains one in-progress part while its channel is empty. A pending source read must
// release its scheduler slot so reverse-order writers can wake and complete.
#[tokio::test]
async fn test_many_uploads_no_deadlock() {
    let (_guard, _rx) = capture_test_logs();
    let tm = MultipartMock::new().tm();

    let mut transfers = Vec::with_capacity(MANY_ASYNC_UPLOADS_CNT);
    for i in 0..MANY_ASYNC_UPLOADS_CNT {
        let (tx, rx) = mpsc::channel(1);
        let stream = AccumulatingPartStream::new(rx, MANY_ASYNC_UPLOADS_OBJECT_SIZE);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key(format!("many-async-uploads-{}.txt", i))
            .body(InputStream::from_part_stream(stream))
            .initiate()
            .unwrap();

        transfers.push((handle, tx));
    }

    let mut handles = Vec::with_capacity(MANY_ASYNC_UPLOADS_CNT);

    // process transfers in reverse order
    while let Some((handle, tx)) = transfers.pop() {
        let mut bytes_written = 0;
        let mut eof = false;
        while !eof {
            let wc = cmp::min(
                MANY_ASYNC_UPLOADS_BYTES_PER_WRITE,
                MANY_ASYNC_UPLOADS_OBJECT_SIZE - bytes_written,
            );
            eof = (bytes_written + wc) == MANY_ASYNC_UPLOADS_OBJECT_SIZE;

            let data = vec![b'z'; wc];
            let buf = Bytes::from(data);
            match tx
                .send_timeout(buf, Duration::from_secs(SEND_DATA_TIMEOUT_S))
                .await
            {
                Ok(_) => {}
                Err(err) => panic!("failed to send due to timeout or closed channel: {}", err),
            }
            bytes_written += wc;
        }

        drop(tx);
        handles.push(handle);
    }

    // wait for everything to finish
    while let Some(handle) = handles.pop() {
        handle.join().await.unwrap();
    }
}

#[tokio::test]
async fn test_large_upload_part_size_bump() {
    let tm = MultipartMock::new().tm();

    let (tx, rx) = mpsc::channel(1);
    let size_hint = 100 * ByteUnit::Gibibyte.as_bytes_u64();
    let stream = TestStream::with_size_hint(rx, SizeHint::default().with_upper(Some(size_hint)));
    let observed_part_size = stream.observed_part_size();

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-large-upload-part-size".to_string())
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();

    // actual object is empty, but we will bump the part_size based on size_hint
    drop(tx);
    handle.join().await.unwrap();
    // part_size must be bumped using size_hint.div_ceil(MAX_PARTS) to fit the MAX_PARTS limit.
    let expected_part_size = 10737419;
    assert_eq!(
        observed_part_size.load(Ordering::Relaxed),
        expected_part_size
    );
}

/// CompleteMultipartUpload must carry `MpuObjectSize` set to the full content
/// length so S3 rejects the request if the object it assembled is a different
/// size (a dropped or duplicated part). Required by SEP step 7.
#[tokio::test]
async fn test_complete_mpu_sends_mpu_object_size() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // Two full parts plus a partial one, so the total is not a part-size multiple
    // and a stale part-count-derived value would not match.
    let content_length = 2 * part_size + 1024;

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(3);
    let stream = TestStream::exact(rx, content_length as u64);

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-mpu-object-size")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();

    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; 1024])).await.unwrap();
    drop(tx);
    handle.join().await.expect("upload should succeed");
    assert_eq!(
        vec![Some(content_length as i64)],
        mock.record().mpu_object_sizes(),
        "CompleteMPU must carry MpuObjectSize = content length"
    );
}

// --- Unknown content length --------------------------------------------------
//
// An unknown-length source is always a `PartStream`, which is `is_mpu_only`, so
// these all exercise the multipart path. Termination comes from the reader
// reporting end-of-stream rather than from a part count.

/// A stream with no declared length uploads via multipart and completes.
#[tokio::test]
async fn test_unknown_length_multipart_upload() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = MultipartMock::new().tm();

    let (tx, rx) = mpsc::channel(2);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    // Two full parts, then end-of-stream by dropping the sender.
    for _ in 0..2 {
        tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    }
    drop(tx);

    handle
        .join()
        .await
        .expect("an unknown-length stream must upload, not panic");
}

/// An empty stream with no declared bounds must complete as a normal 0-byte object.
///
/// S3 rejects a CompleteMultipartUpload that lists no parts, so "no bytes" cannot
/// mean "no parts": the transfer synthesizes a single empty part 1. The mock
/// asserts exactly that shape — one UploadPart, part number 1, zero bytes.
#[tokio::test]
async fn test_unknown_length_empty_stream() {
    let (tx, rx) = mpsc::channel(1);
    drop(tx);
    assert_single_empty_multipart_upload(InputStream::from_part_stream(UnknownLengthStream::new(
        rx,
    )))
    .await;
}

/// A zero-byte source satisfies an upper-only declaration because its lower bound remains zero.
#[tokio::test]
async fn test_upper_only_hint_allows_empty_stream() {
    let (tx, rx) = mpsc::channel(1);
    drop(tx);
    let stream =
        TestStream::with_size_hint(rx, SizeHint::default().with_upper(Some(5 * 1024 * 1024)));
    assert_single_empty_multipart_upload(InputStream::from_part_stream(stream)).await;
}

/// An exact zero-byte declaration is valid and still requires one empty MPU part.
#[tokio::test]
async fn test_exact_zero_hint_allows_empty_stream() {
    let (tx, rx) = mpsc::channel(1);
    drop(tx);
    assert_single_empty_multipart_upload(InputStream::from_part_stream(TestStream::exact(rx, 0)))
        .await;
}

/// An explicitly yielded zero-byte part is the empty object; the transfer must not synthesize a
/// duplicate after observing EOF.
#[tokio::test]
async fn test_explicit_zero_byte_part_is_not_duplicated() {
    let stream = SegmentedPartStream {
        part: Some(PartData::new(1, Bytes::new())),
        size: 0,
    };
    assert_single_empty_multipart_upload(InputStream::from_part_stream(stream)).await;
}

/// `MpuObjectSize` for an unknown-length upload is the sum of the bytes actually
/// uploaded. With no declared length there is no independent witness, but the
/// value must still be sent so S3 can reject a mismatched assembly.
#[tokio::test]
async fn test_unknown_length_mpu_object_size_is_running_sum() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // Deliberately not a part-size multiple, so a part-count-derived value wouldn't match.
    let total = 2 * part_size + 1024;

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(2);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; 1024])).await.unwrap();
    drop(tx);

    handle.join().await.expect("upload should succeed");
    assert_eq!(
        vec![Some(total as i64)],
        mock.record().mpu_object_sizes(),
        "CompleteMPU must carry MpuObjectSize equal to the summed bytes of an unknown-length stream"
    );
}

/// An upper bound sizes the transfer but does not become the completed object size.
///
/// The stream declares at most two full parts and ends after one full part plus a tail.
/// `MpuObjectSize` must carry the validated bytes actually emitted, not the planning bound.
#[tokio::test]
async fn test_bounded_length_sends_validated_actual_size() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let upper = 2 * part_size;
    let actual = part_size + 1024;

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let stream = TestStream::with_size_hint(rx, SizeHint::default().with_upper(Some(upper as u64)));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
    tx.send(Bytes::from(vec![0u8; actual])).await.unwrap();
    drop(tx);

    handle.join().await.expect("upload should succeed");
    assert_eq!(
        vec![Some(actual as i64)],
        mock.record().mpu_object_sizes(),
        "a bounded upload must send its validated actual size as MpuObjectSize"
    );
}

/// Lower-only and two-sided bounds constrain the source without becoming an exact object size.
#[tokio::test]
async fn test_ranged_hints_send_validated_actual_mpu_object_size() {
    assert_ranged_mpu_object_size(SizeHint::default().with_lower(5), 7).await;
    assert_ranged_mpu_object_size(SizeHint::default().with_lower(5).with_upper(Some(10)), 7).await;
}

/// An exact size hint is a contract, not only a planning input.
#[tokio::test]
async fn test_exact_length_rejects_early_end_of_stream() {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::exact(rx, 10)))
        .initiate()
        .unwrap();
    tx.send(Bytes::from_static(b"short")).await.unwrap();
    drop(tx);

    let error = handle
        .join()
        .await
        .expect_err("an exact stream ending below its size must fail");
    assert_eq!(*error.kind(), ErrorKind::InputInvalid);
    assert_eq!(mock.record().upload_part_calls(), 1);
    assert_eq!(mock.record().complete_calls(), 0);
}

/// The upper bound is enforced before an oversized part is sent to S3.
#[tokio::test]
async fn test_exact_length_rejects_overflow_before_upload_part() {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::exact(rx, 5)))
        .initiate()
        .unwrap();
    tx.send(Bytes::from_static(b"excess")).await.unwrap();
    drop(tx);

    let error = handle
        .join()
        .await
        .expect_err("a stream exceeding its exact size must fail");
    assert_eq!(*error.kind(), ErrorKind::InputInvalid);
    assert_eq!(mock.record().upload_part_calls(), 0);
    assert_eq!(mock.record().complete_calls(), 0);
}

/// A bounded stream must satisfy its lower bound when EOF closes dispatch.
#[tokio::test]
async fn test_bounded_length_rejects_below_lower_bound() {
    let tm = MultipartMock::new().tm();
    let (tx, rx) = mpsc::channel(1);
    let hint = SizeHint::default().with_lower(5).with_upper(Some(10));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::with_size_hint(
            rx, hint,
        )))
        .initiate()
        .unwrap();
    tx.send(Bytes::from_static(b"four")).await.unwrap();
    drop(tx);

    let error = handle
        .join()
        .await
        .expect_err("a bounded stream ending below its lower bound must fail");
    assert_eq!(*error.kind(), ErrorKind::InputInvalid);
}

/// Contradictory bounds are rejected before any upload work is scheduled.
#[test]
fn test_stream_rejects_lower_bound_above_upper_bound() {
    let (_tx, rx) = mpsc::channel(1);
    let hint = SizeHint::default().with_lower(11).with_upper(Some(10));
    let tm = tm_with(mock_client!(aws_sdk_s3, []));

    let error = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::with_size_hint(
            rx, hint,
        )))
        .initiate()
        .expect_err("contradictory stream bounds must be rejected");
    assert_eq!(*error.kind(), ErrorKind::InputInvalid);
}

/// Uploads `produced` bytes, as one part, from a stream with no size bounds, on a request that
/// declares `content_length`. Returns the outcome with the mock it ran against.
async fn upload_with_content_length(
    content_length: i64,
    produced: usize,
) -> (Result<(), Error>, MultipartMock) {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .content_length(content_length)
        .body(InputStream::from_part_stream(TestStream::with_size_hint(
            rx,
            SizeHint::default(),
        )))
        .initiate()
        .unwrap();
    tx.send(Bytes::from(vec![0u8; produced])).await.unwrap();
    drop(tx);
    let result = handle.join().await.map(|_| ());
    (result, mock)
}

/// A stream with no size bounds that ends short of the declared `content_length` fails before
/// CompleteMultipartUpload, rather than storing the short object.
#[tokio::test]
async fn test_content_length_rejects_short_stream_before_completion() {
    let (result, mock) = upload_with_content_length(10, 5).await;
    let error = result.expect_err("a stream ending below its content_length must fail");
    assert_eq!(ErrorKind::InputInvalid, *error.kind());
    assert!(mock.record().completions().is_empty());
}

/// A stream with no size bounds that runs past the declared `content_length` fails before the part
/// that exceeds it is sent.
#[tokio::test]
async fn test_content_length_rejects_long_stream_before_upload_part() {
    let (result, mock) = upload_with_content_length(10, 11).await;
    let error = result.expect_err("a stream running past its content_length must fail");
    assert_eq!(ErrorKind::InputInvalid, *error.kind());
    assert_eq!(0, mock.record().upload_part_calls());
    assert!(mock.record().completions().is_empty());
}

/// A stream that produces exactly the declared `content_length` uploads, and CompleteMultipartUpload
/// carries the declaration as `MpuObjectSize`.
#[tokio::test]
async fn test_content_length_matching_stream_uploads() {
    let (result, mock) = upload_with_content_length(10, 10).await;
    result.expect("a stream producing exactly its content_length must upload");
    assert_eq!(vec![Some(10)], mock.record().mpu_object_sizes());
}

/// A `content_length` that is negative or contradicts the body's own size fails at `initiate()`.
#[tokio::test]
async fn test_content_length_contradicting_body_fails_at_initiate() {
    let tm = tm_with(mock_client!(aws_sdk_s3, []));
    let initiate = |content_length: i64, body: InputStream| {
        tm.upload()
            .bucket("test-bucket")
            .key("test-key")
            .content_length(content_length)
            .body(body)
            .initiate()
            .map(|_| ())
    };

    let (_tx, rx) = mpsc::channel(1);
    let exact_stream = InputStream::from_part_stream(TestStream::exact(rx, 5));
    let error = initiate(10, exact_stream).expect_err("content_length above an exact hint");
    assert_eq!(ErrorKind::InputInvalid, *error.kind());

    let error = initiate(5, InputStream::from_static(b"abc"))
        .expect_err("content_length above an in-memory body's length");
    assert_eq!(ErrorKind::InputInvalid, *error.kind());

    let error =
        initiate(-1, InputStream::from_static(b"abc")).expect_err("negative content_length");
    assert_eq!(ErrorKind::InputInvalid, *error.kind());
}

/// A stream that yields data must never *also* send an empty part 1.
///
/// The empty-part rule only applies when the stream turned out to be empty. Parts
/// are dispatched speculatively, so the dispatch that reads end-of-stream can run
/// while the first data part is still in flight; if "has anything been emitted" is
/// recorded only once that send completes, the end-of-stream dispatch sees "nothing
/// emitted" and synthesizes a *second* part 1. CompleteMultipartUpload would then
/// list part 1 twice — and whichever write S3 kept last could truncate the object.
#[tokio::test]
async fn test_unknown_length_with_data_never_sends_empty_part() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    // Exactly one data part, then end-of-stream.
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    drop(tx);

    handle.join().await.expect("upload should succeed");

    // Split the recorded parts by body size so the empty ones' count is the assertion.
    let (empty_parts, data_parts): (Vec<_>, Vec<_>) = mock
        .record()
        .upload_parts()
        .into_iter()
        .partition(|&(_, content_length)| content_length == 0);
    assert_eq!(
        1,
        data_parts.len(),
        "the single data part must be uploaded once"
    );
    assert_eq!(
        0,
        empty_parts.len(),
        "a stream that yielded data must not also send an empty part 1"
    );
}

pin_project! {
    /// An unknown-length `PartStream` that also supplies a full-object checksum.
    ///
    /// `full_object_checksum` is consulted once, after the final part, and only when the
    /// upload uses a `FullObject` checksum strategy without a value set up front. Pairing
    /// it with an unknown length is the combination the design flagged as needing
    /// coverage: the size is not known when the strategy is chosen.
    #[derive(Debug)]
    struct UnknownLengthChecksumStream {
        next_part_num: u64,
        rx: mpsc::Receiver<Bytes>,
        full_object_checksum: Option<String>,
    }
}

impl UnknownLengthChecksumStream {
    fn new(rx: mpsc::Receiver<Bytes>, full_object_checksum: Option<String>) -> Self {
        Self {
            next_part_num: 1,
            rx,
            full_object_checksum,
        }
    }
}

impl PartStream for UnknownLengthChecksumStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        let data = ready!(this.rx.poll_recv(cx));
        let part = data.map(|b| {
            let part_num = *this.next_part_num;
            *this.next_part_num += 1;
            Ok(PartData::new(part_num, b))
        });
        Poll::Ready(part)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }

    fn full_object_checksum(&self) -> Option<String> {
        self.full_object_checksum.clone()
    }
}

/// A full-object checksum supplied by the stream must reach CompleteMultipartUpload even
/// though the length was never declared.
///
/// The checksum is a property of the stream, not of any part, so it is orthogonal to the
/// unknown-length machinery — this pins that, since the value is only produced after the
/// final part and the transfer decides "final" from end-of-stream rather than a count.
#[tokio::test]
async fn test_unknown_length_forwards_full_object_checksum() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // Base64 of a CRC32 value; the test only checks that it is forwarded verbatim.
    let expected_checksum = "AAAAAA==";

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(1);
    let stream = UnknownLengthChecksumStream::new(rx, Some(expected_checksum.to_owned()));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();

    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    drop(tx);

    handle.join().await.expect("upload should succeed");
    let completions = mock.record().completions();
    let checksums: Vec<_> = completions
        .iter()
        .map(CompleteMultipartUploadInput::checksum_crc32)
        .collect();
    assert_eq!(
        vec![Some(expected_checksum)],
        checksums,
        "a stream-supplied full-object checksum must be sent on CompleteMPU"
    );
}

/// An unknown-length transfer cannot report a total while in flight, but its final
/// metrics must report the true size.
///
/// `total_bytes` is seeded from the size hint, which is absent here, so it is set once
/// end-of-stream makes the summed size exact. Without that, a completed streaming upload
/// would report `total_bytes: None` forever.
#[tokio::test]
async fn test_unknown_length_reports_total_bytes_after_completion() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tail = 512;
    let expected = (part_size + tail) as u64;

    let tm = MultipartMock::new().tm();

    let (tx, rx) = mpsc::channel(2);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    // No declared length, so nothing to report yet.
    assert_eq!(
        None,
        handle.metrics().total_bytes,
        "an unknown-length transfer must not invent a total while in flight"
    );

    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; tail])).await.unwrap();
    drop(tx);

    let output = handle.join().await.expect("upload should succeed");

    assert_eq!(
        Some(expected),
        output.metrics.total_bytes,
        "final metrics must report the streamed size once end-of-stream makes it exact"
    );
    assert_eq!(
        expected, output.metrics.network_tx,
        "every streamed byte must be accounted for on the wire"
    );
}

/// A reader error part-way through an unknown-length stream fails the transfer rather
/// than being mistaken for end-of-stream.
///
/// `Ok(None)` means "done" and `Err` means "broken"; conflating them would silently
/// complete a truncated object, which is the worst possible outcome for an upload.
#[tokio::test]
async fn test_unknown_length_reader_error_fails_transfer() {
    let tm = MultipartMock::new().tm();

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(FailingUnknownLengthStream {
            yielded: false,
        }))
        .initiate()
        .unwrap();

    let err = handle
        .join()
        .await
        .expect_err("a reader error must fail the upload, not complete it");
    assert!(
        matches!(err.kind(), ErrorKind::IOError),
        "expected an IO error, got {:?}",
        err.kind()
    );
}

/// Yields one part, then errors — never reports end-of-stream.
#[derive(Debug)]
struct FailingUnknownLengthStream {
    yielded: bool,
}

impl PartStream for FailingUnknownLengthStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        if self.yielded {
            return Poll::Ready(Some(Err(std::io::Error::other("simulated reader failure"))));
        }
        self.yielded = true;
        let data = vec![0u8; stream_cx.part_size()];
        Poll::Ready(Some(Ok(PartData::new(1, data))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// A stream that yields more parts than the unknown-length default capacity must still
/// complete with every part accounted for.
///
/// With no declared length there is no part count to size `completed_parts` by, so it
/// starts at a small default (32) and grows. This walks past that boundary so a
/// mis-sized or truncated part list would show up as a wrong `MpuObjectSize`.
#[tokio::test]
async fn test_unknown_length_many_parts_grows_part_list() {
    // Smallest permitted part size keeps the test cheap; 40 parts crosses the 32 default.
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let num_parts = 40usize;
    let expected_total = (part_size * num_parts) as i64;

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(4);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    tokio::spawn(async move {
        for _ in 0..num_parts {
            if tx.send(Bytes::from(vec![0u8; part_size])).await.is_err() {
                return;
            }
        }
        drop(tx);
    });

    handle
        .join()
        .await
        .expect("an unknown-length stream must handle more parts than the default capacity");

    assert_eq!(
        vec![Some(expected_total)],
        mock.record().mpu_object_sizes(),
        "MpuObjectSize must count every part"
    );
    assert_eq!(
        num_parts,
        mock.record().upload_part_calls(),
        "every part must be uploaded exactly once"
    );
}

pin_project! {
    /// An unknown-length `PartStream` that yields `total` one-byte parts, then end-of-stream.
    ///
    /// Parts are produced without waiting on a channel, so a test can drive the part count past a
    /// limit without also taking on caller-data backpressure.
    #[derive(Debug)]
    struct CountingUnknownLengthStream {
        next_part_num: u64,
        total: u64,
    }
}

impl PartStream for CountingUnknownLengthStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        if *this.next_part_num > *this.total {
            return Poll::Ready(None);
        }
        let part_number = *this.next_part_num;
        *this.next_part_num += 1;
        Poll::Ready(Some(Ok(PartData::new(
            part_number,
            Bytes::from_static(b"x"),
        ))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// A stream exceeding the S3 part limit fails with a message naming the remedy.
///
/// The guard keys on parts the reader actually yielded, so an exactly-`MAX_PARTS` stream must still
/// succeed — only the part past the limit trips it. Driving this through the public API is what pins
/// the trip point; the part size is irrelevant to the count, so one byte per part suffices.
#[tokio::test]
async fn test_unknown_length_exceeding_part_limit_fails() {
    const MAX_PARTS: u64 = 10_000;

    let tm = MultipartMock::new().tm();

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(CountingUnknownLengthStream {
            next_part_num: 1,
            total: MAX_PARTS + 1,
        }))
        .initiate()
        .unwrap();

    let err = handle
        .join()
        .await
        .expect_err("a stream past the part limit must fail");

    assert_eq!(ErrorKind::InputInvalid, *err.kind());
    let msg = format!(
        "{}",
        aws_smithy_types::error::display::DisplayErrorContext(&err)
    );
    assert!(
        msg.contains("maximum of 10000 parts"),
        "error must name the part limit and the remedy, got: {msg}"
    );
}

/// A stream of exactly `MAX_PARTS` must upload, not trip the limit.
///
/// End-of-stream on a custom stream is only observed on the dispatch after the last part, so a guard
/// keyed on dispatches rather than parts actually yielded would reject this legitimate object.
#[tokio::test]
async fn test_unknown_length_exactly_max_parts_succeeds() {
    const MAX_PARTS: u64 = 10_000;

    let tm = MultipartMock::new().tm();

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(CountingUnknownLengthStream {
            next_part_num: 1,
            total: MAX_PARTS,
        }))
        .initiate()
        .unwrap();

    handle
        .join()
        .await
        .expect("a stream of exactly the maximum part count must upload");
}

/// Returns the error a failed numbered-part upload reported, as a message.
fn part_number_failure_message(error: &Error) -> String {
    assert_eq!(ErrorKind::InputInvalid, *error.kind());
    format!(
        "{}",
        aws_smithy_types::error::display::DisplayErrorContext(error)
    )
}

/// A part number outside S3's range fails the upload before that part is sent.
///
/// 0 and 10,001 are outside the range. 2^32 + 1 is too, and narrowing it to 32 bits would place
/// the part at position 1.
#[tokio::test]
async fn test_part_numbers_outside_range_fail_before_upload_part() {
    for rejected in [0u64, 10_001, (1 << 32) + 1] {
        let (result, mock) = upload_numbered_parts(&[rejected]).await;

        let error = result.expect_err("an unusable part number must fail the upload");
        let message = part_number_failure_message(&error);
        assert!(
            message.contains(&format!("part number {rejected}")),
            "error must name the rejected part number, got: {message}"
        );
        assert!(
            mock.record().upload_parts().is_empty(),
            "part number {rejected} must not be sent"
        );
        assert!(
            mock.record().completions().is_empty(),
            "part number {rejected} must not complete the upload"
        );
    }
}

/// A part number used twice fails the upload before CompleteMultipartUpload.
///
/// Two parts at one position would describe an object neither produced. The repeat is found when
/// the part list is assembled, so both parts reach UploadPart; what must not happen is completing
/// the upload over that list.
#[tokio::test]
async fn test_repeated_part_numbers_fail_before_completing_the_upload() {
    let (result, mock) = upload_numbered_parts(&[1, 1]).await;

    let error = result.expect_err("a repeated part number must fail the upload");
    let message = part_number_failure_message(&error);
    assert!(
        message.contains("part number 1 more than once"),
        "error must name the repeated part number, got: {message}"
    );
    assert!(
        mock.record().completions().is_empty(),
        "a repeated part number must not complete the upload"
    );
}

/// Part numbers within S3's range reach UploadPart and CompleteMultipartUpload unchanged, with each
/// part's data, whatever order the stream yields them in.
#[tokio::test]
async fn test_part_numbers_in_range_reach_the_wire_in_any_order() {
    for part_numbers in [&[1, 2, 3][..], &[2, 1]] {
        let (result, mock) = upload_numbered_parts(part_numbers).await;
        result.expect("unique part numbers within range must upload");

        let mut expected: Vec<(i32, i64)> = part_numbers
            .iter()
            .enumerate()
            .map(|(position, &part_number)| (part_number as i32, position as i64 + 1))
            .collect();
        expected.sort_unstable();
        let mut upload_parts = mock.record().upload_parts();
        upload_parts.sort_unstable();
        assert_eq!(expected, upload_parts, "part numbers {part_numbers:?}");

        let mut listed: Vec<i32> = part_numbers.iter().map(|&n| n as i32).collect();
        listed.sort_unstable();
        assert_eq!(
            vec![listed],
            mock.record().completed_part_numbers(),
            "CompleteMultipartUpload must list every part in part-number order"
        );
    }
}

/// A positive lower bound rejects an empty source without synthesizing a part.
///
/// Empty-part synthesis is based on whether zero satisfies the declared bounds, not merely whether
/// a size hint exists.
#[tokio::test]
async fn test_positive_lower_bound_never_synthesizes_empty_part() {
    let mock = MultipartMock::new();
    let tm = mock.tm();

    // Declare a positive exact size and then reach EOF without yielding a part.
    let (tx, rx) = mpsc::channel(1);
    let declared = 5 * ByteUnit::Mebibyte.as_bytes_u64();
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(TestStream::exact(
            rx, declared,
        )))
        .initiate()
        .unwrap();

    drop(tx);
    let error = handle
        .join()
        .await
        .expect_err("an empty stream below its declared lower bound must fail");

    assert_eq!(*error.kind(), ErrorKind::InputInvalid);
    // Split the recorded parts by body size so the empty ones' count is the assertion.
    let (empty_parts, data_parts): (Vec<_>, Vec<_>) = mock
        .record()
        .upload_parts()
        .into_iter()
        .partition(|&(_, content_length)| content_length == 0);
    assert_eq!(
        0,
        empty_parts.len(),
        "a positive lower bound must prevent empty-part synthesis"
    );
    assert_eq!(0, data_parts.len());
    assert_eq!(0, mock.record().complete_calls());
}

// --- UploadPart responses ----------------------------------------------------

/// Uploads parts 1 to 3, where `part_two`, a rule matching only part 2's UploadPart, answers it
/// without a usable ETag. Checks that the upload fails with a `ServiceError` reporting no ETag
/// for part 2, without re-sending part 2 or completing, and returns the upload's error.
async fn upload_failing_on_part_two(part_two: Rule) -> Error {
    let mock = MultipartMock::new().with_override(&part_two);
    let tm = mock.tm();

    let error = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(NumberedPartStream::new(&[
            1, 2, 3,
        ])))
        .initiate()
        .unwrap()
        .join()
        .await
        .expect_err("a part without an ETag must fail the upload");

    assert_eq!(ErrorKind::ServiceError, *error.kind());
    let message = format!(
        "{}",
        aws_smithy_types::error::display::DisplayErrorContext(&error)
    );
    assert!(
        message.contains("UploadPart returned no ETag for part 2"),
        "error must name the operation and part, got: {message}"
    );
    assert_eq!(1, part_two.num_calls(), "part 2 must not be re-sent");
    assert_eq!(0, mock.record().complete_calls());
    error
}

/// Uploads parts 1 to 3, where the UploadPart response for part 2 carries `part_two_e_tag`, and
/// checks that the upload fails without re-sending part 2 or completing.
async fn assert_upload_fails_on_part_two_e_tag(part_two_e_tag: Option<&'static str>) {
    let part_two = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests(|req| req.part_number() == Some(2))
        .then_output(move || {
            UploadPartOutput::builder()
                .set_e_tag(part_two_e_tag.map(str::to_owned))
                .build()
        });
    upload_failing_on_part_two(part_two).await;
}

/// A successful UploadPart response without an ETag fails the upload before
/// CompleteMultipartUpload, which could not list the part.
#[tokio::test]
async fn test_upload_part_without_e_tag_fails_upload() {
    assert_upload_fails_on_part_two_e_tag(None).await;
}

/// An empty ETag identifies no part, and is treated as a missing one.
#[tokio::test]
async fn test_upload_part_with_empty_e_tag_fails_upload() {
    assert_upload_fails_on_part_two_e_tag(Some("")).await;
}

/// The error for an UploadPart response without an ETag carries the operation name and the
/// response's request ids, as an error from a failed UploadPart does.
#[tokio::test]
async fn test_upload_part_without_e_tag_error_carries_request_ids() {
    const REQUEST_ID: &str = "upload-part-request-id";
    const EXTENDED_REQUEST_ID: &str = "upload-part-extended-request-id";
    // The output builder cannot set request ids, so part 2 is answered with a raw 200 response
    // that carries them and no ETag.
    let part_two = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests(|req| req.part_number() == Some(2))
        .then_http_response(|| {
            let mut response =
                HttpResponse::new(StatusCode::try_from(200).unwrap(), SdkBody::empty());
            response
                .headers_mut()
                .insert("x-amz-request-id", REQUEST_ID);
            response
                .headers_mut()
                .insert("x-amz-id-2", EXTENDED_REQUEST_ID);
            response
        });

    let error = upload_failing_on_part_two(part_two).await;

    assert_eq!(Some("UploadPart"), error.operation_name());
    assert_eq!(Some(REQUEST_ID), error.request_id());
    assert_eq!(Some(EXTENDED_REQUEST_ID), error.extended_request_id());
    let display = error.to_string();
    assert!(
        display.contains(REQUEST_ID),
        "Display must include the request id, got: {display}"
    );
}

// --- Request body ------------------------------------------------------------

/// An upload without a body fails at `initiate()` and sends nothing, rather than storing an empty
/// object over the key. This holds whether the body was never set or was set to `None`.
#[tokio::test]
async fn test_upload_without_body_fails_at_initiate() {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
    let tm = tm_with(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]));

    let never_set = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .content_type("text/plain")
        .initiate()
        .expect_err("an upload with no body must not start");
    assert_eq!(ErrorKind::InputInvalid, *never_set.kind());

    let set_to_none = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .set_body(None)
        .initiate()
        .expect_err("an upload whose body was set to None must not start");
    assert_eq!(ErrorKind::InputInvalid, *set_to_none.kind());

    assert_eq!(0, put_object.num_calls());
}

/// An explicitly empty body uploads an empty object with one PutObject.
#[tokio::test]
async fn test_explicit_empty_body_uploads_empty_object() {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(|req| req.body().bytes() == Some(&b""[..]))
        .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
    let tm = tm_with(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]));

    tm.upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_static(b""))
        .initiate()
        .unwrap()
        .join()
        .await
        .expect("an explicitly empty body must upload an empty object");
    assert_eq!(1, put_object.num_calls());
}

// --- Conditional-write preconditions -----------------------------------------
//
// Assertion shape: check the header on the request the mock received, so a
// missing/wrong precondition fails the test rather than silently passing. A
// PutObject rule uses `match_requests`, which fails the join; the multipart path
// reads the `MultipartRecord`.

/// A single-PUT upload with `if_none_match("*")` must forward the header to
/// `PutObject`. Satisfied case: the mock rule matches, upload succeeds.
#[tokio::test]
async fn test_put_object_forwards_if_none_match() {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(|req| req.if_none_match() == Some("*"))
        .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
    let tm = tm_with(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]));

    tm.upload()
        .bucket("test-bucket")
        .key("test-key")
        .if_none_match("*")
        .body(InputStream::from(vec![0u8; 1024]))
        .initiate()
        .unwrap()
        .join()
        .await
        .expect("PutObject must carry the caller's If-None-Match header");
}

/// When S3 returns 412 on the single-PUT path, the transfer must fail with the
/// service code reachable — that is how a caller tells a failed `If-Match` from
/// any other service error. The 412 must also be terminal: neither the SDK's own
/// retry (412 is a non-retryable 4xx) nor the TM's outer retry loop may re-issue
/// it, so the request is sent exactly once.
#[tokio::test]
async fn test_put_object_412_surfaces_precondition_failed_code() {
    let put_object = mock!(aws_sdk_s3::Client::put_object).then_http_response(|| {
        HttpResponse::new(
            StatusCode::try_from(412).unwrap(),
            SdkBody::from("<Error><Code>PreconditionFailed</Code></Error>"),
        )
    });
    let tm = tm_with(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]));

    let err = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .if_match("\"stale-etag\"")
        .body(InputStream::from(vec![0u8; 1024]))
        .initiate()
        .unwrap()
        .join()
        .await
        .expect_err("a 412 on PutObject must fail the upload");
    assert!(
        matches!(err.kind(), ErrorKind::ServiceError),
        "expected ErrorKind::ServiceError, got {:?}",
        err.kind()
    );
    assert_eq!(
        err.code(),
        Some("PreconditionFailed"),
        "caller must be able to identify the precondition failure by service code"
    );
    // The underlying SdkError is preserved as the source for callers who want
    // the raw 412 detail.
    let src = std::error::Error::source(&err).expect("source is set");
    assert!(
        src.downcast_ref::<SdkError<PutObjectError, HttpResponse>>()
            .is_some(),
        "source should be the underlying PutObject SdkError"
    );
    // A precondition failure is deterministic — it must not be retried.
    assert_eq!(
        put_object.num_calls(),
        1,
        "PutObject 412 must be issued exactly once (no SDK or TM retry)"
    );
}

/// The multipart path must forward `if_match` / `if_none_match` to
/// `CompleteMultipartUpload` (and only there — never on `CreateMultipartUpload`
/// or `UploadPart`, where S3 doesn't accept them and mixing them in would
/// silently drop the header at build time).
#[tokio::test]
async fn test_complete_mpu_forwards_if_match() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let content_length = 2 * part_size;
    let expected_etag = "\"expected-etag\"";

    let mock = MultipartMock::new();
    let tm = mock.tm();

    let (tx, rx) = mpsc::channel(2);
    let stream = TestStream::exact(rx, content_length as u64);

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .if_match(expected_etag)
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    drop(tx);
    handle.join().await.expect("upload should succeed");
    let completions = mock.record().completions();
    let if_match: Vec<_> = completions
        .iter()
        .map(CompleteMultipartUploadInput::if_match)
        .collect();
    assert_eq!(
        vec![Some(expected_etag)],
        if_match,
        "CompleteMultipartUpload must carry the caller's If-Match header"
    );
}

/// When S3 rejects `CompleteMultipartUpload` with 412, the multipart path must
/// surface the same service code the single-PUT path does. Mirrors that test but
/// exercises the MPU code path (which fails at a different site than PutObject).
#[tokio::test]
async fn test_complete_mpu_412_surfaces_precondition_failed_code() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let content_length = 2 * part_size;

    let complete_mpu =
        mock!(aws_sdk_s3::Client::complete_multipart_upload).then_http_response(|| {
            HttpResponse::new(
                StatusCode::try_from(412).unwrap(),
                SdkBody::from("<Error><Code>PreconditionFailed</Code></Error>"),
            )
        });
    let tm = MultipartMock::new().with_override(&complete_mpu).tm();

    let (tx, rx) = mpsc::channel(2);
    let stream = TestStream::exact(rx, content_length as u64);

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .if_match("\"stale-etag\"")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    tx.send(Bytes::from(vec![0u8; part_size])).await.unwrap();
    drop(tx);
    let err = handle
        .join()
        .await
        .expect_err("a 412 on CompleteMultipartUpload must fail the upload");
    assert!(
        matches!(err.kind(), ErrorKind::ServiceError),
        "expected ErrorKind::ServiceError, got {:?}",
        err.kind()
    );
    assert_eq!(
        err.code(),
        Some("PreconditionFailed"),
        "caller must be able to identify the precondition failure by service code"
    );
    let src = std::error::Error::source(&err).expect("source is set");
    assert!(
        src.downcast_ref::<SdkError<CompleteMultipartUploadError, HttpResponse>>()
            .is_some(),
        "source should be the underlying CompleteMultipartUpload SdkError"
    );
}
