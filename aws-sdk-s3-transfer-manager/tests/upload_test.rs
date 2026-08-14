/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp;
use std::sync::Arc;
use std::task::ready;
use std::{task::Poll, time::Duration};

use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use bytes::Bytes;
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
        content_len: usize,
        size_hint: u64,
        observed_part_size: Arc<AtomicUsize>,
    }
}

impl TestStream {
    fn new(rx: mpsc::Receiver<Bytes>, content_len: usize, size_hint: u64) -> Self {
        Self {
            next_part_num: 1,
            rx,
            content_len,
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
        SizeHint::exact(self.size_hint)
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

fn mock_s3_client_for_multipart_upload() -> aws_sdk_s3::Client {
    let upload_id = "test-upload-id".to_owned();

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    let upload_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests({
            let upload_id = upload_id.clone();
            move |input| input.upload_id.as_ref() == Some(&upload_id)
        })
        .then_output(|| UploadPartOutput::builder().build());

    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests({
            let upload_id = upload_id.clone();
            move |r| r.upload_id.as_ref() == Some(&upload_id)
        })
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    )
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
// PartStream deadlock: 200 uploads consume all concurrency slots blocking on poll_part(),
// starving transfers that have data ready.
#[ignore = "PartStream deadlock — workers block in execute waiting for user data, consuming all slots"]
#[tokio::test]
async fn test_many_uploads_no_deadlock() {
    let (_guard, _rx) = capture_test_logs();
    let client = mock_s3_client_for_multipart_upload();
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();

    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let mut transfers = Vec::with_capacity(MANY_ASYNC_UPLOADS_CNT);
    for i in 0..MANY_ASYNC_UPLOADS_CNT {
        let (tx, rx) = mpsc::channel(1);
        let stream = TestStream::new(
            rx,
            MANY_ASYNC_UPLOADS_OBJECT_SIZE,
            MANY_ASYNC_UPLOADS_OBJECT_SIZE as u64,
        );

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
    let client = mock_s3_client_for_multipart_upload();
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();

    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let (tx, rx) = mpsc::channel(1);
    let size_hint = 100 * ByteUnit::Gibibyte.as_bytes_u64();
    let stream = TestStream::new(rx, 0, size_hint);
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
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // Two full parts plus a partial one, so the total is not a part-size multiple
    // and a stale part-count-derived value would not match.
    let content_length = 2 * part_size + 1024;

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    let upload_part =
        mock!(aws_sdk_s3::Client::upload_part).then_output(|| UploadPartOutput::builder().build());

    // Match only when MpuObjectSize equals the full content length. With the
    // field absent (or wrong) no rule matches and the upload fails, so the
    // assertion below is what pins the behavior.
    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests(move |req| req.mpu_object_size() == Some(content_length as i64))
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    );

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let (tx, rx) = mpsc::channel(1);
    let stream = TestStream::new(rx, content_length, content_length as u64);

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-mpu-object-size")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();

    drop(tx);
    handle
        .join()
        .await
        .expect("upload should succeed: CompleteMPU must carry MpuObjectSize = content length");
}

// --- Unknown content length (RUST-1228) --------------------------------------
//
// An unknown-length source is always a `PartStream`, which is `is_mpu_only`, so
// these all exercise the multipart path. Termination comes from the reader
// reporting end-of-stream rather than from a part count. See
// `docs/design/unknown-length-upload.md`.

/// A stream with no declared length uploads via multipart and completes.
/// Previously this panicked on `content_length required`.
#[tokio::test]
async fn test_unknown_length_multipart_upload() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let client = mock_s3_client_for_multipart_upload();
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

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

/// An empty unknown-length stream must complete as a normal 0-byte object.
///
/// S3 rejects a CompleteMultipartUpload that lists no parts, so "no bytes" cannot
/// mean "no parts": the transfer synthesizes a single empty part 1. The mock
/// asserts exactly that shape — one UploadPart, part number 1, zero bytes.
#[tokio::test]
async fn test_unknown_length_empty_stream() {
    let upload_id = "test-upload-id".to_owned();

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    // Matches only a zero-length part 1; anything else leaves the rule unproven.
    let upload_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests(|req| req.part_number() == Some(1) && req.content_length() == Some(0))
        .then_output(|| UploadPartOutput::builder().e_tag("empty-etag").build());

    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests(|req| req.mpu_object_size() == Some(0))
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[&create_mpu, &upload_part, &complete_mpu]
    );
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let (tx, rx) = mpsc::channel(1);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    // Empty: end-of-stream without ever sending data.
    drop(tx);

    handle
        .join()
        .await
        .expect("an empty unknown-length stream must complete as a 0-byte object");

    // Exactly one part, so CompleteMPU cannot have carried a duplicate part 1.
    assert_eq!(
        upload_part.num_calls(),
        1,
        "empty stream must upload exactly one (empty) part"
    );
}

/// `MpuObjectSize` for an unknown-length upload is the sum of the bytes actually
/// uploaded. With no declared length there is no independent witness, but the
/// value must still be sent so S3 can reject a mismatched assembly.
#[tokio::test]
async fn test_unknown_length_mpu_object_size_is_running_sum() {
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // Deliberately not a part-size multiple, so a part-count-derived value wouldn't match.
    let total = 2 * part_size + 1024;

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });
    let upload_part =
        mock!(aws_sdk_s3::Client::upload_part).then_output(|| UploadPartOutput::builder().build());
    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests(move |req| req.mpu_object_size() == Some(total as i64))
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    );
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

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

    handle.join().await.expect(
        "CompleteMPU must carry MpuObjectSize equal to the summed bytes of an unknown-length stream",
    );
}

/// A known-length upload must keep sending its *declared* size as `MpuObjectSize`,
/// not a sum accumulated from the parts it uploaded.
///
/// The declared length is an independent witness: it comes from the source rather
/// than from this crate's own part accounting, so it also catches a part the crate
/// itself dropped or duplicated. A running sum cannot. Unifying the two paths onto
/// one accumulator would look like a simplification and would quietly lose that,
/// so this pins the known-length path against that refactor.
#[tokio::test]
async fn test_known_length_sends_declared_size_not_running_sum() {
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let declared = 2 * part_size;

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });
    // Drop every part on the floor: no part response contributes an ETag, so a
    // sum-derived MpuObjectSize would be wrong while the declared one stays right.
    let upload_part =
        mock!(aws_sdk_s3::Client::upload_part).then_output(|| UploadPartOutput::builder().build());
    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests(move |req| req.mpu_object_size() == Some(declared as i64))
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    );
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let (tx, rx) = mpsc::channel(2);
    let stream = TestStream::new(rx, declared, declared as u64);
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
    drop(tx);

    handle
        .join()
        .await
        .expect("a known-length upload must send its declared content length as MpuObjectSize");
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
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    // Split the part rules by body size so the empty one's call count is the assertion.
    let empty_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests(|req| req.content_length() == Some(0))
        .then_output(|| UploadPartOutput::builder().e_tag("empty-etag").build());
    let data_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests(|req| req.content_length() != Some(0))
        .then_output(|| UploadPartOutput::builder().e_tag("data-etag").build());

    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[&create_mpu, &empty_part, &data_part, &complete_mpu]
    );
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

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

    assert_eq!(
        1,
        data_part.num_calls(),
        "the single data part must be uploaded once"
    );
    assert_eq!(
        0,
        empty_part.num_calls(),
        "a stream that yielded data must not also send an empty part 1"
    );
}
