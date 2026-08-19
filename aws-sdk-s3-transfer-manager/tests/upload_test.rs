/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp;
use std::sync::Arc;
use std::task::ready;
use std::{task::Poll, time::Duration};

use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadOutput,
};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3_transfer_manager::error::ErrorKind;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
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

// --- Conditional-write preconditions -----------------------------------------
//
// Assertion shape: use `match_requests` to fail the mock unless the request
// carries the header we expect, so a missing/wrong precondition surfaces as a
// join failure rather than a silent pass.

/// A single-PUT upload with `if_none_match("*")` must forward the header to
/// `PutObject`. Satisfied case: the mock rule matches, upload succeeds.
#[tokio::test]
async fn test_put_object_forwards_if_none_match() {
    let put_object = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(|req| req.if_none_match() == Some("*"))
        .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

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
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&put_object]);
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

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
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let content_length = 2 * part_size;

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

    let expected_etag = "\"expected-etag\"";
    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests(move |req| req.if_match() == Some(expected_etag))
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
        .key("test-key")
        .if_match(expected_etag)
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
    drop(tx);
    handle
        .join()
        .await
        .expect("CompleteMultipartUpload must carry the caller's If-Match header");
}

/// When S3 rejects `CompleteMultipartUpload` with 412, the multipart path must
/// surface the same service code the single-PUT path does. Mirrors that test but
/// exercises the MPU code path (which fails at a different site than PutObject).
#[tokio::test]
async fn test_complete_mpu_412_surfaces_precondition_failed_code() {
    let upload_id = "test-upload-id".to_owned();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let content_length = 2 * part_size;

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
    let complete_mpu =
        mock!(aws_sdk_s3::Client::complete_multipart_upload).then_http_response(|| {
            HttpResponse::new(
                StatusCode::try_from(412).unwrap(),
                SdkBody::from("<Error><Code>PreconditionFailed</Code></Error>"),
            )
        });

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
        .key("test-key")
        .if_match("\"stale-etag\"")
        .body(InputStream::from_part_stream(stream))
        .initiate()
        .unwrap();
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
