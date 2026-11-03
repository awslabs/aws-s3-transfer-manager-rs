/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integration tests.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3_transfer_manager::config::S3ClientConfig;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::PartSize;
use aws_sdk_s3_transfer_manager::RuntimeConfig;
use aws_smithy_runtime_api::client::orchestrator::Metadata;
use s3_mock_server::S3MockServer;
use tokio::sync::mpsc;

use crate::assertions::assert_same_content;
use crate::harness::{mock_tm, mock_tm_with, MockTm};
use crate::test_data::{deterministic_data, deterministic_data_seeded};

async fn setup() -> MockTm {
    mock_tm(RuntimeConfig::default()).await
}

/// A multipart upload of patterned data spanning several parts reports an ETag and an upload ID,
/// and stores exactly the source bytes.
#[tokio::test]
async fn test_mpu_upload_stores_source_bytes() {
    let m = setup().await;

    // 24 MiB = 3 parts at the default 8 MiB part size.
    let content = deterministic_data(24 * ByteUnit::Mebibyte.as_bytes_usize());

    let upload_handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from(content.clone()))
        .initiate()
        .expect("initiate upload");

    let result = upload_handle.join().await.expect("upload complete");
    assert!(result.e_tag().is_some(), "should have etag");
    assert!(
        result.upload_id().is_some(),
        "should have upload_id for MPU"
    );

    let stored = m.stored_object("test-bucket", "test-key").await;
    assert_same_content(&content, &stored);

    m.handle.shutdown().await.expect("shutdown");
}

async fn test_mpu_upload_concurrent(rt: RuntimeConfig) {
    let m = mock_tm(rt).await;

    let mut uploads = Vec::new();

    // Start multiple concurrent uploads, each with its own seeded data. Each object is exactly the
    // default multipart threshold, so every upload is a two-part multipart upload and the
    // threshold boundary itself is covered.
    for i in 0..5 {
        let content = deterministic_data_seeded(16 * ByteUnit::Mebibyte.as_bytes_usize(), i);
        let key = format!("concurrent-key-{}", i);

        let upload_handle = m
            .client
            .upload()
            .bucket("test-bucket")
            .key(&key)
            .body(InputStream::from(content.clone()))
            .initiate()
            .expect("initiate upload");

        uploads.push((key, content, upload_handle));
    }

    // Wait for all uploads to complete, and check each stored its own data
    for (key, content, handle) in uploads {
        let output = handle
            .join()
            .await
            .unwrap_or_else(|e| panic!("upload {key} should succeed: {e:?}"));
        assert!(
            output.upload_id().is_some(),
            "upload {key} at the multipart threshold must be a multipart upload"
        );
        let stored = m.stored_object("test-bucket", &key).await;
        assert_same_content(&content, &stored);
    }

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_mpu_upload_concurrent_mock_gp() {
    test_mpu_upload_concurrent(RuntimeConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpu_upload_concurrent_tokio_mt() {
    test_mpu_upload_concurrent(RuntimeConfig::multi_thread_tokio()).await;
}

/// Part size for the part-order tests, set on the client so that every part but the last is
/// exactly one part long.
const ORDERED_PART_SIZE: usize = 5 * ByteUnit::Mebibyte.as_bytes_usize();

/// Number of parts in the part-order tests.
const ORDERED_PART_COUNT: u64 = 8;

/// Length of the last part: shorter than a part, and odd, so the object does not end on a part or
/// power-of-two boundary.
const LAST_PART_LEN: usize = ByteUnit::Mebibyte.as_bytes_usize() + 17;

/// Arrival order for the fixed-order test. It is not part-number order, and the short last part
/// arrives third.
const FIXED_ARRIVAL: [u64; ORDERED_PART_COUNT as usize] = [5, 2, 8, 1, 7, 3, 6, 4];

/// A `PartStream` that yields parts in the order its channel receives them.
///
/// Models a caller whose parts finish in arbitrary order: each part is sent when it is ready,
/// carrying its own part number, and the stream ends once every sender is dropped.
struct ChannelParts {
    rx: mpsc::Receiver<PartData>,
    /// The exact total length of the parts.
    size_hint: SizeHint,
}

impl PartStream for ChannelParts {
    fn poll_part(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        self.get_mut().rx.poll_recv(cx).map(|part| part.map(Ok))
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }
}

/// Length of part `part_number` in the part-order tests.
fn ordered_part_len(part_number: u64) -> usize {
    if part_number == ORDERED_PART_COUNT {
        LAST_PART_LEN
    } else {
        ORDERED_PART_SIZE
    }
}

/// The bytes of part `part_number` in the part-order tests.
///
/// Byte `i` is `(part_number * 31 + i * 7) as u8`, so a part stored at another part's position,
/// shifted, or truncated no longer matches.
fn ordered_part_bytes(part_number: u64) -> Vec<u8> {
    (0..ordered_part_len(part_number))
        .map(|i| (part_number as usize * 31 + i * 7) as u8)
        .collect()
}

/// The object the part-order tests must store: parts `1..=ORDERED_PART_COUNT` concatenated in
/// part-number order.
fn ordered_object() -> Vec<u8> {
    (1..=ORDERED_PART_COUNT)
        .flat_map(ordered_part_bytes)
        .collect()
}

/// Sends part `part_number` to a `ChannelParts` stream.
///
/// A closed channel means the upload has already failed and dropped its stream. `join()` reports
/// that failure, so the part is discarded.
async fn send_ordered_part(tx: &mpsc::Sender<PartData>, part_number: u64) {
    let part = PartData::new(part_number, ordered_part_bytes(part_number));
    let _ = tx.send(part).await;
}

/// Asserts that `actual` equals `expected`, an object made of the part-order tests' parts.
///
/// Compares lengths first. On a byte mismatch, names the first differing offset and the part it
/// falls in, rather than printing the buffers.
fn assert_ordered_content(expected: &[u8], actual: &[u8]) {
    assert_eq!(expected.len(), actual.len(), "stored object length");
    if let Some(offset) = expected.iter().zip(actual).position(|(e, a)| e != a) {
        panic!(
            "stored object differs from the parts in part-number order at byte {offset}: \
             byte {} of part {}",
            offset % ORDERED_PART_SIZE,
            offset / ORDERED_PART_SIZE + 1,
        );
    }
}

/// Uploads the parts `produce` sends through a `ChannelParts` stream, then checks that the stored
/// object is the parts concatenated in part-number order.
///
/// `produce` is given the only sender. It must send each of parts `1..=ORDERED_PART_COUNT` once,
/// in any order, and drop every sender when done. The expected object is built before the upload
/// starts.
async fn assert_assembled_in_part_number_order<F, Fut>(produce: F)
where
    F: FnOnce(mpsc::Sender<PartData>) -> Fut,
    Fut: Future<Output = ()>,
{
    let expected = ordered_object();

    let m = mock_tm_with(RuntimeConfig::default(), |b| {
        b.part_size(PartSize::Target(ORDERED_PART_SIZE as u64))
    })
    .await;
    let (tx, rx) = mpsc::channel(1);
    let upload = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("ordered-parts")
        .body(InputStream::from_part_stream(ChannelParts {
            rx,
            size_hint: SizeHint::exact(expected.len() as u64),
        }))
        .initiate()
        .expect("initiate upload");

    let (result, ()) = tokio::join!(upload.join(), produce(tx));
    result.expect("parts arriving in any order must upload");

    let stored = m.stored_object("test-bucket", "ordered-parts").await;
    assert_ordered_content(&expected, &stored);

    m.handle.shutdown().await.expect("shutdown");
}

/// Parts that arrive in a fixed order other than part-number order, with the short last part
/// arriving third, are stored in part-number order.
#[tokio::test]
async fn test_parts_in_fixed_out_of_order_arrival_assemble_by_part_number() {
    assert_assembled_in_part_number_order(|tx| async move {
        for part_number in FIXED_ARRIVAL {
            send_ordered_part(&tx, part_number).await;
        }
    })
    .await;
}

/// Parts sent by concurrent producers, one part each, arrive in whatever order the producers
/// finish and are stored in part-number order.
#[tokio::test(flavor = "multi_thread")]
async fn test_parts_from_concurrent_producers_assemble_by_part_number() {
    assert_assembled_in_part_number_order(|tx| async move {
        let producers: Vec<_> = (1..=ORDERED_PART_COUNT)
            .map(|part_number| {
                let tx = tx.clone();
                tokio::spawn(async move { send_ordered_part(&tx, part_number).await })
            })
            .collect();
        drop(tx);
        for producer in producers {
            producer.await.expect("producer task");
        }
    })
    .await;
}

/// A `PartStream` that yields one prebuilt part, then ends.
///
/// The part is complete before the upload starts, so it is held directly instead of arriving
/// through a channel as in [`ChannelParts`].
struct SinglePart {
    /// The part to yield; `None` once it has been yielded.
    part: Option<PartData>,
    /// The exact length of the part.
    size_hint: SizeHint,
}

impl PartStream for SinglePart {
    fn poll_part(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        // First poll returns the part; later polls return `None`, which ends the stream.
        Poll::Ready(self.get_mut().part.take().map(Ok))
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }
}

/// Records the operation name of every request attempt the S3 client sends.
#[derive(Debug, Clone, Default)]
struct OperationRecorder {
    /// Operation names in send order, one entry per attempt.
    seen: Arc<Mutex<Vec<String>>>,
}

impl OperationRecorder {
    /// Number of recorded attempts of `operation`.
    fn count(&self, operation: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|op| *op == operation)
            .count()
    }
}

impl Intercept for OperationRecorder {
    fn name(&self) -> &'static str {
        "OperationRecorder"
    }

    // Runs once per attempt, so SDK retries are counted.
    fn read_before_transmit(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(metadata) = cfg.load::<Metadata>() {
            self.seen.lock().unwrap().push(metadata.name().to_string());
        }
        Ok(())
    }
}

/// Base64 big-endian CRC32 of `data`, the encoding S3 expects in `x-amz-checksum-crc32`.
fn crc32_base64(data: &[u8]) -> String {
    let mut hasher = aws_smithy_checksums::ChecksumAlgorithm::Crc32.into_impl();
    hasher.update(data);
    aws_smithy_types::base64::encode(hasher.finalize())
}

/// A part whose valid CRC32 was calculated over different bytes is rejected by `UploadPart`, and
/// the upload is never completed.
#[tokio::test]
async fn test_part_checksum_over_different_bytes_fails_before_complete() {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");
    server
        .create_bucket("test-bucket")
        .await
        .expect("create bucket");

    // Record each request attempt so the test can show CompleteMultipartUpload is never sent.
    let recorder = OperationRecorder::default();
    let s3 = {
        let base = handle.client().await;
        let conf = base
            .config()
            .to_builder()
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .interceptor(recorder.clone())
            .build();
        aws_sdk_s3::Client::from_conf(conf)
    };
    let tm = aws_sdk_s3_transfer_manager::Client::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .s3_config(S3ClientConfig::new(s3.config().to_builder()))
            .build(),
    );

    // A validly encoded CRC32 of bytes other than the part body.
    let sent = b"bytes the transfer manager transmits".to_vec();
    let checksummed = b"bytes the checksum was computed over".to_vec();
    let part = PartData::new(1, sent.clone()).with_checksum(crc32_base64(&checksummed));

    // The part checksum is sent only when the upload has a checksum strategy.
    let result = tm
        .upload()
        .bucket("test-bucket")
        .key("mismatched-checksum")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from_part_stream(SinglePart {
            part: Some(part),
            size_hint: SizeHint::exact(sent.len() as u64),
        }))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    // The mock rejects the part, nothing retries it, and completion is never attempted.
    let err = result.expect_err("a part checksum that does not match its body must fail");
    assert_eq!(err.operation_name(), Some("UploadPart"), "{err:?}");
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
    assert_eq!(recorder.count("UploadPart"), 1);
    assert_eq!(recorder.count("CompleteMultipartUpload"), 0);

    handle.shutdown().await.expect("shutdown");
}
