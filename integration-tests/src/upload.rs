/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integration tests.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{PartSize, RuntimeMode};
use tokio::sync::mpsc;

use crate::harness::{mock_tm, mock_tm_with, MockTm};

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
}

#[tokio::test]
async fn test_mpu_upload_small_file() {
    let m = setup().await;

    let content = vec![0u8; 16 * ByteUnit::Mebibyte.as_bytes_usize()]; // 16MB = 2 parts at 8MB default
    let expected_content = content.clone();

    let upload_handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    let result = upload_handle.join().await.expect("upload complete");
    assert!(result.e_tag().is_some(), "should have etag");
    assert!(
        result.upload_id().is_some(),
        "should have upload_id for MPU"
    );

    let s3_client = m.handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("test-key")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(body.to_vec(), expected_content);

    m.handle.shutdown().await.expect("shutdown");
}

async fn test_mpu_upload_concurrent(rt: RuntimeMode) {
    let m = mock_tm(rt).await;

    let mut handles = Vec::new();

    // Start multiple concurrent uploads
    for i in 0..5 {
        let content = vec![i as u8; 8 * ByteUnit::Mebibyte.as_bytes_usize()];
        let key = format!("concurrent-key-{}", i);

        let upload_handle = m
            .client
            .upload()
            .bucket("test-bucket")
            .key(&key)
            .body(InputStream::from(content))
            .initiate()
            .expect("initiate upload");

        handles.push((key, upload_handle));
    }

    // Wait for all uploads to complete
    for (key, handle) in handles {
        let result = handle.join().await;
        assert!(
            result.is_ok(),
            "upload {} should succeed: {:?}",
            key,
            result
        );
    }

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_mpu_upload_concurrent_mock_gp() {
    test_mpu_upload_concurrent(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpu_upload_concurrent_tokio_mt() {
    test_mpu_upload_concurrent(RuntimeMode::MultiThreadTokio).await;
}

#[tokio::test]
async fn test_upload_verify_data_integrity() {
    let m = setup().await;

    // Create content with recognizable pattern
    let content: Vec<u8> = (0..24 * ByteUnit::Mebibyte.as_bytes_usize()) // 24MB = 3 parts
        .map(|i| (i % 256) as u8)
        .collect();
    let expected_content = content.clone();

    let upload_handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("integrity-test")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    upload_handle.join().await.expect("upload complete");

    let s3_client = m.handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("integrity-test")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(
        body.to_vec(),
        expected_content,
        "data integrity check failed"
    );

    m.handle.shutdown().await.expect("shutdown");
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

    let m = mock_tm_with(RuntimeMode::Managed, |b| {
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

    let stored = m
        .handle
        .client()
        .await
        .get_object()
        .bucket("test-bucket")
        .key("ordered-parts")
        .send()
        .await
        .expect("get object")
        .body
        .collect()
        .await
        .expect("collect body")
        .to_vec();
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
