/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integration tests.

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::RuntimeMode;

use crate::harness::{mock_tm, MockTm};

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
}

/// Every single-object upload entry point that can take a sink must actually report.
///
/// `upload` was the one operation of the four with no event coverage at any level — the
/// download side had three entry points tested and both composites had per-object tests, while
/// `client.upload().events(sink)` was exercised by nothing. That is the blind spot that let
/// `write_to_file` ship a builder method which accepted a sink and discarded it, so this closes
/// it on the direction where the same defect would have been equally silent.
///
/// `InitiateWith` covers `UploadInputBuilder::initiate_with_events`. Its plain sibling
/// `initiate_with` builds a fresh fluent builder and copies only the input, so a sink was
/// structurally unreachable through it.
///
/// Asserted per arm: exactly one `Decided` carrying a view, exactly one `Settled`, and the
/// view reporting the payload on `network_tx` — the upload numerator — against a `Final` total
/// the leaf knows from its own size hint before a byte moves.
#[tokio::test]
async fn test_upload_single_object_entry_points_all_report_events() {
    use aws_sdk_s3_transfer_manager::events::TransferEvent;
    use aws_sdk_s3_transfer_manager::types::ByteTotal;
    use std::time::Duration;

    #[derive(Debug)]
    enum Entry {
        Fluent,
        InitiateWith,
    }

    // Two parts at the 8 MiB default, so the transfer is a real MPU rather than a single PUT.
    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();

    for variant in [Entry::Fluent, Entry::InitiateWith] {
        let m = setup().await;
        let content = vec![7u8; size];

        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(8).expect("capacity > 0"),
        );

        let handle = match variant {
            Entry::Fluent => m
                .client
                .upload()
                .bucket("test-bucket")
                .key("ev-upload-key")
                .body(InputStream::from(content))
                .events(sink)
                .initiate()
                .expect("initiate"),
            Entry::InitiateWith => {
                aws_sdk_s3_transfer_manager::operation::upload::UploadInput::builder()
                    .bucket("test-bucket")
                    .key("ev-upload-key")
                    .body(InputStream::from(content))
                    .initiate_with_events(&m.client, sink)
                    .expect("initiate_with_events")
            }
        };

        handle.join().await.expect("join upload");

        // `Settled` comes from `on_terminal`, which the scheduler runs after `join()` has
        // already returned, so this polls to a deadline rather than awaiting stream
        // termination — the handle is gone but the crate's sink clone is released later.
        let mut decided = 0usize;
        let mut settled = 0usize;
        let mut view = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while settled == 0 && tokio::time::Instant::now() < deadline {
            match stream.try_next() {
                Ok(TransferEvent::Decided {
                    parent: None,
                    view: v,
                    ..
                }) => {
                    decided += 1;
                    view = v;
                }
                Ok(TransferEvent::Settled { parent: None, .. }) => settled += 1,
                Ok(_) => continue,
                Err(aws_sdk_s3_transfer_manager::events::TryNextError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(_) => break,
            }
        }

        assert_eq!(
            1, decided,
            "{variant:?}: a registered sink must receive exactly one Decided"
        );
        assert_eq!(1, settled, "{variant:?}: and exactly one Settled");
        let view = view.unwrap_or_else(|| panic!("{variant:?}: a real transfer owes a view"));
        assert_eq!(
            ByteTotal::Final(size as u64),
            view.byte_total(),
            "{variant:?}: a known-length body gives the leaf a final total up front"
        );
        assert_eq!(
            size as u64,
            view.metrics().network_tx,
            "{variant:?}: upload's numerator is network_tx, and it must reach the payload"
        );

        m.handle.shutdown().await.expect("shutdown");
    }
}

/// A successful empty-object upload must still be observable as having moved zero bytes.
///
/// The SEP makes progress being reported *"at least once for a successful transfer"* a MUST,
/// and it does not condition that on the count being non-zero. An empty object is a successful
/// transfer: S3 accepts a zero-length PUT, and `aws s3 sync` of a tree containing an empty file
/// must not report that file as never having been acted on.
///
/// What this rules out: a 0-byte entry that a consumer cannot distinguish from one that never
/// started. The evidence has to be readable rather than inferable — a bar drawn from
/// `network_tx / byte_total()` is `0 / 0` for this transfer, so the entry's own `Decided`,
/// its `Settled { Succeeded }`, and a `Final(0)` denominator are what say "this happened and
/// moved nothing" instead of "nothing happened".
#[tokio::test]
async fn test_upload_empty_object_is_observable_as_zero_bytes() {
    use aws_sdk_s3_transfer_manager::events::{Outcome, TransferEvent};
    use aws_sdk_s3_transfer_manager::types::ByteTotal;
    use std::time::Duration;

    let m = setup().await;

    let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
        std::num::NonZeroUsize::new(16).expect("capacity > 0"),
    );

    let handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("empty-object-key")
        .body(InputStream::from(Vec::new()))
        .events(sink)
        .initiate()
        .expect("initiate");

    handle.join().await.expect("join upload");

    // `Settled` is emitted from `on_terminal`, which the scheduler runs after `join()` has
    // returned, so drain to a deadline rather than waiting for the stream to end.
    let mut view = None;
    let mut settled = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match stream.try_next() {
            Ok(TransferEvent::Decided { view: v, .. }) => view = v,
            Ok(TransferEvent::Settled { outcome, .. }) => {
                settled = Some(outcome);
                break;
            }
            Ok(_) => continue,
            Err(aws_sdk_s3_transfer_manager::events::TryNextError::Empty) => {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(_) => break,
        }
    }
    m.handle.shutdown().await.expect("shutdown");

    let outcome = settled.expect("a zero-length PUT succeeds, so a terminal must arrive");
    assert!(
        matches!(outcome, Outcome::Succeeded { .. }),
        "a zero-length PUT succeeds; got {outcome:?}"
    );

    let view =
        view.expect("the entry must announce itself with a view, or there is nothing to read");
    assert_eq!(
        0,
        view.metrics().network_tx,
        "an empty body moves no payload bytes"
    );
    assert_eq!(
        ByteTotal::Final(0),
        view.byte_total(),
        "the reading must be *reported*, not merely absent -- `Final(0)` is what \
         distinguishes a transfer that moved nothing from one whose total is still unknown"
    );
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
