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

use crate::assertions::assert_same_content;
use crate::harness::{mock_tm, mock_tm_with, MockTm};
use crate::test_data::{deterministic_data, deterministic_data_seeded};

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
/// Asserted per arm: exactly one `Planned` carrying a view, exactly one `Ended`, and the
/// view reporting the payload on `network_tx` — the upload numerator — against a `Final` total
/// the leaf knows from its own size hint before a byte moves.
#[tokio::test]
async fn test_upload_single_object_entry_points_all_report_events() {
    use aws_sdk_s3_transfer_manager::events::TransferEvent;
    use aws_sdk_s3_transfer_manager::types::Total;
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

        // `Ended` comes from `on_terminal`, which the scheduler runs after `join()` has
        // already returned, so this polls to a deadline rather than awaiting stream
        // termination — the handle is gone but the crate's sink clone is released later.
        let mut decided = 0usize;
        let mut settled = 0usize;
        let mut view = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while settled == 0 && tokio::time::Instant::now() < deadline {
            match stream.try_next() {
                Ok(TransferEvent::Planned(p)) if p.parent().is_none() => {
                    decided += 1;
                    view = p.view().cloned();
                }
                Ok(TransferEvent::Ended(e)) if e.parent().is_none() => settled += 1,
                Ok(_) => continue,
                Err(aws_sdk_s3_transfer_manager::events::TryNextError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(_) => break,
            }
        }

        assert_eq!(
            1, decided,
            "{variant:?}: a registered sink must receive exactly one Planned"
        );
        assert_eq!(1, settled, "{variant:?}: and exactly one Ended");
        let view = view.unwrap_or_else(|| panic!("{variant:?}: a real transfer owes a view"));
        assert_eq!(
            Total::Final(size as u64),
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
/// Progress must be reported at least once for a successful transfer, whatever the count. An
/// empty object is a successful transfer: S3 accepts a zero-length PUT, and `aws s3 sync` of a
/// tree containing an empty file must not report that file as never having been acted on.
///
/// What this rules out: a 0-byte entry that a consumer cannot distinguish from one that never
/// started. The evidence has to be readable rather than inferable — a bar drawn from
/// `network_tx / byte_total()` is `0 / 0` for this transfer, so the entry's own `Planned`,
/// its `Ended { Succeeded }`, and a `Final(0)` denominator are what say "this happened and
/// moved nothing" instead of "nothing happened".
#[tokio::test]
async fn test_upload_empty_object_is_observable_as_zero_bytes() {
    use aws_sdk_s3_transfer_manager::events::{Outcome, TransferEvent};
    use aws_sdk_s3_transfer_manager::types::Total;
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

    // `Ended` is emitted from `on_terminal`, which the scheduler runs after `join()` has
    // returned, so drain to a deadline rather than waiting for the stream to end.
    let mut view = None;
    let mut settled = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match stream.try_next() {
            Ok(TransferEvent::Planned(p)) => view = p.view().cloned(),
            Ok(TransferEvent::Ended(e)) => {
                settled = Some(e.outcome().clone());
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
        Total::Final(0),
        view.byte_total(),
        "the reading must be *reported*, not merely absent -- `Final(0)` is what \
         distinguishes a transfer that moved nothing from one whose total is still unknown"
    );
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

async fn test_mpu_upload_concurrent(rt: RuntimeMode) {
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
    test_mpu_upload_concurrent(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpu_upload_concurrent_tokio_mt() {
    test_mpu_upload_concurrent(RuntimeMode::MultiThreadTokio).await;
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

    let stored = m.stored_object("test-bucket", "ordered-parts").await;
    assert_ordered_content(&expected, &stored);

    m.handle.shutdown().await.expect("shutdown");
}

/// The drain loop `config.rs` documents for a *request*-level stream must terminate.
///
/// `Config::Builder::events` contrasts the two registration levels: a
/// client-level stream "stays open for as long as the client does", so
/// `while let Some(ev) = stream.next().await` "never returns", "where the same loop over a
/// *request*-level stream ends when that operation does". This pins that second clause, which
/// is the one a caller writes code against.
///
/// Phase 1 is that loop, verbatim, with the handle still held -- which is where a caller is
/// when the loop is their render loop. They cannot have called `join()` yet: `join(self)`
/// consumes the handle, and reaching it is what the loop is supposed to let them do.
///
/// Phases 2 and 3 are controls that say how far the sink's lifetime actually extends, so the
/// failure cannot be read as "you should have joined first": phase 2 re-runs the loop after
/// `join()` has consumed the handle, and phase 3 after the client is gone as well.
#[tokio::test]
async fn test_request_level_event_stream_ends_with_the_operation() {
    use aws_sdk_s3_transfer_manager::events::{Outcome, TransferEvent};
    use std::time::Duration;

    // Two parts at the 8 MiB default, so this is a real MPU, as in the entry-point test above.
    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup().await;

    let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
        std::num::NonZeroUsize::new(64).expect("capacity > 0"),
    );

    let handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("drain-loop-key")
        .body(InputStream::from(vec![9u8; size]))
        .events(sink)
        .initiate()
        .expect("initiate");

    // Phase 1: the documented loop, with the handle still held.
    let mut seen = 0usize;
    let mut succeeded = false;
    let phase1 = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(ev) = stream.next().await {
            seen += 1;
            if let TransferEvent::Ended(e) = &ev {
                if e.parent().is_none() && matches!(e.outcome(), Outcome::Succeeded { .. }) {
                    succeeded = true;
                }
            }
        }
    })
    .await;

    // Phase 2 control: `join` takes the handle by value, so after this nothing the caller
    // holds refers to the transfer.
    handle.join().await.expect("join upload");
    let phase2 = tokio::time::timeout(Duration::from_secs(3), async {
        while stream.next().await.is_some() {}
    })
    .await;

    // Phase 3 control: and the client is the owner of the scheduler and the runtime.
    drop(m.client);
    let phase3 = tokio::time::timeout(Duration::from_secs(3), async {
        while stream.next().await.is_some() {}
    })
    .await;

    m.handle.shutdown().await.expect("shutdown");

    let ended = |r: &Result<(), tokio::time::error::Elapsed>| {
        if r.is_ok() {
            "ended"
        } else {
            "hung"
        }
    };

    assert!(
        succeeded,
        "the upload itself must finish, or phase 1 proves nothing about the loop: \
         saw {seen} events, terminal Succeeded = {succeeded}"
    );
    assert!(
        phase1.is_ok(),
        "`Config::Builder::events` says a request-level stream's `while let Some(ev) = \
         stream.next().await` ends when the operation does. It does not: the upload reached a \
         successful terminal, {seen} events were delivered, and then the loop hung -- so a \
         caller rendering events in that loop never reaches the `join()` that the doc assumes \
         comes after it. Controls: after join() consumed the handle the same loop {}; after \
         drop(client) it {}. A request-level sink's Sender therefore outlives the operation, \
         the handle and the client, which docs/design/transfer-events.md says must not happen \
         (\"a stored sink would hold a stream open past the operation that created it\").",
        ended(&phase2),
        ended(&phase3)
    );
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
