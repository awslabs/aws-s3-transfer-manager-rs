/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download integration tests.

use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, RuntimeMode};

use crate::harness::{mock_tm, mock_tm_with, MockTm};

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
}

async fn setup_concurrent(part_size: usize, concurrency: usize) -> MockTm {
    mock_tm_with(RuntimeMode::Managed, |b| {
        b.part_size(PartSize::Target(part_size as u64))
            .concurrency(ConcurrencyMode::Explicit(concurrency))
    })
    .await
}

async fn setup_with_part_size(part_size: usize) -> MockTm {
    setup_concurrent(part_size, 1).await
}

/// Helper to drain download body
async fn drain_body(
    handle: &mut aws_sdk_s3_transfer_manager::operation::download::DownloadHandle,
) -> Result<Vec<u8>, aws_sdk_s3_transfer_manager::error::Error> {
    let mut result = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        let chunk = chunk?;
        result.extend_from_slice(&chunk.data.to_vec());
    }
    Ok(result)
}

/// Test basic download with multiple ranges and verify data integrity
#[tokio::test]
async fn test_download_basic() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_with_part_size(part_size).await;

    // Create test data that spans multiple parts (12MB = 3 parts at 5MB)
    let content: Vec<u8> = (0..12 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect();
    let expected = content.clone();

    m.server
        .add_object("test-bucket", "test-key", content, None)
        .await
        .expect("add object");

    let mut handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    let body = drain_body(&mut handle).await.expect("download body");

    assert_eq!(body.len(), expected.len(), "size mismatch");
    assert_eq!(body, expected, "data integrity check failed");

    m.handle.shutdown().await.expect("shutdown");
}

/// Test that handle can be dropped without consuming body
#[tokio::test]
async fn test_download_body_not_consumed() {
    let m = setup().await;

    let content = vec![0u8; 16 * ByteUnit::Mebibyte.as_bytes_usize()];
    m.server
        .add_object("test-bucket", "test-key", content, None)
        .await
        .expect("add object");

    let mut handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    // Only consume first chunk, then drop
    let _ = handle.body_mut().next().await;
    drop(handle);

    // If we get here without hanging/panicking, test passes
    m.handle.shutdown().await.expect("shutdown");
}

/// Test abort cancels in-flight work
#[tokio::test]
async fn test_download_abort() {
    let part_size = ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_with_part_size(part_size).await;

    let content = vec![0u8; 25 * ByteUnit::Mebibyte.as_bytes_usize()];
    m.server
        .add_object("test-bucket", "test-key", content, None)
        .await
        .expect("add object");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    // Wait for discovery to complete
    let _ = handle.object_meta().await;

    // Abort before completion
    handle.abort().await;

    // If we get here without hanging, abort worked
    m.handle.shutdown().await.expect("shutdown");
}

/// Test download of non-existent object returns error
#[tokio::test]
async fn test_download_not_found() {
    let m = setup().await;

    let mut handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("non-existent-key")
        .initiate()
        .expect("initiate download");

    let result = drain_body(&mut handle).await;
    assert!(result.is_err(), "should fail for non-existent object");

    m.handle.shutdown().await.expect("shutdown");
}

/// Test object metadata is available after discovery
#[tokio::test]
async fn test_download_object_meta() {
    let m = setup().await;

    let content = vec![42u8; ByteUnit::Mebibyte.as_bytes_usize()];
    m.server
        .add_object("test-bucket", "test-key", content.clone(), None)
        .await
        .expect("add object");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    let meta = handle.object_meta().await.expect("get object meta");
    assert_eq!(
        meta.total_object_size(),
        content.len() as u64,
        "content length should match"
    );

    m.handle.shutdown().await.expect("shutdown");
}

/// Test concurrent downloads
async fn test_download_concurrent(rt: RuntimeMode) {
    let m = mock_tm(rt).await;

    // Add multiple objects directly to server
    for i in 0..5 {
        let content: Vec<u8> = (0..2 * ByteUnit::Mebibyte.as_bytes_usize())
            .map(|j| ((i + j) % 256) as u8)
            .collect();
        m.server
            .add_object(
                "test-bucket",
                &format!("concurrent-key-{}", i),
                content,
                None,
            )
            .await
            .expect("add object");
    }

    // Start concurrent downloads
    let mut handles = Vec::new();
    for i in 0..5 {
        let handle = m
            .client
            .download()
            .bucket("test-bucket")
            .key(format!("concurrent-key-{}", i))
            .initiate()
            .expect("initiate download");
        handles.push((i, handle));
    }

    // Wait for all downloads
    for (i, mut handle) in handles {
        let body = drain_body(&mut handle).await;
        assert!(
            body.is_ok(),
            "download {} should succeed: {:?}",
            i,
            body.err()
        );
    }

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_download_concurrent_mock_gp() {
    test_download_concurrent(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_concurrent_tokio_mt() {
    test_download_concurrent(RuntimeMode::CurrentTokio).await;
}

/// Generate deterministic data using prime 251 to avoid alignment patterns.
fn deterministic_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Test download to file path with concurrent multi-part download (100 MB, 5 MB parts, 8 workers).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "write-to-path-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("write-to-path-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    // No .s3tmp files should remain
    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().to_string_lossy().contains(".s3tmp"))
        .collect();
    assert!(tmp_files.is_empty(), "leftover temp files: {:?}", tmp_files);

    m.handle.shutdown().await.expect("shutdown");
}

/// Test download to caller-provided file handle (50 MB, 5 MB parts, 8 workers).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_file() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    let size = 50 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "write-to-file-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("file_output.dat");
    let file = std::fs::File::create(&file_path).unwrap();

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("write-to-file-key")
        .write_to_file(file)
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&file_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    m.handle.shutdown().await.expect("shutdown");
}

/// Test ranged download to file path (bytes 10000000-59999999 of 100 MB object).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_ranged() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "ranged-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("ranged_output.dat");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("ranged-key")
        .range("bytes=10000000-59999999")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    let expected_len = 59_999_999 - 10_000_000 + 1;
    assert_eq!(written.len(), expected_len, "ranged file size mismatch");
    assert_eq!(
        &written[..],
        &content[10_000_000..=59_999_999],
        "ranged data integrity check failed"
    );

    m.handle.shutdown().await.expect("shutdown");
}

/// Test single-part download to file (2 MB object, 5 MB part size — no range splitting).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_single_part() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    let size = 2 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "single-part-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("single_part.dat");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("single-part-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    m.handle.shutdown().await.expect("shutdown");
}

/// Integrity stress test: 100 MB, 5 MB parts, 16 concurrent workers.
/// Exercises the batched flush path under high concurrency.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_integrity() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 16).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "integrity-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("integrity.dat");

    let handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("integrity-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "byte-for-byte integrity check failed");

    m.handle.shutdown().await.expect("shutdown");
}

// TODO(vnext): integration tests to add
//
// Data integrity:
// - test_download_write_to_path_auto_concurrency: Auto concurrency mode, 50 MB
//   Verifies adaptive controller works end-to-end without crash/deadlock.
//
// Scale:
// - test_download_many_transfers: 100+ concurrent downloads, all complete with
//   correct checksums. Exercises scheduler fairness and slot buffer under load.
// - test_download_whale_and_small: one 200 MB transfer + 50 × 2 MB transfers
//   running simultaneously. Verifies large transfers don't starve small ones.
// - test_mixed_upload_download: concurrent uploads and downloads against same
//   mock server. Exercises scheduler with mixed workload types.
//
// Cancellation:
// - test_download_abort_one_of_many: start 20 transfers, abort 5 mid-flight,
//   verify the other 15 complete with correct data and no temp files leak.
// - test_download_cancel_half: start 100 transfers, cancel 50, verify rest complete.
//
// Scheduler stress:
// - test_download_high_transfer_count_limited_concurrency: 100+ transfers with
//   low explicit concurrency (e.g. 4). Verifies no starvation, all complete.
//
// Infrastructure improvements:
// - Switch all large data assertions from assert_eq! to checksum comparison
//   (e.g. aws-smithy-checksums CRC32) for better failure output and efficiency.
// - Add priority change tests when priority API is exposed on handles.

/// Test downloading an empty (0-byte) object completes without hanging.
#[tokio::test]
async fn test_download_empty_object() {
    let m = setup().await;

    m.server
        .add_object("test-bucket", "empty-key", vec![], None)
        .await
        .expect("add object");

    let mut handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key("empty-key")
        .initiate()
        .expect("initiate download");

    let body = handle.body_mut();
    let mut data = Vec::new();
    while let Some(chunk) = body.next().await {
        data.extend_from_slice(&chunk.unwrap().data.into_bytes());
    }

    assert_eq!(data.len(), 0);
    handle.join().await.expect("join should succeed");
    m.handle.shutdown().await.expect("shutdown");
}

// ── Read-ahead window gate: slow-consumer progress ───────────────────────────
//
// The read-ahead gate bounds issuance to `issued - released < window`. When the
// in-order consumer lags, occupancy fills the window and the gate closes; it must
// reopen as the consumer drains so the transfer completes, never stall permanently.
// (On the stream path `released` advances with the delivery cursor `consumed`.)
// These tests drive the gate against the localhost mock (real HTTP client, real
// concurrent in-flight GETs) with a deliberately slow consumer.
//
// Each test is wrapped in a `timeout` so a stalled gate surfaces as a failure with
// output (run with `RUST_LOG=aws_sdk_s3_transfer_manager::transfer=trace` to see the
// gate-closed line and whether `consumed` is advancing), not as a hung test.

/// A many-part download whose consumer drains slower than the issuer fetches, so the
/// gate repeatedly fills and must reopen. Must complete and return every byte intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_download_slow_consumer_does_not_wedge() {
    use std::time::Duration;
    use tokio::time::timeout;

    let part_size = ByteUnit::Mebibyte.as_bytes_usize();
    // 64 parts at 1 MiB, 16-way concurrency: the issuer runs well ahead of the slow
    // in-order consumer, exercising the gate's reopen path repeatedly.
    let m = setup_concurrent(part_size, 16).await;

    let content: Vec<u8> = (0..64 * part_size).map(|i| (i % 256) as u8).collect();
    let expected = content.clone();
    m.server
        .add_object("test-bucket", "slow-key", content, None)
        .await
        .expect("add object");

    let result = timeout(Duration::from_secs(30), async {
        let mut handle = m
            .client
            .download()
            .bucket("test-bucket")
            .key("slow-key")
            .initiate()
            .expect("initiate download");

        let mut data = Vec::new();
        while let Some(chunk) = handle.body_mut().next().await {
            let chunk = chunk.expect("chunk");
            data.extend_from_slice(&chunk.data.into_bytes());
            // Slow consumer: pull, then pause, holding the in-order cursor back so
            // occupancy fills the window and the gate closes between pulls.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.join().await.expect("join should succeed");
        data
    })
    .await
    .expect("slow-consumer download did not complete within 30s (gate failed to reopen)");

    assert_eq!(result.len(), expected.len(), "size mismatch");
    assert_eq!(result, expected, "data integrity check failed");
    m.handle.shutdown().await.expect("shutdown");
}

/// A disk download whose part count exceeds the read-ahead window must complete.
///
/// The disk consumer drains via the block surface, which frees runs and does not
/// advance the in-order delivery cursor. The read-ahead gate must release occupancy on
/// that drain, or issuance latches shut at the window and the transfer wedges with the
/// buffer drained to empty. A small `ReadAhead::Parts` window makes the object exceed
/// it cheaply (no multi-GiB object); the window here spans more than one segment so
/// segments seal and drain whole. Under a timeout so the wedge fails fast instead of
/// hanging.
#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_download_to_disk_exceeding_read_ahead_window_does_not_wedge() {
    use aws_sdk_s3_transfer_manager::types::ReadAhead;
    use std::time::Duration;
    use tokio::time::timeout;

    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    // 40 parts (200 MiB) against a window of 32 (Parts(31) → 31 speculative + 1
    // demand): the gate fills and must reopen on disk drains. The window spans two
    // 16-part segments, so a segment seals and drains while issuance is gated — the
    // path that wedged before the gate counted disk drains. Kept modest: the 5 MiB
    // minimum part size makes larger objects costly on the test filesystem.
    let size = 40 * part_size;
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "disk-window-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let written = timeout(Duration::from_secs(30), async {
        let handle = m
            .client
            .download()
            .bucket("test-bucket")
            .key("disk-window-key")
            .read_ahead(ReadAhead::Parts(31))
            .write_to_path(&dest_path)
            .await
            .unwrap();
        handle.join().await.unwrap();
        std::fs::read(&dest_path).unwrap()
    })
    .await
    .expect("disk download did not complete within 30s (gate failed to reopen on drain)");

    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");
    m.handle.shutdown().await.expect("shutdown");
}

/// Disk download with a read-ahead window *below* the buffer's segment size — the
/// relief regime. Here a segment can never fill within the window, so the drain batch
/// drops below the segment size and segments drain as partial contiguous runs rather
/// than whole. This exercises the sub-segment drain path (and proves it does not wedge:
/// a window that cannot seal a segment must still drain and reopen the gate). A small
/// `ReadAhead::Parts(2)` window against a multi-segment object keeps it cheap. The
/// positioned writes must still reassemble the object byte-for-byte under a timeout.
#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_download_to_disk_below_segment_window_drains_in_runs() {
    use aws_sdk_s3_transfer_manager::types::ReadAhead;
    use std::time::Duration;
    use tokio::time::timeout;

    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let m = setup_concurrent(part_size, 8).await;

    // 40 parts against a window of 3 (Parts(2) → 2 speculative + 1 demand), far below
    // the 16-part segment. No segment ever seals within the window, so every drain is a
    // partial run; the gate must still reopen on each run drain or the transfer wedges.
    let size = 40 * part_size;
    let content = deterministic_data(size);
    m.server
        .add_object("test-bucket", "disk-subseg-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let written = timeout(Duration::from_secs(30), async {
        let handle = m
            .client
            .download()
            .bucket("test-bucket")
            .key("disk-subseg-key")
            .read_ahead(ReadAhead::Parts(2))
            .write_to_path(&dest_path)
            .await
            .unwrap();
        handle.join().await.unwrap();
        std::fs::read(&dest_path).unwrap()
    })
    .await
    .expect("sub-segment-window disk download did not complete within 30s (gate failed to reopen on a partial-run drain)");

    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");
    m.handle.shutdown().await.expect("shutdown");
}

// ── Global memory budget: concurrent disk transfers below the drain batch ─────
//
// The memory budget is fungible across all transfers. A disk transfer's chunk
// reservation releases only when the drain copies it out, and a non-terminal drain
// fires only at the drain batch (16 parts) or a full segment. Concurrent multi-part
// disk transfers each below that batch hold their parts resident until terminal —
// which needs every part issued, which needs budget. Under a shared budget too tight
// for all of them at once, none could finalize/drain/release: a global stall. The fix
// flushes a transfer's resident run before it parks on the budget. This drives the
// exact wedge from the public API (real HTTP, real scheduler, real disk writes) and
// asserts every transfer completes with bytes intact.

/// Several concurrent multi-part downloads to disk, each below the drain batch, sharing
/// a budget too small to hold them all at once, must all complete. A regression
/// re-wedges and the timeout fires.
#[cfg(any(unix, windows))]
async fn test_concurrent_disk_downloads_under_tight_budget_do_not_wedge(rt: RuntimeMode) {
    use aws_sdk_s3_transfer_manager::types::MemoryBudgetConfig;
    use std::time::Duration;
    use tokio::time::timeout;

    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let parts_per_object = 6; // multi-part, below the 16-part drain batch
    let object_size = parts_per_object * part_size;
    let transfers = 4;

    // The budget accounting chunk is 8 MiB, so a 5 MiB part costs one chunk. The four
    // objects together need `transfers * parts_per_object` = 24 chunks; cap at 8 — far
    // below that, so no object can hold its full part count and the pre-fix wedge is
    // reachable, while leaving room for a few resident parts per transfer.
    let budget_bytes = 8 * 8 * ByteUnit::Mebibyte.as_bytes_usize();

    let m = mock_tm_with(rt, |b| {
        b.part_size(PartSize::Target(part_size as u64))
            .concurrency(ConcurrencyMode::Explicit(transfers))
            .memory_budget(MemoryBudgetConfig::Limit(budget_bytes))
    })
    .await;

    let mut expected = Vec::with_capacity(transfers);
    for i in 0..transfers {
        let content = deterministic_data(object_size);
        m.server
            .add_object("test-bucket", &format!("obj-{i}"), content.clone(), None)
            .await
            .expect("add object");
        expected.push(content);
    }

    let dir = tempfile::tempdir().unwrap();
    let results = timeout(Duration::from_secs(30), async {
        let mut tasks = Vec::with_capacity(transfers);
        for i in 0..transfers {
            let tm = m.client.clone();
            let dest = dir.path().join(format!("out-{i}.dat"));
            tasks.push(tokio::spawn(async move {
                let handle = tm
                    .download()
                    .bucket("test-bucket")
                    .key(format!("obj-{i}"))
                    .write_to_path(&dest)
                    .await
                    .expect("initiate download");
                handle.join().await.expect("join download");
                std::fs::read(&dest).expect("read output")
            }));
        }
        let mut out = Vec::with_capacity(transfers);
        for t in tasks {
            out.push(t.await.expect("task join"));
        }
        out
    })
    .await
    .expect(
        "concurrent disk downloads did not complete within 30s \
         (budget deadlock: resident parts pinned across transfers)",
    );

    for (i, (got, want)) in results.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got.len(), want.len(), "obj-{i} size mismatch");
        assert_eq!(got, want, "obj-{i} data integrity check failed");
    }
    m.handle.shutdown().await.expect("shutdown");
}

#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_disk_downloads_under_tight_budget_do_not_wedge_mock_gp() {
    test_concurrent_disk_downloads_under_tight_budget_do_not_wedge(RuntimeMode::Managed).await;
}

#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_disk_downloads_under_tight_budget_do_not_wedge_tokio_mt() {
    test_concurrent_disk_downloads_under_tight_budget_do_not_wedge(RuntimeMode::CurrentTokio).await;
}
