#![cfg(e2e_test)]
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
// TODO(vnext): fold these tests into the integration-tests e2e harness (see integration-tests/src/harness.rs Target).
// Tests here requires AWS account with pre-configured S3 bucket to run the tests.
// Refer to https://github.com/awslabs/aws-c-s3/tree/main/tests/test_helper to help set up the S3 in the account
// Set S3_TEST_BUCKET_NAME_RS environment variables to the bucket created.
// By default, it uses aws-s3-transfer-manager-rs-test-bucket
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::PartSize;
use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use test_common::{create_test_dir, drain, global_uuid_str};
use tokio::time::Sleep;

const PUT_OBJECT_PREFIX: &str = "upload";

fn get_bucket_names() -> (String, String) {
    let bucket_name = option_env!("S3_TEST_BUCKET_NAME_RS")
        .unwrap_or("aws-s3-transfer-manager-rs-test-bucket")
        .to_owned();
    let express_bucket_name = format!("{}--usw2-az1--x-s3", bucket_name.as_str());
    (bucket_name, express_bucket_name)
}

fn generate_key(readable_key: &str) -> String {
    format!(
        "{}/{}/{}",
        PUT_OBJECT_PREFIX,
        global_uuid_str(),
        readable_key
    )
}

async fn test_tm() -> (aws_sdk_s3_transfer_manager::Client, aws_sdk_s3::Client) {
    let tm_config = aws_sdk_s3_transfer_manager::from_env()
        .part_size(PartSize::Target(8 * ByteUnit::Mebibyte.as_bytes_u64()))
        .load()
        .await;
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);
    (tm, s3_client)
}

fn create_input_stream(size: usize) -> InputStream {
    "This is a test"
        .bytes()
        .cycle()
        .take(size)
        .collect::<Vec<u8>>()
        .into()
}

async fn perform_upload(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket_name: &str,
    key: &str,
    strategy: Option<ChecksumStrategy>,
    stream: InputStream,
) {
    let mut upload = tm.upload().bucket(bucket_name).key(key).body(stream);

    if let Some(strategy) = strategy {
        upload = upload.checksum_strategy(strategy);
    }

    upload.initiate().unwrap().join().await.unwrap();
}

async fn round_trip_helper(file_size: usize, bucket_name: &str, object_key: &str) {
    let (tm, _) = test_tm().await;
    perform_upload(
        &tm,
        bucket_name,
        object_key,
        None,
        create_input_stream(file_size),
    )
    .await;
    let mut download_handle = tm
        .download()
        .bucket(bucket_name)
        .key(object_key)
        .initiate()
        .unwrap();

    let body = drain(&mut download_handle).await.unwrap();

    assert_eq!(body.len(), file_size);
}

#[tokio::test]
async fn test_single_part_file_round_trip() {
    let _logs = show_test_logs();
    let file_size = 1024 * 1024; // 1MB
    let object_key = generate_key("1MB");
    let (bucket_name, express_bucket_name) = get_bucket_names();
    round_trip_helper(file_size, bucket_name.as_str(), &object_key).await;
    round_trip_helper(file_size, express_bucket_name.as_str(), &object_key).await;
}

#[tokio::test]
async fn test_multi_part_file_round_trip() {
    let _logs = show_test_logs();
    let file_size = 20 * 1024 * 1024; // 20MB
    let object_key = generate_key("20MB");
    let (bucket_name, express_bucket_name) = get_bucket_names();
    round_trip_helper(file_size, bucket_name.as_str(), &object_key).await;
    round_trip_helper(file_size, express_bucket_name.as_str(), &object_key).await;
}

async fn get_object_checksum(
    s3_client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> String {
    s3_client
        .head_object()
        .bucket(bucket_name)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
        .checksum_crc32()
        .unwrap()
        .to_owned()
}

async fn upload_and_get_object_checksum(
    tm: &aws_sdk_s3_transfer_manager::Client,
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    strategy: ChecksumStrategy,
    size: usize,
) -> String {
    perform_upload(tm, bucket, key, Some(strategy), create_input_stream(size)).await;
    get_object_checksum(s3_client, bucket, key).await
}

async fn checksum_test_helper(size_mb: usize, key_suffix: &str, is_multi_part: bool) {
    let file_size = size_mb * 1024 * 1024;
    let object_key = generate_key(key_suffix);
    let (tm, s3_client) = test_tm().await;

    let (bucket_name, express_bucket_name) = get_bucket_names();
    // Test both regular S3 and S3 express
    for bucket in [bucket_name.as_str(), express_bucket_name.as_str()] {
        // First upload: calculated CRC32
        let checksum = upload_and_get_object_checksum(
            &tm,
            &s3_client,
            bucket,
            &object_key,
            ChecksumStrategy::with_calculated_crc32(),
            file_size,
        )
        .await;

        // Second upload: precomputed CRC32
        upload_and_get_object_checksum(
            &tm,
            &s3_client,
            bucket,
            &object_key,
            ChecksumStrategy::with_crc32(&checksum),
            file_size,
        )
        .await;

        // Third upload: composite CRC32
        let composite_key = format!("{}-composite", object_key);
        let composite_checksum = upload_and_get_object_checksum(
            &tm,
            &s3_client,
            bucket,
            &composite_key,
            ChecksumStrategy::with_calculated_crc32_composite_if_multipart(),
            file_size,
        )
        .await;

        if is_multi_part {
            // A multipart upload, so that the composite checksum should ends with `-<number of parts>`
            let num_parts = (size_mb as f64 / 8f64).ceil() as u32;
            assert!(composite_checksum.ends_with(&format!("-{}", num_parts)));
        } else {
            // A single object upload, so that the composite checksum should not contain `-` and matches the checksum before
            assert!(!composite_checksum.contains('-'));
            assert_eq!(composite_checksum, checksum);
        }
    }
}

#[tokio::test]
async fn test_multi_part_file_checksum_upload() {
    let _logs = show_test_logs();
    checksum_test_helper(20, "20MB-crc32", true).await;
}

#[tokio::test]
async fn test_single_part_file_checksum_upload() {
    let _logs = show_test_logs();
    checksum_test_helper(1, "1MB-crc32", false).await;
}

// TODO: add checksum validation tests for get object

#[derive(Debug)]
struct DelayStream {
    idx: usize,
    remaining: usize,
    delay: Option<Pin<Box<Sleep>>>,
}

impl DelayStream {
    fn new(total_size: usize) -> Self {
        let delay = Box::pin(tokio::time::sleep(Duration::from_secs(5)));
        Self {
            idx: 0,
            remaining: total_size,
            delay: Some(delay),
        }
    }
}

impl PartStream for DelayStream {
    fn poll_part(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let part_size = stream_cx.part_size();
        // Check if we need to wait for the delay
        if let Some(delay) = &mut self.delay {
            match delay.as_mut().poll(cx) {
                Poll::Ready(_) => self.delay = None,
                Poll::Pending => return Poll::Pending,
            }
        }

        if self.remaining == 0 {
            // End of stream
            Poll::Ready(None)
        } else {
            let part_data_size = std::cmp::min(part_size, self.remaining);
            let data = "This is a test"
                .bytes()
                .cycle()
                .take(part_data_size)
                .collect::<Vec<u8>>();
            let part = PartData::new((self.idx + 1) as u64, data);

            // Update state
            self.idx += 1;
            self.remaining -= part_data_size;
            // Schedule delay for NEXT part
            self.delay = Some(Box::pin(tokio::time::sleep(Duration::from_secs(5))));
            Poll::Ready(Some(Ok(part)))
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.remaining as u64)
    }
}

// This test is intended to reproduce the error we seen in https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/77
// https://github.com/awslabs/aws-s3-transfer-manager-rs/actions/runs/13317812974/job/37196043327?pr=102 is the failed run
// But it's not guaranteed to reproduce the error yet.
// Also, it sometimes triggers S3 to response 400 and ClientUploadSpeedTooSlow
// https://github.com/awslabs/aws-s3-transfer-manager-rs/actions/runs/13333953491/job/37244814880
// ignore this test as default.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_upload_with_long_running_stream() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let file_size = 10 * 1024 * 1024; // 10MB
    let num_uploads = 10;
    let (bucket_name, express_bucket_name) = get_bucket_names();
    for bucket in [bucket_name.as_str(), express_bucket_name.as_str()] {
        let object_keys: Vec<String> = (0..num_uploads)
            .map(|i| generate_key(i.to_string().as_str()))
            .collect();

        let mut handles = vec![];
        for key in object_keys {
            let stream = DelayStream::new(file_size);

            let upload = tm
                .upload()
                .bucket(bucket)
                .key(key.as_str())
                .body(InputStream::from_part_stream(stream));

            handles.push(upload.initiate().unwrap());
        }
        for handle in handles {
            handle.join().await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_empty_object_download() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();

    let object_key = "pre-existing-empty";

    let mut download_handle = tm
        .download()
        .bucket(bucket_name)
        .key(object_key)
        .initiate()
        .unwrap();

    let body = drain(&mut download_handle).await.unwrap();

    assert_eq!(body.len(), 0);
}

async fn range_download_helper(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key: &str,
    range: &str,
    expected_length: usize,
    description: &str,
) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let mut download_handle = tm
        .download()
        .bucket(bucket)
        .key(key)
        .range(range)
        .initiate()?;

    let body = drain(&mut download_handle).await?;

    assert_eq!(
        body.len(),
        expected_length,
        "desc: {}; range: {}",
        description,
        range
    );
    Ok(())
}

#[tokio::test]
async fn test_object_download_range() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();

    let object_key = "pre-existing-10MB";

    let success_ranges = [
        ("bytes=0-10", "Range is inclusive"),
        ("bytes=0-104857600", "Over the size of the object"),
        (
            "bytes=-104857600",
            "without start means the length to fetch. fetch more than the size of object",
        ),
        (
            "bytes=-10485760",
            "without start means the length to fetch. fetch the whole object",
        ),
        (
            "bytes=-10485759",
            "without start means the length to fetch. fetch the whole object - 1",
        ),
        (
            "bytes=10485759-",
            "start from exact end of the object without end (only 1 byte)",
        ),
        (
            "bytes=1-",
            "start from 1, which will ignore the first byte (index 0).",
        ),
        (
            "bytes=10485759-10485759",
            "exact end of the object (only 1 byte)",
        ),
    ];

    let success_expected_length = [11, 10485760, 10485760, 10485760, 10485759, 1, 10485759, 1];
    // Test success case:
    for i in 0..success_ranges.len() {
        range_download_helper(
            &tm,
            &bucket_name,
            object_key,
            success_ranges[i].0,
            success_expected_length[i],
            success_ranges[i].1,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn test_object_download_range_failures() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();

    let object_key = "pre-existing-10MB";
    let expect_fail_ranges = [
        ("bytes=104857600-", "start over the size of the object"),
        (
            "bytes=10485760-",
            "start over the size of the object, 10MiB -> 10485760, but range starts from 0",
        ),
        (
            "bytes=10485760-104857600",
            "start over the size of the object, 10MiB -> 10485760, but range starts from 0",
        ),
        ("bytes=0-499, -499", "multiple range is not supported."),
    ];
    for i in expect_fail_ranges {
        // The fail case should error out.
        range_download_helper(&tm, &bucket_name, object_key, i.0, 0, i.1)
            .await
            .unwrap_err();
    }
}

#[tokio::test]
async fn test_objects_transfer() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();

    // SSE-C objects require the key to download, skipping it.
    fn sse_c_filter(obj: &aws_sdk_s3::types::Object) -> bool {
        let key = obj.key().unwrap_or("");
        let is_sse_c = key.ends_with("aes256-c");
        !is_sse_c
    }

    let temp_dir = create_test_dir(Some("e2e_downloads"), vec![], &[]);

    // Download all pre-existing objects except the ones with `aes256-c` suffix
    // The objects details can be found https://github.com/awslabs/aws-c-s3/tree/main/tests/test_helper
    let download_handle = tm
        .download_objects()
        .bucket(bucket_name.as_str())
        .key_prefix("pre-existing")
        .walker(
            aws_sdk_s3_transfer_manager::io::walk::S3Walker::builder()
                .filter(sse_c_filter)
                .prefix("pre-existing")
                .build(),
        )
        .destination(temp_dir.path())
        .initiate()
        .unwrap();
    download_handle.join().await.unwrap();

    let file_count = std::fs::read_dir(temp_dir.path())
        .expect("Failed to read directory")
        .map(|entry| {
            let entry = entry.expect("Failed to access directory entry");
            let file_type = entry.file_type().expect("Failed to determine file type");
            assert!(
                file_type.is_file(),
                "Expected only files in directory, but found non-file: {:?}",
                entry.path()
            );
            entry
        })
        .count();

    assert_eq!(
        file_count, 7,
        "Expected exactly 7 files to be downloaded, but found {}",
        file_count
    );

    let upload_handle = tm
        .upload_objects()
        .bucket(bucket_name.as_str())
        .set_key_prefix(Some(generate_key("test")))
        .source(temp_dir.path())
        .initiate()
        .unwrap();
    upload_handle.join().await.unwrap();
}

/// A multipart upload sets `MpuObjectSize = full content length` on
/// CompleteMultipartUpload, and S3 accepts it and stores the correct size —
/// covering the real-S3 half of SEP step 7's server-side enforcement.
///
/// A "S3 rejects a mismatch" negative path is intentionally omitted. The TM
/// upload path derives `MpuObjectSize` from the same content length it uses
/// to size and count the parts it sends, so it cannot naturally emit a value
/// that disagrees with what it uploaded; a negative case would require either
/// a test-only backdoor into production code (leaks a foot-gun into the
/// public surface) or a hand-rolled MPU that bypasses TM entirely (tests
/// S3, not TM). The mock-level tests in `upload_test.rs` and
/// `upload_objects_test.rs` cover the "field must be present and correct on
/// the emitted request" half.
#[tokio::test]
async fn test_multi_part_upload_sends_mpu_object_size() {
    let _logs = show_test_logs();
    let file_size = 20 * 1024 * 1024; // 20 MiB — above the 8 MiB default part size, so multipart
    let object_key = generate_key("mpu-object-size");
    let (bucket_name, express_bucket_name) = get_bucket_names();

    for bucket in [bucket_name.as_str(), express_bucket_name.as_str()] {
        let (tm, s3_client) = test_tm().await;

        perform_upload(
            &tm,
            bucket,
            &object_key,
            None,
            create_input_stream(file_size),
        )
        .await;

        let head = s3_client
            .head_object()
            .bucket(bucket)
            .key(&object_key)
            .send()
            .await
            .expect("HeadObject succeeds after upload");
        assert_eq!(
            head.content_length(),
            Some(file_size as i64),
            "S3 must store the full content length; a divergence would mean either \
             MpuObjectSize was not sent (S3 would still store the wrong size but not \
             flag it) or was wrong and S3 rejected the complete (which surfaces as an \
             upload failure above, not a size mismatch here)",
        );
    }
}

/// A `PartStream` with no size-hint upper bound: the unknown-content-length case.
///
/// Parts are handed over a channel and closing the sender is end-of-stream, which
/// is how a caller like Mountpoint streams an object whose final size is only
/// known once the writer closes.
#[derive(Debug)]
struct UnknownLengthStream {
    next_part_num: u64,
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
}

impl UnknownLengthStream {
    fn new(rx: tokio::sync::mpsc::Receiver<bytes::Bytes>) -> Self {
        Self {
            next_part_num: 1,
            rx,
        }
    }
}

impl PartStream for UnknownLengthStream {
    fn poll_part(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(b)) => {
                let part_num = self.next_part_num;
                self.next_part_num += 1;
                Poll::Ready(Some(Ok(PartData::new(part_num, b))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// No upper bound — routes the upload down the unknown-length path.
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Stream `parts`, one message each, then end-of-stream, as an unknown-length upload;
/// download it back and assert the bytes round-trip exactly. Returns nothing — panics on
/// any mismatch.
///
/// Downloading and comparing the actual bytes (not just the size) is what catches a
/// dropped, duplicated, or reordered part — the failure modes an unknown-length upload is
/// most exposed to, since it has no declared size for S3 to cross-check against.
async fn unknown_length_round_trip(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key: &str,
    parts: Vec<bytes::Bytes>,
) {
    let expected: Vec<u8> = parts.iter().flatten().copied().collect();

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let handle = tm
        .upload()
        .bucket(bucket)
        .key(key)
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    tokio::spawn(async move {
        for part in parts {
            if tx.send(part).await.is_err() {
                return;
            }
        }
        drop(tx); // end of stream
    });

    handle
        .join()
        .await
        .expect("an unknown-length stream must upload successfully");

    let mut download = tm.download().bucket(bucket).key(key).initiate().unwrap();
    let body = drain(&mut download).await.unwrap();

    assert_eq!(
        body.len(),
        expected.len(),
        "downloaded object size must equal the streamed size"
    );
    assert_eq!(
        body, expected,
        "downloaded bytes must match what was streamed, in order"
    );
}

/// A multi-part unknown-length stream round-trips byte-for-byte. The tail part is
/// deliberately not a part-size multiple, so any size taken from a part count rather than
/// the bytes actually read would be wrong.
#[tokio::test]
async fn test_upload_unknown_content_length_multipart() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();
    let key = generate_key("unknown-content-length-multipart");

    let part_size = 8 * ByteUnit::Mebibyte.as_bytes_usize();
    let parts = vec![
        bytes::Bytes::from(vec![b'a'; part_size]),
        bytes::Bytes::from(vec![b'b'; part_size]),
        bytes::Bytes::from(vec![b'c'; 1024]), // partial tail
    ];
    unknown_length_round_trip(&tm, bucket_name.as_str(), key.as_str(), parts).await;
}

/// A single-part unknown-length stream (a small streamed file) round-trips.
///
/// One data part is read while the end-of-stream read runs concurrently, so a spurious empty part 1
/// would surface here as a wrong-sized object.
#[tokio::test]
async fn test_upload_unknown_content_length_single_part() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();
    let key = generate_key("unknown-content-length-single");

    let parts = vec![bytes::Bytes::from(vec![b'x'; 1024])];
    unknown_length_round_trip(&tm, bucket_name.as_str(), key.as_str(), parts).await;
}

/// Many small parts, well past the unknown-length default part-list capacity, round-trip in
/// order. Confirms the growing part list assembles correctly on real S3.
#[tokio::test]
async fn test_upload_unknown_content_length_many_parts() {
    let _logs = show_test_logs();
    let (tm, _) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();
    let key = generate_key("unknown-content-length-many");

    // 40 full parts at the 5 MiB minimum crosses the 32 default capacity. Distinct byte per
    // part so a reordering shows up as a content mismatch, not just a size match.
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let parts: Vec<bytes::Bytes> = (0..40u8)
        .map(|i| bytes::Bytes::from(vec![i; part_size]))
        .collect();
    unknown_length_round_trip(&tm, bucket_name.as_str(), key.as_str(), parts).await;
}

/// An empty stream with no declared content length completes as a normal 0-byte object,
/// with and without a checksum strategy.
///
/// S3 rejects a CompleteMultipartUpload that lists no parts, so the transfer uploads a
/// single empty part. This confirms against real S3 what the mock cannot: that such an
/// upload is accepted, stores a 0-byte object, downloads back to zero bytes, and — with a
/// checksum strategy — that the full-object checksum over zero bytes is accepted.
#[tokio::test]
async fn test_upload_unknown_content_length_empty() {
    let _logs = show_test_logs();
    let (tm, s3_client) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();

    for checksum in [
        None,
        Some(ChecksumStrategy::default()),
        Some(ChecksumStrategy::with_calculated_crc32()),
    ] {
        let key = generate_key("unknown-content-length-empty");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        // Empty: end-of-stream without ever sending a part.
        drop(tx);

        let handle = tm
            .upload()
            .bucket(bucket_name.as_str())
            .key(key.as_str())
            .set_checksum_strategy(checksum.clone())
            .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
            .initiate()
            .unwrap();

        handle
            .join()
            .await
            .expect("an empty unknown-length stream must complete as a 0-byte object");

        let head = s3_client
            .head_object()
            .bucket(bucket_name.as_str())
            .key(key.as_str())
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("uploaded empty object must exist");
        assert_eq!(
            Some(0),
            head.content_length(),
            "an empty stream must store a 0-byte object"
        );

        // And it must download back as zero bytes.
        let mut download = tm
            .download()
            .bucket(bucket_name.as_str())
            .key(key.as_str())
            .initiate()
            .unwrap();
        let body = drain(&mut download).await.unwrap();
        assert!(body.is_empty(), "empty object must download as zero bytes");
    }
}

/// An unknown-length upload that supplies a full-object checksum has it stored on the
/// object, verified against real S3 (the mock only checks the header is sent, not that S3
/// accepts and records it).
#[tokio::test]
async fn test_upload_unknown_content_length_with_checksum() {
    let _logs = show_test_logs();
    let (tm, s3_client) = test_tm().await;
    let (bucket_name, _) = get_bucket_names();
    let key = generate_key("unknown-content-length-checksum");

    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let handle = tm
        .upload()
        .bucket(bucket_name.as_str())
        .key(key.as_str())
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from_part_stream(UnknownLengthStream::new(rx)))
        .initiate()
        .unwrap();

    tokio::spawn(async move {
        for _ in 0..2 {
            let _ = tx.send(bytes::Bytes::from(vec![b'k'; part_size])).await;
        }
        drop(tx);
    });

    handle
        .join()
        .await
        .expect("an unknown-length upload with a checksum strategy must succeed");

    let head = s3_client
        .head_object()
        .bucket(bucket_name.as_str())
        .key(key.as_str())
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("uploaded object must exist");
    assert!(
        head.checksum_crc32().is_some(),
        "S3 must have stored the CRC32 checksum for an unknown-length upload"
    );
}
