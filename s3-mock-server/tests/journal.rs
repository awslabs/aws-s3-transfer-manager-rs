// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! The request journal, byte-positioned faults and body holds, observed through SDK requests.

use std::time::Duration;

use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, ChecksumType};
use bytes::Bytes;
use s3_mock_server::{
    AddObjectRequest, BodyEnding, ChecksumScope, FaultTarget, FaultType, HoldEnd, Occurrence,
    Operation, RequestRecord, S3MockServer, ServerHandle,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const B: &str = "journal";
const K: &str = "obj";
const MIB: usize = 1 << 20;

fn data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn server_with(len: usize) -> Result<(S3MockServer, ServerHandle, Vec<u8>)> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .with_request_journal()
        .build()?;
    let handle = server.start().await?;
    server.create_bucket(B).await?;
    let body = data(len);
    server
        .add_object_with(
            B,
            K,
            AddObjectRequest::new(body.clone()).checksum_algorithms([ChecksumAlgorithm::Crc32]),
        )
        .await?;
    Ok((server, handle, body))
}

/// A GET of `range` (a `Range` header value) or, if `None`, of part 1, reading the body to the
/// end. Checksum validation is left to the caller: these tests read the bytes the server sent.
async fn get(handle: &ServerHandle, range: Option<&str>) -> Result<Vec<u8>> {
    get_with(&unvalidating_client(handle).await, range).await
}

/// A client that does not validate response checksums or retry, so a test reads the bytes and
/// errors the server sent.
async fn unvalidating_client(handle: &ServerHandle) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::from(&handle.shared_config().await)
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

async fn get_with(client: &aws_sdk_s3::Client, range: Option<&str>) -> Result<Vec<u8>> {
    let request = client.get_object().bucket(B).key(K);
    let request = match range {
        Some(range) => request.range(range),
        None => request.part_number(1),
    };
    let response = request.send().await?;
    Ok(response.body.collect().await?.to_vec())
}

fn gets(server: &S3MockServer) -> Vec<RequestRecord> {
    server
        .requests()
        .into_iter()
        .filter(|r| r.operation == Operation::GetObject)
        .collect()
}

/// Waits until every GET in the journal has a recorded body ending.
async fn bodies_ended(server: &S3MockServer) -> Vec<RequestRecord> {
    for _ in 0..200 {
        let gets = gets(server);
        if gets.iter().all(|r| r.body.is_some()) {
            return gets;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("bodies did not end: {:#?}", gets(server));
}

#[tokio::test]
async fn corrupt_byte_flips_the_covered_byte_of_range_and_part_number_gets() -> Result<()> {
    let (server, handle, body) = server_with(4096).await?;
    server.insert_fault_for(
        FaultTarget::object(B, K).covering(1000),
        FaultType::CorruptByte { offset: 1000 },
        0,
        Occurrence::Always,
    );

    // A range without the byte is untouched.
    assert_eq!(get(&handle, Some("bytes=0-999")).await?, body[..1000]);
    // A range with it has it flipped, at its offset in the range.
    let got = get(&handle, Some("bytes=900-1099")).await?;
    let mut expected = body[900..1100].to_vec();
    expected[100] ^= 0xFF;
    assert_eq!(got, expected);
    // So does a partNumber GET of the part that holds it.
    let got = get(&handle, None).await?;
    let mut expected = body.clone();
    expected[1000] ^= 0xFF;
    assert_eq!(got, expected);

    let gets = bodies_ended(&server).await;
    let ranges: Vec<_> = gets.iter().map(|r| r.range.clone()).collect();
    assert_eq!(ranges, [Some(0..1000), Some(900..1100), Some(0..4096)]);
    assert!(gets[0].fault.is_none());
    assert!(matches!(
        gets[1].fault,
        Some(FaultType::CorruptByte { offset: 1000 })
    ));
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn journal_records_how_each_body_ended() -> Result<()> {
    let (server, handle, _) = server_with(16 * MIB).await?;
    get(&handle, Some("bytes=0-1023")).await?;

    server.insert_fault_for(
        FaultTarget::object(B, K).covering(0),
        FaultType::TruncateBody { after_bytes: 100 },
        0,
        Occurrence::Once,
    );
    assert!(get(&handle, Some("bytes=0-8388607")).await.is_err());

    // A body the client drops before reading it.
    let response = handle
        .client()
        .await
        .get_object()
        .bucket(B)
        .key(K)
        .send()
        .await?;
    drop(response);

    let endings: Vec<_> = bodies_ended(&server)
        .await
        .iter()
        .map(|r| r.body.expect("ended").ending)
        .collect();
    assert_eq!(
        endings,
        [
            BodyEnding::Sent,
            BodyEnding::CutByFault,
            BodyEnding::EndedEarly
        ]
    );
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn journal_records_the_checksum_each_response_carried() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .with_request_journal()
        .build()?;
    let handle = server.start().await?;
    server.create_bucket(B).await?;
    let s3 = handle.client().await;
    let upload = s3
        .create_multipart_upload()
        .bucket(B)
        .key(K)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(ChecksumType::Composite)
        .send()
        .await?;
    let upload_id = upload.upload_id().expect("upload id");
    let mut parts = Vec::new();
    for (n, len) in [(1, 5 * MIB), (2, 1024)] {
        let part = s3
            .upload_part()
            .bucket(B)
            .key(K)
            .upload_id(upload_id)
            .part_number(n)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(Bytes::from(data(len)).into())
            .send()
            .await?;
        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(n)
                .e_tag(part.e_tag().expect("etag"))
                .checksum_crc32(part.checksum_crc32().expect("part checksum"))
                .build(),
        );
    }
    s3.complete_multipart_upload()
        .bucket(B)
        .key(K)
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await?;
    server.clear_requests();

    let ranged = |range: &str| {
        s3.get_object()
            .bucket(B)
            .key(K)
            .range(range)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
    };
    ranged(&format!("bytes=0-{}", 5 * MIB - 1))
        .await?
        .body
        .collect()
        .await?;
    ranged("bytes=0-99").await?.body.collect().await?;
    s3.head_object()
        .bucket(B)
        .key(K)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;

    let records = server.requests();
    let sent: Vec<_> = records
        .iter()
        .map(|r| (r.operation, r.checksum.as_ref().map(|c| c.scope)))
        .collect();
    assert_eq!(
        sent,
        [
            (Operation::GetObject, Some(ChecksumScope::Part)),
            (Operation::GetObject, None),
            (Operation::HeadObject, Some(ChecksumScope::Object)),
        ]
    );
    let head = records[2].checksum.as_ref().expect("head checksum");
    assert_eq!(head.algorithm, ChecksumAlgorithm::Crc32);
    assert_eq!(head.checksum_type, Some(ChecksumType::Composite));
    assert_eq!(records[2].range, Some(0..(5 * MIB + 1024) as u64));
    handle.shutdown().await?;
    Ok(())
}

/// Registers a hold on the GET covering byte 0, released when one other body ends.
fn hold_first_range(server: &S3MockServer, deadline: Duration) {
    server.insert_fault_for(
        FaultTarget::object(B, K).covering(0),
        FaultType::HoldBody {
            others: 1,
            deadline,
        },
        0,
        Occurrence::Once,
    );
}

#[tokio::test]
async fn hold_releases_when_other_bodies_end() -> Result<()> {
    let (server, handle, body) = server_with(4 * MIB).await?;
    hold_first_range(&server, Duration::from_secs(30));

    let held = tokio::spawn({
        let client = unvalidating_client(&handle).await;
        async move { get_with(&client, Some("bytes=0-1048575")).await }
    });
    // The held body does not end on its own.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!held.is_finished());
    get(&handle, Some("bytes=1048576-2097151")).await?;
    assert_eq!(held.await??, body[..MIB]);

    let gets = bodies_ended(&server).await;
    assert_eq!(gets[0].hold, Some(HoldEnd::Released));
    assert_eq!(gets[0].body.unwrap().ending, BodyEnding::Sent);
    assert!(gets[0].body.unwrap().order > gets[1].body.unwrap().order);
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn hold_releases_when_another_body_is_cut_short() -> Result<()> {
    let (server, handle, _) = server_with(16 * MIB).await?;
    hold_first_range(&server, Duration::from_secs(30));
    server.insert_fault_for(
        FaultTarget::object(B, K).covering(MIB as u64),
        FaultType::TruncateBody { after_bytes: 10 },
        0,
        Occurrence::Once,
    );

    let held = tokio::spawn({
        let client = unvalidating_client(&handle).await;
        async move { get_with(&client, Some("bytes=0-1048575")).await }
    });
    assert!(get(&handle, Some("bytes=1048576-16777215")).await.is_err());
    held.await??;

    // The two GETs are sent concurrently, so either can be admitted first.
    let gets = bodies_ended(&server).await;
    let (held, cut): (Vec<_>, Vec<_>) = gets
        .iter()
        .partition(|r| matches!(r.fault, Some(FaultType::HoldBody { .. })));
    assert_eq!(held[0].hold, Some(HoldEnd::Released));
    assert_eq!(cut[0].body.unwrap().ending, BodyEnding::CutByFault);
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn journal_is_off_unless_requested() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    server.create_bucket(B).await?;
    server.add_object(B, K, data(1024), None).await?;
    get(&handle, Some("bytes=0-99")).await?;
    assert!(server.requests().is_empty());
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn hold_expires_at_its_deadline() -> Result<()> {
    let (server, handle, body) = server_with(4 * MIB).await?;
    hold_first_range(&server, Duration::from_millis(200));

    assert_eq!(get(&handle, Some("bytes=0-1048575")).await?, body[..MIB]);

    let gets = bodies_ended(&server).await;
    assert_eq!(gets[0].hold, Some(HoldEnd::Expired));
    assert_eq!(gets[0].body.unwrap().ending, BodyEnding::Sent);
    handle.shutdown().await?;
    Ok(())
}
