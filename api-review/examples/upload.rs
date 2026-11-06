//! Upload a single object.
//!
//! Each function is the code a customer writes for one task. `main` runs them against a local S3
//! endpoint and prints what happened.

#[path = "support/mock.rs"]
mod mock;

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use aws_sdk_s3_transfer_manager::Client;
use aws_sdk_s3_transfer_manager::io::adapters::TokioIo;
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::operation::upload::{ChecksumStrategy, UploadOutput};
use aws_sdk_s3_transfer_manager::types::{AbortedUpload, TransferStatus};
use bytes::Bytes;
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const MIB: usize = 1024 * 1024;

/// Upload bytes already in memory.
async fn upload_bytes(client: &Client, key: &str, data: Vec<u8>) -> Result<UploadOutput, BoxError> {
    let handle = client
        .upload()
        .bucket(BUCKET)
        .key(key)
        .body(InputStream::from(data))
        .initiate()?;
    Ok(handle.join().await?)
}

/// Upload a file.
async fn upload_file(client: &Client, key: &str, path: &Path) -> Result<UploadOutput, BoxError> {
    let body = InputStream::from_path(path)?;
    let handle = client.upload().bucket(BUCKET).key(key).body(body).initiate()?;
    Ok(handle.join().await?)
}

/// Upload a byte range of a file.
async fn upload_file_range(
    client: &Client,
    key: &str,
    path: &Path,
    offset: u64,
    length: u64,
) -> Result<UploadOutput, BoxError> {
    let body = InputStream::read_from()
        .path(path)
        .offset(offset)
        .length(length)
        .build()?;
    let handle = client.upload().bucket(BUCKET).key(key).body(body).initiate()?;
    Ok(handle.join().await?)
}

/// Upload from any `AsyncRead` whose length isn't known up front.
async fn upload_unknown_length(
    client: &Client,
    key: &str,
    reader: impl tokio::io::AsyncRead + Send + Sync + 'static,
) -> Result<UploadOutput, BoxError> {
    let body = InputStream::from_part_stream(TokioIo::new(reader, SizeHint::default()));
    let handle = client.upload().bucket(BUCKET).key(key).body(body).initiate()?;
    Ok(handle.join().await?)
}

/// A producer that already has its data split into parts, for example chunks from another service.
struct ChunkProducer {
    chunks: std::vec::IntoIter<Bytes>,
    next_part: u64,
    total: u64,
}

impl PartStream for ChunkProducer {
    fn poll_part(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let Some(chunk) = self.chunks.next() else {
            return Poll::Ready(None);
        };
        let part = PartData::new(self.next_part, chunk).mark_last(self.chunks.len() == 0);
        self.next_part += 1;
        Poll::Ready(Some(Ok(part)))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.total)
    }
}

/// Upload from a custom part producer.
async fn upload_part_stream(
    client: &Client,
    key: &str,
    chunks: Vec<Bytes>,
) -> Result<UploadOutput, BoxError> {
    let total = chunks.iter().map(|c| c.len() as u64).sum();
    let stream = ChunkProducer { chunks: chunks.into_iter(), next_part: 1, total };
    let body = InputStream::from_part_stream(stream);
    let handle = client.upload().bucket(BUCKET).key(key).body(body).initiate()?;
    Ok(handle.join().await?)
}

/// Upload with object metadata, a chosen checksum, and a create-only condition.
async fn upload_with_options(client: &Client, key: &str) -> Result<UploadOutput, BoxError> {
    let handle = client
        .upload()
        .bucket(BUCKET)
        .key(key)
        .body(InputStream::from("report contents"))
        .content_type("text/plain")
        .metadata("owner", "api-review")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32_c())
        .if_none_match("*")
        .initiate()?;
    Ok(handle.join().await?)
}

/// Read what the upload produced.
fn describe(output: &UploadOutput) -> String {
    format!(
        "e_tag={:?} checksum_type={:?} crc64nvme={:?} crc32c={:?} upload_id={:?} request_id={:?}",
        output.e_tag(),
        output.checksum_type(),
        output.checksum_crc64_nvme(),
        output.checksum_crc32_c(),
        output.upload_id(),
        output.request_id(),
    )
}

/// Start an upload, adjust its priority, watch it, then abort it.
async fn control_and_abort(client: &Client, key: &str) -> Result<AbortedUpload, BoxError> {
    // A producer that never sends data, standing in for a slow source.
    let (_writer, reader) = tokio::io::duplex(64);
    let body = InputStream::from_part_stream(TokioIo::new(reader, SizeHint::default()));
    let handle = client.upload().bucket(BUCKET).key(key).body(body).initiate()?;

    handle.scheduling().set_priority(200);
    assert_eq!(handle.status(), TransferStatus::Active);
    let metrics = handle.metrics();
    println!("  in flight: network_tx={} total_bytes={:?}", metrics.network_tx, metrics.total_bytes);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(handle.abort().await?)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mock = mock::Mock::start().await;
    let client = mock.client().await;
    let dir = std::env::temp_dir().join(format!("tm-api-review-upload-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let file = dir.join("source.bin");
    std::fs::write(&file, vec![7u8; 20 * MIB])?;

    let out = upload_bytes(&client, "bytes.bin", vec![1u8; 1024]).await?;
    println!("upload_bytes:          {}", describe(&out));
    let out = upload_file(&client, "file.bin", &file).await?;
    println!("upload_file:           {}", describe(&out));
    let out = upload_file_range(&client, "range.bin", &file, 1024, 6 * MIB as u64).await?;
    println!("upload_file_range:     {}", describe(&out));
    let out = upload_unknown_length(&client, "stream.bin", std::io::Cursor::new(vec![3u8; 9 * MIB])).await?;
    println!("upload_unknown_length: {}", describe(&out));
    let chunks = vec![Bytes::from(vec![5u8; 8 * MIB]), Bytes::from(vec![6u8; MIB])];
    let out = upload_part_stream(&client, "parts.bin", chunks).await?;
    println!("upload_part_stream:    {}", describe(&out));
    let out = upload_with_options(&client, "report.txt").await?;
    println!("upload_with_options:   {}", describe(&out));
    match upload_with_options(&client, "report.txt").await {
        Ok(_) => println!("upload_with_options (again): unexpectedly succeeded"),
        Err(e) => println!("upload_with_options (again): {e}"),
    }
    let aborted = control_and_abort(&client, "aborted.bin").await?;
    println!(
        "control_and_abort:     outcome={:?} upload_id={:?}",
        aborted.outcome(),
        aborted.upload_id()
    );

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
