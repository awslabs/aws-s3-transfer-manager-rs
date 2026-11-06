//! Download a single object.
//!
//! Each function is the code a customer writes for one task. `main` runs them against a local S3
//! endpoint and prints what happened. Functions named `probe_*` aren't customer tasks: they check a
//! specific behavior the review describes.

#[path = "support/mock.rs"]
mod mock;

use std::path::Path;
use std::time::Duration;

use aws_sdk_s3_transfer_manager::Client;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::operation::download::{DownloadInput, DownloadInputBuilder, DownloadOutput};
use aws_sdk_s3_transfer_manager::types::{ChecksumMode, ReadAhead};
use bytes::{Bytes, BytesMut};
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const MIB: usize = 1024 * 1024;

/// Download an object into memory, chunk by chunk.
async fn download_to_memory(client: &Client, key: &str) -> Result<Bytes, BoxError> {
    let mut handle = client.download().bucket(BUCKET).key(key).initiate()?;
    let mut contents = BytesMut::new();
    while let Some(chunk) = handle.body_mut().next().await {
        for segment in chunk?.data.into_segments() {
            contents.extend_from_slice(&segment);
        }
    }
    handle.join().await?;
    Ok(contents.freeze())
}

/// Download an object to a file path.
async fn download_to_path(client: &Client, key: &str, path: &Path) -> Result<DownloadOutput, BoxError> {
    let handle = client.download().bucket(BUCKET).key(key).write_to_path(path).await?;
    Ok(handle.join().await?)
}

/// Download to a path and report progress while the transfer manager writes the file.
async fn download_to_path_with_progress(
    client: &Client,
    key: &str,
    path: &Path,
) -> Result<(DownloadOutput, Vec<(u64, u64)>), BoxError> {
    let handle = client.download().bucket(BUCKET).key(key).write_to_path(path).await?;
    let size = handle.object_meta().await?.total_object_size();
    handle.scheduling().set_priority(64);

    let mut progress = Vec::new();
    while !handle.status().is_terminal() {
        let metrics = handle.metrics();
        progress.push((metrics.network_rx * 100 / size.max(1), metrics.disk_write * 100 / size.max(1)));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok((handle.join().await?, progress))
}

/// Download an object into a file the application already opened.
async fn download_into_file(
    client: &Client,
    key: &str,
    file: std::fs::File,
) -> Result<DownloadOutput, BoxError> {
    let handle = client.download().bucket(BUCKET).key(key).write_to_file(file)?;
    Ok(handle.join().await?)
}

/// Download one byte range of an object.
async fn download_range(client: &Client, key: &str, first: u64, last: u64) -> Result<Bytes, BoxError> {
    let mut handle = client
        .download()
        .bucket(BUCKET)
        .key(key)
        .range(format!("bytes={first}-{last}"))
        .initiate()?;
    let mut contents = BytesMut::new();
    while let Some(chunk) = handle.body_mut().next().await {
        for segment in chunk?.data.into_segments() {
            contents.extend_from_slice(&segment);
        }
    }
    handle.join().await?;
    Ok(contents.freeze())
}

/// Read the object's metadata before the body, then the integrity outcome after.
async fn metadata_and_integrity(client: &Client, key: &str) -> Result<String, BoxError> {
    let mut handle = client
        .download()
        .bucket(BUCKET)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .initiate()?;
    let meta = handle.object_meta().await?;
    let summary = format!(
        "size={} e_tag={:?} content_type={:?} crc64nvme={:?} checksum_type={:?}",
        meta.total_object_size(),
        meta.e_tag(),
        meta.content_type(),
        meta.checksum_crc64_nvme(),
        meta.checksum_type(),
    );
    while let Some(chunk) = handle.body_mut().next().await {
        chunk?;
    }
    let output = handle.join().await?;
    Ok(format!("{summary} validation={:?}", output.integrity_checks().checksum_validation()))
}

/// Adjust a running download: raise its priority and narrow its read-ahead.
async fn control_running(client: &Client, key: &str) -> Result<String, BoxError> {
    let mut handle = client.download().bucket(BUCKET).key(key).initiate()?;
    handle.scheduling().set_priority(200);
    handle.io_ctl().set_read_ahead(ReadAhead::Parts(1));
    let mut chunks = 0;
    while let Some(chunk) = handle.body_mut().next().await {
        chunk?;
        chunks += 1;
    }
    let status = handle.status();
    let metrics = handle.metrics();
    handle.join().await?;
    Ok(format!("chunks={chunks} status={status:?} network_rx={}", metrics.network_rx))
}

/// Give up on a download after a deadline.
async fn download_with_deadline(client: &Client, key: &str, path: &Path, deadline: Duration) -> String {
    let handle = match client.download().bucket(BUCKET).key(key).write_to_path(path).await {
        Ok(handle) => handle,
        Err(e) => return format!("failed to start: {e}"),
    };
    match tokio::time::timeout(deadline, handle.join()).await {
        Ok(Ok(_)) => "completed".to_string(),
        Ok(Err(e)) => format!("failed: {e}"),
        // The handle was moved into `join()`, so nothing is left to abort or await.
        Err(_) => "timed out".to_string(),
    }
}

/// Probe: `join()` without reading the body, when the object needs more parts than read-ahead allows.
async fn probe_join_without_reading_body(client: &Client, key: &str) -> String {
    let handle = client
        .download()
        .bucket(BUCKET)
        .key(key)
        .read_ahead(ReadAhead::Parts(0))
        .initiate()
        .expect("download starts");
    match tokio::time::timeout(Duration::from_secs(5), handle.join()).await {
        Ok(Ok(_)) => "join returned Ok".to_string(),
        Ok(Err(e)) => format!("join returned an error: {e}"),
        Err(_) => "join still pending after 5s".to_string(),
    }
}

/// Probe: `part_number` is a public field on `DownloadInput`, but has no setter.
fn probe_part_number(client: &Client, key: &str) -> String {
    let mut input = DownloadInput::builder().bucket(BUCKET).key(key).build().expect("valid input");
    input.part_number = Some(1);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        DownloadInputBuilder::from(input).initiate_with(client).map(|_| ())
    }));
    match result {
        Ok(Ok(())) => "initiated".to_string(),
        Ok(Err(e)) => format!("error: {e}"),
        Err(panic) => format!(
            "panicked: {}",
            panic.downcast_ref::<String>().cloned().or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()
        ),
    }
}

/// Probe: `write_to_path` onto a path that already exists.
async fn probe_existing_destination(client: &Client, key: &str, path: &Path) -> Result<String, BoxError> {
    std::fs::write(path, b"existing local data")?;
    download_to_path(client, key, path).await?;
    let now = std::fs::read(path)?;
    Ok(format!("destination now holds {} bytes (was 19)", now.len()))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mock = mock::Mock::start().await;
    let client = mock.client().await;
    let dir = std::env::temp_dir().join(format!("tm-api-review-download-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;

    mock.put("small.txt", "hello, transfer manager").await;
    // A 20 MiB object uploaded through the transfer manager, so it's stored as multipart parts.
    let large = vec![9u8; 20 * MIB];
    client
        .upload()
        .bucket(BUCKET)
        .key("large.bin")
        .body(InputStream::from(large.clone()))
        .initiate()?
        .join()
        .await?;

    let bytes = download_to_memory(&client, "large.bin").await?;
    println!("download_to_memory:     {} bytes, matches={}", bytes.len(), bytes[..] == large[..]);
    let out = download_to_path(&client, "large.bin", &dir.join("large.bin")).await?;
    println!("download_to_path:       size={}", out.object_meta.total_object_size());
    let (out, progress) = download_to_path_with_progress(&client, "large.bin", &dir.join("progress.bin")).await?;
    println!("download_to_path_with_progress: (received %, written %) samples {progress:?}");
    println!(
        "  final metrics: network_rx={} disk_write={} total_bytes={:?}",
        out.metrics.network_rx, out.metrics.disk_write, out.metrics.total_bytes
    );
    let file = std::fs::File::create(dir.join("into.bin"))?;
    let out = download_into_file(&client, "small.txt", file).await?;
    println!("download_into_file:     size={}", out.object_meta.total_object_size());
    let range = download_range(&client, "large.bin", 100, 1123).await?;
    println!("download_range:         {} bytes", range.len());
    println!("metadata_and_integrity: small: {}", metadata_and_integrity(&client, "small.txt").await?);
    println!("metadata_and_integrity: large: {}", metadata_and_integrity(&client, "large.bin").await?);
    println!("control_running:        {}", control_running(&client, "large.bin").await?);
    let path = dir.join("deadline.bin");
    println!(
        "download_with_deadline: {} (temp files left: {})",
        download_with_deadline(&client, "large.bin", &path, Duration::from_millis(1)).await,
        std::fs::read_dir(&dir)?.filter(|e| e.as_ref().is_ok_and(|e| e.file_name().to_string_lossy().contains(".s3tmp."))).count()
    );

    println!("probe_join_without_reading_body: {}", probe_join_without_reading_body(&client, "large.bin").await);
    println!("probe_part_number: {}", probe_part_number(&client, "large.bin"));
    println!("probe_existing_destination: {}", probe_existing_destination(&client, "small.txt", &dir.join("exists.txt")).await?);

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
