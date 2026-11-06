//! Download a bucket or prefix to a local directory.
//!
//! Each function is the code a customer writes for one task. `main` runs them against a local S3
//! endpoint and prints what happened. Functions named `probe_*` aren't customer tasks: they check a
//! specific behavior the review describes.

#[path = "support/mock.rs"]
mod mock;

use std::path::{Path, PathBuf};

use aws_sdk_s3_transfer_manager::Client;
use aws_sdk_s3_transfer_manager::io::walk::S3Walker;
use aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsOutput;
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Download everything under a prefix into a directory.
async fn download_prefix(client: &Client, prefix: &str, dest: &Path) -> Result<DownloadObjectsOutput, BoxError> {
    let handle = client
        .download_objects()
        .bucket(BUCKET)
        .walker(S3Walker::builder().prefix(prefix).build())
        .destination(dest)
        .initiate()?;
    Ok(handle.join().await?)
}

/// Download only objects that match a filter on their listing entry.
async fn download_filtered(client: &Client, prefix: &str, dest: &Path) -> Result<DownloadObjectsOutput, BoxError> {
    let walker = S3Walker::builder()
        .prefix(prefix)
        .filter(|object| object.key().is_some_and(|key| key.ends_with(".json")))
        .build();
    let handle = client
        .download_objects()
        .bucket(BUCKET)
        .walker(walker)
        .destination(dest)
        .initiate()?;
    Ok(handle.join().await?)
}

/// Download everything that can be downloaded, then report what couldn't.
async fn download_continue_on_failure(client: &Client, prefix: &str, dest: &Path) -> Result<String, BoxError> {
    let handle = client
        .download_objects()
        .bucket(BUCKET)
        .walker(S3Walker::builder().prefix(prefix).build())
        .destination(dest)
        .failure_policy(FailedTransferPolicy::Continue)
        .initiate()?;
    let output = handle.join().await?;
    let mut report = format!("downloaded={}", output.objects_downloaded());
    for failed in output.failed_transfers() {
        report += &format!(
            "\n    failed: key={:?} kind={:?} error={}",
            failed.input().key(),
            failed.error().kind(),
            failed.error()
        );
    }
    Ok(report)
}

/// Watch a directory download, then stop it.
async fn control_and_abort(client: &Client, dest: &Path) -> Result<String, BoxError> {
    let handle = client.download_objects().bucket(BUCKET).destination(dest).initiate()?;
    handle.scheduling().set_priority(32);
    let status = handle.status();
    let metrics = handle.metrics();
    handle.abort().await;
    Ok(format!("status={status:?} network_rx={} total_bytes={:?}", metrics.network_rx, metrics.total_bytes))
}

/// Probe: a destination that doesn't exist yet.
async fn probe_missing_destination(client: &Client, dest: &Path) -> String {
    match download_prefix(client, "logs/", dest).await {
        Ok(out) => format!("ok: downloaded={}", out.objects_downloaded()),
        Err(e) => format!("error: {e}"),
    }
}

/// Probe: a relative destination path.
async fn probe_relative_destination(client: &Client, dest: &Path) -> String {
    match download_prefix(client, "logs/", dest).await {
        Ok(out) => format!("ok: downloaded={} failed={}", out.objects_downloaded(), out.failed_transfers().len()),
        Err(e) => format!("error: {e}"),
    }
}

/// Probe: local files that already exist at a destination path.
async fn probe_existing_files(client: &Client, dest: &Path) -> Result<String, BoxError> {
    let existing = dest.join("2026/11/06.log");
    std::fs::create_dir_all(existing.parent().expect("has parent"))?;
    std::fs::write(&existing, b"local edits")?;
    download_prefix(client, "logs/", dest).await?;
    Ok(format!("{} now holds {:?}", existing.display(), String::from_utf8(std::fs::read(&existing)?)?))
}

fn tree(dir: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.push(path.strip_prefix(root).unwrap_or(&path).display().to_string());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mock = mock::Mock::start().await;
    let client = mock.client().await;
    let root: PathBuf = std::env::temp_dir().join(format!("tm-api-review-download-objects-{}", std::process::id()));

    for (key, body) in [
        ("logs/2026/11/06.log", "today"),
        ("logs/2026/11/05.log", "yesterday"),
        ("logs/2025/12/31.log", "last year"),
        ("logs/summary.json", "{}"),
        ("logs/archive/", ""),
        ("data/report.json", "{\"rows\": 3}"),
        ("data/../../escape.txt", "outside"),
    ] {
        mock.put(key, body.to_string()).await;
    }

    let dest = root.join("logs");
    std::fs::create_dir_all(&dest)?;
    let out = download_prefix(&client, "logs/", &dest).await?;
    println!("download_prefix:              {} objects -> {:?}", out.objects_downloaded(), tree(&dest));
    let dest = root.join("json");
    std::fs::create_dir_all(&dest)?;
    let out = download_filtered(&client, "", &dest).await?;
    println!("download_filtered:            {} objects -> {:?}", out.objects_downloaded(), tree(&dest));
    let dest = root.join("data");
    std::fs::create_dir_all(&dest)?;
    println!("download_continue_on_failure: {}", download_continue_on_failure(&client, "data/", &dest).await?);
    let dest = root.join("aborted");
    std::fs::create_dir_all(&dest)?;
    println!("control_and_abort:            {}", control_and_abort(&client, &dest).await?);

    println!("probe_missing_destination:    {}", probe_missing_destination(&client, &root.join("not-created")).await);
    std::env::set_current_dir(&root)?;
    std::fs::create_dir_all(root.join("relative"))?;
    for relative in ["./relative", "relative"] {
        println!("probe_relative_destination:   {relative:?}: {}", probe_relative_destination(&client, Path::new(relative)).await);
    }
    let dest = root.join("existing");
    println!("probe_existing_files:         {}", probe_existing_files(&client, &dest).await?);

    std::env::set_current_dir(std::env::temp_dir())?;
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
