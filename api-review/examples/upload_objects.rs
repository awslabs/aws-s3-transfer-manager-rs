//! Upload a local directory.
//!
//! Each function is the code a customer writes for one task. `main` runs them against a local S3
//! endpoint and prints what happened. Functions named `probe_*` aren't customer tasks: they check a
//! specific behavior the review describes.

#[path = "support/mock.rs"]
mod mock;

use std::path::Path;

use aws_sdk_s3_transfer_manager::Client;
use aws_sdk_s3_transfer_manager::error::ErrorKind;
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::operation::upload_objects::UploadObjectsOutput;
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Upload a directory tree under a key prefix.
async fn upload_tree(client: &Client, dir: &Path, prefix: &str) -> Result<UploadObjectsOutput, BoxError> {
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .key_prefix(prefix)
        .walker(FsWalker::builder().recursive(true).build())
        .initiate()?;
    Ok(handle.join().await?)
}

/// Upload only the `.log` files in a tree.
async fn upload_logs_only(client: &Client, dir: &Path, prefix: &str) -> Result<UploadObjectsOutput, BoxError> {
    let walker = FsWalker::builder()
        .recursive(true)
        .filter(|entry| entry.relative_path().extension().is_some_and(|ext| ext == "log"))
        .build();
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .key_prefix(prefix)
        .walker(walker)
        .initiate()?;
    Ok(handle.join().await?)
}

/// Upload everything that can be uploaded, then report what couldn't.
async fn upload_continue_on_failure(client: &Client, dir: &Path, prefix: &str) -> Result<String, BoxError> {
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .key_prefix(prefix)
        .walker(FsWalker::builder().recursive(true).build())
        .failure_policy(FailedTransferPolicy::Continue)
        .initiate()?;
    let output = handle.join().await?;
    let mut report = format!("uploaded={}", output.objects_uploaded());
    for failed in output.failed_transfers() {
        report += &format!(
            "\n    failed: path={:?} kind={:?} error={}",
            failed.source_path(),
            failed.error().kind(),
            failed.error()
        );
    }
    Ok(report)
}

/// The default policy stops at the first failure.
async fn upload_abort_on_failure(client: &Client, dir: &Path, prefix: &str) -> String {
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .key_prefix(prefix)
        .walker(FsWalker::builder().recursive(true).build())
        .initiate()
        .expect("upload starts");
    match handle.join().await {
        Ok(output) => format!("ok: uploaded={}", output.objects_uploaded()),
        Err(e) if *e.kind() == ErrorKind::ChildOperationFailed => format!(
            "error: {e} (failed_uploads={})",
            e.failed_uploads().map_or(0, |f| f.len())
        ),
        Err(e) => format!("error ({:?}): {e}", e.kind()),
    }
}

/// Watch a directory upload, then stop it.
async fn control_and_abort(client: &Client, dir: &Path, prefix: &str) -> Result<String, BoxError> {
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .key_prefix(prefix)
        .walker(FsWalker::builder().recursive(true).build())
        .initiate()?;
    handle.scheduling().set_priority(32);
    let status = handle.status();
    let metrics = handle.metrics();
    handle.abort().await;
    Ok(format!("status={status:?} network_tx={} total_bytes={:?}", metrics.network_tx, metrics.total_bytes))
}

/// Probe: what the default walker uploads.
async fn probe_default_walker(client: &Client, dir: &Path, prefix: &str) -> Result<u64, BoxError> {
    let handle = client.upload_objects().bucket(BUCKET).source(dir).key_prefix(prefix).initiate()?;
    Ok(handle.join().await?.objects_uploaded())
}

/// Probe: keys produced with an empty key prefix.
async fn probe_empty_prefix(client: &Client, dir: &Path) -> Result<u64, BoxError> {
    let handle = client.upload_objects().bucket(BUCKET).source(dir).key_prefix("").initiate()?;
    Ok(handle.join().await?.objects_uploaded())
}

fn write(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(path.parent().expect("has parent"))?;
    std::fs::write(path, contents)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mock = mock::Mock::start().await;
    let client = mock.client().await;
    let root = std::env::temp_dir().join(format!("tm-api-review-upload-objects-{}", std::process::id()));

    let tree = root.join("tree");
    write(&tree.join("README.md"), b"top level")?;
    write(&tree.join("app/server.log"), b"log line")?;
    write(&tree.join("app/config/settings.toml"), b"key = 1")?;
    write(&tree.join(".hidden/token"), b"secret-ish")?;

    let broken = root.join("broken");
    write(&broken.join("ok-1.txt"), b"one")?;
    write(&broken.join("ok-2.txt"), b"two")?;
    write(&broken.join("unreadable.txt"), b"three")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(broken.join("unreadable.txt"), std::fs::Permissions::from_mode(0o000))?;
    }

    let out = upload_tree(&client, &tree, "backup/2026-11-06").await?;
    println!("upload_tree:                {} objects, network_tx={}", out.objects_uploaded(), out.metrics().network_tx);
    let out = upload_logs_only(&client, &tree, "logs").await?;
    println!("upload_logs_only:           {} objects", out.objects_uploaded());
    println!("upload_continue_on_failure: {}", upload_continue_on_failure(&client, &broken, "continue").await?);
    println!("upload_abort_on_failure:    {}", upload_abort_on_failure(&client, &broken, "abort").await);
    println!("control_and_abort:          {}", control_and_abort(&client, &tree, "aborted").await?);
    println!("probe_default_walker:       {} objects (tree has 4 files)", probe_default_walker(&client, &tree, "default").await?);
    println!("probe_empty_prefix:         {} objects", probe_empty_prefix(&client, &tree).await?);
    println!("keys in bucket:");
    for key in mock.keys().await {
        println!("    {key}");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(broken.join("unreadable.txt"), std::fs::Permissions::from_mode(0o644))?;
    }
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
