/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// ======================================================================
// Real-bucket runs. `#[ignore]`d, so the ordinary suite never reaches S3:
//
//   S3_TEST_BUCKET_NAME_RS=<bucket> AWS_REGION=<region> \
//   SSL_CERT_FILE=/etc/ssl/cert.pem \
//     cargo test -p aws-sdk-s3-transfer-manager --lib real_bucket \
//     -- --ignored --test-threads=1 --nocapture
//
// rustls may not read the platform root certificates. Set `SSL_CERT_FILE` before running ignored
// real-bucket tests.
//
// TODO(sync): Ignored real-bucket tests call crate-private sync APIs. Move customer-facing cases to
// `examples/` after the public API can start a sync run.
// ======================================================================

use super::*;

const BUCKET_VAR: &str = "S3_TEST_BUCKET_NAME_RS";

fn named_bucket(var: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| {
        panic!("set {var} to a bucket these tests may write to and delete from")
    })
}

fn regular_bucket() -> String {
    named_bucket(BUCKET_VAR)
}

async fn real_client() -> aws_sdk_s3::Client {
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    aws_sdk_s3::Client::new(&sdk)
}

fn a_run_prefix(what: &str) -> String {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_nanos();
    format!("sync-e2e/{what}-{stamp}/")
}

async fn bucket_keys(c: &aws_sdk_s3::Client, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = c.list_objects_v2().bucket(regular_bucket()).prefix(prefix);
        if let Some(t) = token {
            req = req.continuation_token(t);
        }
        let page = req.send().await.expect("the listing answers");
        for o in page.contents() {
            if let Some(k) = o.key() {
                out.push(k.strip_prefix(prefix).unwrap_or(k).to_string());
            }
        }
        token = page.next_continuation_token().map(str::to_string);
        if token.is_none() {
            break;
        }
    }
    out.sort();
    out
}

fn tree_files(root: &Path) -> Vec<String> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, base, out);
            } else {
                let rel = p.strip_prefix(base).expect("a path under the root");
                out.push(
                    rel.to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/"),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

fn a_tree_with_contents(root: &Path, files: &[(&str, &str)]) {
    for (rel, body) in files {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().expect("a parent")).expect("directories are made");
        std::fs::write(&p, body).expect("the file is written");
    }
}

async fn remove_prefix(c: &aws_sdk_s3::Client, prefix: &str) {
    let keys = bucket_keys(c, prefix).await;
    for chunk in keys.chunks(1000) {
        let ids: Vec<_> = chunk
            .iter()
            .filter_map(|k| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(format!("{prefix}{k}"))
                    .build()
                    .ok()
            })
            .collect();
        if ids.is_empty() {
            continue;
        }
        let del = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(ids))
            .build()
            .expect("a delete request");
        let _ = c
            .delete_objects()
            .bucket(regular_bucket())
            .delete(del)
            .send()
            .await;
    }
}

#[derive(Debug)]
struct RealRun {
    moved: u64,
    transfers: u64,
    transferred: u64,
    deleted: u64,
    failures: u64,
    skipped: u64,
}

fn counters<S: KeyStream, D: KeyStream>(t: &SyncTransfer<S, D>) -> RealRun {
    let snapshot = t.inner.state.lock().snapshot();
    RealRun {
        moved: snapshot.bytes,
        transfers: snapshot.decided.transfers,
        transferred: snapshot.arrived,
        deleted: snapshot.removed,
        failures: snapshot.transfer_failures + snapshot.walk_failures,
        skipped: snapshot.decided.skipped.total(),
    }
}

async fn real_upload(
    local: &Path,
    prefix: &str,
    c: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
) -> RealRun {
    let config = crate::Config::builder().client(c.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(c.clone())
                .bucket(regular_bucket())
                .prefix(prefix)
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnUpload::new(
            ctx.handle.clone(),
            regular_bucket(),
            Some(prefix),
        )),
        Arc::new(DeleteFromBucket::new(c, regular_bucket(), Some(prefix))),
        RunSettings {
            max_children: 8,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(300), rx)
        .await
        .expect("the upload did not finish inside five minutes")
        .expect("the terminal signal was dropped");
    counters(&transfer)
}

async fn real_download(
    local: &Path,
    prefix: &str,
    c: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
) -> RealRun {
    let config = crate::Config::builder().client(c.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .downloading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(c)
                .bucket(regular_bucket())
                .prefix(prefix)
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().downloading(),
        Arc::new(SpawnDownload::new(
            ctx.handle.clone(),
            regular_bucket(),
            Some(prefix),
            local,
        )),
        Arc::new(DeleteFromLocalTree::new(local)),
        RunSettings {
            max_children: 8,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(300), rx)
        .await
        .expect("the download did not finish inside five minutes")
        .expect("the terminal signal was dropped");
    counters(&transfer)
}

const REAL_TREE: &[(&str, &str)] = &[
    ("a.txt", "alpha"),
    ("b.txt", "bravo"),
    ("nested/c.txt", "charlie"),
    ("nested/deep/d.txt", "delta"),
    ("empty.txt", ""),
];

fn real_tree_keys() -> Vec<String> {
    let mut v: Vec<String> = REAL_TREE.iter().map(|(k, _)| k.to_string()).collect();
    v.sort();
    v
}

#[ignore = "will be moved to examples"]
#[tokio::test]
async fn real_bucket_upload_puts_every_key_under_its_prefix() {
    let c = real_client().await;
    let prefix = a_run_prefix("up-prefix");
    let dir = tempfile::tempdir().expect("a temp dir");
    a_tree_with_contents(dir.path(), REAL_TREE);

    let run = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  upload: {run:?}");

    assert_eq!(
        bucket_keys(&c, &prefix).await,
        real_tree_keys(),
        "the bucket does not hold the tree under {prefix}"
    );
    assert_eq!(run.failures, 0, "a key failed");
    let stray = bucket_keys(&c, "a.txt").await;
    assert!(
        stray.is_empty(),
        "a key landed at the bucket root: {stray:?}"
    );

    remove_prefix(&c, &prefix).await;
}

#[ignore = "will be moved to examples"]
#[tokio::test]
async fn real_bucket_download_strips_the_prefix_from_every_key() {
    let c = real_client().await;
    let prefix = a_run_prefix("down-prefix");
    let src = tempfile::tempdir().expect("a temp dir");
    a_tree_with_contents(src.path(), REAL_TREE);
    real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;

    let dst = tempfile::tempdir().expect("a temp dir");
    let run = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  download: {run:?}");

    assert_eq!(
        tree_files(dst.path()),
        real_tree_keys(),
        "the local tree does not mirror the keys under {prefix}"
    );
    assert_eq!(run.failures, 0, "a key failed");
    assert!(
        !dst.path().join("sync-e2e").exists(),
        "the prefix reached the local tree as a directory"
    );
    for (rel, body) in REAL_TREE {
        let got = std::fs::read_to_string(dst.path().join(rel)).expect("the file reads");
        assert_eq!(&got, body, "{rel} arrived with the wrong contents");
    }

    remove_prefix(&c, &prefix).await;
}

#[ignore = "will be moved to examples"]
#[tokio::test]
async fn real_bucket_second_run_moves_nothing() {
    let c = real_client().await;
    let prefix = a_run_prefix("idempotent");
    let dir = tempfile::tempdir().expect("a temp dir");
    a_tree_with_contents(dir.path(), REAL_TREE);

    let up1 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  upload 1: {up1:?}");
    let up2 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  upload 2: {up2:?}");
    assert_eq!(
        up2.transfers,
        0,
        "the second upload re-sent {} of {} keys",
        up2.transfers,
        REAL_TREE.len()
    );

    let dst = tempfile::tempdir().expect("a temp dir");
    let down1 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  download 1: {down1:?}");
    let down2 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  download 2: {down2:?}");
    assert_eq!(
        down2.transfers,
        0,
        "the second download re-fetched {} of {} keys",
        down2.transfers,
        REAL_TREE.len()
    );

    remove_prefix(&c, &prefix).await;
}

#[ignore = "will be moved to examples"]
#[tokio::test]
async fn real_bucket_moves_an_object_past_the_multipart_threshold() {
    let c = real_client().await;
    let prefix = a_run_prefix("multipart");
    let src = tempfile::tempdir().expect("a temp dir");
    let body: Vec<u8> = (0..20 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(src.path().join("big.dat"), &body).expect("the file is written");

    let up = real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  upload: {up:?}");
    assert_eq!(up.failures, 0, "the multipart upload failed");
    assert_eq!(
        up.moved,
        body.len() as u64,
        "the run moved {} bytes of {}",
        up.moved,
        body.len()
    );

    let dst = tempfile::tempdir().expect("a temp dir");
    let down = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  download: {down:?}");
    assert_eq!(down.failures, 0, "the multipart download failed");
    let back = std::fs::read(dst.path().join("big.dat")).expect("the copy reads");
    assert_eq!(
        back.len(),
        body.len(),
        "the copy is {} bytes against {}",
        back.len(),
        body.len()
    );
    assert!(back == body, "the bytes came back in a different order");

    let again = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
    println!("  download 2: {again:?}");
    assert_eq!(
        again.transfers, 0,
        "the second download fetched the object again"
    );

    remove_prefix(&c, &prefix).await;
}

// This spawner builds real upload children and refuses to build one for `refused`.
struct SpawnUploadRefusing {
    upload: SpawnUpload,
    refused: &'static str,
}

impl SpawnChild<crate::io::walk::FsEntry> for SpawnUploadRefusing {
    fn spawn(
        &self,
        key: &str,
        source: &crate::io::walk::FsEntry,
        parent: u64,
    ) -> Result<SyncChild, crate::error::Error> {
        if key == self.refused {
            return Err(crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                "the test refuses this key",
            ));
        }
        self.upload.spawn(key, source, parent)
    }
}

async fn incomplete_uploads(c: &aws_sdk_s3::Client, prefix: &str) -> Vec<String> {
    let page = c
        .list_multipart_uploads()
        .bucket(regular_bucket())
        .prefix(prefix)
        .send()
        .await
        .expect("the multipart listing answers");
    page.uploads()
        .iter()
        .filter_map(|u| u.key().map(str::to_string))
        .collect()
}

#[ignore = "will be moved to examples"]
#[tokio::test]
async fn real_bucket_abort_leaves_no_incomplete_multipart_upload() {
    let c = real_client().await;
    let prefix = a_run_prefix("abort-multipart");
    let src = tempfile::tempdir().expect("a temp dir");
    // The large file sorts first, so its upload starts before the refused key fails the run.
    std::fs::write(src.path().join("a.dat"), vec![7u8; 64 * 1024 * 1024])
        .expect("the file is written");
    std::fs::write(src.path().join("z.txt"), "refused").expect("the file is written");

    let config = crate::Config::builder().client(c.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(src.path())
                .client(c.clone())
                .bucket(regular_bucket())
                .prefix(&prefix)
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnUploadRefusing {
            upload: SpawnUpload::new(ctx.handle.clone(), regular_bucket(), Some(&prefix)),
            refused: "z.txt",
        }),
        Arc::new(DeleteFromBucket::new(
            c.clone(),
            regular_bucket(),
            Some(&prefix),
        )),
        RunSettings {
            max_children: 8,
            delete_mode: DeleteMode::Off,
            failure_policy: FailedTransferPolicy::Abort,
        },
    );
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(300), rx)
        .await
        .expect("the aborting run did not finish inside five minutes")
        .expect("the terminal signal was dropped");

    let snapshot = transfer.inner.state.lock().snapshot();
    println!(
        "  abort: cancelled={} arrived={}",
        snapshot.cancelled, snapshot.arrived
    );
    assert!(ctx.is_failed(), "the aborting run did not fail");
    assert_eq!(
        snapshot.cancelled, 1,
        "the large upload finished first, so this run proves nothing about the cancel"
    );
    assert_eq!(
        incomplete_uploads(&c, &prefix).await,
        Vec::<String>::new(),
        "the cancelled child left its multipart upload incomplete"
    );
    assert_eq!(
        bucket_keys(&c, &prefix).await,
        Vec::<String>::new(),
        "the cancelled child left an object behind"
    );

    remove_prefix(&c, &prefix).await;
}
