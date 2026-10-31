/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer scenarios against the mock S3 server.
//!
//! Each scenario uses a fresh [`Store`] and client, then drops both. Every
//! task they spawned must exit by then. File scenarios work in their own
//! subdirectory of the work directory.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use aws_sdk_s3_transfer_manager::error::ErrorKind;
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use aws_sdk_s3_transfer_manager::Client;
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::Bytes;

use crate::store::Store;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// The automatic memory limit on WASI targets.
const AUTO_MEMORY_BYTES: u64 = 2 * 1024 * MIB as u64;

/// Epoch flushes that advance a single-participant collector far enough to
/// destroy garbage deferred before the first flush.
const EPOCH_FLUSHES: usize = 4;

/// How long dropped clients' tasks may take to exit.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The target this module was built for.
const TARGET: &str = if cfg!(target_env = "p3") {
    "wasm32-wasip3"
} else if cfg!(target_env = "p2") {
    "wasm32-wasip2"
} else {
    "wasm32-wasip1"
};

/// Returns `Err(message)` from the enclosing scenario when `condition` fails.
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

/// Runs every scenario in order and returns the first failure. `server` is the
/// mock S3 server's address.
///
/// Prints `wasm32-wasi-tests: <count> scenarios on <target>` first, then
/// `PASS <scenario>: <summary>` for each scenario, where the summary lists the
/// requests the scenario's clients sent and the tasks that exited, then
/// `<count> scenarios passed`.
pub(crate) async fn run_all(work: &Path, server: SocketAddr) -> Result<(), String> {
    macro_rules! scenarios {
        ($($name:literal => $scenario:ident),+ $(,)?) => {{
            let count = [$($name),+].len();
            println!("wasm32-wasi-tests: {count} scenarios on {TARGET}");
            $(
                let summary = $scenario(Store::new(server), &work.join($name.replace(' ', "-")))
                    .await
                    .map_err(|message| format!("FAIL {}: {message}", $name))?;
                println!("PASS {}: {summary}", $name);
            )+
            println!("{count} scenarios passed");
        }};
    }
    scenarios! {
        "client construction" => client_construction,
        "single-part round trip" => single_part_round_trip,
        "multipart round trip" => multipart_round_trip,
        "outer retry" => outer_retry,
        "upload from file" => upload_from_file,
        "download to path" => download_to_path,
        "write to open file" => write_to_open_file,
        "directory round trip" => directory_round_trip,
        "symlink cycle" => symlink_cycle,
        "many files" => many_files,
    }
    Ok(())
}

/// Bytes whose value depends on their offset and `seed`, so a misplaced range
/// or a mixed-up file is detected.
fn pattern(len: usize, seed: u8) -> Bytes {
    (0..len)
        .map(|i| {
            (i as u8)
                .wrapping_mul(31)
                .wrapping_add((i >> 16) as u8)
                .wrapping_add(seed)
        })
        .collect()
}

fn error(context: &str, e: impl std::error::Error) -> String {
    format!("{context}: {}", DisplayErrorContext(e))
}

async fn upload(client: &Client, key: &str, body: InputStream) -> Result<(), String> {
    client
        .upload()
        .bucket("bucket")
        .key(key)
        .body(body)
        .initiate()
        .map_err(|e| error("initiate upload", e))?
        .join()
        .await
        .map(drop)
        .map_err(|e| error("upload", e))
}

async fn download(client: &Client, key: &str) -> Result<Vec<u8>, String> {
    let mut handle = client
        .download()
        .bucket("bucket")
        .key(key)
        .initiate()
        .map_err(|e| error("initiate download", e))?;
    let mut data = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        let chunk = chunk.map_err(|e| error("download chunk", e))?;
        data.extend_from_slice(&chunk.data.into_contiguous());
    }
    handle.join().await.map_err(|e| error("download", e))?;
    Ok(data)
}

fn ensure_same(what: &str, expected: &[u8], actual: &[u8]) -> Result<(), String> {
    ensure!(
        expected.len() == actual.len(),
        "{what}: {} bytes, expected {}",
        actual.len(),
        expected.len()
    );
    if let Some(offset) = expected.iter().zip(actual).position(|(a, b)| a != b) {
        return Err(format!("{what}: bytes differ at offset {offset}"));
    }
    Ok(())
}

fn fs_error(context: impl std::fmt::Display) -> impl FnOnce(std::io::Error) -> String {
    move |e| format!("{context}: {e}")
}

/// Creates `dir` and every parent.
fn create_dir(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(fs_error(dir.display()))
}

fn read(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(fs_error(path.display()))
}

fn write(path: &Path, data: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        create_dir(parent)?;
    }
    std::fs::write(path, data).map_err(fs_error(path.display()))
}

/// Drops `client` and `store`, waits until every task spawned since the
/// scenario started has exited, and returns a summary of the work: the
/// requests the scenario's clients sent and the number of tasks that exited.
///
/// Workers exit when the client's shared state drops, and connection tasks
/// exit when the store's connection pool drops. The scheduler's lock-free sets
/// hand removed transfer descriptors, which reference the client's state, to
/// crossbeam-epoch, which destroys them only after the epoch advances.
/// Flushing the epoch makes the drop deterministic.
async fn shut_down(client: Client, tasks_before: usize, store: Store) -> Result<String, String> {
    let tasks = alive_tasks().saturating_sub(tasks_before);
    let requests = store.to_string();
    drop(client);
    drop(store);
    let metrics = tokio::runtime::Handle::current().metrics();
    let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
    loop {
        for _ in 0..EPOCH_FLUSHES {
            crossbeam_epoch::pin().flush();
        }
        let alive = metrics.num_alive_tasks();
        if alive <= tasks_before {
            return Ok(format!(
                "{requests}; {tasks} tasks exited after the client was dropped"
            ));
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "{} spawned tasks remain alive {SHUTDOWN_TIMEOUT:?} after the client was dropped",
            alive - tasks_before
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// A client builds from explicit capabilities, and its automatic memory limit
/// is the target's fallback.
async fn client_construction(store: Store, _work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let capacity = client.metrics().memory().configured_capacity_bytes();
    ensure!(
        capacity == AUTO_MEMORY_BYTES,
        "automatic memory capacity is {capacity} bytes, expected {AUTO_MEMORY_BYTES}"
    );
    shut_down(client, before, store).await
}

/// A 64 KiB object uploads with one `PutObject` and downloads byte for byte.
async fn single_part_round_trip(store: Store, _work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let data = pattern(64 * KIB, 1);
    upload(&client, "single", InputStream::from(data.clone())).await?;
    ensure_same("single", &data, &download(&client, "single").await?)?;
    let requests = store.requests();
    ensure!(
        requests.put_object == 1 && requests.upload_part == 0 && requests.get_object == 1,
        "unexpected requests: {requests:?}"
    );
    shut_down(client, before, store).await
}

/// A 20 MiB object uploads as three 8 MiB-target parts and downloads as three
/// ranged `GetObject` requests.
async fn multipart_round_trip(store: Store, _work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let data = pattern(20 * MIB, 2);
    upload(&client, "multipart", InputStream::from(data.clone())).await?;
    ensure_same("multipart", &data, &download(&client, "multipart").await?)?;
    let requests = store.requests();
    ensure!(
        requests.upload_part == 3 && requests.multipart_control == 2,
        "unexpected upload requests: {requests:?}"
    );
    let mut ranges = store.get_ranges();
    ranges.sort_unstable();
    let expected = [0, 8 * MIB as u64, 16 * MIB as u64].map(Some);
    ensure!(ranges == expected, "unexpected download ranges: {ranges:?}");
    shut_down(client, before, store).await
}

/// The server answers the first `PutObject` for `retried` with `503
/// SlowDown`. The error reaches the transfer manager, which waits through the
/// configured sleep implementation and re-sends the request.
async fn outer_retry(store: Store, _work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let data = pattern(64 * KIB, 3);
    upload(&client, "retried", InputStream::from(data.clone())).await?;
    let requests = store.requests();
    ensure!(
        requests.throttled == 1 && requests.put_object == 2,
        "expected one throttled and one accepted PutObject, which needs the server's \
         `--slow-down-once bucket/retried`: {requests:?}"
    );
    ensure_same("retried", &data, &store.object("retried").await?)?;
    shut_down(client, before, store).await
}

/// A 20 MiB file uploads as three parts read from the file.
async fn upload_from_file(store: Store, work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let path = work.join("source.bin");
    let data = pattern(20 * MIB + 7, 4);
    write(&path, &data)?;
    let body = InputStream::from_path(&path).map_err(|e| error("open source", e))?;
    upload(&client, "from-file", body).await?;
    let requests = store.requests();
    ensure!(
        requests.upload_part == 3,
        "unexpected upload requests: {requests:?}"
    );
    ensure_same("from-file", &data, &store.object("from-file").await?)?;
    shut_down(client, before, store).await
}

/// A multipart object downloads into a new file at a path.
async fn download_to_path(store: Store, work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let data = pattern(18 * MIB + 5, 5);
    store.insert("to-path", data.clone()).await?;
    create_dir(work)?;
    let path = work.join("destination.bin");
    client
        .download()
        .bucket("bucket")
        .key("to-path")
        .write_to_path(&path)
        .await
        .map_err(|e| error("initiate download", e))?
        .join()
        .await
        .map_err(|e| error("download", e))?;
    ensure_same("destination.bin", &data, &read(&path)?)?;
    shut_down(client, before, store).await
}

/// A download into an append-mode file is rejected without touching it. A
/// download into a longer ordinary file writes at the part offsets and
/// truncates the file to the object's size.
async fn write_to_open_file(store: Store, work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let data = pattern(9 * MIB + 3, 6);
    store.insert("to-file", data.clone()).await?;

    let appended = work.join("append.bin");
    write(&appended, b"header")?;
    let file = std::fs::File::options()
        .append(true)
        .open(&appended)
        .map_err(fs_error(appended.display()))?;
    match client
        .download()
        .bucket("bucket")
        .key("to-file")
        .write_to_file(file)
    {
        Ok(_) => return Err("an append-mode file was accepted".to_owned()),
        Err(e) if matches!(e.kind(), ErrorKind::InputInvalid) => {}
        Err(e) => return Err(error("append-mode file: unexpected error", e)),
    }
    ensure_same("append.bin", b"header", &read(&appended)?)?;

    let target = work.join("existing.bin");
    write(&target, &vec![0xAA; 20 * MIB])?;
    let file = std::fs::File::options()
        .write(true)
        .open(&target)
        .map_err(fs_error(target.display()))?;
    client
        .download()
        .bucket("bucket")
        .key("to-file")
        .write_to_file(file)
        .map_err(|e| error("initiate download", e))?
        .join()
        .await
        .map_err(|e| error("download", e))?;
    ensure_same("existing.bin", &data, &read(&target)?)?;
    let summary = shut_down(client, before, store).await?;
    Ok(format!("append-mode file rejected; {summary}"))
}

/// Writes files of assorted sizes, including a multipart one, under `root`,
/// and returns their relative paths and contents.
fn write_tree(root: &Path) -> Result<Vec<(PathBuf, Bytes)>, String> {
    let files = [
        ("a.txt", 10),
        ("b.bin", 300 * KIB),
        ("nested/c.bin", 9 * MIB),
        ("nested/deeper/d.txt", 1),
    ];
    let mut tree = Vec::new();
    for (seed, (relative, len)) in files.into_iter().enumerate() {
        let data = pattern(len, seed as u8);
        write(&root.join(relative), &data)?;
        tree.push((PathBuf::from(relative), data));
    }
    Ok(tree)
}

/// A recursive directory uploads under a key prefix and downloads into
/// another directory with the same layout and contents.
async fn directory_round_trip(store: Store, work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let source = work.join("source");
    let destination = work.join("destination");
    let tree = write_tree(&source)?;

    let uploaded = client
        .upload_objects()
        .bucket("bucket")
        .source(&source)
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix("tree/")
        .initiate()
        .map_err(|e| error("initiate upload_objects", e))?
        .join()
        .await
        .map_err(|e| error("upload_objects", e))?;
    ensure!(
        uploaded.objects_uploaded() == tree.len() as u64 && uploaded.failed_transfers().is_empty(),
        "uploaded {} objects, failures {:?}",
        uploaded.objects_uploaded(),
        uploaded.failed_transfers()
    );
    for (relative, data) in &tree {
        let key = format!("tree/{}", relative.display());
        ensure_same(&key, data, &store.object(&key).await?)?;
    }

    create_dir(&destination)?;
    let downloaded = client
        .download_objects()
        .bucket("bucket")
        .key_prefix("tree/")
        .destination(&destination)
        .initiate()
        .map_err(|e| error("initiate download_objects", e))?
        .join()
        .await
        .map_err(|e| error("download_objects", e))?;
    ensure!(
        downloaded.objects_downloaded() == tree.len() as u64
            && downloaded.failed_transfers().is_empty(),
        "downloaded {} objects, failures {:?}",
        downloaded.objects_downloaded(),
        downloaded.failed_transfers()
    );
    for (relative, data) in &tree {
        let path = destination.join(relative);
        ensure_same(&path.display().to_string(), data, &read(&path)?)?;
    }
    shut_down(client, before, store).await
}

/// A recursive walk that follows symlinks reports a link to an ancestor as a
/// cycle and uploads every real file once.
async fn symlink_cycle(store: Store, work: &Path) -> Result<String, String> {
    let before = alive_tasks();
    let client = store.client(|config| config);
    let root = work.join("cycle");
    write(&root.join("a.txt"), b"a")?;
    write(&root.join("sub/b.txt"), b"b")?;
    let link = root.join("sub/loop");
    // `std::os::wasi::fs::symlink_path` is unstable; `soft_link` is the stable
    // portable spelling of the same call.
    #[allow(deprecated)]
    std::fs::soft_link("..", &link).map_err(fs_error(link.display()))?;

    let uploaded = client
        .upload_objects()
        .bucket("bucket")
        .source(&root)
        .walker(
            FsWalker::builder()
                .recursive(true)
                .follow_symlinks(true)
                .build(),
        )
        .failure_policy(FailedTransferPolicy::Continue)
        .key_prefix("cycle/")
        .initiate()
        .map_err(|e| error("initiate upload_objects", e))?
        .join()
        .await
        .map_err(|e| error("upload_objects", e))?;
    let failures = format!("{:?}", uploaded.failed_transfers());
    ensure!(
        uploaded.objects_uploaded() == 2 && failures.contains("SymlinkCycle"),
        "uploaded {} objects, failures {failures}",
        uploaded.objects_uploaded()
    );
    let summary = shut_down(client, before, store).await?;
    Ok(format!(
        "{} SymlinkCycle failure; {summary}",
        uploaded.failed_transfers().len()
    ))
}

/// Hundreds of small files upload and download as a directory.
async fn many_files(store: Store, work: &Path) -> Result<String, String> {
    const FILES: usize = 300;
    let before = alive_tasks();
    let client = store.client(|config| config);
    let source = work.join("source");
    let destination = work.join("destination");
    create_dir(&source)?;
    create_dir(&destination)?;
    let contents = |index: usize| pattern(1 + index * 97, index as u8);
    for index in 0..FILES {
        write(&source.join(format!("f{index:04}")), &contents(index))?;
    }

    let uploaded = client
        .upload_objects()
        .bucket("bucket")
        .source(&source)
        .key_prefix("many/")
        .initiate()
        .map_err(|e| error("initiate upload_objects", e))?
        .join()
        .await
        .map_err(|e| error("upload_objects", e))?;
    let downloaded = client
        .download_objects()
        .bucket("bucket")
        .key_prefix("many/")
        .destination(&destination)
        .initiate()
        .map_err(|e| error("initiate download_objects", e))?
        .join()
        .await
        .map_err(|e| error("download_objects", e))?;
    ensure!(
        uploaded.objects_uploaded() == FILES as u64
            && downloaded.objects_downloaded() == FILES as u64,
        "uploaded {} and downloaded {} of {FILES} files",
        uploaded.objects_uploaded(),
        downloaded.objects_downloaded()
    );
    for index in 0..FILES {
        let path = destination.join(format!("f{index:04}"));
        ensure_same(&path.display().to_string(), &contents(index), &read(&path)?)?;
    }
    shut_down(client, before, store).await
}
