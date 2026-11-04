/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::time::Duration;

use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::types::Object;
use aws_smithy_mocks::{mock, mock_client, RuleMode};

use crate::io::walk::{FsWalk, S3Walk};
use crate::operation::sync::modes::Mode;
use crate::operation::sync::walk::{LocalAndBucket, Walker};

use super::child::*;
use super::delete::*;
use super::state::*;
use super::test_util::*;
use super::*;

fn uploading(
    local: &Path,
    bucket_keys: &[&str],
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let client = a_bucket_holding(bucket_keys);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);

    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    (
        SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            Arc::new(RecordDeletes::new(DELETE_BATCH)),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailedTransferPolicy::Continue,
            },
        ),
        ctx,
    )
}

async fn drive(transfer: &SyncTransfer<FsWalk, S3Walk>) -> u64 {
    let run = async {
        loop {
            match transfer.poll_work() {
                PollWork::Ready { io: mut work, .. } => {
                    transfer.execute(&mut work).await;
                }
                PollWork::Done => break,
                PollWork::Pending => {
                    panic!("nothing else can make progress, so this would park forever")
                }
                PollWork::Spawned => {}
            }
        }
        transfer.inner.state.lock().snapshot().paired
    };
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the run parked: a poll answered Pending with no wake to follow")
}

fn downloading_into(
    local: &Path,
    client: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
) -> (
    SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    TransferContext,
    crate::transfer::StateMachineTerminalReceiver,
) {
    downloading_into_walking(local, client, delete_mode, Walker::builder().build())
}

fn downloading_into_walking(
    local: &Path,
    client: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
    walker: Walker,
) -> (
    SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    TransferContext,
    crate::transfer::StateMachineTerminalReceiver,
) {
    downloading_into_with(
        local,
        client,
        delete_mode,
        Arc::new(DeleteFromLocalTree::new(local)),
        walker,
    )
}

fn downloading_into_with(
    local: &Path,
    client: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
    deleter: Arc<dyn DeleteKeys>,
    walker: Walker,
) -> (
    SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    TransferContext,
    crate::transfer::StateMachineTerminalReceiver,
) {
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = walker
        .downloading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
            delete_mode,
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().downloading(),
        Arc::new(SpawnDownload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            None,
            local,
        )),
        deleter,
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx, rx)
}

async fn run_managed(
    transfer: &SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    ctx: &TransferContext,
    rx: crate::transfer::StateMachineTerminalReceiver,
) {
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(20), rx)
        .await
        .expect("the run did not finish inside twenty seconds")
        .expect("the terminal signal was dropped");
}

fn spawn_until_something_else(transfer: &SyncTransfer<FsWalk, S3Walk>) -> PollWork {
    loop {
        match transfer.poll_work() {
            PollWork::Spawned => continue,
            other => return other,
        }
    }
}

fn uploading_with(
    local: &Path,
    spawner: Arc<dyn SpawnChild<crate::io::walk::FsEntry>>,
    cap: usize,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    uploading_with_policy(local, spawner, cap, FailedTransferPolicy::Continue)
}

fn uploading_with_policy(
    local: &Path,
    spawner: Arc<dyn SpawnChild<crate::io::walk::FsEntry>>,
    cap: usize,
    failure_policy: FailedTransferPolicy,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let client = a_bucket_holding(&[]);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        spawner,
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: cap,
            delete_mode: DeleteMode::On,
            failure_policy,
        },
    );
    (transfer, ctx)
}

fn deleting(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    deleting_with(local, bucket_keys, deleter, DeleteMode::On)
}

fn with_spawner(
    local: &Path,
    spawner: SpawnEnded,
    failure_policy: FailedTransferPolicy,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let client = a_bucket_holding(&[]);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(spawner),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy,
        },
    );
    (transfer, ctx)
}

fn deleting_with_policy(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
    delete_mode: DeleteMode,
    failure_policy: FailedTransferPolicy,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let client = a_bucket_holding(bucket_keys);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnEnded::new(0, false)),
        deleter,
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy,
        },
    );
    (transfer, ctx)
}

fn uploading_comparing_with<C>(
    local: &Path,
    comparison: &'static C,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext)
where
    C: Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> + Send + Sync + 'static,
{
    let client = a_bucket_holding(&[]);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        comparison,
        Arc::new(SpawnEnded::new(0, false)),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::Off,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx)
}

fn deleting_with(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
    delete_mode: DeleteMode,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let client = a_bucket_holding(bucket_keys);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnEnded::new(0, false)),
        deleter,
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx)
}

const B: &str = "amzn-s3-demo-bucket";

// This mock bucket lists `keys` and records every key a `DeleteObjects` request names.
fn listing_and_recording_deletes(
    keys: &[&str],
    size: i64,
) -> (aws_sdk_s3::Client, Arc<Mutex<Vec<String>>>) {
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(size)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                .build()
        })
        .collect();
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
        ListObjectsV2Output::builder()
            .set_contents(Some(contents.clone()))
            .build()
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let last = Arc::new(Mutex::new(Vec::<String>::new()));
    let (rec, rec_last) = (seen.clone(), last.clone());
    let delete = mock!(aws_sdk_s3::Client::delete_objects)
        .match_requests(move |req| {
            let batch: Vec<String> = req
                .delete()
                .map(|d| d.objects().iter().map(|o| o.key().to_string()).collect())
                .unwrap_or_default();
            rec.lock().extend(batch.iter().cloned());
            *rec_last.lock() = batch;
            true
        })
        .then_output(move || {
            let deleted = last
                .lock()
                .iter()
                .map(|k| aws_sdk_s3::types::DeletedObject::builder().key(k).build())
                .collect();
            aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput::builder()
                .set_deleted(Some(deleted))
                .build()
        });
    let put = mock!(aws_sdk_s3::Client::put_object)
        .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
    (
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &delete, &put]),
        seen,
    )
}

// Start an upload run in delete mode under the default `Continue` policy.
fn upload_run(
    root: &Path,
    client: aws_sdk_s3::Client,
    walker: Walker,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_tokio(config);
    let (ctx, _rx) = TransferContext::new(handle);
    let walk = walker
        .uploading(
            LocalAndBucket::builder()
                .local_root(root)
                .client(client.clone())
                .bucket(B)
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnEnded::new(0, false)),
        Arc::new(DeleteFromBucket::new(client, B, None)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx)
}

mod deletes;
mod failures;
mod links;
mod local;
mod plan;
mod real_bucket;
mod scheduling;
mod stopping;
