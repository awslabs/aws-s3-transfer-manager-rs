/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! This module holds the test doubles for `SyncTransfer`.
//!
//! Each double replaces one collaborator: a bucket, a deleter, a child factory, or a comparison. A
//! test picks the double that controls the outcome it checks.

use std::path::Path;

use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::types::Object;
use aws_smithy_mocks::{mock, mock_client, RuleMode};

use crate::operation::sync::walk::Pairing;

use super::child::*;
use super::delete::*;
use super::*;

pub(super) fn a_bucket_holding(keys: &[&str]) -> aws_sdk_s3::Client {
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(0)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                .build()
        })
        .collect();
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
        ListObjectsV2Output::builder()
            .set_contents(Some(contents.clone()))
            .build()
    });
    let put = mock!(aws_sdk_s3::Client::put_object)
        .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put])
}

pub(super) fn a_bucket_recording_puts(
    keys: &[&str],
) -> (aws_sdk_s3::Client, Arc<Mutex<Vec<String>>>) {
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(0)
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
    let recorder = seen.clone();
    let put = mock!(aws_sdk_s3::Client::put_object)
        .match_requests(move |req| {
            if let Some(key) = req.key() {
                recorder.lock().push(key.to_string());
            }
            true
        })
        .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put]);
    (client, seen)
}

pub(super) fn still_running() -> StopCheck<'static> {
    &|| false
}

pub(super) fn a_local_tree(root: &Path, keys: &[&str]) {
    for key in keys {
        let path = root.join(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("a parent directory");
        }
        std::fs::write(&path, b"").expect("a file");
    }
}

pub(crate) struct RecordDeletes {
    batch: usize,
    sent: Mutex<Vec<Vec<String>>>,
    refuse: bool,
    namespace: Namespace,
}

impl RecordDeletes {
    pub(super) fn new(batch: usize) -> Self {
        Self {
            batch,
            sent: Mutex::new(Vec::new()),
            refuse: false,
            namespace: Namespace::Flat,
        }
    }

    pub(super) fn refusing(batch: usize) -> Self {
        Self {
            refuse: true,
            ..Self::new(batch)
        }
    }

    // This double refuses every key and stands in for a local tree.
    pub(super) fn refusing_in_a_tree(batch: usize) -> Self {
        Self {
            namespace: Namespace::Tree,
            ..Self::refusing(batch)
        }
    }

    pub(super) fn batches(&self) -> Vec<Vec<String>> {
        self.sent.lock().clone()
    }

    pub(super) fn keys_sent(&self) -> usize {
        self.sent.lock().iter().map(Vec::len).sum()
    }
}

impl RecordDeletes {
    pub(crate) fn batch_size(&self) -> usize {
        self.batch
    }

    pub(crate) async fn delete(&self, keys: Vec<String>) -> Vec<KeyOutcome> {
        self.sent.lock().push(keys.clone());
        let refuse = self.refuse;
        keys.into_iter()
            .map(|key| {
                if refuse {
                    let error =
                        crate::error::Error::new(crate::error::ErrorKind::ServiceError, "refused");
                    Err(FailedSyncKey::new(key, error))
                } else {
                    Ok(key)
                }
            })
            .collect()
    }
}

impl DeleteKeys for RecordDeletes {
    fn batch_size(&self) -> usize {
        RecordDeletes::batch_size(self)
    }

    fn namespace(&self) -> Namespace {
        self.namespace
    }

    fn delete<'a>(
        &'a self,
        keys: Vec<String>,
        _stopped: StopCheck<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<KeyOutcome>> + Send + 'a>> {
        Box::pin(RecordDeletes::delete(self, keys))
    }
}

pub(super) struct SpawnEnded {
    moved: u64,
    fails: bool,
    refuses: bool,
    refuse_at: Option<usize>,
    asked: std::sync::atomic::AtomicUsize,
    ended: Arc<std::sync::atomic::AtomicBool>,
}

impl SpawnEnded {
    pub(super) fn new(moved: u64, fails: bool) -> Self {
        Self {
            moved,
            fails,
            refuses: false,
            refuse_at: None,
            asked: std::sync::atomic::AtomicUsize::new(0),
            ended: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    pub(super) fn refusing_to_spawn() -> Self {
        Self {
            refuses: true,
            ..Self::new(0, false)
        }
    }

    pub(super) fn holding_open_but_refusing_at(at: usize) -> Self {
        let spawner = Self {
            refuse_at: Some(at),
            ..Self::new(0, false)
        };
        spawner
            .ended
            .store(false, std::sync::atomic::Ordering::SeqCst);
        spawner
    }

    pub(super) fn holding_children_open() -> Self {
        Self::holding_children_open_having_moved(0)
    }

    pub(super) fn holding_children_open_having_moved(moved: u64) -> Self {
        let spawner = Self::new(moved, false);
        spawner
            .ended
            .store(false, std::sync::atomic::Ordering::SeqCst);
        spawner
    }

    pub(super) fn asked_count(&self) -> usize {
        self.asked.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn release(&self, ctx: &TransferContext) {
        self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
        ctx.try_wake();
    }
}

impl SpawnChild<crate::io::walk::FsEntry> for SpawnEnded {
    fn spawn(
        &self,
        _key: &str,
        _source: &crate::io::walk::FsEntry,
        _parent: u64,
    ) -> Result<SyncChild, crate::error::Error> {
        let n = self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.refuses || self.refuse_at == Some(n) {
            return Err(crate::error::Error::new(
                crate::error::ErrorKind::ObjectNotDiscoverable,
                "the child could not be built",
            ));
        }
        Ok(SyncChild {
            id: crate::transfer::TransferId {
                id: 900_000 + n as u64,
                parent: None,
            },
            inner: ChildInner::Controlled {
                ended: self.ended.clone(),
                moved: self.moved,
                failed: self.fails,
            },
        })
    }
}

pub(super) struct AlwaysDefers;

impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysDefers {
    fn compare_described(
        &self,
        _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
        _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
    ) -> Verdict {
        unreachable!("compare is overridden, so no arm reaches a described pair")
    }

    fn compare(
        &self,
        _pairing: &crate::operation::sync::walk::Pairing<
            crate::io::walk::FsEntry,
            aws_sdk_s3::types::Object,
        >,
    ) -> Verdict {
        Verdict::Deferred(crate::operation::sync::compare::Deferred {})
    }
}

pub(super) struct AlwaysUnknown(pub(super) crate::io::key::stream::KeysLost);

impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysUnknown {
    fn compare_described(
        &self,
        _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
        _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
    ) -> Verdict {
        unreachable!("compare is overridden, so no arm reaches a described pair")
    }

    fn compare(
        &self,
        _pairing: &crate::operation::sync::walk::Pairing<
            crate::io::walk::FsEntry,
            aws_sdk_s3::types::Object,
        >,
    ) -> Verdict {
        Verdict::decided(crate::operation::sync::compare::Decision::skip_unknown(
            self.0,
        ))
    }
}

pub(super) struct AlwaysObstructed(pub(super) crate::io::key::stream::Obstruction);

impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysObstructed {
    fn compare_described(
        &self,
        _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
        _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
    ) -> Verdict {
        unreachable!("compare is overridden, so no arm reaches a described pair")
    }

    fn compare(
        &self,
        _pairing: &crate::operation::sync::walk::Pairing<
            crate::io::walk::FsEntry,
            aws_sdk_s3::types::Object,
        >,
    ) -> Verdict {
        Verdict::decided(crate::operation::sync::compare::Decision::skip_obstructed(
            self.0,
        ))
    }
}

pub(super) fn a_bucket_to_download(keys: &[&str], at: i64) -> aws_sdk_s3::Client {
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(at))
                .build()
        })
        .collect();
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
        ListObjectsV2Output::builder()
            .set_contents(Some(contents.clone()))
            .build()
    });
    let get = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
        aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
            .content_length(5)
            .last_modified(aws_smithy_types::DateTime::from_secs(at))
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
            .build()
    });
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get])
}

pub(super) fn a_bucket_recording_gets(
    keys: &[&str],
    at: i64,
) -> (aws_sdk_s3::Client, Arc<Mutex<Vec<String>>>) {
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(at))
                .build()
        })
        .collect();
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
        ListObjectsV2Output::builder()
            .set_contents(Some(contents.clone()))
            .build()
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = seen.clone();
    let get = mock!(aws_sdk_s3::Client::get_object)
        .match_requests(move |req| {
            if let Some(key) = req.key() {
                recorder.lock().push(key.to_string());
            }
            true
        })
        .then_output(move || {
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(at))
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get]);
    (client, seen)
}

pub(super) struct AlwaysTransfers;

impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysTransfers {
    fn compare_described(
        &self,
        _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
        _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
    ) -> Verdict {
        unreachable!("compare is overridden")
    }

    fn compare(
        &self,
        _pairing: &Pairing<crate::io::walk::FsEntry, aws_sdk_s3::types::Object>,
    ) -> Verdict {
        Verdict::decided(Decision::transfer(
            crate::operation::sync::compare::TransferReason::Missing,
        ))
    }
}
