/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Exploratory slice for sync: can a local walk and an S3 listing be merge-joined?
//
// The join infers "this key is absent from the other side" from position alone, so
// it only works if both sides agree on the key space (prefix stripping, separator
// translation) and on the total order. Pointing it at a local directory and an S3
// prefix holding identical content must therefore yield no transfers and no deletes.
//
// Both sides are pulled lazily and the join holds one buffered entry per side, so
// memory does not scale with the number of entries and decisions are emitted before
// either side is exhausted.
//
// Happy path only: no filters, no errors, ASCII names, aligned prefix.

use aws_sdk_s3::types::Object;
use aws_sdk_s3_transfer_manager::io::walk::{
    FsWalk, FsWalkContext, FsWalker, S3Walk, S3WalkContext, S3Walker,
};
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_types::DateTime;
use std::cmp::Ordering;
use std::path::Path;
use tempfile::TempDir;

const PREFIX: &str = "data/";

// Keys chosen so a breadth-first walk would order them differently from S3: '/' is
// 0x2F, below every alphanumeric, so nested keys interleave with siblings.
const KEYS: &[&str] = &[
    "a.txt",
    "a/c",
    "az.txt",
    "test-123.txt",
    "test.txt",
    "test/inner.txt",
    "test0.txt",
];

#[derive(Debug, Clone, Copy, PartialEq)]
struct EntryMeta {
    size: u64,
    last_modified: i64,
}

type Entry = (String, EntryMeta);

#[derive(Debug, PartialEq)]
enum Action {
    Transfer,
    Delete,
    Skip,
}

// Local walk in key order; relative path becomes a key with '/' separators.
struct LocalKeys(FsWalk);

impl LocalKeys {
    fn new(root: &Path) -> Self {
        let walker = FsWalker::builder().recursive(true).key_order(true).build();
        Self(walker.walk(FsWalkContext::builder().root(root).build()))
    }

    async fn next(&mut self) -> Option<Entry> {
        let entry = self.0.next().await?.expect("walk failed");
        let key = entry
            .relative_path()
            .to_str()
            .expect("ascii fixture")
            .replace(std::path::MAIN_SEPARATOR, "/");
        let meta = entry.metadata();
        let last_modified = meta
            .modified()
            .expect("mtime")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("post-epoch")
            .as_secs() as i64;
        Some((
            key,
            EntryMeta {
                size: meta.len(),
                last_modified,
            },
        ))
    }
}

// Object listing; the key loses the root prefix.
struct S3Keys(S3Walk);

impl S3Keys {
    fn new(client: aws_sdk_s3::Client) -> Self {
        let walker = S3Walker::builder().prefix(PREFIX).build();
        let ctx = S3WalkContext::builder()
            .client(client)
            .bucket("test-bucket")
            .build();
        Self(walker.walk(ctx))
    }

    async fn next(&mut self) -> Option<Entry> {
        let obj = self.0.next().await?.expect("listing failed");
        let key = obj
            .key()
            .expect("key")
            .strip_prefix(PREFIX)
            .expect("prefix")
            .to_string();
        Some((
            key,
            EntryMeta {
                size: obj.size().expect("size") as u64,
                last_modified: obj.last_modified().expect("last_modified").secs(),
            },
        ))
    }
}

// Upload direction: local is the source, S3 the destination, so a same-size entry
// is skipped unless the destination is older (FR-Cmp-1).
fn decide(src: &EntryMeta, dest: &EntryMeta) -> Action {
    if src.size != dest.size || dest.last_modified < src.last_modified {
        Action::Transfer
    } else {
        Action::Skip
    }
}

// One buffered entry per side; each decision is emitted as soon as both sides have
// passed the key it concerns.
async fn merge_join(src: &mut LocalKeys, dest: &mut S3Keys) -> Vec<(String, Action)> {
    let mut plan = Vec::new();
    let mut s = src.next().await;
    let mut d = dest.next().await;
    loop {
        match (&s, &d) {
            (None, None) => break,
            (Some((sk, _)), None) => {
                plan.push((sk.clone(), Action::Transfer));
                s = src.next().await;
            }
            (None, Some((dk, _))) => {
                plan.push((dk.clone(), Action::Delete));
                d = dest.next().await;
            }
            (Some((sk, sm)), Some((dk, dm))) => match sk.cmp(dk) {
                Ordering::Less => {
                    plan.push((sk.clone(), Action::Transfer));
                    s = src.next().await;
                }
                Ordering::Greater => {
                    plan.push((dk.clone(), Action::Delete));
                    d = dest.next().await;
                }
                Ordering::Equal => {
                    plan.push((sk.clone(), decide(sm, dm)));
                    s = src.next().await;
                    d = dest.next().await;
                }
            },
        }
    }
    plan
}

fn build_local(root: &Path) {
    for key in KEYS {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, *key).unwrap();
    }
}

// Listing returns keys in UTF-8 byte order as S3 does, sized to match the local
// files, with a timestamp newer than them.
fn mock_listing() -> aws_sdk_s3::Client {
    let mut keys: Vec<&str> = KEYS.to_vec();
    keys.sort();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let contents: Vec<Object> = keys
        .iter()
        .map(|k| {
            Object::builder()
                .key(format!("{PREFIX}{k}"))
                .size(k.len() as i64)
                .last_modified(DateTime::from_secs(now + 60))
                .build()
        })
        .collect();
    let output = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
        .set_contents(Some(contents))
        .build();
    let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || output.clone());
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[rule])
}

#[tokio::test]
async fn identical_sides_produce_no_transfers_or_deletes() {
    let dir = TempDir::new().unwrap();
    build_local(dir.path());

    let mut src = LocalKeys::new(dir.path());
    let mut dest = S3Keys::new(mock_listing());
    let plan = merge_join(&mut src, &mut dest).await;

    let unexpected: Vec<_> = plan.iter().filter(|(_, a)| *a != Action::Skip).collect();
    assert!(
        unexpected.is_empty(),
        "identical sides must plan nothing, got {unexpected:?}"
    );
    assert_eq!(plan.len(), KEYS.len(), "every key should be accounted for");
}
