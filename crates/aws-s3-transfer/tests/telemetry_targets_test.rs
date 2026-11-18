/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tracing records from transfers use the documented targets.
//!
//! A record with no explicit target takes its module path, such as
//! `aws_s3_transfer::operation::upload::transfer`, which no
//! `aws_s3_transfer::transfer` directive matches. This binary
//! installs a process-wide subscriber that keeps every record at every level,
//! runs one transfer of each operation with state-machine events enabled, and
//! checks the target of every record the crate wrote.

use std::sync::Mutex;
use std::time::Duration;

use aws_s3_transfer::io::walk::{FsWalker, S3Walker};
use aws_s3_transfer::io::InputStream;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::Object;
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// Every target the crate documents in its `telemetry` module.
const DOCUMENTED_TARGETS: &[&str] = &[
    "aws_s3_transfer::concurrency",
    "aws_s3_transfer::scheduling",
    "aws_s3_transfer::execution",
    "aws_s3_transfer::memory",
    "aws_s3_transfer::transfer",
    "aws_s3_transfer::transfer::upload",
    "aws_s3_transfer::transfer::download",
    "aws_s3_transfer::transfer::upload_objects",
    "aws_s3_transfer::transfer::download_objects",
];

const OBJECTS: usize = 3;

#[derive(Debug)]
struct Record {
    target: String,
    module_path: String,
    /// Event message, or span name.
    name: String,
}

static RECORDS: Mutex<Vec<Record>> = Mutex::new(Vec::new());

struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// Keeps the metadata of every event and span.
struct Capture;

impl Capture {
    fn keep(meta: &tracing::Metadata<'_>, name: String) {
        RECORDS.lock().unwrap().push(Record {
            target: meta.target().to_owned(),
            module_path: meta.module_path().unwrap_or_default().to_owned(),
            name,
        });
    }
}

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut message = Message(String::new());
        event.record(&mut message);
        Capture::keep(event.metadata(), message.0);
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: Context<'_, S>,
    ) {
        Capture::keep(attrs.metadata(), attrs.metadata().name().to_owned());
    }
}

fn s3_client() -> aws_sdk_s3::Client {
    let put =
        mock!(aws_sdk_s3::Client::put_object).then_output(|| PutObjectOutput::builder().build());
    let head = mock!(aws_sdk_s3::Client::head_object).then_output(|| {
        aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
            .content_length(5)
            .e_tag("my-etag")
            .build()
    });
    let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .content_length(5)
            .content_range("bytes 0-4/5")
            .e_tag("my-etag")
            .body(ByteStream::from_static(b"hello"))
            .build()
    });
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
        ListObjectsV2Output::builder()
            .set_contents(Some(
                (0..OBJECTS)
                    .map(|i| Object::builder().key(format!("k{i}")).size(5).build())
                    .collect(),
            ))
            .build()
    });
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put, head, get, list])
}

/// Counts records named `name` on `target`.
fn count(records: &[Record], target: &str, name: &str) -> usize {
    records
        .iter()
        .filter(|r| r.target == target && r.name == name)
        .count()
}

#[tokio::test]
async fn transfer_records_use_documented_targets() {
    // Read when the client's config is built. Level 2 enables state-machine
    // events, so their records are checked too.
    std::env::set_var("AWS_S3_TM_DIAGNOSTICS", "transfer.detail=2");
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture))
        .expect("the only subscriber in this binary");

    let config = aws_s3_transfer::Config::builder()
        .s3_config(test_common::s3_config_with_test_http(
            s3_client().config().to_builder(),
        ))
        .build()
        .expect("valid config");
    let tm = aws_s3_transfer::Client::new(config);

    tokio::time::timeout(Duration::from_secs(30), async {
        tm.upload()
            .bucket("test-bucket")
            .key("single")
            .body(InputStream::from_static(b"hello"))
            .initiate()
            .unwrap()
            .join()
            .await
            .unwrap();

        let mut download = tm
            .download()
            .bucket("test-bucket")
            .key("single")
            .initiate()
            .unwrap();
        test_common::drain(&mut download).await.unwrap();

        let source = tempfile::tempdir().unwrap();
        for i in 0..OBJECTS {
            std::fs::write(source.path().join(format!("f{i}")), b"hello").unwrap();
        }
        let uploaded = tm
            .upload_objects()
            .bucket("test-bucket")
            .source(source.path())
            .walker(FsWalker::builder().build())
            .initiate()
            .unwrap()
            .join()
            .await
            .unwrap();
        assert_eq!(uploaded.objects_uploaded(), OBJECTS as u64);

        let destination = tempfile::tempdir().unwrap();
        let downloaded = tm
            .download_objects()
            .bucket("test-bucket")
            .destination(destination.path())
            .walker(S3Walker::builder().build())
            .initiate()
            .unwrap()
            .join()
            .await
            .unwrap();
        assert_eq!(downloaded.objects_downloaded(), OBJECTS as u64);
    })
    .await
    .expect("transfers finish");

    let records = std::mem::take(&mut *RECORDS.lock().unwrap());
    // State-machine events are TRACE records gated by the detail level; seeing
    // them shows the subscriber kept every level and the level took effect.
    assert!(
        count(
            &records,
            "aws_s3_transfer::transfer::upload_objects",
            "upload_objects state-machine event"
        ) > 0
    );
    let undocumented: Vec<&Record> = records
        .iter()
        .filter(|r| r.module_path.starts_with("aws_s3_transfer"))
        .filter(|r| !DOCUMENTED_TARGETS.contains(&r.target.as_str()))
        .collect();
    assert!(
        undocumented.is_empty(),
        "records outside the documented targets: {undocumented:#?}"
    );

    // Each composite reports once under its own target, and each of its
    // children is an ordinary transfer reporting under the child operation.
    let target = |op: &str| format!("aws_s3_transfer::transfer::{op}");
    let summaries = [
        ("upload_objects", "upload_objects transfer terminal", 1),
        ("download_objects", "download_objects transfer terminal", 1),
        ("upload", "upload transfer terminal", OBJECTS + 1),
        ("download", "download transfer terminal", OBJECTS + 1),
    ];
    for (op, message, expected) in summaries {
        assert_eq!(
            count(&records, &target(op), message),
            expected,
            "`{message}` records on `{}`",
            target(op)
        );
    }
}
