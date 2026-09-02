/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Spans must appear under a filter that names only this crate's telemetry targets.
//!
//! A span whose target defaults to `module_path!()` is invisible to such a filter, so
//! these assertions are about the target each span carries, not about the span existing.
//! Which of the four targets a span belongs on is a separate question from whether it
//! carries one; only the latter is asserted here.
//!
//! Everything runs as one test, in phases, against one shared capture. The capture is a single
//! global accumulator, so concurrent tests reading it would be one test with nondeterministic
//! ordering, and a count taken over the union can be satisfied by a sibling's spans instead of
//! the operation under test. Each phase therefore asserts against only the spans recorded after
//! its own mark, and only for a transfer id not seen in an earlier phase.

use aws_config::Region;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize};
use aws_smithy_mocks::{mock, mock_client, RuleMode};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::span::Attributes;
use tracing::Id;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer};

/// The four targets declared in `telemetry.rs`. A span carrying anything else is
/// invisible to an operator filtering on this crate's concerns.
const TARGETS: [&str; 4] = [
    "aws_sdk_s3_transfer_manager::transfer",
    "aws_sdk_s3_transfer_manager::execution",
    "aws_sdk_s3_transfer_manager::scheduling",
    "aws_sdk_s3_transfer_manager::concurrency",
];

/// All four curated targets at `debug`, everything else at `info`. A span left on a
/// module-path target is suppressed by the leading `info` directive, which is the
/// condition this test exists to catch.
///
/// `execution` is included here but is deliberately absent from the integration
/// harness's default filter, which treats per-work-item detail as opt-in. So this is
/// the filter of an operator who asked for that detail, not the default one — the
/// `execute` span is expected to be invisible without it.
const CURATED_FILTER: &str = "info,\
    aws_sdk_s3_transfer_manager::concurrency=debug,\
    aws_sdk_s3_transfer_manager::scheduling=debug,\
    aws_sdk_s3_transfer_manager::execution=debug,\
    aws_sdk_s3_transfer_manager::transfer=debug";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeenSpan {
    name: &'static str,
    target: &'static str,
    /// Rendered `work` field, when the span carries one.
    work: Option<String>,
    /// Rendered `tid`, when the span belongs to a specific transfer. Spans from one
    /// transfer do not nest, so this is what attributes a span to a transfer.
    tid: Option<String>,
}

#[derive(Debug, Clone)]
struct SeenEvent {
    target: &'static str,
    level: tracing::Level,
    /// Every field and the message, rendered — enough to assert what a line carries.
    rendered: String,
}

#[derive(Debug, Default)]
struct Log {
    spans: Vec<SeenSpan>,
    events: Vec<SeenEvent>,
}

type Captured = Arc<Mutex<Log>>;

/// Records every span created while installed. Spans are created on managed threads and
/// tokio workers, not just the test thread, so this is installed globally rather than
/// with the thread-local `set_default` (which is why
/// `aws_smithy_runtime::test_util::capture_test_logs` cannot be reused here).
struct SpanCapture(Captured);

impl<S: tracing::Subscriber> Layer<S> for SpanCapture {
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        #[derive(Default)]
        struct Fields {
            work: Option<String>,
            tid: Option<String>,
        }
        impl Fields {
            fn set(&mut self, name: &str, value: String) {
                match name {
                    "work" => self.work = Some(value),
                    "tid" => self.tid = Some(value),
                    _ => {}
                }
            }
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.set(f.name(), format!("{v:?}"));
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                self.set(f.name(), v.to_owned());
            }
        }
        let mut fields = Fields::default();
        attrs.record(&mut fields);

        let meta = attrs.metadata();
        self.0.lock().unwrap().spans.push(SeenSpan {
            name: meta.name(),
            target: meta.target(),
            work: fields.work,
            tid: fields.tid,
        });
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        struct Render(String);
        impl tracing::field::Visit for Render {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.push_str(&format!(" {}={v:?}", f.name()));
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                self.0.push_str(&format!(" {}={v}", f.name()));
            }
        }
        let mut render = Render(String::new());
        event.record(&mut render);
        let meta = event.metadata();
        self.0.lock().unwrap().events.push(SeenEvent {
            target: meta.target(),
            level: *meta.level(),
            rendered: render.0,
        });
    }
}

/// The one global subscriber for this test binary.
fn captured() -> &'static Captured {
    static CAPTURED: OnceLock<Captured> = OnceLock::new();
    CAPTURED.get_or_init(|| {
        let buf: Captured = Arc::new(Mutex::new(Log::default()));
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new(CURATED_FILTER))
            .with(SpanCapture(buf.clone()));
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other subscriber in this test binary");
        buf
    })
}

/// How much has been recorded so far. A phase asserts against the tail past its own mark,
/// so nothing an earlier phase produced can satisfy a later phase.
#[derive(Copy, Clone)]
struct Mark {
    spans: usize,
    events: usize,
}

fn mark() -> Mark {
    let log = captured().lock().unwrap();
    Mark {
        spans: log.spans.len(),
        events: log.events.len(),
    }
}

fn spans_since(m: Mark) -> Vec<SeenSpan> {
    let log = captured().lock().unwrap();
    log.spans[m.spans..].to_vec()
}

fn events_since(m: Mark) -> Vec<SeenEvent> {
    let log = captured().lock().unwrap();
    log.events[m.events..].to_vec()
}

fn all_spans() -> Vec<SeenSpan> {
    captured().lock().unwrap().spans.clone()
}

/// Transfer ids that opened a `poll-work` span in `spans`.
fn poll_tids(spans: &[SeenSpan]) -> HashSet<String> {
    spans
        .iter()
        .filter(|s| s.name == "poll-work")
        .filter_map(|s| s.tid.clone())
        .collect()
}

/// Assert the operation driven since `m` opened a `poll-work` span for a transfer of its
/// own, and record that transfer as seen.
///
/// The scheduler may poll an earlier phase's transfer once more after it goes terminal, so
/// a phase's tail is not exclusively its own. Requiring an id *not seen before* excludes
/// those stragglers: a fresh id can only belong to a transfer this phase created. That is
/// what makes this per-operation rather than a count over the union.
fn assert_opened_its_own_poll_span(op: &str, m: Mark, seen: &mut HashSet<String>) {
    let tail = spans_since(m);
    let in_tail = poll_tids(&tail);
    let fresh: HashSet<String> = in_tail.difference(seen).cloned().collect();
    assert!(
        !fresh.is_empty(),
        "`{op}` opened no poll-work span for a transfer of its own; poll tids in its tail: \
         {in_tail:?}, already seen: {seen:?}"
    );
    seen.extend(fresh);
}

fn tm(s3: aws_sdk_s3::Client, part_size: u64) -> aws_sdk_s3_transfer_manager::Client {
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3)
        .part_size(PartSize::Target(part_size))
        // Pinned so a body of a few parts takes the multipart path; the default 16 MiB
        // threshold would route it to a single PutObject and skip the part work entirely.
        .multipart_threshold(PartSize::Target(part_size))
        .concurrency(ConcurrencyMode::Explicit(1))
        .build();
    aws_sdk_s3_transfer_manager::Client::new(config)
}

fn s3_config_defaults(builder: aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder {
    builder
        .region(Region::from_static("us-west-2"))
        .with_test_defaults()
}

const PART_SIZE: u64 = 5 * 1024 * 1024;

/// Every span this crate emits is on one of the four telemetry targets; each of the four
/// operations opens a poll span for its own transfer; the execute span names its work with
/// a compact discriminant; and a failed `CompleteMultipartUpload` warns with the upload id
/// and the reason.
#[tokio::test]
async fn spans_carry_a_telemetry_target_across_all_operations() {
    let _ = captured();
    // Transfers whose poll span an earlier phase already accounted for.
    let mut polled = HashSet::new();

    // --- multipart upload: create / upload-part / complete, plus the initiate span ---
    let m = mark();
    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
        CreateMultipartUploadOutput::builder()
            .upload_id("span-test")
            .build()
    });
    let upload_part =
        mock!(aws_sdk_s3::Client::upload_part).then_output(|| UploadPartOutput::builder().build());
    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .then_output(|| CompleteMultipartUploadOutput::builder().build());
    let up_client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu],
        s3_config_defaults
    );
    tm(up_client, PART_SIZE)
        .upload()
        .bucket("test-bucket")
        .key("big.dat")
        .body(Bytes::from(vec![0u8; (PART_SIZE * 2) as usize]).into())
        .initiate()
        .expect("upload initiated")
        .join()
        .await
        .expect("upload completed");
    assert_opened_its_own_poll_span("upload", m, &mut polled);

    // --- single-object download ---
    let m = mark();
    let data = Bytes::from_static(b"span target test payload");
    let get_object = mock!(aws_sdk_s3::Client::get_object).then_output({
        let data = data.clone();
        move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(data.clone()))
                .content_length(data.len() as i64)
                .content_range(format!("bytes 0-{}/{}", data.len() - 1, data.len()))
                .e_tag("span-etag")
                .build()
        }
    });
    let down_client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[get_object],
        s3_config_defaults
    );
    let mut handle = tm(down_client, PART_SIZE)
        .download()
        .bucket("test-bucket")
        .key("small.dat")
        .initiate()
        .expect("download initiated");
    test_common::drain(&mut handle)
        .await
        .expect("download drained");
    assert_opened_its_own_poll_span("download", m, &mut polled);

    // --- both directory operations, with nothing to transfer ---
    // An empty source directory and an empty listing make no S3 calls, and the initiate
    // span plus the poll span are produced regardless.
    let empty_dir = tempfile::tempdir().expect("temp dir");
    let list = mock!(aws_sdk_s3::Client::list_objects_v2)
        .then_output(|| ListObjectsV2Output::builder().build());
    let list_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list], s3_config_defaults);
    let dir_tm = tm(list_client, PART_SIZE);

    let m = mark();
    dir_tm
        .upload_objects()
        .bucket("test-bucket")
        .source(empty_dir.path())
        .initiate()
        .expect("upload_objects initiated")
        .join()
        .await
        .expect("upload_objects completed");
    assert_opened_its_own_poll_span("upload_objects", m, &mut polled);

    let m = mark();
    dir_tm
        .download_objects()
        .bucket("test-bucket")
        .destination(empty_dir.path())
        .initiate()
        .expect("download_objects initiated")
        .join()
        .await
        .expect("download_objects completed");
    assert_opened_its_own_poll_span("download_objects", m, &mut polled);

    let spans = all_spans();
    assert!(
        !spans.is_empty(),
        "no spans captured under the curated filter"
    );

    // (a) no span leaked a module-path target.
    let stray: Vec<_> = spans
        .iter()
        .filter(|s| !TARGETS.contains(&s.target))
        .collect();
    assert!(
        stray.is_empty(),
        "spans on a target outside the curated set (they vanish under an operator filter): {stray:?}"
    );

    // (b) each operation's entry point and the shared execute span are present.
    let names: HashSet<&str> = spans.iter().map(|s| s.name).collect();
    for expected in [
        "initiate-upload",
        "initiate-download",
        "initiate-upload-objects",
        "initiate-download-objects",
        "poll-work",
        "execute",
        "upload-part",
        "send-upload-part",
        "download-part",
    ] {
        assert!(
            names.contains(expected),
            "span `{expected}` missing under the curated filter; captured: {names:?}"
        );
    }

    // (c) the execute span names its work compactly rather than debug-formatting the work
    // enum, and covers upload, which had no execute span before.
    let work_labels: HashSet<String> = spans
        .iter()
        .filter(|s| s.name == "execute")
        .filter_map(|s| s.work.clone())
        .collect();
    assert!(
        !work_labels.is_empty(),
        "no execute span carried a `work` field"
    );
    for label in &work_labels {
        assert!(
            !label.contains('{') && !label.contains('('),
            "`work` should be a compact discriminant, got a debug rendering: {label}"
        );
    }
    assert!(
        work_labels.contains("upload-part"),
        "expected an execute span for upload work; got {work_labels:?}"
    );

    // --- (d) a failed CompleteMultipartUpload must warn with the upload id and the
    // reason. Without the id an operator cannot abort or inspect the upload S3 still holds
    // parts for; without the reason the line says only that something failed.
    let m = mark();
    let upload_id = "orphaned-upload-id";
    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
        CreateMultipartUploadOutput::builder()
            .upload_id(upload_id)
            .build()
    });
    let upload_part =
        mock!(aws_sdk_s3::Client::upload_part).then_output(|| UploadPartOutput::builder().build());
    let complete_mpu =
        mock!(aws_sdk_s3::Client::complete_multipart_upload).then_http_response(|| {
            HttpResponse::new(
                StatusCode::try_from(500).unwrap(),
                SdkBody::from("<Error><Code>InternalError</Code></Error>"),
            )
        });
    let client = mock_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu],
        s3_config_defaults
    );
    tm(client, PART_SIZE)
        .upload()
        .bucket("test-bucket")
        .key("orphan.dat")
        .body(Bytes::from(vec![7u8; (PART_SIZE * 2) as usize]).into())
        .initiate()
        .expect("upload initiated")
        .join()
        .await
        .expect_err("a failing CompleteMultipartUpload must fail the upload");

    let warnings: Vec<SeenEvent> = events_since(m)
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .collect();
    let warned = warnings
        .iter()
        .find(|e| {
            e.target == "aws_sdk_s3_transfer_manager::transfer" && e.rendered.contains(upload_id)
        })
        .unwrap_or_else(|| {
            panic!(
                "expected a WARN on the transfer target carrying upload id `{upload_id}`; \
                 saw: {warnings:?}"
            )
        });
    assert!(
        warned.rendered.contains("InternalError"),
        "the warning must name the failure reason, not only that it failed: {}",
        warned.rendered
    );
}
