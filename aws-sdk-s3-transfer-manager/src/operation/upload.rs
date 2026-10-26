/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Single-object uploads using either a single PUT or multipart upload.
//!
//! Construct an upload through the client's fluent builder or
//! [`UploadInput::builder`](crate::model::UploadInput::builder).
//! Wait for [`UploadHandle::join`](crate::operation::upload::UploadHandle::join) to obtain
//! [`UploadOutput`](crate::model::UploadOutput), which combines service response metadata with
//! transfer metrics. Optional response fields depend on the selected path
//! and the values returned by S3; see [`UploadOutput`](crate::model::UploadOutput)
//! for their provenance.

/// Operation builders
pub mod builders;
mod checksum_strategy;
mod request;

mod context;
pub(crate) mod file_body;
mod handle;
mod observability;
mod part_body;
mod transfer;

pub use checksum_strategy::{ChecksumStrategy, ChecksumStrategyBuilder};
pub(crate) use transfer::UploadTransfer;

use crate::error;
pub use crate::model::builders::UploadInputBuilder;
pub use crate::model::builders::UploadOutputBuilder;
/// Request type for uploads to Amazon S3
pub use crate::model::UploadInput;
/// Response type for uploads to Amazon S3
pub use crate::model::UploadOutput;
use crate::transfer::TransferContext;
use crate::types::BucketType;
pub use handle::UploadHandle;

use std::sync::Arc;

/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
pub(crate) struct Upload;

impl Upload {
    /// Execute a single `Upload` transfer operation.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        Self::orchestrate_inner(handle, input, None)
    }

    /// Execute an `Upload` as a child of another transfer.
    ///
    /// The child's `TransferContext` is linked to `parent_id` so that
    /// `signal_terminal` on the child wakes the parent (letting the parent's
    /// state machine reap it) and cancelling the parent cascades to this child.
    pub(crate) fn orchestrate_child(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::upload::UploadInput,
        parent_id: u64,
    ) -> Result<UploadHandle, error::Error> {
        Self::orchestrate_inner(handle, input, Some(parent_id))
    }

    fn orchestrate_inner(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
        parent_id: Option<u64>,
    ) -> Result<UploadHandle, error::Error> {
        if input.checksum_strategy.is_none() {
            // User didn't explicitly set checksum strategy.
            // If SDK is configured to send checksums: use default checksum strategy.
            // Else: continue with no checksums
            if handle
                .s3_client
                .config()
                .request_checksum_calculation()
                .cloned()
                .unwrap_or_default()
                == aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported
            {
                input.checksum_strategy = Some(ChecksumStrategy::default());
            }
        }

        let stream = input.take_body();

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        // Create transfer context — linked to parent when this is a child
        // transfer, so signal_terminal wakes the parent and cancellation
        // cascades from the parent.
        let (ctx, completion_rx) = match parent_id {
            Some(pid) => TransferContext::new_child(handle.clone(), pid),
            None => TransferContext::new(handle.clone()),
        };

        let transfer = UploadTransfer::try_new(ctx, bucket_type, input, stream)?;

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(UploadHandle::new(completion_rx, transfer))
    }
}

#[cfg(test)]
mod test {

    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_runtime_api::client::http::{
        http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;
    use futures_util::future::BoxFuture;
    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::{UploadHandle, UploadInput};
    use crate::types::{
        AbortOutcome, AbortedUpload, ConcurrencyMode, FailedMultipartUploadPolicy, PartSize,
        RuntimeMode, TransferStatus,
    };

    /// S3 operations the scripted connector tells apart.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Op {
        Create,
        Part,
        Complete,
        Abort,
        Put,
    }

    /// How the scripted connector answers one operation.
    #[derive(Debug, Clone, Default)]
    enum Reply {
        /// The operation's success response.
        #[default]
        Ok,
        /// An S3 error with this status and code (all non-retryable).
        Err(u16, &'static str),
        /// Never answer: the request stays on the wire until it is dropped.
        Stall,
        /// Wait for a permit on the gate, then answer with the inner reply.
        Gated(Arc<Semaphore>, Box<Reply>),
    }

    impl Reply {
        fn gated(gate: &Arc<Semaphore>, then: Reply) -> Self {
            Reply::Gated(gate.clone(), Box::new(then))
        }
    }

    /// Per-operation answers. Every operation succeeds unless set.
    #[derive(Debug, Clone, Default)]
    struct Script {
        create: Reply,
        part: Reply,
        complete: Reply,
        abort: Reply,
        put: Reply,
    }

    /// Connector that answers each S3 operation as its [`Script`] says and
    /// records every request when it reaches the connector.
    #[derive(Debug)]
    struct ScriptedS3 {
        script: Script,
        sent: Arc<Mutex<Vec<Op>>>,
    }

    impl HttpConnector for ScriptedS3 {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let uri = request.uri().to_string();
            let has_upload_id = uri.contains("uploadId=");
            let op = match (request.method(), has_upload_id) {
                ("POST", false) if uri.contains("uploads") => Op::Create,
                ("PUT", true) if uri.contains("partNumber=") => Op::Part,
                ("POST", true) => Op::Complete,
                ("DELETE", true) => Op::Abort,
                ("PUT", false) => Op::Put,
                _ => panic!("unexpected request: {} {uri}", request.method()),
            };
            self.sent.lock().unwrap().push(op);
            let reply = match op {
                Op::Create => self.script.create.clone(),
                Op::Part => self.script.part.clone(),
                Op::Complete => self.script.complete.clone(),
                Op::Abort => self.script.abort.clone(),
                Op::Put => self.script.put.clone(),
            };
            HttpConnectorFuture::new(answer(op, reply))
        }
    }

    async fn answer(op: Op, mut reply: Reply) -> Result<HttpResponse, ConnectorError> {
        loop {
            match reply {
                Reply::Ok => return Ok(ok_response(op)),
                Reply::Err(status, code) => return Ok(error_response(status, code)),
                Reply::Stall => {
                    return std::future::pending::<Result<HttpResponse, ConnectorError>>().await
                }
                Reply::Gated(gate, then) => {
                    gate.acquire().await.expect("gate closed").forget();
                    reply = *then;
                }
            }
        }
    }

    fn ok_response(op: Op) -> HttpResponse {
        let xml = |body: &'static str| HttpResponse::new(200.try_into().unwrap(), body.into());
        match op {
            Op::Create => xml(r#"<?xml version="1.0" encoding="UTF-8"?>
                <InitiateMultipartUploadResult>
                    <Bucket>test-bucket</Bucket>
                    <Key>test-key</Key>
                    <UploadId>test-upload-id</UploadId>
                </InitiateMultipartUploadResult>"#),
            Op::Complete => xml(r#"<?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUploadResult>
                    <Bucket>test-bucket</Bucket>
                    <Key>test-key</Key>
                    <ETag>"final-etag"</ETag>
                </CompleteMultipartUploadResult>"#),
            Op::Part | Op::Put => {
                let mut response = HttpResponse::new(200.try_into().unwrap(), SdkBody::empty());
                response.headers_mut().insert("ETag", "\"test-etag\"");
                response
            }
            Op::Abort => HttpResponse::new(204.try_into().unwrap(), SdkBody::empty()),
        }
    }

    fn error_response(status: u16, code: &str) -> HttpResponse {
        HttpResponse::new(
            status.try_into().unwrap(),
            SdkBody::from(format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error><Code>{code}</Code><Message>scripted error</Message></Error>"#
            )),
        )
    }

    /// What kind of upload to start.
    #[derive(Debug, Clone, Copy)]
    enum Shape {
        /// One-part multipart upload.
        Multipart,
        /// One-part multipart upload with `FailedMultipartUploadPolicy::Retain`.
        MultipartRetain,
        /// Single PutObject.
        PutObject,
    }

    struct Started {
        // Keeps the client (and its scheduler) alive for the whole test, even
        // after the handle is consumed.
        _tm: crate::Client,
        handle: UploadHandle,
        sent: Arc<Mutex<Vec<Op>>>,
    }

    fn start(runtime_mode: RuntimeMode, script: Script, shape: Shape) -> Started {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(ScriptedS3 {
            script,
            sent: sent.clone(),
        });
        let http_client = http_client_fn(move |_, _| connector.clone());
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client)
                .region(aws_sdk_s3::config::Region::new("us-west-2"))
                .with_test_defaults()
                .build(),
        );

        let mut tm_config = crate::Config::builder()
            // Two slots: AbortMultipartUpload must be able to go out while
            // Complete (or a stalled part) still holds one. With one slot it
            // queues until that request answers.
            .concurrency(ConcurrencyMode::Explicit(2))
            .runtime_mode(runtime_mode)
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client);
        if !matches!(shape, Shape::PutObject) {
            // The 22-byte body is below the default threshold; force multipart.
            tm_config = tm_config.set_multipart_threshold(PartSize::Target(10));
        }
        let tm = crate::Client::new(tm_config.build());

        let mut input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from(Bytes::from_static(
                b"force multipart upload",
            )));
        if let Shape::MultipartRetain = shape {
            input = input.failed_multipart_upload_policy(FailedMultipartUploadPolicy::Retain);
        }
        let handle = input.initiate_with(&tm).unwrap();
        Started {
            _tm: tm,
            handle,
            sent,
        }
    }

    fn sent(sent: &Mutex<Vec<Op>>) -> Vec<Op> {
        sent.lock().unwrap().clone()
    }

    /// Waits until `op` has reached the connector.
    async fn wait_for_request(sent: &Mutex<Vec<Op>>, op: Op) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !sent.lock().unwrap().contains(&op) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{op:?} was never sent: {:?}", sent.lock().unwrap()));
    }

    async fn wait_for_status(handle: &UploadHandle, status: TransferStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while handle.status() != status {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("upload never reached {status:?}"));
    }

    type AbortFuture = BoxFuture<'static, Result<AbortedUpload, crate::error::Error>>;

    /// Starts `abort()` and polls it once, so `request_abort()` has run when
    /// this returns. The future is still waiting for the transfer to end.
    async fn begin_abort(handle: UploadHandle) -> AbortFuture {
        let mut abort: AbortFuture = Box::pin(handle.abort());
        let first = std::future::poll_fn(|cx| Poll::Ready(abort.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "abort ended before the gate was released"
        );
        abort
    }

    async fn finish(
        abort: impl Future<Output = Result<AbortedUpload, crate::error::Error>>,
    ) -> Result<AbortedUpload, crate::error::Error> {
        tokio::time::timeout(Duration::from_secs(5), abort)
            .await
            .expect("abort hung")
    }

    fn assert_cancelled(aborted: &AbortedUpload, upload_id: Option<&str>) {
        assert_eq!(aborted.outcome(), AbortOutcome::Cancelled);
        assert!(aborted.output().is_none());
        assert_eq!(aborted.upload_id(), upload_id);
    }

    /// Runs an abort scenario on both runtimes.
    macro_rules! on_both_runtimes {
        ($scenario:ident) => {
            mod $scenario {
                use crate::types::RuntimeMode;

                #[cfg_attr(miri, ignore)]
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn tokio_runtime() {
                    super::$scenario(RuntimeMode::MultiThreadTokio).await;
                }

                #[cfg_attr(miri, ignore)]
                #[tokio::test]
                async fn managed_runtime() {
                    super::$scenario(RuntimeMode::Managed).await;
                }
            }
        };
    }

    /// CreateMultipartUpload in flight, then 200: abort waits for it, aborts
    /// the upload it returned, and never starts a part.
    async fn abort_create_in_flight(runtime_mode: RuntimeMode) {
        let gate = Arc::new(Semaphore::new(0));
        let script = Script {
            create: Reply::gated(&gate, Reply::Ok),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Create).await;

        let abort = begin_abort(s.handle).await;
        gate.add_permits(1);
        let aborted = finish(abort).await.expect("abort failed");

        assert_cancelled(&aborted, Some("test-upload-id"));
        assert_eq!(sent(&s.sent), [Op::Create, Op::Abort]);
    }
    on_both_runtimes!(abort_create_in_flight);

    /// CreateMultipartUpload in flight, then an error: no upload exists, and
    /// abort returns Create's error.
    async fn abort_create_in_flight_then_create_fails(runtime_mode: RuntimeMode) {
        let gate = Arc::new(Semaphore::new(0));
        let script = Script {
            create: Reply::gated(&gate, Reply::Err(403, "AccessDenied")),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Create).await;

        let abort = begin_abort(s.handle).await;
        gate.add_permits(1);
        let err = finish(abort).await.expect_err("abort must fail");

        assert_eq!(err.code(), Some("AccessDenied"));
        assert_eq!(sent(&s.sent), [Op::Create]);
    }
    on_both_runtimes!(abort_create_in_flight_then_create_fails);

    /// UploadPart in flight (never answers): the part is interrupted, the
    /// upload is aborted, and Complete is never sent.
    async fn abort_part_in_flight(runtime_mode: RuntimeMode) {
        let script = Script {
            part: Reply::Stall,
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Part).await;

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_cancelled(&aborted, Some("test-upload-id"));
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part, Op::Abort]);
    }
    on_both_runtimes!(abort_part_in_flight);

    /// UploadPart in flight with `Retain`: the part is interrupted and the
    /// upload is left open.
    async fn abort_part_in_flight_with_retain(runtime_mode: RuntimeMode) {
        let script = Script {
            part: Reply::Stall,
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::MultipartRetain);
        wait_for_request(&s.sent, Op::Part).await;

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_cancelled(&aborted, None);
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part]);
    }
    on_both_runtimes!(abort_part_in_flight_with_retain);

    /// AbortMultipartUpload itself fails: abort returns its error.
    async fn abort_mpu_fails(runtime_mode: RuntimeMode) {
        let script = Script {
            part: Reply::Stall,
            abort: Reply::Err(403, "AccessDenied"),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Part).await;

        let err = finish(s.handle.abort()).await.expect_err("abort must fail");

        assert_eq!(err.code(), Some("AccessDenied"));
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part, Op::Abort]);
    }
    on_both_runtimes!(abort_mpu_fails);

    /// Starts an upload whose CompleteMultipartUpload waits on a gate, aborts
    /// it while Complete is on the wire, checks AbortMultipartUpload goes out
    /// before Complete answers, then lets Complete answer with `then`.
    async fn abort_complete_in_flight(
        runtime_mode: RuntimeMode,
        then: Reply,
    ) -> (Result<AbortedUpload, crate::error::Error>, Vec<Op>) {
        let gate = Arc::new(Semaphore::new(0));
        let script = Script {
            complete: Reply::gated(&gate, then),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Complete).await;

        let abort = begin_abort(s.handle).await;
        // The abort does not wait for Complete before aborting the upload.
        wait_for_request(&s.sent, Op::Abort).await;
        gate.add_permits(1);
        let result = finish(abort).await;
        (result, sent(&s.sent))
    }

    /// Complete in flight, then 200: S3 created the object.
    async fn abort_complete_in_flight_then_complete_succeeds(runtime_mode: RuntimeMode) {
        let (result, sent) = abort_complete_in_flight(runtime_mode, Reply::Ok).await;
        let aborted = result.expect("abort failed");

        assert_eq!(aborted.outcome(), AbortOutcome::AlreadyCompleted);
        assert!(aborted.output().is_some());
        assert_eq!(sent, [Op::Create, Op::Part, Op::Complete, Op::Abort]);
    }
    on_both_runtimes!(abort_complete_in_flight_then_complete_succeeds);

    /// Complete in flight, then `NoSuchUpload`: the abort won, no object.
    async fn abort_complete_in_flight_then_no_such_upload(runtime_mode: RuntimeMode) {
        let (result, sent) =
            abort_complete_in_flight(runtime_mode, Reply::Err(404, "NoSuchUpload")).await;
        let aborted = result.expect("abort failed");

        assert_cancelled(&aborted, Some("test-upload-id"));
        assert_eq!(sent, [Op::Create, Op::Part, Op::Complete, Op::Abort]);
    }
    on_both_runtimes!(abort_complete_in_flight_then_no_such_upload);

    /// Complete in flight, then any other error: abort returns that error.
    async fn abort_complete_in_flight_then_complete_fails(runtime_mode: RuntimeMode) {
        let (result, sent) =
            abort_complete_in_flight(runtime_mode, Reply::Err(400, "InvalidPart")).await;
        let err = result.expect_err("abort must fail");

        assert_eq!(err.code(), Some("InvalidPart"));
        assert_eq!(sent, [Op::Create, Op::Part, Op::Complete, Op::Abort]);
    }
    on_both_runtimes!(abort_complete_in_flight_then_complete_fails);

    /// PutObject in flight (never answers): it is interrupted and abort
    /// returns `Cancelled` without waiting for S3.
    async fn abort_put_object_in_flight(runtime_mode: RuntimeMode) {
        let script = Script {
            put: Reply::Stall,
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::PutObject);
        wait_for_request(&s.sent, Op::Put).await;

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_cancelled(&aborted, None);
        assert_eq!(sent(&s.sent), [Op::Put]);
    }
    on_both_runtimes!(abort_put_object_in_flight);

    /// Already completed: abort reports the object and sends nothing.
    async fn abort_after_completed(runtime_mode: RuntimeMode) {
        let s = start(runtime_mode, Script::default(), Shape::Multipart);
        wait_for_status(&s.handle, TransferStatus::Completed).await;

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_eq!(aborted.outcome(), AbortOutcome::AlreadyCompleted);
        assert!(aborted.output().is_some());
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part, Op::Complete]);
    }
    on_both_runtimes!(abort_after_completed);

    /// Already failed on a part: the failure left the upload open, and abort
    /// aborts it.
    async fn abort_after_part_failed(runtime_mode: RuntimeMode) {
        let script = Script {
            part: Reply::Err(403, "AccessDenied"),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_status(&s.handle, TransferStatus::Failed).await;
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part]);

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_cancelled(&aborted, Some("test-upload-id"));
        assert_eq!(sent(&s.sent), [Op::Create, Op::Part, Op::Abort]);
    }
    on_both_runtimes!(abort_after_part_failed);

    /// Already failed on Complete: same as a part failure.
    async fn abort_after_complete_failed(runtime_mode: RuntimeMode) {
        let script = Script {
            complete: Reply::Err(400, "InvalidPart"),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_status(&s.handle, TransferStatus::Failed).await;

        let aborted = finish(s.handle.abort()).await.expect("abort failed");

        assert_cancelled(&aborted, Some("test-upload-id"));
        assert_eq!(
            sent(&s.sent),
            [Op::Create, Op::Part, Op::Complete, Op::Abort]
        );
    }
    on_both_runtimes!(abort_after_complete_failed);

    /// Dropping the `abort()` future does not stop the abort: the upload
    /// Create returns later is still aborted.
    async fn abort_future_dropped(runtime_mode: RuntimeMode) {
        let gate = Arc::new(Semaphore::new(0));
        let script = Script {
            create: Reply::gated(&gate, Reply::Ok),
            ..Default::default()
        };
        let s = start(runtime_mode, script, Shape::Multipart);
        wait_for_request(&s.sent, Op::Create).await;

        drop(begin_abort(s.handle).await);
        gate.add_permits(1);
        wait_for_request(&s.sent, Op::Abort).await;

        assert_eq!(sent(&s.sent), [Op::Create, Op::Abort]);
    }
    on_both_runtimes!(abort_future_dropped);

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_abort_upload() {
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let create_mpu =
            mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
                CreateMultipartUploadOutput::builder()
                    .upload_id("test-upload-id")
                    .build()
            });

        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());

        let abort_mpu = mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .then_output(|| AbortMultipartUploadOutput::builder().build());

        // The test races abort against the upload completion. Under slow execution
        // (e.g. ASAN-instrumented), the single-part upload can reach
        // CompleteMultipartUpload before abort dispatches. Provide a rule for that
        // case so the mock doesn't panic on no-rule-matched, which would crash a
        // managed thread and leave Arc cycles for LeakSanitizer to flag.
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .then_output(|| CompleteMultipartUploadOutput::builder().build());

        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            &[create_mpu, upload_part, abort_mpu, complete_mpu]
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(client)
            .build();

        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);
        let handle = request.initiate_with(&tm).unwrap();

        // Small delay to let scheduler start processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Abort should complete without error
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.abort())
            .await
            .expect("abort timed out");
        assert!(result.is_ok());
    }
}

/// Integration-style tests using StaticReplayClient for retry behavior
#[cfg(test)]
mod retry_tests {
    use aws_sdk_s3::config::Region;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;

    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::types::{ConcurrencyMode, PartSize};

    fn dummy_request() -> http::Request<SdkBody> {
        http::Request::builder().body(SdkBody::empty()).unwrap()
    }

    /// Test that SDK retries transient errors for upload_part.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_upload_part_retry() {
        // Responses in order: CreateMPU, UploadPart (500), UploadPart (200 retry), CompleteMPU
        let http_client = StaticReplayClient::new(vec![
            // CreateMultipartUpload - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <InitiateMultipartUploadResult>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <UploadId>test-upload-id</UploadId>
                        </InitiateMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
            // UploadPart - first attempt fails with 500
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(500)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>InternalError</Code>
                            <Message>Internal Server Error</Message>
                        </Error>"#,
                    ))
                    .unwrap(),
            ),
            // UploadPart - retry succeeds
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .header("ETag", "\"test-etag\"")
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // CompleteMultipartUpload - success
            ReplayEvent::new(
                dummy_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult>
                            <Location>https://test-bucket.s3.amazonaws.com/test-key</Location>
                            <Bucket>test-bucket</Bucket>
                            <Key>test-key</Key>
                            <ETag>"final-etag"</ETag>
                        </CompleteMultipartUploadResult>"#,
                    ))
                    .unwrap(),
            ),
        ]);

        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .http_client(http_client.clone())
                .region(Region::from_static("us-west-2"))
                .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
                .with_test_defaults()
                .build(),
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencyMode::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .sdk_client(s3_client)
            .build();

        let tm = crate::Client::new(tm_config);

        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream)
            .initiate()
            .unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.join())
            .await
            .expect("join timed out");
        assert!(
            result.is_ok(),
            "upload should succeed after retry: {:?}",
            result.err()
        );

        // Verify all 4 requests were made (including the retry)
        let requests: Vec<_> = http_client.actual_requests().collect();
        assert_eq!(
            4,
            requests.len(),
            "should have made 4 requests (including retry)"
        );
    }
}
