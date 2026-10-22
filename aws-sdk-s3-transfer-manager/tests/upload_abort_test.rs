/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// ref: awslabs/aws-s3-transfer-manager-rs#211

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3_transfer_manager::error::ErrorKind;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::types::RuntimeMode;
use futures_util::poll;

use test_util::{client, ReleaseRequestDrop, RequestState, PAYLOAD};

async fn abort_while_joining(runtime_mode: RuntimeMode) {
    let state = Arc::new(RequestState::new(false));
    // Release the runtime thread even if an assertion or the watchdog fails.
    let release = ReleaseRequestDrop(Arc::clone(&state));
    let tm = client(runtime_mode, Arc::clone(&state));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("blocked-put")
        .body(InputStream::from_static(PAYLOAD))
        .initiate()
        .unwrap();
    let abort_handle = handle.abort_handle();
    let resume_abort_handle = abort_handle.clone();
    let mut join = Box::pin(handle.join());
    assert!(poll!(join.as_mut()).is_pending());

    state.started.acquire().await.unwrap().forget();
    assert!(state.active.load(Ordering::SeqCst));
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);

    // No response permit is supplied: cancellation must interrupt the genuinely
    // pending connector future, rather than wait for the request to succeed.
    let mut abort = Box::pin(abort_handle.abort());
    assert!(poll!(abort.as_mut()).is_pending());
    state.drop_started.acquire().await.unwrap().forget();
    // Cancelling the abort waiter must not discard the idle receipt. At the
    // next join poll, no abort future remains to keep that receipt alive.
    drop(abort);

    // The connector's destructor still owns the request and its source body.
    // Terminal notification alone must not release the consuming join.
    assert!(state.active.load(Ordering::SeqCst));
    assert!(!state.dropped.load(Ordering::SeqCst));
    assert!(poll!(join.as_mut()).is_pending());
    let mut abort = Box::pin(resume_abort_handle.abort());
    assert!(poll!(abort.as_mut()).is_pending());

    drop(release);
    // Check each completion independently; awaiting abort first would hide a
    // join that reports cancellation before managed request custody ends.
    let ((), ()) = tokio::join!(
        async {
            let error = join.await.expect_err("interrupted upload must not succeed");
            assert_eq!(*error.kind(), ErrorKind::OperationCancelled);
            assert!(state.dropped.load(Ordering::SeqCst));
            assert!(!state.active.load(Ordering::SeqCst));
        },
        async {
            abort.await.expect("owned abort must finish");
            assert!(state.dropped.load(Ordering::SeqCst));
            assert!(!state.active.load(Ordering::SeqCst));
        },
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
    assert!(state.observed_body.lock().is_empty());
}

async fn abort_after_dropping_join(runtime_mode: RuntimeMode) {
    let state = Arc::new(RequestState::new(false));
    let release = ReleaseRequestDrop(Arc::clone(&state));
    let tm = client(runtime_mode, Arc::clone(&state));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("dropped-join-put")
        .body(InputStream::from_static(PAYLOAD))
        .initiate()
        .unwrap();
    let abort_handle = handle.abort_handle();
    let mut join = Box::pin(handle.join());
    assert!(poll!(join.as_mut()).is_pending());
    state.started.acquire().await.unwrap().forget();

    // Dropping the consuming join initiates cancellation before the surviving
    // control has ever called abort. Its idle receipt must survive that drop.
    drop(join);
    state.drop_started.acquire().await.unwrap().forget();
    assert!(state.active.load(Ordering::SeqCst));
    assert!(!state.dropped.load(Ordering::SeqCst));
    let mut abort = Box::pin(abort_handle.abort());
    assert!(poll!(abort.as_mut()).is_pending());

    drop(release);
    abort
        .await
        .expect("owned abort must wait after join is dropped");
    assert!(state.dropped.load(Ordering::SeqCst));
    assert!(!state.active.load(Ordering::SeqCst));
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
    assert!(state.observed_body.lock().is_empty());
}

async fn drop_abort_controls_while_joining(runtime_mode: RuntimeMode) {
    let state = Arc::new(RequestState::new(true));
    let tm = client(runtime_mode, Arc::clone(&state));
    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("uncancelled-put")
        .body(InputStream::from_static(PAYLOAD))
        .initiate()
        .unwrap();
    let abort_handle = handle.abort_handle();
    let extra_abort_handle = abort_handle.clone();
    let mut join = Box::pin(handle.join());
    assert!(poll!(join.as_mut()).is_pending());
    state.started.acquire().await.unwrap().forget();

    // Drop every independent control while the original handle is owned by
    // the actively polled join. None may inherit UploadHandle's cancel-on-drop.
    drop(extra_abort_handle);
    drop(abort_handle);
    assert!(poll!(join.as_mut()).is_pending());
    state.respond.add_permits(1);

    let output = join.await.expect("dropping abort controls must not cancel");
    assert_eq!(output.e_tag(), Some("uploaded-etag"));
    assert_eq!(output.metrics().total_bytes, Some(PAYLOAD.len() as u64));
    assert_eq!(&*state.observed_body.lock(), PAYLOAD);
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
    assert!(state.dropped.load(Ordering::SeqCst));
    assert!(!state.active.load(Ordering::SeqCst));
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_abort_interrupts_request_during_join_on_tokio_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        abort_while_joining(RuntimeMode::MultiThreadTokio),
    )
    .await
    .expect("owned abort or join hung with a blocked request");
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_abort_interrupts_request_during_join_on_managed_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        abort_while_joining(RuntimeMode::Managed),
    )
    .await
    .expect("owned abort or join hung with a blocked request");
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_abort_controls_preserves_upload_on_tokio_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        drop_abort_controls_while_joining(RuntimeMode::MultiThreadTokio),
    )
    .await
    .expect("upload hung after its response was released");
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_abort_controls_preserves_upload_on_managed_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        drop_abort_controls_while_joining(RuntimeMode::Managed),
    )
    .await
    .expect("upload hung after its response was released");
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_abort_waits_after_dropping_join_on_tokio_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        abort_after_dropping_join(RuntimeMode::MultiThreadTokio),
    )
    .await
    .expect("owned abort hung after dropping the consuming join");
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_abort_waits_after_dropping_join_on_managed_runtime() {
    tokio::time::timeout(
        Duration::from_secs(10),
        abort_after_dropping_join(RuntimeMode::Managed),
    )
    .await
    .expect("owned abort hung after dropping the consuming join");
}

mod test_util {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, RuntimeMode};
    use aws_sdk_s3_transfer_manager::{Client, Config};
    use aws_smithy_runtime_api::client::http::{
        http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_types::body::SdkBody;
    use futures_util::future::poll_fn;
    use http_body_1x::Body;
    use parking_lot::{Condvar, Mutex};
    use tokio::sync::Semaphore;

    pub const PAYLOAD: &[u8] = b"owned upload source";

    #[derive(Debug)]
    pub struct RequestState {
        pub started: Semaphore,
        pub drop_started: Semaphore,
        pub respond: Semaphore,
        pub active: AtomicBool,
        pub dropped: AtomicBool,
        pub requests: AtomicUsize,
        pub observed_body: Mutex<Vec<u8>>,
        drop_released: Mutex<bool>,
        drop_release: Condvar,
    }

    impl RequestState {
        pub fn new(drop_released: bool) -> Self {
            Self {
                started: Semaphore::new(0),
                drop_started: Semaphore::new(0),
                respond: Semaphore::new(0),
                active: AtomicBool::new(false),
                dropped: AtomicBool::new(false),
                requests: AtomicUsize::new(0),
                observed_body: Mutex::new(Vec::new()),
                drop_released: Mutex::new(drop_released),
                drop_release: Condvar::new(),
            }
        }
    }

    pub struct ReleaseRequestDrop(pub Arc<RequestState>);

    impl Drop for ReleaseRequestDrop {
        fn drop(&mut self) {
            *self.0.drop_released.lock() = true;
            self.0.drop_release.notify_all();
        }
    }

    struct InFlightRequest {
        request: Option<HttpRequest>,
        state: Arc<RequestState>,
    }

    impl Drop for InFlightRequest {
        fn drop(&mut self) {
            self.state.drop_started.add_permits(1);
            let mut released = self.state.drop_released.lock();
            while !*released {
                self.state.drop_release.wait(&mut released);
            }
            // Publish release only after the actual request/body is gone, not
            // merely when cancellation starts dropping the connector future.
            drop(self.request.take());
            self.state.active.store(false, Ordering::SeqCst);
            self.state.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct BlockedPutConnector(Arc<RequestState>);

    impl HttpConnector for BlockedPutConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            assert_eq!(request.method(), "PUT");
            assert!(!request.uri().contains("uploadId="));
            let state = Arc::clone(&self.0);
            HttpConnectorFuture::new(async move {
                let mut in_flight = InFlightRequest {
                    request: Some(request),
                    state: Arc::clone(&state),
                };
                assert!(!state.active.swap(true, Ordering::SeqCst));
                state.requests.fetch_add(1, Ordering::SeqCst);
                state.started.add_permits(1);
                state.respond.acquire().await.unwrap().forget();

                let body = in_flight.request.as_mut().unwrap().body_mut();
                while let Some(frame) = poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await {
                    if let Ok(data) = frame.unwrap().into_data() {
                        state.observed_body.lock().extend_from_slice(&data);
                    }
                }
                let mut response = HttpResponse::new(200.try_into().unwrap(), SdkBody::empty());
                response.headers_mut().insert("etag", "uploaded-etag");
                Ok(response)
            })
        }
    }

    pub fn client(runtime_mode: RuntimeMode, state: Arc<RequestState>) -> Client {
        let connector = SharedHttpConnector::new(BlockedPutConnector(state));
        Client::new(
            Config::builder()
                .concurrency(ConcurrencyMode::Explicit(1))
                .runtime_mode(runtime_mode)
                .s3_config(crate::common::s3_config(http_client_fn(move |_, _| {
                    connector.clone()
                })))
                .build(),
        )
    }
}
