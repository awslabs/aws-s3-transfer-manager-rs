/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Client-side connector that injects a dispatch-layer IO error.
//!
//! The CI failure was `hyper::Error(BodyWrite, Os { code: 55 })` (`ENOBUFS`) on
//! an `UploadPart` send: the client's kernel could not allocate socket buffer
//! space while writing the request body. The SDK surfaces that as
//! `SdkError::DispatchFailure(ConnectorError { kind: Io, .. })`. The mock server's
//! `socket_fault` wraps the *server* socket, so it cannot reproduce a *client*
//! send failure; this connector injects at the client connector instead, which is
//! the faithful side of the wire.
//!
//! What the retry path keys on is `ConnectorError::is_io()`, so returning
//! `ConnectorError::io(..)` from `call` on a targeted attempt reproduces the
//! classification-relevant shape exactly. Non-targeted requests delegate to a
//! real loopback HTTP connector, so a retried `UploadPart` actually succeeds and
//! a test can assert recovery (transient) vs exhaustion (persistent).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use aws_smithy_http_client::Connector;
use aws_smithy_runtime_api::client::http::{
    http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;

/// How many matching requests to fail with an injected IO error before letting
/// them through to the real connector.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FailCount {
    /// Fail the first `n` matching requests, then pass through. `n = 1` models a
    /// transient blip that a retry recovers. Note: this faults the first `n`
    /// dispatches *globally*, so a large `n` keeps faulting across retries — a
    /// sustained outage, not a transient burst.
    First(u32),
    /// Fail every matching request. Models a persistent condition that exhausts
    /// the retry budget.
    Always,
    /// Fail the FIRST dispatch of each distinct part exactly once, then let
    /// every re-issue of that part through. This is the faithful transient-burst
    /// model: every part's initial write fails (as under mbuf saturation), but
    /// the pressure clears so a retry of the same part succeeds. Requires the
    /// request to carry a `partNumber` (UploadPart); requests without one are
    /// never faulted.
    EachPartOnce,
}

/// Shared counter of how many requests this connector has failed. Lets a test
/// assert the injection actually fired (guards against a test that passes because
/// the fault never triggered).
#[derive(Debug, Clone, Default)]
pub(crate) struct InjectionTally(Arc<AtomicU32>);

impl InjectionTally {
    pub(crate) fn count(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct IoFaultConnector {
    inner: SharedHttpConnector,
    /// Fail only requests whose method+query match this predicate. `None` matches
    /// every request.
    matches: fn(&HttpRequest) -> bool,
    fail: FailCount,
    failed: InjectionTally,
    /// Per-part dispatch counts, for the `EachPartOnce` fault mode.
    per_part: PerPartCounts,
}

impl HttpConnector for IoFaultConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        // Record this part's dispatch and capture its new count (1 = first time).
        let dispatch_count = part_number(&request).map(|pn| {
            let mut map = self.per_part.lock().unwrap();
            let c = map.entry(pn).or_insert(0);
            *c += 1;
            *c
        });
        let should_fail = (self.matches)(&request)
            && match self.fail {
                FailCount::Always => {
                    self.failed.0.fetch_add(1, Ordering::Relaxed);
                    true
                }
                FailCount::First(n) => {
                    // Atomically claim a failure slot: increment iff still below n.
                    self.failed
                        .0
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                            (c < n).then_some(c + 1)
                        })
                        .is_ok()
                }
                FailCount::EachPartOnce => {
                    // Fault only this part's FIRST dispatch; re-issues pass through.
                    let first = dispatch_count == Some(1);
                    if first {
                        self.failed.0.fetch_add(1, Ordering::Relaxed);
                    }
                    first
                }
            };
        if should_fail {
            return HttpConnectorFuture::ready(Err(ConnectorError::io(
                // errno 55 = ENOBUFS on macOS, matching the CI failure source.
                std::io::Error::from_raw_os_error(55).into(),
            )));
        }
        self.inner.call(request)
    }
}

/// `true` for an `UploadPart` request: a PUT carrying a `partNumber` query param.
pub(crate) fn is_upload_part(req: &HttpRequest) -> bool {
    req.method() == "PUT" && req.uri().contains("partNumber=")
}

/// Extract the `partNumber` query value from an UploadPart URI, if present.
pub(crate) fn part_number(req: &HttpRequest) -> Option<u32> {
    req.uri()
        .split(&['?', '&'][..])
        .find_map(|kv| kv.strip_prefix("partNumber="))
        .and_then(|v| v.parse().ok())
}

/// Per-part dispatch counter, keyed by `partNumber`. Internal to the connector:
/// the `EachPartOnce` fault mode uses it to fault only each part's first
/// dispatch and let re-issues through.
type PerPartCounts = Arc<std::sync::Mutex<std::collections::HashMap<u32, u32>>>;

/// Build an `http_client` that injects a dispatch IO error on requests matching
/// `matches`, delegating everything else to a real loopback HTTP connector.
/// Returns the client and a tally of injected failures.
pub(crate) fn io_fault_http_client(
    matches: fn(&HttpRequest) -> bool,
    fail: FailCount,
) -> (SharedHttpClient, InjectionTally) {
    let failed = InjectionTally::default();
    let tally = failed.clone();
    let per_part: PerPartCounts = Default::default();
    let client = http_client_fn(move |settings, _components| {
        // Plain HTTP connector for the non-TLS loopback mock. Built per
        // selection call; the SDK caches the selection.
        let inner = SharedHttpConnector::new(
            Connector::builder()
                .connector_settings(settings.clone())
                .build_http(),
        );
        SharedHttpConnector::new(IoFaultConnector {
            inner,
            matches,
            fail,
            failed: failed.clone(),
            per_part: per_part.clone(),
        })
    });
    (client, tally)
}
