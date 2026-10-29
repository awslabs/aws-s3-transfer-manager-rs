/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic, traceable fault injection for the mock server.
//!
//! Faults are registered per [`FaultTarget`] as an ordered queue (a *script*
//! consumed over successive matching requests). Targeting and timing are counted,
//! never random, so a test's outcome is reproducible and every fired fault is
//! logged with the request number that triggered it.

use std::sync::Mutex;

use crate::Operation;

/// Body delivery cadence for [`FaultType::PaceBody`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyCadence {
    /// Wait this long before each piece after the first, then deliver it; the
    /// body runs to EOF. Throughput stays positive, so this models a healthy but
    /// slow link — distinct from a stall.
    Slow(std::time::Duration),
    /// Deliver the first piece, then never yield again and never EOF (zero
    /// throughput). Stalled-stream protection observes this as a stall.
    Stall,
}

/// What a fault does to a matching request. One effect per request.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum FaultType {
    /// GET returns a valid-format but incorrect `x-amz-checksum-*` (first byte
    /// XOR'd). Drives a download checksum mismatch.
    WrongStoredChecksum,
    /// GET returns tampered body bytes (byte 0 XOR'd) with the checksum left
    /// intact. Exercises body-content validation, not just header comparison.
    CorruptBody,
    /// GET yields `after_bytes` body bytes, then errors the body stream, so hyper
    /// aborts the connection. The client observes a connection reset / incomplete
    /// body mid-stream.
    TruncateBody { after_bytes: u64 },
    /// GET delivers the body in `piece_bytes`-sized pieces on a cadence, modeling a
    /// slow or dead link rather than an errored stream. The cadence decides whether
    /// the body eventually completes.
    PaceBody {
        piece_bytes: u64,
        cadence: BodyCadence,
    },
    /// GET ends the body stream cleanly after `actual_bytes`, fewer than the
    /// Content-Length header advertises. The client observes a length mismatch
    /// (body shorter than declared / unexpected EOF).
    ShortBody { actual_bytes: u64 },
    /// GET aborts the connection with a TCP RST after `after_bytes` further bytes
    /// are written to the client (counted from when the fault fires). The client
    /// observes a `ConnectionReset`.
    ConnectionReset { after_bytes: u64 },
    /// The request fails with this error before it is served, e.g.
    /// [`Error::SlowDown`](crate::Error::SlowDown). Unlike the body-stream faults, this
    /// reaches the SDK's retry and token-bucket layer rather than the TM body-read loop.
    /// Retryable errors (500/503) drive the SDK to spend retry tokens.
    ///
    /// The response is the error's S3 error response: its code, HTTP status and message.
    /// Storage is not read or modified, and the request body is not read.
    /// [`FaultType::internal_error`] and [`FaultType::slow_down`] build the two retryable errors
    /// S3 sends.
    Error(crate::Error),
    /// While reading the request (upload) body, consume up to `after_bytes` then
    /// stall (stop reading, never complete). The client's request-body poll parks
    /// indefinitely, which stalled-stream protection observes as a stall.
    StallRequestRead { after_bytes: u64 },
    /// Read the whole request, then never send the response status line or
    /// headers. An upload body is fully consumed and stored first. The connection
    /// stays open with no reset and no EOF, so the client waits for a response
    /// that never arrives. Unlike [`StallRequestRead`](Self::StallRequestRead),
    /// the request body is never stalled. Applies to GetObject, PutObject and
    /// UploadPart.
    WithholdResponseHeaders,
}

/// How many times an eligible fault fires before it is consumed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Occurrence {
    /// Fire once, then pop the entry.
    Once,
    /// Fire `n` times, then pop the entry.
    NTimes(u32),
    /// Fire forever; never popped.
    Always,
}

/// The requests a fault queue applies to.
///
/// A target matches a request when the request's operation is one the target
/// covers and, where the target names them, the request's bucket and key are
/// equal to the target's. The key of a ListObjectsV2 request is its prefix,
/// bucket operations have no key, and ListBuckets has neither a bucket nor a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultTarget {
    operations: Operations,
    bucket: Option<String>,
    key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operations {
    /// GetObject, PutObject and UploadPart: the requests that read or write an
    /// object's data.
    ObjectData,
    One(Operation),
}

impl FaultTarget {
    /// GetObject, PutObject and UploadPart requests for `key` in `bucket`. These
    /// are the requests [`S3MockServer::insert_fault`](crate::S3MockServer::insert_fault)
    /// targets.
    pub fn object(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            operations: Operations::ObjectData,
            bucket: Some(bucket.into()),
            key: Some(key.into()),
        }
    }

    /// Requests for `operation`, in any bucket and for any key.
    pub fn operation(operation: Operation) -> Self {
        Self {
            operations: Operations::One(operation),
            bucket: None,
            key: None,
        }
    }

    /// Restrict the target to requests for `bucket`. Requests without a bucket do
    /// not match.
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Restrict the target to requests for `key`. Requests without a key do not
    /// match.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    fn matches(&self, operation: Operation, bucket: Option<&str>, key: Option<&str>) -> bool {
        let operation_matches = match self.operations {
            Operations::ObjectData => matches!(
                operation,
                Operation::GetObject | Operation::PutObject | Operation::UploadPart
            ),
            Operations::One(target) => target == operation,
        };
        operation_matches
            && self.bucket.as_deref().is_none_or(|b| Some(b) == bucket)
            && self.key.as_deref().is_none_or(|k| Some(k) == key)
    }
}

impl FaultType {
    /// An [`Error`](Self::Error) fault with 500 `InternalError`, an internal error of the server.
    pub fn internal_error() -> Self {
        FaultType::Error(crate::Error::Internal("injected fault".to_string()))
    }

    /// An [`Error`](Self::Error) fault with 503 `SlowDown`, S3's throttling error.
    pub const fn slow_down() -> Self {
        FaultType::Error(crate::Error::SlowDown)
    }
}

#[derive(Debug, Clone)]
struct FaultEntry {
    fault: FaultType,
    /// Warm-up: matching requests before this entry becomes eligible, measured
    /// against the monotonic per-target request counter (absolute).
    skip: u32,
    occurrence: Occurrence,
    /// Remaining fires for `NTimes`; ignored for `Once`/`Always`.
    remaining: u32,
}

#[derive(Debug)]
struct TargetState {
    target: FaultTarget,
    /// Monotonic count of matching requests seen for this target.
    seen: u32,
    queue: std::collections::VecDeque<FaultEntry>,
}

/// Target-scoped fault registry. Cheap to clone the `Arc` that holds it.
#[derive(Debug, Default)]
pub(crate) struct FaultRegistry {
    /// One queue per distinct target, in the order the targets were first
    /// registered.
    targets: Mutex<Vec<TargetState>>,
}

impl FaultRegistry {
    /// Append a fault to the queue of `target`.
    pub(crate) fn insert(
        &self,
        target: FaultTarget,
        fault: FaultType,
        skip: u32,
        occurrence: Occurrence,
    ) {
        let remaining = match occurrence {
            Occurrence::NTimes(n) => n,
            _ => 0,
        };
        let entry = FaultEntry {
            fault,
            skip,
            occurrence,
            remaining,
        };
        let mut targets = self.targets.lock().unwrap();
        match targets.iter_mut().find(|state| state.target == target) {
            Some(state) => state.queue.push_back(entry),
            None => targets.push(TargetState {
                target,
                seen: 0,
                queue: [entry].into(),
            }),
        }
    }

    /// Drop the queue of `target`, and its request counter.
    pub(crate) fn clear(&self, target: &FaultTarget) {
        self.targets
            .lock()
            .unwrap()
            .retain(|state| state.target != *target);
    }

    /// Drop every queue and request counter.
    pub(crate) fn clear_all(&self) {
        self.targets.lock().unwrap().clear();
    }

    /// Record a request and return the fault to apply, if any.
    ///
    /// Increments the request counter of every target that matches the request,
    /// then consults the head entry of each matching queue in registration order:
    /// the first one with `seen > skip` fires and is decremented or popped per its
    /// occurrence. Later queues still count the request but do not fire for it.
    /// Emits a `tracing` event on every fire for reproducibility.
    pub(crate) fn next_fault(
        &self,
        operation: Operation,
        bucket: Option<&str>,
        key: Option<&str>,
    ) -> Option<FaultType> {
        let mut targets = self.targets.lock().unwrap();
        let mut fired = None;
        for state in targets
            .iter_mut()
            .filter(|state| state.target.matches(operation, bucket, key))
        {
            state.seen += 1;
            if fired.is_some() {
                continue;
            }
            let seen = state.seen;
            let Some(entry) = state.queue.front_mut() else {
                continue;
            };
            if seen <= entry.skip {
                continue;
            }
            let fault = entry.fault.clone();
            let occurrence = entry.occurrence;
            let pop = match entry.occurrence {
                Occurrence::Once => true,
                Occurrence::NTimes(_) => {
                    entry.remaining = entry.remaining.saturating_sub(1);
                    entry.remaining == 0
                }
                Occurrence::Always => false,
            };
            if pop {
                state.queue.pop_front();
            }
            tracing::warn!(
                target: "s3_mock_server::fault",
                ?operation, ?bucket, ?key, request = seen, ?fault, ?occurrence,
                "fault fired",
            );
            fired = Some(fault);
        }
        fired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg() -> FaultRegistry {
        FaultRegistry::default()
    }

    #[test]
    fn always_fires_every_request() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::Always,
        );
        for _ in 0..5 {
            assert!(matches!(
                r.next_fault(Operation::GetObject, Some("b"), Some("k")),
                Some(FaultType::CorruptBody)
            ));
        }
    }

    #[test]
    fn once_fires_exactly_once() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::Once,
        );
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::CorruptBody)
        ));
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none());
    }

    #[test]
    fn ntimes_fires_n_then_stops() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::NTimes(2),
        );
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_some());
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_some());
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none());
    }

    #[test]
    fn skip_warms_up_against_monotonic_counter() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::WrongStoredChecksum,
            2,
            Occurrence::Always,
        );
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none()); // req 1
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none()); // req 2
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_some()); // req 3
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_some()); // req 4
    }

    #[test]
    fn queue_consumed_in_order() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::Once,
        );
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::WrongStoredChecksum,
            0,
            Occurrence::Always,
        );
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::CorruptBody)
        ));
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::WrongStoredChecksum)
        ));
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::WrongStoredChecksum)
        ));
    }

    #[test]
    fn clear_drops_queue() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::Always,
        );
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_some());
        r.clear(&FaultTarget::object("b", "k"));
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none());
    }

    #[test]
    fn faults_are_key_scoped() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k1"),
            FaultType::CorruptBody,
            0,
            Occurrence::Always,
        );
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k2"))
            .is_none());
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k1"))
            .is_some());
    }

    #[test]
    fn object_target_matches_object_data_requests_for_its_key() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            1,
            Occurrence::Always,
        );
        // Requests that the target does not cover neither count nor fire.
        for (operation, bucket, key) in [
            (Operation::HeadObject, "b", "k"),
            (Operation::GetObject, "other", "k"),
            (Operation::GetObject, "b", "other"),
        ] {
            assert!(r.next_fault(operation, Some(bucket), Some(key)).is_none());
        }
        // The first covered request is the skipped one; the next ones fire.
        assert!(r
            .next_fault(Operation::PutObject, Some("b"), Some("k"))
            .is_none());
        for operation in [Operation::UploadPart, Operation::GetObject] {
            assert!(matches!(
                r.next_fault(operation, Some("b"), Some("k")),
                Some(FaultType::CorruptBody)
            ));
        }
    }

    #[test]
    fn operation_target_matches_any_bucket_and_key_unless_restricted() {
        let r = reg();
        r.insert(
            FaultTarget::operation(Operation::ListObjectsV2),
            FaultType::Error(crate::Error::AccessDenied),
            0,
            Occurrence::Always,
        );
        assert!(matches!(
            r.next_fault(Operation::ListObjectsV2, Some("b"), None),
            Some(FaultType::Error(crate::Error::AccessDenied))
        ));
        assert!(matches!(
            r.next_fault(Operation::ListObjectsV2, Some("other"), Some("prefix/")),
            Some(FaultType::Error(crate::Error::AccessDenied))
        ));
        assert!(r
            .next_fault(Operation::HeadObject, Some("b"), Some("k"))
            .is_none());

        let r = reg();
        r.insert(
            FaultTarget::operation(Operation::HeadObject)
                .bucket("b")
                .key("k"),
            FaultType::Error(crate::Error::AccessDenied),
            0,
            Occurrence::Always,
        );
        assert!(matches!(
            r.next_fault(Operation::HeadObject, Some("b"), Some("k")),
            Some(FaultType::Error(crate::Error::AccessDenied))
        ));
        assert!(r
            .next_fault(Operation::HeadObject, Some("b"), Some("x"))
            .is_none());
        assert!(r
            .next_fault(Operation::HeadObject, Some("x"), Some("k"))
            .is_none());
        assert!(r
            .next_fault(Operation::HeadObject, Some("b"), None)
            .is_none());
    }

    #[test]
    fn first_registered_matching_target_fires_and_every_match_counts() {
        let r = reg();
        r.insert(
            FaultTarget::operation(Operation::GetObject).key("k"),
            FaultType::internal_error(),
            0,
            Occurrence::Once,
        );
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            1,
            Occurrence::Once,
        );
        // Both targets count the first request; only the first target fires.
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::Error(crate::Error::Internal(_)))
        ));
        // The second target has seen one request, so its skip of 1 is used up.
        assert!(matches!(
            r.next_fault(Operation::GetObject, Some("b"), Some("k")),
            Some(FaultType::CorruptBody)
        ));
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none());
    }

    #[test]
    fn clear_all_drops_every_target() {
        let r = reg();
        r.insert(
            FaultTarget::object("b", "k"),
            FaultType::CorruptBody,
            0,
            Occurrence::Always,
        );
        r.insert(
            FaultTarget::operation(Operation::HeadObject),
            FaultType::slow_down(),
            0,
            Occurrence::Always,
        );
        r.clear_all();
        assert!(r
            .next_fault(Operation::GetObject, Some("b"), Some("k"))
            .is_none());
        assert!(r
            .next_fault(Operation::HeadObject, Some("b"), Some("k"))
            .is_none());
    }

    #[test]
    fn helpers_fail_with_retryable_errors() {
        let FaultType::Error(error) = FaultType::internal_error() else {
            panic!("internal_error is an Error fault");
        };
        let error = s3s::S3Error::from(error);
        assert_eq!(error.code(), &s3s::S3ErrorCode::InternalError);
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::INTERNAL_SERVER_ERROR)
        );

        let FaultType::Error(error) = FaultType::slow_down() else {
            panic!("slow_down is an Error fault");
        };
        let error = s3s::S3Error::from(error);
        assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );
    }
}
