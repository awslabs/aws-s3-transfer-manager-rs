/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic, traceable fault injection for the mock server.
//!
//! Faults are registered per `(bucket, key)` as an ordered queue (a *script*
//! consumed over successive matching requests). Targeting and timing are counted,
//! never random, so a test's outcome is reproducible and every fired fault is
//! logged with the request number that triggered it.

use std::collections::HashMap;
use std::sync::Mutex;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// GET returns an HTTP error status at send time (before any body), e.g. 503
    /// SlowDown. Unlike the body-stream faults, this reaches the SDK's retry and
    /// token-bucket layer rather than the TM body-read loop. Retryable statuses
    /// (500/503) drive the SDK to spend retry tokens.
    ServiceError { status: u16 },
    /// While reading the request (upload) body, consume up to `after_bytes` then
    /// stall (stop reading, never complete). The client's request-body poll parks
    /// indefinitely, which stalled-stream protection observes as a stall.
    StallRequestRead { after_bytes: u64 },
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

#[derive(Debug, Clone, Copy)]
struct FaultEntry {
    fault: FaultType,
    /// Warm-up: matching requests before this entry becomes eligible, measured
    /// against the monotonic per-key request counter (absolute).
    skip: u32,
    occurrence: Occurrence,
    /// Remaining fires for `NTimes`; ignored for `Once`/`Always`.
    remaining: u32,
}

#[derive(Debug, Default)]
struct KeyState {
    /// Monotonic count of matching requests seen for this key.
    seen: u32,
    queue: std::collections::VecDeque<FaultEntry>,
}

/// Key-scoped fault registry. Cheap to clone the `Arc` that holds it.
#[derive(Debug, Default)]
pub(crate) struct FaultRegistry {
    keys: Mutex<HashMap<(String, String), KeyState>>,
}

impl FaultRegistry {
    /// Append a fault to the `(bucket, key)` queue.
    pub(crate) fn insert(
        &self,
        bucket: &str,
        key: &str,
        fault: FaultType,
        skip: u32,
        occurrence: Occurrence,
    ) {
        let remaining = match occurrence {
            Occurrence::NTimes(n) => n,
            _ => 0,
        };
        self.keys
            .lock()
            .unwrap()
            .entry((bucket.to_string(), key.to_string()))
            .or_default()
            .queue
            .push_back(FaultEntry {
                fault,
                skip,
                occurrence,
                remaining,
            });
    }

    /// Drop the entire fault queue for `(bucket, key)`.
    pub(crate) fn clear(&self, bucket: &str, key: &str) {
        self.keys
            .lock()
            .unwrap()
            .remove(&(bucket.to_string(), key.to_string()));
    }

    /// Record a matching request and return the fault to apply, if any.
    ///
    /// Increments the per-key request counter, then consults the head entry:
    /// fires only once `seen > skip`; decrements/pops per occurrence. Emits a
    /// `tracing` event on every fire for reproducibility.
    pub(crate) fn next_fault(&self, bucket: &str, key: &str) -> Option<FaultType> {
        let mut keys = self.keys.lock().unwrap();
        let state = keys.get_mut(&(bucket.to_string(), key.to_string()))?;
        state.seen += 1;
        let seen = state.seen;
        let entry = state.queue.front_mut()?;
        if seen <= entry.skip {
            return None;
        }
        let fault = entry.fault;
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
            %bucket, %key, request = seen, ?fault, ?occurrence,
            "fault fired",
        );
        Some(fault)
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
        r.insert("b", "k", FaultType::CorruptBody, 0, Occurrence::Always);
        for _ in 0..5 {
            assert_eq!(r.next_fault("b", "k"), Some(FaultType::CorruptBody));
        }
    }

    #[test]
    fn once_fires_exactly_once() {
        let r = reg();
        r.insert("b", "k", FaultType::CorruptBody, 0, Occurrence::Once);
        assert_eq!(r.next_fault("b", "k"), Some(FaultType::CorruptBody));
        assert_eq!(r.next_fault("b", "k"), None);
    }

    #[test]
    fn ntimes_fires_n_then_stops() {
        let r = reg();
        r.insert("b", "k", FaultType::CorruptBody, 0, Occurrence::NTimes(2));
        assert!(r.next_fault("b", "k").is_some());
        assert!(r.next_fault("b", "k").is_some());
        assert_eq!(r.next_fault("b", "k"), None);
    }

    #[test]
    fn skip_warms_up_against_monotonic_counter() {
        let r = reg();
        r.insert(
            "b",
            "k",
            FaultType::WrongStoredChecksum,
            2,
            Occurrence::Always,
        );
        assert_eq!(r.next_fault("b", "k"), None); // req 1
        assert_eq!(r.next_fault("b", "k"), None); // req 2
        assert!(r.next_fault("b", "k").is_some()); // req 3
        assert!(r.next_fault("b", "k").is_some()); // req 4
    }

    #[test]
    fn queue_consumed_in_order() {
        let r = reg();
        r.insert("b", "k", FaultType::CorruptBody, 0, Occurrence::Once);
        r.insert(
            "b",
            "k",
            FaultType::WrongStoredChecksum,
            0,
            Occurrence::Always,
        );
        assert_eq!(r.next_fault("b", "k"), Some(FaultType::CorruptBody));
        assert_eq!(r.next_fault("b", "k"), Some(FaultType::WrongStoredChecksum));
        assert_eq!(r.next_fault("b", "k"), Some(FaultType::WrongStoredChecksum));
    }

    #[test]
    fn clear_drops_queue() {
        let r = reg();
        r.insert("b", "k", FaultType::CorruptBody, 0, Occurrence::Always);
        assert!(r.next_fault("b", "k").is_some());
        r.clear("b", "k");
        assert_eq!(r.next_fault("b", "k"), None);
    }

    #[test]
    fn faults_are_key_scoped() {
        let r = reg();
        r.insert("b", "k1", FaultType::CorruptBody, 0, Occurrence::Always);
        assert_eq!(r.next_fault("b", "k2"), None);
        assert!(r.next_fault("b", "k1").is_some());
    }
}
