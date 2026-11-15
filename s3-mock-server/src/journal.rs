/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The request journal: what the server observed about each request it served.
//!
//! Every request a handler admits gets a [`RequestRecord`]. GetObject and HeadObject records also
//! hold the resolved range and the checksum the response carried, and a GetObject record holds
//! the fault or hold applied to it and how its body ended. The journal records only what the
//! server can observe: it cannot tell whether a client used a body it was sent in full.
//!
//! The journal is off unless the server is built with
//! [`with_request_journal`](crate::S3MockServerBuilder::with_request_journal), since it keeps
//! every record until [`clear_requests`](crate::S3MockServer::clear_requests) and delivers
//! bodies in bounded pieces.

use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;

use crate::faults::FaultType;
use crate::Operation;

/// One request, as the server observed it.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RequestRecord {
    /// The operation.
    pub operation: Operation,
    /// The bucket, for a request that names one.
    pub bucket: Option<String>,
    /// The key, for a request that names one.
    pub key: Option<String>,
    /// For GetObject and HeadObject, the object bytes the response covers, end exclusive: from
    /// `Range`, from `partNumber`, or the whole object. `None` for an empty object, and for a
    /// request that failed before its range was resolved.
    pub range: Option<Range<u64>>,
    /// For GetObject and HeadObject, the checksum the response carried.
    pub checksum: Option<SentChecksum>,
    /// For GetObject, the fault applied to the response.
    pub fault: Option<FaultType>,
    /// For GetObject, how a [`FaultType::HoldBody`] hold on the body ended.
    pub hold: Option<HoldEnd>,
    /// For GetObject, how the response body ended. `None` until it ends, and for a request that
    /// failed before a body was sent.
    pub body: Option<BodyEnd>,
}

/// A checksum a response carried.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentChecksum {
    /// The checksum's algorithm.
    pub algorithm: aws_sdk_s3::types::ChecksumAlgorithm,
    /// The object's checksum type, as the response reported it.
    pub checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
    /// Whether the value is the object's or one part's.
    pub scope: ChecksumScope,
}

/// Whether a checksum covers the object or one part.
///
/// Inferred by value: [`Object`](Self::Object) when the value equals the object's stored value,
/// [`Part`](Self::Part) otherwise. A part that is the whole object, as part 1 of a one-part
/// upload, has the object's value and is reported as `Object`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumScope {
    /// The object's stored checksum.
    Object,
    /// One part's stored checksum.
    Part,
}

/// How a held body's hold ended.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldEnd {
    /// The awaited number of other bodies ended.
    Released,
    /// The deadline passed first.
    Expired,
}

/// How a response body ended.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyEnd {
    /// How it ended.
    pub ending: BodyEnding,
    /// The order in which bodies ended, counted across the server from 0.
    pub order: u64,
}

/// How a response body ended.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyEnding {
    /// The body yielded its `Content-Length` bytes to the connection.
    Sent,
    /// The body's stream failed, or ended before `Content-Length` bytes. The mock's body faults
    /// are the only source of either.
    CutByFault,
    /// The connection dropped the body before its end, for example because the client closed it.
    EndedEarly,
}

/// A request's position among every request the journal has admitted, counted from 0 across
/// [`Journal::clear`], so an id never names a later request's record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestId(usize);

/// Every request the server admitted, in admission order.
#[derive(Debug, Default)]
pub(crate) struct Journal {
    /// Whether requests are recorded. A disabled journal records nothing and is never notified.
    enabled: bool,
    state: Mutex<JournalState>,
    /// Notified whenever a body ends.
    body_ended: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct JournalState {
    /// The records of requests `cleared..`.
    records: Vec<RequestRecord>,
    /// The number of requests admitted before the last [`Journal::clear`].
    cleared: usize,
    bodies_ended: u64,
}

impl JournalState {
    fn record_mut(&mut self, id: RequestId) -> Option<&mut RequestRecord> {
        id.0.checked_sub(self.cleared)
            .and_then(|i| self.records.get_mut(i))
    }
}

impl Journal {
    /// A journal that records requests.
    pub(crate) fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Whether the journal records requests.
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Records a request. On a disabled journal, returns an id no record has.
    pub(crate) fn admit(
        &self,
        operation: Operation,
        bucket: Option<&str>,
        key: Option<&str>,
    ) -> RequestId {
        if !self.enabled {
            return RequestId(usize::MAX);
        }
        let mut state = self.state.lock().unwrap();
        state.records.push(RequestRecord {
            operation,
            bucket: bucket.map(str::to_owned),
            key: key.map(str::to_owned),
            range: None,
            checksum: None,
            fault: None,
            hold: None,
            body: None,
        });
        RequestId(state.cleared + state.records.len() - 1)
    }

    /// Applies `update` to `id`'s record. Does nothing if the journal no longer holds it, or is
    /// disabled.
    pub(crate) fn update(&self, id: RequestId, update: impl FnOnce(&mut RequestRecord)) {
        if let Some(record) = self.state.lock().unwrap().record_mut(id) {
            update(record);
        }
    }

    /// The records held, in admission order.
    pub(crate) fn snapshot(&self) -> Vec<RequestRecord> {
        self.state.lock().unwrap().records.clone()
    }

    /// Drops every record. A request admitted before the call, such as a body still being sent,
    /// updates nothing when it ends.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.cleared += state.records.len();
        state.records.clear();
    }

    fn end_body(&self, id: RequestId, ending: BodyEnding) {
        {
            let mut state = self.state.lock().unwrap();
            let order = state.bodies_ended;
            if let Some(record) = state.record_mut(id) {
                if record.body.is_none() {
                    record.body = Some(BodyEnd { ending, order });
                    state.bodies_ended += 1;
                }
            }
        }
        self.body_ended.notify_waiters();
    }

    /// The number of GetObject bodies for `bucket`/`key`, other than `id`'s, that have ended.
    fn other_bodies_ended(&self, id: RequestId, bucket: &str, key: &str) -> usize {
        let state = self.state.lock().unwrap();
        state
            .records
            .iter()
            .enumerate()
            .filter(|(i, r)| {
                state.cleared + i != id.0
                    && r.operation == Operation::GetObject
                    && r.bucket.as_deref() == Some(bucket)
                    && r.key.as_deref() == Some(key)
                    && r.body.is_some()
            })
            .count()
    }

    /// Waits until `count` GetObject bodies of `bucket`/`key`, other than `id`'s, have ended, or
    /// until `deadline` passes, and records which happened. On a disabled journal, no body is
    /// recorded as ended, so the wait lasts until `deadline`.
    pub(crate) async fn hold(
        &self,
        id: RequestId,
        bucket: &str,
        key: &str,
        count: u32,
        deadline: Duration,
    ) {
        let deadline = tokio::time::sleep(deadline);
        tokio::pin!(deadline);
        let end = loop {
            let notified = self.body_ended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.other_bodies_ended(id, bucket, key) >= count as usize {
                break HoldEnd::Released;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = &mut deadline => break HoldEnd::Expired,
            }
        };
        if end == HoldEnd::Expired {
            tracing::warn!(target: "s3_mock_server::fault", %bucket, %key, count, "hold expired");
        }
        self.update(id, |r| r.hold = Some(end));
    }
}

/// The largest piece a [`JournaledBody`] yields. The connection asks for the next piece only
/// once it has room to write, so a body ends shortly after its last piece reaches the socket,
/// not as soon as storage has produced it.
const PIECE_BYTES: usize = 64 * 1024;

/// A response body that records how it ended in the journal, yielded in pieces of at most
/// [`PIECE_BYTES`].
///
/// Ends as [`BodyEnding::Sent`] when it has yielded the response's `Content-Length` bytes or the
/// inner stream ends, [`BodyEnding::CutByFault`] when the inner stream yields an error, and
/// [`BodyEnding::EndedEarly`] when it is dropped before any of these.
pub(crate) struct JournaledBody<S> {
    inner: S,
    /// Bytes from the inner stream not yet yielded.
    pending: Bytes,
    /// Bytes still to yield before the response's `Content-Length` is reached.
    remaining: u64,
    state: Arc<crate::s3s::ServerState>,
    id: RequestId,
    ended: bool,
}

impl<S> JournaledBody<S> {
    pub(crate) fn new(
        inner: S,
        content_length: u64,
        state: Arc<crate::s3s::ServerState>,
        id: RequestId,
    ) -> Self {
        Self {
            inner,
            pending: Bytes::new(),
            remaining: content_length,
            state,
            id,
            ended: false,
        }
    }

    fn end(&mut self, ending: BodyEnding) {
        if !self.ended {
            self.ended = true;
            self.state.journal.end_body(self.id, ending);
        }
    }
}

impl<S, E> Stream for JournaledBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if !self.pending.is_empty() {
                let n = self.pending.len().min(PIECE_BYTES);
                let piece = self.pending.split_to(n);
                self.remaining = self.remaining.saturating_sub(n as u64);
                if self.remaining == 0 {
                    // The connection may drop the body without polling it again.
                    self.end(BodyEnding::Sent);
                }
                return Poll::Ready(Some(Ok(piece)));
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => self.pending = chunk,
                other => return self.finish(other),
            }
        }
    }
}

impl<S> JournaledBody<S> {
    fn finish<E>(
        &mut self,
        polled: Poll<Option<Result<Bytes, E>>>,
    ) -> Poll<Option<Result<Bytes, std::io::Error>>>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        match polled {
            Poll::Ready(None) => {
                // A stream that ends before Content-Length was cut short, by `ShortBody`.
                self.end(match self.remaining {
                    0 => BodyEnding::Sent,
                    _ => BodyEnding::CutByFault,
                });
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.end(BodyEnding::CutByFault);
                Poll::Ready(Some(Err(std::io::Error::other(e))))
            }
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for JournaledBody<S> {
    fn drop(&mut self) {
        self.end(BodyEnding::EndedEarly);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request admitted before `clear` updates nothing afterward, even when a request admitted
    /// after it has taken its old position in the record list.
    #[test]
    fn ids_admitted_before_clear_update_nothing() {
        let journal = Journal::enabled();
        let before = journal.admit(Operation::GetObject, Some("b"), Some("k"));
        journal.clear();
        let after = journal.admit(Operation::GetObject, Some("b"), Some("k"));
        journal.end_body(before, BodyEnding::Sent);
        journal.update(before, |r| r.hold = Some(HoldEnd::Expired));
        let records = journal.snapshot();
        assert_eq!(records.len(), 1);
        assert!(records[0].body.is_none() && records[0].hold.is_none());
        journal.end_body(after, BodyEnding::Sent);
        assert_eq!(
            journal.snapshot()[0].body.map(|b| b.ending),
            Some(BodyEnding::Sent)
        );
    }

    /// A disabled journal records nothing.
    #[test]
    fn disabled_journal_records_nothing() {
        let journal = Journal::default();
        let id = journal.admit(Operation::GetObject, Some("b"), Some("k"));
        journal.end_body(id, BodyEnding::Sent);
        assert!(journal.snapshot().is_empty());
    }
}
