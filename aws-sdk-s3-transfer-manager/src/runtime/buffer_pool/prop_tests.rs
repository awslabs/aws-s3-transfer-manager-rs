/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Property tests for public buffer-pool ownership and accounting.
//!
//! Each case creates a four-carrier pool and fixed slots for reservation futures, granted
//! reservations, mutable buffers, and immutable values. A generated [`Operation`] may reserve or
//! queue memory, poll or cancel a request, acquire or grow a buffer, write or publish bytes, create
//! immutable aliases, consume a value, or drop an owner. [`Runner`] applies the operation to both
//! the real public API and an independent [`PoolModel`].
//!
//! The model represents carrier ownership with opaque logical tokens and tracks bytes, reservation
//! coverage, sticky uncovered charges, prepared capacity, and FIFO order without reproducing the
//! pool's bitmap or synchronization implementation. After every operation, the runner compares
//! mutable buffers, immutable payloads, public metrics, live ownership, and the complete pool audit.
//! It then drops every remaining handle and requires demand, ownership, and queued work to reach
//! zero.
//!
//! Operation arguments are selectors mapped into the state that exists when they execute.
//! Inapplicable operations deliberately become no-ops, allowing shrinking to remove setup without
//! invalidating the rest of a sequence. Deterministic sequences exercise the same runner under
//! Miri; synchronization schedules remain covered by the pool's Loom tests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use bytes::{Buf, BufMut, Bytes};
#[cfg(not(miri))]
use proptest::prelude::*;

use super::test_util::PoolAuditReport;
use super::{
    test_pool, AcquireError, BufferPool, CarrierCount, PooledBufMut, Reservation, ReserveError,
    ReserveFuture, SegmentedBytes,
};

const BLOCK_CARRIERS: usize = 2;
const CONFIGURED_CARRIERS: usize = 4;
const RESERVATION_SLOTS: usize = 4;
const REQUEST_SLOTS: usize = 4;
const BUFFER_SLOTS: usize = 4;
const VALUE_SLOTS: usize = 6;

/// One state transition attempted through the public pool and buffer interfaces.
///
/// Slot and length fields are selectors normalized by [`Runner`] against its fixed handle tables
/// and the state present when the operation executes.
#[derive(Clone, Debug)]
enum Operation {
    TryReserve {
        slot: u8,
        envelope: u8,
    },
    StartReserve {
        slot: u8,
        envelope: u8,
    },
    PollReserve {
        request: u8,
        reservation: u8,
    },
    CancelReserve {
        slot: u8,
    },
    CloseReservation {
        slot: u8,
    },
    Acquire {
        reservation: u8,
        buffer: u8,
        carriers: u8,
    },
    Grow {
        buffer: u8,
        writable: u16,
    },
    Write {
        buffer: u8,
        count: u16,
        byte: u8,
    },
    Publish {
        buffer: u8,
        value: u8,
        count: u16,
    },
    Freeze {
        buffer: u8,
        value: u8,
    },
    CloneValue {
        source: u8,
        output: u8,
    },
    SliceView {
        source: u8,
        output: u8,
        start: u16,
        len: u16,
    },
    AdvanceValue {
        value: u8,
        count: u16,
    },
    AppendValue {
        target: u8,
        source: u8,
    },
    DropBuffer {
        slot: u8,
    },
    DropValue {
        slot: u8,
    },
}

/// Real immutable handle retained by the generated runner.
enum ValueHandle {
    View(Bytes),
    Segmented(SegmentedBytes),
}

impl ValueHandle {
    fn kind(&self) -> ValueKind {
        match self {
            Self::View(_) => ValueKind::View,
            Self::Segmented(_) => ValueKind::Segmented,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::View(value) => value.len(),
            Self::Segmented(value) => value.len(),
        }
    }

    fn to_bytes(&self) -> Bytes {
        match self {
            Self::View(value) => value.clone(),
            Self::Segmented(value) => value.clone().into_contiguous(),
        }
    }

    fn clone_value(&self) -> Self {
        match self {
            Self::View(value) => Self::View(value.clone()),
            Self::Segmented(value) => Self::Segmented(value.clone()),
        }
    }

    fn advance(&mut self, count: usize) {
        match self {
            Self::View(value) => value.advance(count),
            Self::Segmented(value) => value.advance(count),
        }
    }
}

/// Public handle and the private reservation state it names.
struct ReservationHandle {
    value: Reservation,
    state: usize,
}

/// Lazy reservation request retained between generated polls.
struct RequestHandle {
    value: ReserveFuture,
}

/// Logical carrier range retained by one mutable buffer.
#[derive(Clone, Debug)]
struct ModelCarrier {
    token: u64,
    range_len: usize,
    initialized: Vec<u8>,
}

/// Mutable buffer state independent of `PooledBufMut` internals.
#[derive(Clone, Debug)]
struct ModelBuffer {
    reservation: usize,
    carriers: Vec<ModelCarrier>,
}

impl ModelBuffer {
    fn capacity(&self) -> usize {
        self.carriers.iter().map(|carrier| carrier.range_len).sum()
    }

    fn len(&self) -> usize {
        self.carriers
            .iter()
            .map(|carrier| carrier.initialized.len())
            .sum()
    }

    fn remaining_mut(&self) -> usize {
        self.capacity()
            .checked_sub(self.len())
            .expect("model initialization exceeds capacity")
    }

    fn initialized_chunk(&self) -> &[u8] {
        self.carriers
            .iter()
            .find_map(|carrier| {
                (!carrier.initialized.is_empty()).then_some(carrier.initialized.as_slice())
            })
            .unwrap_or_default()
    }

    fn write(&mut self, mut bytes: &[u8]) {
        for carrier in &mut self.carriers {
            if bytes.is_empty() {
                break;
            }
            let writable = carrier
                .range_len
                .checked_sub(carrier.initialized.len())
                .expect("model carrier initialization exceeds its range");
            let count = writable.min(bytes.len());
            carrier.initialized.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
        }
        assert!(bytes.is_empty(), "model write exceeded mutable capacity");
    }

    fn publish_prefix(&mut self, count: usize) -> ModelValue {
        let index = self
            .carriers
            .iter()
            .position(|carrier| !carrier.initialized.is_empty())
            .expect("model publication has no initialized carrier");
        let carrier = &mut self.carriers[index];
        assert!(count != 0 && count <= carrier.initialized.len());
        let bytes = carrier.initialized.drain(..count).collect();
        carrier.range_len = carrier
            .range_len
            .checked_sub(count)
            .expect("model publication exceeds carrier range");
        let token = carrier.token;
        if carrier.range_len == 0 {
            self.carriers.remove(index);
        }
        ModelValue {
            kind: ValueKind::View,
            chunks: vec![ModelChunk { token, bytes }],
        }
    }

    fn freeze(self) -> ModelValue {
        let chunks = self
            .carriers
            .into_iter()
            .filter_map(|carrier| {
                (!carrier.initialized.is_empty()).then_some(ModelChunk {
                    token: carrier.token,
                    bytes: carrier.initialized,
                })
            })
            .collect();
        ModelValue {
            kind: ValueKind::Segmented,
            chunks,
        }
    }
}

/// Kind of immutable public value represented by the model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueKind {
    View,
    Segmented,
}

/// Logical bytes whose lifetime retains one carrier charge.
#[derive(Clone, Debug)]
struct ModelChunk {
    token: u64,
    bytes: Vec<u8>,
}

/// Immutable bytes and the carrier identities retaining their charges.
#[derive(Clone, Debug)]
struct ModelValue {
    kind: ValueKind,
    chunks: Vec<ModelChunk>,
}

impl ModelValue {
    fn len(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }

    fn bytes(&self) -> Vec<u8> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect()
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.len());
        if self.kind == ValueKind::View {
            let chunk = self
                .chunks
                .first_mut()
                .expect("model view has no owner range");
            chunk.bytes.drain(..count);
            return;
        }
        while count != 0 {
            let front = self.chunks.first_mut().expect("model value has no bytes");
            if count < front.bytes.len() {
                front.bytes.drain(..count);
                break;
            }
            count -= front.bytes.len();
            self.chunks.remove(0);
        }
    }

    fn slice(&self, start: usize, len: usize) -> Self {
        assert_eq!(self.kind, ValueKind::View);
        assert_eq!(self.chunks.len(), 1);
        let chunk = &self.chunks[0];
        Self {
            kind: ValueKind::View,
            chunks: vec![ModelChunk {
                token: chunk.token,
                bytes: chunk.bytes[start..start + len].to_vec(),
            }],
        }
    }
}

/// One reservation state retained after its public handle closes.
#[derive(Clone, Debug)]
struct ModelReservation {
    envelope: usize,
    direct_outstanding: usize,
    open: bool,
}

/// State of one lazy reservation request.
#[derive(Clone, Debug)]
enum ModelRequest {
    Unpolled { envelope: usize },
    Queued { envelope: usize },
    Granted { reservation: usize },
}

/// Plain accounting and ownership state used as the reference implementation.
struct PoolModel {
    active: usize,
    available: usize,
    uncovered: usize,
    prepared: usize,
    reservation_enqueues_total: u64,
    queue: VecDeque<usize>,
    reservations: Vec<ModelReservation>,
    handles: Vec<Option<usize>>,
    requests: Vec<Option<ModelRequest>>,
    buffers: Vec<Option<ModelBuffer>>,
    values: Vec<Option<ModelValue>>,
    token_reservations: HashMap<u64, usize>,
    next_token: u64,
}

impl PoolModel {
    fn new() -> Self {
        Self {
            active: 0,
            available: 0,
            uncovered: 0,
            prepared: 0,
            reservation_enqueues_total: 0,
            queue: VecDeque::new(),
            reservations: Vec::new(),
            handles: empty_slots(RESERVATION_SLOTS),
            requests: empty_slots(REQUEST_SLOTS),
            buffers: empty_slots(BUFFER_SLOTS),
            values: empty_slots(VALUE_SLOTS),
            token_reservations: HashMap::new(),
            next_token: 0,
        }
    }

    fn admission_used(&self) -> usize {
        self.active
            .checked_add(self.uncovered)
            .expect("model admission usage overflowed")
    }

    fn charged(&self) -> usize {
        self.active
            .checked_sub(self.available)
            .and_then(|covered| covered.checked_add(self.uncovered))
            .expect("model coverage exceeds active demand")
    }

    fn can_grant(&self, envelope: usize) -> bool {
        self.admission_used()
            .checked_add(envelope)
            .is_some_and(|next| next <= CONFIGURED_CARRIERS)
    }

    fn grant(&mut self, envelope: usize) -> usize {
        assert!(self.can_grant(envelope));
        self.active += envelope;
        self.available += envelope;
        self.prepare_to(self.admission_used());
        let reservation = self.reservations.len();
        self.reservations.push(ModelReservation {
            envelope,
            direct_outstanding: 0,
            open: true,
        });
        reservation
    }

    fn close_reservation(&mut self, reservation: usize) {
        let state = &mut self.reservations[reservation];
        if !state.open {
            return;
        }
        let remaining_active = self
            .active
            .checked_sub(state.envelope)
            .expect("model reservation close exceeds active demand");
        let potentially_unused = state
            .envelope
            .checked_sub(state.direct_outstanding)
            .expect("model direct ownership exceeds its envelope");
        let nominally_unused = potentially_unused.min(self.available);
        let required_for_active = self.available.saturating_sub(remaining_active);
        let removed = nominally_unused.max(required_for_active);
        let reclassified = state
            .envelope
            .checked_sub(removed)
            .expect("model reservation removal exceeds its envelope");
        self.active = remaining_active;
        self.available -= removed;
        self.uncovered += reclassified;
        state.open = false;
        self.drain();
    }

    fn debit(&mut self, reservation: usize, count: usize) -> bool {
        let state = &self.reservations[reservation];
        if !state.open
            || state
                .direct_outstanding
                .checked_add(count)
                .is_none_or(|next| next > state.envelope)
        {
            return false;
        }
        let covered = count.min(self.available);
        self.available -= covered;
        self.uncovered += count - covered;
        self.reservations[reservation].direct_outstanding += count;
        self.prepare_to(self.admission_used());
        true
    }

    fn allocate_carriers(&mut self, reservation: usize, count: usize) -> Vec<ModelCarrier> {
        (0..count)
            .map(|_| {
                let token = self.next_token;
                self.next_token += 1;
                self.token_reservations.insert(token, reservation);
                ModelCarrier {
                    token,
                    range_len: 0,
                    initialized: Vec::new(),
                }
            })
            .collect()
    }

    fn live_tokens(&self) -> HashSet<u64> {
        let buffer_tokens = self
            .buffers
            .iter()
            .flatten()
            .flat_map(|buffer| buffer.carriers.iter().map(|carrier| carrier.token));
        let value_tokens = self
            .values
            .iter()
            .flatten()
            .flat_map(|value| value.chunks.iter().map(|chunk| chunk.token));
        buffer_tokens.chain(value_tokens).collect()
    }

    fn release_dead_tokens(&mut self, previously_live: HashSet<u64>) {
        let now_live = self.live_tokens();
        let mut released_uncovered = false;
        for token in previously_live.difference(&now_live) {
            let reservation = self.token_reservations[token];
            let state = &mut self.reservations[reservation];
            state.direct_outstanding = state
                .direct_outstanding
                .checked_sub(1)
                .expect("model direct ownership underflowed");
            if self.uncovered != 0 {
                self.uncovered -= 1;
                released_uncovered = true;
            } else {
                self.available += 1;
            }
        }
        if released_uncovered {
            self.drain();
        }
    }

    fn drain(&mut self) {
        while let Some(&request) = self.queue.front() {
            let envelope = match self.requests[request].as_ref() {
                Some(ModelRequest::Queued { envelope }) => *envelope,
                _ => panic!("model FIFO contains a nonqueued request"),
            };
            if !self.can_grant(envelope) {
                break;
            }
            self.queue.pop_front();
            let reservation = self.grant(envelope);
            self.requests[request] = Some(ModelRequest::Granted { reservation });
        }
    }

    fn prepare_to(&mut self, target: usize) {
        if target == 0 {
            return;
        }
        let rounded = target
            .div_ceil(BLOCK_CARRIERS)
            .checked_mul(BLOCK_CARRIERS)
            .expect("model prepared capacity overflowed");
        self.prepared = self.prepared.max(rounded);
    }
}

/// Applies operations to the real pool and its independent reference state.
///
/// The runner retains only public handles. Its one privileged observation is
/// [`BufferPool::audit_quiescent`], which reconciles accounting and live ownership after each
/// operation.
struct Runner {
    pool: BufferPool,
    carrier_size: usize,
    reservations: Vec<Option<ReservationHandle>>,
    requests: Vec<Option<RequestHandle>>,
    buffers: Vec<Option<PooledBufMut>>,
    values: Vec<Option<ValueHandle>>,
    model: PoolModel,
}

impl Runner {
    fn new() -> Self {
        let (pool, carrier_size) = test_pool(BLOCK_CARRIERS, CONFIGURED_CARRIERS);
        Self {
            pool,
            carrier_size,
            reservations: empty_slots(RESERVATION_SLOTS),
            requests: empty_slots(REQUEST_SLOTS),
            buffers: empty_slots(BUFFER_SLOTS),
            values: empty_slots(VALUE_SLOTS),
            model: PoolModel::new(),
        }
    }

    fn apply(&mut self, operation: &Operation) {
        match *operation {
            Operation::TryReserve { slot, envelope } => {
                self.try_reserve(slot as usize % RESERVATION_SLOTS, envelope as usize)
            }
            Operation::StartReserve { slot, envelope } => {
                self.start_reserve(slot as usize % REQUEST_SLOTS, envelope as usize)
            }
            Operation::PollReserve {
                request,
                reservation,
            } => self.poll_reserve(
                request as usize % REQUEST_SLOTS,
                reservation as usize % RESERVATION_SLOTS,
            ),
            Operation::CancelReserve { slot } => self.cancel_reserve(slot as usize % REQUEST_SLOTS),
            Operation::CloseReservation { slot } => {
                self.close_reservation(slot as usize % RESERVATION_SLOTS)
            }
            Operation::Acquire {
                reservation,
                buffer,
                carriers,
            } => self.acquire(
                reservation as usize % RESERVATION_SLOTS,
                buffer as usize % BUFFER_SLOTS,
                carriers as usize,
            ),
            Operation::Grow { buffer, writable } => {
                self.grow(buffer as usize % BUFFER_SLOTS, writable as usize)
            }
            Operation::Write {
                buffer,
                count,
                byte,
            } => self.write(buffer as usize % BUFFER_SLOTS, count as usize, byte),
            Operation::Publish {
                buffer,
                value,
                count,
            } => self.publish(
                buffer as usize % BUFFER_SLOTS,
                value as usize % VALUE_SLOTS,
                count as usize,
            ),
            Operation::Freeze { buffer, value } => {
                self.freeze(buffer as usize % BUFFER_SLOTS, value as usize % VALUE_SLOTS)
            }
            Operation::CloneValue { source, output } => {
                self.clone_value(source as usize % VALUE_SLOTS, output as usize % VALUE_SLOTS)
            }
            Operation::SliceView {
                source,
                output,
                start,
                len,
            } => self.slice_view(
                source as usize % VALUE_SLOTS,
                output as usize % VALUE_SLOTS,
                start as usize,
                len as usize,
            ),
            Operation::AdvanceValue { value, count } => {
                self.advance_value(value as usize % VALUE_SLOTS, count as usize)
            }
            Operation::AppendValue { target, source } => {
                self.append_value(target as usize % VALUE_SLOTS, source as usize % VALUE_SLOTS)
            }
            Operation::DropBuffer { slot } => self.drop_buffer(slot as usize % BUFFER_SLOTS),
            Operation::DropValue { slot } => self.drop_value(slot as usize % VALUE_SLOTS),
        }
    }

    fn try_reserve(&mut self, slot: usize, envelope: usize) {
        if self.reservations[slot].is_some() {
            return;
        }
        let result = self.pool.try_reserve(self.bytes_for_envelope(envelope));
        if envelope == 0 {
            assert!(matches!(result, Err(ReserveError::InvalidSize)));
            return;
        }
        if envelope > CONFIGURED_CARRIERS {
            assert!(matches!(result, Err(ReserveError::ExceedsCapacity)));
            return;
        }
        if !self.model.queue.is_empty() || !self.model.can_grant(envelope) {
            assert!(matches!(result, Ok(None)));
            return;
        }
        let reservation = result
            .expect("valid immediate reservation failed")
            .expect("eligible immediate reservation was not granted");
        let state = self.model.grant(envelope);
        self.model.handles[slot] = Some(state);
        self.reservations[slot] = Some(ReservationHandle {
            value: reservation,
            state,
        });
    }

    fn start_reserve(&mut self, slot: usize, envelope: usize) {
        if self.requests[slot].is_some() {
            return;
        }
        self.requests[slot] = Some(RequestHandle {
            value: self.pool.reserve(self.bytes_for_envelope(envelope)),
        });
        self.model.requests[slot] = Some(ModelRequest::Unpolled { envelope });
    }

    fn poll_reserve(&mut self, request: usize, reservation: usize) {
        if self.reservations[reservation].is_some() || self.requests[request].is_none() {
            return;
        }
        let state = self.model.requests[request]
            .take()
            .expect("real request has no model state");
        let poll = poll_future(&mut self.requests[request].as_mut().unwrap().value);
        match state {
            ModelRequest::Unpolled { envelope: 0 } => {
                assert!(matches!(poll, Poll::Ready(Err(ReserveError::InvalidSize))));
                self.requests[request] = None;
            }
            ModelRequest::Unpolled { envelope } if envelope > CONFIGURED_CARRIERS => {
                assert!(matches!(
                    poll,
                    Poll::Ready(Err(ReserveError::ExceedsCapacity))
                ));
                self.requests[request] = None;
            }
            ModelRequest::Unpolled { envelope }
                if self.model.queue.is_empty() && self.model.can_grant(envelope) =>
            {
                let Poll::Ready(Ok(value)) = poll else {
                    panic!("eligible first reservation poll did not complete");
                };
                let state = self.model.grant(envelope);
                self.model.handles[reservation] = Some(state);
                self.reservations[reservation] = Some(ReservationHandle { value, state });
                self.requests[request] = None;
            }
            ModelRequest::Unpolled { envelope } => {
                assert!(matches!(poll, Poll::Pending));
                self.model.requests[request] = Some(ModelRequest::Queued { envelope });
                self.model.queue.push_back(request);
                self.model.reservation_enqueues_total =
                    self.model.reservation_enqueues_total.saturating_add(1);
            }
            ModelRequest::Queued { envelope } => {
                assert!(matches!(poll, Poll::Pending));
                self.model.requests[request] = Some(ModelRequest::Queued { envelope });
            }
            ModelRequest::Granted { reservation: state } => {
                let Poll::Ready(Ok(value)) = poll else {
                    panic!("granted reservation was not ready");
                };
                self.model.handles[reservation] = Some(state);
                self.reservations[reservation] = Some(ReservationHandle { value, state });
                self.requests[request] = None;
            }
        }
    }

    fn cancel_reserve(&mut self, slot: usize) {
        if self.requests[slot].take().is_none() {
            return;
        }
        let state = self.model.requests[slot]
            .take()
            .expect("real request has no model state");
        match state {
            ModelRequest::Unpolled { .. } => {}
            ModelRequest::Queued { .. } => {
                self.model.queue.retain(|queued| *queued != slot);
                self.model.drain();
            }
            ModelRequest::Granted { reservation } => {
                self.model.close_reservation(reservation);
            }
        }
    }

    fn close_reservation(&mut self, slot: usize) {
        let Some(handle) = self.reservations[slot].take() else {
            return;
        };
        handle.value.close_acquisition();
        let state = self.model.handles[slot]
            .take()
            .expect("real reservation has no model handle");
        assert_eq!(state, handle.state);
        self.model.close_reservation(state);
    }

    fn acquire(&mut self, reservation: usize, buffer: usize, carriers: usize) {
        if carriers == 0
            || self.buffers[buffer].is_some()
            || self.reservations[reservation].is_none()
        {
            return;
        }
        let handle = self.reservations[reservation].as_ref().unwrap();
        let can_debit = {
            let state = &self.model.reservations[handle.state];
            state.open
                && state
                    .direct_outstanding
                    .checked_add(carriers)
                    .is_some_and(|next| next <= state.envelope)
        };
        let result = self
            .pool
            .acquire(&handle.value, self.bytes_for_envelope(carriers));
        if !can_debit {
            assert!(matches!(
                result,
                Err(AcquireError::ReservationCapacityExceeded)
            ));
            return;
        }
        let value = result.expect("valid reserved acquisition failed");
        assert!(self.model.debit(handle.state, carriers));
        let mut model_carriers = self.model.allocate_carriers(handle.state, carriers);
        for carrier in &mut model_carriers {
            carrier.range_len = self.carrier_size;
        }
        self.model.buffers[buffer] = Some(ModelBuffer {
            reservation: handle.state,
            carriers: model_carriers,
        });
        self.buffers[buffer] = Some(value);
    }

    fn grow(&mut self, buffer: usize, selector: usize) {
        let Some(real) = self.buffers[buffer].as_mut() else {
            return;
        };
        let model = self.model.buffers[buffer].as_ref().unwrap();
        let min_writable = selector % (self.carrier_size * 3 + 1);
        let shortfall = min_writable.saturating_sub(model.remaining_mut());
        let added = shortfall.div_ceil(self.carrier_size);
        let state = &self.model.reservations[model.reservation];
        let expected = if added == 0 {
            Ok(())
        } else if !state.open {
            Err(AcquireError::ReservationClosed)
        } else if state.direct_outstanding + added > state.envelope {
            Err(AcquireError::ReservationCapacityExceeded)
        } else {
            Ok(())
        };
        let before = (real.capacity(), real.len(), real.remaining_mut());
        let result = real.reserve(min_writable);
        assert_eq!(result, expected);
        if result.is_err() {
            assert_eq!((real.capacity(), real.len(), real.remaining_mut()), before);
            return;
        }
        if added != 0 {
            let reservation = model.reservation;
            assert!(self.model.debit(reservation, added));
            let mut carriers = self.model.allocate_carriers(reservation, added);
            for carrier in &mut carriers {
                carrier.range_len = self.carrier_size;
            }
            self.model.buffers[buffer]
                .as_mut()
                .unwrap()
                .carriers
                .extend(carriers);
        }
    }

    fn write(&mut self, buffer: usize, selector: usize, byte: u8) {
        let Some(real) = self.buffers[buffer].as_mut() else {
            return;
        };
        let remaining = real.remaining_mut();
        let count = selector % (remaining + 1);
        let bytes = vec![byte; count];
        real.put_slice(&bytes);
        self.model.buffers[buffer].as_mut().unwrap().write(&bytes);
    }

    fn publish(&mut self, buffer: usize, value: usize, selector: usize) {
        if self.values[value].is_some() {
            return;
        }
        let Some(real) = self.buffers[buffer].as_mut() else {
            return;
        };
        let available = real.initialized_chunk().len();
        if available == 0 {
            return;
        }
        let count = 1 + selector % available;
        let published = real.publish_prefix(count);
        let model = self.model.buffers[buffer]
            .as_mut()
            .unwrap()
            .publish_prefix(count);
        self.values[value] = Some(ValueHandle::View(published));
        self.model.values[value] = Some(model);
    }

    fn freeze(&mut self, buffer: usize, value: usize) {
        if self.values[value].is_some() || self.buffers[buffer].is_none() {
            return;
        }
        let previously_live = self.model.live_tokens();
        let real = self.buffers[buffer].take().unwrap().freeze();
        let model = self.model.buffers[buffer].take().unwrap().freeze();
        self.values[value] = Some(ValueHandle::Segmented(real));
        self.model.values[value] = Some(model);
        self.model.release_dead_tokens(previously_live);
    }

    fn clone_value(&mut self, source: usize, output: usize) {
        if source == output || self.values[output].is_some() {
            return;
        }
        let Some(real) = self.values[source].as_ref() else {
            return;
        };
        self.values[output] = Some(real.clone_value());
        self.model.values[output] = self.model.values[source].clone();
    }

    fn slice_view(
        &mut self,
        source: usize,
        output: usize,
        start_selector: usize,
        len_selector: usize,
    ) {
        if source == output || self.values[output].is_some() {
            return;
        }
        let Some(ValueHandle::View(real)) = self.values[source].as_ref() else {
            return;
        };
        if real.is_empty() {
            return;
        }
        let start = start_selector % real.len();
        let len = 1 + len_selector % (real.len() - start);
        self.values[output] = Some(ValueHandle::View(real.slice(start..start + len)));
        self.model.values[output] = Some(
            self.model.values[source]
                .as_ref()
                .unwrap()
                .slice(start, len),
        );
    }

    fn advance_value(&mut self, value: usize, selector: usize) {
        let Some(real) = self.values[value].as_mut() else {
            return;
        };
        let count = selector % (real.len() + 1);
        let previously_live = self.model.live_tokens();
        real.advance(count);
        self.model.values[value].as_mut().unwrap().advance(count);
        self.model.release_dead_tokens(previously_live);
    }

    fn append_value(&mut self, target: usize, source: usize) {
        if target == source {
            return;
        }
        if !matches!(self.values[target], Some(ValueHandle::Segmented(_)))
            || !matches!(self.values[source], Some(ValueHandle::Segmented(_)))
        {
            return;
        }
        let source_real = self.values[source].take().unwrap();
        let ValueHandle::Segmented(source_real) = source_real else {
            unreachable!();
        };
        let ValueHandle::Segmented(target_real) = self.values[target].as_mut().unwrap() else {
            unreachable!();
        };
        target_real.append(source_real);

        let source_model = self.model.values[source].take().unwrap();
        self.model.values[target]
            .as_mut()
            .unwrap()
            .chunks
            .extend(source_model.chunks);
    }

    fn drop_buffer(&mut self, slot: usize) {
        if self.buffers[slot].take().is_none() {
            return;
        }
        let previously_live = self.model.live_tokens();
        self.model.buffers[slot] = None;
        self.model.release_dead_tokens(previously_live);
    }

    fn drop_value(&mut self, slot: usize) {
        if self.values[slot].take().is_none() {
            return;
        }
        let previously_live = self.model.live_tokens();
        self.model.values[slot] = None;
        self.model.release_dead_tokens(previously_live);
    }

    fn assert_consistent(&self, step: usize, operation: &Operation) {
        for (index, (real, model)) in self.buffers.iter().zip(&self.model.buffers).enumerate() {
            assert_eq!(
                real.is_some(),
                model.is_some(),
                "step {step} {operation:?}: buffer {index} presence"
            );
            let (Some(real), Some(model)) = (real.as_ref(), model.as_ref()) else {
                continue;
            };
            assert_eq!(
                real.capacity(),
                model.capacity(),
                "step {step} {operation:?}: buffer {index} capacity"
            );
            assert_eq!(
                real.len(),
                model.len(),
                "step {step} {operation:?}: buffer {index} length"
            );
            assert_eq!(
                real.remaining_mut(),
                model.remaining_mut(),
                "step {step} {operation:?}: buffer {index} writable capacity"
            );
            assert_eq!(
                real.initialized_chunk(),
                model.initialized_chunk(),
                "step {step} {operation:?}: buffer {index} initialized chunk"
            );
        }

        for (index, (real, model)) in self.values.iter().zip(&self.model.values).enumerate() {
            assert_eq!(
                real.is_some(),
                model.is_some(),
                "step {step} {operation:?}: value {index} presence"
            );
            let (Some(real), Some(model)) = (real.as_ref(), model.as_ref()) else {
                continue;
            };
            assert_eq!(
                real.kind(),
                model.kind,
                "step {step} {operation:?}: value {index} kind"
            );
            assert_eq!(
                real.to_bytes().as_ref(),
                model.bytes(),
                "step {step} {operation:?}: value {index} bytes"
            );
        }

        let live_tokens = self.model.live_tokens();
        assert_eq!(
            live_tokens.len(),
            self.model.charged(),
            "step {step} {operation:?}: model live ownership"
        );

        let metrics = self.pool.metrics();
        let carrier_size = self.carrier_size as u64;
        assert_eq!(
            metrics.active_planned_demand_bytes(),
            self.model.active as u64 * carrier_size,
            "step {step} {operation:?}: active demand"
        );
        assert_eq!(
            metrics.charged_capacity_bytes(),
            self.model.charged() as u64 * carrier_size,
            "step {step} {operation:?}: charged capacity"
        );
        assert_eq!(
            metrics.admission_used_bytes(),
            self.model.admission_used() as u64 * carrier_size,
            "step {step} {operation:?}: admission usage"
        );
        assert_eq!(
            metrics.prepared_capacity_bytes(),
            self.model.prepared as u64 * carrier_size,
            "step {step} {operation:?}: prepared capacity"
        );
        assert_eq!(
            metrics.queued_reservations(),
            self.model.queue.len(),
            "step {step} {operation:?}: FIFO depth"
        );
        assert_eq!(
            metrics.reservation_enqueues_total(),
            self.model.reservation_enqueues_total,
            "step {step} {operation:?}: cumulative FIFO entries"
        );

        let audit = self.pool.audit_quiescent();
        assert_eq!(
            audit,
            PoolAuditReport {
                active_planned_demand: CarrierCount::new(self.model.active),
                available_coverage: CarrierCount::new(self.model.available),
                uncovered_charges: CarrierCount::new(self.model.uncovered),
                charged_capacity: CarrierCount::new(self.model.charged()),
                prepared_capacity: CarrierCount::new(self.model.prepared),
                live_carriers: CarrierCount::new(live_tokens.len()),
                queued_reservations: self.model.queue.len(),
                cleanup_pending_blocks: 0,
            },
            "step {step} {operation:?}: global audit"
        );
    }

    fn finish(mut self) {
        self.buffers.clear();
        self.values.clear();
        self.reservations.clear();
        self.requests.clear();
        let audit = self.pool.audit_quiescent();
        assert_eq!(audit.active_planned_demand, CarrierCount::ZERO);
        assert_eq!(audit.available_coverage, CarrierCount::ZERO);
        assert_eq!(audit.uncovered_charges, CarrierCount::ZERO);
        assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
        assert_eq!(audit.queued_reservations, 0);
        assert_eq!(audit.cleanup_pending_blocks, 0);
    }

    fn bytes_for_envelope(&self, envelope: usize) -> usize {
        match envelope {
            0 => 0,
            envelope => (envelope - 1)
                .checked_mul(self.carrier_size)
                .and_then(|bytes| bytes.checked_add(1))
                .expect("generated envelope overflowed"),
        }
    }
}

fn empty_slots<T>(count: usize) -> Vec<Option<T>> {
    std::iter::repeat_with(|| None).take(count).collect()
}

fn poll_future(future: &mut ReserveFuture) -> Poll<Result<Reservation, ReserveError>> {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(future).poll(&mut context)
}

/// Applies a complete operation sequence and requires all ownership to drain on teardown.
fn run_sequence(operations: &[Operation]) {
    let mut runner = Runner::new();
    for (step, operation) in operations.iter().enumerate() {
        runner.apply(operation);
        runner.assert_consistent(step, operation);
    }
    runner.finish();
}

#[cfg(not(miri))]
/// Generates state transitions with higher weight on ownership-producing operations.
fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
        4 => (0u8..8, 0u8..6).prop_map(|(slot, envelope)| Operation::TryReserve { slot, envelope }),
        4 => (0u8..8, 0u8..6).prop_map(|(slot, envelope)| Operation::StartReserve { slot, envelope }),
        5 => (0u8..8, 0u8..8).prop_map(|(request, reservation)| Operation::PollReserve {
            request,
            reservation,
        }),
        2 => (0u8..8).prop_map(|slot| Operation::CancelReserve { slot }),
        3 => (0u8..8).prop_map(|slot| Operation::CloseReservation { slot }),
        6 => (0u8..8, 0u8..8, 1u8..5).prop_map(
            |(reservation, buffer, carriers)| Operation::Acquire {
                reservation,
                buffer,
                carriers,
            },
        ),
        4 => (0u8..8, any::<u16>())
            .prop_map(|(buffer, writable)| Operation::Grow { buffer, writable }),
        7 => (0u8..8, any::<u16>(), any::<u8>())
            .prop_map(|(buffer, count, byte)| Operation::Write {
                buffer,
                count,
                byte,
            }),
        5 => (0u8..8, 0u8..12, any::<u16>()).prop_map(
            |(buffer, value, count)| Operation::Publish {
                buffer,
                value,
                count,
            },
        ),
        4 => (0u8..8, 0u8..12)
            .prop_map(|(buffer, value)| Operation::Freeze { buffer, value }),
        3 => (0u8..12, 0u8..12)
            .prop_map(|(source, output)| Operation::CloneValue { source, output }),
        2 => (0u8..12, 0u8..12, any::<u16>(), any::<u16>()).prop_map(
            |(source, output, start, len)| Operation::SliceView {
                source,
                output,
                start,
                len,
            },
        ),
        4 => (0u8..12, any::<u16>())
            .prop_map(|(value, count)| Operation::AdvanceValue { value, count }),
        2 => (0u8..12, 0u8..12)
            .prop_map(|(target, source)| Operation::AppendValue { target, source }),
        2 => (0u8..8).prop_map(|slot| Operation::DropBuffer { slot }),
        3 => (0u8..12).prop_map(|slot| Operation::DropValue { slot }),
    ]
}

#[cfg(not(miri))]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        max_shrink_iters: 16_384,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_pool_sequences_preserve_bytes_and_ownership(
        operations in proptest::collection::vec(operation_strategy(), 1..80),
    ) {
        run_sequence(&operations);
    }
}

#[test]
fn deterministic_property_corpus_covers_queue_publication_and_owner_return() {
    run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 2,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 2,
        },
        Operation::Write {
            buffer: 0,
            count: u16::MAX,
            byte: b'a',
        },
        Operation::Publish {
            buffer: 0,
            value: 0,
            count: 17,
        },
        Operation::Freeze {
            buffer: 0,
            value: 1,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::StartReserve {
            slot: 0,
            envelope: 3,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloneValue {
            source: 1,
            output: 2,
        },
        Operation::DropValue { slot: 0 },
        Operation::DropValue { slot: 1 },
        Operation::AdvanceValue {
            value: 2,
            count: u16::MAX,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloseReservation { slot: 1 },
        Operation::DropValue { slot: 2 },
    ]);
}

#[test]
fn deterministic_property_corpus_grants_after_final_owner_advance() {
    run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 4,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 4,
        },
        Operation::Write {
            buffer: 0,
            count: 10,
            byte: b'x',
        },
        Operation::Freeze {
            buffer: 0,
            value: 0,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::StartReserve {
            slot: 0,
            envelope: 4,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::AdvanceValue {
            value: 0,
            count: 10,
        },
        Operation::PollReserve {
            request: 0,
            reservation: 1,
        },
        Operation::CloseReservation { slot: 1 },
    ]);
}

#[test]
fn deterministic_property_corpus_preserves_closed_growth_and_view_aliases() {
    run_sequence(&[
        Operation::TryReserve {
            slot: 0,
            envelope: 3,
        },
        Operation::Acquire {
            reservation: 0,
            buffer: 0,
            carriers: 1,
        },
        Operation::Grow {
            buffer: 0,
            writable: 32_768,
        },
        Operation::Write {
            buffer: 0,
            count: 100,
            byte: b'g',
        },
        Operation::Publish {
            buffer: 0,
            value: 0,
            count: 40,
        },
        Operation::SliceView {
            source: 0,
            output: 1,
            start: 1,
            len: 10,
        },
        Operation::CloneValue {
            source: 1,
            output: 2,
        },
        Operation::CloseReservation { slot: 0 },
        Operation::Grow {
            buffer: 0,
            writable: 40_000,
        },
        Operation::DropBuffer { slot: 0 },
        Operation::DropValue { slot: 0 },
        Operation::DropValue { slot: 1 },
        Operation::AdvanceValue {
            value: 2,
            count: 11,
        },
    ]);
}
