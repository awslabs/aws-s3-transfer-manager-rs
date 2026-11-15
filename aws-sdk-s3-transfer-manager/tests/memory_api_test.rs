/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3_transfer_manager::types::ByteSize;
use std::error::Error;
use std::io;
use std::task::{Context, Poll, Waker};

use aws_sdk_s3_transfer_manager::config::{MemoryBudgetConfig, MemoryConfig};
use aws_sdk_s3_transfer_manager::memory::{
    AcquireError, BufferPool, BufferPoolBuildError, BufferPoolBuilder, PooledBufMut, Reservation,
    ReserveError, ReserveFuture, SegmentedBytes,
};
use bytes::Buf;

const POOL_BUDGET: usize = 1024 * 1024;

fn assert_public_error<T: Error + Send + Sync + 'static>() {}

fn test_pool() -> BufferPool {
    let builder: BufferPoolBuilder =
        BufferPool::builder().memory_budget(MemoryBudgetConfig::Limit(POOL_BUDGET));
    builder.build().expect("valid explicit pool")
}

fn pool_capacity(pool: &BufferPool) -> usize {
    usize::try_from(pool.metrics().capacity_bytes()).expect("pool capacity fits usize")
}

fn poll_acquire(buffer: &mut PartBuffer, envelope: usize) -> Poll<io::Result<()>> {
    let mut cx = Context::from_waker(Waker::noop());
    buffer.poll_acquire(&mut cx, envelope)
}

/// A caller-built pool charges a part written through `PartBuffer` until the frozen bytes are
/// consumed, and the same pool can be handed to a client.
#[test]
fn public_memory_api_supports_an_explicit_pool_flow() {
    assert_public_error::<BufferPoolBuildError>();

    let builder: BufferPoolBuilder =
        BufferPool::builder().memory_budget(MemoryBudgetConfig::Limit(ByteSize::new(1024 * 1024)));
    let pool: BufferPool = builder.build().expect("valid explicit pool");
    let _client_config = MemoryConfig::Explicit(pool.clone());
    assert_eq!(pool.metrics().in_use_bytes(), 0);

    let mut buffer = PartBuffer::new(&pool, 3);
    assert!(matches!(poll_acquire(&mut buffer, 3), Poll::Ready(Ok(()))));
    assert_eq!(buffer.fill_from_slice(b"abc"), 3);

    let mut data: SegmentedBytes = buffer.freeze();
    assert_eq!(data.chunk(), b"abc");
    let in_use = pool.metrics().in_use_bytes();
    assert!(
        in_use >= 3,
        "frozen part is not charged: {in_use} bytes in use"
    );

    data.advance(3);
    assert_eq!(pool.metrics().in_use_bytes(), 0);
}

/// A `PartBuffer` waiting for admission withdraws its request when dropped, so it cannot hold
/// a place ahead of later requests.
#[test]
fn dropping_a_pending_part_buffer_leaves_no_waiting_request() {
    let pool = test_pool();
    let capacity = pool_capacity(&pool);
    let mut holder = PartBuffer::new(&pool, capacity);
    assert!(matches!(
        poll_acquire(&mut holder, capacity),
        Poll::Ready(Ok(()))
    ));

    let mut waiter = PartBuffer::new(&pool, 1);
    assert!(poll_acquire(&mut waiter, 1).is_pending());
    assert_eq!(pool.metrics().waiting_requests(), 1);

    drop(waiter);
    assert_eq!(pool.metrics().waiting_requests(), 0);

    drop(holder);
    assert_eq!(pool.metrics().admitted_bytes(), 0);
    assert_eq!(pool.metrics().in_use_bytes(), 0);
}

/// An envelope larger than the pool's capacity can never be admitted, so it fails on the first
/// poll instead of waiting.
#[test]
fn oversized_envelope_fails_with_io_error() {
    let pool = test_pool();
    let oversized = pool_capacity(&pool) + 1;
    let mut buffer = PartBuffer::new(&pool, oversized);

    let Poll::Ready(result) = poll_acquire(&mut buffer, oversized) else {
        panic!("oversized envelope waited for admission");
    };
    let error = result.expect_err("oversized envelope was admitted");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(error.get_ref().is_some(), "pool error source was dropped");

    assert_eq!(pool.metrics().waiting_requests(), 0);
    assert_eq!(pool.metrics().waits_total(), 0);
    assert_eq!(pool.metrics().admitted_bytes(), 0);
}
