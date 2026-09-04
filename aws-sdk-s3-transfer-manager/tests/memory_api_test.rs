/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::error::Error;

use aws_sdk_s3_transfer_manager::memory::{
    AcquireError, BufferPool, BufferPoolBuildError, BufferPoolBuilder, MemoryBudgetConfig,
    MemoryConfig, PooledBufMut, Reservation, ReserveError, ReserveFuture, SegmentedBytes,
};
use bytes::{Buf, BufMut};

fn assert_public_error<T: Error + Send + Sync + 'static>() {}

#[test]
fn public_memory_api_supports_an_explicit_pool_flow() {
    assert_public_error::<AcquireError>();
    assert_public_error::<BufferPoolBuildError>();
    assert_public_error::<ReserveError>();

    let builder: BufferPoolBuilder =
        BufferPool::builder().memory_budget(MemoryBudgetConfig::Limit(1024 * 1024));
    let pool: BufferPool = builder.build().expect("valid explicit pool");
    let _client_config = MemoryConfig::Explicit(pool.clone());
    let carrier_size = pool.carrier_size();

    let reservation: Reservation = pool
        .try_reserve(carrier_size * 2)
        .expect("reservation attempt")
        .expect("immediate reservation");
    let mut buffer: PooledBufMut = pool
        .acquire(&reservation, carrier_size)
        .expect("reserved acquisition");
    buffer.put_slice(b"abc");

    let mut data: SegmentedBytes = buffer.freeze();
    reservation.close_acquisition();

    assert_eq!(data.chunk(), b"abc");
    assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);

    data.advance(3);
    assert_eq!(pool.metrics().charged_capacity_bytes(), 0);

    let unpolled: ReserveFuture = pool.reserve(carrier_size);
    drop(unpolled);
    assert_eq!(pool.metrics().reservation_enqueues_total(), 0);
}
