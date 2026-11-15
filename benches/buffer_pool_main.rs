/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Buffer-pool benchmark entry point.
//!
//! The benchmarks reach the crate-private allocation API through
//! `aws_sdk_s3_transfer_manager::__bench`, which exists only when both crates
//! are built with `RUSTFLAGS='--cfg s3_tm_bench'`. Without the cfg this target
//! builds a `main` that reports how to enable the benchmarks.

#[cfg(s3_tm_bench)]
#[path = "buffer_pool.rs"]
mod buffer_pool;

#[cfg(s3_tm_bench)]
criterion::criterion_main!(buffer_pool::benches);

#[cfg(not(s3_tm_bench))]
fn main() {
    eprintln!("buffer_pool benchmarks require RUSTFLAGS='--cfg s3_tm_bench'");
}
