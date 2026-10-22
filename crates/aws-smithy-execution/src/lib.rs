/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Managed runtimes with execution partitions.
//!
//! [`ManagedRuntime`] contains one execution partition for each selected topology
//! placement. Each partition runs a Tokio current-thread runtime on a dedicated
//! managed thread. A
//! [`ManagedRuntimeHandle`] exposes topology and cloneable [`RuntimePartition`]
//! values for targeted Send spawning. Runtime and partition identities are
//! separate from logical processor identities.
//!
//! CPU pinning applies identified targets by default; [`ManagedRuntimeBuilder::pin_threads`]
//! can disable it. A partition retains its intended [`RuntimePartition::placement`]
//! separately from the [`RuntimePartition::bound_processor`] recorded at startup.
//! Unknown targets remain unbound.
//!
//! Keep the runtime owner alive while threads should run. Dropping it requests
//! shutdown and detaches its threads without waiting. Handles and partitions do
//! not prevent that request. Each managed thread destroys its Tokio runtime as
//! it exits, so owner destruction can occur inside managed work. Async tasks
//! follow Tokio cancellation behavior. Running blocking tasks or async tasks
//! that do not yield can delay thread exit.
//! Use [`ManagedRuntime::shutdown`] to consume the owner and asynchronously wait
//! for managed-thread exit. It requires no ambient Tokio runtime.
//!
//! With the optional `dial9` feature, the builder accepts a caller-owned
//! recorder handle. Each partition attaches to that recorder, and partition
//! submissions record polls and wakes using Dial9's spawning integration.
//! Enabling the feature alone does not attach a recorder. Shut down the runtime
//! before shutting down the recorder so managed-thread events can flush.
//!
//! Each partition currently permits one Tokio blocking-pool thread. These
//! helper threads are outside the execution partition collection.
//! Thread binding does not establish a memory allocation policy.
//!
//! ```rust
//! use aws_smithy_execution::{topology::Topology, ManagedRuntime};
//!
//! let runtime = ManagedRuntime::builder()
//!     .topology(Topology::detect()?.with_max_placements(2))
//!     .build()?;
//! let execution = runtime.handle();
//! let task = execution.partitions()[0].spawn(async { 42 });
//!
//! let caller = tokio::runtime::Builder::new_current_thread().build()?;
//! assert_eq!(caller.block_on(task)?, 42);
//! caller.block_on(runtime.shutdown())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![warn(
    missing_debug_implementations,
    missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

mod error;
mod handle;
mod identity;
mod partition;
mod runtime;
mod shutdown;
mod thread;

pub use error::{BuildError, ShutdownError};
pub use handle::ManagedRuntimeHandle;
pub use identity::{RuntimeId, RuntimePartitionId};
pub use partition::RuntimePartition;
pub use runtime::{ManagedRuntime, ManagedRuntimeBuilder};

/// Hardware topology and placement primitives.
pub use aws_smithy_topology as topology;
