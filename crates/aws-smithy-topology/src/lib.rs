/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Hardware topology and placement primitives for the AWS SDK and smithy-rs ecosystem.
//!
//! [`Topology`] combines the processors usable in the discovery context with a
//! selection of intended execution targets. Discovery starts no workers and
//! changes no thread affinity.
//!
//! # Resources and placement
//!
//! A [`LogicalProcessor`] is an OS-visible CPU execution context: one hardware
//! thread that the operating system can schedule work onto. Multiple logical
//! processors can share a physical core through simultaneous multithreading.
//! The discovery model does not describe physical-core or sibling relationships.
//!
//! A [`NumaNode`] describes a reported memory locality domain and its usable
//! logical processors. This is resource association information; the crate
//! does not allocate memory on a particular node.
//!
//! A [`Placement`] describes an intended execution target, such as a worker
//! thread's CPU, with a NUMA association when available. The target describes
//! where execution is intended to occur; it does not change thread affinity or
//! memory placement. Applying CPU binding is a separate operation.
//!
//! The resource inventory and placement selection answer different questions.
//! An application might be allowed to run on four CPUs but have a CPU-time
//! quota corresponding to two. The topology retains all four identified CPUs
//! while selecting two default placements. Capping placements likewise preserves
//! the resource inventory.
//!
//! ```rust
//! use aws_smithy_topology::Topology;
//!
//! let topology = Topology::detect()?
//!     .with_max_placements(8);
//!
//! println!("Default parallelism: {}", topology.parallelism_estimate());
//! for placement in topology.placements() {
//!     println!(
//!         "Target CPU: {:?}, NUMA association: {:?}",
//!         placement.logical_processor(),
//!         placement.numa_node(),
//!     );
//! }
//! # Ok::<(), aws_smithy_topology::DetectError>(())
//! ```
//!
//! # Discovery behavior
//!
//! Linux and Windows discovery obtains native processor identities, restricting
//! the usable inventory to processors available to the calling thread before
//! limiting default placements by resource quota. Fractional quotas round down,
//! with at least one placement for a nonempty usable set. On Linux, per-CPU
//! sysfs links confirm NUMA associations; unavailable associations stay unknown.
//!
//! On other platforms and under Miri, discovery obtains only a positive
//! available-parallelism estimate. The processor and NUMA inventories are empty,
//! and placements have no CPU or NUMA target. A count is never converted into
//! fabricated hardware identities. Consumers can inspect the resource lists
//! and optional placement targets directly.
//!
//! A topology is immutable and cheaply cloneable. A placement cap takes a prefix
//! of the ordered default placements and preserves all discovered facts,
//! including [`Topology::parallelism_estimate`]. A zero cap is treated as one,
//! preserving a nonempty selection. The cap does not rebalance targets
//! across NUMA nodes. Native resources and placements are ordered by processor
//! identifier within a discovered view; the chosen subset can differ between
//! discoveries.
//!
//! Discovery may reuse cached hardware inventory. The returned value is a
//! captured view, not a live monitor of CPU hotplug, affinity changes, or quotas.
//!
//! # CPU binding
//!
//! [`Topology::bind_current_thread`] explicitly applies affinity on Linux and
//! Windows. It accepts an identified processor from the resource
//! inventory, rechecks the calling thread's available processors, and verifies
//! the reported restriction after binding. It does not widen existing thread
//! restrictions or change the selected placements. Count-only topology supplies no
//! processor IDs to bind; the portable discovery fallback never reports a
//! successful no-op binding. [`BindError::kind`] classifies failures, and
//! [`BindError::processor`] identifies the requested target. Classification does
//! not guarantee that a failed binding left affinity unchanged.
//!
//! Bind a newly created worker so the calling thread keeps its own affinity:
//!
//! ```rust
//! use aws_smithy_topology::Topology;
//!
//! let topology = Topology::detect()?;
//! if let Some(processor) = topology.placements()[0].logical_processor() {
//!     std::thread::spawn(move || {
//!         topology.bind_current_thread(processor)?;
//!         // Run work on this bound thread.
//!         Ok::<(), aws_smithy_topology::BindError>(())
//!     }).join().expect("worker panicked")?;
//! }
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
mod placement;
mod platform;
mod resource;
mod topology;

pub use error::{BindError, BindErrorKind, DetectError};
pub use placement::Placement;
pub use resource::{LogicalProcessor, LogicalProcessorId, NumaNode, NumaNodeId};
pub use topology::Topology;
