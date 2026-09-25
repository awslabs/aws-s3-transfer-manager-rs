/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Execution runtime for running IO requests dispatched by the scheduler.
//!
//! The scheduler decides WHAT to run and WHEN. The runtime decides WHERE and HOW.

mod tokio_mt;
pub(crate) use tokio_mt::TokioMultiThreadRuntime;

mod managed;
pub(crate) use managed::ManagedThreadRuntime;

mod topology;
pub(crate) use topology::Topology;

#[allow(dead_code)]
pub(crate) mod buffer_pool;
pub(crate) mod platform;
pub(crate) mod sync;

use aws_smithy_runtime_api::client::http::SharedHttpClient;

use crate::runtime::sync::SubmissionGuard;
use crate::scheduler::descriptor::TransferDescriptor;
use crate::transfer::{IoRequest, TransferId};

/// Work item with scheduler tracking attached.
///
/// Wraps an `IoRequest` with a `TransferDescriptor` that the runtime uses to
/// report execution lifecycle events back to the scheduler. The runtime calls
/// `descriptor.work_started()` when execution begins and the scheduler observes
/// completion via `descriptor.work_finished()` in `on_completion`. This lets the
/// scheduler track outstanding work without prescribing when or how the runtime
/// executes it.
#[derive(Debug)]
pub(crate) struct ScheduledWork {
    pub(crate) item: IoRequest,
    pub(crate) descriptor: TransferDescriptor,
}

/// The execution layer that runs IO requests dispatched by the scheduler.
///
/// The scheduler decides WHAT to run and WHEN. The runtime decides WHERE and HOW.
pub(crate) trait ExecutionRuntime: Send + Sync + std::fmt::Debug {
    /// Dispatch a batch of IO requests for execution.
    fn dispatch(&self, batch: &mut SubmissionGuard<'_, ScheduledWork>);

    /// Shut down the runtime, draining in-flight work.
    fn shutdown(&self);

    /// Remove all pending work for a transfer. Returns count removed.
    fn remove_pending_for_transfer(&self, id: TransferId) -> usize;

    /// Runtime-provided components for S3 client construction and execution.
    fn components(&self) -> &RuntimeComponents;
}

/// Options for the HTTP transport a runtime provides to the S3 client.
///
/// Present only when that transport will be installed, so a runtime given
/// `None` builds no HTTP client.
#[derive(Debug, Clone, Default)]
pub(crate) struct RuntimeHttpOptions {
    /// Interfaces to bind connections to, assigned to worker threads
    /// round-robin. Empty leaves interface selection to OS routing.
    pub(crate) network_interfaces: Vec<String>,
}

/// Components provided by the execution runtime to the rest of the system.
///
/// The runtime populates these based on its execution model.
#[derive(Debug, Clone, Default)]
pub(crate) struct RuntimeComponents {
    http_client: Option<SharedHttpClient>,
    /// When true, file I/O runs directly on the calling thread (managed threads).
    /// When false, file I/O is offloaded via spawn_blocking (shared runtimes).
    direct_io: bool,
}

impl RuntimeComponents {
    /// HTTP client appropriate for this runtime's execution model.
    pub(crate) fn http_client(&self) -> Option<&SharedHttpClient> {
        self.http_client.as_ref()
    }

    /// Set the HTTP client.
    pub(crate) fn set_http_client(&mut self, client: SharedHttpClient) {
        self.http_client = Some(client);
    }

    /// Whether file I/O should run directly on the calling thread.
    pub(crate) fn direct_io(&self) -> bool {
        self.direct_io
    }

    /// Set direct I/O mode.
    pub(crate) fn set_direct_io(&mut self, direct: bool) {
        self.direct_io = direct;
    }
}
