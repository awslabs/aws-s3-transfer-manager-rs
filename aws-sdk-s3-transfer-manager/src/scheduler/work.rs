/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Work abstraction for the scheduler.

use super::concurrency::ErrorKind;
use crate::metrics::IoSample;
use std::any::Any;

/// Opaque work data carried by work items. Each state machine defines its own type.
/// The scheduler never inspects this — it ferries it across the scheduling boundary
/// for the transfer to reclaim via `IoRequest::data_mut::<T>()`.
pub(crate) trait WorkData: Any + Send + std::fmt::Debug {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any + Send + std::fmt::Debug> WorkData for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// The kind of I/O to be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoKind {
    /// Disk I/O (read for uploads, write for downloads)
    Disk,
    /// HTTP request (uploads and downloads)
    Network,
}

/// Unique identifier for a transfer, with optional parent for hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TransferId {
    pub(crate) id: u64,
    pub(crate) parent: Option<u64>,
}

impl std::fmt::Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parent {
            Some(parent) => write!(f, "{}-{}", self.id, parent),
            None => write!(f, "{}", self.id),
        }
    }
}

/// A unit of I/O to be scheduled and executed by the runtime.
#[derive(Debug)]
pub(crate) struct IoRequest {
    pub(crate) kind: IoKind,
    pub(crate) data: Option<Box<dyn WorkData>>,
}

impl IoRequest {
    /// Downcast data to a concrete type. Panics if wrong type or None.
    pub(crate) fn data_mut<T: 'static>(&mut self) -> &mut T {
        (**self.data.as_mut().expect("work item has no data"))
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("work data type mismatch")
    }
}

/// Result of polling a transfer for work.
#[derive(Debug)]
pub(crate) enum PollWork {
    /// Work is available to execute.
    Ready(IoRequest),
    /// Transfer is blocked waiting for in-flight work to complete.
    /// Scheduler should not poll again until `wake(transfer_id)` is called.
    Pending,
    /// Transfer has completed all work.
    Done,
}

/// Result of executing a work item.
///
/// Contract between transfer state machines and the scheduler:
/// - `Success`: Transfer is still active. Scheduler handles follow-on work and continues polling.
/// - `Failed`: Transfer has already transitioned itself to terminal state (via `set_failed` +
///   `signal_terminal`). Scheduler will not poll it again and will remove it once idle.
/// - `Cancelled`: Transfer is already terminal (failed or cancelled by another work item).
///   Same cleanup as `Failed`.
pub(crate) enum WorkOutcome {
    /// Work completed successfully. Optionally schedule follow-on work.
    Success {
        schedule_next: Option<IoKind>,
        data: Option<Box<dyn WorkData>>,
        metrics: Option<IoSample>,
    },
    /// Work failed. Transfer must have called `set_failed` + `signal_terminal` before returning.
    Failed { classification: Option<ErrorKind> },
    /// Work was skipped or aborted because the transfer is already terminal.
    Cancelled,
}

impl std::fmt::Debug for WorkOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkOutcome::Success {
                schedule_next,
                data,
                metrics,
            } => f
                .debug_struct("Success")
                .field("schedule_next", schedule_next)
                .field("data", data)
                .field("metrics", metrics)
                .finish(),
            WorkOutcome::Failed { classification } => f
                .debug_struct("Failed")
                .field("classification", classification)
                .finish(),
            WorkOutcome::Cancelled => write!(f, "Cancelled"),
        }
    }
}
