/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::transfer::{PendingCategory, PendingCause};

/// Why the `upload_objects` state machine cannot produce another work item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadObjectsPendingReason {
    /// The filesystem walk has not produced an entry to spawn.
    Listing,
    /// Entries are waiting, and the child concurrency limit is reached.
    ChildCapacity,
    /// The walk is exhausted, and running children must finish.
    ChildCompletion,
    /// Reaped children are still being recorded.
    Reaping,
    /// The transfer is cancelled or failed, and in-flight work must drain.
    Draining,
}

impl From<UploadObjectsPendingReason> for PendingCause {
    fn from(reason: UploadObjectsPendingReason) -> Self {
        match reason {
            UploadObjectsPendingReason::Listing => Self::new(PendingCategory::Source, "listing"),
            UploadObjectsPendingReason::ChildCapacity => Self::in_flight_work("child_capacity"),
            UploadObjectsPendingReason::ChildCompletion => Self::in_flight_work("child_completion"),
            UploadObjectsPendingReason::Reaping => Self::in_flight_work("reap_completion"),
            UploadObjectsPendingReason::Draining => Self::in_flight_work("draining"),
        }
    }
}
