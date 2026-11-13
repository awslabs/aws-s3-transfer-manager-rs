/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::transfer::{PendingCategory, PendingCause};

/// Why the `download_objects` state machine cannot produce another work item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadObjectsPendingReason {
    /// The object listing has not produced an entry to spawn.
    Listing,
    /// Entries are waiting, and the child concurrency limit is reached.
    ChildCapacity,
    /// The listing is exhausted, and running children must finish.
    ChildCompletion,
    /// Reaped children are still being recorded.
    Reaping,
    /// The transfer is cancelled or failed, and in-flight work must drain.
    Draining,
}

impl From<DownloadObjectsPendingReason> for PendingCause {
    fn from(reason: DownloadObjectsPendingReason) -> Self {
        match reason {
            DownloadObjectsPendingReason::Listing => Self::new(PendingCategory::Source, "listing"),
            DownloadObjectsPendingReason::ChildCapacity => Self::in_flight_work("child_capacity"),
            DownloadObjectsPendingReason::ChildCompletion => {
                Self::in_flight_work("child_completion")
            }
            DownloadObjectsPendingReason::Reaping => Self::in_flight_work("reap_completion"),
            DownloadObjectsPendingReason::Draining => Self::in_flight_work("draining"),
        }
    }
}
