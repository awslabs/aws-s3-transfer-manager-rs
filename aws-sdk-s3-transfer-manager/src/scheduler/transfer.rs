/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types for scheduler integration.

use tokio_util::sync::CancellationToken;

use super::{PollWork, TransferId, WorkOutcome};
use crate::operation::download::DownloadTransfer;
use crate::operation::upload::UploadTransfer;
use crate::scheduler::WorkItem;

/// A transfer operation that generates and executes work.
///
/// Transfers are cheaply cloneable (interior mutability via Arc).
#[derive(Debug, Clone)]
pub(crate) enum Transfer {
    Upload(UploadTransfer),
    Download(DownloadTransfer),
    #[cfg(test)]
    Mock(MockTransfer),
}

impl Transfer {
    pub(crate) fn id(&self) -> TransferId {
        match self {
            Transfer::Upload(u) => u.id(),
            Transfer::Download(d) => d.id(),
            #[cfg(test)]
            Transfer::Mock(m) => m.id(),
        }
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - blocked, don't poll until woken
    /// - `PollWork::Done` - transfer complete
    pub(crate) fn poll_work(&self) -> PollWork {
        match self {
            Transfer::Upload(u) => u.poll_work(),
            Transfer::Download(d) => d.poll_work(),
            #[cfg(test)]
            Transfer::Mock(m) => m.poll_work(),
        }
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match self {
            Transfer::Upload(u) => u.execute(work).await,
            Transfer::Download(d) => d.execute(work).await,
            #[cfg(test)]
            Transfer::Mock(m) => m.execute(work).await,
        }
    }

    /// Get the cancellation token for this transfer.
    pub(crate) fn cancellation_token(&self) -> &CancellationToken {
        match self {
            Transfer::Upload(u) => u.cancellation_token(),
            Transfer::Download(d) => d.cancellation_token(),
            #[cfg(test)]
            Transfer::Mock(m) => m.cancellation_token(),
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(crate) use mock::MockTransfer;
