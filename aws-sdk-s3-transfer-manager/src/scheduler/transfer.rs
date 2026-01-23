/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types for scheduler integration.

use super::{TransferId, WorkItem, WorkOutcome};
use crate::operation::upload::UploadTransfer;

/// A transfer operation that generates and executes work.
///
/// Transfers are cheaply cloneable (interior mutability via Arc).
#[derive(Debug, Clone)]
pub(crate) enum Transfer {
    Upload(UploadTransfer),
    Download(DownloadTransfer),
}

impl Transfer {
    pub(crate) fn id(&self) -> TransferId {
        match self {
            Transfer::Upload(u) => u.id(),
            Transfer::Download(d) => d.id,
        }
    }

    pub(crate) fn next_work(&self) -> Option<WorkItem> {
        match self {
            Transfer::Upload(u) => u.next_work(),
            Transfer::Download(_) => todo!("download not yet implemented"),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        match self {
            Transfer::Upload(u) => u.is_done(),
            Transfer::Download(_) => todo!("download not yet implemented"),
        }
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match self {
            Transfer::Upload(u) => u.execute(work).await,
            Transfer::Download(_) => todo!("download not yet implemented"),
        }
    }
}

/// Download transfer - not yet implemented
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    pub(crate) id: TransferId,
}
