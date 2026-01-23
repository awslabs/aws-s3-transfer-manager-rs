/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::{Arc, Mutex};

use super::{TransferId, WorkItem, WorkOutcome, WorkPhase};

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

    pub(crate) async fn execute(&self, work: &WorkItem) -> WorkOutcome {
        match self {
            Transfer::Upload(u) => u.execute(work).await,
            Transfer::Download(_) => todo!("download not yet implemented"),
        }
    }
}

/// Upload transfer stub for testing.
///
/// Real implementation will wrap `Arc<UploadState>` or similar.
#[derive(Debug, Clone)]
pub(crate) struct UploadTransfer {
    inner: Arc<Mutex<UploadTransferInner>>,
}

#[derive(Debug)]
struct UploadTransferInner {
    id: TransferId,
    remaining: usize,
}

impl UploadTransfer {
    /// Create stub with N work items to generate
    #[cfg(test)]
    pub(crate) fn stub(id: TransferId, work_count: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UploadTransferInner {
                id,
                remaining: work_count,
            })),
        }
    }

    fn id(&self) -> TransferId {
        self.inner.lock().unwrap().id
    }

    fn next_work(&self) -> Option<WorkItem> {
        let mut inner = self.inner.lock().unwrap();
        if inner.remaining == 0 {
            return None;
        }
        inner.remaining -= 1;
        Some(WorkItem {
            transfer_id: inner.id,
            phase: WorkPhase::DataIO,
        })
    }

    fn is_done(&self) -> bool {
        self.inner.lock().unwrap().remaining == 0
    }

    async fn execute(&self, work: &WorkItem) -> WorkOutcome {
        // Stub: return success with next phase
        let next_phase = match work.phase {
            WorkPhase::DataIO => Some(WorkPhase::Network),
            WorkPhase::Network => None,
        };
        WorkOutcome::Success { next_phase }
    }
}

/// Download transfer - not yet implemented
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    id: TransferId,
}
