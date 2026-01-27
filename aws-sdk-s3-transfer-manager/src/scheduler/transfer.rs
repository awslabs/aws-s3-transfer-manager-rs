/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types for scheduler integration.

use super::{PollWork, TransferId, WorkOutcome};
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
            Transfer::Download(d) => d.id,
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
            Transfer::Download(_) => todo!("download not yet implemented"),
            #[cfg(test)]
            Transfer::Mock(m) => m.poll_work(),
        }
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match self {
            Transfer::Upload(u) => u.execute(work).await,
            Transfer::Download(_) => todo!("download not yet implemented"),
            #[cfg(test)]
            Transfer::Mock(m) => m.execute(work).await,
        }
    }
}

/// Download transfer - not yet implemented
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    pub(crate) id: TransferId,
}

/// Mock transfer for testing scheduler behavior
#[cfg(test)]
mod mock {
    use super::*;
    use crate::scheduler::{WorkData, WorkKind};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    /// A configurable mock transfer for scheduler tests.
    #[derive(Debug, Clone)]
    pub(crate) struct MockTransfer {
        inner: Arc<MockTransferInner>,
    }

    #[derive(Debug)]
    struct MockTransferInner {
        id: TransferId,
        total_work: u64,
        next_work_num: AtomicU64,
        completed: AtomicU64,
        done: AtomicBool,
    }

    impl MockTransfer {
        pub(crate) fn new(id: TransferId, work_count: u64) -> Self {
            Self {
                inner: Arc::new(MockTransferInner {
                    id,
                    total_work: work_count,
                    next_work_num: AtomicU64::new(0),
                    completed: AtomicU64::new(0),
                    done: AtomicBool::new(false),
                }),
            }
        }

        pub(crate) fn id(&self) -> TransferId {
            self.inner.id
        }

        pub(crate) fn poll_work(&self) -> PollWork {
            if self.inner.done.load(Ordering::SeqCst) {
                return PollWork::Done;
            }

            let num = self.inner.next_work_num.fetch_add(1, Ordering::SeqCst);
            if num >= self.inner.total_work {
                // Reset so we don't keep incrementing
                self.inner
                    .next_work_num
                    .store(self.inner.total_work, Ordering::SeqCst);
                // All work generated, but not done until completed
                return PollWork::Pending;
            }
            PollWork::Ready(WorkItem {
                transfer_id: self.inner.id,
                kind: WorkKind::Network,
                data: WorkData::UploadPart {
                    part_number: num + 1,
                    part_data: None,
                },
            })
        }

        pub(crate) async fn execute(&self, _work: &mut WorkItem) -> WorkOutcome {
            let completed = self.inner.completed.fetch_add(1, Ordering::SeqCst) + 1;
            if completed >= self.inner.total_work {
                self.inner.done.store(true, Ordering::SeqCst);
            }
            WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::UploadPart {
                    part_number: completed,
                    part_data: None,
                },
            }
        }
    }
}

#[cfg(test)]
pub(crate) use mock::MockTransfer;
