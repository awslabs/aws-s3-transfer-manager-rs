/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::time::{Duration, Instant};

use aws_sdk_s3_transfer_manager::operation::download::ManagedDownloadHandle;
use aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsHandle;
use aws_sdk_s3_transfer_manager::operation::upload::UploadHandle;
use aws_sdk_s3_transfer_manager::operation::upload_objects::UploadObjectsHandle;
use aws_sdk_s3_transfer_manager::types::TransferStatus;

/// How often a tracked transfer is checked for completion.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Minimum time between progress reports while a transfer is running.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// Receives the number of bytes transferred since the previous report.
pub(crate) struct Reporter(Box<dyn Fn(u64) + Send + Sync>);

impl Reporter {
    pub(crate) fn new(report: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self(Box::new(report))
    }
}

/// A transfer handle whose progress can be observed before it is joined.
pub(crate) trait Tracked {
    fn status(&self) -> TransferStatus;

    /// Payload bytes moved so far, capped at the expected total when known.
    fn transferred(&self) -> u64;
}

macro_rules! impl_tracked {
    ($($handle:ty => $field:ident),* $(,)?) => {$(
        impl Tracked for $handle {
            fn status(&self) -> TransferStatus {
                <$handle>::status(self)
            }

            fn transferred(&self) -> u64 {
                let metrics = self.metrics();
                metrics
                    .total_bytes
                    .map_or(metrics.$field, |total| metrics.$field.min(total))
            }
        }
    )*};
}

impl_tracked! {
    UploadHandle => network_tx,
    ManagedDownloadHandle => network_rx,
    UploadObjectsHandle => network_tx,
    DownloadObjectsHandle => network_rx,
}

/// Reports progress on `handle` until it reaches a terminal state.
///
/// Returns immediately when there is no reporter, leaving completion to the
/// caller's `join`, which does not poll.
pub(crate) async fn track(handle: &impl Tracked, reporter: Option<&Reporter>) {
    let Some(Reporter(report)) = reporter else {
        return;
    };
    let mut reported = 0;
    let mut last_report = Instant::now();
    loop {
        let finished = handle.status().is_terminal();
        if finished || last_report.elapsed() >= REPORT_INTERVAL {
            let transferred = handle.transferred();
            if transferred > reported {
                report(transferred - reported);
                reported = transferred;
            }
            last_report = Instant::now();
        }
        if finished {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
