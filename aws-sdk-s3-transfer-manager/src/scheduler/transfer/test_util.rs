/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared test utilities for transfer state machine tests.

use crate::scheduler::{PollWork, Transfer, WorkItem, WorkOutcome};

/// Assert poll returns Ready and extract the work item.
pub(crate) fn assert_ready(poll: PollWork) -> WorkItem {
    match poll {
        PollWork::Ready(w) => w,
        PollWork::Pending => panic!("expected Ready, got Pending"),
        PollWork::Done => panic!("expected Ready, got Done"),
    }
}

/// Assert poll returns Pending.
pub(crate) fn assert_pending(poll: PollWork) {
    assert!(
        matches!(poll, PollWork::Pending),
        "expected Pending, got {:?}",
        poll
    );
}

/// Assert poll returns Done.
pub(crate) fn assert_done(poll: PollWork) {
    assert!(
        matches!(poll, PollWork::Done),
        "expected Done, got {:?}",
        poll
    );
}

/// Execute work and handle follow-on work (e.g., DataIO -> Network phase transitions).
pub(crate) async fn execute(transfer: &Transfer, work: &mut WorkItem) -> WorkOutcome {
    let outcome = transfer.execute(work).await;

    // If there's follow-on work, execute it too
    if let WorkOutcome::Success {
        schedule_next: Some(kind),
        data,
    } = outcome
    {
        let mut follow_on = WorkItem {
            transfer_id: work.transfer_id,
            kind,
            data,
        };
        return transfer.execute(&mut follow_on).await;
    }
    outcome
}
