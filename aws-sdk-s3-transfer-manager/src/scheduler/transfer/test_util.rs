/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared test utilities for transfer state machine tests.

use crate::scheduler::{PollWork, WorkItem};

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
