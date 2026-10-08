/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! This module holds the settings a caller chooses for a sync run.
//!
//! The settings say how many children may run at once, whether the run may delete, and what a
//! failure does to the rest of the run.

use crate::types::FailedTransferPolicy;

// Whether a run may remove destination keys. The named type shows a caller what `true` would
// enable.
//
// Delete mode is off by default. A delete can remove data the caller never sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DeleteMode {
    On,
    #[default]
    Off,
}

// Settings chosen by the caller. The comparison, child factory, and deleter choose the direction.
#[derive(Debug, Clone)]
pub(crate) struct RunSettings {
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    pub(crate) max_children: usize,
    pub(crate) delete_mode: DeleteMode,
    // Sync continues after a failure unless the caller selects `Abort`.
    pub(crate) failure_policy: FailedTransferPolicy,
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            max_children: crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN,
            delete_mode: DeleteMode::default(),
            failure_policy: FailedTransferPolicy::Continue,
        }
    }
}
