/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synchronization primitives for the runtime.

mod submission;
pub(crate) use submission::{Submission, SubmissionGuard, SubmissionQueue};

// Loom compatibility layer — swaps std/parking_lot for loom types under `cfg(s3_tm_loom)`.
#[cfg(not(all(test, s3_tm_loom)))]
#[allow(unused)]
mod std;
#[cfg(not(all(test, s3_tm_loom)))]
#[allow(unused_imports)] // TODO(loom): remove once submission.rs uses the compat layer
pub(crate) use self::std::*;

#[cfg(all(test, s3_tm_loom))]
#[allow(unused)]
mod loom;
#[cfg(all(test, s3_tm_loom))]
#[allow(unused_imports)] // TODO(loom): remove once submission.rs uses the compat layer
pub(crate) use self::loom::*;
