/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synchronization primitives for the runtime.

mod submission;
pub(crate) use submission::{SubmissionGuard, SubmissionQueue};
