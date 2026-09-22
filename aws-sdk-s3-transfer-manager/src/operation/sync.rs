/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Deciding, key by key, what a sync should do about what each side holds.
//
// Enumeration produces two streams of keyed entries. The walk joins them into one view per
// key, and a comparison answers what to do about each one. They are kept apart so that a
// caller can replace the comparison without touching the join.
//
// Neither performs I/O of its own, which is why both live here and not under `io`. The walk
// drives two streams that do, and a comparison is a plain function, so a per-key request is
// out of its reach.

pub(crate) mod compare;
pub(crate) mod walk;
