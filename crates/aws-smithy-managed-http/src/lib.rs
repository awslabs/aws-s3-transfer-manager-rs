/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![warn(
    missing_debug_implementations,
    missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]
#![doc = include_str!("../README.md")]

/// Connection-pool construction and clients with managed execution placement.
#[cfg(feature = "client")]
pub mod client;
