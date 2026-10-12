/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared download test support.
//!
//! [`sink`] scripts faults into a real download's destination writes, and
//! [`fixtures`] builds the objects, S3 clients, handles and transfers those
//! tests run against.

pub(crate) mod fixtures;
pub(crate) mod sink;
