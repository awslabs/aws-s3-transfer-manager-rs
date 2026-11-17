/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Support for the integrity case tables: the words, the macros that declare cases, and the
//! runner and checks a case's test functions call. The macros' self-test is the only test here.

pub(crate) mod expect;
#[macro_use]
pub(crate) mod macros;
pub(crate) mod runner;
pub(crate) mod vocab;
