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

/// Returns the table case whose full path ends with `suffix`.
///
/// Panics when neither case table contains a matching path.
pub(super) fn table_case(suffix: &str) -> &'static vocab::Case {
    super::table_cases()
        .into_iter()
        .find(|case| case.path.ends_with(suffix))
        .unwrap_or_else(|| panic!("no case ends with {suffix}"))
}
