/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The functions the module imports from the embedding host's `host` module.
//!
//! The crate documentation states the contract that the host implements.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod raw {
    #[link(wasm_import_module = "host")]
    extern "C" {
        pub(super) fn monotonic_now() -> i64;
        pub(super) fn wall_clock_now() -> i64;
        pub(super) fn http_send(id: u32, ptr: *const u8, len: usize);
        pub(super) fn http_cancel(id: u32);
    }
}

/// Time on the host's monotonic clock. A negative reading is zero.
pub(crate) fn monotonic_now() -> Duration {
    // SAFETY: the import takes no arguments and only returns a value.
    let nanos = unsafe { raw::monotonic_now() };
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

/// The host's wall-clock time. A reading before the Unix epoch is the epoch.
pub(crate) fn wall_clock_now() -> SystemTime {
    // SAFETY: the import takes no arguments and only returns a value.
    let nanos = unsafe { raw::wall_clock_now() };
    UNIX_EPOCH + Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

/// Starts request `id`, serialized in `request`.
pub(crate) fn http_send(id: u32, request: &[u8]) {
    // SAFETY: the host reads `request.len()` bytes at its address only during
    // the call, and the slice stays borrowed until the call returns.
    unsafe { raw::http_send(id, request.as_ptr(), request.len()) }
}

/// Tells the host that the module dropped request `id` before it was answered.
pub(crate) fn http_cancel(id: u32) {
    // SAFETY: the import takes a plain integer and returns nothing.
    unsafe { raw::http_cancel(id) }
}
