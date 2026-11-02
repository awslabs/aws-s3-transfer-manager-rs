/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runtime tests for the transfer manager on `wasm32-unknown-unknown`.
//!
//! The crate builds a WebAssembly core module whose embedding host supplies
//! its clocks and performs its HTTP requests. The module runs its own
//! cooperative executor and sleeps on the host's monotonic clock. Its clients
//! send S3 requests to the host, which sends them to a mock S3 server. The
//! module never blocks: the host calls `scenario_step` repeatedly and, between
//! calls, waits for a response or the deadline that the call returned.
//!
//! The module imports exactly these functions from the module `host`:
//!
//! - `monotonic_now() -> i64` returns nanoseconds on a clock that never goes
//!   backwards.
//! - `wall_clock_now() -> i64` returns nanoseconds since the Unix epoch.
//! - `http_send(id: u32, ptr: *const u8, len: usize)` starts request `id`,
//!   serialized in the `len` bytes at `ptr`, which are valid only during the
//!   call. Request ids are nonzero and never reused within an instance.
//! - `http_cancel(id: u32)` reports that the module dropped request `id`
//!   before the host answered it. The host aborts the request and never
//!   answers it.
//!
//! Requests and responses use the wire format that the `host_http` module
//! documents. The host must not call into the module from an import.
//!
//! Exports:
//!
//! - `scenario_count() -> i32` returns the number of scenarios.
//! - `scenario_start(index: i32)` starts scenario `index` with fresh clients
//!   whose endpoint is the URL in the host buffer, such as
//!   `http://127.0.0.1:49152`. The server at the endpoint must have the
//!   bucket `bucket` and answer the first request for `bucket/retried` with
//!   `503 SlowDown`, as `s3-mock-server --slow-down-once bucket/retried
//!   bucket` does. It traps if a scenario is running.
//! - `scenario_step() -> i64` runs the executor until nothing can progress.
//!   It returns -1 once the scenario has passed and every worker it spawned
//!   has exited. Otherwise it returns the earliest live timer deadline on the
//!   `monotonic_now` clock, or `i64::MAX` when the scenario awaits only
//!   responses. It traps if no scenario is running.
//! - `host_buffer(len) -> *mut u8` returns a `len`-byte buffer for the host
//!   to fill before `scenario_start` or `host_respond` consumes it.
//! - `host_respond(id: u32) -> i32` answers request `id` with the response in
//!   the buffer. It returns 1, or 0 if the module dropped the request.
//! - `client_without_runtime() -> i32` constructs a client with the default
//!   runtime mode, which this target does not support. It must trap.
//! - `client_without_sleep() -> i32` constructs a client whose S3
//!   configuration has no sleep implementation. It must trap.
//! - `message_ptr() -> *const u8` and `message_len() -> usize` locate a UTF-8
//!   report with a `PASS <scenario>: <summary>` line for each passed scenario,
//!   followed by the failure or panic message.
//!
//! Every export traps on failure. The crate is empty on every other target.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

mod host;
mod host_http;
mod imports;
mod scenarios;
mod store;

use std::sync::Mutex;

/// Report text exposed through [`message_ptr`] and [`message_len`].
static MESSAGE: Mutex<String> = Mutex::new(String::new());

/// Appends one line to the report.
pub(crate) fn report(line: &str) {
    let mut message = MESSAGE.lock().unwrap_or_else(|e| e.into_inner());
    message.push_str(line);
    message.push('\n');
}

/// Records panic messages in the report before the module traps.
fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| report(&format!("panic: {info}"))));
}

/// Fails by panicking, which traps on this target.
fn fail(message: String) -> ! {
    panic!("{message}");
}

/// Number of scenarios.
#[no_mangle]
pub extern "C" fn scenario_count() -> i32 {
    i32::try_from(scenarios::scenario_count()).expect("scenario count fits in i32")
}

/// Starts scenario `index` against the endpoint URL in the host buffer.
#[no_mangle]
pub extern "C" fn scenario_start(index: i32) {
    install_panic_hook();
    let endpoint = match String::from_utf8(host_http::take_buffer().to_vec()) {
        Ok(endpoint) => endpoint,
        Err(e) => fail(format!("the endpoint is not UTF-8: {e}")),
    };
    let Ok(index) = usize::try_from(index) else {
        fail(format!("no scenario {index}"));
    };
    if let Err(message) = scenarios::start(index, endpoint) {
        fail(message);
    }
}

/// Drives the running scenario. Returns -1 once it has passed, or the
/// earliest timer deadline in nanoseconds, or `i64::MAX` without one.
#[no_mangle]
pub extern "C" fn scenario_step() -> i64 {
    match scenarios::step() {
        Ok(scenarios::Step::Passed) => -1,
        Ok(scenarios::Step::Waiting(None)) => i64::MAX,
        Ok(scenarios::Step::Waiting(Some(deadline))) => {
            i64::try_from(deadline.as_nanos()).unwrap_or(i64::MAX)
        }
        Err(message) => fail(message),
    }
}

/// Constructs a client with the default runtime mode. Expected to trap.
#[no_mangle]
pub extern "C" fn client_without_runtime() -> i32 {
    install_panic_hook();
    scenarios::client_without_runtime();
    report("constructed a client with the default runtime mode");
    1
}

/// Constructs a client without an S3 sleep implementation. Expected to trap.
#[no_mangle]
pub extern "C" fn client_without_sleep() -> i32 {
    install_panic_hook();
    scenarios::client_without_sleep();
    report("constructed a client without a sleep implementation");
    1
}

/// Address of the UTF-8 report.
#[no_mangle]
pub extern "C" fn message_ptr() -> *const u8 {
    MESSAGE.lock().unwrap_or_else(|e| e.into_inner()).as_ptr()
}

/// Length in bytes of the UTF-8 report.
#[no_mangle]
pub extern "C" fn message_len() -> usize {
    MESSAGE.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// Returns a `len`-byte buffer for the host to fill.
#[no_mangle]
pub extern "C" fn host_buffer(len: usize) -> *mut u8 {
    host_http::buffer(len)
}

/// Answers request `id` with the buffer's response. Returns 0 if it was dropped.
#[no_mangle]
pub extern "C" fn host_respond(id: u32) -> i32 {
    match host_http::respond(id) {
        Ok(delivered) => i32::from(delivered),
        Err(message) => fail(message),
    }
}
