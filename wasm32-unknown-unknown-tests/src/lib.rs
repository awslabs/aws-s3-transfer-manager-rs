/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runtime tests for the transfer manager on `wasm32-unknown-unknown`.
//!
//! The crate builds a WebAssembly core module that imports nothing, so any
//! conforming engine can run it. The module supplies its own host: a
//! cooperative executor, a logical monotonic clock, a sleep implementation on
//! that clock, and an in-memory S3 connector whose requests complete only
//! after the executor delivers a simulated I/O completion.
//!
//! Exports:
//!
//! - `scenario_count() -> i32` returns the number of scenarios `run` runs.
//! - `run() -> i32` runs every scenario and returns how many passed, which
//!   equals `scenario_count()`. A failure traps.
//! - `client_without_runtime() -> i32` constructs a client with the default
//!   runtime mode, which this target does not support. It must trap.
//! - `client_without_sleep() -> i32` constructs a client whose S3
//!   configuration has no sleep implementation. It must trap.
//! - `message_ptr() -> *const u8` and `message_len() -> usize` locate a UTF-8
//!   report with a `PASS <scenario>: <summary>` line for each passed scenario,
//!   followed by the failure or panic message.
//!
//! The host round trip sends its requests to the embedding host, which plays
//! S3. Its exports use the wire format that the `host_http` module documents:
//!
//! - `host_buffer(len) -> *mut u8` returns a `len`-byte buffer for the host to
//!   fill before the next call below that consumes it.
//! - `host_round_trip_start() -> i32` starts uploading the buffer's bytes as
//!   one object, then downloading it.
//! - `host_round_trip_step() -> i32` drives the round trip until it needs the
//!   host. It returns 1 while requests await responses, and 0 once the
//!   downloaded bytes match the upload and every worker has exited.
//! - `host_next_request() -> u32` returns the id of the next unanswered
//!   request, or 0 when none is queued. `host_request_ptr() -> *const u8` and
//!   `host_request_len() -> usize` locate its bytes until the next call.
//! - `host_respond(id) -> i32` answers request `id` with the response in the
//!   buffer. It returns 1, or 0 if the request was dropped.
//!
//! Every export traps on failure. The crate is empty on every other target.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

mod host;
mod host_http;
mod scenarios;

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

/// Number of scenarios that [`run`] runs.
#[no_mangle]
pub extern "C" fn scenario_count() -> i32 {
    to_i32(scenarios::scenario_count())
}

/// Runs every scenario. Returns how many passed; any failure traps.
#[no_mangle]
pub extern "C" fn run() -> i32 {
    install_panic_hook();
    match scenarios::run_all() {
        Ok(passed) => to_i32(passed),
        Err(message) => fail(message),
    }
}

fn to_i32(count: usize) -> i32 {
    i32::try_from(count).expect("scenario count fits in i32")
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

/// Starts the host round trip with the buffer's bytes as the object.
#[no_mangle]
pub extern "C" fn host_round_trip_start() -> i32 {
    install_panic_hook();
    if let Err(message) = scenarios::host_round_trip_start(host_http::take_buffer()) {
        fail(message);
    }
    0
}

/// Drives the host round trip. Returns 1 while it awaits the host, 0 when done.
#[no_mangle]
pub extern "C" fn host_round_trip_step() -> i32 {
    match scenarios::host_round_trip_step() {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(message) => fail(message),
    }
}

/// Takes the next queued request. Returns its id, or 0 when none is queued.
#[no_mangle]
pub extern "C" fn host_next_request() -> u32 {
    host_http::next_request()
}

/// Address of the request most recently taken.
#[no_mangle]
pub extern "C" fn host_request_ptr() -> *const u8 {
    host_http::taken().0
}

/// Length in bytes of the request most recently taken.
#[no_mangle]
pub extern "C" fn host_request_len() -> usize {
    host_http::taken().1
}

/// Answers request `id` with the buffer's response. Returns 0 if it was dropped.
#[no_mangle]
pub extern "C" fn host_respond(id: u32) -> i32 {
    match host_http::respond(id) {
        Ok(delivered) => i32::from(delivered),
        Err(message) => fail(message),
    }
}
