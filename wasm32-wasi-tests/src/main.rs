/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runtime tests for the transfer manager on `wasm32-wasip2` and
//! `wasm32-wasip3`.
//!
//! Usage: `wasm32-wasi-tests <work-dir> <server-address>`, where `<work-dir>`
//! is an empty, writable directory that the host preopens, and
//! `<server-address>` is the IP address and port of a running
//! `s3-mock-server`, such as `127.0.0.1:49152`. The server must have the
//! bucket `bucket`, and the host must let the module open TCP connections to
//! the address.
//!
//! The binary runs every scenario on a current-thread tokio runtime. Most
//! scenarios run twice: against an in-memory S3 connector, and over HTTP to the
//! server. File scenarios read and write under `<work-dir>`. The binary first
//! prints `wasm32-wasi-tests: <count> scenarios on <target>`, where each run
//! counts as a scenario. For each run that passes it prints `PASS <transport>
//! <scenario>: <summary>`, where the transport is `in-memory` or `HTTP`, and
//! the summary counts the requests the run sent and the tasks that exited when
//! its client was dropped. After the last run it prints `<count> scenarios
//! passed`. On the first failure it prints `FAIL <transport> <scenario>:
//! <reason>` to standard error and exits with status 1. A panic aborts the
//! module.
//!
//! On other targets the binary prints an error and exits with status 1.

#[cfg(target_os = "wasi")]
mod http;
#[cfg(target_os = "wasi")]
mod scenarios;
#[cfg(target_os = "wasi")]
mod store;

#[cfg(target_os = "wasi")]
fn main() {
    const USAGE: &str = "usage: wasm32-wasi-tests <work-dir> <server-address>";
    let mut args = std::env::args().skip(1);
    let (Some(work), Some(server)) = (args.next(), args.next()) else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    let Ok(server) = server.parse() else {
        eprintln!("{USAGE}\n<server-address> must be an IP address and port: {server}");
        std::process::exit(2);
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("a current-thread runtime builds on WASI");
    if let Err(message) = runtime.block_on(scenarios::run_all(std::path::Path::new(&work), server))
    {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "wasi"))]
fn main() {
    eprintln!("wasm32-wasi-tests runs only on WASI targets");
    std::process::exit(1);
}
