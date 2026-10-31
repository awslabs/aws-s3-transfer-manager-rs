/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runtime tests for the transfer manager on `wasm32-wasip2` and
//! `wasm32-wasip3`.
//!
//! Usage: `wasm32-wasi-tests <work-dir>`, where `<work-dir>` is an empty,
//! writable directory that the host preopens.
//!
//! The binary runs every scenario on a current-thread tokio runtime against an
//! in-memory S3 connector, and file scenarios read and write under
//! `<work-dir>`. It first prints `wasm32-wasi-tests: <count> scenarios on
//! <target>`. For each scenario that passes it prints `PASS <scenario>:
//! <summary>`, where the summary counts the requests and bytes the connector
//! served and the transfer-manager tasks that exited when the client was
//! dropped. After the last scenario it prints `<count> scenarios passed`. On
//! the first failure it prints `FAIL <scenario>: <reason>` to standard error
//! and exits with status 1. A panic aborts the module.
//!
//! On other targets the binary prints an error and exits with status 1.

#[cfg(target_os = "wasi")]
mod scenarios;

#[cfg(target_os = "wasi")]
fn main() {
    let Some(work) = std::env::args().nth(1) else {
        eprintln!("usage: wasm32-wasi-tests <work-dir>");
        std::process::exit(2);
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a current-thread runtime builds on WASI");
    if let Err(message) = runtime.block_on(scenarios::run_all(std::path::Path::new(&work))) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "wasi"))]
fn main() {
    eprintln!("wasm32-wasi-tests runs only on WASI targets");
    std::process::exit(1);
}
