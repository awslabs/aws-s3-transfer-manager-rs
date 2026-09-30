/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Native core of the `aws_sdk_s3_transfer_manager` Python package.
//!
//! The public Python API lives in the package's Python modules; this extension
//! exposes the private `_core` module they are built on.

mod body;
mod client;
mod convert;
mod download;
mod error;
mod options;
mod progress;
mod runtime;

use pyo3::pymodule;

#[pymodule]
mod _core {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::client::Client;
    #[pymodule_export]
    use super::download::DownloadStream;
    #[pymodule_export]
    use super::runtime::Operation;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        super::runtime::init();
        m.add("__version__", env!("CARGO_PKG_VERSION"))
    }
}
