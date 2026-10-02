/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Construction of the positioned-write target behind a disk download.

use super::body::{FileSink, SinkWrite};

/// Opens the [`SinkWrite`] that receives one disk download's positioned writes,
/// preparation, and finalization.
///
/// Constructing the sink apart from the transfer lets destination I/O be
/// replaced without changing how the transfer schedules and drains its writes.
pub(crate) trait SinkFactory: Send + Sync + std::fmt::Debug {
    /// Returns the sink for `file`. `owns_file` is true when the transfer
    /// manager created `file` and may therefore preallocate it.
    fn open(&self, file: std::fs::File, owns_file: bool) -> Box<dyn SinkWrite>;
}

/// [`SinkFactory`] whose sinks write directly to the destination file.
#[derive(Debug, Default)]
pub(crate) struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    fn open(&self, file: std::fs::File, owns_file: bool) -> Box<dyn SinkWrite> {
        Box::new(FileSink::new(file, owns_file))
    }
}
