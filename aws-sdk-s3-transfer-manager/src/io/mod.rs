/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Adapters for other IO library traits to map to `InputStream`
pub mod adapters;
/// Download Body Type
mod aggregated_bytes;
mod buffer;
pub(crate) mod part_reader;
mod path_body;
mod stream;

/// Error types related to I/O abstractions
pub mod error;
pub(crate) mod fs;
pub(crate) mod key;
// Unimplemented until S1-T11/S1-T12; its tests define the API.
#[allow(dead_code)]
pub(crate) mod key_filter;
// Nothing outside this module's own tests calls it until the comparison lands.
#[allow(dead_code)]
pub(crate) mod key_stream;
mod size_hint;
/// Walker types for traversing filesystems and S3 buckets.
pub mod walk;

// re-exports
pub use self::aggregated_bytes::AggregatedBytes;
pub(crate) use self::buffer::Buffer;
pub use self::path_body::PathBodyBuilder;
pub use self::size_hint::SizeHint;
pub use self::stream::InputStream;
pub use self::stream::PartData;
pub use self::stream::PartStream;
pub use self::stream::StreamContext;
