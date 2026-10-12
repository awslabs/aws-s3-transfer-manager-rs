/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! HTTP clients bound to managed execution partitions.
//!
//! [`crate::client::ManagedHttpPool`] combines a native connection-pool builder with an
//! [`aws_ort_execution::ManagedRuntimeHandle`]. Transport configuration stays on
//! the native builder. Each lookup returns an existing [`crate::client::Client`] bound to the
//! explicitly chosen execution partition.
//!
//! Clients implement Smithy's `HttpClient` and support its `IntoShared`
//! conversion. The caller may poll SDK operations on another runtime; connection
//! establishment and connection-owned tasks use the selected managed partition.
//! The native pool's reuse scope still determines whether peer connections may
//! serve a request.
//!
//! Retain the runtime owner while using these clients. Pool construction and
//! client clones retain execution capabilities, not ownership of runtime
//! shutdown.
//!
//! # Features
//!
//! Enable `client` to use this module. For HTTPS, enable a TLS provider feature on
//! `aws-smithy-http-client` and select it on the supplied native builder.
//! `dial9` enables the execution crate's optional instrumentation; the runtime
//! builder still requires a caller-supplied recorder handle to attach tracing.
//!
//! ```rust,no_run
//! use aws_ort_execution::ManagedRuntime;
//! use aws_ort_http::client::{ManagedHttpPool, pool};
//! use aws_smithy_runtime_api::client::http::SharedHttpClient;
//! use aws_smithy_runtime_api::shared::IntoShared;
//!
//! let runtime = ManagedRuntime::builder().build()?;
//! let execution = runtime.handle();
//! // A cleartext transport; select a provider on the native builder for TLS.
//! let transport = pool::ConnectionPool::builder();
//! let http = ManagedHttpPool::builder()
//!     .pool_builder(transport)
//!     .runtime(execution.clone())
//!     .build()?;
//! let client = http.client_for_partition(execution.partitions()[0].id())?;
//! let shared: SharedHttpClient = client.into_shared();
//! # let _ = shared;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod builder;
mod error;
mod managed;

pub use builder::Builder;
pub use error::{BuildError, PartitionError};
pub use managed::ManagedHttpPool;

/// Native connection-pool configuration, placement, observation, and clients.
pub use aws_smithy_http_client::pool;
/// The native Smithy client bound to one pool partition.
pub use aws_smithy_http_client::pool::Client;
/// Native proxy configuration.
pub use aws_smithy_http_client::proxy;
