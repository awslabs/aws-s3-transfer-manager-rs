/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! HTTP clients bound to managed execution partitions.
//!
//! [`crate::client::ManagedHttpPool`] combines native connection-pool configuration with an
//! [`aws_smithy_execution::ManagedRuntimeHandle`]. Transport settings stay on
//! the native builder. [`crate::client::ManagedHttpPool::from_pool`] accepts a prebuilt native
//! pool, preserving its partition configuration. Manually configured native
//! spawners must submit work to the corresponding execution partitions.
//!
//! The pool implements Smithy's `HttpClient` and supports its `IntoShared`
//! conversion. It selects the current partition when the caller is executing
//! on this runtime, or a random partition for outside or foreign-runtime
//! callers. [`crate::client::ManagedHttpPool::client_for_partition`] returns an existing
//! [`crate::client::Client`] for explicit targeting.
//!
//! The caller may poll SDK operations on another runtime. Connection
//! establishment and connection-owned tasks use the native partition's spawner.
//! The native pool's reuse scope may borrow another partition's connection, so
//! client selection does not guarantee request-body polling locality.
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
//! use aws_smithy_execution::ManagedRuntime;
//! use aws_smithy_managed_http::client::{ManagedHttpPool, pool};
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
//! let shared: SharedHttpClient = http.into_shared();
//! # let _ = (client, shared);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod builder;
mod error;
mod managed;

pub use builder::Builder;
pub use error::{BuildError, BuildErrorKind, PartitionError};
pub use managed::ManagedHttpPool;

/// Native connection-pool configuration, placement, observation, and clients.
pub use aws_smithy_http_client::pool;
/// The native Smithy client bound to one pool partition.
pub use aws_smithy_http_client::pool::Client;
/// Native proxy configuration.
pub use aws_smithy_http_client::proxy;
