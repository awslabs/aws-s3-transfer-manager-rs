/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Execution configuration with managed ownership or caller-provided Tokio execution.

use crate::runtime::buffer_pool::BufferPool;
use crate::types::ByteSize;
use aws_smithy_types::error::operation::BuildError;

/// Selects execution for transfer-manager work.
///
/// Defaults to owned managed execution. Configuring HTTP on the SDK or S3
/// configuration does not change this selection.
///
/// ```
/// use aws_s3_transfer::{config::Managed, config::RuntimeConfig};
///
/// let managed = RuntimeConfig::managed(Managed::default());
/// let caller_runtime = RuntimeConfig::multi_thread_tokio();
/// ```
///
/// # HTTP transport
///
/// With managed execution and no HTTP client configured, the transfer manager
/// builds its own transport:
///
/// - One connection pool, partitioned by managed thread, so each connection is
///   driven by the thread that opened it.
/// - Connections per host capped according to the process's file-descriptor
///   limit. Requests beyond the cap wait for a connection.
/// - Proxy settings from `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and
///   `NO_PROXY`.
///
/// wasm targets have no such transport; see [`Managed`].
///
/// An HTTP client set on the [`SdkConfig`](aws_types::SdkConfig) or S3
/// configuration replaces this transport: its pool, TLS, DNS and proxy settings
/// apply, its connections are not partitioned by managed thread, and the
/// transfer manager does not cap its connections per host. With
/// [`multi_thread_tokio`](Self::multi_thread_tokio), the S3 client uses the
/// configured HTTP client or, without one, the SDK default.
///
/// Configure an HTTP client to change TLS, DNS or connection settings.
#[non_exhaustive]
#[derive(Debug)]
pub struct RuntimeConfig {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Managed(Managed),
    MultiThreadTokio,
}

impl RuntimeConfig {
    /// Uses execution owned by the transfer manager.
    pub fn managed(managed: Managed) -> Self {
        Self {
            kind: Kind::Managed(managed),
        }
    }

    /// Uses the caller's Tokio runtime without creating managed threads.
    ///
    /// Transfer tasks are spawned on the runtime performing dispatch. Use a
    /// multi-thread runtime so concurrent work can execute on different threads.
    /// wasm targets are the exception: their threads are unavailable or
    /// cooperative, so a current-thread runtime is supported there.
    pub fn multi_thread_tokio() -> Self {
        Self {
            kind: Kind::MultiThreadTokio,
        }
    }

    pub(crate) fn managed_config(&self) -> Option<&Managed> {
        match &self.kind {
            Kind::Managed(managed) => Some(managed),
            Kind::MultiThreadTokio => None,
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn managed_config_mut(&mut self) -> Option<&mut Managed> {
        match &mut self.kind {
            Kind::Managed(managed) => Some(managed),
            Kind::MultiThreadTokio => None,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self::managed(Managed::default())
    }
}

/// Configures managed execution owned by the transfer manager.
///
/// Default construction discovers usable execution targets when the client
/// is created. Active transfers retain the owner after client clones are
/// dropped. Owner destruction requests shutdown without synchronously joining
/// the managed threads. [`crate::Client::shutdown_completion`] provides a
/// non-owning observer of their joins.
///
/// wasm targets cannot run managed threads. There, managed execution behaves
/// as [`RuntimeConfig::multi_thread_tokio`], including its support for a
/// current-thread runtime, and creates no owner to observe.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Managed {
    #[cfg_attr(
        target_family = "wasm",
        expect(dead_code, reason = "wasm targets build no managed runtime")
    )]
    source: Source,
}

#[derive(Debug, Default)]
enum Source {
    #[default]
    Automatic,
    #[cfg(feature = "__unstable_ext")]
    Adopted(Option<aws_smithy_execution::ManagedRuntime>),
}

impl Managed {
    #[cfg(feature = "__unstable_ext")]
    pub(crate) fn from_runtime(runtime: aws_smithy_execution::ManagedRuntime) -> Self {
        Self {
            source: Source::Adopted(Some(runtime)),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn build(
        &mut self,
    ) -> Result<aws_smithy_execution::ManagedRuntime, aws_smithy_runtime_api::box_error::BoxError>
    {
        match &mut self.source {
            Source::Automatic => aws_smithy_execution::ManagedRuntime::builder()
                .thread_name_prefix("s3-tm")
                .pin_threads(false)
                .build()
                .map_err(Into::into),
            #[cfg(feature = "__unstable_ext")]
            Source::Adopted(owner) => {
                // Config retains its managed selection after execution takes
                // ownership. A second take must not construct another runtime.
                Ok(owner.take().expect("adopted managed runtime already taken"))
            }
        }
    }
}

#[cfg(all(test, feature = "__unstable_ext", not(miri)))]
mod tests {
    use super::*;
    use aws_smithy_execution::{topology::Topology, ManagedRuntime};

    #[test]
    fn adoption_preserves_identity_topology_and_names_and_takes_owner_once() {
        let runtime = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .thread_name_prefix("external")
            .pin_threads(false)
            .build()
            .unwrap();
        let before = runtime.handle();
        let mut managed = Managed::from_runtime(runtime);
        let runtime = managed.build().unwrap();
        let after = runtime.handle();
        assert_eq!(before.id(), after.id());
        assert_eq!(
            before.topology().placements(),
            after.topology().placements()
        );
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let partition = after.partitions()[0].clone();
        let expected = partition.id();
        let observed = after.clone();
        caller
            .block_on(partition.spawn(async move {
                assert_eq!(observed.current_partition().unwrap().id(), expected);
                assert!(std::thread::current()
                    .name()
                    .unwrap()
                    .starts_with("external-"));
            }))
            .unwrap();
        let repeated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| managed.build()));
        assert!(
            repeated.is_err(),
            "a consumed owner must not become automatic execution"
        );
        caller.block_on(runtime.shutdown()).unwrap();
    }
}

/// The concurrency mode the client should use for executing requests.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum ConcurrencyMode {
    /// Automatically configured concurrency based on the execution environment.
    #[default]
    Auto,

    /// Concurrency sized for a network throughput target, in gigabits per second.
    ///
    /// The target replaces the throughput estimated from the execution
    /// environment. It sets the in-flight request target once, when the client
    /// is constructed; it does not limit throughput at run time. Requests beyond
    /// what the per-host connection cap or the payload-memory ceiling allows wait
    /// for a connection or for memory.
    TargetThroughput(TargetThroughput),

    /// Explicit concurrency control
    Explicit(usize),
}

/// A network throughput target for [`ConcurrencyMode::TargetThroughput`].
///
/// One target currently applies to uploads and downloads alike. The value is
/// private so that per-direction targets can be added later without breaking
/// callers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TargetThroughput {
    gbps: f64,
}

impl TargetThroughput {
    /// A target of `gbps` gigabits per second, for uploads and downloads.
    ///
    /// Returns an error unless `gbps` is finite and positive.
    pub fn gbps(gbps: f64) -> Result<Self, BuildError> {
        if gbps.is_finite() && gbps > 0.0 {
            Ok(Self { gbps })
        } else {
            Err(BuildError::invalid_field(
                "gbps",
                format!("throughput target must be finite and positive, got {gbps}"),
            ))
        }
    }

    /// The target in gigabits per second.
    pub fn as_gbps(&self) -> f64 {
        self.gbps
    }
}

/// Selects the payload-memory pool used by a transfer-manager client.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum MemoryConfig {
    /// Construct an automatically sized pool when the client is created.
    ///
    /// The ceiling follows [`MemoryBudgetConfig::Auto`], raised to at least
    /// 256 MiB but not above half of detected memory.
    #[default]
    Auto,
    /// Use an existing pool.
    ///
    /// Every client or component using a clone of this pool shares one
    /// admission ceiling, prepared-storage cache, and metrics domain.
    Explicit(BufferPool),
}

/// Configured capacity for reservation-based pooled transfer data.
///
/// Requests that fit within the resolved capacity backpressure when capacity
/// is unavailable. A single reservation larger than the resolved capacity is
/// rejected. Unreserved acquisition and process or runtime overhead are
/// outside this ceiling.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum MemoryBudgetConfig {
    /// Use up to one quarter of detected effective memory, capped at 32 GiB.
    ///
    /// Effective memory includes process or container limits where the platform
    /// exposes them. The result may be rounded down to the pool's allocation
    /// unit. If detection is unavailable, the pool uses 2 GiB.
    #[default]
    Auto,
    /// Use the given fraction of detected effective memory.
    ///
    /// The fraction must be finite and in `(0.0, 1.0]`. This policy requires
    /// memory detection and does not apply the automatic 32 GiB cap.
    Fraction(f64),
    /// An explicit byte limit that bypasses memory detection.
    ///
    /// The resolved capacity never exceeds this value and may be smaller by
    /// less than one platform allocation unit. Values too small to fund one
    /// allocation unit are rejected, and so is a pool that cannot fund one part
    /// of the configured part size when it is passed to
    /// [`MemoryConfig::Explicit`] and the config is built. The resolved capacity
    /// is reported by
    /// [`MemoryMetrics::capacity_bytes`].
    ///
    /// [`MemoryMetrics::capacity_bytes`]:
    ///     crate::metrics::MemoryMetrics::capacity_bytes
    Limit(ByteSize),
}
