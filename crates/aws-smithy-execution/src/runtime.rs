/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    BuildError, ManagedRuntimeHandle, RuntimeId, RuntimePartition, RuntimePartitionId,
    ShutdownError, handle::RuntimeState, identity, shutdown, thread as managed_thread,
};
use aws_smithy_topology::Topology;
use std::{io, sync::Arc, thread};
use tokio::sync::watch;

/// Owns execution partitions and their managed threads.
///
/// Each partition runs a Tokio current-thread runtime on its managed thread.
/// Construction creates one partition per selected topology placement. Retain this
/// owner while work should run; handles and partitions do not own its lifetime.
///
/// Dropping the owner requests shutdown and detaches its managed threads without
/// waiting for them, including when called from one of them. Each thread destroys
/// its Tokio runtime as it exits. Running tasks must yield for shutdown, and
/// Tokio blocking tasks can delay thread exit.
///
/// Use [`Self::shutdown`] to request shutdown and await thread completion.
#[non_exhaustive]
#[derive(Debug)]
pub struct ManagedRuntime {
    handle: ManagedRuntimeHandle,
    stop: Option<watch::Sender<()>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl ManagedRuntime {
    /// Returns a builder with automatic topology discovery and CPU pinning enabled.
    pub fn builder() -> ManagedRuntimeBuilder {
        ManagedRuntimeBuilder::default()
    }

    /// Returns cloneable access to topology and execution partitions.
    pub fn handle(&self) -> ManagedRuntimeHandle {
        self.handle.clone()
    }

    /// Requests shutdown and waits for every managed thread to exit.
    ///
    /// Consumes the runtime owner. Shutdown starts when this future is polled.
    /// Outstanding async tasks follow Tokio cancellation behavior; this does not
    /// drain them. Running blocking tasks finish before their managed thread
    /// exits. Non-yielding async tasks or blocking tasks that never finish can
    /// prevent completion.
    ///
    /// Joins run on a separate thread, so waiting does not block the caller's
    /// executor and requires no ambient Tokio runtime. Success means all managed
    /// threads have been joined, including their Tokio runtime destruction.
    ///
    /// # Cancellation
    ///
    /// Dropping this future requests shutdown if it has not started yet. Once
    /// joining has started, it continues even if this future is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if started on this runtime's managed or blocking-pool
    /// threads: they cannot wait for their own exit. The consumed owner still
    /// requests shutdown and detaches its threads in that case.
    ///
    /// Also reports failure to create the joining thread, a lost completion
    /// result, or a managed-thread panic. All threads are joined before reporting
    /// the first panic in partition order. If the joining thread cannot start,
    /// shutdown is requested and the managed threads detach.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        let id = self.handle.id();
        if identity::owns_current_thread(id) {
            return Err(ShutdownError::from_runtime(id));
        }
        drop(self.stop.take());
        let threads = std::mem::take(&mut self.threads);
        shutdown::join(id, threads).await
    }
}

impl Drop for ManagedRuntime {
    fn drop(&mut self) {
        drop(self.stop.take());
        // Dropping the join handles detaches the threads. Each thread owns its
        // Tokio runtime and destroys it after observing the shutdown request.
        self.threads.clear();
    }
}

/// Constructs a managed runtime with topology and thread configuration.
///
/// Defaults to automatic discovery, binding identified processor targets, and
/// thread names prefixed with `smithy`. Targets without processor identities remain
/// unbound. Binding failures fail construction.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct ManagedRuntimeBuilder {
    topology: Option<Topology>,
    thread_name_prefix: Option<String>,
    pin_threads: Option<bool>,
    #[cfg(feature = "dial9")]
    dial9_handle: Option<dial9::Dial9Handle>,
}

impl ManagedRuntimeBuilder {
    /// Uses these resources and selected placements instead of automatic discovery.
    pub fn topology(mut self, topology: Topology) -> Self {
        self.set_topology(Some(topology));
        self
    }

    /// Sets topology, or restores automatic discovery with `None`.
    pub fn set_topology(&mut self, topology: Option<Topology>) -> &mut Self {
        self.topology = topology;
        self
    }

    /// Sets the prefix for managed thread names.
    ///
    /// Names append the runtime's numeric identity and the partition index.
    /// A prefix containing a NUL byte fails construction.
    pub fn thread_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.set_thread_name_prefix(Some(prefix.into()));
        self
    }

    /// Sets the thread name prefix, or restores `smithy` with `None`.
    pub fn set_thread_name_prefix(&mut self, prefix: Option<String>) -> &mut Self {
        self.thread_name_prefix = prefix;
        self
    }

    /// Enables or disables binding identified CPU targets during startup.
    ///
    /// Enabled by default. Disabling binding retains placement metadata while
    /// allowing the operating system to schedule threads. Unidentified targets
    /// remain unbound even when enabled.
    pub fn pin_threads(mut self, enabled: bool) -> Self {
        self.set_pin_threads(enabled);
        self
    }

    /// Sets whether identified targets are bound during startup.
    pub fn set_pin_threads(&mut self, enabled: bool) -> &mut Self {
        self.pin_threads = Some(enabled);
        self
    }

    /// Attaches each partition's Tokio runtime to this caller-owned recorder.
    ///
    /// Traces use the managed thread names. Partition submissions record polls
    /// and wakes through Dial9's spawning integration. Tasks spawned directly
    /// through Tokio do not receive that wrapper.
    ///
    /// Keep the recorder alive while recording. Await [`ManagedRuntime::shutdown`]
    /// before shutting down the recorder to flush managed-thread events.
    /// A disabled handle builds ordinary, untraced runtimes.
    ///
    /// ```rust
    /// use aws_smithy_execution::{topology::Topology, ManagedRuntime};
    /// use dial9::{recorder, MemoryBuffer};
    /// use std::time::Duration;
    ///
    /// let recorder = recorder(MemoryBuffer::new(1 << 20)?).build();
    /// let runtime = ManagedRuntime::builder()
    ///     .topology(Topology::detect()?.with_max_placements(2))
    ///     .dial9_handle(recorder.handle().clone())
    ///     .build()?;
    /// let task = runtime.handle().partitions()[0].spawn(async { 42 });
    /// let caller = tokio::runtime::Builder::new_current_thread().build()?;
    /// assert_eq!(caller.block_on(task)?, 42);
    /// caller.block_on(runtime.shutdown())?;
    /// recorder.graceful_shutdown(Duration::from_secs(5));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "dial9")]
    pub fn dial9_handle(mut self, handle: dial9::Dial9Handle) -> Self {
        self.set_dial9_handle(Some(handle));
        self
    }

    /// Sets the recorder handle, or disables attachment with `None`.
    ///
    /// See [`Self::dial9_handle`] for tracing and recorder ownership.
    #[cfg(feature = "dial9")]
    pub fn set_dial9_handle(&mut self, handle: Option<dial9::Dial9Handle>) -> &mut Self {
        self.dial9_handle = handle;
        self
    }

    /// Starts all threads and returns their non-cloneable owner.
    ///
    /// Succeeds only after every thread has initialized its binding and Tokio
    /// runtime. Thread startup proceeds concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid naming, topology discovery failure, runtime
    /// identity exhaustion, or any thread initialization failure. Started
    /// threads are stopped and joined before returning an error.
    pub fn build(self) -> Result<ManagedRuntime, BuildError> {
        self.build_with(managed_thread::spawn)
    }

    fn build_with(
        self,
        mut start: impl FnMut(
            managed_thread::Config,
            watch::Receiver<()>,
        ) -> io::Result<managed_thread::Pending>,
    ) -> Result<ManagedRuntime, BuildError> {
        let prefix = self
            .thread_name_prefix
            .unwrap_or_else(|| "smithy".to_owned());
        if prefix.contains('\0') {
            return Err(BuildError::invalid_thread_name());
        }
        let topology = match self.topology {
            Some(topology) => topology,
            None => Topology::detect().map_err(BuildError::discovery)?,
        };
        let id = RuntimeId::allocate().ok_or_else(BuildError::identity_exhausted)?;
        let (stop, _) = watch::channel(());
        let mut startup = Startup {
            stop: Some(stop),
            threads: Vec::new(),
        };
        let mut pending = Vec::new();
        for (index, &placement) in topology.placements().iter().enumerate() {
            let partition_id = RuntimePartitionId::new(id, index);
            let config = managed_thread::Config {
                id: partition_id,
                name: format!("{prefix}-{}-{index}", id.0),
                topology: topology.clone(),
                placement,
                pin_threads: self.pin_threads.unwrap_or(true),
                #[cfg(feature = "dial9")]
                dial9_handle: self.dial9_handle.clone(),
            };
            let stop = startup
                .stop
                .as_ref()
                .expect("startup owns stop sender")
                .subscribe();
            let thread =
                start(config, stop).map_err(|source| BuildError::thread(partition_id, source))?;
            startup.threads.push(thread.thread);
            pending.push((partition_id, placement, thread.ready));
        }
        let mut partitions = Vec::new();
        for (partition_id, placement, ready) in pending {
            let ready = ready
                .recv()
                .map_err(|source| BuildError::ready(partition_id, source))??;
            partitions.push(RuntimePartition {
                id: partition_id,
                placement,
                bound_processor: ready.bound_processor,
                tokio: ready.handle,
                #[cfg(feature = "dial9")]
                traced: ready.traced,
            });
        }
        let handle = ManagedRuntimeHandle {
            state: Arc::new(RuntimeState {
                id,
                topology,
                partitions: partitions.into(),
            }),
        };
        Ok(ManagedRuntime {
            handle,
            stop: startup.stop.take(),
            threads: std::mem::take(&mut startup.threads),
        })
    }
}

struct Startup {
    stop: Option<watch::Sender<()>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Drop for Startup {
    fn drop(&mut self) {
        drop(self.stop.take());
        // No execution capabilities have escaped an incomplete construction.
        // These new threads are distinct from the thread running the builder.
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BuildErrorKind;
    use std::{error::Error, sync::mpsc};

    fn topology() -> Topology {
        Topology::detect().unwrap().with_max_placements(2)
    }

    #[test]
    fn startup_failure_stops_every_started_runtime_and_preserves_source() {
        let topology = topology();
        let last = topology.placements().len() - 1;
        let mut handles = Vec::new();
        let error = ManagedRuntime::builder()
            .topology(topology)
            .pin_threads(false)
            .build_with(|config, stop| {
                let id = config.id;
                let pending = managed_thread::spawn(config, stop)?;
                let initialized = pending.ready.recv().unwrap().unwrap();
                handles.push(initialized.handle.clone());
                let (tx, ready) = mpsc::channel();
                if id.index() == last {
                    tx.send(Err(BuildError::tokio(
                        id,
                        io::Error::other("injected initialization failure"),
                    )))
                    .unwrap();
                } else {
                    tx.send(Ok(initialized)).unwrap();
                }
                Ok(managed_thread::Pending {
                    thread: pending.thread,
                    ready,
                })
            })
            .unwrap_err();
        assert_eq!(error.kind(), BuildErrorKind::RuntimeInitialization);
        assert!(error.to_string().contains("partition-"));
        assert_eq!(
            error.source().unwrap().to_string(),
            "injected initialization failure"
        );
        assert_eq!(handles.len(), last + 1);
        let caller = ::tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for handle in handles {
            assert!(
                caller
                    .block_on(handle.spawn(async {}))
                    .unwrap_err()
                    .is_cancelled()
            );
        }
    }

    #[test]
    fn thread_creation_failure_preserves_os_error() {
        let error = ManagedRuntime::builder()
            .topology(topology())
            .build_with(|_, _| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "thread creation denied",
                ))
            })
            .unwrap_err();
        assert_eq!(error.kind(), BuildErrorKind::Thread);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied,
        );
    }

    #[test]
    fn invalid_thread_name_is_a_construction_error() {
        let error = ManagedRuntime::builder()
            .topology(topology())
            .thread_name_prefix("invalid\0name")
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), BuildErrorKind::InvalidThreadName);
        assert!(error.to_string().contains("NUL byte"));
    }
}
