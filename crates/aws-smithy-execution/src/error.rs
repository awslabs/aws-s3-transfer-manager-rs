/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{RuntimeId, RuntimePartitionId};
use aws_smithy_topology::{BindError, DetectError};
use std::{any::Any, error::Error, fmt, io, sync::mpsc::RecvError};
use tokio::sync::oneshot;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Classifies a failure to construct a managed runtime.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildErrorKind {
    /// Topology discovery failed.
    Discovery,
    /// No unused runtime identity remains.
    IdentityExhausted,
    /// The thread name prefix contains a NUL byte.
    InvalidThreadName,
    /// A managed OS thread could not be created.
    Thread,
    /// A managed thread could not establish its requested CPU binding.
    Binding,
    /// A partition's async runtime could not be initialized.
    RuntimeInitialization,
    /// A managed thread's initialization panicked.
    InitializationPanic,
    /// A managed thread did not report its initialization result.
    Ready,
}

/// Failure to discover topology or start all managed threads.
///
/// Threads started before a construction failure are stopped and joined before
/// construction returns. Native initialization panics are contained in unwind
/// builds; the panic hook still runs, and aborting panics cannot be recovered.
#[non_exhaustive]
#[derive(Debug)]
pub struct BuildError {
    kind: BuildErrorKind,
    partition: Option<RuntimePartitionId>,
    source: Option<BoxError>,
    message: Option<String>,
}

impl BuildError {
    /// Returns the classification of this construction failure.
    pub fn kind(&self) -> BuildErrorKind {
        self.kind
    }

    pub(crate) fn discovery(source: DetectError) -> Self {
        Self {
            kind: BuildErrorKind::Discovery,
            partition: None,
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn identity_exhausted() -> Self {
        Self {
            kind: BuildErrorKind::IdentityExhausted,
            partition: None,
            source: None,
            message: None,
        }
    }

    pub(crate) fn invalid_thread_name() -> Self {
        Self {
            kind: BuildErrorKind::InvalidThreadName,
            partition: None,
            source: None,
            message: None,
        }
    }

    pub(crate) fn thread(id: RuntimePartitionId, source: io::Error) -> Self {
        Self {
            kind: BuildErrorKind::Thread,
            partition: Some(id),
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn binding(id: RuntimePartitionId, source: BindError) -> Self {
        Self {
            kind: BuildErrorKind::Binding,
            partition: Some(id),
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn tokio(id: RuntimePartitionId, source: io::Error) -> Self {
        Self {
            kind: BuildErrorKind::RuntimeInitialization,
            partition: Some(id),
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn initialization_panic(
        id: RuntimePartitionId,
        payload: Box<dyn Any + Send>,
    ) -> Self {
        Self {
            kind: BuildErrorKind::InitializationPanic,
            partition: Some(id),
            source: None,
            message: Some(panic_message(payload.as_ref())),
        }
    }

    pub(crate) fn ready(id: RuntimePartitionId, source: RecvError) -> Self {
        Self {
            kind: BuildErrorKind::Ready,
            partition: Some(id),
            source: Some(Box::new(source)),
            message: None,
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            BuildErrorKind::Discovery => f.write_str("unable to discover runtime topology"),
            BuildErrorKind::IdentityExhausted => f.write_str("runtime identities exhausted"),
            BuildErrorKind::InvalidThreadName => {
                f.write_str("thread name prefix contains a NUL byte")
            }
            BuildErrorKind::Thread => write!(
                f,
                "unable to create thread {}",
                self.partition.expect("thread failure has a partition"),
            ),
            BuildErrorKind::Binding => write!(
                f,
                "unable to bind thread {}",
                self.partition.expect("binding failure has a partition"),
            ),
            BuildErrorKind::RuntimeInitialization => write!(
                f,
                "unable to create Tokio runtime for {}",
                self.partition
                    .expect("Tokio initialization failure has a partition"),
            ),
            BuildErrorKind::InitializationPanic => write!(
                f,
                "thread {} initialization panicked",
                self.partition
                    .expect("initialization panic has a partition"),
            ),
            BuildErrorKind::Ready => write!(
                f,
                "thread {} did not report readiness",
                self.partition.expect("readiness failure has a partition"),
            ),
        }?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// Classifies a failure to wait for a managed runtime's shutdown.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownErrorKind {
    /// Shutdown was awaited from a thread owned by this runtime.
    FromRuntime,
    /// The thread that joins managed threads could not be created.
    Joiner,
    /// The shutdown completion result was lost.
    Completion,
    /// A managed thread panicked.
    ThreadPanic,
}

/// Failure to wait for a managed runtime's shutdown.
///
/// Shutdown is requested even when waiting fails. A managed-thread panic is
/// reported after all managed threads are joined. Other errors can prevent
/// confirmation that every thread has exited.
#[non_exhaustive]
#[derive(Debug)]
pub struct ShutdownError {
    kind: ShutdownErrorKind,
    runtime: RuntimeId,
    partition: Option<RuntimePartitionId>,
    source: Option<BoxError>,
    message: Option<String>,
}

impl ShutdownError {
    /// Returns the classification of this shutdown failure.
    pub fn kind(&self) -> ShutdownErrorKind {
        self.kind
    }

    pub(crate) fn from_runtime(id: RuntimeId) -> Self {
        Self {
            kind: ShutdownErrorKind::FromRuntime,
            runtime: id,
            partition: None,
            source: None,
            message: None,
        }
    }

    pub(crate) fn joiner(id: RuntimeId, source: io::Error) -> Self {
        Self {
            kind: ShutdownErrorKind::Joiner,
            runtime: id,
            partition: None,
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn completion(id: RuntimeId, source: oneshot::error::RecvError) -> Self {
        Self {
            kind: ShutdownErrorKind::Completion,
            runtime: id,
            partition: None,
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub(crate) fn thread_panic(id: RuntimePartitionId, payload: Box<dyn Any + Send>) -> Self {
        Self {
            kind: ShutdownErrorKind::ThreadPanic,
            runtime: id.runtime_id(),
            partition: Some(id),
            source: None,
            message: Some(panic_message(payload.as_ref())),
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ShutdownErrorKind::FromRuntime => write!(
                f,
                "cannot wait for {} shutdown from a thread it owns",
                self.runtime,
            ),
            ShutdownErrorKind::Joiner => write!(
                f,
                "unable to start shutdown joining thread for {}",
                self.runtime,
            ),
            ShutdownErrorKind::Completion => {
                write!(f, "shutdown completion for {} was lost", self.runtime,)
            }
            ShutdownErrorKind::ThreadPanic => write!(
                f,
                "managed thread for {} panicked",
                self.partition
                    .expect("managed-thread panic has a partition"),
            ),
        }?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "unrecognized panic payload".to_owned()
    }
}
