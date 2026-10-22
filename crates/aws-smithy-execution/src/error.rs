/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{RuntimeId, RuntimePartitionId};
use aws_smithy_topology::{BindError, DetectError};
use std::{any::Any, error::Error, fmt, io, sync::mpsc::RecvError};
use tokio::sync::oneshot;

/// Failure to discover topology or start all managed threads.
///
/// Threads started before a construction failure are stopped and joined before
/// construction returns. Native initialization panics are contained in unwind
/// builds; the panic hook still runs, and aborting panics cannot be recovered.
#[non_exhaustive]
#[derive(Debug)]
pub struct BuildError {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Discovery(DetectError),
    IdentityExhausted,
    InvalidThreadName,
    Thread(RuntimePartitionId, io::Error),
    Binding(RuntimePartitionId, BindError),
    Tokio(RuntimePartitionId, io::Error),
    InitializationPanic(RuntimePartitionId, String),
    Ready(RuntimePartitionId, RecvError),
}

impl BuildError {
    pub(crate) fn discovery(source: DetectError) -> Self {
        Self {
            kind: Kind::Discovery(source),
        }
    }

    pub(crate) fn identity_exhausted() -> Self {
        Self {
            kind: Kind::IdentityExhausted,
        }
    }

    pub(crate) fn invalid_thread_name() -> Self {
        Self {
            kind: Kind::InvalidThreadName,
        }
    }

    pub(crate) fn thread(id: RuntimePartitionId, source: io::Error) -> Self {
        Self {
            kind: Kind::Thread(id, source),
        }
    }

    pub(crate) fn binding(id: RuntimePartitionId, source: BindError) -> Self {
        Self {
            kind: Kind::Binding(id, source),
        }
    }

    pub(crate) fn tokio(id: RuntimePartitionId, source: io::Error) -> Self {
        Self {
            kind: Kind::Tokio(id, source),
        }
    }

    pub(crate) fn initialization_panic(
        id: RuntimePartitionId,
        payload: Box<dyn Any + Send>,
    ) -> Self {
        let message = panic_message(payload.as_ref());
        Self {
            kind: Kind::InitializationPanic(id, message),
        }
    }

    pub(crate) fn ready(id: RuntimePartitionId, source: RecvError) -> Self {
        Self {
            kind: Kind::Ready(id, source),
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            Kind::Discovery(source) => write!(f, "unable to discover runtime topology: {source}"),
            Kind::IdentityExhausted => f.write_str("runtime identities exhausted"),
            Kind::InvalidThreadName => f.write_str("thread name prefix contains a NUL byte"),
            Kind::Thread(id, source) => write!(f, "unable to create thread {id}: {source}"),
            Kind::Binding(id, source) => write!(f, "unable to bind thread {id}: {source}"),
            Kind::Tokio(id, source) => {
                write!(f, "unable to create Tokio runtime for {id}: {source}")
            }
            Kind::InitializationPanic(id, message) => {
                write!(f, "thread {id} initialization panicked: {message}")
            }
            Kind::Ready(id, source) => write!(f, "thread {id} did not report readiness: {source}"),
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            Kind::Discovery(source) => Some(source),
            Kind::Thread(_, source) | Kind::Tokio(_, source) => Some(source),
            Kind::Binding(_, source) => Some(source),
            Kind::Ready(_, source) => Some(source),
            _ => None,
        }
    }
}

/// Failure to wait for a managed runtime's shutdown.
///
/// Shutdown is requested even when waiting fails. A managed-thread panic is
/// reported after all managed threads are joined. Other errors can prevent
/// confirmation that every thread has exited.
#[non_exhaustive]
#[derive(Debug)]
pub struct ShutdownError {
    kind: ShutdownKind,
}

#[derive(Debug)]
enum ShutdownKind {
    FromRuntime(RuntimeId),
    Joiner(RuntimeId, io::Error),
    Completion(RuntimeId, oneshot::error::RecvError),
    ThreadPanic(RuntimePartitionId, String),
}

impl ShutdownError {
    pub(crate) fn from_runtime(id: RuntimeId) -> Self {
        Self {
            kind: ShutdownKind::FromRuntime(id),
        }
    }

    pub(crate) fn joiner(id: RuntimeId, source: io::Error) -> Self {
        Self {
            kind: ShutdownKind::Joiner(id, source),
        }
    }

    pub(crate) fn completion(id: RuntimeId, source: oneshot::error::RecvError) -> Self {
        Self {
            kind: ShutdownKind::Completion(id, source),
        }
    }

    pub(crate) fn thread_panic(id: RuntimePartitionId, payload: Box<dyn Any + Send>) -> Self {
        Self {
            kind: ShutdownKind::ThreadPanic(id, panic_message(payload.as_ref())),
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ShutdownKind::FromRuntime(id) => {
                write!(f, "cannot wait for {id} shutdown from a thread it owns")
            }
            ShutdownKind::Joiner(id, source) => write!(
                f,
                "unable to start shutdown joining thread for {id}: {source}"
            ),
            ShutdownKind::Completion(id, source) => {
                write!(f, "shutdown completion for {id} was lost: {source}")
            }
            ShutdownKind::ThreadPanic(id, message) => {
                write!(f, "managed thread for {id} panicked: {message}")
            }
        }
    }
}

impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ShutdownKind::Joiner(_, source) => Some(source),
            ShutdownKind::Completion(_, source) => Some(source),
            _ => None,
        }
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
