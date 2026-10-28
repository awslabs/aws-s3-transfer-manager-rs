/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Opaque observation of transfer-manager-owned runtime destruction.

use std::{error::Error, fmt};

/// Observes managed-runtime completion without retaining the client or owner.
///
/// Obtain this value from [`crate::Client::shutdown_completion`] before releasing
/// the client. Clones and repeated waits observe the same shutdown outcome.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShutdownCompletion {
    inner: aws_smithy_execution::ShutdownCompletion,
}

impl ShutdownCompletion {
    pub(crate) fn new(inner: aws_smithy_execution::ShutdownCompletion) -> Self {
        Self { inner }
    }

    /// Waits for all managed runtime threads to be joined after owner release.
    ///
    /// Waiting does not request shutdown or cancel transfers. Release client
    /// clones and transfer ownership first; active work and delayed scheduler
    /// reclamation can retain the owner. Success includes runtime destruction
    /// and managed-thread local cleanup, but not destruction of independently
    /// retained HTTP clients or SDK configuration.
    ///
    /// Running blocking work must finish; indefinitely blocked or non-yielding
    /// work can prevent completion. No ambient Tokio runtime is required.
    /// Dropping this future does not stop joining that has already started.
    ///
    /// # Errors
    ///
    /// Rejects waiting from this managed runtime's own threads. Also reports
    /// joiner startup failure, lost completion, and managed-thread panics.
    pub async fn wait(&self) -> Result<(), ShutdownError> {
        self.inner
            .wait()
            .await
            .map_err(ShutdownError::from_execution)
    }
}

/// Classifies a failure to observe managed-runtime shutdown.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownErrorKind {
    /// Waiting was attempted from a thread owned by the managed runtime.
    FromRuntime,
    /// The joining thread could not be created.
    Joiner,
    /// The joining thread did not provide a completion result.
    Completion,
    /// A managed thread panicked; all managed threads have been joined.
    ThreadPanic,
    /// An unclassified shutdown failure occurred.
    Other,
}

/// Failure to observe transfer-manager-owned runtime completion.
#[non_exhaustive]
#[derive(Debug)]
pub struct ShutdownError {
    kind: ShutdownErrorKind,
    source: crate::error::BoxError,
}

impl ShutdownError {
    /// Returns the classification of this shutdown failure.
    pub fn kind(&self) -> ShutdownErrorKind {
        self.kind
    }
}

impl ShutdownError {
    fn from_execution(source: aws_smithy_execution::ShutdownError) -> Self {
        use aws_smithy_execution::ShutdownErrorKind as Kind;
        let kind = match source.kind() {
            Kind::FromRuntime => ShutdownErrorKind::FromRuntime,
            Kind::Joiner => ShutdownErrorKind::Joiner,
            Kind::Completion => ShutdownErrorKind::Completion,
            Kind::ThreadPanic => ShutdownErrorKind::ThreadPanic,
            _ => ShutdownErrorKind::Other,
        };
        Self {
            kind,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "managed runtime shutdown failed: {}", self.source)
    }
}

impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
