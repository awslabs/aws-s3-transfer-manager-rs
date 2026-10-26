/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::LogicalProcessorId;
use std::{any::Any, fmt};

/// Failure to discover a usable topology.
#[non_exhaustive]
#[derive(Debug)]
pub struct DetectError {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    NoProcessors,
    Invalid(&'static str),
    Parallelism(std::io::Error),
    DiscoveryPanic(String),
}

impl DetectError {
    pub(crate) fn no_processors() -> Self {
        Self {
            kind: Kind::NoProcessors,
        }
    }
    pub(crate) fn invalid(message: &'static str) -> Self {
        Self {
            kind: Kind::Invalid(message),
        }
    }
    pub(crate) fn parallelism(source: std::io::Error) -> Self {
        Self {
            kind: Kind::Parallelism(source),
        }
    }
    pub(crate) fn discovery_panic(payload: Box<dyn Any + Send>) -> Self {
        Self {
            kind: Kind::DiscoveryPanic(panic_message(payload)),
        }
    }
}

/// Classifies a failure to bind a thread to a logical processor.
///
/// Native failures and verification failures may leave thread affinity changed.
/// The classification does not indicate whether affinity was modified.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindErrorKind {
    /// The processor does not belong to the topology's resource inventory.
    UnknownProcessor,
    /// The processor is not available to the calling thread at binding time.
    UnavailableProcessor,
    /// Native CPU binding is unsupported on this platform.
    Unsupported,
    /// A native affinity query or binding operation failed.
    NativeFailure,
    /// Verification did not establish the requested processor restriction.
    NotBound,
}

/// Failure to bind the calling thread to an identified logical processor.
///
/// An error after the native binding operation may leave the thread's affinity
/// changed. The previous affinity is not restored.
#[non_exhaustive]
#[derive(Debug)]
pub struct BindError {
    processor: LogicalProcessorId,
    kind: BindErrorKind,
    message: Option<String>,
}

impl BindError {
    /// Returns the classification of this binding failure.
    pub fn kind(&self) -> BindErrorKind {
        self.kind
    }

    /// Returns the target processor for the failed binding operation.
    pub fn processor(&self) -> LogicalProcessorId {
        self.processor
    }

    pub(crate) fn unknown_processor(processor: LogicalProcessorId) -> Self {
        Self {
            processor,
            kind: BindErrorKind::UnknownProcessor,
            message: None,
        }
    }

    pub(crate) fn unavailable_processor(processor: LogicalProcessorId) -> Self {
        Self {
            processor,
            kind: BindErrorKind::UnavailableProcessor,
            message: None,
        }
    }

    pub(crate) fn unsupported(processor: LogicalProcessorId) -> Self {
        Self {
            processor,
            kind: BindErrorKind::Unsupported,
            message: None,
        }
    }

    pub(crate) fn binding_panic(
        processor: LogicalProcessorId,
        payload: Box<dyn Any + Send>,
    ) -> Self {
        Self {
            processor,
            kind: BindErrorKind::NativeFailure,
            message: Some(panic_message(payload)),
        }
    }

    pub(crate) fn not_bound(processor: LogicalProcessorId) -> Self {
        Self {
            processor,
            kind: BindErrorKind::NotBound,
            message: None,
        }
    }
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot bind current thread to {}: ", self.processor)?;
        match self.kind {
            BindErrorKind::UnknownProcessor => f.write_str("processor is not in this topology"),
            BindErrorKind::UnavailableProcessor => {
                f.write_str("processor is not currently available to this thread")
            }
            BindErrorKind::Unsupported => {
                f.write_str("CPU binding is unsupported on this platform")
            }
            BindErrorKind::NativeFailure => f.write_str("native CPU binding failed"),
            BindErrorKind::NotBound => {
                f.write_str("CPU binding did not establish the requested processor restriction")
            }
        }?;
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BindError {}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "unrecognized panic payload".to_owned()
    }
}

impl fmt::Display for DetectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            Kind::NoProcessors => f.write_str("no processors usable in the discovery context"),
            Kind::Invalid(message) => write!(f, "invalid discovered topology: {message}"),
            Kind::Parallelism(_) => f.write_str("unable to estimate available parallelism"),
            Kind::DiscoveryPanic(message) => write!(f, "processor discovery failed: {message}"),
        }
    }
}

impl std::error::Error for DetectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            Kind::Parallelism(source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    // ref: awslabs/aws-s3-transfer-manager-rs#216
    #[test]
    fn binding_errors_expose_classification_and_requested_processor() {
        let target = LogicalProcessorId(42);
        let errors = [
            (
                BindError::unknown_processor(target),
                BindErrorKind::UnknownProcessor,
            ),
            (
                BindError::unavailable_processor(target),
                BindErrorKind::UnavailableProcessor,
            ),
            (BindError::unsupported(target), BindErrorKind::Unsupported),
            (
                BindError::binding_panic(target, Box::new("native affinity query failed")),
                BindErrorKind::NativeFailure,
            ),
            (BindError::not_bound(target), BindErrorKind::NotBound),
        ];
        for (error, expected) in errors {
            assert_eq!(error.kind(), expected);
            assert_eq!(error.processor(), target);
        }
    }

    #[test]
    fn parallelism_error_preserves_its_source() {
        let error = DetectError::parallelism(std::io::Error::other("OS query failed"));
        assert_eq!(
            error.to_string(),
            "unable to estimate available parallelism",
        );
        assert_eq!(error.source().unwrap().to_string(), "OS query failed");
    }
}
