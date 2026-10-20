/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Execution configuration with managed ownership or caller-provided Tokio execution.

/// Selects execution for transfer-manager work.
///
/// Defaults to owned managed execution. Configuring HTTP on the SDK or S3
/// configuration does not change this selection.
///
/// ```
/// use aws_sdk_s3_transfer_manager::{Managed, RuntimeConfig};
///
/// let managed = RuntimeConfig::managed(Managed::default());
/// let caller_runtime = RuntimeConfig::multi_thread_tokio();
/// ```
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
/// the managed threads.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Managed {
    _private: (),
}

impl Managed {
    pub(crate) fn build(
        &self,
        #[cfg(feature = "dial9")] recorder: Option<dial9::Dial9Handle>,
    ) -> Result<aws_ort_execution::ManagedRuntime, aws_ort_execution::BuildError> {
        let builder = aws_ort_execution::ManagedRuntime::builder()
            .thread_name_prefix("s3-tm")
            .pin_threads(false);
        #[cfg(feature = "dial9")]
        let builder = match recorder {
            Some(recorder) => builder.dial9_handle(recorder),
            None => builder,
        };
        builder.build()
    }
}
