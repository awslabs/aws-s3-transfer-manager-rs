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
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Managed {
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

    pub(crate) fn build(
        &mut self,
        #[cfg(feature = "dial9")] recorder: Option<dial9::Dial9Handle>,
    ) -> Result<aws_smithy_execution::ManagedRuntime, aws_smithy_runtime_api::box_error::BoxError>
    {
        match &mut self.source {
            Source::Automatic => {
                let builder = aws_smithy_execution::ManagedRuntime::builder()
                    .thread_name_prefix("s3-tm")
                    .pin_threads(false);
                #[cfg(feature = "dial9")]
                let builder = match recorder {
                    Some(recorder) => builder.dial9_handle(recorder),
                    None => builder,
                };
                builder.build().map_err(Into::into)
            }
            #[cfg(feature = "__unstable_ext")]
            Source::Adopted(owner) => {
                #[cfg(feature = "dial9")]
                if recorder.is_some() {
                    return Err("configure Dial9 on the runtime builder before adoption; \
                         a transfer-manager dial9_handle cannot instrument an adopted runtime"
                        .into());
                }
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
        let runtime = managed
            .build(
                #[cfg(feature = "dial9")]
                None,
            )
            .unwrap();
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
        let repeated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            managed.build(
                #[cfg(feature = "dial9")]
                None,
            )
        }));
        assert!(
            repeated.is_err(),
            "a consumed owner must not become automatic execution"
        );
        caller.block_on(runtime.shutdown()).unwrap();
    }

    #[cfg(feature = "dial9")]
    #[test]
    fn separate_recorder_is_rejected_without_consuming_the_owner() {
        let runtime = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .pin_threads(false)
            .dial9_handle(dial9::Dial9Handle::disabled())
            .build()
            .unwrap();
        let id = runtime.handle().id();
        let mut managed = Managed::from_runtime(runtime);
        let error = managed
            .build(Some(dial9::Dial9Handle::disabled()))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("configure Dial9 on the runtime builder before adoption"));
        let runtime = managed.build(None).unwrap();
        assert_eq!(runtime.handle().id(), id);
        drop(runtime);
    }
}
