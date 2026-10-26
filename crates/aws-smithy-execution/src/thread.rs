/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    BuildError, RuntimePartitionId,
    identity::{self, CurrentPartition},
};
use aws_smithy_topology::{LogicalProcessorId, Placement, Topology};
use std::{io, panic, sync::mpsc, thread};
use tokio::sync::watch;

pub(crate) struct Config {
    pub(crate) id: RuntimePartitionId,
    pub(crate) name: String,
    pub(crate) topology: Topology,
    pub(crate) placement: Placement,
    pub(crate) pin_threads: bool,
    #[cfg(feature = "dial9")]
    pub(crate) dial9_handle: Option<dial9::Dial9Handle>,
}

pub(crate) struct Pending {
    pub(crate) thread: thread::JoinHandle<()>,
    pub(crate) ready: mpsc::Receiver<Result<Ready, BuildError>>,
}

pub(crate) struct Ready {
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) bound_processor: Option<LogicalProcessorId>,
    #[cfg(feature = "dial9")]
    pub(crate) traced: bool,
}

pub(crate) fn spawn(config: Config, mut stop: watch::Receiver<()>) -> io::Result<Pending> {
    let (ready_tx, ready) = mpsc::channel();
    let thread = thread::Builder::new()
        .name(config.name.clone())
        .spawn(move || {
            identity::mark_runtime_thread(config.id.runtime_id());
            let _current = CurrentPartition::enter(config.id);
            let initialized = panic::catch_unwind(|| initialize(&config))
                .map_err(|payload| BuildError::initialization_panic(config.id, payload))
                .and_then(|result| result);
            match initialized {
                Ok((runtime, bound_processor)) => {
                    #[cfg(feature = "dial9")]
                    let traced = config
                        .dial9_handle
                        .as_ref()
                        .is_some_and(dial9::Dial9Handle::is_connected);
                    let initialized = Ready {
                        handle: runtime.handle().clone(),
                        bound_processor,
                        #[cfg(feature = "dial9")]
                        traced,
                    };
                    if ready_tx.send(Ok(initialized)).is_ok() {
                        // Only the owner holds a sender. Its destruction wakes every
                        // thread even when execution handles are still retained.
                        let stopped = async move {
                            let _ = stop.changed().await;
                        };
                        #[cfg(feature = "dial9")]
                        if traced {
                            dial9::block_on(&runtime, stopped);
                        } else {
                            runtime.block_on(stopped);
                        }
                        #[cfg(not(feature = "dial9"))]
                        runtime.block_on(stopped);
                    }
                    // Keep ordinary Tokio destruction on this thread, outside its
                    // async context. Running blocking tasks can delay its exit.
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            }
        })?;
    Ok(Pending { thread, ready })
}

fn initialize(
    config: &Config,
) -> Result<(tokio::runtime::Runtime, Option<LogicalProcessorId>), BuildError> {
    let bound_processor = if config.pin_threads {
        if let Some(processor) = config.placement.logical_processor() {
            config
                .topology
                .bind_current_thread(processor)
                .map_err(|source| BuildError::binding(config.id, source))?;
            Some(processor)
        } else {
            None
        }
    } else {
        None
    };
    let id = config.id.runtime_id();
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder
        .enable_all()
        .max_blocking_threads(1)
        .on_thread_start(move || identity::mark_runtime_thread(id));
    #[cfg(feature = "dial9")]
    let runtime = match &config.dial9_handle {
        Some(handle) => {
            use dial9::{Dial9HandleTokioExt, TokioAttachOptions, TokioHooks};

            // Active attachment replaces Tokio's callbacks. Compose the ownership
            // marker there too; disabled attachment keeps the builder's callback.
            let mut hooks = TokioHooks::default();
            hooks.on_thread_start(move || identity::mark_runtime_thread(id));
            let options = TokioAttachOptions::builder()
                .runtime_name(config.name.clone())
                .tokio_hooks(hooks)
                .build();
            handle.attach_tokio_runtime(builder, options)
        }
        None => builder.build(),
    };
    #[cfg(not(feature = "dial9"))]
    let runtime = builder.build();
    let runtime = runtime.map_err(|source| BuildError::tokio(config.id, source))?;
    Ok((runtime, bound_processor))
}
