/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;
use crate::dns::{CachingResolverConfig, CachingResolverHandle, CachingResolverImpl, CachingResolverOperation, CachingResolverProcessConfig};
use aws_smithy_runtime_api::client::dns::{ResolveDns};

#[derive(Default)]
struct SraCachingResolverState {
    // eventually, name -> HostEntry which will hold healthy IPs, unhealthy IPs, pending requests,
    // IP usage, etc...
}

fn handle_operation_sra(config: &CachingResolverProcessConfig, state: &mut SraCachingResolverState, operation: CachingResolverOperation) {
    match operation {
        CachingResolverOperation::ResolveRequest(request) => {
            // someone's asking for name resolution
            //
            // for proof-of-concept, just submit the resolve request as a task on the runtime.
            // this means we bypass submitting the resolution result and instead just
            // complete the future directly in the task, which isn't what we'll do in the
            // real logic
            let handle = tokio::runtime::Handle::current();
            let internal_resolver = config.internal_resolver.clone();
            let name = request.request.name;
            let tx = request.result_target;
            handle.spawn(
                async move {
                    let resolve_result = internal_resolver.resolve_dns(&name).await;
                    let _ = tx.send(resolve_result);
                }
            );
        }
        CachingResolverOperation::ResolveResult(_) => {
            // a resolve attempt completed
        }
        CachingResolverOperation::HostEvent(_) => {
            // apply a host-related event to our internals
        }
    }
}

/// Creates a new caching resolver that follows the SRA specification for domain name resolution
pub fn new_sra_caching_resolver(config: CachingResolverConfig) -> CachingResolverHandle {
    CachingResolverHandle {
        resolver: Arc::new(CachingResolverImpl::new(config, handle_operation_sra))
    }
}
