/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::dns::*;
use aws_smithy_runtime_api::client::dns::ResolveDns;
use priority_queue::PriorityQueue;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

struct AddressEntry {
    id: u64,

    ip_address: IpAddr,

    use_count: u32, // maybe not needed

    ttl_expiration: Instant, // maybe not needed

    unhealthy_expiration: Instant, // maybe not needed
}

struct HostEntry {
    name: String,

    pending_requests: VecDeque<CachingResolveRequestInternal>,

    addresses: HashMap<u64, AddressEntry>,

    addresses_to_id: HashMap<IpAddr, u64>,

    healthy_addresses_by_use_count: PriorityQueue<u64, u32>,

    unhealthy_addresses_by_expiration: PriorityQueue<u64, Instant>,

    addresses_by_ttl: PriorityQueue<u64, Instant>,
}

impl HostEntry {
    fn new(name: &str) -> HostEntry {
        HostEntry {
            name: name.to_string(),
            pending_requests: VecDeque::new(),
            addresses: HashMap::new(),
            addresses_to_id: HashMap::new(),
            healthy_addresses_by_use_count: PriorityQueue::new(),
            unhealthy_addresses_by_expiration: PriorityQueue::new(),
            addresses_by_ttl: PriorityQueue::new(),
        }
    }
}

#[derive(Default)]
struct SraCachingResolverState {
    hosts: HashMap<String, HostEntry>,
}

fn find_or_create_host_entry<'a>(
    state: &'a mut SraCachingResolverState,
    name: &str,
) -> &'a mut HostEntry {
    if !state.hosts.contains_key(name) {
        state.hosts.insert(name.to_string(), HostEntry::new(name));
    }

    state.hosts.get_mut(name).unwrap()
}

fn handle_resolve_request(
    config: &CachingResolverProcessConfig,
    state: &mut SraCachingResolverState,
    request: CachingResolveRequestInternal,
) {
    let name = request.request.name.to_string();
    let host_entry = find_or_create_host_entry(state, &name);

    let name = request.request.name.clone();
    host_entry.pending_requests.push_back(request);

    let handle = tokio::runtime::Handle::current();
    let internal_resolver = config.internal_resolver.clone();
    let operation_channel = config.sender.clone();
    handle.spawn(async move {
        let result = internal_resolver.resolve_dns(&name).await;
        let caching_result =
            CachingResolverOperation::ResolveResult(CachingResolveResult { name, result });
        let _ = operation_channel.send(caching_result);
    });
}

fn handle_resolve_result(
    _: &CachingResolverProcessConfig,
    state: &mut SraCachingResolverState,
    result: CachingResolveResult,
) {
    if let Some(entry) = state.hosts.get_mut(&result.name) {
        if let Some(pending_request) = entry.pending_requests.pop_front() {
            (pending_request.completion_callback)(result.result);
        }
    }
}

fn handle_host_event(
    _: &CachingResolverProcessConfig,
    _: &mut SraCachingResolverState,
    _: CachingHostEvent,
) {
}

fn handle_operation_sra(
    config: &CachingResolverProcessConfig,
    state: &mut SraCachingResolverState,
    operation: CachingResolverOperation,
) {
    match operation {
        CachingResolverOperation::ResolveRequest(request) => {
            handle_resolve_request(config, state, request);
        }
        CachingResolverOperation::ResolveResult(result) => {
            handle_resolve_result(config, state, result);
        }
        CachingResolverOperation::HostEvent(event) => handle_host_event(config, state, event),
    }
}

/// Creates a new caching resolver that follows the SRA specification for domain name resolution
pub fn new_sra_caching_resolver(config: CachingResolverConfig) -> CachingResolverHandle {
    CachingResolverHandle {
        resolver: Arc::new(CachingResolverImpl::new_default(
            config,
            handle_operation_sra,
        )),
    }
}
