/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::dns::*;
use aws_smithy_runtime_api::client::dns::ResolveDns;
use priority_queue::PriorityQueue;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Instant;

struct GenerationalAddressSet<T> where T : Hash + Eq + Clone {
    current_generation: u32,

    healthy_addresses: PriorityQueue<T, u32>,
    unhealthy_addresses_by_expiration: PriorityQueue<T, Instant>,
}

impl<T> GenerationalAddressSet<T> where T : Hash + Eq + Clone {
    fn new() -> Self {
        GenerationalAddressSet {
            current_generation: 1,
            healthy_addresses: PriorityQueue::new(),
            unhealthy_addresses_by_expiration: PriorityQueue::new(),
        }
    }

    fn contains(&self, value: &T) -> bool {
        self.healthy_addresses.contains(value) || self.unhealthy_addresses_by_expiration.contains(value)
    }

    fn empty(&self) -> bool {
        self.healthy_addresses.is_empty() && self.unhealthy_addresses_by_expiration.is_empty()
    }

    fn add_address(&mut self, value: T) {
        if !self.contains(&value) {
            self.healthy_addresses.push(value, 0);
        }
    }

    fn remove_address(&mut self, value: &T) {
        self.healthy_addresses.remove(value);
        self.unhealthy_addresses_by_expiration.remove(value);
    }

    fn acquire_address(&mut self) -> Option<T> {
        if self.empty() {
            return None;
        }

        // promote any addresses whose demotion has expired
        let now = Instant::now();
        while !self.unhealthy_addresses_by_expiration.is_empty() {
            if self.unhealthy_addresses_by_expiration.peek().unwrap().1 <= &now {
                let promoted_addr = self.unhealthy_addresses_by_expiration.pop().unwrap().0;
                self.healthy_addresses.push(promoted_addr, 0);
            } else {
                break;
            }
        }

        // force promote an address if no healthy address exists
        if self.healthy_addresses.is_empty() {
            let promoted_addr = self.unhealthy_addresses_by_expiration.pop().unwrap().0;
            self.healthy_addresses.push(promoted_addr, 0);
        }

        // guaranteed to have a healthy address at this point
        let (item, generation) = self.healthy_addresses.pop().unwrap();
        if generation == self.current_generation {
            self.current_generation += 1;
        }

        self.healthy_addresses.push(item.clone(), self.current_generation);

        Some(item)
    }

    fn demote_address(&mut self, address: T, duration: Duration) {
        self.healthy_addresses.remove(&address);

        let expiration = Instant::now() + duration;
        self.unhealthy_addresses_by_expiration.push(address, expiration);
    }
}

struct HostEntry {
    name: String,

    pending_requests: VecDeque<CachingResolveRequestInternal>,

    ipv4_addresses: GenerationalAddressSet<Ipv4Addr>,
    ipv6_addresses: GenerationalAddressSet<Ipv6Addr>,

    addresses_by_ttl: PriorityQueue<IpAddr, Instant>,

    last_resolution_timepoint: Option<Instant>,
    pending_resolution: bool,

    ttl: Duration,
}

impl HostEntry {
    fn new(name: &str, ttl: Duration) -> HostEntry {
        HostEntry {
            name: name.to_string(),
            pending_requests: VecDeque::new(),
            ipv4_addresses: GenerationalAddressSet::new(),
            ipv6_addresses: GenerationalAddressSet::new(),
            addresses_by_ttl: PriorityQueue::new(),
            last_resolution_timepoint: None,
            pending_resolution: false,
            ttl
        }
    }

    fn purge_address(&mut self, address: IpAddr) {
        self.addresses_by_ttl.remove(&address);

        match &address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses.remove_address(v4addr);
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses.remove_address(v6addr);
            }
        }
    }

    fn on_address_resolved(&mut self, address: IpAddr) {
        let ttl = Instant::now() + self.ttl;

        if self.addresses_by_ttl.contains(&address) {
            self.addresses_by_ttl.change_priority(&address, ttl);
            return;
        }

        self.addresses_by_ttl.push(address.clone(), ttl);

        match address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses.add_address(v4addr);
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses.add_address(v6addr);
            }
        }
    }

    fn demote_address(&mut self, address: IpAddr, duration: Duration) {
        match address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses.demote_address(v4addr, duration);
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses.demote_address(v6addr, duration);
            }
        }
    }

    fn purge_expired(&mut self) {
        let now = Instant::now();
        while !self.addresses_by_ttl.is_empty() {
            if self.addresses_by_ttl.peek().unwrap().1 <= &now {
                let purged = self.addresses_by_ttl.pop().unwrap().0;
                self.purge_address(purged);
            } else {
                break;
            }
        }
    }

    fn handle_resolve_request(
        &mut self,
        request: CachingResolveRequestInternal,
        config: &CachingResolverProcessConfig,
    ) {
        let name = request.request.name.clone();
        self.pending_requests.push_back(request);

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

    fn handle_resolve_result(&mut self, result: CachingResolveResult) {
        match result.result {
            Ok(addresses) => {
                for address in addresses {
                    self.on_address_resolved(address);
                }
            }
            Err(err) => {
                for pending_request in self.pending_requests {
                    (pending_request.completion_callback)(Err(err.clone()))
                }
                return;
            }
        }

        if let Some(pending_request) = self.pending_requests.pop_front() {
            (pending_request.completion_callback)(result.result);
        }
    }

    fn handle_host_event(&mut self, event: CachingHostEvent) {

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
    let host_entry = find_or_create_host_entry(state, &request.request.name);

    host_entry.handle_resolve_request(request, config);
}

fn handle_resolve_result(state: &mut SraCachingResolverState, result: CachingResolveResult) {
    if let Some(entry) = state.hosts.get_mut(&result.name) {
        entry.handle_resolve_result(result);
    }
}

fn handle_host_event(state: &mut SraCachingResolverState, event: CachingHostEvent) {
    if let Some(entry) = state.hosts.get_mut(&event.name) {
        entry.handle_host_event(event);
    }
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
            handle_resolve_result(state, result);
        }
        CachingResolverOperation::HostEvent(event) => handle_host_event(state, event),
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
