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

struct GenerationalAddressSet<T>
where
    T: Hash + Eq + Clone,
{
    current_generation: u32,

    healthy_addresses: PriorityQueue<T, u32>,
    unhealthy_addresses_by_expiration: PriorityQueue<T, Instant>,
}

impl<T> GenerationalAddressSet<T>
where
    T: Hash + Eq + Clone,
{
    fn new() -> Self {
        GenerationalAddressSet {
            current_generation: 1,
            healthy_addresses: PriorityQueue::new(),
            unhealthy_addresses_by_expiration: PriorityQueue::new(),
        }
    }

    fn contains(&self, value: &T) -> bool {
        self.healthy_addresses.contains(value)
            || self.unhealthy_addresses_by_expiration.contains(value)
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

        self.healthy_addresses
            .push(item.clone(), self.current_generation);

        Some(item)
    }

    fn demote_address(&mut self, address: T, duration: Duration) {
        self.healthy_addresses.remove(&address);

        let expiration = Instant::now() + duration;
        self.unhealthy_addresses_by_expiration
            .push(address, expiration);
    }
}

const MINIMUM_DELAY_BETWEEN_RESOLVES: Duration = Duration::from_secs(1);

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
            ttl,
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

    fn handle_connection_error(&mut self, address: IpAddr, error: CachingHostEventConnectionError) {
        let mut demotion_duration: Option<Duration> = None;

        match error.category {
            ConnectionErrorCategory::Fatal => {
                // remove from the sets, keep in TTL, effectively banishes the address until
                // forgotten
                match address {
                    IpAddr::V4(v4addr) => {
                        self.ipv4_addresses.remove_address(&v4addr);
                    }
                    IpAddr::V6(v6addr) => {
                        self.ipv6_addresses.remove_address(&v6addr);
                    }
                }

                return;
            }
            ConnectionErrorCategory::Major => {
                demotion_duration = Some(self.ttl);
            }
            ConnectionErrorCategory::Standard(duration) => {
                demotion_duration = Some(duration);
            }
        }

        match address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses
                    .demote_address(v4addr, demotion_duration.unwrap());
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses
                    .demote_address(v6addr, demotion_duration.unwrap());
            }
        }
    }

    pub fn complete_request(&mut self, request: CachingResolveRequestInternal) {
        let mut addresses: Vec<IpAddr> = Vec::new();

        let v4_addr_option = self.ipv4_addresses.acquire_address();
        if let Some(v4_addr) = v4_addr_option {
            addresses.push(IpAddr::V4(v4_addr));
        }

        let v6_addr_option = self.ipv6_addresses.acquire_address();
        if let Some(v6_addr) = v6_addr_option {
            addresses.push(IpAddr::V6(v6_addr));
        }

        (request.completion_callback)(Ok(addresses));
    }

    fn handle_resolve_request(
        &mut self,
        request: CachingResolveRequestInternal,
        context: &CachingOperationHandlerContext,
    ) {
        self.purge_expired();

        let now = Instant::now();
        if let Some(last_resolution_timepoint) = self.last_resolution_timepoint {
            if now >= last_resolution_timepoint + MINIMUM_DELAY_BETWEEN_RESOLVES
                && !self.pending_resolution
            {
                self.pending_resolution = true;
                self.last_resolution_timepoint = Some(now);

                (context.resolution_function)(request.request.name.clone());
            }
        }

        if self.ipv4_addresses.empty() && self.ipv6_addresses.empty() {
            self.pending_requests.push_back(request);
            return;
        }

        self.complete_request(request);
    }

    fn handle_resolve_result(&mut self, result: CachingResolveResult) {
        self.purge_expired();
        self.pending_resolution = false;

        match result.result {
            Ok(addresses) => {
                for address in addresses {
                    self.on_address_resolved(address);
                }
            }
            Err(err) => {
                while !self.pending_requests.is_empty() {
                    let pending_request = self.pending_requests.pop_front().unwrap();
                    // we can't actually clone errors but we need to complete one or more requests
                    // with the resolve error.  This is the best I could come up with =/
                    let cloned = ResolveDnsError::new(err.to_string());
                    (pending_request.completion_callback)(Err(cloned));
                }
                return;
            }
        }

        while !self.pending_requests.is_empty() {
            let pending_request = self.pending_requests.pop_front().unwrap();
            self.complete_request(pending_request);
        }
    }

    fn handle_host_event(&mut self, event: CachingHostEvent) {
        self.purge_expired();

        match event.event_data {
            CachingHostEventInformation::ConnectionError(error) => {
                self.handle_connection_error(event.address, error)
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct SraCachingResolverState {
    hosts: HashMap<String, HostEntry>,
}

fn handle_resolve_request(
    context: &CachingOperationHandlerContext,
    state: &mut SraCachingResolverState,
    request: CachingResolveRequestInternal,
) {
    if !state.hosts.contains_key(&request.request.name) {
        let name = request.request.name.clone();
        state
            .hosts
            .insert(name.to_string(), HostEntry::new(&name, context.ttl));
    }

    let host_entry = state.hosts.get_mut(&request.request.name).unwrap();

    host_entry.handle_resolve_request(request, context);
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
    context: &CachingOperationHandlerContext,
    state: &mut SraCachingResolverState,
    operation: CachingResolverOperation,
) {
    match operation {
        CachingResolverOperation::ResolveRequest(request) => {
            handle_resolve_request(context, state, request);
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
