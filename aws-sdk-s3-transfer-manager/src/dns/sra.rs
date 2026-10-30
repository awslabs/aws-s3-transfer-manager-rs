/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::dns::*;
use priority_queue::PriorityQueue;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Instant;

/*

Proof of concept for a resolver that uses shared state to distribute IPs to resolution requests
from arbitrary sources.  This allows us to properly implement SRA-style name resolution which
has round-robin IP distribution shared across all threads/runtimes interested in establishing
connections to a given name.  This resolver can be shared across a transfer manager's runtime
threads, multiple SDK clients, etc...

Shared state via explicit mutual exclusion is a nightmare, so we use a channel-based
CSP architecture.

The resolver can only be interacted with via a communication channel that can contain resolution
requests, results, and more.  A lightweight recurrent async task loops on that channel's receiver
processing incoming operations.  This creates a natural "serialization" of all resolution
requests regardless of source, with an associated private shared state for bookkeeping.

The async processing task might behave as follows:

On a resolution request:
   If we have good IPs for that name, resolve the request with the least-used good IP
   If there are 1. no good IPs for that name or 2. it's been a while since we last resolved that
      name, and no resolution is pending, kick off a resolution on the resolver's runtime.  If the
      request was not resolved earlier, park the request until a resolution returns a result.

On a resolution result:
   Process the list of IPs returned, adding new ones to the good IP set for that name and refreshing
     the TTL on existing ones.
   For all parked requests on that name, complete the request with a good IP (by least-used priority)
     or the error if the resolution failed.

On a host event:
   On connection error:
      If the error is fatal, remove the ip address from consideration permanently
      If the error is major, mark the ip address as unhealthy for a time period based on
        the TTL for lookups.  Unhealthy IPs cannot be used to answer resolve requests.  Unhealthy
        IPs can be prematurely promoted back to healthy if no healthy IPs exist.

On IP TTL expiration:
   Remove the ip from the resolver cache completely

 */

/// Tracks the cached state of IP addresses of a specific type.  Addresses are classified as
/// healthy (can be used) or unhealthy (temporarily not allowed to be used).  Healthy addresses
/// are distributed in a round-robin "generational" fashion.  When an address is first seen, it
/// is placed at generation 0, which puts it at the front of the queue.  When an address gets
/// mapped to a resolve request, its generation is set to the current value.  When all addresses
/// in the set are at the current generation value, we increment the current generation value.
///
/// This unconventional counting scheme is primarily due to the fact that we currently cannot tell
/// how many outstanding, in-use connections are using a given IP because the consumers (http
/// clients) of the resolver do not expose visibility into that at the moment.  If we knew how
/// many open connections were using a given IP, we would sort on that instead of "generation"
struct GenerationalAddressSet<T>
where
    T: Hash + Eq + Clone,
{
    /// A marker value that represents the approximate number of times we've cycled through
    /// the set of healthy addresses when distributing IPs.
    current_generation: u32,

    /// Priority queue of IpAddress structs ordered by the generation they were last acquired in
    healthy_addresses: PriorityQueue<T, u32>,

    /// Priority queue of IpAddress structs ordered by the timepoint that the address should
    /// move from unhealthy back to healthy
    unhealthy_addresses_by_expiration: PriorityQueue<T, Instant>,
}

impl<T> GenerationalAddressSet<T>
where
    T: Hash + Eq + Clone,
{
    /// Creates a new address set
    fn new() -> Self {
        GenerationalAddressSet {
            current_generation: 1,
            healthy_addresses: PriorityQueue::new(),
            unhealthy_addresses_by_expiration: PriorityQueue::new(),
        }
    }

    /// Is an address already in the set?
    fn contains(&self, value: &T) -> bool {
        self.healthy_addresses.contains(value)
            || self.unhealthy_addresses_by_expiration.contains(value)
    }

    /// Is the set empty?
    fn empty(&self) -> bool {
        self.healthy_addresses.is_empty() && self.unhealthy_addresses_by_expiration.is_empty()
    }

    /// Add an address to the set by inserting it into the healthy subset.
    fn add_address(&mut self, value: T) {
        if !self.contains(&value) {
            self.healthy_addresses.push(value, 0);
        }
    }

    /// Removes an address from the set
    fn remove_address(&mut self, value: &T) {
        self.healthy_addresses.remove(value);
        self.unhealthy_addresses_by_expiration.remove(value);
    }

    /// Returns an address from the set to be appended into a DNS resolution result
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

        // force promote an address if no healthy address exists.  We know in this case an unhealthy
        // address exists because we previously checked if we were empty.
        if self.healthy_addresses.is_empty() {
            let promoted_addr = self.unhealthy_addresses_by_expiration.pop().unwrap().0;
            self.healthy_addresses.push(promoted_addr, 0);
        }

        // guaranteed to have a healthy address at this point
        let (item, generation) = self.healthy_addresses.pop().unwrap();

        // if the best address's generation is current, we've acquired every healthy address in the
        // set while this generation is active, so increment the generation.
        if generation == self.current_generation {
            self.current_generation += 1;
        }

        // push the address back on the set using the current generation value
        self.healthy_addresses
            .push(item.clone(), self.current_generation);

        Some(item)
    }

    /// Moves an address from a healthy to unhealthy state
    fn demote_address(&mut self, address: T, duration: Duration) {
        self.healthy_addresses.remove(&address);

        let expiration = Instant::now() + duration;
        self.unhealthy_addresses_by_expiration
            .push(address, expiration);
    }
}

/// Amount of time that needs to elapse between resolves on a given name.  We may want to
/// make this an external configuration option.  Unlike the CRT, we don't "ping" resolves at
/// this interval, instead we lazily do them over time in response to lookups.
const MINIMUM_DELAY_BETWEEN_RESOLVES: Duration = Duration::from_secs(1);

/// Per-host-name struct that tracks resolved addresses, pending resolution requests, and more.
struct HostEntry {
    /// ordered queue of resolution requests that have not yet been completed
    pending_requests: VecDeque<ResolveRequestInternal>,

    /// known ipv4 addresses
    ipv4_addresses: GenerationalAddressSet<Ipv4Addr>,

    /// know ipv6 addresses
    ipv6_addresses: GenerationalAddressSet<Ipv6Addr>,

    /// all known addresses ordered by when we should forget them.  It is possible for an address
    /// to be in this set while not in either of the generational sets.  This is the fate of
    /// addresses which received a fatal connection error and prevents them from ever being used
    /// (even promoted from unhealthy to healthy by an emergency) as long as they are remembered.
    addresses_by_ttl: PriorityQueue<IpAddr, Instant>,

    /// last time we spawned an async resolution task to resolve this name
    last_resolution_timepoint: Option<Instant>,

    /// is an async resolution task currently outstanding?
    pending_resolution: bool,

    /// what is the base TTL value for addresses that we resolve?  An address may live longer than
    /// the TTL.  Every time a resolve sees an address, if it is already known, we extend its
    /// lifetime accordingly.
    ttl: Duration,
}

impl HostEntry {
    /// Create a new HostEntry instance
    fn new(ttl: Duration) -> HostEntry {
        HostEntry {
            pending_requests: VecDeque::new(),
            ipv4_addresses: GenerationalAddressSet::new(),
            ipv6_addresses: GenerationalAddressSet::new(),
            addresses_by_ttl: PriorityQueue::new(),
            last_resolution_timepoint: None,
            pending_resolution: false,
            ttl,
        }
    }

    /// Removes an address completely from the host entry.
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

    /// Updates the entry with a newly-resolved address
    fn on_address_resolved(&mut self, address: IpAddr) {
        let ttl = Instant::now() + self.ttl;

        // If we already know about this address, just extend its TTL, regardless of any other
        // (healthy/unhealthy/banned) status
        if self.addresses_by_ttl.contains(&address) {
            self.addresses_by_ttl.change_priority(&address, ttl);
            return;
        }

        // track the address by lifetime
        self.addresses_by_ttl.push(address.clone(), ttl);

        // add it to the correct set
        match address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses.add_address(v4addr);
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses.add_address(v6addr);
            }
        }
    }

    /// Mark an address as unhealthy.  If the address is already unhealthy, extend the duration
    /// it will stay in that state.
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

    /// Removes all addresses whose TTL have expired from the host entry
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

    /// Penalize an address based on the connection error category
    fn handle_connection_error(&mut self, address: IpAddr, error: HostEventConnectionError) {
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
                self.demote_address(address, self.ttl);
            }
            ConnectionErrorCategory::Standard(duration) => {
                self.demote_address(address, duration);
            }
        }
    }

    /// Pulls addresses from the cache to complete a resolution request.  If no addresses exist,
    /// fail the request
    fn complete_request(&mut self, request: ResolveRequestInternal) {
        let mut addresses: Vec<IpAddr> = Vec::new();

        let v4_addr_option = self.ipv4_addresses.acquire_address();
        if let Some(v4_addr) = v4_addr_option {
            addresses.push(IpAddr::V4(v4_addr));
        }

        let v6_addr_option = self.ipv6_addresses.acquire_address();
        if let Some(v6_addr) = v6_addr_option {
            addresses.push(IpAddr::V6(v6_addr));
        }

        if addresses.is_empty() {
            (request.completion_callback)(Err(ResolveDnsError::new(
                "No addresses could be resolved",
            )));
        } else {
            (request.completion_callback)(Ok(addresses));
        }
    }

    /// handles a resolution request by appropriately using cached addresses and potentially kicking
    /// off a background resolution
    fn handle_resolve_request(
        &mut self,
        request: ResolveRequestInternal,
        context: &OperationHandlerContext,
    ) {
        self.purge_expired();

        // if it's our first request or enough time has passed since the last one
        let mut should_spawn_resolve: bool = self.last_resolution_timepoint.is_none();
        let now = Instant::now();
        if let Some(last_resolution_timepoint) = self.last_resolution_timepoint {
            if now >= last_resolution_timepoint + MINIMUM_DELAY_BETWEEN_RESOLVES
                && !self.pending_resolution
            {
                should_spawn_resolve = true;
            }
        }

        if should_spawn_resolve {
            self.pending_resolution = true;
            self.last_resolution_timepoint = Some(now);

            (context.resolution_function)(request.request.name.clone());
        }

        // if no addresses are available and we have a background resolution in progress, enqueue
        // the request to be finished when the pending resolution finishes
        if self.ipv4_addresses.empty() && self.ipv6_addresses.empty() && self.pending_resolution {
            self.pending_requests.push_back(request);
            return;
        }

        // complete the request with what we have
        self.complete_request(request);
    }

    /// applies the result of a background resolution task to the host entry.  If resolution was
    /// a failure, then fail all pending requests.  Otherwise, apply all resolved addresses
    /// to the cached state appropriately, then attempt to complete pending requests normally.
    fn handle_resolve_result(&mut self, result: ResolveResult) {
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

        // complete any pending requests with what we have available, if anything
        while !self.pending_requests.is_empty() {
            let pending_request = self.pending_requests.pop_front().unwrap();
            self.complete_request(pending_request);
        }
    }

    /// Applies a host event to a host's entry in the resolver
    fn handle_host_event(&mut self, event: HostEvent) {
        self.purge_expired();

        match event.event_data {
            HostEventInformation::ConnectionError(error) => {
                self.handle_connection_error(event.address, error)
            }
        }
    }
}

#[derive(Default)]
struct SraCachingResolverState {
    hosts: HashMap<String, HostEntry>,
}

fn handle_resolve_request(
    context: &OperationHandlerContext,
    state: &mut SraCachingResolverState,
    request: ResolveRequestInternal,
) {
    if !state.hosts.contains_key(&request.request.name) {
        let name = request.request.name.clone();
        state
            .hosts
            .insert(name.to_string(), HostEntry::new(context.ttl));
    }

    let host_entry = state.hosts.get_mut(&request.request.name).unwrap();

    host_entry.handle_resolve_request(request, context);
}

fn handle_resolve_result(state: &mut SraCachingResolverState, result: ResolveResult) {
    if let Some(entry) = state.hosts.get_mut(&result.name) {
        entry.handle_resolve_result(result);
    }
}

fn handle_host_event(state: &mut SraCachingResolverState, event: HostEvent) {
    if let Some(entry) = state.hosts.get_mut(&event.name) {
        entry.handle_host_event(event);
    }
}

fn handle_operation_sra(
    context: &OperationHandlerContext,
    state: &mut SraCachingResolverState,
    operation: ResolverOperation,
) {
    match operation {
        ResolverOperation::ResolveRequest(request) => {
            handle_resolve_request(context, state, request);
        }
        ResolverOperation::ResolveResult(result) => {
            handle_resolve_result(state, result);
        }
        ResolverOperation::HostEvent(event) => handle_host_event(state, event),
    }
}

/// Creates a new caching resolver that follows the SRA specification for domain name resolution
pub fn new_sra_caching_resolver(config: ResolverConfig) -> ResolverHandle {
    ResolverHandle {
        resolver: Arc::new(ResolverImpl::new_default(config, handle_operation_sra)),
    }
}
