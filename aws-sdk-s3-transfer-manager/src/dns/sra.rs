/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::dns::*;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Instant;

/*

A resolver that uses shared state to distribute IPs to resolution requests
from arbitrary sources.  This allows us to properly implement SRA-style name resolution which
has round-robin IP distribution shared across all threads/runtimes interested in establishing
connections to a given name.  This resolver can be shared across a transfer manager's runtime
threads, multiple SDK clients, etc...

Shared state via explicit mutual exclusion is a nightmare, so we use a channel-based
CSP architecture.

The resolver can only be interacted with via a communication channel that can contain resolution
requests, results, and more.  A lightweight recurrent async task loops on that channel's receiver
processing incoming operations.  This creates a natural "serialization" of all resolution
requests regardless of source/location, with an associated private shared state for bookkeeping.

Currently, the async processing task behaves as follows:

On a resolution request:
   If we have good IPs for that name, resolve the request with the least-used good IP
   If there are

      1. no good IPs for that name or
      2. it's been a while since we last resolved that name

      and no resolution is pending, kick off a resolution on the resolver's runtime.  If the
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

IP TTL expiration (or promotion) is processed lazily.

 */

// it's useful to be able to override the resolver's notion of elapsed time when testing
type ClockFn = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Determines what kind of addresses resolution should return.  Happy eyeballs is implemented
/// internally in hyper and so if we want to support it we need to return both ipv4 and ipv6
/// addresses (if they exist).  This makes error correlation much harder (impossible really)
/// because partial failures aren't detectable (for ex: fail to connect to ipv6 but succeed on
/// ipv4 will not trigger any detectable way of penalizing the ipv6 address).
#[derive(Debug, Default, Clone, Copy)]
pub enum NameResolutionAddressTypes {
    /// Resolution should return one ipv4 and ipv6 address, if they exist
    #[default]
    All,

    /// Resolution should only return ipv4 addresses
    IpV4,

    /// Resolution should only return ipv6 addresses
    IpV6,
}

/// Configuration options specific to an SRA Resolver
#[derive(Clone)]
pub struct SraResolverConfig {
    address_types: NameResolutionAddressTypes,

    resolution_ttl: Duration,

    clock: ClockFn,
}

impl Default for SraResolverConfig {
    fn default() -> Self {
        Self {
            address_types: NameResolutionAddressTypes::default(),
            resolution_ttl: DEFAULT_CACHE_TTL,
            clock: Arc::new(Instant::now),
        }
    }
}

impl SraResolverConfig {
    /// Creates a new builder for SraResolverConfig instances
    pub fn builder() -> SraResolverConfigBuilder {
        SraResolverConfigBuilder {
            config: SraResolverConfig::default(),
        }
    }

    /// What types of addresses to resolve
    pub fn address_types(&self) -> NameResolutionAddressTypes {
        self.address_types
    }

    /// What TTL value to use for resolved addresses
    pub fn resolution_ttl(&self) -> Duration {
        self.resolution_ttl
    }

    /// Clock function to use when computing time stamps
    pub fn clock(&self) -> ClockFn {
        self.clock.clone()
    }
}

impl Debug for SraResolverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("SraResolverConfig");
        formatter.field("address_types", &self.address_types);
        formatter.field("resolution_ttl", &self.resolution_ttl);
        formatter.finish()
    }
}

/// Builder type for SraResolverConfig instances
#[derive(Debug)]
pub struct SraResolverConfigBuilder {
    config: SraResolverConfig,
}

impl SraResolverConfigBuilder {
    /// Configures what kind of addresses resolution should return
    pub fn with_address_types(&mut self, address_types: NameResolutionAddressTypes) -> &mut Self {
        self.config.address_types = address_types;
        self
    }

    /// Configures the time-to-live for each resolved address
    pub fn with_resolution_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.config.resolution_ttl = ttl;
        self
    }

    /// (Test-only) Clock function to use when computing time stamps
    #[cfg(test)]
    pub fn with_clock(&mut self, clock: ClockFn) -> &mut Self {
        self.config.clock = clock.clone();
        self
    }

    /// Creates a new SraResolverConfig instance
    pub fn build(self) -> SraResolverConfig {
        self.config.clone()
    }
}

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
    healthy_addresses: PriorityQueue<T, Reverse<u32>>,

    /// Priority queue of IpAddress structs ordered by the timepoint that the address should
    /// move from unhealthy back to healthy
    unhealthy_addresses_by_expiration: PriorityQueue<T, Reverse<Instant>>,
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
            self.healthy_addresses.push(value, Reverse(0));
        }
    }

    /// Removes an address from the set
    fn remove_address(&mut self, value: &T) {
        self.healthy_addresses.remove(value);
        self.unhealthy_addresses_by_expiration.remove(value);
    }

    /// Returns an address from the set to be appended into a DNS resolution result
    fn acquire_address(&mut self, now: Instant) -> Option<T> {
        if self.empty() {
            return None;
        }

        // promote any addresses whose demotion has expired
        while !self.unhealthy_addresses_by_expiration.is_empty() {
            if self.unhealthy_addresses_by_expiration.peek().unwrap().1 .0 <= now {
                let promoted_addr = self.unhealthy_addresses_by_expiration.pop().unwrap().0;
                self.healthy_addresses.push(promoted_addr, Reverse(0));
            } else {
                break;
            }
        }

        // force promote an address if no healthy address exists.  We know in this case an unhealthy
        // address exists because we previously checked if we were empty.
        if self.healthy_addresses.is_empty() {
            let promoted_addr = self.unhealthy_addresses_by_expiration.pop().unwrap().0;
            self.healthy_addresses.push(promoted_addr, Reverse(0));
        }

        // guaranteed to have a healthy address at this point
        let (item, generation) = self.healthy_addresses.pop().unwrap();

        // if the best address's generation is current, we've acquired every healthy address in the
        // set while this generation is active, so increment the generation.
        if generation.0 >= self.current_generation {
            self.current_generation += 1;
        }

        // push the address back on the set using the current generation value
        self.healthy_addresses
            .push(item.clone(), Reverse(self.current_generation));

        Some(item)
    }

    /// Moves an address from a healthy to unhealthy state
    fn demote_address(&mut self, address: T, duration: Duration, now: Instant) {
        self.healthy_addresses.remove(&address);

        let expiration = now + duration;
        self.unhealthy_addresses_by_expiration
            .push(address, Reverse(expiration));
    }

    #[cfg(test)]
    fn current_generation(&self) -> u32 {
        self.current_generation
    }

    #[cfg(test)]
    fn get_healthy_data(&self, address: T) -> Option<u32> {
        if self.healthy_addresses.contains(&address) {
            Some(self.healthy_addresses.get(&address).unwrap().1 .0)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn get_unhealthy_timestamp(&self, address: T) -> Option<Instant> {
        if self.unhealthy_addresses_by_expiration.contains(&address) {
            Some(
                self.unhealthy_addresses_by_expiration
                    .get(&address)
                    .unwrap()
                    .1
                     .0,
            )
        } else {
            None
        }
    }
}

// generation tests
//
// acquire with unhealthy promotion by necessity (new, add, demote, acquire)

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
}

impl HostEntry {
    /// Create a new HostEntry instance
    fn new() -> HostEntry {
        HostEntry {
            pending_requests: VecDeque::new(),
            ipv4_addresses: GenerationalAddressSet::new(),
            ipv6_addresses: GenerationalAddressSet::new(),
            addresses_by_ttl: PriorityQueue::new(),
            last_resolution_timepoint: None,
            pending_resolution: false,
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
    fn on_address_resolved(&mut self, address: IpAddr, config: &SraResolverConfig) {
        let ttl = (config.clock)() + config.resolution_ttl;

        // If we already know about this address, just extend its TTL, regardless of any other
        // (healthy/unhealthy/banned) status
        if self.addresses_by_ttl.contains(&address) {
            self.addresses_by_ttl.change_priority(&address, ttl);
            return;
        }

        // track the address by lifetime
        self.addresses_by_ttl.push(address, ttl);

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
    fn demote_address(&mut self, address: IpAddr, duration: Duration, config: &SraResolverConfig) {
        let now = (config.clock)();
        match address {
            IpAddr::V4(v4addr) => {
                self.ipv4_addresses.demote_address(v4addr, duration, now);
            }
            IpAddr::V6(v6addr) => {
                self.ipv6_addresses.demote_address(v6addr, duration, now);
            }
        }
    }

    /// Removes all addresses whose TTL have expired from the host entry
    fn purge_expired(&mut self, config: &SraResolverConfig) {
        let now = (config.clock)();

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
    fn handle_connection_error(
        &mut self,
        address: IpAddr,
        error: HostEventConnectionError,
        config: &SraResolverConfig,
    ) {
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
            }
            ConnectionErrorCategory::Major => {
                self.demote_address(address, config.resolution_ttl, config);
            }
            ConnectionErrorCategory::Standard(duration) => {
                self.demote_address(address, duration, config);
            }
        }
    }

    /// Pulls addresses from the cache to complete a resolution request.  If no addresses exist,
    /// fail the request
    fn complete_request(&mut self, request: ResolveRequestInternal, config: &SraResolverConfig) {
        let mut addresses: Vec<IpAddr> = Vec::new();

        let now = (config.clock)();
        let v4_addr_option = self.ipv4_addresses.acquire_address(now);
        if let Some(v4_addr) = v4_addr_option {
            addresses.push(IpAddr::V4(v4_addr));
        }

        let v6_addr_option = self.ipv6_addresses.acquire_address(now);
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
        config: &SraResolverConfig,
    ) {
        self.purge_expired(config);

        let now = (config.clock)();
        // if it's our first request or enough time has passed since the last one
        let mut should_spawn_resolve: bool = self.last_resolution_timepoint.is_none();
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
        self.complete_request(request, config);
    }

    /// applies the result of a background resolution task to the host entry.  If resolution was
    /// a failure, then fail all pending requests.  Otherwise, apply all resolved addresses
    /// to the cached state appropriately, then attempt to complete pending requests normally.
    fn handle_resolve_result(&mut self, result: ResolveResult, config: &SraResolverConfig) {
        self.purge_expired(config);
        self.pending_resolution = false;

        match result.result {
            Ok(addresses) => {
                for address in addresses {
                    self.on_address_resolved(address, config);
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
            self.complete_request(pending_request, config);
        }
    }

    /// Applies a host event to a host's entry in the resolver
    fn handle_host_event(&mut self, event: HostEvent, config: &SraResolverConfig) {
        self.purge_expired(config);

        match event.event_data {
            HostEventInformation::ConnectionError(error) => {
                self.handle_connection_error(event.address, error, config)
            }
        }
    }
}

struct SraCachingResolverState {
    hosts: HashMap<String, HostEntry>,

    sra_config: SraResolverConfig,
}

impl SraCachingResolverState {
    fn new(sra_config: SraResolverConfig) -> Self {
        SraCachingResolverState {
            hosts: HashMap::new(),
            sra_config,
        }
    }
}

fn handle_resolve_request(
    context: &OperationHandlerContext,
    state: &mut SraCachingResolverState,
    request: ResolveRequestInternal,
) {
    if !state.hosts.contains_key(&request.request.name) {
        let name = request.request.name.clone();
        state.hosts.insert(name.to_string(), HostEntry::new());
    }

    let host_entry = state.hosts.get_mut(&request.request.name).unwrap();

    host_entry.handle_resolve_request(request, context, &state.sra_config);
}

fn handle_resolve_result(state: &mut SraCachingResolverState, result: ResolveResult) {
    if let Some(entry) = state.hosts.get_mut(&result.name) {
        entry.handle_resolve_result(result, &state.sra_config);
    }
}

fn handle_host_event(state: &mut SraCachingResolverState, event: HostEvent) {
    if let Some(entry) = state.hosts.get_mut(&event.name) {
        entry.handle_host_event(event, &state.sra_config);
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
pub fn new_sra_caching_resolver(
    sra_config: SraResolverConfig,
    caching_config: ResolverConfig,
) -> ResolverHandle {
    let state = SraCachingResolverState::new(sra_config);
    ResolverHandle {
        resolver: Arc::new(ResolverImpl::new(
            caching_config,
            state,
            handle_operation_sra,
        )),
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    #[test]
    fn generational_address_set_newly_created_is_empty() {
        let address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        assert!(address_set.empty());
        assert!(!address_set.contains(&Ipv4Addr::new(127, 0, 0, 1)));
    }

    #[test]
    fn generational_address_set_add_multiple_are_present() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);
        assert!(!address_set.empty());
        assert!(address_set.contains(&loopback));
        assert_eq!(Some(0), address_set.get_healthy_data(loopback));
        assert_eq!(None, address_set.get_unhealthy_timestamp(loopback));

        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);
        assert!(!address_set.empty());
        assert!(address_set.contains(&local));
        assert_eq!(Some(0), address_set.get_healthy_data(local));
        assert_eq!(None, address_set.get_unhealthy_timestamp(local));
    }

    #[test]
    fn generational_address_set_remove_multiple_no_longer_present() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);
        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);

        // local is gone
        address_set.remove_address(&local);
        assert!(!address_set.empty());
        assert!(!address_set.contains(&local));
        assert_eq!(None, address_set.get_healthy_data(local));
        assert_eq!(None, address_set.get_unhealthy_timestamp(local));

        // nothing has happened to loopback yet
        assert!(address_set.contains(&loopback));
        assert_eq!(Some(0), address_set.get_healthy_data(loopback));
        assert_eq!(None, address_set.get_unhealthy_timestamp(loopback));

        // loopback is gone
        address_set.remove_address(&loopback);
        assert!(address_set.empty());
        assert!(!address_set.contains(&loopback));
        assert_eq!(None, address_set.get_healthy_data(loopback));
        assert_eq!(None, address_set.get_unhealthy_timestamp(loopback));
    }

    #[test]
    fn generational_address_set_acquire_multiple_single_generation() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);
        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);

        let now = Instant::now();
        let mut acquired_addresses: Vec<Ipv4Addr> = Vec::new();
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());

        assert!(acquired_addresses.contains(&loopback));
        assert!(acquired_addresses.contains(&local));
        assert_eq!(1, address_set.current_generation());
    }

    #[test]
    fn generational_address_set_acquire_multiple_only_one_available() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);

        let now = Instant::now();
        let mut acquired_addresses: Vec<Ipv4Addr> = Vec::new();
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());

        for address in &acquired_addresses {
            assert_eq!(loopback, *address);
        }

        assert_eq!(3, address_set.current_generation());
    }

    #[test]
    fn generational_address_set_acquire_multiple_multiple_generations() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);
        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);

        let now = Instant::now();
        let mut acquired_addresses: Vec<Ipv4Addr> = Vec::new();
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());

        for idx in 0..3 {
            let start_index = idx * 2;
            let end_index = start_index + 2;

            let generation_slice = &acquired_addresses[start_index..end_index];
            assert!(generation_slice.contains(&loopback));
            assert!(generation_slice.contains(&local));
        }

        assert_eq!(3, address_set.current_generation());
    }

    #[test]
    fn generational_address_set_empty_acquire_failure() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let now = Instant::now();

        assert!(address_set.acquire_address(now).is_none());
        assert!(address_set.acquire_address(now).is_none());
    }

    #[test]
    fn generational_address_set_demote() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);

        let now = Instant::now();
        let unhealthy_duration = Duration::from_secs(10);
        address_set.demote_address(loopback, unhealthy_duration, now);

        assert!(!address_set.empty());
        assert!(address_set.contains(&loopback));
        assert_eq!(None, address_set.get_healthy_data(loopback));
        assert_eq!(
            Some(now + unhealthy_duration),
            address_set.get_unhealthy_timestamp(loopback)
        );
    }

    #[test]
    fn generational_address_set_acquire_unhealthy_promoted_by_time() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);

        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);

        let mut now = Instant::now();
        let unhealthy_duration = Duration::from_secs(10);
        address_set.demote_address(local, unhealthy_duration, now);

        let mut acquired_addresses: Vec<Ipv4Addr> = Vec::new();
        // only loopback should resolve since local is unhealthy for 10 seconds
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        assert_eq!(loopback, acquired_addresses[0]);
        assert_eq!(loopback, acquired_addresses[1]);

        // acquire with advanced "time" so that local should now be healthy
        acquired_addresses.clear();
        now += unhealthy_duration;
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        assert!(acquired_addresses.contains(&loopback));
        assert!(acquired_addresses.contains(&local));
    }

    #[test]
    fn generational_address_set_acquire_unhealthy_promoted_by_necessity() {
        let mut address_set: GenerationalAddressSet<Ipv4Addr> = GenerationalAddressSet::new();

        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        address_set.add_address(loopback);

        let local = Ipv4Addr::new(192, 168, 0, 1);
        address_set.add_address(local);

        let now = Instant::now();
        let unhealthy_duration = Duration::from_secs(10);
        address_set.demote_address(local, unhealthy_duration, now);

        let mut acquired_addresses: Vec<Ipv4Addr> = Vec::new();
        // only loopback should resolve since local is unhealthy for 10 seconds
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        assert_eq!(loopback, acquired_addresses[0]);
        assert_eq!(loopback, acquired_addresses[1]);

        // remove loopback, leaving only an unhealthy local
        address_set.remove_address(&loopback);

        // acquire with current time, forcing local to be promoted
        acquired_addresses.clear();
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        acquired_addresses.push(address_set.acquire_address(now).unwrap());
        assert_eq!(local, acquired_addresses[0]);
        assert_eq!(local, acquired_addresses[1]);
    }
}
