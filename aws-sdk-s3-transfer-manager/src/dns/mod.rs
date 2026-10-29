/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Module for a caching resolver that implements the SRA specification for name resolution
pub mod sra;

use aws_smithy_runtime_api::client::dns::{
    DnsFuture, ResolveDns, ResolveDnsError, SharedDnsResolver,
};
use std::fmt::{Debug, Formatter};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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

A very basic async processing task might behave as follows:

On a resolution request:
   If we have good IPs for that name, resolve the request with the least-used good IP*
   If there are no good IPs for that name, and no resolution is pending, kick off a resolution on
      the resolver's runtime.  Either way, park the request until a resolution returns a result.
   * - [More advanced] Even if there are good IPs, if the (# connections / IP) ratio is above X, spawn
      a resolution task to try and find more IPs (but don't park the request) in an effort to drive
      down the ratio (assuming continued use).   This replaces the CRT's DNS "pinging" behavior which
      has been classified as potentially undesirable.

On a resolution result:
   Process the list of IPs returned, adding new ones to the good IP set for that name and refreshing
     the TTL on existing ones.
   For all parked requests on that name, complete the request with a good IP (by least-used priority)
     or the error if the resolution failed.

On a host event:
   (Host events are anything IP-related that might shape our IP distribution decision-making, so
    things like a connection failing for some reason, a connection closing, etc...)  These events
    will tend to update the internal bookkeeping shared state (good IP -> bad IP, # connections for
    an IP, etc...).  It is unknown how we will connect the systems that know enough to emit the events
    with the caching resolver public API.

Other notes:
    IPs will have a (configurable) TTL.  IPs will be forgotten about if not seen by
      a resolution for that period of time.  Unfortunately, getaddrinfo based resolution does
      not expose the actual TTL value in any way, so this will be somewhat artificial.
    In addition to good IPs there will also be some sort of bad ip list which good IPs may get
      moved to if host events dictate (connection failures, etc...)

 */

/// Determines what kind of addresses resolution should return.  Happy eyeballs is implemented
/// internally in hyper and so if we want to support it we need to return both ipv4 and ipv6
/// addresses (if they exist).  This makes error correlation much harder (impossible really)
/// because partial failures aren't detectable (for ex: fail to connect to ipv6 but succeed on
/// ipv4 will not trigger any detectable way of penalizing the ipv6 address).
#[derive(Debug, Clone)]
pub enum NameResolutionAddressTypes {
    /// Resolution should return at least one ipv4 and ipv6 address, if they exist
    All,

    /// Resolution should only return ipv4 addresses
    IpV4,

    /// Resolution should only return ipv6 addresses
    IpV6,
}

impl Default for NameResolutionAddressTypes {
    fn default() -> Self {
        NameResolutionAddressTypes::All
    }
}

/// All configuration options for a caching DNS resolver
#[derive(Clone, Debug, Default)]
pub struct CachingResolverConfig {
    internal_resolver: Option<SharedDnsResolver>,
    runtime: Option<tokio::runtime::Handle>,
    address_types: NameResolutionAddressTypes,
    default_ttl: Option<Duration>,
}

/// Builder type for caching resolver configuration instances
#[derive(Debug)]
pub struct CachingResolverConfigBuilder {
    config: CachingResolverConfig,
}

impl CachingResolverConfigBuilder {
    /// Creates a new caching resolver configuration builder instance
    pub fn new() -> Self {
        Self {
            config: CachingResolverConfig::default(),
        }
    }

    /// Configures what will perform our internal name resolution when the situation requires
    pub fn with_internal_resolver(&mut self, resolver: SharedDnsResolver) -> &Self {
        self.config.internal_resolver = Some(resolver);
        self
    }

    /// Configures the runtime that will execute our recurrent async processing task as well as
    /// name resolution lookups.  We may need to split into two if we want to use anything other
    /// than tokio.  The recurrent async processing task requires tokio (mpsc, one-shot, select!),
    /// while the name resolution lookups could use any kind of future-handling runtime.
    pub fn with_runtime(&mut self, runtime: tokio::runtime::Handle) -> &Self {
        self.config.runtime = Some(runtime);
        self
    }

    /// Configures what kind of addresses resolution should return
    pub fn with_address_types(&mut self, address_types: NameResolutionAddressTypes) -> &mut Self {
        self.config.address_types = address_types;
        self
    }

    /// Configures the time-to-live for each resolved address
    pub fn with_default_ttl(&mut self, default_ttl: Duration) -> &mut Self {
        self.config.default_ttl = Some(default_ttl);
        self
    }

    /// Creates a new configuration instance for caching resolvers
    pub fn build(&self) -> CachingResolverConfig {
        self.config.clone()
    }
}

// Why doesn't this exist in smithy dns?
/// Fundamental name lookup result type
pub type ResolveDnsResult = Result<Vec<IpAddr>, ResolveDnsError>;

/// Models a name resolution request.
/// Future possibilities could include TTL override, IPv4 vs IPv6 restrictions, etc...
/// This is the request type used in the public resolution API.
#[derive(Debug, Clone)]
pub struct CachingResolveRequest {
    name: String,
}

impl CachingResolveRequest {
    /// the host name to perform a name resolution for
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Builder type for CachingResolveRequest instances
#[derive(Debug)]
pub struct CachingResolveRequestBuilder {
    request: CachingResolveRequest,
}

impl CachingResolveRequestBuilder {
    /// Create a new builder instance
    pub fn new(name: &str) -> Self {
        CachingResolveRequestBuilder {
            request: CachingResolveRequest {
                name: name.to_string(),
            },
        }
    }

    /// Create a new CachingResolveRequest instance
    pub fn build(&self) -> CachingResolveRequest {
        self.request.clone()
    }
}

/// Type signature for a resolution request's completion callback
pub type ResolutionCompletionFn = Box<dyn FnOnce(ResolveDnsResult) -> () + Send + Sync>;

/// Internal resolve request type.  Needs to be public so that caching resolver variants can
/// be implemented by others.
pub struct CachingResolveRequestInternal {
    request: CachingResolveRequest,

    completion_callback: ResolutionCompletionFn,
}

impl CachingResolveRequestInternal {
    /// The name resolution request
    pub fn request(&self) -> &CachingResolveRequest {
        &self.request
    }

    /// Channel that completes the user-facing resolution request's future
    pub fn completion_callback(&self) -> &ResolutionCompletionFn {
        &self.completion_callback
    }
}

impl std::fmt::Debug for CachingResolveRequestInternal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

/// The result of a name resolution lookup attempt
#[derive(Debug)]
pub struct CachingResolveResult {
    name: String,
    result: ResolveDnsResult,
}

impl CachingResolveResult {
    /// Create a new CachingResolveResult instance
    pub fn new(name: &str, result: ResolveDnsResult) -> Self {
        CachingResolveResult {
            name: name.to_string(),
            result,
        }
    }

    /// What host name this is a resolve result for
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The result of looking up the host name
    pub fn result(&self) -> &ResolveDnsResult {
        &self.result
    }
}

/// Classifies a connection-related error.  Determines how the address is "penalized" with respect
/// to future resolution requests.
#[derive(Debug, Eq, PartialEq)]
pub enum ConnectionErrorCategory {
    /// Never use this address again for as long as it's tracked
    Fatal,

    /// Avoid this address unless there's no other alternative
    /// Equivalent to Standard(Infinite)
    Major,

    /// Avoid using this address for at least a configurable amount of time
    Standard(Duration),
}

/// Event emitted when a connection attempt using a resolved address fails.  Later we'll add
/// failure context.
#[derive(Debug)]
pub struct CachingHostEventConnectionError {
    /// How the resolver should penalize the address in future resolutions
    pub category: ConnectionErrorCategory,

    /// Actual source error triggering this event
    pub error: Option<Box<dyn std::error::Error + Send + Sync>>, // fix this type into an Into or something
}

/// Per-host-event-type variants
#[non_exhaustive]
#[derive(Debug)]
pub enum CachingHostEventInformation {
    /// Notifies the resolver that a consumer encountered an error condition while using an
    /// address sourced from the resolver
    ConnectionError(CachingHostEventConnectionError),
}

/// A connection-related event that could cause us to update our internal bookkeeping that
/// helps distribute IPs across connections.
#[derive(Debug)]
pub struct CachingHostEvent {
    /// Name of the host that the address was resolver for
    pub name: String,

    /// IP address of the connection that encountered an error
    pub address: IpAddr,

    /// Per-event-type data
    pub event_data: CachingHostEventInformation,
}

/// The fundamental operation type that a caching resolver works with.
#[non_exhaustive]
#[derive(Debug)]
pub enum CachingResolverOperation {
    /// An operation that requests an IP address for a host name
    ResolveRequest(CachingResolveRequestInternal),

    /// An operation holding the result of looking up IP addresses for a host name
    ResolveResult(CachingResolveResult),

    /// An operation detailing an event that may cause the resolver to change how it
    /// completes resolve requests for a host name
    HostEvent(CachingHostEvent),
}

pub type ResolutionFunction = Box<dyn Fn(String) + Send + Sync>;

/// Configuration for the async recurrent task that handles all incoming CachingResolverOperation
/// instances
pub struct CachingOperationHandlerContext {
    /// function object that will actually perform lookups.  Lookups are performed on the runtime
    /// associated with the caching resolver
    resolution_function: ResolutionFunction,

    /// How long a lookup stays cached; may be extended indefinitely by future lookups refreshing
    /// an individual address's ttl
    ttl: Duration,
}

/// Represents the centralized, recurrent task that handles CachingResolverOperation instances
struct CachingResolverProcess<T> {
    /// (semi-)immutable configuration
    context: CachingOperationHandlerContext,

    /// input queue receiver, all communication from the outside world (including resolution task
    /// results) comes through here
    receiver: mpsc::UnboundedReceiver<CachingResolverOperation>,

    /// the (mutable) bookkeeping state (cache) the caching resolver uses to make resolution decisions
    state: T,
}

impl<T> CachingResolverProcess<T>
where
    T: Default,
{
    /// Instantiate a new caching resolver
    fn new(
        state: T,
        internal_resolver: SharedDnsResolver,
        sender: mpsc::UnboundedSender<CachingResolverOperation>,
        receiver: mpsc::UnboundedReceiver<CachingResolverOperation>,
        ttl: Duration,
    ) -> Self {
        let resolver = internal_resolver.clone();
        let operation_channel = sender.clone();
        let resolution_function: ResolutionFunction = Box::new(move |name| {
            let handle = tokio::runtime::Handle::current();
            let internal_name = name.to_string();
            let internal_resolver = resolver.clone();
            let internal_channel = operation_channel.clone();
            handle.spawn(async move {
                let result = internal_resolver.resolve_dns(&internal_name).await;
                let caching_result =
                    CachingResolverOperation::ResolveResult(CachingResolveResult {
                        name: internal_name,
                        result,
                    });
                let _ = internal_channel.send(caching_result);
            });
        });

        CachingResolverProcess {
            context: CachingOperationHandlerContext {
                resolution_function,
                ttl,
            },
            receiver,
            state,
        }
    }

    // is it a problem to run this function on the same runtime that the internal resolves are being
    // spawned on?  The worry is that long-running timeouts on blocking-style resolutions (like
    // getaddrinfo) could choke this task's ability to distribute IPs to requests for other names.
    // that aren't having a problem.  Name lookups can take very different amounts of time depending
    // on whether they hit a local system cache or must go out on the network.
    //
    // If we find we need to split them, then we'll need two runtimes.  This function must run on
    // tokio (due to mpsc and select!), while the resolution lookups could run on anything that can
    // handle futures.

    /// Core recurrent loop that handles incoming operations
    async fn run(
        &mut self,
        operation_handler: fn(
            context: &CachingOperationHandlerContext,
            &mut T,
            CachingResolverOperation,
        ) -> (),
    ) {
        let receiver = &mut self.receiver;
        let context = &mut self.context;
        let state = &mut self.state;

        // runs until recv returns None implying all input queue Sender clones are gone
        loop {
            // eventually we may select on time too in order to support TTL expiration
            tokio::select! {
                operation_result = receiver.recv() => {
                    match operation_result {
                        Some(operation) => {
                            (operation_handler)(&context, state, operation);
                        }
                        None => {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Unit result type for resolver API(s)
pub type ResolverResult = Result<(), aws_smithy_runtime_api::box_error::BoxError>;

/// Public API for the caching resolver.  Intentionally distinct from ResolveDns.
pub trait CachingResolver: Debug + Send + Sync {
    /// Perform a resolution request.  Equivalent to ResolveDns::resolve_dns but with a different
    /// API to allow for extensibility (additional flags, options, etc...)
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static>;

    /// Notify the resolver of an external host-related event that may affect how it
    /// chooses to distribute IP addresses to resolution requests
    fn notify(&self, event: CachingHostEvent) -> ResolverResult;
}

/// Function object signature for the logic that will perform a lookup for a name and
/// route the result back to the caching resolver via the input channel
pub type ResolverFunction = Box<dyn Fn(String)>;

/// Internal caching resolver implementation type that serves as the entry point for
/// resolution requests
#[derive(Debug)]
struct CachingResolverImpl {
    self_created_runtime: Option<tokio::runtime::Runtime>,
    operation_channel: mpsc::UnboundedSender<CachingResolverOperation>,
}

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);

impl CachingResolverImpl {
    fn new_default<T>(
        config: CachingResolverConfig,
        operation_handler: fn(
            context: &CachingOperationHandlerContext,
            &mut T,
            CachingResolverOperation,
        ) -> (),
    ) -> Self
    where
        T: Default + Send + 'static,
    {
        CachingResolverImpl::new(config, T::default(), operation_handler)
    }

    /// Combines configuration and an operation handler function to create a new caching resolver
    /// instance.
    fn new<T>(
        config: CachingResolverConfig,
        state: T,
        operation_handler: fn(
            context: &CachingOperationHandlerContext,
            &mut T,
            CachingResolverOperation,
        ) -> (),
    ) -> Self
    where
        T: Default + Send + 'static,
    {
        let (tx, rx): (
            mpsc::UnboundedSender<CachingResolverOperation>,
            mpsc::UnboundedReceiver<CachingResolverOperation>,
        ) = mpsc::unbounded_channel();

        let mut caching_resolver = CachingResolverImpl {
            self_created_runtime: None,
            operation_channel: tx.clone(),
        };

        // Config resolver || basic getaddrinfo resolver
        let internal_resolver = config.internal_resolver.unwrap_or(SharedDnsResolver::new(
            aws_smithy_runtime::client::dns::TokioDnsResolver::new(),
        ));
        let mut resolver_impl = CachingResolverProcess::<T>::new(
            state,
            internal_resolver,
            tx,
            rx,
            config.default_ttl.unwrap_or(DEFAULT_CACHE_TTL),
        );

        // Config runtime || create a new runtime
        if let Some(config_runtime) = config.runtime {
            config_runtime.spawn(async move {
                resolver_impl.run(operation_handler).await;
            });
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("caching-resolver-fallback-rt")
                .build()
                .unwrap();

            runtime.spawn(async move {
                resolver_impl.run(operation_handler).await;
            });

            // need to keep this alive, a handle is insufficient
            caching_resolver.self_created_runtime = Some(runtime);
        }

        caching_resolver
    }
}

impl CachingResolver for CachingResolverImpl {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static> {
        let (tx, rx) = tokio::sync::oneshot::channel::<ResolveDnsResult>();
        let internal_request = CachingResolveRequestInternal {
            request,
            completion_callback: Box::new(move |result| {
                // if this fails, there's nothing we can do; it's likely the waiter is gone
                let _ = tx.send(result);
            }),
        };

        let send_result = self
            .operation_channel
            .send(CachingResolverOperation::ResolveRequest(internal_request));

        DnsFuture::new(async move {
            if let Err(send_result_err) = send_result {
                return Err(ResolveDnsError::new(std::io::Error::other(send_result_err)));
            }

            let result = rx.await;

            result.unwrap_or_else(|err| Err(ResolveDnsError::new(std::io::Error::other(err))))
        })
    }

    fn notify(&self, event: CachingHostEvent) -> ResolverResult {
        self.operation_channel
            .send(CachingResolverOperation::HostEvent(event))
            .map_err(|err| err.into()) // don't quite understand what's going on here tbh
    }
}

/// Clonable, sharable handle to a caching resolver.  This is the public-facing instance type for
/// all caching resolver variants.  It implements both its internal trait (CachingResolver) and the
/// public external smithy DNS trait (ResolveDns).
#[derive(Clone)]
pub struct CachingResolverHandle {
    resolver: Arc<dyn CachingResolver>,
}

impl CachingResolverHandle {
    /// Factory function for creating caching resolver instances
    pub fn new<T>(
        config: CachingResolverConfig,
        operation_handler: fn(
            context: &CachingOperationHandlerContext,
            &mut T,
            CachingResolverOperation,
        ) -> (),
    ) -> Self
    where
        T: Default + Send + 'static,
    {
        CachingResolverHandle {
            resolver: Arc::new(CachingResolverImpl::new_default(config, operation_handler)),
        }
    }
}

impl CachingResolver for CachingResolverHandle {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static> {
        self.resolver.resolve(request)
    }

    fn notify(&self, event: CachingHostEvent) -> ResolverResult {
        self.resolver.notify(event)
    }
}

impl std::fmt::Debug for CachingResolverHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.resolver.fmt(f)
    }
}

impl ResolveDns for CachingResolverHandle {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let request = CachingResolveRequest {
            name: name.to_string(),
        };

        self.resolve(request)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::future::Future;
    use std::net::Ipv4Addr;

    #[derive(Default, Debug)]
    pub(crate) struct MockCachingResolverState {}

    fn handle_operation_mock_lookup_success(
        _: &CachingResolverProcessConfig,
        _: &mut MockCachingResolverState,
        operation: CachingResolverOperation,
    ) {
        match operation {
            CachingResolverOperation::ResolveRequest(request) => {
                let address: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
                let addresses = vec![address];
                let success_result: ResolveDnsResult = Ok(addresses);
                (request.completion_callback)(success_result);
            }

            CachingResolverOperation::ResolveResult(_) => {}

            CachingResolverOperation::HostEvent(_) => {}
        }
    }

    fn new_caching_resolver_mock_lookup_success(
        config: CachingResolverConfig,
    ) -> CachingResolverHandle {
        CachingResolverHandle {
            resolver: Arc::new(CachingResolverImpl::new_default(
                config,
                handle_operation_mock_lookup_success,
            )),
        }
    }

    #[test]
    fn caching_resolver_lookup_success() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let resolver = new_caching_resolver_mock_lookup_success(
            CachingResolverConfigBuilder::new()
                .with_runtime(handle.clone())
                .build(),
        );

        let lookup_future =
            resolver.resolve(CachingResolveRequestBuilder::new("hello.world.com").build());
        let lookup_result = handle.block_on(lookup_future);

        assert!(lookup_result.is_ok());

        let addresses = lookup_result.unwrap();
        assert_eq!(1, addresses.len());

        let address = &addresses[0];
        assert_eq!("127.0.0.1", address.to_string());
    }

    fn handle_operation_mock_lookup_failure(
        _: &CachingResolverProcessConfig,
        _: &mut MockCachingResolverState,
        operation: CachingResolverOperation,
    ) {
        match operation {
            CachingResolverOperation::ResolveRequest(request) => {
                let failure_result: ResolveDnsResult =
                    Err(ResolveDnsError::new("Catastrophic Failure"));
                (request.completion_callback)(failure_result);
            }

            CachingResolverOperation::ResolveResult(_) => {}

            CachingResolverOperation::HostEvent(_) => {}
        }
    }

    fn new_caching_resolver_mock_lookup_failure(
        config: CachingResolverConfig,
    ) -> CachingResolverHandle {
        CachingResolverHandle {
            resolver: Arc::new(CachingResolverImpl::new_default(
                config,
                handle_operation_mock_lookup_failure,
            )),
        }
    }

    #[test]
    fn caching_resolver_lookup_fail() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let resolver = new_caching_resolver_mock_lookup_failure(
            CachingResolverConfigBuilder::new()
                .with_runtime(handle.clone())
                .build(),
        );

        let lookup_future =
            resolver.resolve(CachingResolveRequestBuilder::new("hello.world.com").build());
        let lookup_result = handle.block_on(lookup_future);

        assert!(lookup_result.is_err());
    }

    #[derive(Default)]
    pub(crate) struct MockHostEventCachingResolverState {
        on_host_event_fn: Option<Box<dyn FnOnce(CachingHostEvent) + Send + Sync>>,
    }

    fn handle_operation_mock_host_event(
        _: &CachingResolverProcessConfig,
        state: &mut MockHostEventCachingResolverState,
        operation: CachingResolverOperation,
    ) {
        match operation {
            CachingResolverOperation::ResolveRequest(_) => {}

            CachingResolverOperation::ResolveResult(_) => {}

            CachingResolverOperation::HostEvent(event) => {
                let completion_callback = state.on_host_event_fn.take();
                if let Some(callback) = completion_callback {
                    (callback)(event);
                }
            }
        }
    }

    fn new_caching_resolver_mock_host_event(
        config: CachingResolverConfig,
    ) -> (
        CachingResolverHandle,
        std::pin::Pin<Box<dyn Future<Output = CachingHostEvent>>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel::<CachingHostEvent>();

        let resolver_state = MockHostEventCachingResolverState {
            on_host_event_fn: Some(Box::new(move |event: CachingHostEvent| {
                let _ = tx.send(event);
            })),
        };

        let resolver = CachingResolverHandle {
            resolver: Arc::new(CachingResolverImpl::new(
                config,
                resolver_state,
                handle_operation_mock_host_event,
            )),
        };

        let event_future = async move || rx.await.unwrap();
        (resolver, Box::pin(event_future()))
    }

    #[test]
    fn caching_resolver_connection_error() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let (resolver, event_future) = new_caching_resolver_mock_host_event(
            CachingResolverConfigBuilder::new()
                .with_runtime(handle.clone())
                .build(),
        );

        let host_event = CachingHostEvent {
            name: "localhost".to_string(),
            address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            event_data: CachingHostEventInformation::ConnectionError(
                CachingHostEventConnectionError {
                    category: ConnectionErrorCategory::Fatal,
                    error: None,
                },
            ),
        };

        let notify_result = resolver.notify(host_event);
        assert!(notify_result.is_ok());

        let received_event = runtime.block_on(event_future);

        assert_eq!(IpAddr::V4(Ipv4Addr::LOCALHOST), received_event.address);
        assert_eq!("localhost", received_event.name);

        match received_event.event_data {
            CachingHostEventInformation::ConnectionError(error_event) => {
                assert_eq!(ConnectionErrorCategory::Fatal, error_event.category);
                assert!(error_event.error.is_none());
            }
            _ => {
                assert!(false);
            } // is there a better way to verify variant?
        }
    }
}
