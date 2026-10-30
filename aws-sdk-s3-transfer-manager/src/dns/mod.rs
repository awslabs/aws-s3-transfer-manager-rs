/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Module containing a caching resolver that implements the SRA specification for name resolution
pub mod sra;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::dns::{
    DnsFuture, ResolveDns, ResolveDnsError, SharedDnsResolver,
};
use std::fmt::{Debug, Formatter};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Determines what kind of addresses resolution should return.  Happy eyeballs is implemented
/// internally in hyper and so if we want to support it we need to return both ipv4 and ipv6
/// addresses (if they exist).  This makes error correlation much harder (impossible really)
/// because partial failures aren't detectable (for ex: fail to connect to ipv6 but succeed on
/// ipv4 will not trigger any detectable way of penalizing the ipv6 address).
#[derive(Debug, Default, Clone, Copy)]
pub enum NameResolutionAddressTypes {
    /// Resolution should return at least one ipv4 and ipv6 address, if they exist
    #[default]
    All,

    /// Resolution should only return ipv4 addresses
    IpV4,

    /// Resolution should only return ipv6 addresses
    IpV6,
}

/// All configuration options for a caching DNS resolver
#[derive(Clone, Debug, Default)]
pub struct ResolverConfig {
    internal_resolver: Option<SharedDnsResolver>,
    runtime: Option<tokio::runtime::Handle>,
    address_types: NameResolutionAddressTypes,
    default_ttl: Option<Duration>,
}

impl ResolverConfig {
    /// Creates a new caching resolver configuration builder instance
    pub fn builder() -> ResolverConfigBuilder {
        ResolverConfigBuilder {
            config: ResolverConfig::default(),
        }
    }
}

/// Builder type for caching resolver configuration instances
#[derive(Debug)]
pub struct ResolverConfigBuilder {
    config: ResolverConfig,
}

impl ResolverConfigBuilder {
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
    pub fn build(&self) -> ResolverConfig {
        self.config.clone()
    }
}

/// Fundamental name lookup result type based on what's in smithy-dns
pub type ResolveDnsResult = Result<Vec<IpAddr>, ResolveDnsError>;

/// Models a name resolution request.
/// Future possibilities could include TTL override, IPv4 vs IPv6 restrictions, etc...
/// This is the request type used in the public resolution API.
#[derive(Debug, Clone)]
pub struct ResolveRequest {
    name: String,
}

impl ResolveRequest {
    /// Create a new request builder instance
    pub fn builder(name: &str) -> ResolveRequestBuilder {
        ResolveRequestBuilder {
            request: ResolveRequest {
                name: name.to_string(),
            },
        }
    }

    /// the host name to perform a name resolution for
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Builder type for ResolveRequest instances
#[derive(Debug)]
pub struct ResolveRequestBuilder {
    request: ResolveRequest,
}

impl ResolveRequestBuilder {
    /// Create a new ResolveRequest instance
    pub fn build(&self) -> ResolveRequest {
        self.request.clone()
    }
}

/// Type signature for a resolution request's completion callback
pub type ResolutionCompletionFn = Box<dyn FnOnce(ResolveDnsResult) -> () + Send + Sync>;

/// Internal resolve request type.  Needs to be public so that caching resolver variants can
/// be implemented externally.
pub struct ResolveRequestInternal {
    request: ResolveRequest,

    completion_callback: ResolutionCompletionFn,
}

impl ResolveRequestInternal {
    /// The name resolution request
    pub fn request(&self) -> &ResolveRequest {
        &self.request
    }

    /// Function that will complete the resolution request's user-facing future
    pub fn completion_callback(&self) -> &ResolutionCompletionFn {
        &self.completion_callback
    }
}

impl std::fmt::Debug for ResolveRequestInternal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

/// The result of a name resolution lookup attempt
#[derive(Debug)]
pub struct ResolveResult {
    name: String,
    result: ResolveDnsResult,
}

impl ResolveResult {
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
#[non_exhaustive]
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

/// Event emitted when something goes wrong with a connection.  This can either be during
/// connection establishment or while something is using an established connection.  This event
/// is used to discourage or route away from the associated IP address.
#[derive(Debug)]
pub struct HostEventConnectionError {
    category: ConnectionErrorCategory,

    source: Option<BoxError>,
}

impl HostEventConnectionError {
    /// Creates a new builder for a connection error host event variant
    pub fn builder(category: ConnectionErrorCategory) -> HostEventConnectionErrorBuilder {
        return HostEventConnectionErrorBuilder {
            error: HostEventConnectionError {
                category,
                source: None,
            },
        };
    }

    /// How the resolver should penalize the associated address in future resolutions
    pub fn category(&self) -> &ConnectionErrorCategory {
        &self.category
    }

    /// Actual source error triggering this event
    pub fn source(&self) -> Option<&BoxError> {
        self.source.as_ref()
    }
}

/// Builder type for HostEventConnectionError instances
#[derive(Debug)]
pub struct HostEventConnectionErrorBuilder {
    error: HostEventConnectionError,
}

impl HostEventConnectionErrorBuilder {
    /// Sets the source error that caused the connection error
    pub fn with_source(&mut self, error: BoxError) -> &mut HostEventConnectionErrorBuilder {
        self.error.source = Some(error);
        self
    }

    /// (Consuming) builds a new HostEventConnectionError instance
    pub fn build(self) -> HostEventConnectionError {
        self.error
    }
}

/// Per-host-event-type variants
#[non_exhaustive]
#[derive(Debug)]
pub enum HostEventInformation {
    /// Notifies the resolver that a consumer encountered an error condition while using a
    /// connection to the address sourced from the resolver
    ConnectionError(HostEventConnectionError),
}

/// A connection-related event that could cause us to update our internal bookkeeping that
/// helps distribute IPs across connections.
#[derive(Debug)]
pub struct HostEvent {
    /// Name of the host that the address was resolver for
    name: String,

    /// IP address of the connection that encountered an error
    address: IpAddr,

    /// Per-event-type data
    event_data: HostEventInformation,
}

impl HostEvent {
    /// Creates a new builder for HostEvents
    pub fn builder(
        name: &str,
        address: IpAddr,
        event_data: HostEventInformation,
    ) -> HostEventBuilder {
        HostEventBuilder {
            event: HostEvent {
                name: name.to_string(),
                address,
                event_data,
            },
        }
    }

    /// Host name this event corresponds to
    pub fn name(&self) -> &str {
        &self.name
    }

    /// IP address this event corresponds to
    pub fn address(&self) -> IpAddr {
        self.address
    }

    /// Host event variant data
    pub fn event_data(&self) -> &HostEventInformation {
        &self.event_data
    }
}

/// Builder type for HostEvent instances
#[derive(Debug)]
pub struct HostEventBuilder {
    event: HostEvent,
}

impl HostEventBuilder {
    /// Creates a new HostEvent instance
    pub fn build(self) -> HostEvent {
        self.event
    }
}

/// The fundamental operation type that a caching resolver works with.
#[non_exhaustive]
#[derive(Debug)]
pub enum ResolverOperation {
    /// An operation that requests an IP address for a host name
    ResolveRequest(ResolveRequestInternal),

    /// An operation holding the result of looking up IP addresses for a host name
    ResolveResult(ResolveResult),

    /// An operation detailing an event that may cause the resolver to change how it
    /// distributes IP addresses to resolve requests
    HostEvent(HostEvent),
}

/// Type signature for the function that performs background task name lookup resolution
pub type ResolutionFunction = Box<dyn Fn(String) + Send + Sync>;

/// Configuration for the async recurrent task that handles all incoming ResolverOperation
/// instances.  This context is passed into the operation handler function a caching resolver
/// was created with, allowing it to do name lookups and know how long an address should stay
/// valid.
pub struct OperationHandlerContext {
    resolution_function: ResolutionFunction,

    ttl: Duration,
}

impl Debug for OperationHandlerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("OperationHandlerContext");
        formatter.field("ttl", &self.ttl);
        formatter.finish()
    }
}

impl OperationHandlerContext {
    /// function object that will actually perform lookups.  Lookups are performed on the runtime
    /// associated with the caching resolver
    pub fn resolution_function(&self) -> &ResolutionFunction {
        &self.resolution_function
    }

    /// How long a lookup stays cached; may be extended indefinitely by future lookups refreshing
    /// an individual address's ttl
    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

/// Type signature for a function (pointer) that handles resolver operations together with
/// an associated bookkeeping context (of type T)
pub type ResolverOperationHandlerFn<T> =
    fn(context: &OperationHandlerContext, &mut T, ResolverOperation) -> ();

/// Represents the centralized, recurrent task that handles ResolverOperation instances
struct ResolverProcess<T> {
    /// Operation handling context passed to the resolver's operation handler function on each
    /// incoming operation.
    context: OperationHandlerContext,

    /// input queue receiver, all communication from the outside world (including resolution task
    /// results) comes through here
    receiver: mpsc::UnboundedReceiver<ResolverOperation>,

    /// the (mutable) bookkeeping state (cache) the caching resolver uses to make resolution decisions
    state: T,
}

impl<T> ResolverProcess<T>
where
    T: Default,
{
    fn new(
        state: T,
        internal_resolver: SharedDnsResolver,
        sender: mpsc::UnboundedSender<ResolverOperation>,
        receiver: mpsc::UnboundedReceiver<ResolverOperation>,
        ttl: Duration,
    ) -> Self {
        // build the resolver function that runs resolution as a background task on the tokio
        // runtime and then takes the result and sends it back to the resolver process via
        // the operation input channel
        let resolver = internal_resolver.clone();
        let operation_channel = sender.clone();
        let resolution_function: ResolutionFunction = Box::new(move |name| {
            let handle = tokio::runtime::Handle::current();
            let internal_name = name.to_string();
            let internal_resolver = resolver.clone();
            let internal_channel = operation_channel.clone();
            handle.spawn(async move {
                let result = internal_resolver.resolve_dns(&internal_name).await;
                let caching_result = ResolverOperation::ResolveResult(ResolveResult {
                    name: internal_name,
                    result,
                });
                let _ = internal_channel.send(caching_result);
            });
        });

        ResolverProcess {
            context: OperationHandlerContext {
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
    async fn run(&mut self, operation_handler: ResolverOperationHandlerFn<T>) {
        let receiver = &mut self.receiver;
        let context = &mut self.context;
        let state = &mut self.state;

        // runs until recv returns None implying all input queue Sender clones are gone
        loop {
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

/// Unit result type for our public API(s)
pub type ResolverResult = Result<(), BoxError>;

/// Public API for the caching resolver.  Intentionally distinct from ResolveDns.
pub trait Resolver: Debug + Send + Sync {
    /// Perform a resolution request.  Equivalent to ResolveDns::resolve_dns but with a different
    /// API to allow for extensibility (additional flags, options, etc...)
    fn resolve(&self, request: ResolveRequest) -> DnsFuture<'static>;

    /// Notify the resolver of an external host-related event that may affect how it
    /// chooses to distribute IP addresses to resolution requests
    fn notify(&self, event: HostEvent) -> ResolverResult;
}

/// Internal caching resolver implementation type that serves as the entry point for
/// resolution requests
#[derive(Debug)]
pub struct ResolverImpl {
    // if we create our own runtime, we need to hold onto it here, since no one else will
    self_created_runtime: Option<tokio::runtime::Runtime>,
    operation_channel: mpsc::UnboundedSender<ResolverOperation>,
}

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);

impl ResolverImpl {
    /// Creates a new resolver implementation using the default constructor for the bookkeeping
    /// state
    pub fn new_default<T>(
        config: ResolverConfig,
        operation_handler: ResolverOperationHandlerFn<T>,
    ) -> Self
    where
        T: Default + Send + 'static,
    {
        ResolverImpl::new(config, T::default(), operation_handler)
    }

    /// Creates a new resolver implementation
    pub fn new<T>(
        config: ResolverConfig,
        state: T,
        operation_handler: ResolverOperationHandlerFn<T>,
    ) -> Self
    where
        T: Default + Send + 'static,
    {
        let (tx, rx): (
            mpsc::UnboundedSender<ResolverOperation>,
            mpsc::UnboundedReceiver<ResolverOperation>,
        ) = mpsc::unbounded_channel();

        let mut caching_resolver = ResolverImpl {
            self_created_runtime: None,
            operation_channel: tx.clone(),
        };

        // Config resolver || basic getaddrinfo resolver
        let internal_resolver = config.internal_resolver.unwrap_or(SharedDnsResolver::new(
            aws_smithy_runtime::client::dns::TokioDnsResolver::new(),
        ));
        let mut resolver_impl = ResolverProcess::<T>::new(
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

impl Resolver for ResolverImpl {
    fn resolve(&self, request: ResolveRequest) -> DnsFuture<'static> {
        let (tx, rx) = tokio::sync::oneshot::channel::<ResolveDnsResult>();
        let internal_request = ResolveRequestInternal {
            request,
            completion_callback: Box::new(move |result| {
                // if this fails, there's nothing we can do; it's likely the waiter is gone
                let _ = tx.send(result);
            }),
        };

        // kick off the resolve request
        let send_result = self
            .operation_channel
            .send(ResolverOperation::ResolveRequest(internal_request));

        DnsFuture::new(async move {
            if let Err(send_result_err) = send_result {
                return Err(ResolveDnsError::new(std::io::Error::other(send_result_err)));
            }

            let result = rx.await;

            result.unwrap_or_else(|err| Err(ResolveDnsError::new(std::io::Error::other(err))))
        })
    }

    fn notify(&self, event: HostEvent) -> ResolverResult {
        self.operation_channel
            .send(ResolverOperation::HostEvent(event))
            .map_err(|err| err.into())
    }
}

/// Clonable, sharable handle to a caching resolver.  This is the public-facing instance type for
/// all caching resolver variants.  It implements both its internal trait (CachingResolver) and the
/// public external smithy DNS trait (ResolveDns).
#[derive(Clone)]
pub struct ResolverHandle {
    resolver: Arc<dyn Resolver>,
}

impl ResolverHandle {
    /// Factory function for creating caching resolver instances
    pub fn new<T>(config: ResolverConfig, operation_handler: ResolverOperationHandlerFn<T>) -> Self
    where
        T: Default + Send + 'static,
    {
        ResolverHandle {
            resolver: Arc::new(ResolverImpl::new_default(config, operation_handler)),
        }
    }
}

impl Resolver for ResolverHandle {
    fn resolve(&self, request: ResolveRequest) -> DnsFuture<'static> {
        self.resolver.resolve(request)
    }

    fn notify(&self, event: HostEvent) -> ResolverResult {
        self.resolver.notify(event)
    }
}

impl std::fmt::Debug for ResolverHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.resolver.fmt(f)
    }
}

impl ResolveDns for ResolverHandle {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let request = ResolveRequest {
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
    pub(crate) struct EmptyResolverState {}

    fn handle_operation_mock_lookup_success(
        _: &OperationHandlerContext,
        _: &mut EmptyResolverState,
        operation: ResolverOperation,
    ) {
        match operation {
            ResolverOperation::ResolveRequest(request) => {
                let address: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
                let addresses = vec![address];
                let success_result: ResolveDnsResult = Ok(addresses);
                (request.completion_callback)(success_result);
            }

            ResolverOperation::ResolveResult(_) => {}

            ResolverOperation::HostEvent(_) => {}
        }
    }

    fn new_caching_resolver_mock_lookup_success(config: ResolverConfig) -> ResolverHandle {
        ResolverHandle {
            resolver: Arc::new(ResolverImpl::new_default(
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
            ResolverConfig::builder()
                .with_runtime(handle.clone())
                .build(),
        );

        let lookup_future = resolver.resolve(ResolveRequest::builder("hello.world.com").build());
        let lookup_result = handle.block_on(lookup_future);

        assert!(lookup_result.is_ok());

        let addresses = lookup_result.unwrap();
        assert_eq!(1, addresses.len());

        let address = &addresses[0];
        assert_eq!("127.0.0.1", address.to_string());
    }

    fn handle_operation_mock_lookup_failure(
        _: &OperationHandlerContext,
        _: &mut EmptyResolverState,
        operation: ResolverOperation,
    ) {
        match operation {
            ResolverOperation::ResolveRequest(request) => {
                let failure_result: ResolveDnsResult =
                    Err(ResolveDnsError::new("Catastrophic Failure"));
                (request.completion_callback)(failure_result);
            }

            ResolverOperation::ResolveResult(_) => {}

            ResolverOperation::HostEvent(_) => {}
        }
    }

    fn new_caching_resolver_mock_lookup_failure(config: ResolverConfig) -> ResolverHandle {
        ResolverHandle {
            resolver: Arc::new(ResolverImpl::new_default(
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
            ResolverConfig::builder()
                .with_runtime(handle.clone())
                .build(),
        );

        let lookup_future = resolver.resolve(ResolveRequest::builder("hello.world.com").build());
        let lookup_result = handle.block_on(lookup_future);

        assert!(lookup_result.is_err());
    }

    #[derive(Default)]
    pub(crate) struct MockHostEventCachingResolverState {
        on_host_event_fn: Option<Box<dyn FnOnce(HostEvent) + Send + Sync>>,
    }

    fn handle_operation_mock_host_event(
        _: &OperationHandlerContext,
        state: &mut MockHostEventCachingResolverState,
        operation: ResolverOperation,
    ) {
        match operation {
            ResolverOperation::ResolveRequest(_) => {}

            ResolverOperation::ResolveResult(_) => {}

            ResolverOperation::HostEvent(event) => {
                let completion_callback = state.on_host_event_fn.take();
                if let Some(callback) = completion_callback {
                    (callback)(event);
                }
            }
        }
    }

    fn new_caching_resolver_mock_host_event(
        config: ResolverConfig,
    ) -> (
        ResolverHandle,
        std::pin::Pin<Box<dyn Future<Output = HostEvent>>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel::<HostEvent>();

        let resolver_state = MockHostEventCachingResolverState {
            on_host_event_fn: Some(Box::new(move |event: HostEvent| {
                let _ = tx.send(event);
            })),
        };

        let resolver = ResolverHandle {
            resolver: Arc::new(ResolverImpl::new(
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
            ResolverConfig::builder()
                .with_runtime(handle.clone())
                .build(),
        );

        let connection_error =
            HostEventConnectionError::builder(ConnectionErrorCategory::Fatal).build();
        let host_event = HostEvent::builder(
            "localhost",
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            HostEventInformation::ConnectionError(connection_error),
        )
        .build();

        let notify_result = resolver.notify(host_event);
        assert!(notify_result.is_ok());

        let received_event = runtime.block_on(event_future);

        assert_eq!(IpAddr::V4(Ipv4Addr::LOCALHOST), received_event.address);
        assert_eq!("localhost", received_event.name);

        match received_event.event_data {
            HostEventInformation::ConnectionError(error_event) => {
                assert_eq!(ConnectionErrorCategory::Fatal, error_event.category);
                assert!(error_event.source().is_none());
            }
            _ => {
                assert!(false);
            } // is there a better way to verify variant?
        }
    }
}
