/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Module for a caching resolver that implements the SRA specification for name resolution
pub mod sra;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError, SharedDnsResolver};
use tokio::sync::mpsc;
use std::net::IpAddr;

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

The current proof-of-concept does not perform any of this shared processing.  It just sketches
the communication channels and rough public API type contracts.  Instead, when a resolution request
is received internally, a dedicated lookup is performed as an async task and the result applied to
complete the public API future.

Eventually we should be able to genericize the async processing function and associated shared state
so that we can create caching resolver variants that compose (and compare) different
behaviors.  This will allow us to explore if resolution policies are helping in adverse scenarios.

 */

/// All configuration options for a caching DNS resolver
#[derive(Clone, Debug)]
pub struct CachingResolverConfig {
    internal_resolver: Option<SharedDnsResolver>,
    runtime: Option<tokio::runtime::Handle>
}

/// Builder type for caching resolver configuration instances
#[derive(Debug)]
pub struct CachingResolverConfigBuilder {
    config: CachingResolverConfig
}

impl CachingResolverConfigBuilder {

    /// Creates a new caching resolver configuration builder instance
    pub fn new() -> Self {
        Self {
            config: CachingResolverConfig {
                internal_resolver: None,
                runtime: None
            }
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
    request: CachingResolveRequest
}

impl CachingResolveRequestBuilder {

    /// Create a new builder instance
    pub fn new(name : &str) -> Self {
        CachingResolveRequestBuilder {
            request: CachingResolveRequest {
                name : name.to_string()
            }
        }
    }

    /// Create a new CachingResolveRequest instance
    pub fn build(&self) -> CachingResolveRequest {
        self.request.clone()
    }
}

/// Internal resolve request type.  Needs to be public so that caching resolver variants can
/// be implemented by others.
pub struct CachingResolveRequestInternal {

    request: CachingResolveRequest,

    result_target: tokio::sync::oneshot::Sender<ResolveDnsResult>
}

impl CachingResolveRequestInternal {

    /// The name resolution request
    pub fn request(&self) -> &CachingResolveRequest {
        &self.request
    }

    /// Channel that completes the user-facing resolution request's future
    pub fn result_target(&self) -> &tokio::sync::oneshot::Sender<ResolveDnsResult> {
        &self.result_target
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
    result: ResolveDnsResult
}

impl CachingResolveResult {

    /// Create a new CachingResolveResult instance
    pub fn new(name: &str, result : ResolveDnsResult) -> Self {
        CachingResolveResult {
            name: name.to_string(),
            result
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

/// A connection-related event that could cause us to update our internal bookkeeping that
/// helps distribute IPs across connections.
///
/// IPReleased, connection(IP) failed, etc...
#[derive(Debug)]
pub struct CachingHostEvent {
    // TBD
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
    HostEvent(CachingHostEvent)
}

/// Configuration for the async recurrent task that handles all incoming CachingResolverOperation
/// instances
#[derive(Debug)]
pub struct CachingResolverProcessConfig {

    /// resolver that will actually perform lookups.  Lookups are performed on the runtime
    /// associated with the caching resolver
    internal_resolver: SharedDnsResolver,

    /// handle to our input queue sender; needed so that resolution tasks can submit their results
    /// as a CachingResolverOperation::ResolveResult
    sender: mpsc::UnboundedSender<CachingResolverOperation>,

    /// input queue receiver, all communication from the outside world (including resolution task
    /// results) comes through here
    receiver: mpsc::UnboundedReceiver<CachingResolverOperation>,
}

/// Represents the centralized, recurrent task that handles CachingResolverOperation instances
struct CachingResolverProcess<T> {

    /// (semi-)immutable configuration
    config: CachingResolverProcessConfig,

    /// the (mutable) bookkeeping state (cache) the caching resolver uses to make resolution decisions
    state: T,
}

impl <T> CachingResolverProcess<T> where T : Default {

    /// Instantiate a new caching resolver
    fn new(internal_resolver: SharedDnsResolver,
           sender: mpsc::UnboundedSender<CachingResolverOperation>,
           receiver: mpsc::UnboundedReceiver<CachingResolverOperation>) -> Self {
        CachingResolverProcess {
            config: CachingResolverProcessConfig {
                internal_resolver,
                sender,
                receiver
            },
            state: Default::default(),
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
    async fn run(&mut self, operation_handler: fn (&CachingResolverProcessConfig, &mut T, CachingResolverOperation) -> ()) {
        let config = &mut self.config;
        let state = &mut self.state;

        // runs until recv returns None implying all input queue Sender clones are gone
        loop {
            // eventually we may select on time too in order to support TTL expiration
            tokio::select! {
                operation_result = config.receiver.recv() => {
                    match operation_result {
                        Some(operation) => {
                            (operation_handler)(config, state, operation);
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

/// Public API for the caching resolver.  Intentionally distinct from ResolveDns.
pub trait CachingResolver : Debug + Send + Sync {

    /// Perform a resolution request.  Equivalent to ResolveDns::resolve_dns but with a different
    /// API to allow for extensibility (additional flags, options, etc...)
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static>;
}

/// Internal caching resolver implementation type that serves as the entry point for
/// resolution requests
#[derive(Debug)]
struct CachingResolverImpl {
    self_created_runtime: Option<tokio::runtime::Runtime>,
    operation_channel: mpsc::UnboundedSender<CachingResolverOperation>
}

impl CachingResolverImpl {

    /// Combines configuration and an operation handler function to create a new caching resolver
    /// instance.
    fn new<T>(config: CachingResolverConfig, operation_handler: fn (&CachingResolverProcessConfig, &mut T, CachingResolverOperation) -> ()) -> Self where T : Default + Send + 'static {
        let (tx, rx) : (mpsc::UnboundedSender<CachingResolverOperation>, mpsc::UnboundedReceiver<CachingResolverOperation>) = mpsc::unbounded_channel();

        let mut caching_resolver = CachingResolverImpl {
            self_created_runtime : None,
            operation_channel: tx.clone()
        };

        // Config resolver || basic getaddrinfo resolver
        let internal_resolver = config.internal_resolver.unwrap_or(SharedDnsResolver::new(aws_smithy_runtime::client::dns::TokioDnsResolver::new()));
        let mut resolver_impl = CachingResolverProcess::<T>::new(internal_resolver, tx, rx);

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
            result_target: tx
        };

        let send_result = self.operation_channel.send(CachingResolverOperation::ResolveRequest(internal_request));

        DnsFuture::new(async move {
            if let Err(send_result_err) = send_result {
                return Err(ResolveDnsError::new(std::io::Error::other(send_result_err)));
            }

            let result = rx.await;

            result.unwrap_or_else(|err| Err(ResolveDnsError::new(std::io::Error::other(err))))
        })
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
    pub fn new<T>(config: CachingResolverConfig, operation_handler: fn (&CachingResolverProcessConfig, &mut T, CachingResolverOperation) -> ()) -> Self where T : Default + Send + 'static {
        CachingResolverHandle {
            resolver: Arc::new(CachingResolverImpl::new(config, operation_handler))
        }
    }
}

impl CachingResolver for CachingResolverHandle {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static>{
        self.resolver.resolve(request)
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


