/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::fmt::Formatter;
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
    /// than tokio.  The recurrent async processing task requires tokio (mpsc, one-shot, select),
    /// while the name resolution lookups could use any kind of runtime.
    pub fn with_runtime(&mut self, runtime: tokio::runtime::Handle) -> &Self {
        self.config.runtime = Some(runtime);
        self
    }

    /// Creates a new configuration instance for caching resolvers
    pub fn build(&self) -> CachingResolverConfig {
        self.config.clone()
    }
}

/// Models a name resolution request.  Eventually convert to a builder pattern for future expansion.
/// Future possibilities could include TTL override, IPv4 vs IPv6 restrictions, etc...
pub struct CachingResolveRequest {

    /// the host name to perform a name resolution for
    pub name: String,
}

/// Why doesn't this exist in smithy dns?
pub type ResolveDnsResult = Result<Vec<IpAddr>, ResolveDnsError>;

struct CachingResolveRequestInternal {
    request: CachingResolveRequest,

    /// this is how we complete the user-facing resolution request future
    result_target: tokio::sync::oneshot::Sender<ResolveDnsResult>
}

/// The result of a name resolution lookup attempt
pub struct CachingResolveResult {
    name: String,
    result: ResolveDnsResult
}

/// A connection-related event that could cause us to update our internal bookkeeping that
/// helps distribute IPs across connections.
///
/// IPReleased, connection(IP) failed, etc...
pub struct CachingHostEvent {
    // TBD
}

/// The fundamental operation type that the caching resolver works with.  Operations are submitted
/// via mpsc.
#[non_exhaustive]
pub enum CachingResolverOperation {
    ResolveRequest(CachingResolveRequestInternal),
    ResolveResult(CachingResolveResult),
    HostEvent(CachingHostEvent)
}

struct CachingResolverProcess<T> {

    /// resolver that will actually perform lookups.  Lookups are performed on the runtime
    /// associated with the caching resolver
    internal_resolver: SharedDnsResolver,

    /// handle to our input queue sender; needed so that resolution tasks can submit their results
    /// as a CachingResolverOperation::ResolveResult
    sender: mpsc::UnboundedSender<CachingResolverOperation>,

    /// input queue receiver, all communication from the outside world (including resolution task
    /// results) comes through here
    receiver: mpsc::UnboundedReceiver<CachingResolverOperation>,

    state: T,
}

//     operation_handler: Box<dyn Fn(&mut CachingResolverProcess<T>, CachingResolverOperation)>, // should probably return a result

impl <T> CachingResolverProcess<T> where T : Default {

    /// Instantiate a new caching resolver that follows the SRA specification
    fn new(internal_resolver: SharedDnsResolver,
           sender: mpsc::UnboundedSender<CachingResolverOperation>,
           receiver: mpsc::UnboundedReceiver<CachingResolverOperation>) -> Self {
        CachingResolverProcess {
            internal_resolver,
            sender,
            receiver,
            state: Default::default(),
        }
    }



    // is it a problem to run this function on the same runtime that the internal resolves are being
    // spawned on?  The worry is that long-running timeouts on blocking-style resolutions (like
    // getaddrinfo) could choke this task's ability to distribute IPs to requests for other names.
    // that aren't having a problem.  Name lookups can take very different amounts of time depending
    // on whether they hit a local system cache or must go out on the network.
    async fn run(&mut self, operation_handler: Box<dyn Fn(&mut CachingResolverProcess<T>, CachingResolverOperation)>) {
        // runs until recv returns None implying all input queue Sender clones are gone
        loop {
            // eventually we may select on time too in order to support TTL expiration
            tokio::select! {
                operation_result = self.receiver.recv() => {
                    match operation_result {
                        Some(operation) => {
                            (operation_handler)(&mut *self, operation);
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

struct SraCachingResolverState {

}

fn handle_operation_sra(sra_impl: &mut CachingResolverProcess<SraCachingResolverState>, operation: CachingResolverOperation) {
    match operation {
        CachingResolverOperation::ResolveRequest(request) => {
            // someone's asking for name resolution
            //
            // for proof-of-concept, just submit the resolve request as a task on the runtime
            // this means we bypass submitting the resolution result and instead just
            // complete the future directly in the task, which isn't what we'll do in the
            // real logic
            let handle = tokio::runtime::Handle::current();
            let internal_resolver = sra_impl.internal_resolver.clone();
            let name = request.request.name;
            let tx = request.result_target;
            handle.spawn(
                async move {
                    let resolve_result = internal_resolver.resolve_dns(&name).await;
                    tx.send(resolve_result);
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

struct CachingResolverImpl {
    self_created_runtime: Option<tokio::runtime::Runtime>,
    channel: mpsc::UnboundedSender<CachingResolverOperation>
}

impl CachingResolverImpl {
    pub fn new<T>(config: CachingResolverConfig, operation_handler: Box<dyn Fn(&mut CachingResolverProcess<T>, CachingResolverOperation)>) -> Self where T : Default {
        let (tx, rx) : (mpsc::UnboundedSender<CachingResolverOperation>, mpsc::UnboundedReceiver<CachingResolverOperation>) = mpsc::unbounded_channel();

        let mut caching_resolver = CachingResolverImpl {
            self_created_runtime : None,
            channel: tx.clone()
        };

        // Config resolver || basic getaddrinfo resolver
        let internal_resolver = config.internal_resolver.unwrap_or(SharedDnsResolver::new(aws_smithy_runtime::client::dns::TokioDnsResolver::new()));
        let mut resolver_impl : CachingResolverImpl<T> = CachingResolverImpl::new<T>(internal_resolver, tx, rx, operation_handler);

        // Config runtime || create a new runtime
        if let Some(config_runtime) = config.runtime {
            config_runtime.spawn(async move {
                resolver_impl.run().await;
            });
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("sra-caching-resolver-fallback")
                .build()
                .unwrap();

            runtime.spawn(async move {
                resolver_impl.run().await;
            });

            // need to keep this alive, a handle is insufficient
            caching_resolver.self_created_runtime = Some(runtime);
        }

        caching_resolver
    }
}

pub(crate) trait CachingResolver : Send + Sync {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static>;
}

#[derive(Clone)]
pub struct CachingResolverHandle {
    resolver: Arc<dyn CachingResolver>,
}

impl CachingResolver for CachingResolverHandle {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static>{
        self.resolver.resolve(request)
    }
}

impl CachingResolver for SraCachingResolver {
    fn resolve(&self, request: CachingResolveRequest) -> DnsFuture<'static> {
        let (tx, rx) = tokio::sync::oneshot::channel::<ResolveDnsResult>();
        let internal_request = CachingResolveRequestInternal {
            request,
            result_target: tx
        };

        let send_result = self.channel.send(CachingResolverOperation::ResolveRequest(internal_request));

        DnsFuture::new(async move {
            if let Err(send_result_err) = send_result {
                return Err(ResolveDnsError::new(std::io::Error::other(send_result_err)));
            }

            let result = rx.await;

            result.unwrap_or_else(|err| Err(ResolveDnsError::new(std::io::Error::other(err))))
        })
    }
}

impl std::fmt::Debug for CachingResolverHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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

pub fn new_sra_caching_resolver(config: CachingResolverConfig) -> CachingResolverHandle {
    CachingResolverHandle {
        resolver: Arc::new(SraCachingResolver::new(config))
    }
}


