# Execution and HTTP ownership

## Execution selection

`RuntimeConfig` selects owned managed execution or the caller's Tokio runtime.
The default `Managed` configuration discovers topology at client construction,
builds `aws-smithy-execution::ManagedRuntime`, and disables thread pinning. The
execution runtime has one partition per selected placement and one Tokio
current-thread runtime per partition. Each partition permits one blocking-pool
thread.

`RuntimeConfig::multi_thread_tokio()` submits work through the Tokio runtime
performing dispatch. It creates no managed runtime owner. Concurrent transfer
execution requires a multi-thread Tokio runtime.

The scheduler retains admission, prioritization, cancellation, and completion
accounting. Managed dispatch selects execution partitions using the existing
two-choice load routing and submission queue. The reusable execution crate owns
threads and task submission; it does not implement transfer scheduling.

## Runtime ownership

`ManagedExecution` owns the `ManagedRuntime` behind a mutex. Cloneable execution
handles and partition handles retain capabilities without retaining shutdown
ownership. Active transfers retain the TM client handle and therefore its
execution owner.

With `__unstable_ext`, the extension's `managed(runtime)` transfers the concrete
owner into opaque `Managed` configuration. Client construction consumes that
owner once, preserving runtime identity, topology, CPU bindings, thread names,
and instrumentation. A repeated attempt to take it is a contract violation and
panics. Ordinary public configuration exposes no execution crate types.

Dropping unused adopted configuration or the final client owner requests runtime
shutdown without synchronously joining managed threads. Pending async tasks are
cancelled when the partition's Tokio runtime exits. Running blocking tasks and
async tasks that do not yield can delay thread exit.

`Client::shutdown_completion` returns a non-owning observer for automatic and
adopted managed execution, and `None` for caller-owned Tokio execution. Retaining
it keeps neither the client nor runtime owner alive. Waiting does not request
shutdown, cancel transfers, or close independently retained HTTP clients.
Release client clones and transfer ownership before waiting. Success includes
joins of every managed thread, runtime destruction, and managed-thread local
cleanup. Clones and repeated waits observe the same result.

The standalone execution owner provides consuming asynchronous shutdown and a
separate `ShutdownCompletion` observer. Owner destruction publishes join handles
without blocking or starting a joining thread. The first external waiter after
owner release starts the joining thread; cancelling that waiter does not stop
joins already started. Waits from managed or blocking-pool threads owned by the
runtime are rejected. A managed-thread panic is reported after all threads have
been joined. Other failures can prevent confirmation of thread completion.

Adopting an owner transfers shutdown authority to TM while retained completion
observers remain usable. Configuring a separate
TM Dial9 recorder for an already built adopted runtime causes client construction
to panic;
its recorder must be attached during execution construction.

## Resource sizing

`runtime::sizing::resolve` turns the signals available at client construction
into the payload-memory ceiling, the concurrency target, the part size, and the
per-host connection cap. It is policy over the facts `runtime::platform`
detects and performs no detection itself. Resolution is one pass in a fixed
order, each step reading only quantities already resolved, so every result is
closed-form and the function is re-callable on new signals.

Its inputs are a throughput target, usable memory, usable vCPUs, the
descriptor-derived connection cap, and explicit configuration. Throughput comes
from `ConcurrencyMode::TargetThroughput` when set and otherwise from the detected
instance family's bandwidth estimate; a value that is not finite or not positive
is treated as unset. Machine facts are discovery-only and have no public
override. Explicitly configured memory, concurrency, and part size are
authoritative and are never narrowed: a value the host cannot fund is reported
rather than rejected, because memory admission and connection acquisition still
bound execution, so an unfundable value costs backpressure rather than
correctness.

The pool ceiling is a safety bound against exhausting host memory and nothing
more. Demand never lowers it. This holds because the pool commits no memory at
construction and reclaims toward a fraction of its configured blocks once the
scheduler is globally idle, so an unused ceiling is not resource use. Throughput
and concurrency set the operating point beneath it. The automatic ceiling uses
the pool's own `MemoryBudgetConfig::Auto` share, cap, and fallback, plus one
addition: a floor that lets a small host fund the minimum concurrency target at
the default part size. The floor never exceeds half of detected memory, so on a
host smaller than twice the floor it yields rather than admit more than the host
holds.

Where one signal is the binding constraint, the remaining quantities are sized
to the performance achievable under it, so the client does not reserve
capability it cannot use. Both the connection cap and the part demand the
ceiling funds therefore lower a derived concurrency target; a configured target
is exempt. Part size is an input to that pass rather than an output — the
configured value when set, and otherwise one default shared by uploads and
downloads. The pass also reports the largest concurrency the ceiling funds, so a
later adaptive layer reads its own upper bound instead of discovering it through
admission stalls.

The connection cap is the descriptor-derived cap, not narrowed to the
concurrency target. It is a bound that keeps the process within its descriptor
limit, and connections open lazily, so headroom above the target costs nothing
until used. That headroom keeps connections local to the partition that opened
them instead of borrowed or reclaimed across partitions at the cap, and leaves
room for requests outside the scheduler, such as listing and S3 Express session
creation. The descriptor budget is the smaller of a fraction of the soft limit
and that limit less a fixed reserve, because the descriptors a process holds
besides its connections are closer to a fixed count than to a share of the
limit. A floor applies underneath both, so a soft limit below the reserve still
yields a cap the client can attempt a transfer with.

Sizing resolves before the managed execution owner is constructed, so an adopted
runtime's own topology is not visible to it and the vCPU count comes from the
detected machine profile. This affects only the fallback path taken when no
throughput is available and the instance family is unrecognized.

Construction emits one record naming every resolved quantity, the signal the
concurrency target was derived from, and the signal that bounded it, followed by
a warning for each resource a signal held below its target. Resolved sizing is
observable only through that record; it is not reported on `Client::metrics`.

## HTTP precedence and partitions

`Config::runtime_http` supplies default transport options only for managed
execution when the S3 configuration has no HTTP client. A configured client is
preserved under either execution choice. `S3ClientConfig` captures the SDK
configuration before runtime construction so that precedence is decided once.
Shared `SdkConfig` is an ordinary input; S3-specific SDK builder conversion
requires `sdk-v1`.

TM's native pool construction retains its TLS, DNS, proxy, connection-limit,
reuse, and interface configuration. Native partition IDs match execution
partition indices, and each driver spawner submits to that execution partition.
Interface assignment follows configuration order round-robin; CPU/NUMA topology
does not derive NIC locality. `ManagedHttpPool::from_pool` resolves each native
client and supplies the shared Smithy HTTP view without owning runtime shutdown.

Managed HTTP selects the current execution partition when it belongs to the
same runtime, or a random partition for outside and foreign-runtime callers.
Native connection reuse can borrow peer connections. Client selection does not
guarantee request-body polling on the selected partition. Explicit native client
lookup remains available independently of aggregate routing.

## File-I/O placement

`RuntimeComponents::direct_io` controls reads and writes performed by transfer
tasks. Managed tasks perform those operations directly; caller-runtime tasks
use blocking offload.

`RuntimeComponents::direct_body_io` controls file-backed single-request upload
bodies separately. It is enabled only when TM constructs its managed transport.
Configured HTTP controls its polling threads, so its file bodies always offload
positioned reads through the transfer request's Tokio runtime and its blocking
pool. That runtime is captured before HTTP handoff and retained across retries;
body polling requires no Tokio context. This also applies when the configured
pool and adopted execution share a runtime. TM-built transport retains direct
reads on the managed polling thread, including for borrowed connections.

Both file-body paths preserve the same open file identity, memory pool,
chunk bounds, cancellation behavior, and retry reconstruction. Transport choice
does not alter pool accounting or immutable byte ownership.

## wasm targets

wasm targets cannot run managed threads or a blocking pool: wasip1 and wasip2
cannot spawn threads, and wasip3's cooperative threads run one at a time.
`Managed` configuration therefore runs on the caller's Tokio runtime, as
`RuntimeConfig::multi_thread_tokio()` does, and a current-thread runtime is
supported. No managed owner exists, so `Client::shutdown_completion` returns
`None`, and `__unstable_ext` does not compile.

Neither usable memory nor the descriptor limit is detectable on wasm, so the
pool ceiling comes from the no-detection fallback and the connection cap is
immaterial because TM builds no transport.

TM builds no transport; the S3 configuration must supply HTTP. Transfer tasks
and file-backed request bodies both read and write directly on the polling
thread, because offloading cannot move the work elsewhere.
