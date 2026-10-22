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
async tasks that do not yield can delay thread exit. Scheduler descriptors
retired through epoch reclamation may retain client ownership until reclamation.

The standalone execution owner provides asynchronous shutdown that waits for
thread exit. Adopting it transfers that ownership to TM. Configuring a separate
TM Dial9 recorder for an already built adopted runtime causes client construction
to panic;
its recorder must be attached during execution construction.

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
positioned reads to the polling Tokio runtime's blocking pool, including when
the configured pool and adopted execution share a runtime.

Both file-body paths preserve the same open file identity, memory pool,
chunk bounds, cancellation behavior, and retry reconstruction. Transport choice
does not alter pool accounting or immutable byte ownership.
