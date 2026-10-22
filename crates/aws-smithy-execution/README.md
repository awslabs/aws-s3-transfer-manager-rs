# aws-smithy-execution

Managed async execution with topology placement. Create a runtime, retain its
owner, and submit work to a chosen partition independently of HTTP transport or
AWS service clients.

```rust
use aws_smithy_execution::{topology::Topology, ManagedRuntime};

let runtime = ManagedRuntime::builder()
    .topology(Topology::detect()?.with_max_placements(8))
    .build()?;

let execution = runtime.handle();
let task = execution.partitions()[0].spawn(async { 42 });
# let caller = tokio::runtime::Builder::new_current_thread().build()?;
# assert_eq!(caller.block_on(task)?, 42);
# caller.block_on(runtime.shutdown())?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

See the crate documentation for thread ownership, CPU binding, and Tokio
integration contracts.

Call `runtime.shutdown().await` to wait for thread exit. Dropping the owner
requests shutdown without waiting.

Run the standalone example with:

```sh
cargo run -p aws-smithy-execution --example threads
```

Enable the optional `dial9` feature to attach a caller-owned recorder with
`ManagedRuntime::builder().dial9_handle(recorder.handle().clone())`.
Await runtime shutdown before shutting down the recorder. See the builder's
crate documentation for tracing behavior and recorder ownership.
