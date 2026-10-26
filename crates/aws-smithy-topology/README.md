# aws-smithy-topology

Hardware topology and placement primitives for the AWS SDK and smithy-rs
ecosystem. Inspect available processors or choose targets for threads
independently of an async runtime, HTTP client, or AWS service client.

```rust
use aws_smithy_topology::Topology;

let topology = Topology::detect()?;
println!("Available parallelism: {}", topology.parallelism_estimate());

for processor in topology.logical_processors() {
    println!("{}: NUMA {:?}", processor.id(), processor.numa_node());
}
# Ok::<(), aws_smithy_topology::DetectError>(())
```

Discovery starts no threads and changes no thread affinity. See the crate
documentation for the resource model, placement behavior, explicit CPU binding,
and platform details.

Run the standalone inspection example from the repository root:

```sh
cargo run -p aws-smithy-topology --example discover
```
