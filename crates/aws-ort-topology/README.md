# aws-ort-topology

Hardware topology and placement primitives for the AWS SDK and smithy-rs
ecosystem. Inspect available processors or choose targets for worker threads
independently of an async runtime, HTTP client, or AWS service client.

```rust
use aws_ort_topology::Topology;

let topology = Topology::detect()?;
println!("Available parallelism: {}", topology.parallelism_estimate());

for processor in topology.logical_processors() {
    println!("{}: NUMA {:?}", processor.id(), processor.numa_node());
}
# Ok::<(), aws_ort_topology::DetectError>(())
```

Discovery starts no workers and changes no thread affinity. See the crate
documentation for the resource model, placement behavior, explicit CPU binding,
and platform details.

Run the standalone inspection example from the repository root:

```sh
cargo run -p aws-ort-topology --example discover
```
