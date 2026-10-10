# AWS ORT

AWS ORT (Oxidized Runtime) is a family of foundational Rust components for
hardware topology, managed execution, and HTTP transport. These Cargo packages
share a repository with S3 Transfer Manager and have their own versions.

| Package | Boundary | ORT dependency |
| --- | --- | --- |
| [`aws-ort-topology`](aws-ort-topology/) | Hardware discovery and placement resources | None |
| [`aws-ort-execution`](aws-ort-execution/) | Worker ownership and execution partitions | `aws-ort-topology` |
| [`aws-ort-http`](aws-ort-http/) | Connection pooling, HTTP transport, and adapter views | `aws-ort-execution` |

`aws-ort-execution` re-exports the topology crate as `topology`. The component
dependency chain points from HTTP to execution to topology. Transfer scheduling,
S3 configuration, and file-I/O policy belong to S3 Transfer Manager.
