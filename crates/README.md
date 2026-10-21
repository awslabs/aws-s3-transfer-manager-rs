# Managed execution and HTTP transport

These Rust components provide hardware topology, managed execution, and HTTP
transport for the AWS SDK and smithy-rs ecosystem. They share a repository with
S3 Transfer Manager and have their own versions.

| Package | Boundary | Component dependencies |
| --- | --- | --- |
| [`aws-smithy-topology`](aws-smithy-topology/) | Hardware discovery and placement resources | None |
| [`aws-smithy-execution`](aws-smithy-execution/) | Managed thread ownership and execution partitions | `aws-smithy-topology` |
| [`aws-smithy-managed-http`](aws-smithy-managed-http/) | Connection-pool composition with managed execution | `aws-smithy-execution` |
| [`aws-sdk-s3-transfer-manager-ext`](aws-sdk-s3-transfer-manager-ext/) | Optional TM integrations: runtime adoption and Smithy HTTP composition | TM, `aws-smithy-execution`, `aws-smithy-managed-http` |

`aws-smithy-execution` re-exports the topology crate as `topology`. The component
dependency chain points from managed HTTP to execution to topology. Transfer
scheduling, S3 configuration, and file-I/O policy belong to S3 Transfer Manager.
