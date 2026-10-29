# Hardware topology and managed execution

These Rust components provide hardware topology and execution resources for the
AWS SDK and smithy-rs ecosystem. They share a repository with S3 Transfer Manager
and have their own versions.

| Package | Boundary | Component dependencies |
| --- | --- | --- |
| [`aws-smithy-topology`](aws-smithy-topology/) | Hardware discovery and placement resources | None |
| [`aws-smithy-execution`](aws-smithy-execution/) | Managed thread ownership and execution partitions | `aws-smithy-topology` |
| [`aws-smithy-managed-http`](aws-smithy-managed-http/) | Native connection-pool composition with managed execution | `aws-smithy-execution` |

`aws-smithy-execution` re-exports topology as `topology`. Transfer scheduling,
S3 configuration, and file-I/O policy belong to S3 Transfer Manager.
