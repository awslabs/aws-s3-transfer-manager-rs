# Hardware topology and managed execution

These Rust components provide hardware topology and execution resources for the
AWS SDK and smithy-rs ecosystem. They share a repository with Transfer Manager
for Amazon S3 and have their own versions. This directory also holds the transfer
manager, [`aws-s3-transfer`](aws-s3-transfer/), and the mock S3 server its tests run
against, [`s3-mock-server`](s3-mock-server/).

| Package | Boundary | Component dependencies |
| --- | --- | --- |
| [`aws-smithy-topology`](aws-smithy-topology/) | Hardware discovery and placement resources | None |
| [`aws-smithy-execution`](aws-smithy-execution/) | Managed thread ownership and execution partitions | `aws-smithy-topology` |
| [`aws-smithy-managed-http`](aws-smithy-managed-http/) | Native connection-pool composition with managed execution | `aws-smithy-execution` |
| [`aws-s3-transfer-ext`](aws-s3-transfer-ext/) | Experimental transfer of managed runtime ownership into the transfer manager | `aws-s3-transfer`, `aws-smithy-execution` |

`aws-smithy-execution` re-exports topology as `topology`. Transfer scheduling,
S3 configuration, and file-I/O policy belong to the transfer manager.
