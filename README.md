# Transfer Manager for Amazon S3

Transfer Manager for Amazon S3 is a high-performance data plane client for uploading and downloading
objects and directories between your application and Amazon S3. It splits large uploads and downloads
into concurrent part requests, retries failed parts individually, and transfers whole directories in
one call.

## Packages

| Package | Description |
| --- | --- |
| [`aws-s3-transfer`](crates/aws-s3-transfer/) | The transfer manager. [Getting started](crates/aws-s3-transfer/README.md), [API reference](https://docs.rs/aws-s3-transfer). |
| [Runtime components](crates/README.md) | Hardware topology, managed execution and HTTP transport, versioned independently. |

## Getting help

- [GitHub discussions](https://github.com/awslabs/aws-s3-transfer-manager-rs/discussions): ideas, RFCs and general questions
- [GitHub issues](https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/new/choose): bug reports and feature requests

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md). [Maintainer documentation](docs/dev/README.md) covers the
architecture, standards and release process; [development.md](docs/dev/development.md) covers building,
testing, the `cp` example and profiling.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
