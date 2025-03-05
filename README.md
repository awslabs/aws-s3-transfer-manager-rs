# AWS S3 Transfer Manager

A high performance Amazon S3 client for Rust.

## :warning: Developer Preview

This library is currently in developer preview and is **NOT** recommended for production environments.

It is meant for early access and feedback purposes at this time. We'd love to hear from you on use cases, feature prioritization, and API feedback.

See the AWS SDK and Tools [maintenance policy](https://docs.aws.amazon.com/sdkref/latest/guide/maint-policy.html#version-life-cycle)
descriptions for more information.

## Development

**Run all tests**

```sh
cargo test --all-features
```

**Run individual test**

```sh
cargo test --lib download::worker::tests::test_distribute_work
```

### Examples

NOTE: You can use the `profiling` profile from `.cargo/config.toml` to enable release with debug info for any example.

**Copy**

See all options:

```sh
cargo run --example cp -- -h
```

**Download a file from S3**

```sh
AWS_PROFILE=<profile-name> RUST_LOG=trace cargo run --example cp s3://<my-bucket>/<my-key> /local/path/<filename>
```

NOTE: To run in release mode add `--release/-r` to the command, see `cargo run -h` .
NOTE: `trace` may be too verbose, you can see just this library's logs with `RUST_LOG=aws_sdk_s3_transfer_manager=trace`

**Upload a file to S3**

```sh
AWS_PROFILE=<profile-name> RUST_LOG=trace cargo run --example cp /local/path/<filename> s3://<my-bucket>/<my-key>
```

NOTE: To run in release mode add `--release/-r` to the command, see `cargo run -h` .
NOTE: `trace` may be too verbose, you can see just this library's logs with `RUST_LOG=aws_sdk_s3_transfer_manager=trace`

#### Flamegraphs

See [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph) for more prerequisites and installation information.

Generate a flamegraph (default is to output to `flamegraph.svg` ):

```sh
sudo AWS_PROFILE=<profile-name> RUST_LOG=aws_sdk_s3_transfer_manager=info cargo flamegraph --profile profiling --example cp -- s3://test-sdk-rust-aaron/mb-128.dat /tmp/mb-128.dat
```

#### Using tokio-console

By default examples use `tracing` crate for logs. You can pass the `--tokio-console` flag to examples to
use [ `console-subscriber` ](https://crates.io/crates/console-subscriber) instead. This allows you to run them with
[tokio-console](https://github.com/tokio-rs/console) to help debug task execution.

NOTE: This requires you build the examples with `RUSTFLAGS="--cfg tokio_unstable"` or setting the equivalent in
your cargo config.

```sh
RUSTFLAGS="--cfg tokio_unstable" AWS_PROFILE=<profile-name> RUST_LOG=debug cargo run --example cp --tokio-console ...
```

Follow installation instructions for [tokio-console](https://github.com/tokio-rs/console) and then run the
example with `tokio-console` running.

#### End to end tests

End-to-end tests are available in tests/e2e_transfer_test.rs to validate functionality against a real S3 server.
To run these tests:

1. Enable end-to-end testing by setting `RUSTFLAGS="--cfg e2e_test"`
2. Set up a test bucket by following the aws-c-s3 test helper instructions [here](https://github.com/awslabs/aws-c-s3/blob/main/tests/test_helper/README.md)

Sample commands:

```sh
# Install required Python dependency
pip3 install boto3

# Configure test environment
export S3_TEST_BUCKET_NAME_RS=<your-bucket-name> # e2e tests takes bucket name from environment variable
export AWS_REGION=us-west-2  # Note: Currently only us-west-2 is supported

# Initialize test buckets
python3 path/to/test_helper.py init <your-bucket-name>

# Run the end-to-end tests
RUSTFLAGS="--cfg e2e_test" cargo test --all-features --test e2e_transfer_test
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
