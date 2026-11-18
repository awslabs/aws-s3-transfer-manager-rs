# Development

Building, testing, and profiling the transfer manager from a checkout of this repository. Run commands from
the repository root.

## Modeled API tooling

Developer-only model tooling is documented in
[s3-tm-model-codegen](../../tools/codegen/s3-tm-model-codegen/README.md). Run `just --list` for available
shorthand commands. Normal Cargo builds do not run this tooling.

## Tests

Run all tests:

```sh
cargo test --all-features
```

Run an individual test:

```sh
cargo test -p aws-sdk-s3-transfer-manager --lib operation::download::body::tests::disk_eof_partial_tail
```

## The `cp` example

The `cp` example copies a local file or S3 object to another location locally or in S3. List all options:

```sh
cargo run --example cp -- -h
```

Download an object from S3:

```sh
AWS_PROFILE=<profile-name> RUST_LOG=trace cargo run --example cp -- s3://<my-bucket>/<my-key> /local/path/<filename>
```

Upload a file to S3:

```sh
AWS_PROFILE=<profile-name> RUST_LOG=trace cargo run --example cp -- /local/path/<filename> s3://<my-bucket>/<my-key>
```

Add `--release` (`-r`) to build in release mode; see `cargo run -h`. The `profiling` profile in
`.cargo/config.toml` builds a release binary with debug info for any example: pass `--profile profiling`.
`RUST_LOG=trace` includes every dependency's logs; `RUST_LOG=aws_sdk_s3_transfer_manager=trace` limits
them to the transfer manager's.

### Runtime diagnostics

`AWS_S3_TM_DIAGNOSTICS` enables optional diagnostic collection, and `RUST_LOG` selects which collected
records are emitted. The crate documentation's
[Runtime diagnostics](https://docs.rs/aws-sdk-s3-transfer-manager/latest/aws_sdk_s3_transfer_manager/#runtime-diagnostics)
section defines the accepted settings and the tracing targets.

Report memory-pool snapshots every second with detailed allocator counters:

```sh
AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms,memory.detail=1 \
RUST_LOG=aws_sdk_s3_transfer_manager::memory=debug \
cargo run --release --example cp -- ...
```

Collect per-transfer summaries:

```sh
AWS_S3_TM_DIAGNOSTICS=transfer.detail=1 \
RUST_LOG=aws_sdk_s3_transfer_manager::transfer=debug \
cargo run --release --example cp -- ...
```

Each operation logs under its own target below `aws_sdk_s3_transfer_manager::transfer`
(`transfer::upload`, `transfer::download`, `transfer::upload_objects`, `transfer::download_objects`), so
the `transfer` target enables all of them. A directive also matches longer targets that start with it, so
`transfer::upload` includes `transfer::upload_objects`. Add a directive for the longer target to set it to
a different level:

```sh
RUST_LOG=aws_sdk_s3_transfer_manager::transfer::upload=debug,aws_sdk_s3_transfer_manager::transfer::upload_objects=warn
```

### Flamegraphs

See [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph) for prerequisites and installation.

Generate a flamegraph (written to `flamegraph.svg` by default):

```sh
sudo AWS_PROFILE=<profile-name> RUST_LOG=aws_sdk_s3_transfer_manager=info cargo flamegraph --profile profiling --example cp -- s3://amzn-s3-demo-bucket/<key> /tmp/<filename>
```

### tokio-console

The examples log through the `tracing` crate. Pass `--tokio-console` to use
[`console-subscriber`](https://crates.io/crates/console-subscriber) instead, so that
[tokio-console](https://github.com/tokio-rs/console) can inspect task execution.

`--tokio-console` requires building with `RUSTFLAGS="--cfg tokio_unstable"` or the equivalent setting in
your Cargo configuration:

```sh
RUSTFLAGS="--cfg tokio_unstable" AWS_PROFILE=<profile-name> RUST_LOG=debug cargo run --example cp -- --tokio-console ...
```

Install [tokio-console](https://github.com/tokio-rs/console) as its documentation describes, then run the
example while `tokio-console` is running.

### dial9 runtime tracing

The `cp` example supports optional [dial9](https://github.com/dial9-rs/dial9) instrumentation that traces
all internal worker runtimes, for diagnosing hangs, load imbalance, and poll latency.

```sh
RUSTFLAGS="--cfg s3_tm_dial9 --cfg tokio_unstable -C force-frame-pointers=yes" cargo build --example cp --release
```

`--cfg s3_tm_dial9` builds the example's tracing support, which attaches the recorder to a managed runtime
that the transfer manager then adopts. `--cfg tokio_unstable` enables per-task poll and spawn events for
the worker runtimes. `-C force-frame-pointers=yes` is required for the stacks captured by
`--cpu-profiling` (Linux only).

Run with `--trace-dir` to enable tracing with the default managed runtime:

```sh
./target/release/examples/cp s3://amzn-s3-demo-bucket/<key> /tmp/download --trace-dir /tmp/traces
```

The `S3FIO_TRACE_DIR` environment variable can be used instead of `--trace-dir`. Trace files are written
to that directory and can be viewed with the
[dial9 viewer](https://github.com/dial9-rs/dial9/tree/main/dial9-viewer). Each worker runtime appears as
worker 0..N in the trace.

## End-to-end tests

`crates/aws-s3-transfer/tests/e2e_transfer_test.rs` validates transfers against Amazon S3. To run
these tests:

1. Enable them with `RUSTFLAGS="--cfg e2e_test"`.
2. Set up a test bucket by following the aws-c-s3
   [test helper instructions](https://github.com/awslabs/aws-c-s3/blob/main/tests/test_helper/README.md).

The tests read the bucket name from `S3_TEST_BUCKET_NAME_RS` when they are compiled, and default to
`aws-s3-transfer-manager-rs-test-bucket`. They support only `us-west-2`.

```sh
# Install the test helper's Python dependency.
pip3 install boto3

# Configure the test environment.
export S3_TEST_BUCKET_NAME_RS=<your-bucket-name>
export AWS_REGION=us-west-2

# Initialize the test buckets.
python3 path/to/test_helper.py init <your-bucket-name>

# Run the end-to-end tests.
RUSTFLAGS="--cfg e2e_test" cargo test --all-features --test e2e_transfer_test
```
