# Extensions for S3 Transfer Manager

Optional integrations for S3 Transfer Manager. The runtime-adoption API transfers
a caller-built managed runtime into TM ownership. HTTP composition is provided independently by
`aws-smithy-managed-http`.

```rust,no_run
use aws_sdk_s3_transfer_manager as tm;
use aws_sdk_s3_transfer_manager_ext as tm_ext;
use aws_smithy_execution::ManagedRuntime;
use aws_smithy_managed_http::client::{ManagedHttpPool, pool};

# fn example(sdk_config: aws_types::SdkConfig) -> Result<(), Box<dyn std::error::Error>> {
let runtime = ManagedRuntime::builder().build()?;
let execution = runtime.handle();

// Configure transport on the native builder. This example uses cleartext HTTP;
// for HTTPS, enable and select a native TLS provider on that builder.
let transport = pool::ConnectionPool::builder();
let pool = ManagedHttpPool::builder()
    .pool_builder(transport)
    .runtime(execution)
    .build()?;

let sdk_config = sdk_config.into_builder().http_client(pool.clone()).build();
let client = tm::Client::new(
    tm::Config::builder()
        .s3_config(&sdk_config)
        .runtime_config(tm::RuntimeConfig::managed(tm_ext::managed(runtime)))
        .build(),
);
# drop(client);
# Ok(())
# }
```

For runtime adoption alone, omit the custom pool and HTTP client. TM constructs
its existing transport preset against the adopted runtime when HTTP is absent.
TLS, DNS, proxies, timeouts, limits, and connection reuse stay on the native pool
builder. This adapter adds no transport configuration facade.

TM owns the adopted runtime. Active transfers retain TM client state, and owner
destruction requests shutdown without waiting for managed thread completion.
Execution handles and HTTP clients do not own shutdown. Managed HTTP pool
clones and targeted native clients retain the shared connection pool. Dropping
an unused managed value or configuration requests runtime shutdown.

Configured HTTP preserves TM's offloaded file-backed single-PUT body path.
Reads use the blocking pool of the Tokio runtime on which the body is polled.
Multipart part reads performed by managed transfer tasks retain their direct
read path.

This 0.x adapter enables TM's non-default `__unstable_ext` feature through its
dependency; applications need no extra compiler flags. The gated hook is
experimental independently of TM's ordinary API. Adapter releases pin the tested
TM version and support the declared 0.1 execution line; use that pairing.
Changing those dependency requirements requires validating the combination.

The optional `dial9` feature enables tracing support. Configure the recorder on
the execution builder before building the runtime and keep it alive while
recording. TM's separate `dial9_handle` setting cannot instrument an already-built
runtime and causes client construction to panic for adoption.
