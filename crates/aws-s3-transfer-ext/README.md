# Extensions for Transfer Manager for Amazon S3

Optional integrations for Transfer Manager for Amazon S3. The runtime-adoption
API transfers a caller-built managed runtime into transfer manager ownership.
HTTP composition is provided independently by `aws-smithy-managed-http`.

```rust,no_run
use aws_sdk_s3_transfer_manager as tm;
use aws_sdk_s3_transfer_manager_ext as tm_ext;
use aws_smithy_execution::ManagedRuntime;
use aws_smithy_managed_http::client::{ManagedHttpPool, pool};

# async fn example(sdk_config: aws_types::SdkConfig) -> Result<(), Box<dyn std::error::Error>> {
let runtime = ManagedRuntime::builder().build()?;
let execution = runtime.handle();
let completion = runtime.shutdown_completion();

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
        .runtime_config(tm::config::RuntimeConfig::managed(tm_ext::managed(runtime)))
        .build()?,
);
drop(client);
completion.wait().await?;
# Ok(())
# }
```

For runtime adoption alone, omit the custom pool and HTTP client. The transfer
manager constructs its existing transport preset against the adopted runtime
when HTTP is absent. TLS, DNS, proxies, timeouts, limits, and connection reuse
stay on the native pool builder. This adapter adds no transport configuration
facade.

The transfer manager owns the adopted runtime. Active transfers retain transfer
manager client state, and owner destruction requests shutdown without waiting
for managed thread completion. Retain `runtime.shutdown_completion()` before
adoption, or obtain the transfer manager-owned opaque observer from
`client.shutdown_completion()`. Both wait for actual managed-thread joins after
ownership is released. Waiting does not cancel or drain transfers, and retained
client clones or transfer ownership can delay it. Automatic managed execution
exposes the same transfer manager observer; caller-owned Tokio execution returns
`None`.

Execution handles and HTTP clients do not own shutdown. Managed HTTP pool
clones and targeted native clients retain the shared connection pool. Dropping
an unused managed value or configuration requests runtime shutdown.

Configured HTTP preserves the transfer manager's offloaded file-backed
single-PUT body path. Reads use the blocking pool of the Tokio runtime executing
the transfer request. The body can be polled outside Tokio; retries retain the
captured read runtime. Multipart part reads performed by managed transfer tasks
retain their direct read path.

This 0.x adapter enables the transfer manager's non-default `__unstable_ext`
feature through its dependency; applications need no extra compiler flags. The
gated hook is experimental independently of the transfer manager's ordinary API.
Adapter releases pin the tested transfer manager version and support the
declared 0.1 execution line; use that pairing. Changing those dependency
requirements requires validating the combination.

The optional `dial9` feature enables the execution crate's dial9 tracing. Pass
the recorder's handle to `ManagedRuntime::builder().dial9_handle(..)` before
building the runtime, then adopt the runtime with `managed(runtime)`. Keep the
recorder alive while recording. To flush managed-thread events, wait for
shutdown completion after releasing the client and its transfers, then shut
down the recorder.
