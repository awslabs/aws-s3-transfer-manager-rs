//! Inherited SDK settings, execution runtime, HTTP client, and TLS.
//!
//! Each function builds a client the way a customer would for one situation. `main` runs one upload
//! through each against a local S3 endpoint and prints what the client reports about itself.

#[path = "support/mock.rs"]
mod mock;

use std::time::Duration;

use aws_config::SdkConfig;
use aws_config::retry::RetryConfig;
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::{Client, Config, RuntimeConfig};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bring retry, stalled-stream, and timeout settings through shared AWS configuration.
fn inherited_sdk_settings(sdk_config: &SdkConfig) -> Client {
    let sdk_config = sdk_config
        .to_builder()
        .retry_config(RetryConfig::disabled())
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(2))
                .operation_attempt_timeout(Duration::from_secs(30))
                .build(),
        )
        .build();
    Client::new(Config::builder().s3_config(&sdk_config).build())
}

/// Run transfer work on the application's own Tokio runtime instead of managed threads.
fn caller_tokio_runtime(sdk_config: &SdkConfig) -> Client {
    let config = Config::builder()
        .s3_config(sdk_config)
        .runtime_config(RuntimeConfig::multi_thread_tokio())
        .build();
    Client::new(config)
}

/// Bring a plain HTTP client with a chosen TLS provider. The transfer manager keeps it. This works, but
/// it is one connection pool shared by every managed thread: connections and their drivers aren't tied
/// to the threads that use them.
fn plain_http_client(sdk_config: &SdkConfig) -> Client {
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .build_https();
    let sdk_config = sdk_config.to_builder().http_client(http_client).build();
    Client::new(Config::builder().s3_config(&sdk_config).build())
}

/// Configure the managed runtime and a partitioned HTTP pool on it, then hand both to the transfer
/// manager. Each managed thread gets its own pool partition, as with the default transport.
#[cfg(feature = "ext")]
fn managed_runtime_and_http(sdk_config: &SdkConfig) -> Result<Client, BoxError> {
    use aws_smithy_execution::{ManagedRuntime, topology::Topology};
    use aws_smithy_managed_http::client::{ManagedHttpPool, pool};

    let runtime = ManagedRuntime::builder()
        .topology(Topology::detect()?.with_max_placements(4))
        .thread_name_prefix("app-transfer")
        .build()?;
    let transport = pool::ConnectionPool::builder()
        .max_connections_per_host(256)
        .idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(60))
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc));
    let http_client = ManagedHttpPool::builder()
        .pool_builder(transport)
        .runtime(runtime.handle())
        .build()?;

    let sdk_config = sdk_config.to_builder().http_client(http_client).build();
    let config = Config::builder()
        .s3_config(&sdk_config)
        .runtime_config(RuntimeConfig::managed(aws_sdk_s3_transfer_manager_ext::managed(runtime)))
        .build();
    Ok(Client::new(config))
}

/// Spread connections across network interfaces (managed transport only).
#[cfg(target_os = "linux")]
fn network_interfaces(sdk_config: &SdkConfig) -> Client {
    use aws_sdk_s3_transfer_manager::S3ClientConfig;
    let s3_config = S3ClientConfig::new(sdk_config).network_interfaces(["lo"]);
    Client::new(Config::builder().s3_config(s3_config).build())
}

/// Adopt an application-built managed runtime through the extension crate.
#[cfg(feature = "ext")]
fn adopted_runtime(sdk_config: &SdkConfig) -> Result<Client, BoxError> {
    let runtime = aws_smithy_execution::ManagedRuntime::builder()
        .thread_name_prefix("app-transfer")
        .build()?;
    let config = Config::builder()
        .s3_config(sdk_config)
        .runtime_config(RuntimeConfig::managed(aws_sdk_s3_transfer_manager_ext::managed(runtime)))
        .build();
    Ok(Client::new(config))
}

/// What a client can say about the runtime and transport it ended up with.
fn describe(client: &Client) -> String {
    format!(
        "runtime_config={:?} managed_shutdown_observer={}",
        client.config().runtime_config(),
        client.shutdown_completion().is_some()
    )
}

async fn upload_one(client: &Client, key: &str) -> Result<(), BoxError> {
    let handle = client
        .upload()
        .bucket(BUCKET)
        .key(key)
        .body(InputStream::from(vec![0u8; 1024]))
        .initiate()?;
    handle.join().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mock = mock::Mock::start().await;
    let sdk_config = mock.handle.shared_config().await;

    let mut clients = vec![
        ("default", mock.client().await),
        ("inherited_sdk_settings", inherited_sdk_settings(&sdk_config)),
        ("caller_tokio_runtime", caller_tokio_runtime(&sdk_config)),
        ("plain_http_client", plain_http_client(&sdk_config)),
    ];
    #[cfg(target_os = "linux")]
    clients.push(("network_interfaces", network_interfaces(&sdk_config)));
    #[cfg(feature = "ext")]
    clients.push(("adopted_runtime", adopted_runtime(&sdk_config)?));
    #[cfg(feature = "ext")]
    clients.push(("managed_runtime_and_http", managed_runtime_and_http(&sdk_config)?));

    for (name, client) in &clients {
        let result = upload_one(client, name).await.map_err(|e| e.to_string());
        println!("{name}: upload={result:?}\n    {}", describe(client));
    }
    Ok(())
}
