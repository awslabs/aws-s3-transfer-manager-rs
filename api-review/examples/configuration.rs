//! Configuration and client lifecycle.
//!
//! Each function builds a client the way a customer would for one situation. `main` builds them all
//! without sending requests.

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, ReadAhead};
use aws_sdk_s3_transfer_manager::{Client, Config};

const MIB: u64 = 1024 * 1024;

/// Defaults: AWS configuration and transfer settings all come from the environment.
async fn defaults() -> Client {
    let config = aws_sdk_s3_transfer_manager::from_env().load().await;
    Client::new(config)
}

/// Tune transfer settings on top of the default AWS configuration.
async fn tuned() -> Client {
    let config = aws_sdk_s3_transfer_manager::from_env()
        .part_size(PartSize::Target(16 * MIB))
        .multipart_threshold(PartSize::Target(64 * MIB))
        .concurrency(ConcurrencyMode::Explicit(64))
        .load()
        .await;
    Client::new(config)
}

/// `read_ahead` is only on the builder, so tuning it means leaving the loader.
async fn tuned_read_ahead() -> Client {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let config = Config::builder()
        .s3_config(&sdk_config)
        .read_ahead(ReadAhead::Parts(4))
        .build();
    Client::new(config)
}

/// Choose a region, profile, and credentials. The loader can't take AWS configuration, so this builds
/// an `SdkConfig` with `aws-config` and hands it to `Config::builder`.
async fn region_profile_credentials() -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("eu-west-1"))
        .profile_name("transfer")
        .credentials_provider(Credentials::new("AKIDEXAMPLE", "secret", None, None, "example"))
        .load()
        .await;
    let config = Config::builder()
        .s3_config(&sdk_config)
        .part_size(PartSize::Target(16 * MIB))
        .build();
    Client::new(config)
}

/// Share one `SdkConfig` between an AWS SDK S3 client and the transfer manager.
async fn shared_with_sdk_client() -> (aws_sdk_s3::Client, Client) {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3 = aws_sdk_s3::Client::new(&sdk_config);
    let tm = Client::new(Config::builder().s3_config(&sdk_config).build());
    (s3, tm)
}

/// A custom endpoint, set on the shared AWS configuration.
async fn custom_endpoint() -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .load()
        .await;
    Client::new(Config::builder().s3_config(&sdk_config).build())
}

/// Path-style addressing is an S3 client setting that `SdkConfig` doesn't carry. Passing an S3
/// configuration builder needs the transfer manager's `sdk-v1` feature.
#[cfg(feature = "sdk-v1")]
async fn path_style_endpoint() -> Client {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .endpoint_url("http://localhost:9000")
        .force_path_style(true);
    Client::new(Config::builder().s3_config(s3_config).build())
}

/// Share a client across tasks, then wait for its runtime to stop.
async fn lifecycle(client: Client) {
    // Clones share one runtime, connection pool, and memory pool.
    let worker = client.clone();
    tokio::spawn(async move {
        let _ = worker.upload();
    })
    .await
    .unwrap();

    // Shutdown starts when the last clone and transfer handle are dropped. Waiting doesn't request it.
    let completion = client.shutdown_completion();
    drop(client);
    if let Some(completion) = completion {
        completion.wait().await.expect("runtime shut down cleanly");
    }
}

#[tokio::main]
async fn main() {
    lifecycle(defaults().await).await;
    lifecycle(tuned().await).await;
    lifecycle(tuned_read_ahead().await).await;
    lifecycle(region_profile_credentials().await).await;
    let (_s3, tm) = shared_with_sdk_client().await;
    lifecycle(tm).await;
    lifecycle(custom_endpoint().await).await;
    #[cfg(feature = "sdk-v1")]
    lifecycle(path_style_endpoint().await).await;
}
