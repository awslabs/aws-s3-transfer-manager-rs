//! Captures the `x-amz-user-agent` header that an AWS SDK S3 client and a transfer manager client send
//! when both are built from the same `SdkConfig` carrying framework metadata.

use aws_config::{BehaviorVersion, FrameworkMetadata, Region, SdkConfig};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3_transfer_manager::{Client, Config};
use aws_smithy_http_client::test_util::{CaptureRequestReceiver, capture_request};

/// Shared AWS configuration with framework metadata and an HTTP client that records the request.
async fn sdk_config_with_framework() -> (SdkConfig, CaptureRequestReceiver) {
    let (http_client, captured) = capture_request(None);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .credentials_provider(Credentials::new("AKIDEXAMPLE", "secret", None, None, "example"))
        .http_client(http_client)
        .framework_metadata(
            FrameworkMetadata::new("my-framework", Some("2.1")).expect("valid framework metadata"),
        )
        .load()
        .await;
    (sdk_config, captured)
}

async fn sdk_client_user_agent() -> String {
    let (sdk_config, captured) = sdk_config_with_framework().await;
    let s3 = aws_sdk_s3::Client::new(&sdk_config);
    let _ = s3.get_object().bucket("bucket").key("key").send().await;
    user_agent(captured)
}

async fn transfer_manager_user_agent() -> String {
    let (sdk_config, captured) = sdk_config_with_framework().await;
    let tm = Client::new(Config::builder().s3_config(&sdk_config).build());
    let mut handle = tm.download().bucket("bucket").key("key").initiate().expect("download starts");
    let _ = handle.body_mut().next().await;
    user_agent(captured)
}

fn user_agent(captured: CaptureRequestReceiver) -> String {
    let request = captured.expect_request();
    let header = request.headers().get("x-amz-user-agent").expect("user agent header");
    header.to_string()
}

#[tokio::main]
async fn main() {
    println!("aws_sdk_s3::Client: {}", sdk_client_user_agent().await);
    println!("transfer manager:   {}", transfer_manager_user_agent().await);
}
