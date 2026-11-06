//! A local S3 endpoint for running transfer scenarios end to end. Not part of any scenario.

// Each example uses a different subset.
#![allow(dead_code)]

use aws_sdk_s3_transfer_manager::{Client, Config};
use s3_mock_server::{S3MockServer, ServerHandle};

pub const BUCKET: &str = "review-bucket";

pub struct Mock {
    pub server: S3MockServer,
    pub handle: ServerHandle,
}

impl Mock {
    pub async fn start() -> Mock {
        let server = S3MockServer::builder()
            .with_in_memory_store()
            .build()
            .expect("build mock server");
        let handle = server.start().await.expect("start mock server");
        server.create_bucket(BUCKET).await.expect("create bucket");
        Mock { server, handle }
    }

    /// A transfer manager client for this endpoint, built the way 1.4 builds one.
    pub async fn client(&self) -> Client {
        let sdk_config = self.handle.shared_config().await;
        Client::new(Config::builder().s3_config(&sdk_config).build())
    }

    pub async fn put(&self, key: &str, body: impl Into<bytes::Bytes>) {
        self.server.add_object(BUCKET, key, body, None).await.expect("seed object");
    }

    /// Every key in the bucket, sorted.
    pub async fn keys(&self) -> Vec<String> {
        let s3 = self.handle.client().await;
        let mut keys = Vec::new();
        let mut pages = s3.list_objects_v2().bucket(BUCKET).into_paginator().send();
        while let Some(page) = pages.next().await {
            let page = page.expect("list objects");
            keys.extend(page.contents().iter().filter_map(|o| o.key().map(str::to_string)));
        }
        keys.sort();
        keys
    }
}
