# S3 Mock Server

A mock S3 server for testing and benchmarking the AWS S3 Transfer Manager.

## Overview

The S3 Mock Server provides a local HTTP server that implements the S3 API operations required by the Transfer Manager, allowing for comprehensive testing of concurrent operations, error handling, and performance characteristics.

## Features

- Support for all S3 API operations used by the Transfer Manager
- Multiple storage backends (in-memory and filesystem)
- Object and upload inspection capabilities
- Network simulation options
- Integration with AWS S3 Transfer Manager

## Usage

```rust
use s3_mock_server::{S3MockServer};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a mock S3 server with in-memory storage
    let mock_s3 = S3MockServer::builder()
        .with_in_memory_store()
        .with_port(9000)
        .build()?;

    // Add a mock object
    mock_s3.add_object("test-key", Bytes::from("test content"), None)?;

    // Start the server
    let _server_handle = mock_s3.start().await?;

    // Create a Transfer Manager client that uses the mock server
    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(mock_s3.client())
        .build();

    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    // Use the Transfer Manager with the mock server
    // ...

    // Inspect the state of objects and uploads
    let objects = mock_s3.objects();
    let uploads = mock_s3.uploads();

    Ok(())
}
```

## License

This project is licensed under the Apache License, Version 2.0.
