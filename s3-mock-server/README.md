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

### Basic Setup

```rust
use s3_mock_server::S3MockServer;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_with_mock_server() -> Result<(), Box<dyn std::error::Error>> {
    // Create a mock S3 server with in-memory storage
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .with_port(9000)  // Optional: specify port
        .build()?;

    // Start the server
    let handle = server.start().await?;
    
    // Get an S3 client configured for the mock server
    let s3_client = handle.client().await;
    
    let content = "Hello mock S3";
    
    // Use the client with AWS SDK to upload an object
    s3_client
        .put_object()
        .bucket("test-bucket")
        .key("test-key")
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await?;

    // Download the object
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let body = response.body.collect().await?;
    let downloaded_content = String::from_utf8(body.to_vec())?;
    assert_eq!(downloaded_content, content);

    // Server automatically shuts down when handle is dropped
    Ok(())
}
```

### With Transfer Manager

```rust
use s3_mock_server::S3MockServer;
use aws_sdk_s3_transfer_manager::{Config, Client};
use std::path::Path;

#[tokio::test]
async fn test_transfer_manager_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Create mock server
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()?;
    
    let handle = server.start().await?;
    let s3_client = handle.client().await;

    // Create Transfer Manager with mock server client
    let tm_config = Config::builder()
        .client(s3_client)
        .build();
    let tm = Client::new(tm_config);

    // Use Transfer Manager for uploads/downloads
    let upload_handle = tm
        .upload()
        .bucket("test-bucket")
        .key("large-file.dat")
        .body(Path::new("/path/to/large/file"))
        .send()
        .await?;

    upload_handle.join().await?;

    Ok(())
}
```

### Filesystem Storage

```rust
use s3_mock_server::S3MockServer;
use tempfile::TempDir;

#[tokio::test]
async fn test_filesystem_storage() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    
    let server = S3MockServer::builder()
        .with_local_dir_store(temp_dir.path())
        .await?
        .build()?;
    
    let handle = server.start().await?;
    let s3_client = handle.client().await;

    // Objects will be persisted to the filesystem
    // Directory structure: temp_dir/objects/, temp_dir/uploads/
    
    Ok(())
}
```

### Request Counts and Faults

The server counts the requests it receives per operation, and can fail or reshape the requests a
`FaultTarget` matches:

```rust
use s3_mock_server::{FaultTarget, FaultType, Occurrence, Operation};

// Fail the second HeadObject request for `key` that the target sees with 403 AccessDenied.
server.insert_fault_for(
    FaultTarget::operation(Operation::HeadObject).bucket("test-bucket").key("key"),
    FaultType::ServiceError { status: 403, code: "AccessDenied" },
    1,
    Occurrence::Once,
);

// ... run the code under test ...

assert_eq!(server.request_count(Operation::HeadObject), 2);
```

`insert_fault(bucket, key, ..)` targets the GetObject, PutObject and UploadPart requests for one
key. Counts and faults apply per HTTP request, so each retry and each part request counts.

## License

This project is licensed under the Apache License, Version 2.0.
