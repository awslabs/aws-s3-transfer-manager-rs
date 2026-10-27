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

    // Start the server, and create the bucket the test uses
    let handle = server.start().await?;
    server.create_bucket("test-bucket").await?;
    
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
    server.create_bucket("test-bucket").await?;
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

### Conditional Requests

- GetObject, HeadObject and DeleteObject honor `If-Match`; PutObject honors `If-Match` with an
  ETag and `If-None-Match: *`. The server ignores other conditions. The in-memory store checks a
  write's or a delete's condition atomically with it.

### Archived Objects

- Objects in `GLACIER` and `DEEP_ARCHIVE` cannot be read until restored (403
  InvalidObjectState). RestoreObject restores them immediately, answering 200 (S3 answers 202 for
  a new restore); `S3MockServer::set_restore_status` sets any restore status,
  including a restore in progress, which a further RestoreObject fails with 409.
- A RestoreObject without `Days` restores the object for 1 day.

### Storage Classes

GetObject and HeadObject omit `x-amz-storage-class` for `STANDARD` objects; ListObjectsV2 reports
`STANDARD` for objects stored without a storage class, as S3 general purpose buckets do.

### Checksums

- GetObject and HeadObject return an object's checksums only when the request sets
  `x-amz-checksum-mode: ENABLED`, as S3 does. The SDK sets it on GetObject unless response
  checksum validation is only performed when required.
- PutObject and UploadPart check `x-amz-checksum-*` and `Content-MD5` headers as S3 general
  purpose buckets do: a malformed value fails with InvalidRequest (InvalidDigest for
  `Content-MD5`) before the body is read, and a body that does not match fails with BadDigest,
  storing nothing.
- Objects report their checksum type, on reads, writes and listings:
  COMPOSITE for a multipart object whose checksum combines its parts' checksums, FULL_OBJECT
  otherwise.
- A GetObject or HeadObject with `partNumber` returns or describes that part, with its
  `Content-Range`; an object uploaded in one request is a single part. A HeadObject with a `Range`
  describes the range. HeadObject is always answered 200, where S3 answers 206 for a part or a
  range.
- A GetObject or HeadObject of part of a multipart object with a full-object checksum, by
  `partNumber` or by a range, returns no checksum, as S3 general purpose buckets do.

### Listing

ListObjectsV2 pages hold at most `max-keys` entries, counting each common prefix once, and never
split a common prefix across pages. Listed objects report their checksum algorithms, storage class
and, when requested, restore status.

### Buckets

Create buckets with CreateBucket or `S3MockServer::create_bucket` before using them: object
requests for a missing bucket fail with NoSuchBucket.

### Seeding and Inspection

`add_object_with` seeds an object directly in storage. `AddObjectRequest` sets the content type,
user metadata and last-modified time, and also an explicit ETag, storage class, restore status and
checksum algorithms. `list_multipart_uploads` reports the multipart uploads in progress.

### Request Counts and Faults

The server counts the requests it receives per operation, and can fail or reshape the requests a
`FaultTarget` matches:

```rust
use s3_mock_server::{Error, FaultTarget, FaultType, Occurrence, Operation};

// Fail the second HeadObject request for `key` that the target sees with 403 AccessDenied.
server.insert_fault_for(
    FaultTarget::operation(Operation::HeadObject).bucket("test-bucket").key("key"),
    FaultType::Error(Error::AccessDenied),
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
