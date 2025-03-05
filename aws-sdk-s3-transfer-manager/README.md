# AWS SDK S3 Transfer Manager

A high performance Amazon S3 client for Rust.

## :warning: Developer Preview

This library is currently in developer preview and is **NOT** recommended for production environments.

It is meant for early access and feedback purposes at this time. We'd love to hear from you on use cases, feature prioritization, and API feedback.

See the AWS SDK and Tools [maintenance policy](https://docs.aws.amazon.com/sdkref/latest/guide/maint-policy.html#version-life-cycle)
descriptions for more information.

## Getting started

To begin using the Transfer Manager, follow these examples:

**Add dependency:**
First, you need to add the dependency in your Cargo.toml file.

```
aws_sdk_s3_transfer_manager = "0.1.1"
```

**Create Transfer-Manager:**
Create a transfer manager with the default recommended settings:

```
let config = aws_sdk_s3_transfer_manager::from_env()
    .load()
    .await;
let transfer_manager = aws_sdk_s3_transfer_manager::Client::new(config);
```

**Upload file example:**
This will upload the file by automatically splitting the request into part_size chunks and uploading them in parallel.

```
`// Upload a single file to S3`
let bucket = "<BUCKET-NAME>";
let key = "<OBJECT-KEY>";
let path = "<OBJECT-PATH>";

let stream = InputStream::from_path(path)?;
let response = transfer_manager
    .upload()
    .bucket(bucket)
    .key(key)
    .body(stream)
    .initiate()?
    .join()
    .await;
```

**Download file example:**
This will split the download into part-size chunks, download them in parallel, and then deliver them in-order.

```
// Download a single object from S3
let bucket = "<BUCKET-NAME>";
let key = "<OBJECT-KEY>";

let mut handle = transfer_manager
    .download()
    .bucket(bucket)
    .key(key)
    .initiate()?;

while let Some(chunk_result) = handle
    .body_mut()
    .next()
    .await {
    let chunk = chunk_result?.data.into_bytes();
    println!("Received {} bytes", chunk.len());
}
```

**Upload directory example:**
This will recursively upload all files in the directory, combining the given prefix with each file's path from the filesystem. For example, if your prefix is "prefix" and the file path is "test/docs/key.json", it will be uploaded with the key "prefix/test/docs/key.json".

```
// Upload a directory to S3
let bucket = "<BUCKET-NAME>";
let source_dir = "<SOURCE-DIRECTORY-PATH>";
let key_prefix = "<KEY-PREFIX>";

let handle = transfer_manager
    .upload_objects()
    .key_prefix(key_prefix)
    .bucket(bucket)
    .source(source_dir)
    .recursive(true)
    .send()
    .await?;

let response = handle.join().await?;
```

**Download directory example:**
This will download every object under the prefix and will create a local directory with similar hierarchy.

```
// Download objects with a common prefix to a local directory
let bucket = "<BUCKET-NAME>";
let destination_dir = "<DESTINATION-DIRECTORY-PATH>";
let key_prefix = "<KEY-PREFIX>";

let handle = transfer_manager
    .download_objects()
    .key_prefix(key_prefix)
    .bucket(bucket)
    .destination(destination_dir)
    .send()
    .await?;

let response = handle.join().await?;
```

## Using the SDK

Until the SDK is released, we will be adding information about using the SDK to the
[Developer Guide](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/welcome.html). Feel free to suggest
additional sections for the guide by opening an issue and describing what you are trying to do.

## Getting Help

* [GitHub discussions](https://github.com/awslabs/aws-s3-transfer-manager-rs/discussions) - For ideas, RFCs & general questions
* [GitHub issues](https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/new/choose) - For bug reports & feature requests
* [Developer Guidance](https://github.com/awslabs/aws-s3-transfer-manager-rs/blob/main/README.md) -- More guidance about development and contributing.

## License

This project is licensed under the Apache-2.0 License.
