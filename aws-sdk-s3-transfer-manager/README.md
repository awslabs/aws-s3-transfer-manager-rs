# Transfer Manager for Amazon S3 [![Crates.io][crates-badge]][crates-url] [![API docs][docs-badge]][docs-url] [![Apache 2.0 licensed][apache-badge]][apache-url]

[crates-badge]: https://img.shields.io/crates/v/aws-sdk-s3-transfer-manager.svg
[crates-url]: https://crates.io/crates/aws-sdk-s3-transfer-manager
[apache-badge]: https://img.shields.io/badge/License-Apache_2.0-blue.svg
[apache-url]:https://opensource.org/licenses/Apache-2.0
[docs-badge]: https://img.shields.io/docsrs/aws-sdk-s3-transfer-manager
[docs-url]: https://docs.rs/crate/aws-sdk-s3-transfer-manager/latest

Transfer Manager for Amazon S3 - a high-performance data plane client for uploading and downloading objects and directories.

## Key features and benefits

* **Automatic Request Splitting:** Improves throughput by automatically splitting large requests into part-sized chunks and processing them in parallel across multiple connections. This overcomes single-connection bandwidth limitations.
* **Automatic Retries:** Enhances reliability by automatically retrying failed chunks independently, eliminating the need to restart entire transfers when intermittent errors occur.
* **Optimized Parallel Reads:** Implements parallel read operations across different sections of large files during uploads, providing superior performance compared to sequential reading.
* **Simplified Directory Operations:** Upload the files in a local directory, optionally recursing into its subdirectories, or download the objects sharing a common prefix with a single API call, streamlining bulk transfer operations.
* **Automatic sizing:** part size, concurrency and memory use are sized to the host by default, and each can be set explicitly.

## Getting started

Add the crate to your project:

```sh
cargo add aws-sdk-s3-transfer-manager
```

**Create a client:**
`from_env` loads the S3 client configuration from the shared AWS configuration sources and sizes the automatic defaults to the host.

```rust,no_run
use aws_sdk_s3_transfer_manager::Client;

async fn create_client() -> Client {
    let config = aws_sdk_s3_transfer_manager::from_env().load().await;
    Client::new(config)
}
```

**Upload a file:**
A large file is split into part-sized chunks that are uploaded in parallel.

```rust,no_run
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::Client;

async fn upload_file(client: &Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let stream = InputStream::from_path("/path/to/file")?;
    let output = client
        .upload()
        .bucket("amzn-s3-demo-bucket")
        .key("my-key")
        .body(stream)
        .initiate()?
        .join()
        .await?;
    println!("uploaded, ETag {:?}", output.e_tag());
    Ok(())
}
```

**Download to a file:**
A large object is downloaded in parallel part-sized ranges. The data is written to a temporary file in the destination's directory, which is renamed to the destination when the download succeeds.

```rust,no_run
use aws_sdk_s3_transfer_manager::Client;

async fn download_file(client: &Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let handle = client
        .download()
        .bucket("amzn-s3-demo-bucket")
        .key("my-key")
        .write_to_path("/path/to/file")
        .await?;
    handle.join().await?;
    Ok(())
}
```

**Stream a download:**
The body delivers the object's data in order as its parts arrive. `join` returns the result of the transfer, including any checksum mismatch; accept the data only after it succeeds.

```rust,no_run
use aws_sdk_s3_transfer_manager::Client;

async fn download_stream(client: &Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let mut handle = client
        .download()
        .bucket("amzn-s3-demo-bucket")
        .key("my-key")
        .initiate()?;

    while let Some(chunk) = handle.body_mut().next().await {
        let bytes = chunk?.data.into_contiguous();
        println!("received {} bytes", bytes.len());
    }

    handle.join().await?;
    Ok(())
}
```

**Upload a directory:**
By default only the files directly inside the source directory are uploaded. This example supplies a recursive walker, so it uploads every file in the directory tree, combining the given prefix with each file's path from the filesystem.
For example, if your prefix is `"prefix"` and the file path is `"test/docs/key.json"`, it will be uploaded with the key `"prefix/test/docs/key.json"`.

```rust,no_run
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::Client;

async fn upload_directory(client: &Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let output = client
        .upload_objects()
        .bucket("amzn-s3-demo-bucket")
        .source("/path/to/directory")
        .key_prefix("prefix")
        .walker(FsWalker::builder().recursive(true).build())
        .initiate()?
        .join()
        .await?;
    println!(
        "uploaded {} objects, {} failed",
        output.objects_uploaded(),
        output.failed_transfers().len()
    );
    Ok(())
}
```

**Download a prefix:**
This will download every object under the prefix, writing each one at its full key under the destination directory. With the prefix `"photos/"`, the object `"photos/2023/a.jpg"` is written to `"/path/to/directory/photos/2023/a.jpg"`.

```rust,no_run
use aws_sdk_s3_transfer_manager::io::walk::S3Walker;
use aws_sdk_s3_transfer_manager::Client;

async fn download_prefix(client: &Client) -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
    let output = client
        .download_objects()
        .bucket("amzn-s3-demo-bucket")
        .walker(S3Walker::builder().prefix("photos/").build())
        .destination("/path/to/directory")
        .initiate()?
        .join()
        .await?;
    println!(
        "downloaded {} objects, {} failed",
        output.objects_downloaded(),
        output.failed_transfers().len()
    );
    Ok(())
}
```

## Learn more

The [API documentation](https://docs.rs/aws-sdk-s3-transfer-manager) on docs.rs describes the client configuration, each transfer operation and its output, integrity checks, runtime and HTTP options, and runtime diagnostics.

## Getting help

* [GitHub discussions](https://github.com/awslabs/aws-s3-transfer-manager-rs/discussions) - For ideas, RFCs & general questions
* [GitHub issues](https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/new/choose) - For bug reports & feature requests

## Contributing

See [CONTRIBUTING](https://github.com/awslabs/aws-s3-transfer-manager-rs/blob/main/CONTRIBUTING.md).

## License

This project is licensed under the Apache-2.0 License.
