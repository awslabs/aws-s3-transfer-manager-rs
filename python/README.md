# aws-s3-transfer

A high-performance transfer manager for Amazon S3, built on the
[Rust transfer manager](https://github.com/awslabs/aws-s3-transfer-manager-rs).
Large objects are split into parts that are transferred in parallel, with
automatic retries and memory-bounded buffering. It is independent of boto3's
`s3transfer` package.

> [!WARNING]
> **Experimental:** the API may change without notice between releases, and
> this library is not recommended for production use.

## Installation

```sh
pip install aws-s3-transfer
```

Python 3.11 or later is required.

## Usage

```python
from aws_s3_transfer import TransferManager

# Credentials and Region are resolved as in other AWS SDKs.
with TransferManager() as tm:
    # Transfer local files; large files are split into parallel parts.
    tm.upload_file("data.bin", "my-bucket", "data.bin")
    tm.download_file("my-bucket", "data.bin", "copy.bin")

    # Upload from memory, setting S3 options as keyword arguments.
    tm.upload(b"hello", "my-bucket", "hello.txt", content_type="text/plain")

    # Stream a download; metadata is available before the body is read.
    with tm.download("my-bucket", "hello.txt") as stream:
        print(stream.metadata.object_size, stream.read())

    # Transfer whole directories under a key prefix.
    tm.upload_directory("photos/", "my-bucket", key_prefix="photos/")
    tm.download_directory("my-bucket", "restored/", key_prefix="photos/")
```

`AsyncTransferManager` provides the same operations for `asyncio`:

```python
from aws_s3_transfer import AsyncTransferManager

async with AsyncTransferManager() as tm:
    await tm.upload_file("data.bin", "my-bucket", "data.bin")

    # Iterate over the object's contents as they arrive, in order.
    async with tm.download("my-bucket", "data.bin") as stream:
        async for chunk in stream:
            ...
```

Uploads accept bytes-like objects, binary file objects, and iterables of bytes
(plus async iterables with `AsyncTransferManager`). Every transfer accepts a
`progress` callback, and errors derive from `TransferError`.
