# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-10-09

### Fixed
- A download to disk (`write_to_path`, `write_to_file`, or `download_objects`) could report success,
  and `write_to_path` could rename its file into place, while a write made early to relieve memory
  pressure was still in progress. If that write then failed, the error was discarded and part of the
  file held zeros or its previous contents. Downloads now complete only after those writes finish,
  and a failed write fails the download with `ErrorKind::IOError`.
- A download to disk now fails with `ErrorKind::IOError`, instead of resizing and publishing the
  destination, if fewer bytes were written than the object holds.
- `TransferMetrics::disk_write` counted bytes when they were received rather than when they were
  written, so it could run ahead of the file during a download and count unwritten parts after a
  failure. It now matches the bytes written to the destination, both during a download and after a
  failure. The value on success is unchanged.
- `write_to_file` accepted a file opened in append mode. On Linux and Android every write went to
  the end of the file and the final resize cut the result, so `join` returned `Ok` over the wrong
  bytes. An append-mode destination is now rejected with `ErrorKind::InputInvalid` on every
  platform, before any request is sent, and the file is left unchanged.
- `write_to_path` and `download_objects` opened their temporary file with a truncating create. A
  file already at the temporary name was overwritten and published as the download, a symbolic
  link there was followed and its target overwritten, and two downloads to one path that drew
  the same name shared one file. A temporary file is now created only if no entry with that name
  exists. On a collision another name is drawn, up to three attempts, after which the download
  fails with `ErrorKind::IOError`. A temporary path is now renamed or removed at most once, so a
  file another download has since created under that name is left alone.
- A `PartStream` part number of 2^32 or more was narrowed to a different part number, and a
  repeated part number was accepted. A part number outside 1–10,000 now fails the upload with
  `ErrorKind::InputInvalid` before that part is sent, and a part number repeated within an upload
  fails it before the upload is completed.
- An upload built without a body stored an empty object. It now fails at `initiate()` with
  `ErrorKind::InputInvalid`. To upload an empty object, pass `InputStream::from_static(b"")`.
- `content_length` on an upload was ignored. When set, it is now the exact body size: a value that
  is negative or contradicts the body's size fails at `initiate()`, and a body that produces a
  different number of bytes fails the upload before `CompleteMultipartUpload` is sent.
- An `UploadPart` response without an ETag was recorded as a completed part. It now fails the
  upload with `ErrorKind::ServiceError`, which carries the operation name and the response's
  request IDs, as errors from a failed request do.
- `TokioIo` published pooled memory it never wrote when the reader replaced the `ReadBuf` it was
  given. That read now fails with `io::ErrorKind::InvalidData`. `TokioIo` also read its source
  again after end of file, uploading data that arrived later or blocking on a terminal. It now
  stops at the first end of file.
- The `ChecksumStrategy` documentation said the `with_calculated_*` strategies calculate a full
  object checksum while uploading. The transfer manager calculates none: a multipart upload sends
  per-part checksums and the checksum type, and S3 computes the full object checksum from the
  parts. The documentation now says so.

## [0.3.0] - 2026-09-30

Uploads and downloads now share one bounded, reusable pool for payload memory, and the managed
runtime sends requests through a partitioned connection pool. Memory configuration and download
chunk types change incompatibly; see Changed and Removed.

### Added
- Shared payload memory: each client draws part buffers from one bounded pool that is sized from
  the host and reuses prepared storage across transfers. To set a fixed limit or share a pool across
  clients, pass `MemoryConfig::Explicit` with a `memory::BufferPool` to `config::Builder::memory`.
- `Client::metrics` reports current memory use through `ClientMetrics::memory`.
- `StreamContext::part_buffer` gives custom `PartStream` implementations pool-backed part storage
  (`io::PartBuffer`), and `PartData::from_segmented` builds a part from a segmented payload.
- Runtime diagnostics, configured with `AWS_S3_TM_DIAGNOSTICS`: periodic memory-pool snapshots,
  allocator detail, and per-transfer request, retry, and wait summaries. Every transfer emits one
  terminal record on the `aws_sdk_s3_transfer_manager::transfer` tracing target at `DEBUG`.
- Conditional writes: `if_match` and `if_none_match` on uploads, sent on `PutObject` or, for a
  multipart upload, on `CompleteMultipartUpload`. A failed precondition is a service error with
  code `PreconditionFailed`.
- Network interface binding for the managed runtime: `S3ClientConfig::network_interfaces` and
  `ConfigLoader::network_interfaces` assign managed threads to the named interfaces round-robin.
  Available on Linux, Android, Apple platforms, illumos, Solaris, and Fuchsia.
- `S3ClientConfig::new` accepts `&SdkConfig` or an S3 config builder, and
  `config::Builder::s3_config` accepts anything convertible into an `S3ClientConfig`.

### Changed
- The managed runtime dispatches requests through one connection pool with a partition per managed
  thread, replacing its per-thread HTTP clients. Connections per host are capped at half the soft
  `RLIMIT_NOFILE`, between 10 and 4096, and a warning is logged when that cap is below the
  concurrency target. Idle connections close after 15 seconds instead of 90.
- The managed runtime's HTTP transport honors `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and
  `NO_PROXY`, as the SDK's default HTTPS client does.
- `CompleteMultipartUpload` sends `MpuObjectSize`, so S3 rejects an assembled object whose size
  differs from the upload's.
- Download chunks carry `memory::SegmentedBytes` in `ChunkOutput::data`. `into_contiguous`
  replaces `into_bytes`; `into_segments` and `Buf` are unchanged.
- `MemoryBudgetConfig::Fraction` outside `(0.0, 1.0]`, or where memory cannot be detected, fails
  pool construction with a `BufferPoolBuildError` instead of falling back.
- The minimum supported Rust version is 1.94.1.
- Requires `aws-smithy-http-client` 1.5.0, `aws-smithy-runtime` 1.16.0, and
  `aws-smithy-runtime-api` 1.19.0 or later.

### Removed
- `config::Builder::memory_budget` and `Config::memory_budget`. Use `config::Builder::memory` with
  `MemoryConfig::Explicit(BufferPool::builder().memory_budget(budget).build()?)`.
- `Client::memory_budget` and `types::MemoryBudgetSnapshot`. Use `Client::metrics().memory()`.
- `io::AggregatedBytes`, replaced by `memory::SegmentedBytes`.

### Fixed
- `UploadHandle::abort` panicked on the managed runtime when the transfer manager built its own S3
  client, which is the default configuration.
- An upload from a `PartStream` with no size hint panicked instead of uploading.
- The caller's `app/<id>` user-agent metadata was dropped from transfer-manager requests.
- Aborting a transfer, or joining one after a failure, waited for in-flight requests to finish, and
  waited indefinitely for a request that never completed. Executing work is now interrupted.
- Aborting while `CompleteMultipartUpload` was in flight, or after it failed, left the multipart
  upload and its parts in place.
- `DownloadHandle::object_meta` could wait indefinitely when discovery failed before it was called.

## [0.2.0] - 2026-07-18

A ground-up rearchitecture of the transfer manager. The public API keeps its shape; the machinery
beneath it is new.

### Added
- Adaptive concurrency: the number of in-flight requests is discovered at runtime — seeded from the
  instance and ramped toward the throughput the network sustains — rather than fixed at a constant.
- Bounded memory: a global memory budget and an occupancy-paced receive buffer cap resident memory
  independently of concurrency and consumer speed, so a fast network draining to a slow disk cannot
  grow memory without limit.
- Fair scheduling across concurrent transfers, so `upload_objects`/`download_objects` calls share
  throughput by transfer rather than by object count.
- Data integrity: checksum validation on upload and download, with a corrupt body failing the
  transfer rather than being silently retried.
- Resilience: recovery from download body-stream failures the SDK's own retry does not cover,
  throttle-storm recovery with per-bucket retry isolation, and speculative hedging of slow requests
  under a self-limiting budget.

### Changed
- Execution model: the transfer manager now runs its own per-core threads and dispatches work to
  them, replacing the shared general-purpose thread pool. This gives the client direct control over
  request ordering and placement for tighter latency behavior.

## [0.1.3] - 2025-09-08

### Added
- Validate the content range and request count of ranged GET requests.
- Validate field mappings between transfer manager and S3 input/output types.
- Validate the content length and part-number alignment of `UploadPart` requests.

## [0.1.1] - 2025-03-05

### Fixed
- Publishing on crates.io: add the crate README, fix the repository URL, and add the description,
  categories, and keywords.

## [0.1.0] - 2025-03-05

### Added
- Initial developer-preview release of a high-performance Amazon S3 client for Rust.
