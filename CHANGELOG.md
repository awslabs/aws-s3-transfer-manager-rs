# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `AbortHandle`, returned by `ManagedDownloadHandle::abort_handle`. After a timeout drops the `join`
  future, `AbortHandle::abort` cancels the download and returns when no write or resize of the
  destination is still running, so a file passed to `write_to_file` is safe to reuse. It returns an
  `AbortOutcome`. Dropping a handle still cancels without waiting.
- `UploadObjectsInput::builder`, matching the other operation inputs.
- `set_failure_policy` on `UploadObjectsInputBuilder`, `DownloadObjectsInputBuilder`,
  `UploadObjectsFluentBuilder`, and `DownloadObjectsFluentBuilder`. `None` selects the default,
  `FailedTransferPolicy::Abort`.
- `ConfigLoader::s3_config` uses an existing `SdkConfig` or `S3ClientConfig` in place of loading
  the shared AWS configuration; `load` still detects the machine profile. A non-empty list set with
  `ConfigLoader::network_interfaces` replaces the supplied configuration's network interfaces, and
  an empty list keeps them.
- `ConfigLoader::read_ahead` sets the client's default download read-ahead, as
  `config::Builder::read_ahead` does.
- `ConfigLoader` is exported as `config::ConfigLoader`.
- `Error::upload_id` returns the ID of a multipart upload that a failed upload left open in S3:
  under `FailedMultipartUploadPolicy::Retain`, when AbortMultipartUpload failed, or when a worker
  panic, such as one in the caller's `PartStream`, failed the upload after the multipart upload
  was created. It is `None` when the multipart upload was aborted.
- Client construction logs one `INFO` record of the resolved sizing (concurrency target and the
  signal it came from, memory ceiling, part size, connection cap), a `WARN` when the part size
  exceeds the memory ceiling, and a `WARN` when the soft file-descriptor limit holds connections
  per host below the concurrency target.
- `aws-sdk-s3-transfer-manager`: downloads report `ChecksumValidation::Validated` when a response
  checksum covered every delivered byte and every one of those checksums matched.
  `IntegrityChecks::checksum_validation` previously never reported `Validated`.
- `aws-sdk-s3-transfer-manager`: `NotValidatedReason::UnsupportedAlgorithm`, reported when a
  download's responses carried checksums only in algorithms the SDK does not compute, such as MD5.

### Changed
- `aws-sdk-s3-transfer-manager`, `aws-sdk-s3-transfer-manager-ext`, `aws-smithy-topology`,
  `aws-smithy-execution` and `aws-smithy-managed-http`: the minimum supported Rust version is
  1.96.0.
- `error::Error` no longer implements `From<tokio::task::JoinError>` or
  `From<std::sync::PoisonError<T>>`. Construct an error of the intended kind with `Error::new`.
- `io::error` is no longer public. `InputStream::from_path` and `PathBodyBuilder::build` return
  `error::Error`: a missing path or an offset past the end of the file is
  `ErrorKind::InputInvalid`, and a failure reading file metadata, such as a missing file, is
  `ErrorKind::IOError` with the `std::io::Error` as its source.
- A failure of the transfer manager's own file reader that is not a read error, such as a panicked
  read task, is `ErrorKind::RuntimeError` instead of `ErrorKind::IOError`.
- `PartData::mark_last` is removed. A `PartStream` ends an upload by returning `None` from
  `poll_part`.
- `DownloadInput::part_number` and its getter are no longer public. Single-part downloads are
  not supported.
- `io::walk::WalkErrorKind` and `metrics::unit::ByteUnit` are `#[non_exhaustive]`.
- The fields of `metrics::ThroughputDisplayContext` and `metrics::unit::ByteCountDisplayContext`
  are private. Create them with `Throughput::display_as`, `ByteUnit::display`, or
  `ByteCountDisplayContext::new`.
- `config::Builder::build` returns `Result<Config, BuildError>` and returns an error when
  `s3_config` is not set, instead of panicking.
- The transfer manager no longer enables the `behavior-version-latest` feature of `aws-config` or
  `aws-sdk-s3`. Configuration loaded without a behavior version, such as with
  `aws_config::load_from_env`, needs one: use `aws_config::load_defaults(BehaviorVersion::latest())`
  or enable the feature in the application.
- The `dial9` feature, `config::Builder::dial9_handle`, and `ConfigLoader::dial9_handle` are
  removed. To trace managed execution, pass the recorder's handle to
  `ManagedRuntime::builder().dial9_handle`, then adopt the runtime with
  `aws_sdk_s3_transfer_manager_ext::managed`. The `aws-sdk-s3-transfer-manager-ext` `dial9` feature
  enables `aws-smithy-execution/dial9` only.
- The hidden `config::Builder::framework_metadata`, `ConfigLoader::framework_metadata`, and
  `Config::framework_metadata` are removed. Set framework metadata on the `SdkConfig` or S3
  configuration.
- `ErrorKind::IOError` and `ErrorKind::InputInvalid` errors from a local filesystem operation on a
  known path name the operation and the path, as in `I/O error creating directory "out/logs"`,
  and the object key for a `download_objects` object.
  This covers temporary file creation and renaming, for downloads to a path and for
  `download_objects` objects, directory creation in `download_objects`, opening and reading upload source files, and the directory walk. Except for
  the directory walk, whose source remains the `io::walk::WalkError`, the error's source is the
  `std::io::Error` itself, where some of these sites previously used a message that embedded it,
  such as `failed to open …` or `failed to create directory for key …`.
- The source of an `ErrorKind::ChildOperationFailed` error from `upload_objects` or
  `download_objects` is the error of the first failed object, the one that stopped the transfer
  under `FailedTransferPolicy::Abort`, instead of a message naming it.
- `download_objects` writes each object at its full key under the destination. The walker's
  prefix only selects objects and is no longer removed from local paths: with the prefix
  `photos/`, the key `photos/a.txt` was written to `<dest>/a.txt` and is now written to
  `<dest>/photos/a.txt`. A prefix that ends partway through a name no longer cuts it: with the
  prefix `photos`, the key `photos-other/x.txt` was written to `<dest>/-other/x.txt`.
- `TargetThroughput::gbps` replaces `new_gigabits_per_sec`. It takes an `f64` instead of a `u64`,
  so targets below 1 Gbps can be expressed, and returns `Result<TargetThroughput, BuildError>`,
  rejecting a value that is not finite and positive.
- `TargetThroughput` holds one target for uploads and downloads. `upload()` and `download()` are
  replaced by `as_gbps()`.
- The concurrency target, payload-memory ceiling, part size, and connection cap are resolved
  together at client construction. The concurrency target is not lowered by the memory ceiling
  or the connection cap: requests beyond either wait for memory admission or a connection.
- The automatic part size is 8 MiB for both uploads and downloads. Downloads previously used
  5 MiB, so objects larger than 5 MiB and up to 8 MiB now download with a single GET.
- The per-host connection cap's descriptor budget is the smaller of half the soft
  `RLIMIT_NOFILE` and the soft limit less 64 descriptors, so it binds only below a soft limit
  of 128.
- The automatic memory ceiling for a client is at least 256 MiB, limited to half of detected
  memory. `BufferPool` built with `MemoryBudgetConfig::Auto` directly is unchanged.
- `aws-sdk-s3-transfer-manager`: the minimum `aws-sdk-s3` is now 1.158.0, the first release that
  reports the response checksum outcome the verdict is built from.
- The bandwidth table adds m5n, m5dn, r5n, r5dn, m5zn, x2iezn, d3, d3en, is4gen, c8ine, g4dn
  and g6e, and uses one network card's bandwidth for p5, p5e, p5en, p6-b200, p6-b300, trn1 and
  trn1n.

### Fixed
- `PathBodyBuilder::build` without a path returns an `ErrorKind::InputInvalid` error instead of
  panicking.
- Framework metadata set on the `SdkConfig` or S3 configuration was missing from the
  `x-amz-user-agent` header of requests the transfer manager sent. The header now carries each
  entry as `lib/<name>/<version>` alongside the transfer manager's attribution, in the order
  configured and without duplicates.

### Fixed
- `upload_objects` with a custom delimiter could derive one key from two files, such as `a-/b` and
  `a/-b` with the delimiter `--`, and report both as uploaded while the later write replaced the
  earlier one. It now fails the second file with `ErrorKind::InputInvalid`, names the first file, and
  follows the `FailedTransferPolicy`. The default and native delimiters keep no key registry.
- `upload_objects` with an empty `key_prefix` uploads each file under its relative path instead of
  a key with a leading `/`.
- `download_objects` accepts a destination containing `.` or `..` components, such as `./out` or
  `out/../out`, instead of rejecting every object as resolving outside it. The destination is
  resolved lexically, without following symbolic links.
- `download_objects` rejects a key that resolves outside a destination made of `..` components, such
  as `../b.txt` under `..`, which was previously accepted.
- `download_objects` creates a destination directory that does not exist, with any missing
  parents, instead of failing.
- `download_objects` fails an object whose key maps to the destination directory itself, such as
  `.`, `./`, or `a/..`, with `ErrorKind::InputInvalid` before creating any file or requesting the
  object. Such an object was previously downloaded to a temporary file beside the destination
  and then failed to be renamed onto it.
- `aws-sdk-s3-transfer-manager`: `IntegrityError::algorithm()` now names the algorithm on a
  checksum mismatch. It was always `None`.

## [release-2026-11-05]

### Added
- `aws-smithy-topology` 0.1.0: CPU and NUMA discovery, execution placement targets, and
  explicit CPU binding for use independently of the transfer manager.
- `aws-smithy-execution` 0.1.0: managed partitioned execution with targeted task submission
  and explicit shutdown completion.
- `aws-smithy-managed-http` 0.1.0: native Smithy pool composition with managed execution
  and the Smithy HTTP client interface, enabled by the `client` feature.
- `aws-sdk-s3-transfer-manager-ext` 0.1.0: adoption of caller-configured managed execution
  through TM's `__unstable_ext` feature and an exact TM version dependency.
- The crate builds for `wasm32-wasip1`, `wasm32-wasip2`, and `wasm32-wasip3` with
  `RUSTFLAGS="--cfg tokio_unstable"`. On these targets the application supplies the HTTP client,
  `Managed` execution runs on the caller's tokio runtime, which may be current-thread, and file
  I/O runs inline. The crate documentation lists the requirements.
- Experimental managed-runtime adoption through the non-default `__unstable_ext` feature,
  used by the new `aws-sdk-s3-transfer-manager-ext` adapter. The adapter transfers runtime
  ownership into opaque TM configuration. Managed pools implement the Smithy HTTP client
  interface independently in `aws-smithy-managed-http`. Configure adopted-runtime tracing
  on its execution builder.

### Changed
- `RuntimeConfig` and opaque `Managed` replace `RuntimeMode`; builders and loaders use
  `runtime_config`. Managed execution uses reusable runtime components while the transfer
  manager retains runtime ownership and scheduling.
- An HTTP client configured on the SDK or S3 configuration is preserved under either execution
  mode. Managed execution supplies its transport only when HTTP is absent. The
  `S3ClientConfig::enable_runtime_http` switch is removed; configure HTTP through the SDK/S3
  configuration instead. File bodies using configured HTTP offload filesystem reads because
  the transport controls their polling threads.
- The `dial9` feature depends on `dial9` 0.5 instead of `dial9-tokio-telemetry` 0.3, and compiles
  without `--cfg tokio_unstable`. Per-task poll, spawn, and terminate events and per-worker queue
  depth for the managed runtime are recorded only when built with the flag.
- `config::Builder::telemetry_guard` and `ConfigLoader::telemetry_guard` are renamed to
  `dial9_handle` and take a `dial9::Dial9Handle`. The caller owns the `dial9::Recorder`: pass
  `recorder.handle().clone()`, and call `Recorder::graceful_shutdown` after the client, its clones,
  and its transfer handles are dropped.

### Fixed
- `ByteUnit::as_bits_usize` defined `Gibibyte` as `1 << 33`, which overflows a 32-bit `usize`
  and stopped the crate from compiling on such targets. `as_bits_u64` now computes every unit in
  `u64`, and `as_bits_usize` panics for `Gibibyte` where `usize` is 32 bits. Values on 64-bit
  targets are unchanged.
- A failed or cancelled upload kept its `PartStream` until the `UploadHandle` and the transfer
  itself were dropped, so a producer waiting for the upload to take its next part waited
  indefinitely while the caller held the handle to learn why the upload failed. The upload now
  drops a `PartStream` when it fails or is cancelled, and a producer observes that through the
  stream's own drop, such as its channel closing. An upload ended by a dropped handle, a cancelled
  parent, or a worker panic sends no AbortMultipartUpload; it now logs the ID of a multipart upload
  it leaves open at `WARN`.
  ([#226](https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/226))
- A finished transfer could be retained for the life of the process, and the client with it:
  dropping the `Client` did not shut down its runtime. A download to disk (`write_to_path`,
  `write_to_file`, or `download_objects`) kept its file open, preventing opens that deny write
  sharing on Windows and keeping deleted files' storage allocated. Ready-set entries now hold weak
  descriptors, so transfers and their resources are released when the registry and remaining
  transient owners retire, without waiting for epoch reclamation. The destination file can remain
  open after `join` or `abort` returns while transfer cleanup finishes.

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
