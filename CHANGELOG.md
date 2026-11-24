# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The `aws-s3-transfer` package includes `LICENSE` and `NOTICE`, and no longer includes
  development-only files such as `.gitignore`, `proptest-regressions/`, the external-types
  configurations and the integration tests.
- `aws-smithy-topology` requires `many_cpus` 2.4.16 or later instead of exactly 2.4.16.
- Declared minimum dependency versions are raised to the versions the dependency graph needs, so a
  build that resolves each dependency to its declared minimum succeeds.

## [release-2026-11-18]

Transfer Manager for Amazon S3 1.0.0 is published as `aws-s3-transfer`, renamed from
`aws-sdk-s3-transfer-manager`. From 1.0.0 its public API follows Semantic Versioning; the
non-default `__unstable_ext` feature remains experimental. Code written for 0.4 needs the changes
in [Upgrading from 0.4](#upgrading-from-04).

This release publishes:

- `aws-s3-transfer` 1.0.0, previously `aws-sdk-s3-transfer-manager` 0.4.0.
- `aws-s3-transfer-ext` 0.1.0, previously `aws-sdk-s3-transfer-manager-ext` 0.1.0. It requires
  `aws-s3-transfer` 1.0.0 exactly.
- `aws-smithy-topology`, `aws-smithy-execution` and `aws-smithy-managed-http` 0.1.1.

The minimum supported Rust version of every package is 1.96.0, up from 1.94.1.

### Upgrading from 0.4

#### Crate names

- Depend on `aws-s3-transfer` and import `aws_s3_transfer`. The extension is
  `aws-s3-transfer-ext`, imported as `aws_s3_transfer_ext`.
- Tracing targets start with `aws_s3_transfer::` instead of `aws_sdk_s3_transfer_manager::`.
  Update `RUST_LOG` and other filters that name them.

#### Moved paths

`config` holds everything `Config` accepts, `types` holds the value types, and `metrics` holds
observation snapshots only. Each type has one path; the old paths are removed, not re-exported.

| 0.4 | 1.0 |
| --- | --- |
| `RuntimeConfig`, `Managed`, `S3ClientConfig` at the crate root | `config::RuntimeConfig`, `config::Managed`, `config::S3ClientConfig` |
| `types::{PartSize, ReadAhead, ConcurrencyMode, TargetThroughput}` | `config::{PartSize, ReadAhead, ConcurrencyMode, TargetThroughput}` |
| `types::{MemoryConfig, MemoryBudgetConfig}`, also re-exported from `memory` | `config::{MemoryConfig, MemoryBudgetConfig}` |
| `types::TransferMetrics` | `metrics::TransferMetrics` |
| `metrics::unit::ByteUnit`, `metrics::{Throughput, ThroughputDisplayContext}` | `types::{ByteUnit, Throughput, ThroughputDisplayContext}` |
| `metrics::unit::ByteCountDisplayContext` | `types::ByteSizeDisplayContext` |
| `operation::upload::ChecksumStrategy` | `types::ChecksumStrategy` |

The crate root still exports `Client`, `Config` and `from_env`.

#### Configuration

- Sizes are `types::ByteSize` values: `PartSize::Target(ByteSize::mib(8))` replaces
  `PartSize::Target(u64)`, and `MemoryBudgetConfig::Limit(ByteSize::gib(2))` replaces
  `MemoryBudgetConfig::Limit(usize)`. `ByteSize::new`, `kib`, `mib` and `gib` are `const fn`;
  `as_u64`, `as_usize` and `as_unit` read the size back, and `Display` formats it in the largest
  binary unit that keeps the value at or above 1.
- `ByteUnit` only names a display unit and is `#[non_exhaustive]`. Its `as_bytes_u64`,
  `as_bytes_usize`, `as_bits_u64`, `as_bits_usize`, `convert` and `display` methods are removed:
  `ByteSize::mib(8)` replaces `8 * ByteUnit::Mebibyte.as_bytes_u64()`, `ByteSize::as_unit`
  replaces `convert`, and `ByteSize`'s `Display` replaces `display`. Create display contexts with
  `ByteSize::display_as` and `Throughput::display_as`; their fields are private and
  `ByteCountDisplayContext::new` is removed.
- `TargetThroughput::gbps(f64)` replaces `new_gigabits_per_sec(u64)`, so targets below 1 Gbps can
  be expressed. It returns `Result<TargetThroughput, BuildError>`, rejecting a value that is not
  finite and positive. One target covers uploads and downloads: `as_gbps()` replaces `upload()`
  and `download()`.
- `config::Builder::build` returns `Result<Config, BuildError>`, failing instead of panicking when
  `s3_config` is not set.
- The transfer manager no longer enables the `behavior-version-latest` feature of `aws-config` or
  `aws-sdk-s3`. Configuration loaded without a behavior version, such as with
  `aws_config::load_from_env`, needs one: use `aws_config::load_defaults(BehaviorVersion::latest())`
  or enable the feature in the application.
- Set framework metadata on the `SdkConfig` or S3 configuration, which now reaches the user agent
  (see Fixed). The hidden `framework_metadata` methods of `config::Builder`, `ConfigLoader` and
  `Config` are removed.

#### Runtime and HTTP

- `S3ClientConfig::network_interfaces` and `ConfigLoader::network_interfaces` are removed. To bind
  connections to network interfaces, build a `ManagedRuntime`, set the S3 configuration's HTTP
  client to an `aws_smithy_managed_http::client::ManagedHttpPool` over a connection pool whose
  partitions, one per runtime thread, bind to the interfaces, and adopt the runtime with
  `aws_s3_transfer_ext::managed`. `examples/cp.rs` builds this for `--network-interfaces`.
- The `dial9` feature and the `dial9_handle` methods of `config::Builder` and `ConfigLoader` are
  removed. To trace managed execution, pass the recorder's handle to
  `ManagedRuntime::builder().dial9_handle` and adopt the runtime with
  `aws_s3_transfer_ext::managed`. The `aws-s3-transfer-ext` `dial9` feature enables
  `aws-smithy-execution/dial9` only.

#### Upload checksums

`ChecksumStrategy` is built from an algorithm type in `types::checksum` (`Crc64Nvme`, `Crc32`,
`Crc32C`, `Sha1` or `Sha256`). `full_object` and `composite` name the checksum type a multipart
upload stores. They cannot fail: a combination S3 doesn't support doesn't compile.

| 0.4 | 1.0 |
| --- | --- |
| `with_calculated_crc64_nvme()`, `with_calculated_crc32()`, `with_calculated_crc32_c()` | `full_object(Crc64Nvme)` (also `ChecksumStrategy::default()`), `full_object(Crc32)`, `full_object(Crc32C)` |
| `with_crc64_nvme(v)`, `with_crc32(v)`, `with_crc32_c(v)` | `full_object(..).with_expected_value(v)` with the same algorithm |
| `with_calculated_*_composite_if_multipart()` | `composite(Crc32)`, `composite(Crc32C)`, `composite(Sha1)`, `composite(Sha256)` |
| `type_if_multipart()`, `full_object_checksum()` | `multipart_checksum_type()`, `expected_value()` |
| `ChecksumStrategy::builder()`, `ChecksumStrategyBuilder` | Removed |

#### Errors

- `error::Error` no longer implements `From<tokio::task::JoinError>`,
  `From<std::sync::PoisonError<T>>`, or `From<SdkError<E, HttpResponse>>` for the `aws-sdk-s3`
  operation errors. Construct an error of the intended kind with `Error::new`. Errors returned by
  transfers are unchanged.
- `io::error` is no longer public. `InputStream::from_path` and `PathBodyBuilder::build` return
  `error::Error`: `ErrorKind::InputInvalid` when no path is set or the offset is past the end of
  the file, and `ErrorKind::IOError`, with the `std::io::Error` as its source, when reading file
  metadata fails, such as for a file that does not exist.
- A download response whose `Content-Range` doesn't match the requested range fails with
  `ErrorKind::ServiceError` carrying the operation and request ids, instead of
  `ObjectNotDiscoverable` during discovery and `RuntimeError` afterwards.
- A failure of the transfer manager's own file reader other than a read error, such as a panicked
  read task, is `ErrorKind::RuntimeError` instead of `ErrorKind::IOError`.
- The source of an `ErrorKind::ChildOperationFailed` error from `upload_objects` or
  `download_objects` is the error of the first failed object, instead of a message naming it.
- `io::walk::WalkErrorKind` is `#[non_exhaustive]`.

#### Custom part streams and memory

- A `PartStream` ends an upload by returning `None` from `poll_part`; `PartData::mark_last` is
  removed.
- `StreamContext::part_buffer` is no longer public. Create a buffer with
  `PartBuffer::new(&pool, stream_cx.part_size())`, charged to a `memory::BufferPool` you own, and
  share the pool with the client through `MemoryConfig::Explicit` to bound their memory together.
- `PartBuffer` no longer implements `bytes::BufMut`: `PartBuffer::writable` replaces
  `remaining_mut`, and `PartBuffer::fill_from_slice`, which copies as much of a slice as fits and
  returns the count, replaces `put_slice`.
- `memory` exports `BufferPool`, `BufferPoolBuilder`, `BufferPoolBuildError` and `SegmentedBytes`.
  `BufferPool::reserve`, `try_reserve`, `acquire`, `acquire_unreserved` and `carrier_size`, and
  `PooledBufMut`, `Reservation`, `ReserveFuture`, `AcquireError` and `ReserveError` are no longer
  public. `PartBuffer::poll_acquire` still reports pool failures as `std::io::ErrorKind::Other`.

#### Metrics

- `TransferMetrics::elapsed` replaces `started_at` and `finished_at`: the time from when the
  transfer was initiated until it recorded its finish, or until the snapshot if it has not. A
  handle's `status` reports whether the transfer has finished.
- `MemoryMetrics` getters are renamed: `configured_capacity_bytes` to `capacity_bytes`,
  `admission_used_bytes` to `admitted_bytes`, `charged_capacity_bytes` to `in_use_bytes`,
  `queued_reservations` to `waiting_requests`, and `reservation_enqueues_total` to `waits_total`.
  `committed_bytes`, returning `Option<u64>`, replaces `prepared_capacity_bytes`.
  `active_planned_demand_bytes` and `admission_overage_bytes` are no longer public.

#### Downloads

- `download_objects` writes each object at its full key under the destination; the prefix only
  selects objects. With the prefix `photos/`, the key `photos/a.txt` was written to
  `<dest>/a.txt` and is now written to `<dest>/photos/a.txt`. A prefix that ends partway through a
  name no longer cuts it: with the prefix `photos`, `photos-other/x.txt` was written to
  `<dest>/-other/x.txt`.
- `DownloadInput::part_number` and its getter are no longer public. Single-part downloads are not
  supported.

### Added

- Whole-object downloads verify a stored CRC32, CRC32C or CRC64NVME full object checksum, including
  when the object is downloaded in independent ranges and when it is empty. A mismatch fails
  `join()` with `ErrorKind::IntegrityError` before the destination is finalized; streamed bytes
  can arrive before the final check. `IntegrityChecks::checksum_validation` reports `Validated`
  when every delivered byte was covered by a matching checksum, which it previously never did.
- `IntegrityChecks` reports stored SHA-512, MD5, XXHASH64, XXHASH3 and XXHASH128 values through
  `checksum_sha512`, `checksum_md5`, `checksum_xxhash64`, `checksum_xxhash3` and
  `checksum_xxhash128`, and `NotValidatedReason::UnsupportedAlgorithm` reports a download whose
  responses carried checksums only in algorithms the SDK does not compute, such as MD5.
- A composite `ChecksumStrategy`, including SHA-1 and SHA-256, can carry an expected value. A
  single-request upload sends it as the PutObject checksum; a multipart upload with one fails
  `initiate` with `ErrorKind::InputInvalid` before any request is sent.
- `Error::upload_id` returns the ID of a multipart upload that a failed upload left open in S3:
  under `FailedMultipartUploadPolicy::Retain`, when AbortMultipartUpload failed, or when a worker
  panic, such as one in the caller's `PartStream`, failed the upload after the multipart upload
  was created.
- `config::ConfigLoader` is public, and gains `s3_config`, which uses an existing `SdkConfig` or
  `S3ClientConfig` in place of loading the shared AWS configuration, and `read_ahead`, which sets
  the default download read-ahead.
- `UploadObjectsInput::builder`, matching the other operation inputs, and `set_failure_policy` on
  the `upload_objects` and `download_objects` input and fluent builders.
- Client construction logs the resolved sizing at `INFO`: the concurrency target and the signal it
  came from, the memory ceiling, the part size and the connection cap. It logs a `WARN` when the
  part size exceeds the memory ceiling, or when the soft file-descriptor limit holds connections
  per host below the concurrency target.
- `aws-smithy-execution` and `aws-smithy-managed-http` 0.1.1: the documentation on docs.rs covers
  every feature and labels the items each feature enables. `aws-smithy-topology` 0.1.1 changes only
  its minimum supported Rust version.

### Changed

- Whole-object downloads with checksum validation enabled start with a checksum-mode HeadObject,
  so `IntegrityChecks` reports the stored checksum values for split downloads too, except a
  composite value, for which it reports only the type. Small nonempty objects take a HEAD and a
  GET instead of one GET, except in `download_objects`, which downloads a listed single-part object
  no larger than the part size with one GET pinned to its listed ETag, falling back to HEAD if the
  object changed since the listing. Caller-range downloads and downloads with validation disabled
  are unchanged.
- A download that fails at HeadObject reports the error as S3 answers a HEAD: with the status and
  request ids but no message. The error code is `PreconditionFailed` for 412, `NotModified` for
  304, `InvalidRange` for 416, and `NotFound` for 404, where GetObject reported `NoSuchKey`.
- The automatic part size is 8 MiB for uploads and downloads. Downloads used 5 MiB, so objects
  larger than 5 MiB and up to 8 MiB now download with one GET.
- Automatic sizing resolves the concurrency target, memory ceiling, part size and connection cap
  together when the client is built. The memory ceiling and the connection cap no longer lower the
  concurrency target; requests beyond either wait for memory or a connection. A client's automatic
  memory ceiling is raised to at least 256 MiB, but not above half of detected memory; a
  `BufferPool` built with `MemoryBudgetConfig::Auto` is unchanged. The per-host connection cap
  budgets at most half the soft `RLIMIT_NOFILE` and leaves at least 64 descriptors free. Sizing
  recognizes more EC2 instance types, and sizes instances with several network cards by one card's
  bandwidth.
- Errors from a local filesystem operation on a known path name the operation and the path, as in
  `I/O error creating directory "out/logs"`, and the object key in `download_objects`. Their
  source is the `std::io::Error` itself, or the `io::walk::WalkError` for the directory walk.
- A ranged GET that covers the whole object accepts a `200 OK` response without `Content-Range`
  when its `Content-Length` is the object's length, as from a server that ignores `Range`.
- The minimum `aws-sdk-s3` is 1.158.0.

### Fixed

- An upload fails with `ErrorKind::InputInvalid`, before any request, when a checksum the caller
  supplied cannot be sent: a `PartStream` part with a checksum on an upload with no
  `ChecksumStrategy`, or a `content_md5` on a multipart upload.
- `IntegrityError::algorithm()` names the algorithm on a checksum mismatch; it was always `None`.
  A download no longer reports a checksum value that does not cover the whole object.
- Validated multipart downloads no longer stall on a capped connection pool by sending a discovery
  request while holding an unread first-range response.
- The metrics in an upload's output are the transfer's metrics at completion. A single-request
  upload reported zero `network_tx`, and zero `disk_read` for a file source.
- Framework metadata set on the `SdkConfig` or S3 configuration is sent in the `x-amz-user-agent`
  header as `lib/<name>/<version>`, in the order configured and without duplicates. It was
  missing.
- `InputStream::from_path` and `PathBodyBuilder::build` reject a path that is not a regular file,
  such as a directory, a device, a FIFO or a Windows pipe, and `PathBodyBuilder::build` without a
  path returns an error instead of panicking. Both are `ErrorKind::InputInvalid`.
- `upload_objects` with a custom delimiter could map two files to one key, such as `a-/b` and
  `a/-b` with the delimiter `--`, and report both uploaded while the second replaced the first. It
  now fails the second file with `ErrorKind::InputInvalid`, naming the first, under the
  `FailedTransferPolicy`.
- `upload_objects` with an empty `key_prefix` uploads each file under its relative path instead of
  a key with a leading `/`.
- `download_objects` resolves its destination lexically, without following symbolic links. It
  accepts a destination containing `.` or `..`, such as `./out`, for which it rejected every
  object, and rejects a key that resolves outside a destination made of `..` components, such as
  `../b.txt` under `..`, which it accepted.
- `download_objects` creates a missing destination directory and its parents instead of failing,
  and fails an object whose key maps to the destination itself, such as `.` or `a/..`, with
  `ErrorKind::InputInvalid` before requesting it.

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
