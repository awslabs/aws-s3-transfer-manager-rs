# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - Unreleased

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
