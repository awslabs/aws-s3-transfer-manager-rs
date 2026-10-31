# AGENTS.md

These instructions are for coding agents working in this repository. The same rules apply to human contributors. [docs/dev/](docs/dev/README.md) holds the full maintainer documentation.

## Repository

- `aws-sdk-s3-transfer-manager/`: the crate.
- `integration-tests/`, `s3-mock-server/`: mock-S3 integration tests.
- `in-memory-s3/`: an in-memory S3 connector for test harnesses that run without a network.
- `wasm32-wasi-tests/`: a WASI command that runs transfers against an in-memory S3 on `wasm32-wasip2` and `wasm32-wasip3`.
- `benches/`: benchmarks.
- `fuzz/`: a separate Cargo workspace with its own lockfile.

[docs/dev/architecture.md](docs/dev/architecture.md) maps the crate's modules and states their boundaries and conventions. Read it before adding a module, type, or trait.

## Before changing a subsystem

Read the subsystem's module documentation (`//!`), then its design document from [docs/design/](docs/design/README.md). The design index lists every correctness invariant. A change must preserve each invariant it touches, or update the design document in the same pull request.

- The scheduler or any `Transfer` state machine: the module documentation in `scheduler/scheduler.rs`, `TransferContext::set_pending` in `transfer.rs`, and [scheduler.md](docs/design/scheduler.md).
- Memory and buffers: [memory.md](docs/design/memory.md).
- Download delivery: [paged-recv-buffer.md](docs/design/paged-recv-buffer.md).
- Retry and hedging: [retry.md](docs/design/retry.md) and [hedging.md](docs/design/hedging.md).

## Before adding code

Extend the existing machinery instead of building a parallel copy. Before adding a type, module, helper, or field:

1. Search for an existing one that does most of the job. Search for the concept, not only the name. If two copies already exist, extract the shared part instead of adding a third.
2. Match the layout of sibling modules.
3. Put shared code in the lowest module that owns the concept, so dependencies point down.
4. Reuse the crate's existing name for a concept that already has one.
5. Put test helpers in a `test_util` module, and make test seams traits.

[docs/dev/reviewing.md](docs/dev/reviewing.md) is the full review checklist. Apply it to your own change before opening a pull request.

## Writing

Documentation and comments state the contract: what an item does, its invariants, errors, panics, and safety obligations. They never contain history, project state, ticket or internal references, future plans, or editorial language. [docs/dev/standards.md](docs/dev/standards.md) has the full rules.

## Validation

Before handing off a source change:

```sh
cargo fmt --check
cargo clippy --all --tests --no-deps -- -D warnings
cargo test --locked --workspace
```

Some changes need a check that CI runs as a separate job:

| Change | Command |
|---|---|
| Documentation or doc comments | `RUSTDOCFLAGS="-Dwarnings --cfg docsrs" cargo +nightly doc --lib --no-deps --document-private-items` |
| Anything under `fuzz/` | `cargo fmt --manifest-path fuzz/Cargo.toml -- --check`, then `RUSTFLAGS='--cfg s3_tm_fuzz' .github/scripts/run-fuzz-targets.sh smoke` |
| Atomics, the wake protocol, `runtime/sync` | `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS='--cfg s3_tm_loom' cargo test -p aws-sdk-s3-transfer-manager --lib --release -- loom_tests` |
| `unsafe` code, buffer ownership | `MIRIFLAGS='-Zmiri-disable-isolation -Zmiri-strict-provenance' cargo +nightly miri test -p aws-sdk-s3-transfer-manager --lib` |
| Feature gates | `cargo hack check --all --feature-powerset` |

[.github/workflows/ci.yml](.github/workflows/ci.yml) defines the full matrix, which runs on every pull request.

## Pull requests

Write the description as [docs/dev/pull-requests.md](docs/dev/pull-requests.md) describes. It explains the assembled result, not the commit history.
