# Standards

These standards apply to code, documentation, and tests in this repository, whoever writes them. [pull-requests.md](pull-requests.md) covers pull request descriptions, and [reviewing.md](reviewing.md) applies both in review.

## Core standard

Write for the next maintainer or reviewer. Describe the current contract, behavior, rationale, and evidence. Do not narrate how the work evolved.

Use kernel-style technical prose: terse, precise, contract-first, and maintainer-focused.

Shipped documentation and code comments must contain no editorialization or project state. Do not include opinions, development phases, ticket status, temporary implementation notes, or descriptions of future work. Record those in planning documents or the issue tracker.

## Documentation comments

Document an item’s contract, not its current usage.

A reader should understand the item without reading its callers.

Document:

- What the item does.
- Inputs whose meaning is not apparent from their types.
- Return-value semantics.
- Invariants and ordering guarantees.
- Preconditions and postconditions.
- Exact error and panic conditions.
- Safety obligations.
- Non-obvious rationale when it affects correct use or maintenance.

Do not document:

- Current callers or module wiring.
- Implementation narration.
- Names and types without additional meaning.
- Tickets, milestones, phases, or temporary project state. A regression test's issue reference is the exception; see [Tests](#tests).
- Change history.
- Future plans.
- Editorial opinions.

Most items require only one precise sentence. Add more detail only when the contract requires it.

```rust
/// Returns the number of bytes accepted from the source.
///
/// The count includes bytes assigned to in-flight work and therefore may exceed
/// the number of bytes whose requests have completed.
```

## Inline comments

Inline comments explain why code has a non-obvious shape. They do not narrate what the next statement does.

Useful subjects include:

- Correctness-critical ordering.
- Why a particular memory ordering is sufficient.
- Why an apparently redundant check closes a race.
- Arithmetic bounds or geometry chosen for a regression test.
- Why a fallback occurs only after another path is exhausted.
- A safety invariant that is not apparent from the surrounding types.

Inline comments must contain no editorialization or project state. Durable follow-up work belongs in the issue tracker or planning documents.

## Module and crate documentation

Module and crate documentation describes:

- The abstraction the module provides.
- Its ownership and responsibility boundaries.
- The primary types a maintainer should begin with.
- Important invariants shared by the module.

It is not a feature list, roadmap, benchmark report, or project-status document.

The module documentation of a public module is user documentation.

## Tests

Test names and comments state the invariant or failure being pinned.

A test comment should explain:

- The scenario being constructed.
- Why particular sizes, offsets, schedules, or geometries matter.
- What regression would make the assertion fail.

Avoid names that require unexplained implementation vocabulary. Derive boundary values from shared production constants where practical so tests cannot silently drift from the geometry they exercise.

A test that pins a reported issue or a fix references the issue in a comment above it:

```rust
// ref: awslabs/aws-s3-transfer-manager-rs#123
```

When there is no public issue, reference the internal tracker id instead (`// ref: RUST-123`). Prefer a public issue, since outside contributors cannot resolve an internal id.

A passing test must establish the named behavior. Do not discard operation results or allow unrelated failures to make an assertion pass vacuously.

Test helpers follow the `test_util` convention in [architecture.md](architecture.md#conventions).

## Code-change quality bar

Every behavioral change includes evidence proportional to its risk. Bug fixes and correctness changes should include a regression test that fails without the fix and passes with it. Refactors preserve existing coverage; documentation and mechanical changes do not require artificial tests.

Run the relevant local checks before handoff. The required CI matrix provides exhaustive platform and toolchain qualification.

### Universal baseline

For source changes, run the applicable repository checks for:

- Formatting.
- Clippy with warnings denied.
- Relevant unit and integration tests.
- Documentation tests when public APIs, examples, or documentation change.

No single Cargo command covers this project’s complete validation surface. The CI matrix separately exercises nextest, documentation tests, feature combinations, platforms, sanitizers, fuzz targets, Miri, and Loom.

### Risk-specific evidence

| Change surface | Expected evidence |
|---|---|
| Bug fix | Regression test that fails against the unfixed implementation when practical |
| State machine or scheduling | Deterministic transition or interleaving tests; Loom when synchronization is involved |
| Unsafe memory or buffer ownership | Focused tests plus Miri, ASan, model/property tests, or fuzzing as applicable |
| Broad input or state space | Property tests or model-based tests |
| Byte decoder or externally supplied binary input | Curated corpus replay and fuzz-target validation |
| Platform-specific code | Compile or execute the affected Windows, Unix, Android, FreeBSD, or musl shape |
| Protocol or S3 behavior | Mock integration tests that reproduce S3 behavior; real-S3 qualification when mocks cannot establish the contract |
| Public API | Documentation, examples, doc tests, and relevant feature combinations |
| Performance-sensitive path | A regression barrier or benchmark tied to the claimed property; representative workload qualification for material changes |

### Red-first testing

For a bug or correctness fix, red-first testing should genuinely distinguish the defect:

1. Reproduce the original failure.
2. Add a test that fails against the unfixed implementation.
3. Apply the fix and verify that the test passes.

For concurrency, unsafe code, model oracles, and arithmetic invariants, mutation or negative-control testing may provide stronger evidence than reproducing one historical failure.

Do not manufacture red-first tests for documentation, formatting, renames, or behavior-preserving mechanical refactors.

### Validation records

- CI runs the full matrix on every pull request, and every check is expected to pass. Do not record test counts, commands run, or local results that CI repeats.
- Explain what each new or changed test establishes.
- Record qualification that CI does not run, such as benchmarks, real-service runs, or manual platform runs, with its method and result.
- Preserve artifacts for fuzz crashes, sanitizer failures, minimized reproducers, or benchmark results needed for review.
- Never imply that a platform, sanitizer, service, or toolchain check ran when it did not.

### Sanitizer targets

Required sanitizer jobs run on Linux x86_64 using the pinned nightly from
`.github/workflows/ci.yml`. The `x86_64-unknown-linux-gnuasan` and
`x86_64-unknown-linux-gnutsan` targets instrument the standard library as well as
workspace code. Install those targets with `rustup target add` and provide an
LLVM symbolizer. Run workspace tests with `sdk-v1`, the matching `s3_tm_asan` or
`s3_tm_tsan` cfg, frame pointers, and one test thread as the workflow specifies.

ASan leak detection and TSan thread-leak detection are enabled. Lifecycle tests
must release running blocking work and await owned-runtime thread completion.
Caller-owned runtimes require their own teardown. Task cancellation or a signal
before thread-local destruction is insufficient evidence of thread completion.

The TSan suppression file permits only the named Tokio reactor registration
race whose ordering crosses kernel delivery. It does not suppress runtime
thread leaks or transfer-manager races. Preserve the intentional sanitizer
handling of guard-page death tests and buffer poisoning.
