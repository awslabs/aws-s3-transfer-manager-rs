# Standards

These standards apply to code, documentation, tests, and pull requests in this repository, whoever writes them. [reviewing.md](reviewing.md) applies them in review.

## Core standard

Write for the next maintainer or reviewer. Describe the current contract, behavior, rationale, and evidence. Do not narrate how the work evolved.

Use kernel-style technical prose: terse, precise, contract-first, and maintainer-focused.

Shipped documentation and code comments must contain no editorialization or project state. Do not include opinions, development phases, ticket status, temporary implementation notes, or descriptions of future work. Record those in planning documents or the issue tracker.

## Pull request descriptions

A PR description explains the assembled result, not the sequence of commits or the development process.

### Recommended structure

1. **Summary**
   - State what changes and why it matters.
   - Include the PR’s position and dependencies when part of a series.
   - Use a compact table when a series contains many PRs.

2. **Design and behavior**
   - Explain the important architecture and control flow.
   - Organize around reviewer concerns rather than files or commits.
   - Identify invariants, ownership boundaries, failure behavior, and compatibility implications.

3. **Developer experience**
   - Show normal configuration or usage where relevant.
   - Prefer representative examples over advanced or internal use cases.
   - Link to detailed configuration documentation rather than duplicating it.

4. **Structure**
   - Name the existing abstraction the change extends.
   - Justify each new module, type, or trait by why no existing one fits.
   - List what was moved or renamed.
   - Identify changes to components other operations share. Such a change carries its own changelog entry, and is usually its own pull request.

5. **Invariants**
   - List each invariant from the [design documents](../design/README.md#invariant-index) or module documentation that the change touches, and how it is preserved.
   - Write "none touched" only after checking.

6. **Correctness and qualification**
   - Explain what the tests establish, not merely which commands ran.
   - Name the test or CI lane that establishes each claim. A requirement or behavior without one is partial, not covered.
   - Separate unit, property, fuzz, concurrency, platform, integration, and real-service coverage where useful.
   - Say which validation ran locally and which remains for CI.
   - State known exclusions or deferred behavior explicitly.

7. **Related work**
   - Name open pull requests this change conflicts with or depends on, and how they reconcile.

Large pull requests add a **Review order**: the order in which to read files or types, and what to check in each.

The [pull request template](../../.github/pull_request_template.md) follows this structure.

### PR prose rules

- Lead with the user- or maintainer-visible outcome.
- Describe the final design rather than its development history.
- Avoid commit-by-commit walkthroughs.
- Use specific section names. Prefer “Property tests and fuzzing” over vague headings such as “Independent validation.”
- Give substantial concerns such as guard pages, unsafe boundaries, compatibility, or platform behavior their own sections.
- Use diagrams only when they materially clarify ownership, sequence, state transitions, or data flow.
- Keep each Markdown paragraph on one physical line. Do not insert hard line breaks within a paragraph.
- Use GitHub-flavored Markdown with blank lines around headings, lists, tables, and code blocks.
- Distinguish measured results, enforced regression barriers, and expected behavior.
- Do not call behavior a “policy” unless it genuinely defines a policy boundary or configurable choice.
- Avoid defensive or self-referential prose such as “this PR is not intended to.”
- Avoid editorial language such as “simply,” “just,” “obviously,” “clearly,” or “worth noting.”

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
- Tickets, milestones, phases, or temporary project state.
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

The module documentation of a public module is user documentation. Maintainer conventions for public modules belong in [architecture.md](architecture.md).

## Tests

Test names and comments state the invariant or failure being pinned.

A test comment should explain:

- The scenario being constructed.
- Why particular sizes, offsets, schedules, or geometries matter.
- What regression would make the assertion fail.

Avoid names that require unexplained implementation vocabulary. Derive boundary values from shared production constants where practical so tests cannot silently drift from the geometry they exercise.

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

- Summarize commands and results in the PR when they provide useful review evidence.
- Explain what each relevant test or gate establishes.
- Let CI retain ordinary command logs.
- Preserve artifacts for fuzz crashes, sanitizer failures, minimized reproducers, or benchmark results needed for review.
- Do not require every local test log to be uploaded.
- State which validation remains for CI or another environment.
- Never imply that a platform, sanitizer, service, or toolchain check ran when it did not.
