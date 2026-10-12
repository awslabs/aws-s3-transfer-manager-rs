# Pull request descriptions

A pull request description has three readers. A reviewer uses it to understand and evaluate the change. An agent uses it the same way, alongside or on behalf of that reviewer. A later maintainer reads it to learn why the code is the way it is. Write it for all three.

The description states the result, the argument for it, and what establishes each claim. It does not narrate how the work was done.

CI runs on every pull request, and every check is expected to pass. The description therefore never lists test counts, file or line counts, or commands that were run.

## Structure

Every description uses the same sections in the same order. A section appears only when its condition holds, and is only as long as its content. A small fix is a Summary and an Evidence section. A new subsystem uses every section.

| Section | Include when | Contents |
|---|---|---|
| `## Summary` | Always | What changes and why it matters. |
| `## Series` | The pull request is part of a series | The position line, or the full table in the pull request that merges the series. |
| `## Design` | There is user-facing behavior or a mechanism to explain | Usage and compatibility first, then one subsection per reviewer concern. |
| `## Interactions` | The change touches a component other code depends on | How the new behavior combines with that component's existing behavior. |
| `## How to review` | The reading order is not obvious from the diff | Files or symbols in reading order, each with what to check. |
| `## Evidence` | Always | What establishes each claim. |
| `## Scope` | Something is deferred, limited, or out of scope | Deferred work, known limitations, and non-goals. |

If the Summary needs a second "It also", the pull request holds two changes. Split it.

### Summary

The first paragraph stands alone: a reader can stop after it.

- **For a defect,** it says what a caller observed, under what conditions, and where the defect lives. It then says what happens now, and the outcome or error kind a caller sees.
- **For new behavior,** it gives the outcome, where it shows up (an API, a configuration, or a behavior), and whether existing behavior changes.

When the reason for the change needs more than a sentence, the following paragraphs give it as named failure modes or requirements, each with the observation that establishes it.

### Series

Each pull request in a series carries one position line: "Part N of the <series> series; depends on #X." The full table appears once, in the pull request that merges the series. That pull request's How to review states what changed beyond the already-reviewed parts. Its Scope carries the deferred items of the whole series. A series keeps one name, and its parts keep their first names.

A pull request that consumes work from another repository links that work. It also states the upstream defaults whose consequences reach this repository.

### Design

When public API, configuration, or user-visible behavior changes, Design opens with **Usage and compatibility**:

- Show normal usage, not advanced or internal use. Link to the rustdoc rather than duplicating it.
- For each existing behavior that changes, give the old outcome and the new one, for example an error where there was a wait.

The mechanism follows, in one subsection per reviewer concern. Each heading names the concern, not a file.

- Derive the design from the constraints that force it.
- State each invariant as one checkable sentence that names the type or function enforcing it.
- Use a table to list states or ownership by stage.
- Use a diagram only for ownership, sequence, state transitions, or data flow. Every cell of a diagram matches the code.
- Give a substantial concern, such as an `unsafe` boundary, guard pages, or platform behavior, its own subsection.

### Interactions

Design explains the change on its own. Interactions explains what happens when it meets existing behavior elsewhere: the scheduler's priority and fairness, buffer-pool admission, the retry and hedging loops, the runtime's threads, or an upstream crate's defaults.

For each component the change touches, state the combined behavior. For example, a new connection cap combined with a default reuse scope can let one thread poll another thread's file reads. A new fast path states its cost when it misses.

### How to review

- Start with the part that stands alone.
- A commit list serves as the review order only when each commit can be reviewed on its own and each entry says what to check.
- End with an **Also changed** line for each part of the diff outside the main concern: test harnesses, the mock server, benchmarks, CI, design documents.

### Evidence

- Name the test or model that pins each claim, and what fails if the behavior regresses. For a fix, this is one line per test.
- For concurrency, name the Loom model that covers each new shared-state path, or state that no model covers it.
- When no test establishes a claim, say what supports it instead: reasoning, a benchmark, or a real-service run.
- Say whether a number is a local observation or a threshold that CI enforces.

### Scope

Scope is not a roadmap. A review finding deferred to later work is added here before merge.

### Skeleton

A fix often needs only two sections:

```markdown
## Summary

<What a caller observed, when, and where the defect lives (`Type::method`). What happens now, and the outcome or error kind a caller sees.>

## Evidence

- `test_name`: <the regression it catches>.
```

The full set, in order:

```markdown
## Summary
## Series
## Design
### Usage and compatibility
### <Reviewer concern>
## Interactions
## How to review
## Evidence
## Scope
```

## Claims

- **Scope every claim.** Name the path or conditions it holds on. If completion waits for writes only on the success path, write "On success, completes only after every write lands", not "Completes only after every write lands".
- **A claim that something does not happen** names the test that holds it. A because-clause is not evidence.
- **Name failure modes by mechanism:** "panics", "returns `Err`", or "fails the request". Not "rejects".
- **Give each new constant its source,** such as a specification, parity with another implementation, or a measurement, or label it an estimate.

## Vocabulary

- Define each new term where it first appears. When a change introduces more than two new terms, define them together in Design. Say whether each is this project's vocabulary or borrowed from a protocol or library.
- A term keeps one meaning throughout the description.
- Type, function, and module names match the final diff.

## Posterity

- **In-repository documents** are referenced by path and section, for example "`docs/design/memory.md`, *Claim-trim gate*". The path stays valid after merge.
- **Code and external pages** are linked by commit permalink. A link that names a branch breaks when the branch is deleted on merge. A link that names a commit SHA keeps resolving.
- **Internal tracker ids** appear only as bare references, never with their contents.
- **Rebase status, CI notes, and "will update after X"** go in comments, not the description. An approval that cites a note stops making sense once the note is edited away.
- **No TODO markers, author worklists, or open questions** in the body. Open questions go in a review comment or an issue.

## Prose and formatting

- Write in kernel style: terse, specific, contract-first. Do not use the first person or narrate development history.
- Avoid editorial words such as "simply", "just", "obviously", "clearly", and "worth noting". Avoid defensive phrasing such as "this PR is not intended to". A Scope list of deferred items is not defensive.
- Call behavior a "policy" only when it defines a policy boundary or a configurable choice.
- Use GitHub-flavored Markdown. Keep each paragraph on one physical line. Put blank lines around headings, lists, tables, and code blocks.
- Collapse code examples longer than about twenty lines in `<details>`, or link to the rustdoc.

## Before opening and before merge

Reconcile the description with the final diff:

- Every part of the diff appears in Design or in an Also changed line.
- Every test, type, and function the description names exists in the final diff, under that name.
- No link names a branch.
- The Summary is still true.
