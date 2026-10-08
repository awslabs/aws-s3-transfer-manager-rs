# Pull request descriptions

A pull request description has three readers. A reviewer decides from it what to check. An agent assists that reviewer. A later maintainer reads it to learn why the code is the way it is. Write it for all three.

The description states the result, the argument for it, and what establishes each claim. It does not narrate how the work was done.

CI runs on every pull request, and every check is expected to pass. The description therefore never lists test counts, file or line counts, or commands that were run.

## Choose a shape

| Shape | Use it for |
|---|---|
| [Fix](#fix) | One concern with a small diff, typically under about 300 changed lines: a defect, a missing check, or a narrow behavior change. |
| [Change](#change) | Everything else. |
| [Integration](#integration) | Merging an already-reviewed series of pull requests to the main branch. |

If the lead paragraph needs a second "It also", the pull request holds two changes. Split it.

## Fix

A fix has no headings. It has two to four paragraphs:

1. **The defect.** What a caller observed, under what conditions, and the symbol where it lives.
2. **The change.** The new behavior, the outcome or error kind a caller now sees, and the symbol where the check happens.
3. **Platform or compatibility differences.** Include this paragraph only when they exist, and name the mechanism of each.
4. **The tests.** One line per test: its name, and the regression it catches.

```markdown
<What a caller observed, when, and where the defect lives (`Type::method`).>

<What happens now: the outcome or error kind (`ErrorKind::IOError`) and where the check is (`SinkFactory::create`).>

- `test_name`: <the regression it catches>.
```

## Change

Start with a lead paragraph that has no heading. Then use the sections below in this order. Omit a section only when it has nothing to say.

```markdown
<Lead paragraph.>

## Why
## Design
### <A reviewer concern, e.g. "Who owns a reservation">
## Interactions
## Compatibility
## Review order
## Evidence
## Scope
```

- **Lead.** The outcome, where it shows up (an API, a configuration, or a behavior), and whether existing behavior changes. A reader can stop after this paragraph.
- **Why.** The problem, as named failure modes or requirements. Each comes with the observation or measurement that establishes it.
- **Design.** One subsection per reviewer concern. The heading names the concern, not a file.
  - Derive the design from the constraints that force it.
  - State each invariant as one checkable sentence that names the type or function enforcing it.
  - Use a table to list states or ownership by stage.
  - Use a diagram only for ownership, sequence, state transitions, or data flow. Every cell of a diagram matches the code.
- **Interactions.** How the change composes with each shared component it touches: the scheduler, the buffer pool, retry, the runtime, or an upstream crate's defaults. A new fast path states its cost when it misses. Omit this section only when the change touches no shared component.
- **Compatibility.** User-visible behavior that changes: an error where there was a wait, a new or changed request field, a changed documented semantic. Required whenever public behavior changes.
- **Review order.**
  - List files or symbols in reading order, each with what to check. Start with the part that stands alone.
  - A commit list serves as the review order only when each commit can be reviewed on its own and each entry says what to check.
  - End with an **Also changed** line for each part of the diff outside the main concern, such as test harnesses, the mock server, benchmarks, CI, or design documents.
- **Evidence.** What establishes each claim.
  - Name the test or model that pins each behavior, and what fails if the behavior regresses.
  - For concurrency, name the Loom model that covers each new shared-state path, or state that no model covers it.
  - When no test establishes a claim, say what supports it instead: reasoning, a benchmark, or a real-service run.
  - Give each measurement its workload, host, before and after values, and whether the number is a baseline or an enforced threshold.
- **Scope.** Deferred work, known limitations, and non-goals. A review finding deferred to later work is added here before merge. This section is not a roadmap.

## Integration

```markdown
<Lead paragraph: what the series delivers, and whether existing behavior changes.>

## Series
| # | Pull request | Delivers |
## Developer experience
## Review focus
## Deferred
```

- **Series.** The full table of the series. Earlier pull requests in the series carry only a position line.
- **Developer experience.** Representative usage of new public API or configuration.
- **Review focus.** What changed beyond the already-reviewed pull requests, or a statement that nothing did.
- **Deferred.** The cumulative list of deferred items across the series.

## Series

Each pull request in a series opens its lead with one position line: "Part N of the <series> series; depends on #X." The full table appears once, in the integration pull request. The series keeps one name, and its parts keep the names they were given first.

A pull request that consumes work from another repository links that work. It also states the upstream defaults whose consequences reach this repository.

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
- Give a substantial concern, such as an `unsafe` boundary, guard pages, or platform behavior, its own Design subsection.

## Before opening and before merge

Reconcile the description with the final diff:

- Every part of the diff appears in Design or in an Also changed line.
- Every test, type, and function the description names exists in the final diff, under that name.
- No link names a branch.
- The lead is still true.
