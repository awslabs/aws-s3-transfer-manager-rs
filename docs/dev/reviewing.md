# Reviewing

This guide applies to reviews by maintainers, contributors, and agents. It also applies to an author checking a change before opening a pull request.

Review in two passes. The first pass settles structure. The second checks correctness and contracts in code whose shape is settled. A finding on code that a structural change will move is deferred to the second pass. Its reproduction is kept for that pass.

## Pass 1: structure

Answer each question with evidence, a path and a symbol, not with an impression. The questions apply to every added or moved module, type, and trait, and to every file with more than about 1,500 lines of production code. Count test lines separately. Defects caused by not reusing existing machinery are Pass 1 findings.

### Reuse

1. **Does existing code already do most of this?** Search for the concept, not only the name. Count the existing copies. A second copy needs a justification; a third copy means the shared part is extracted.
2. **How do the existing copies differ?** Before extracting a shared part, compare the copies side by side. Compare both their data structures and their behavior on each terminal path: completion, failure, cancellation, and abort. An extraction that unifies divergent behavior is a behavior change, and is stated and tested as one. The reviewer lists each divergence; which behavior survives is a blocker question for the maintainers.
3. **Does the new code re-carry data the framework already provides?** Ids, status, and metrics belong to `TransferContext`. A type that keeps its own copy duplicates them and can diverge from them.
4. **Could an existing mechanism be called from a different place instead?** Prefer moving a call site over adding a parallel field, counter, or code path.

### Placement

5. **Which module owns this concept?** Shared code goes in the lowest module that owns its concept, with dependencies pointing down, as [architecture.md](architecture.md) describes.
6. **Does the component that defines completion own each finalization step?** Finalization belongs to the state machine's terminal transition, not to a handle.
7. **Does the work run where the runtime model expects?** Check blocking calls, the thread that runs file I/O, and the cost of each `poll_work` call against [scheduler.md](../design/scheduler.md#bounded-per-call-cost).

### Shape

8. **Does the module layout match its siblings?** Compare the new module's files with a sibling's. The operation layout and the `test_util` convention are in [architecture.md](architecture.md#conventions).
9. **Does a large file mix concerns?** Name the concerns and the seams between them. A large inline test module is a test-support question (Q8), not a production split.
10. **Is the code split too finely?** One file per type scatters a concept. Match the granularity of sibling modules.
11. **Does the API keep its policy inside the type?** A caller that passes a limit or threshold into every call is computing arithmetic that the type should own.
12. **Is a variant-specific or speculative accessor hoisted into a shared surface?**

### Names

13. **Is this a new concept, or a new name for an existing one?** Before accepting a name, check whether the fields or behavior it covers already belong to an existing type.
14. **Does the name collide with vocabulary that the crate, its public API, or the design documents use for something else?**
15. **Are near-identical types one generic type?**

### Change hygiene

16. **Why was existing code removed or reshaped?** Unexplained churn is a finding.
17. **Does a change to a shared component ride inside a feature change?** It needs its own changelog entry, and usually its own pull request.
18. **Does this conflict with other open pull requests?** Name each one and the reconciliation.

### Conflicts and precedent

19. **Do two documents disagree?** When a design document and [architecture.md](architecture.md) give different answers, raise the disagreement as a blocker question. Do not resolve it silently.
20. **Does the change copy an existing violation?** Copying a pattern that breaks a stated rule is a finding. Cite the precedent, and fix the copy.

## Pass 2: correctness and contracts

- **Invariants.** Check each invariant that the change touches, using the [invariant index](../design/README.md#invariant-index) and the module documentation.
- **Evidence.** Check the evidence against the [risk table](standards.md#risk-specific-evidence). Each test must establish the behavior it names.
- **Documentation.** Check it against [standards.md](standards.md).
- **Description.** Check the pull request description against [standards.md](standards.md#pull-request-descriptions). Each claim of coverage names the test that establishes it.

## Triage

| Label | Meaning |
|---|---|
| **Blocker** | Needs an answer before the details are worth reviewing: structure, module ownership, an open design decision, or a defect that would ship. |
| **Recommended** | Raised now and settled during or after any restructure. |
| **Second pass** | The code will move, or the fix is cheap and independent. The reproduction is kept, and the finding is raised once the structure settles. |

A proposed restructure leads the review. It gives the module tree, what moves where, and a landing order: move-only changes first, behavior changes last.

## Comment format

Each comment stands on its own for an author who does not have the reviewer's context:

- a one-line bold title;
- what goes wrong, and why it matters;
- the options, with a recommendation;
- for a blocker, the reproduction or test in a collapsed `<details>` block.

[review-examples.md](review-examples.md) applies these questions to real reviews.
