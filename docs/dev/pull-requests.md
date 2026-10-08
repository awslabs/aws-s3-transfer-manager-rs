# Pull request descriptions

A PR description explains the assembled result, not the sequence of commits or the development process.

## Recommended structure

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

## PR prose rules

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
