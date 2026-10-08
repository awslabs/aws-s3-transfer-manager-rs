<!--
Structure and rules: docs/dev/standards.md#pull-request-descriptions.
Describe the assembled result, not the commit history. Keep each paragraph on one line.
Delete a section that does not apply.
-->

## Summary

<!-- What changes and why it matters, starting with the outcome a user or maintainer sees. For a PR in a series, give its position and dependencies. -->

## Review order

<!-- Large PRs only: the order to read files or types, and what to check in each. -->

## Design and behavior

<!-- Architecture and control flow, organized around reviewer concerns. Invariants, ownership boundaries, failure behavior, compatibility. Link the design document section this implements or changes. -->

## Developer experience

<!-- For public API or configuration changes: a representative example, doc links, and the CHANGELOG entry. Otherwise: "No public change." -->

## Structure

<!-- Required when the PR adds or moves a module, type, or trait. -->

- **Extends:**
- **New, and why no existing item fits:**
- **Moved or renamed:**
- **Shared components changed:**

## Invariants

<!-- Each invariant from docs/design/README.md#invariant-index or module documentation that this change touches, and how it is preserved. "None touched" only after checking. -->

| Invariant | Preserved by |
|---|---|

## Correctness and qualification

<!-- What the tests establish. For a fix, name the test that fails without it. -->

| Claim | Established by (test or lane) | Ran locally or CI only |
|---|---|---|

**Not covered:**

## Related work

<!-- Open PRs this conflicts with or depends on, and the reconciliation. -->
