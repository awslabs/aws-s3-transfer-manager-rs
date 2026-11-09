# Upstream sources

`src/release/requirements.rs` and its regression cases are adapted from
smithy-lang/smithy-rs, commit `f10719ab5a388c8c8049f292cd5f502e1ac5fd83`,
`tools/ci-build/runtime-versioner/src/requirements.rs`.

The adaptation uses internal TM registry/dependency types and Cargo metadata.
Cargo resolves inherited workspace dependencies before the audit; the upstream
unsupported-manifest-form check and its test are therefore omitted. Matching,
SemVer evaluation, findings and applicable regression cases are retained.
Fixtures use the repository's test_util convention. Copyright and Apache-2.0
headers remain intact.

The registry dependency model follows the same revision's
`tools/ci-build/smithy-rs-tool-common/src/index.rs`. Transport and archive source
validation preserve this repository's release contracts.

Compare subsequent upstream fixes against these sources when updating the
checker. Repository discovery and publication policy are independent.
