# S3 TM model tooling

This developer-only pipeline projects S3 Smithy models and generates TM-owned
Rust values and SDK v1 adapters using smithy-rs. Cargo builds use the checked-in
generated source; they do not invoke the tooling or download models.

## Generate, review, and install

Use Python 3.9+, Java 21, Cargo with rustfmt, and the checked-in Gradle 8.14.3
wrapper. Gradle downloads its distribution and Maven dependencies on first use.
Run commands from the repository root:

```sh
just codegen                         # Generate artifacts under target/codegen.
just codegen --dry-run               # Preview artifact changes without publishing.
just install-model --dry-run         # Preview changes to checked-in TM source.
just install-model                  # Regenerate and install both source trees.
just install-model --check --pinned-only  # Verify installed source matches the pin.
just test-codegen
```

`codegen` writes artifacts, not TM source. `install-model` independently
regenerates in temporary storage before comparing or installing
`aws-sdk-s3-transfer-manager/src/model` and `src/sdk_v1`; running `codegen` first
is not required. Review the installed Rust changes with Git.

`just` is optional shorthand for `python3 tools/scripts/codegen`,
`python3 tools/scripts/install-model`, and the Gradle tasks in the root justfile.
Use each script's `--help` for its options. `just --dry-run codegen` only prints
the recipe; `just codegen --dry-run` executes the generation preview.

Generation and installation accept:

- `--model PATH`: use a read-only local model instead of the pin.
- `--offline`: forbid model and dependency fetching.
- `--pinned-only`: reject local input overrides.
- `--dry-run`: regenerate and report changes without updating the destination.
- `--check`: regenerate and return `0` for identical output, `1` for missing,
  extra, or changed files, without updating the destination.

Temporary output is removed after preview/check, including on failure. Model
and dependency caches can still be populated unless `--offline` is selected.

For `codegen`, `--output PATH` selects the artifact directory and
`--project-only` stops after dataplane model export and validation:

```sh
just codegen --model /path/to/dev-s3.json --output target/codegen/dev
just codegen --project-only
just codegen --check
```

Relative paths resolve from the repository root. The output summary identifies
the input and verified-pin/local-override status, projection configuration,
exported model, retained operations, and generated artifact/report paths.
Preview output distinguishes temporary artifacts from the unchanged destination.

`codegen --dry-run` reports added, changed, and removed generated files. It also
uses Smithy's compatibility diff when a destination projected model exists;
otherwise it reports the model that would be written. `codegen --check` compares
the exported model, complete Rust and adapter source trees, manifest/build script,
and reports byte-for-byte. `--project-only --check` compares only the model.
Neither an equality check nor a Smithy model diff proves Rust API compatibility.

## Model input and updates

`gradle.properties` pins the model repository, repository-relative file, full
commit SHA, and SHA-256 digest through its `model.*` properties. The ignored cache
is `target/codegen/models/s3.json`.

```sh
just fetch-model
python3 tools/scripts/fetch-model --offline --pinned-only --json
python3 tools/scripts/fetch-model --model /path/to/dev-s3.json --json
```

A verified cache needs no network access. A missing cache fetches the immutable
file and installs it atomically after digest/format checks. A differing cache
is preserved, not overwritten: move it aside deliberately to restore pinned
input, or select it explicitly with `--model`. Local-input reports do not claim
the pinned revision. Python checks the digest and basic JSON format; the JVM
uses Smithy's assembler to discover traits, validate semantics, and require
`com.amazonaws.s3#AmazonS3`.

Evaluate an upstream update as a local input in separate output:

```sh
just codegen --model /path/to/new-s3.json --output target/codegen/candidate
```

Review the generated `model/src/model` and `sdk_v1` trees against the installed
source, including public types/builders, documentation, and request mappings.
Generation stops if the input introduces an unreviewed routing decision or
incompatible value mapping; resolve that policy before installation.

To examine upstream changes independently of Rust generation, compare only
the supported dataplane projections:

```sh
just codegen --project-only
just codegen --project-only --model /path/to/new-s3.json \
  --output target/codegen/candidate
just diff-model \
  target/codegen/projections/s3-tm-dataplane/model/model.json \
  target/codegen/candidate/s3-tm-dataplane/model/model.json
```

`diff-model` runs the pinned Smithy CLI's compatibility diff with NOTE and higher
severity events. It neither fetches models nor changes the pin. Compatibility
errors return nonzero; this is not a byte-for-byte equality check.

Accepting a model update is an explicit change to the repository, path, revision,
and digest in `gradle.properties`. Move aside a cache that differs from the new
pin before fetching, then regenerate/install, review the Rust diff, and run the
checks below. Check SDK adapter compilation as well as modeled-value compilation;
model additions can require a compatible SDK dependency or a mapping decision.

## Projection and public surface

`smithy-build.json` uses standard Smithy transforms to retain `PutObject`,
`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`,
`AbortMultipartUpload`, `GetObject`, `HeadObject`, and `ListObjectsV2`, plus their
reachable shapes and shared trait/mixin schemas. Source shape IDs, endpoint rules,
and operation traits are preserved; the `smithy.rules#endpointTests` fixture trait
is excluded. The standard `model` plugin exports the dataplane JSON, which is
independently reloaded through Smithy's assembler.

`CodegenModel.kt` flattens mixins and removes wire-binding traits once from an
in-memory copy before TM projection. Generated values do not implement wire
serialization/deserialization; the exported dataplane model retains those traits.

`TmModelProjection.kt` selects value roots, TM names, and response aggregates:

- `GetObjectRequest` becomes `DownloadInput`, retaining all modeled members.
  Caller-selected part downloads are unsupported: `part_number` remains a field
  and value accessor, with internal builder methods and no fluent setters.
  Discovery controls request part numbers.
- `PutObjectRequest` supplies `UploadInput`, with the exposure and routing
  policy described below.
- `UploadOutput` combines PUT, create-MPU, and complete-MPU response members.
- `ObjectMetadata` combines GET/HEAD response members; `ChunkMetadata` uses
  GET members. Both exclude `Body`.
- Listing values and shared enums include `Object`, `Owner`, `RestoreStatus`,
  `ChecksumAlgorithm`, `ChecksumType`, and `ObjectStorageClass`.

Ordinary modeled additions and reachable nested types flow into these values
without a field allowlist. Shared response members must agree on targets and
value-facing traits; conflicts fail generation with the member name.

### Upload exposure and forwarding

Public input exposure and request forwarding are separate policies in
`UploadRequestPolicy.kt`. Membership in the dataplane closure does not mean every
operation input is flattened into `UploadInput`.

`UploadInput` inherits ordinary PUT members automatically. Body and length use
TM runtime handling; checksum selection/values belong to `ChecksumStrategy`,
and append offsets are excluded. An unknown checksum-prefixed member requires
an explicit exposure decision instead of becoming an ordinary field.

PUT forwards ordinary members automatically. Create-MPU, upload-part,
complete-MPU, and abort use reviewed, per-operation forwarding rules, even when
a member has the same name and target on PUT. Forwarded members must have a
compatible PUT source. New unclassified members, incompatible sources, and
stale rules fail full generation before Rust artifact publication.

For example, a new member shared by PUT and create-MPU requires review of its
multipart forwarding; matching names alone do not approve that route.
Whole-object `ContentMD5` is deliberately omitted from upload-part requests,
and abort's `IfMatchInitiatedTime` is deliberately not exposed. Body, length,
multipart identity, and checksums retain explicit TM-controlled mappings.
Review the policy and generated `sdk_v1/mapping.json` when updating the model.

## Generated artifacts and customization points

The default artifact layout is:

```text
target/codegen/projections/
  s3-tm-dataplane/model/model.json
  model/
    Cargo.toml
    build.rs
    dependencies.json
    member-sources.json
    member-policy.json
    provenance.json
    src/
      lib.rs
      model/
        mod.rs
        builders.rs
        error.rs
        sealed_enum_unknown.rs
        _*.rs
  sdk_v1/
    mod.rs
    convert.rs
    compat.rs
    mapping.json
```

`ModelArtifact.kt` assembles and formats artifacts from intermediate output in
this Gradle project's `build/model-codegen/raw`. The entire `src/model` subtree
is generated, including exports, enum helpers, and fluent field delegation.
The standalone crate depends on `aws-smithy-types`, not `aws-sdk-s3`.
Installation copies only the Rust module trees, not manifests, build scripts,
or reports. Values are public under `aws_sdk_s3_transfer_manager::model`;
the SDK adapter module is crate-private.

Change generator policies rather than editing installed files:

- `ModelGenerator.kt` reuses smithy-rs symbol, structure/builder, and infallible
  enum generators. `MemberPolicy.kt` supplies construction, visibility, redaction,
  and runtime substitutions using typed symbols. Required inputs are checked
  during builder construction while retaining optional field representation.
  Value/fluent builder getters follow smithy-rs's `&Option<T>` convention;
  built-value string getters borrow `&str`.
- `TmRuntimeTypes.kt` defines references to handwritten streams, metrics, and
  policies. TM-specific members and fluent delegation use
  `#[cfg(not(s3_tm_out_of_tree))]` across fields, methods, construction,
  validation, and Debug. The standalone `build.rs` selects the reduced shape
  without invoking codegen; in-tree compilation leaves the cfg unset. Derives
  account for the full runtime shape, including non-cloneable upload streams.
- `customizations/EnumDocumentation` supplies TM-owned forward-compatibility
  guidance without changing upstream enum parsing, unknown values, or traits.
- `customizations/S3Expires` uses `DateTime` for upload expiration and preserves
  raw response strings as `expires_string`, without modifying the shared
  upstream target.
- `customizations/S3Optionality` removes boolean/numeric defaults to preserve
  absence. Required inputs with service defaults remain omittable through
  `clientOptional`; ordinary required inputs remain validated.
- `customizations/RequestIdExt` adds metadata request IDs with internal fields
  and builder setters, and public SDK-independent getters.
- `customizations/DownloadMetadata` preserves empty metadata defaults and
  upstream descriptions, with discovery/chunk and GET/HEAD availability guidance.

Upload response docs also preserve upstream member/target descriptions and
explain transfer-path availability and multipart merging. Execution methods
and request-specific body/checksum handling stay outside the generated roots.

The adapters' `convert.rs` contains crate-private enum, nested-value, request,
and response conversions. `mod.rs` re-exports internal helpers and gates
`compat.rs` with `sdk-v1`. Compatibility includes public `From`/`TryFrom`
implementations for SDK enums/nested values and response-to-metadata or
`UploadOutputBuilder` conversions. Multipart completion updates retain
create-only response fields; response conversions do not invent transfer metrics.
Borrowed GET conversions leave the body available; owned conversions discard
it without reading it. Metadata's SDK `RequestId`/`RequestIdExt` trait
implementations require `sdk-v1`, while inherent getters do not.

Reports support maintenance without being installed:

- `member-sources.json`: operation and nested-member correspondence.
- `member-policy.json`: custom names/visibility, runtime types, construction,
  redaction, exclusions, and synthetic request-ID header mappings.
- `sdk_v1/mapping.json`: source-member classifications and conversion mappings.
  A GET response member lists both metadata destinations.
- `provenance.json`: input, projection configuration, projected model, and
  generator-source digests.
- `dependencies.json`: generated dependencies and Smithy/smithy-rs versions.

Reports contain no timestamps or absolute machine paths.

## Replacement and stale files

Artifact output is disposable. Generation assembles a fresh candidate, replaces
the generated `model/src` and `sdk_v1` trees and fixed artifact files, and removes
stale files, including local artifact edits. Files outside that layout, such as
tests, Cargo lockfiles, and build products, are preserved. Project-only
publication leaves existing Rust/adapter output alone. Generated paths reject
symlinks and non-directory parents. Use preview/check for non-mutating comparison.

Installation replaces both complete generated source trees: obsolete files
are reported as removed, and missing required files as added. It rejects
handwritten/unmarked files, symlinks, nonregular entries, and invalid parent
paths. Divergent local Git changes to generated files, including staged,
unstaged, deleted, or untracked files, require explicit approval via:

```sh
just install-model --overwrite
```

Preview/check never require this override, and byte-identical installs are
no-ops. `--overwrite` does not bypass source-tree safety checks. Files outside
`src/model` and `src/sdk_v1` remain untouched.

An installation lock serializes source publication. Both trees are validated
and staged before directory swaps; a failed swap rolls back both destinations.
The two-directory publication is not filesystem-atomic. If restoration fails,
the command reports the preserved backup. After interruption, inspect that
backup and `target/codegen/install-model.lock` before removing a stale lock.

## Validation

```sh
just test-codegen
just install-model --check --pinned-only
cargo test --locked -p aws-sdk-s3-transfer-manager --lib --test model_api_test
cargo test --locked -p aws-sdk-s3-transfer-manager \
  --lib --test sdk_v1_compat_test --features sdk-v1
```

`test-codegen` runs Python acquisition/orchestration/installer tests, JVM
projection/generation/policy tests, and Rust tests for generated fixtures.
`src/test/resources/s3-example-model.smithy` is a synthetic fixture, not the
production input. It exercises model evolution, conflicting aggregates,
traits/defaults, unknown enums, sensitivity, and builders. The generated module
is compiled both standalone and with the cfg unset against runtime doubles.
Publication tests cover full inventories, stale files, dirty-source protection,
symlinks, concurrency, and rollback.

These tests use local fixtures and mocked model downloads; they do not require
the full S3 model or a live model download. Maven/Rust dependencies must be
cached or downloadable. Generated Rust fixture caches are
`target/codegen/cargo-home` and `target/codegen/cargo-target`.

The TM tests use installed code and real stream, policy, and metrics types.
Handwritten internal tests live under `aws-sdk-s3-transfer-manager/src/tests`,
outside generated roots; tests under the crate's `tests` directory exercise
external-consumer APIs. The required modeled-generation CI job runs tooling
tests, pinned installed-source checking, and these real-runtime tests.
