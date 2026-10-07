# S3 TM model tooling

This JVM project projects S3 Smithy models and generates standalone Rust value
types using smithy-rs. Python scripts acquire model inputs and drive generation.
Normal Transfer Manager Cargo builds do not invoke this tooling.

## Prerequisites

Use Python 3.9+, Java 21, Cargo with rustfmt, and the checked-in Gradle 8.14.3
wrapper. `just` is optional shorthand; the underlying commands work directly.
Gradle downloads its
distribution and Maven dependencies on first use.

## Model input

`gradle.properties` specifies the model repository, repository-relative file,
full commit SHA, and SHA-256 digest using the `model.*` properties. The ignored
cache is `target/codegen/models/s3.json` at the repository root.

From the repository root:

```sh
just fetch-model
python3 tools/scripts/fetch-model --offline --pinned-only --json
python3 tools/scripts/fetch-model --model /path/to/dev-s3.json --json
```

A verified cache needs no network access. A missing cache fetches the immutable
raw file and installs it atomically after digest/format checks. A differing
cache is preserved. Move it aside deliberately to restore pinned input, or
select it explicitly using `--model`. Local input is read-only and its report
does not claim the pinned revision. `--pinned-only` rejects local overrides.

Python checks the digest and basic JSON format, not Smithy semantics. The JVM
loader uses Smithy's assembler, discovers trait definitions, validates the
model, and requires `com.amazonaws.s3#AmazonS3`.

## Dataplane projection

`smithy-build.json` defines the `s3-tm-dataplane` projection using standard Smithy
transforms. It retains these S3 operations:

- `PutObject`
- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `GetObject`
- `HeadObject`
- `ListObjectsV2`

The projection keeps their reachable shapes and the shared trait/mixin schemas,
preserving source shape IDs. It excludes the `smithy.rules#endpointTests` fixture
trait, while retaining endpoint rules and operation traits.

The standard `model` plugin writes the artifact to
`target/codegen/projections/s3-tm-dataplane/model/model.json`.

```sh
just codegen
just codegen --project-only
just codegen --model /absolute/path/to/dev-s3.json --output target/codegen/dev
just codegen --dry-run
just codegen --check
```

Each invocation resolves the input, exports the dataplane Smithy model, and
independently reloads the exported JSON through Smithy's assembler.
`codegen` also generates the standalone Rust artifact and SDK v1 adapters;
`--project-only` stops after model export/validation.
`--model` selects a read-only local input; `--output` selects the
artifact directory. Relative paths resolve from the repository root.
`--offline` forbids dependency and model fetching; `--pinned-only` rejects local
input overrides.

The output summary identifies the input path and whether it is a verified pin or
local override, the projection configuration and exported model path, and the
retained operations. Full generation also reports the intermediate codegen
directory, standalone crate, generated module, SDK adapter directory, and
mapping report paths. Dry-run output labels
the unchanged destination separately from the temporary output.

`--dry-run` runs the selected pipeline in temporary storage without changing the
destination. If an exported model exists there, it compares the candidate using
Smithy's compatibility diff; otherwise it reports the model that would be
written. Full generation also reports added, changed, and removed generated
files, comparing the complete Rust source subtree, manifest, and member-source
mapping, plus the SDK adapter subtree and its mapping report.
Cargo lockfiles/build products are excluded. Temporary output is
removed on completion or failure. Dependency and model caches can still be
populated unless `--offline` is selected.

`--check` uses fresh temporary generation and compares the canonical model,
complete Rust and SDK adapter subtrees, crate manifest/build script,
provenance, and policy reports
byte-for-byte. It returns `0` when identical and `1`
when files are missing, extra, or changed. It never updates the destination.
`--project-only --check` compares only the exported model. Unlike `--dry-run`,
this is an equality check rather than Smithy's semantic compatibility diff.

The underlying entry point is `python3 tools/scripts/codegen`.
`just --dry-run codegen` only prints the command and does not execute this preview.

## Rust artifact

`TmModelProjection.kt` defines the value roots and TM-specific names and metadata
aggregation. The roots include `Object`, `Owner`, `RestoreStatus`,
`ChecksumAlgorithm`, `ChecksumType`, and `ObjectStorageClass`. `GetObjectRequest`
keeps its Smithy identity and all modeled input members, and is named
`DownloadInput` in Rust. Caller-selected part downloads are unsupported: the
existing `part_number` field/accessor is retained, its builder methods are
internal, and fluent methods are omitted. Discovery controls SDK request part
numbers. `PutObjectRequest` is named `UploadInput`; TM controls
its body and checksum strategy, and excludes unsupported append offsets.
`UploadOutput` combines PUT, create-multipart-upload, and complete-multipart-upload
outputs. `ObjectMetadata` contains the union of GET/HEAD response members, and
`ChunkMetadata` contains GET response members, both excluding `Body`.
Shared members' targets and value-facing traits must agree; a conflict fails
generation with the member name. `CodegenModel.kt` flattens mixins and removes
wire-binding traits once from an in-memory copy before TM projection; these
values do not implement serialization/deserialization.
Original shapes and wire traits remain in the exported dataplane model. New modeled
members and their reachable nested types flow through generation without a
field allowlist. Normalization uses Smithy's model transformer.
`member-sources.json` records operation and nested-member correspondence.
`member-policy.json` records custom names/visibility, runtime substitutions,
construction and redaction policy, and intentional exclusions.

`customizations/` contains S3-specific value policies:

- `S3Expires` uses `DateTime` for upload expiration and preserves raw response
  strings as `expires_string`, without changing their shared upstream target.
- `S3Optionality` removes boolean/numeric defaults to preserve absence. Required
  inputs with service defaults remain omittable through `clientOptional`;
  ordinary required inputs remain validated.
- `RequestIdExt` adds modeled `RequestId` and `ExtendedRequestId` members to
  object/chunk metadata. Their fields and builder setters are internal, while
  SDK-independent getters are public. Their empty source-member lists identify
  synthetic values; the policy report records the response header mappings.
- `DownloadMetadata` preserves empty metadata defaults and service descriptions,
  including target documentation. Metadata documentation distinguishes discovery
  responses from individual chunk responses and describes GET/HEAD-only values.

`ModelGenerator.kt` calls smithy-rs's symbol provider, structure/builder
generators, and client-compatible infallible enum generator. Required
input fields are checked at builder construction while retaining their optional
field representation. Timestamps use `aws_smithy_types::DateTime`; enum strings,
other defaults, sensitivity, and documentation follow the modeled traits.
Structure and builder customizations share `MemberPolicy.kt`.

TM-owned references use `#[cfg(not(s3_tm_out_of_tree))]` consistently for fields,
methods, construction, validation, and Debug. The standalone crate's generated
`build.rs` selects the reduced compilation shape locally; it does not run codegen.
Normal TM compilation leaves this cfg unset and uses its existing runtime types.
Shared Smithy runtime types remain available in both shapes. Derives reflect the
full runtime shape, including the non-cloneable upload stream.

Intermediate generated files live under this Gradle project's
`build/model-codegen/raw`. `ModelArtifact.kt` assembles and formats the standalone
crate at `target/codegen/projections/model`:

```text
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
```

The entire `src/model` subtree, including exports and enum helpers, is generated.
The standalone crate depends on `aws-smithy-types`, not `aws-sdk-s3`.
`codegen` publishes generated artifacts, not Transfer Manager source.

SDK v1 adapters are generated separately at
`target/codegen/projections/sdk_v1`:

```text
sdk_v1/
  mod.rs
  convert.rs
  compat.rs
  mapping.json
```

`convert.rs` contains crate-private enum, nested-value, request-field, and
response conversions. `mod.rs` re-exports them for internal calls and loads
`compat.rs` only with the `sdk-v1` Cargo feature. That file contains the public
`From`/`TryFrom` implementations for SDK v1 enums and nested values, and
owned/borrowed upload response conversions into `UploadOutputBuilder`.
The builder's `update_from_complete_mpu` method applies completion response
fields while retaining fields present only in the create response.
Response conversions do not supply transfer metrics; building a TM
`UploadOutput` still requires metrics supplied by the transfer runtime.
GET/HEAD responses convert into `ObjectMetadata`, and GET responses also
convert into `ChunkMetadata`. Borrowed conversions leave the GET body available;
owned GET conversions discard it without reading it. Both metadata types
implement the SDK's `RequestId` and `RequestIdExt` traits only with `sdk-v1`;
their inherent identifier getters do not require the feature.
The feature selects interoperability, not the SDK backend dependency.
`mapping.json` records source-member classifications and correspondence;
it is not installed into TM.

`model/_upload_fluent_builder.rs` and `model/_download_fluent_builder.rs` contain
generated field delegation for the handwritten fluent wrappers. Both are gated out of standalone
compilation with `s3_tm_out_of_tree`. Value and fluent builders use smithy-rs
getter conventions (`&Option<T>`); built-value string accessors borrow `&str`.
Execution methods and request-specific checksum/body handling remain outside
the generated roots.

Upload response documentation preserves upstream member or target-shape
descriptions and adds transfer-path availability and multipart merge semantics.
Exact source-operation correspondence is recorded in `member-sources.json`.

`provenance.json` records input, projection-configuration, projected-model, and
generator-source digests. `dependencies.json` reports the generated Cargo
dependency declarations and Smithy/smithy-rs versions. Reports contain no
timestamps or absolute machine paths.

Generation assembles a fresh candidate before publishing. The artifact's
`model/src` and `sdk_v1` subtrees, manifest, build script, reports, and exported dataplane model
are disposable generated output. Regeneration replaces their contents and
removes stale generated files, including local edits. Use `--dry-run` to preview
changes or `--check` to compare without publishing. Files outside this layout,
including tests and Cargo lockfiles/build products, are preserved. Symlinks and
non-directory parents in generated paths are rejected before publication.
Project-only publication retains existing Rust and SDK adapter artifacts.

## Install modeled values and SDK adapters

```sh
just install-model --dry-run
just install-model
just install-model --check --pinned-only
just install-model --overwrite
```

The underlying entry point is `python3 tools/scripts/install-model`.
Every invocation generates fresh artifacts in temporary storage and compares
their complete Rust source trees with the fixed destinations
`aws-sdk-s3-transfer-manager/src/model` and
`aws-sdk-s3-transfer-manager/src/sdk_v1`. The modeled values are exposed through
`aws_sdk_s3_transfer_manager::model`; the SDK adapter module is crate-private.
The standalone manifest, build script, and reports are not installed.
TM uses its runtime types with `s3_tm_out_of_tree`
unset. Cargo builds use the checked-in files and do not invoke generation.

`--dry-run` reports added, changed, and removed files without changing source.
`--check` also leaves source unchanged, returning `0` for identical subtrees
and `1` for missing, extra, or changed files. Both regenerate before comparing.
`--model`, `--offline`, and `--pinned-only` have the same input/cache semantics as
`codegen`; temporary output is removed after the command, while dependency and
model caches can still be populated.

Installation replaces the complete generated subtrees and removes stale files.
It validates that every file is generated Rust source before publication.
Symlinks, non-directory parents, nonregular entries, and unmarked or handwritten
files are rejected. Divergent locally edited generated files, including staged,
unstaged, deleted, and untracked files, require explicit `--overwrite`.
Byte-identical installs are no-ops; preview/check never require this override.
`--overwrite` does not bypass the source-tree safety checks.
Files outside `src/model` and `src/sdk_v1` are preserved.

An installation lock serializes source publication. Both trees are validated
and staged before swapping directories; a failed swap rolls back both
destinations. The two-directory publication is not filesystem-atomic.
If restoration itself fails, the command reports
the preserved backup path. After an interrupted installation, inspect that path
and `target/codegen/install-model.lock` before removing a stale lock.

## Tests

```sh
just test-codegen
```

`test-codegen` runs Python acquisition/orchestration tests along with JVM
model-loader and projection fixtures. Acquisition tests check pin/digest verification,
cache preservation, local overrides, offline errors, and fetch failures using
local fixtures and mocked network responses. No tests need the full S3 model or
a live model download. Maven and Rust dependencies must be available locally
or downloadable.

`src/test/resources/s3-example-model.smithy` is a small synthetic model for
generation tests, not the production S3 input. It covers inherited input
members, shared and one-sided response metadata, and modeled values/traits.
Projection tests check additive field/nested-type evolution and conflicting
GET/HEAD metadata, as well as the operation closure defined by `smithy-build.json`.

The test suite also generates a standalone crate from this example model and runs
Rust consumer tests for unknown enums, collections/timestamps, required/default
fields, builders, sensitivity, and public paths. A separate crate compiles the
same generated module with the cfg unset and trait-constrained TM runtime
doubles, testing runtime fields, metrics, internal metadata construction, and
builders. S3 customization tests cover upload timestamps, raw expiration
strings, boolean/numeric absence, synthetic IDs, and collision failures.
These fixtures use the same
generation/assembly path as the full S3 model. Rust dependencies are cached at
`target/codegen/cargo-home`, with compilation output at
`target/codegen/cargo-target`.
Tests also cover scoped replacement/removal, stale output, symlinks,
complete-inventory checks, and deterministic generation in separate directories.
Installer tests cover fresh generation, preview/check, local Git edits,
handwritten-file protection, concurrent changes, and publication rollback.

The TM tests compile the installed subtree with the real stream, policy, and
metrics types:

```sh
cargo test --locked -p aws-sdk-s3-transfer-manager --lib --test model_api_test
cargo test --locked -p aws-sdk-s3-transfer-manager \
  --lib --test sdk_v1_compat_test --features sdk-v1
```

Handwritten crate-internal tests live in
`aws-sdk-s3-transfer-manager/src/tests/{model,sdk_v1}.rs`, outside both generated
subtrees. The tests under `aws-sdk-s3-transfer-manager/tests` exercise the public
API as external consumers.

The required modeled-generation CI job runs the tooling tests, checks installed
source against the pinned model, and runs these real-runtime tests.

## Compare projected models

```sh
just codegen --project-only
just codegen --project-only --model /path/to/new-s3.json \
  --output target/codegen/projections-new
just diff-model \
  target/codegen/projections/s3-tm-dataplane/model/model.json \
  target/codegen/projections-new/s3-tm-dataplane/model/model.json
```

`diff-model` invokes the pinned Smithy CLI's `diff` command with NOTE and higher
severity events. It compares the supplied projected models without fetching or
updating the source pin. Models are validated with the same Smithy dependency
versions used for projection. Compatibility errors produce a nonzero exit;
this is not a byte-for-byte equality check.

Changing the source pin is an explicit update to the repository, path, revision,
and digest in `gradle.properties`. A cache that differs from the new pin is
preserved; move it aside before fetching the new pinned input.
