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
```

Each invocation resolves the input, exports the dataplane Smithy model, and
independently reload the exported JSON through Smithy's assembler.
`codegen` also generates the standalone Rust artifact;
`--project-only` stops after model export/validation.
`--model` selects a read-only local input; `--output` selects the
artifact directory. Relative paths resolve from the repository root.
`--offline` forbids dependency and model fetching; `--pinned-only` rejects local
input overrides.

The output summary identifies the input path and whether it is a verified pin or
local override, the projection configuration and exported model path, and the
retained operations. Full generation also reports the intermediate codegen
directory, standalone crate, and generated module path. Dry-run output labels
the unchanged destination separately from the temporary output.

`--dry-run` runs the selected pipeline in temporary storage without changing the
destination. If an exported model exists there, it compares the candidate using
Smithy's compatibility diff; otherwise it reports the model that would be
written. Full generation also reports added, changed, and removed generated
files, comparing the complete Rust source subtree, manifest, and member-source
mapping. Cargo lockfiles/build products are excluded. Temporary output is
removed on completion or failure. Dependency and model caches can still be
populated unless `--offline` is selected.

The underlying entry point is `python3 tools/scripts/codegen`.
`just --dry-run codegen` only prints the command and does not execute this preview.

## Rust artifact

`TmModelProjection.kt` defines the value roots and TM-specific names and metadata
aggregation. The roots include `Object`, `Owner`, `RestoreStatus`,
`ChecksumAlgorithm`, `ChecksumType`, and `ObjectStorageClass`. `GetObjectRequest`
keeps its Smithy identity and all modeled input members, and is named
`DownloadInput` in Rust. `ObjectMetadata` contains the union of GET/HEAD response
members, excluding `Body`. Shared members' targets and non-documentation traits
must agree; a conflict fails generation with the member name. New modeled
members and their reachable nested types flow through generation without a
field allowlist. Mixins are flattened through Smithy's transformer.
`member-sources.json` records each operation member's correspondence, including
GET-only and HEAD-only metadata.

`ModelGenerator.kt` calls smithy-rs's symbol provider, structure/builder
generators, and client-compatible infallible enum generator. Required download
input fields are checked at builder construction while retaining their optional
field representation. Timestamps use `aws_smithy_types::DateTime`; enum strings,
defaults, sensitivity, and documentation follow the modeled traits.

Intermediate generated files live under this Gradle project's
`build/model-codegen`. `ModelArtifact.kt` assembles and formats the standalone
crate at `target/codegen/projections/model`:

```text
model/
  Cargo.toml
  member-sources.json
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
The standalone crate depends on `aws-smithy-types`, not `aws-sdk-s3`. Generation
does not install files into Transfer Manager's source tree.

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
fields, builders, sensitivity, and public paths. These fixtures use the same
generation/assembly path as the full S3 model. Rust dependencies are cached at
`target/codegen/cargo-home`, with compilation output at
`target/codegen/cargo-target`.

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
