# S3 TM model tooling

This JVM project loads, projects, and validates S3 Smithy models. A Python
script acquires model inputs. Normal Cargo builds do not invoke this tooling.

## Prerequisites

Use Python 3.9+, Java 21, and the checked-in Gradle 8.14.3 wrapper. `just` is
optional shorthand; the underlying commands work directly. Gradle downloads its
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

`codegen` and `codegen --project-only` resolve the input, export the dataplane
Smithy model, and independently reload the exported JSON through Smithy's
assembler. `--model` selects a read-only local input; `--output` selects the
artifact directory. Relative paths resolve from the repository root.
`--offline` forbids dependency and model fetching; `--pinned-only` rejects local
input overrides.

`--dry-run` exports and validates in temporary storage without changing the
destination. If an exported model exists there, it compares the candidate using
Smithy's compatibility diff; otherwise it reports the model that would be
written. Temporary output is removed on completion or failure. Dependency and
model caches can still be populated unless `--offline` is selected.

The underlying entry point is `python3 tools/scripts/codegen`.
`just --dry-run codegen` only prints the command and does not execute this preview.

## Tests

```sh
just test-codegen
```

`test-codegen` runs Python acquisition/orchestration tests along with JVM model-loader and
projection fixtures. Acquisition tests check pin/digest verification,
cache preservation, local overrides, offline errors, and fetch failures using
local fixtures and mocked network responses. No tests need the full S3 model or
a live download after build dependencies are available.

The separately resolved codegen configuration can be checked explicitly:

```sh
tools/codegen/s3-tm-model-codegen/gradlew \
  --project-dir tools/codegen/s3-tm-model-codegen \
  verifyCodegenDependencies
```

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
