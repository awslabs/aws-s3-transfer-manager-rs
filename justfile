# List developer commands.
default:
    @just --list

# Resolve the pinned S3 model, fetching it only when the cache is missing.
fetch-model:
    python3 tools/scripts/fetch-model

# Run Python/JVM tooling tests and compile/test generated Rust value fixtures.
test-codegen:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen test

# Generate modeled values; accepts --project-only, --dry-run, and --check.
[positional-arguments]
codegen *args:
    python3 tools/scripts/codegen "$@"

# Generate fresh values and install src/model; accepts --dry-run, --check, --overwrite.
[positional-arguments]
install-model *args:
    python3 tools/scripts/install-model "$@"

# Compare two projected model files using Smithy's compatibility diff.
diff-model old new:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen diffModels -PoldModel={{quote(old)}} -PnewModel={{quote(new)}}
