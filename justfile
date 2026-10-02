# List developer commands.
default:
    @just --list

# Resolve the pinned S3 model, fetching it only when the cache is missing.
fetch-model:
    python3 tools/scripts/fetch-model

# Run acquisition/orchestration, model-loader, and projection fixture tests.
test-codegen:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen test

# Export and validate the dataplane model; accepts --project-only and --dry-run.
[positional-arguments]
codegen *args:
    python3 tools/scripts/codegen "$@"

# Compare two projected model files using Smithy's compatibility diff.
diff-model old new:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen diffModels -PoldModel={{quote(old)}} -PnewModel={{quote(new)}}
