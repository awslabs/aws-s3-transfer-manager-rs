# List developer commands.
default:
    @just --list

# Resolve the pinned S3 model, fetching it only when the cache is missing.
fetch-model:
    python3 tools/scripts/fetch-model

# Run Python/JVM tooling tests and compile/test generated Rust value fixtures.
test-codegen:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen test

# Generate modeled values and SDK v1 adapters; accepts --project-only, --dry-run, --check.
[positional-arguments]
codegen *args:
    python3 tools/scripts/codegen "$@"

# Generate and install src/model and src/sdk_v1; accepts --dry-run, --check, --overwrite.
[positional-arguments]
install-model *args:
    python3 tools/scripts/install-model "$@"

# Compare two projected model files using Smithy's compatibility diff.
diff-model old new:
    tools/codegen/s3-tm-model-codegen/gradlew --project-dir tools/codegen/s3-tm-model-codegen diffModels -PoldModel={{quote(old)}} -PnewModel={{quote(new)}}

# Check build freshness, then run the locked developer tool.
[positional-arguments]
tmdev *args:
    cargo build --quiet --locked --manifest-path tools/tmdev/Cargo.toml --target-dir tools/tmdev/target
    tools/tmdev/target/debug/tmdev "$@"

# Prepare, audit, check, tag, or publish workspace releases.
[positional-arguments]
release *args:
    @just tmdev release "$@"

# Build published crates' docs as docs.rs does, on CI's pinned nightly; accepts crate names and cargo-docs-rs flags.
[positional-arguments]
docs *args:
    #!/usr/bin/env bash
    set -euo pipefail
    nightly=$(sed -n 's/^  rust_nightly: *\([^ #]*\).*/\1/p' .github/workflows/ci.yml)
    [[ -n $nightly ]] || { echo "rust_nightly is not set in .github/workflows/ci.yml" >&2; exit 1; }
    crates=()
    flags=()
    for arg in "$@"; do
        if [[ $arg == -* ]]; then flags+=("$arg"); else crates+=("$arg"); fi
    done
    if (( ${#crates[@]} == 0 )); then
        # Publishable workspace members, by tmdev's rule: `publish` unset or including crates-io.
        read -ra crates <<< "$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json, sys; print(*(p["name"] for p in json.load(sys.stdin)["packages"] if p["publish"] is None or "crates-io" in p["publish"]))')"
    fi
    for crate in "${crates[@]}"; do
        RUSTDOCFLAGS=-Dwarnings cargo "+$nightly" docs-rs -p "$crate" ${flags[@]+"${flags[@]}"}
    done
