#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run-fuzz-targets.sh <build|smoke|campaign>
       run-fuzz-targets.sh --help

Discover all targets with `cargo fuzz list`, then:

  build       Compile every fuzz target without executing it.
  smoke       Execute every target for a deterministic, bounded number of runs.
  campaign    Explore every target for a bounded amount of time.

Environment:

  FUZZ_TARGET_TRIPLE    Compilation target for all modes
                        (default: x86_64-unknown-linux-gnu)
  FUZZ_RUNS             Runs per target in smoke mode (default: 4096)
  FUZZ_SEED             libFuzzer seed in smoke mode (default: 1)
  FUZZ_MAX_LEN          Maximum input length in smoke and campaign modes
                        (default: 1024)
  FUZZ_TIMEOUT          Per-input timeout in seconds in smoke and campaign
                        modes (default: 10)
  FUZZ_MAX_TOTAL_TIME   Seconds per target in campaign mode (default: 900)

FUZZ_RUNS, FUZZ_MAX_LEN, FUZZ_TIMEOUT, and FUZZ_MAX_TOTAL_TIME must be
positive decimal integers. FUZZ_SEED must be a nonnegative decimal integer.

Examples, from the repository root:

  .github/scripts/run-fuzz-targets.sh build
  FUZZ_RUNS=64 .github/scripts/run-fuzz-targets.sh smoke
  FUZZ_MAX_TOTAL_TIME=60 .github/scripts/run-fuzz-targets.sh campaign
EOF
}

if (( $# != 1 )); then
    usage >&2
    exit 2
fi

case "$1" in
    -h|--help)
        usage
        exit 0
        ;;
    build|smoke|campaign)
        mode=$1
        ;;
    *)
        echo "unknown fuzz mode: $1" >&2
        usage >&2
        exit 2
        ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
fuzz_dir="$repo_root/fuzz"
curated_root="$repo_root/aws-sdk-s3-transfer-manager/src/runtime/buffer_pool/tests/corpus"
target_triple=${FUZZ_TARGET_TRIPLE:-x86_64-unknown-linux-gnu}

require_positive_integer() {
    local name=$1
    local value=$2

    if [[ ! "$value" =~ ^[0-9]+$ ]] || [[ ! "$value" =~ [1-9] ]]; then
        echo "$name must be a positive integer, got: $value" >&2
        exit 2
    fi
}

require_nonnegative_integer() {
    local name=$1
    local value=$2

    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "$name must be a nonnegative integer, got: $value" >&2
        exit 2
    fi
}

require_curated_corpus() {
    local target=$1
    local path=$2

    if [[ ! -d "$path" ]]; then
        echo "curated corpus for fuzz target '$target' not found: $path" >&2
        exit 1
    fi
}

case "$mode" in
    build)
        ;;
    smoke)
        fuzz_runs=${FUZZ_RUNS:-4096}
        fuzz_seed=${FUZZ_SEED:-1}
        fuzz_max_len=${FUZZ_MAX_LEN:-1024}
        fuzz_timeout=${FUZZ_TIMEOUT:-10}
        require_positive_integer FUZZ_RUNS "$fuzz_runs"
        require_nonnegative_integer FUZZ_SEED "$fuzz_seed"
        require_positive_integer FUZZ_MAX_LEN "$fuzz_max_len"
        require_positive_integer FUZZ_TIMEOUT "$fuzz_timeout"
        ;;
    campaign)
        fuzz_max_total_time=${FUZZ_MAX_TOTAL_TIME:-900}
        fuzz_max_len=${FUZZ_MAX_LEN:-1024}
        fuzz_timeout=${FUZZ_TIMEOUT:-10}
        require_positive_integer FUZZ_MAX_TOTAL_TIME "$fuzz_max_total_time"
        require_positive_integer FUZZ_MAX_LEN "$fuzz_max_len"
        require_positive_integer FUZZ_TIMEOUT "$fuzz_timeout"
        ;;
esac

if ! target_output=$(cd "$fuzz_dir" && cargo fuzz list); then
    echo "cargo fuzz list failed" >&2
    exit 1
fi

targets=()
while IFS= read -r target; do
    if [[ -n "$target" ]]; then
        targets+=("$target")
    fi
done <<<"$target_output"

if (( ${#targets[@]} == 0 )); then
    echo "cargo fuzz list returned no targets" >&2
    exit 1
fi

campaign_status=0
for target in "${targets[@]}"; do
    working="$fuzz_dir/corpus/$target"
    curated="$curated_root/$target"
    artifacts="$fuzz_dir/artifacts/$target"

    case "$mode" in
        build)
            (
                cd "$fuzz_dir"
                cargo fuzz build --target "$target_triple" "$target"
            )
            ;;
        smoke)
            require_curated_corpus "$target" "$curated"
            mkdir -p "$working" "$artifacts"
            (
                cd "$fuzz_dir"
                cargo fuzz run --target "$target_triple" \
                    "$target" "$working" "$curated" -- \
                    -runs="$fuzz_runs" -seed="$fuzz_seed" \
                    -max_len="$fuzz_max_len" -timeout="$fuzz_timeout" \
                    -artifact_prefix="artifacts/$target/"
            )
            ;;
        campaign)
            require_curated_corpus "$target" "$curated"
            mkdir -p "$working" "$artifacts"
            if ! (
                cd "$fuzz_dir" &&
                    cargo fuzz run --target "$target_triple" \
                    "$target" "$working" "$curated" -- \
                    -max_total_time="$fuzz_max_total_time" \
                    -max_len="$fuzz_max_len" -timeout="$fuzz_timeout" \
                    -artifact_prefix="artifacts/$target/"
            ); then
                campaign_status=1
            fi
            ;;
    esac
done

exit "$campaign_status"
