#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run-harness.sh <server-binary> <header-pattern> <command> [<arg>...]
       run-harness.sh --help

Runs a wasm runtime harness against a fresh s3-mock-server and checks the
harness's report.

Starts <server-binary> with the bucket `bucket` and a one-time `503 SlowDown`
for `bucket/retried`, which every harness expects, and runs <command> with the
server's address, such as `127.0.0.1:49152`, appended to its arguments. The
harness must print a first line that matches the extended regular expression
<header-pattern>, whose first group is the number of scenarios, a line
starting with `PASS ` for each scenario, and `<count> scenarios passed` last.

Exits with status 1, after printing the server's output, if the server does
not report its address, the command fails, or the report differs. Stops the
server on exit.
EOF
}

if [[ ${1:-} == --help ]]; then
    usage
    exit 0
fi
if (( $# < 3 )); then
    usage >&2
    exit 2
fi
server_binary=$1
header=$2
shift 2

temp=$(mktemp -d)
server_log="$temp/server.log"
log="$temp/harness.log"
"$server_binary" --slow-down-once bucket/retried bucket > "$server_log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true; rm -rf "$temp"' EXIT

fail() {
    echo "$1" >&2
    echo "s3-mock-server output:" >&2
    cat "$server_log" >&2
    exit 1
}

# The server prints its loopback address once it accepts connections.
address=
for _ in {1..100}; do
    address=$(grep -m 1 -E '^127\.0\.0\.1:[0-9]+$' "$server_log" || true)
    [[ -n $address ]] && break
    sleep 0.1
done
[[ -n $address ]] || fail "s3-mock-server did not report an address"

"$@" "$address" | tee "$log" || fail "the harness exited with an error"

# The harness reports what it ran, so a harness that skips scenarios cannot
# pass by exiting zero.
[[ "$(head -n 1 "$log")" =~ $header ]] || fail "the harness did not report its scenarios"
count=${BASH_REMATCH[1]}
passed=$(grep -c '^PASS ' "$log" || true)
if (( count == 0 || passed != count )) \
    || [[ "$(tail -n 1 "$log")" != "$count scenarios passed" ]]; then
    fail "the harness reported $passed of $count scenarios passing"
fi
