#!/usr/bin/env bash

set -euo pipefail

if (( $# < 1 )); then
    echo "usage: android-test-runner.sh <test-binary> [test-arguments...]" >&2
    exit 2
fi

if [[ -z "${CARGO_NDK_ADB_SERIAL:-}" ]]; then
    echo "CARGO_NDK_ADB_SERIAL must identify the Android test device" >&2
    exit 2
fi

if [[ -z "${ANDROID_TEST_SSL_CERT_FILE:-}" ]]; then
    echo "ANDROID_TEST_SSL_CERT_FILE must name the Android CA bundle" >&2
    exit 2
fi

test_binary=$1
shift

# Maximum seconds any single setup/teardown adb invocation may run.
adb_timeout_seconds=${ADB_TIMEOUT_SECONDS:-60}

# Run an adb command under a hard timeout so a hung device cannot stall the
# script. Returns the command's own status, or 124 if the timeout fired.
adb_bounded() {
    timeout --signal=KILL "$adb_timeout_seconds" adb "$@"
}

device_path="/data/local/tmp/$(basename "$test_binary")"

cleanup() {
    local status=$?
    trap - EXIT
    adb_bounded -s "$CARGO_NDK_ADB_SERIAL" shell rm -f "$device_path" >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT

adb_bounded -s "$CARGO_NDK_ADB_SERIAL" push "$test_binary" "$device_path" >/dev/null
adb_bounded -s "$CARGO_NDK_ADB_SERIAL" shell chmod 755 "$device_path"
echo "Running Android tests through repository runner"
adb -s "$CARGO_NDK_ADB_SERIAL" shell \
    env \
    "RUST_BACKTRACE=${RUST_BACKTRACE:-0}" \
    "SSL_CERT_FILE=$ANDROID_TEST_SSL_CERT_FILE" \
    "$device_path" "$@"
