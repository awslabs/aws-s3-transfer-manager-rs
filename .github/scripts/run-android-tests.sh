#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run-android-tests.sh build <abi>
       run-android-tests.sh emulator <abi> <page-size> <system-image-target>
       run-android-tests.sh --help

Build or execute the transfer-manager library tests for Android:

  build       Cross-compile and link the library test binary for <abi>.
  emulator    Create and boot an Android emulator, assert its runtime page
              size, then execute the library tests on the device.

Arguments:

  abi                  Android ABI accepted by cargo-ndk, such as arm64-v8a
                       or x86_64.
  page-size            Expected runtime page size in bytes.
  system-image-target  Android SDK image target, such as google_apis or
                       google_apis_ps16k.

Environment:

  ANDROID_API_LEVEL    Android API level (default: 35).
  ANDROID_HOME         Android SDK root. Required in emulator mode.

Prerequisites:

  cargo-ndk, the Rust target for <abi>, and an Android NDK must be installed.
  Emulator mode also requires adb, avdmanager, emulator, and the named system
  image. The CI workflow installs these before invoking this script.

Examples, from the repository root:

  .github/scripts/run-android-tests.sh build arm64-v8a
  .github/scripts/run-android-tests.sh emulator x86_64 4096 google_apis
  .github/scripts/run-android-tests.sh emulator x86_64 16384 google_apis_ps16k
EOF
}

if (( $# == 1 )) && [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

if (( $# < 2 )); then
    usage >&2
    exit 2
fi

mode=$1
abi=$2
android_api_level=${ANDROID_API_LEVEL:-35}

require_positive_integer() {
    local name=$1
    local value=$2

    if [[ ! "$value" =~ ^[0-9]+$ ]] || [[ ! "$value" =~ [1-9] ]]; then
        echo "$name must be a positive decimal integer, got: $value" >&2
        exit 2
    fi
}

require_positive_integer ANDROID_API_LEVEL "$android_api_level"

case "$abi" in
    arm64-v8a)
        rust_target=aarch64-linux-android
        ;;
    x86_64)
        rust_target=x86_64-linux-android
        ;;
    *)
        echo "unsupported Android ABI: $abi" >&2
        exit 2
        ;;
esac

run_android_tests() {
    local runner_env
    runner_env=CARGO_TARGET_${rust_target^^}_RUNNER
    runner_env=${runner_env//-/_}
    export "$runner_env=cargo ndk-runner"

    # cargo-ndk 4.1.2's `ndk-test` wrapper does not propagate the child
    # `cargo test` status. Use Cargo's target runner so device failures remain
    # visible to the required CI job.
    cargo ndk --target "$abi" --platform "$android_api_level" \
        test --locked -p aws-sdk-s3-transfer-manager --lib
}

case "$mode" in
    build)
        if (( $# != 2 )); then
            usage >&2
            exit 2
        fi
        cargo ndk --target "$abi" --platform "$android_api_level" \
            test --locked -p aws-sdk-s3-transfer-manager --lib --no-run
        ;;
    emulator)
        if (( $# != 4 )); then
            usage >&2
            exit 2
        fi
        expected_page_size=$3
        system_image_target=$4
        require_positive_integer page-size "$expected_page_size"

        if [[ -z "${ANDROID_HOME:-}" ]]; then
            echo "ANDROID_HOME must name the Android SDK root" >&2
            exit 2
        fi

        system_image="system-images;android-${android_api_level};${system_image_target};${abi}"
        avd_name="s3-tm-${android_api_level}-${system_image_target}-${abi}"
        emulator_log=${RUNNER_TEMP:-/tmp}/s3-tm-android-emulator.log
        emulator_serial=emulator-5554

        printf 'no\n' | avdmanager create avd --force \
            --name "$avd_name" \
            --package "$system_image" \
            --device pixel_6

        if [[ -e /dev/kvm ]]; then
            sudo chmod 666 /dev/kvm
        fi

        emulator -avd "$avd_name" \
            -port 5554 \
            -no-window \
            -no-audio \
            -no-boot-anim \
            -no-metrics \
            -no-snapshot \
            -wipe-data \
            -gpu swiftshader_indirect \
            >"$emulator_log" 2>&1 &
        emulator_pid=$!

        cleanup() {
            local status=$?
            trap - EXIT
            if (( status != 0 )); then
                echo "Android emulator log:" >&2
                tail -200 "$emulator_log" >&2 || true
            fi
            adb -s "$emulator_serial" emu kill >/dev/null 2>&1 || true
            kill "$emulator_pid" >/dev/null 2>&1 || true
            wait "$emulator_pid" 2>/dev/null || true
            exit "$status"
        }
        trap cleanup EXIT

        adb -s "$emulator_serial" wait-for-device
        boot_deadline=$((SECONDS + 600))
        while [[ "$(
            adb -s "$emulator_serial" shell getprop sys.boot_completed 2>/dev/null |
                tr -d '\r'
        )" != "1" ]]; do
            if ! kill -0 "$emulator_pid" 2>/dev/null; then
                echo "Android emulator exited before completing boot" >&2
                wait "$emulator_pid" || true
                exit 1
            fi
            if (( SECONDS >= boot_deadline )); then
                echo "Android emulator did not finish booting within 600 seconds" >&2
                exit 1
            fi
            sleep 2
        done

        actual_page_size=$(adb -s "$emulator_serial" shell getconf PAGE_SIZE | tr -d '\r')
        if [[ "$actual_page_size" != "$expected_page_size" ]]; then
            echo "Android page size mismatch: expected $expected_page_size, got $actual_page_size" >&2
            exit 1
        fi

        export CARGO_NDK_ADB_SERIAL=$emulator_serial
        run_android_tests
        ;;
    *)
        echo "unknown Android test mode: $mode" >&2
        usage >&2
        exit 2
        ;;
esac
