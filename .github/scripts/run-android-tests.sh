#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

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
  ANDROID_AVD_HOME     Android virtual-device directory in emulator mode
                       (default: $HOME/.android/avd).
  ANDROID_HOME         Android SDK root. Required in emulator mode.

Prerequisites:

  cargo-ndk, the Rust target for <abi>, and an Android NDK must be installed.
  Emulator mode also requires adb, avdmanager, emulator, jq, and the named
  system image. The CI workflow installs these before invoking this script.

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
    local build_messages
    local test_binary
    local test_binaries

    # cargo-ndk 4.1.2 unconditionally replaces Cargo's target runner with its
    # own runner. Build without executing, then invoke the repository runner
    # directly so device environment and the child status remain observable.
    build_messages=$(
        cargo ndk --target "$abi" --platform "$android_api_level" \
            test --locked -p aws-sdk-s3-transfer-manager --lib --no-run \
            --message-format=json-render-diagnostics
    )
    test_binaries=$(
        jq -r '
            select(
                .reason == "compiler-artifact"
                and .target.name == "aws_sdk_s3_transfer_manager"
                and .target.kind == ["lib"]
                and .profile.test
            )
            | .executable // empty
        ' <<<"$build_messages"
    )

    if [[ -z "$test_binaries" ]]; then
        echo "cargo did not report the Android library test binary" >&2
        exit 1
    fi
    if [[ "$test_binaries" == *$'\n'* ]]; then
        echo "cargo reported multiple Android library test binaries:" >&2
        printf '%s\n' "$test_binaries" >&2
        exit 1
    fi

    test_binary=$test_binaries
    "$script_dir/android-test-runner.sh" "$test_binary"
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
        android_avd_home=${ANDROID_AVD_HOME:-"$HOME/.android/avd"}
        emulator_bin="$ANDROID_HOME/emulator/emulator"
        emulator_log=${RUNNER_TEMP:-/tmp}/s3-tm-android-emulator.log
        emulator_serial=emulator-5554

        if [[ ! -x "$emulator_bin" ]]; then
            echo "Android emulator binary is missing or not executable: $emulator_bin" >&2
            exit 1
        fi

        # Ubuntu 24.04 runners can give avdmanager and emulator different
        # implicit homes through XDG_CONFIG_HOME. Pin both tools to one
        # directory so the emulator can discover the device just created.
        mkdir -p "$android_avd_home"
        export ANDROID_AVD_HOME=$android_avd_home
        printf 'no\n' | avdmanager create avd --force \
            --name "$avd_name" \
            --path "$android_avd_home/${avd_name}.avd" \
            --package "$system_image" \
            --device pixel_6

        available_avds=$("$emulator_bin" -list-avds)
        if ! grep -Fxq "$avd_name" <<<"$available_avds"; then
            echo "Android emulator cannot find newly created AVD '$avd_name'" >&2
            printf 'Available AVDs:\n%s\n' "$available_avds" >&2
            exit 1
        fi

        if [[ -e /dev/kvm ]]; then
            sudo chmod 666 /dev/kvm
        fi

        "$emulator_bin" -avd "$avd_name" \
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
            if [[ -n "${android_cert_bundle:-}" ]]; then
                adb -s "$emulator_serial" shell rm -f "$android_cert_bundle" >/dev/null 2>&1 || true
            fi
            adb -s "$emulator_serial" emu kill >/dev/null 2>&1 || true
            kill "$emulator_pid" >/dev/null 2>&1 || true
            wait "$emulator_pid" 2>/dev/null || true
            exit "$status"
        }
        trap cleanup EXIT

        boot_deadline=$((SECONDS + 600))
        while true; do
            if ! kill -0 "$emulator_pid" 2>/dev/null; then
                echo "Android emulator exited before registering with adb" >&2
                wait "$emulator_pid" || true
                exit 1
            fi
            if (( SECONDS >= boot_deadline )); then
                echo "Android emulator did not register with adb within 600 seconds" >&2
                exit 1
            fi
            adb_state=$(adb -s "$emulator_serial" get-state 2>/dev/null || true)
            if [[ "$adb_state" == "device" ]]; then
                break
            fi
            sleep 2
        done

        while true; do
            if ! kill -0 "$emulator_pid" 2>/dev/null; then
                echo "Android emulator exited before completing boot" >&2
                wait "$emulator_pid" || true
                exit 1
            fi
            if (( SECONDS >= boot_deadline )); then
                echo "Android emulator did not finish booting within 600 seconds" >&2
                exit 1
            fi
            boot_completed=$(
                adb -s "$emulator_serial" shell getprop sys.boot_completed 2>/dev/null || true
            )
            boot_completed=${boot_completed//$'\r'/}
            if [[ "$boot_completed" == "1" ]]; then
                break
            fi
            sleep 2
        done

        actual_page_size=$(adb -s "$emulator_serial" shell getconf PAGE_SIZE | tr -d '\r')
        if [[ "$actual_page_size" != "$expected_page_size" ]]; then
            echo "Android page size mismatch: expected $expected_page_size, got $actual_page_size" >&2
            exit 1
        fi

        android_cert_sources=
        for cert_dir in \
            /apex/com.android.conscrypt/cacerts \
            /system/etc/security/cacerts
        do
            if adb -s "$emulator_serial" shell test -d "$cert_dir"; then
                android_cert_sources+=" $cert_dir/*"
            fi
        done
        if [[ -z "$android_cert_sources" ]]; then
            echo "Android system CA directories are unavailable" >&2
            exit 1
        fi

        # rustls-native-certs expects an OpenSSL-style PEM file or a directory
        # it can enumerate. Android's system CA directories are not directly
        # usable from the deployed test process, so materialize an accessible
        # bundle in the adb shell domain and fail closed if it contains no roots.
        android_cert_bundle=/data/local/tmp/s3-tm-ca-certificates.pem
        adb -s "$emulator_serial" shell \
            "cat$android_cert_sources > $android_cert_bundle"
        android_cert_count=$(
            adb -s "$emulator_serial" shell \
                "grep -c 'BEGIN CERTIFICATE' $android_cert_bundle || true" |
                tr -d '\r'
        )
        if [[ ! "$android_cert_count" =~ ^[1-9][0-9]*$ ]]; then
            echo "Android CA bundle contains no PEM certificates" >&2
            exit 1
        fi

        export CARGO_NDK_ADB_SERIAL=$emulator_serial
        export ANDROID_TEST_SSL_CERT_FILE=$android_cert_bundle
        run_android_tests
        ;;
    *)
        echo "unknown Android test mode: $mode" >&2
        usage >&2
        exit 2
        ;;
esac
