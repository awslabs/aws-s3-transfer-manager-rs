#!/usr/bin/env python3
"""Check the ordinary and extension public APIs from independent Cargo workspaces."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TM = "aws-sdk-s3-transfer-manager"


def cargo(directory, *arguments, check=True, capture=False):
    environment = os.environ.copy()
    environment.pop("RUSTFLAGS", None)
    environment.pop("CARGO_ENCODED_RUSTFLAGS", None)
    environment.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
    return subprocess.run(
        ["cargo", *arguments],
        cwd=directory,
        env=environment,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def write_consumer(directory, name, extension):
    (directory / "src").mkdir(parents=True)
    dependencies = {
        TM: {"path": str(ROOT / TM), "default-features": False},
    }
    source = """use aws_sdk_s3_transfer_manager::{Managed, RuntimeConfig};

fn main() {
    let _ = RuntimeConfig::managed(Managed::default());
}
"""
    if extension:
        dependencies.update({
            "aws-sdk-s3-transfer-manager-ext": {
                "path": str(ROOT / "crates/aws-sdk-s3-transfer-manager-ext"),
            },
            "aws-smithy-execution": {
                "path": str(ROOT / "crates/aws-smithy-execution"),
            },
            "aws-smithy-managed-http": {
                "path": str(ROOT / "crates/aws-smithy-managed-http"),
                "features": ["client"],
            },
        })
        source = """use aws_sdk_s3_transfer_manager as tm;
use aws_sdk_s3_transfer_manager_ext as tm_ext;
use aws_smithy_execution::{topology::Topology, ManagedRuntime};
use aws_smithy_managed_http::client::{pool, ManagedHttpPool};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ManagedRuntime::builder()
        .topology(Topology::detect()?.with_max_placements(1))
        .pin_threads(false)
        .build()?;
    let execution = runtime.handle();
    let pool = ManagedHttpPool::builder()
        .pool_builder(pool::ConnectionPool::builder())
        .runtime(execution.clone())
        .build()?;
    let _http = tm_ext::http_client(execution, &pool)?;
    let _config = tm::RuntimeConfig::managed(tm_ext::managed(runtime));
    Ok(())
}
"""
    manifest = [
        "[package]",
        f'name = "{name}"',
        'version = "0.0.0"',
        'edition = "2024"',
        "",
        "[workspace]",
        "",
        "[dependencies]",
    ]
    for package, settings in dependencies.items():
        values = []
        for key, value in settings.items():
            values.append(f"{key} = {json.dumps(value)}")
        manifest.append(f"{package} = {{ {', '.join(values)} }}")
    (directory / "Cargo.toml").write_text("\n".join(manifest) + "\n")
    (directory / "src/main.rs").write_text(source)
    shutil.copyfile(ROOT / "Cargo.lock", directory / "Cargo.lock")
    return source


def check_consumer(directory, expected):
    cargo(directory, "check")
    metadata = json.loads(cargo(
        directory, "metadata", "--locked", "--format-version", "1", capture=True,
    ).stdout)
    package = next(package["id"] for package in metadata["packages"] if package["name"] == TM)
    features = next(node["features"] for node in metadata["resolve"]["nodes"] if node["id"] == package)
    if features != expected:
        raise RuntimeError(f"unexpected TM features: {features}, expected {expected}")
    print(f"{directory.name} consumer TM features: {features}", flush=True)


def main():
    with tempfile.TemporaryDirectory(prefix="tm-consumers-") as temporary:
        base = Path(temporary)
        ordinary, extension = base / "ordinary", base / "extension"
        original = write_consumer(ordinary, "tm-ordinary-consumer", False)
        write_consumer(extension, "tm-extension-consumer", True)
        check_consumer(ordinary, [])
        check_consumer(extension, ["__unstable_ext"])
        source = ordinary / "src/main.rs"
        try:
            source.write_text(
                "fn main() { let _ = aws_sdk_s3_transfer_manager::unstable::managed; }\n"
            )
            result = cargo(ordinary, "check", "--locked", check=False, capture=True)
            if result.returncode == 0 or "could not find `unstable`" not in result.stderr:
                raise RuntimeError(f"ordinary consumer gate check failed:\n{result.stderr}")
        finally:
            source.write_text(original)
        print("Ordinary consumer cannot access the unstable integration.", flush=True)


if __name__ == "__main__":
    main()
