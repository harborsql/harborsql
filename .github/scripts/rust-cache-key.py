#!/usr/bin/env python3
"""Compute a Rust dependency cache key that ignores local package versions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def cargo_metadata() -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def workspace_packages(metadata: dict) -> list[dict]:
    workspace_member_ids = set(metadata.get("workspace_members", []))
    packages = [
        package
        for package in metadata.get("packages", [])
        if package.get("id") in workspace_member_ids
    ]
    return sorted(packages, key=lambda package: package.get("manifest_path", ""))


def normalize_manifest(path: Path) -> tuple[str, str]:
    current_section = None
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current_section = stripped.strip("[]").strip()
        if current_section == "package" and re.match(r"^\s*version\s*=", line):
            continue
        lines.append(line.rstrip())
    return str(path.relative_to(ROOT)), "\n".join(lines)


def normalize_lockfile(path: Path, local_names: set[str]) -> str:
    normalized_lines = []
    current_block = []

    def flush_block() -> None:
        if not current_block:
            return
        name = None
        has_source = False
        for line in current_block:
            name_match = re.match(r'^\s*name\s*=\s*"([^"]+)"', line)
            if name_match:
                name = name_match.group(1)
            if re.match(r"^\s*source\s*=", line):
                has_source = True
        for line in current_block:
            if name in local_names and not has_source and re.match(
                r"^\s*version\s*=", line
            ):
                continue
            normalized_lines.append(line.rstrip())

    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() == "[[package]]":
            flush_block()
            current_block = [line]
        elif current_block:
            current_block.append(line)
        else:
            normalized_lines.append(line.rstrip())

    flush_block()
    return "\n".join(normalized_lines)


def optional_file_payload(relative_path: str) -> tuple[str, str] | None:
    path = ROOT / relative_path
    if not path.exists():
        return None
    return relative_path, path.read_text(encoding="utf-8")


def main() -> int:
    root_manifest = ROOT / "Cargo.toml"
    lockfile = ROOT / "Cargo.lock"
    if not root_manifest.exists() or not lockfile.exists():
        print("Cargo.toml and Cargo.lock are required", file=sys.stderr)
        return 1

    metadata = cargo_metadata()
    packages = workspace_packages(metadata)
    manifests = [Path(package["manifest_path"]).resolve() for package in packages]
    package_names = {package["name"] for package in packages}
    payload = {
        "manifests": [normalize_manifest(path) for path in manifests],
        "lockfile": normalize_lockfile(lockfile, package_names),
        "optional_files": [
            item
            for item in (
                optional_file_payload("rust-toolchain"),
                optional_file_payload("rust-toolchain.toml"),
                optional_file_payload(".cargo/config.toml"),
            )
            if item is not None
        ],
    }

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(encoded).hexdigest()[:16]
    print(f"dependency_hash={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
