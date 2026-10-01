#!/usr/bin/env python3
"""Validate JSONL fixtures for the ansible Coral source."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REQUIRED_FILES = [
    "hosts.jsonl",
    "services.jsonl",
    "packages.jsonl",
    "mounts.jsonl",
    "interfaces.jsonl",
    "security.jsonl",
    "roles.jsonl",
]

REQUIRED_KEYS = {
    "hosts.jsonl": ["hostname", "distribution", "service_mgr", "pkg_mgr"],
    "services.jsonl": ["hostname", "name", "state"],
    "packages.jsonl": ["hostname", "name", "version"],
    "mounts.jsonl": ["hostname", "mount"],
    "interfaces.jsonl": ["hostname", "interface"],
    "security.jsonl": ["hostname"],
    "roles.jsonl": ["hostname", "role"],
}

FORBIDDEN_SUBSTRINGS = [
    "password",
    "passwd",
    "secret",
    "token",
    "private_key",
    "BEGIN OPENSSH",
    "BEGIN RSA",
    "vault",
]


def validate_file(path: Path) -> int:
    count = 0
    if path.name not in REQUIRED_KEYS:
        raise ValueError(f"{path} is not a known fixture file")
    keys = REQUIRED_KEYS[path.name]
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            lowered = line.lower()
            for forbidden in FORBIDDEN_SUBSTRINGS:
                if forbidden.lower() in lowered:
                    raise ValueError(f"{path}:{lineno} contains forbidden substring: {forbidden}")
            row = json.loads(line)
            for key in keys:
                if key not in row:
                    raise ValueError(f"{path}:{lineno} missing required key: {key}")
            if path.name == "interfaces.jsonl" and not isinstance(row.get("ipv6_addresses", []), list):
                raise ValueError(f"{path}:{lineno} ipv6_addresses must be a JSON array")
            count += 1
    if count == 0:
        raise ValueError(f"{path} has no rows")
    return count


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_normalizer_workflow() -> None:
    test_dir = Path(__file__).resolve().parent
    source_dir = test_dir.parent
    script_path = source_dir / "scripts" / "normalize-ansible-facts.py"
    payload_path = test_dir / "selected-payload.json"

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        input_dir = temp_path / "input"
        output_dir = temp_path / "output"
        input_dir.mkdir()
        shutil.copyfile(payload_path, input_dir / payload_path.name)

        subprocess.run(
            [sys.executable, str(script_path), "--input", str(input_dir), "--output", str(output_dir)],
            check=True,
            capture_output=True,
            text=True,
        )

        generated_counts = {filename: validate_file(output_dir / filename) for filename in REQUIRED_FILES}
        require(generated_counts["hosts.jsonl"] == 1, "selected payload should generate one host")
        require(generated_counts["services.jsonl"] == 1, "selected payload should generate one service")
        require(generated_counts["packages.jsonl"] == 2, "selected payload should generate two packages")
        require(generated_counts["mounts.jsonl"] == 1, "selected payload should generate one mount")
        require(generated_counts["interfaces.jsonl"] == 1, "selected payload should generate one interface")
        require(generated_counts["security.jsonl"] == 1, "selected payload should generate one security row")
        require(generated_counts["roles.jsonl"] == 1, "selected payload should generate one role")

        host = read_jsonl(output_dir / "hosts.jsonl")[0]
        require(host["hostname"] == "selected-host", "host hostname mismatch")
        require(host["distribution"] == "Debian", "host distribution mismatch")
        require(host["python_version"] == "3.11.2", "python version mismatch")

        interface = read_jsonl(output_dir / "interfaces.jsonl")[0]
        require(interface["ipv6_addresses"] == ["2001:db8::20"], "ipv6_addresses must remain a JSON array")

        security = read_jsonl(output_dir / "security.jsonl")[0]
        require(security["ssh_host_keys_collected"] is False, "selected payload should not report SSH host keys")
        require(security["firewall_hint"] == "ufw-present", "firewall hint mismatch")

        role = read_jsonl(output_dir / "roles.jsonl")[0]
        require(role["expected_service"] == "sshd.service", "role expected_service mismatch")
        require("ignored_extra" not in role, "unexpected role field exported")

    print("OK selected-payload normalization: generated 7 JSONL tables")


def main() -> None:
    fixture_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("fixtures")
    for filename in REQUIRED_FILES:
        path = fixture_dir / filename
        if not path.exists():
            raise FileNotFoundError(path)
        count = validate_file(path)
        print(f"OK {filename}: {count} rows")
    validate_normalizer_workflow()


if __name__ == "__main__":
    main()
