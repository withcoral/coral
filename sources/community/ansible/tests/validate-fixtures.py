#!/usr/bin/env python3
"""Validate JSONL fixtures for the ansible Coral source."""

from __future__ import annotations

import json
import sys
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


def main() -> None:
    fixture_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("fixtures")
    for filename in REQUIRED_FILES:
        path = fixture_dir / filename
        if not path.exists():
            raise FileNotFoundError(path)
        count = validate_file(path)
        print(f"OK {filename}: {count} rows")


if __name__ == "__main__":
    main()
