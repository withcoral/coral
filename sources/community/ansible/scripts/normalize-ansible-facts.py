#!/usr/bin/env python3
"""Normalize selected Ansible fact JSON files into JSONL tables for Coral.

This script is intentionally allowlist-based. It does not export raw facts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

TABLES = ["hosts", "services", "packages", "mounts", "interfaces", "security", "roles"]


def as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def get_facts(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    hostname = payload.get("inventory_hostname") or payload.get("hostname") or payload.get("name")
    facts = payload.get("ansible_facts", payload)
    if not isinstance(facts, dict):
        raise ValueError("expected ansible_facts object")
    if not hostname:
        hostname = facts.get("nodename") or facts.get("hostname")
    if not hostname:
        raise ValueError("could not determine hostname")
    return str(hostname), facts


def normalize_host(hostname: str, facts: dict[str, Any]) -> dict[str, Any]:
    python = facts.get("python", {}) if isinstance(facts.get("python"), dict) else {}
    return {
        "hostname": hostname,
        "fqdn": as_str(facts.get("fqdn")),
        "distribution": as_str(facts.get("distribution")),
        "distribution_version": as_str(facts.get("distribution_version")),
        "distribution_major_version": as_str(facts.get("distribution_major_version")),
        "os_family": as_str(facts.get("os_family")),
        "kernel": as_str(facts.get("kernel")),
        "architecture": as_str(facts.get("architecture")),
        "system": as_str(facts.get("system")),
        "service_mgr": as_str(facts.get("service_mgr")),
        "pkg_mgr": as_str(facts.get("pkg_mgr")),
        "processor_vcpus": as_int(facts.get("processor_vcpus")),
        "processor_cores": as_int(facts.get("processor_cores")),
        "memtotal_mb": as_int(facts.get("memtotal_mb")),
        "uptime_seconds": as_int(facts.get("uptime_seconds")),
        "virtualization_type": as_str(facts.get("virtualization_type")),
        "virtualization_role": as_str(facts.get("virtualization_role")),
        "python_executable": as_str(python.get("executable")),
        "python_version": as_str(python.get("version", {}).get("string") if isinstance(python.get("version"), dict) else python.get("version")),
    }


def normalize_services(hostname: str, facts: dict[str, Any]) -> Iterable[dict[str, Any]]:
    services = facts.get("services", {})
    if not isinstance(services, dict):
        return []
    rows = []
    for name, svc in services.items():
        if not isinstance(svc, dict):
            continue
        rows.append({
            "hostname": hostname,
            "name": as_str(svc.get("name") or name),
            "source": as_str(svc.get("source")),
            "state": as_str(svc.get("state")),
            "status": as_str(svc.get("status")),
        })
    return rows


def normalize_packages(hostname: str, facts: dict[str, Any]) -> Iterable[dict[str, Any]]:
    packages = facts.get("packages", {})
    if not isinstance(packages, dict):
        return []
    rows = []
    for name, versions in packages.items():
        if not isinstance(versions, list):
            continue
        for pkg in versions:
            if not isinstance(pkg, dict):
                continue
            rows.append({
                "hostname": hostname,
                "name": as_str(pkg.get("name") or name),
                "version": as_str(pkg.get("version")),
                "release": as_str(pkg.get("release")),
                "arch": as_str(pkg.get("arch")),
                "source": as_str(pkg.get("source")),
                "epoch": as_str(pkg.get("epoch")),
                "origin": as_str(pkg.get("origin")),
            })
    return rows


def normalize_mounts(hostname: str, facts: dict[str, Any]) -> Iterable[dict[str, Any]]:
    mounts = facts.get("mounts", [])
    if not isinstance(mounts, list):
        return []
    rows = []
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        rows.append({
            "hostname": hostname,
            "mount": as_str(mount.get("mount")),
            "device": as_str(mount.get("device")),
            "fstype": as_str(mount.get("fstype")),
            "size_total": as_int(mount.get("size_total")),
            "size_available": as_int(mount.get("size_available")),
            "options": ",".join(mount.get("options", [])) if isinstance(mount.get("options"), list) else as_str(mount.get("options")),
        })
    return rows


def normalize_interfaces(hostname: str, facts: dict[str, Any]) -> Iterable[dict[str, Any]]:
    rows = []
    for iface in facts.get("interfaces", []) or []:
        if not isinstance(iface, str):
            continue
        details = facts.get(iface.replace("-", "_"), {})
        if not isinstance(details, dict):
            continue
        ipv4 = details.get("ipv4", {}) if isinstance(details.get("ipv4"), dict) else {}
        ipv6 = details.get("ipv6", []) if isinstance(details.get("ipv6"), list) else []
        rows.append({
            "hostname": hostname,
            "interface": iface,
            "ipv4_address": as_str(ipv4.get("address")),
            "ipv6_addresses": json.dumps([entry.get("address") for entry in ipv6 if isinstance(entry, dict) and entry.get("address")]),
            "macaddress": as_str(details.get("macaddress")),
            "mtu": as_int(details.get("mtu")),
            "active": bool(details.get("active")) if details.get("active") is not None else None,
            "type": as_str(details.get("type")),
        })
    return rows


def normalize_security(hostname: str, facts: dict[str, Any]) -> dict[str, Any]:
    selinux = facts.get("selinux", {}) if isinstance(facts.get("selinux"), dict) else {}
    apparmor = facts.get("apparmor", {}) if isinstance(facts.get("apparmor"), dict) else {}
    firewall_hint = "unknown"
    packages = facts.get("packages", {}) if isinstance(facts.get("packages"), dict) else {}
    if "firewalld" in packages:
        firewall_hint = "firewalld-present"
    elif "ufw" in packages:
        firewall_hint = "ufw-present"
    elif "nftables" in packages:
        firewall_hint = "nftables-present"
    return {
        "hostname": hostname,
        "selinux_status": as_str(selinux.get("status")),
        "selinux_mode": as_str(selinux.get("mode")),
        "selinux_policy": as_str(selinux.get("policyvers") or selinux.get("type")),
        "apparmor_status": as_str(apparmor.get("status")),
        "fips": bool(facts.get("fips")) if facts.get("fips") is not None else None,
        "ssh_host_keys_collected": any(str(k).startswith("ssh_host_key") for k in facts.keys()),
        "firewall_hint": firewall_hint,
    }


def normalize_roles(hostname: str, payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    rows = []
    roles = payload.get("coral_roles", [])
    expected = payload.get("coral_expected_services", [])
    if isinstance(roles, list):
        for role in roles:
            if isinstance(role, str):
                rows.append({
                    "hostname": hostname,
                    "role": role,
                    "environment": None,
                    "source_file": None,
                    "expected_service": None,
                })
            elif isinstance(role, dict):
                rows.append({
                    "hostname": hostname,
                    "role": as_str(role.get("role") or role.get("name")),
                    "environment": as_str(role.get("environment")),
                    "source_file": as_str(role.get("source_file")),
                    "expected_service": as_str(role.get("expected_service")),
                })
    if isinstance(expected, list):
        for item in expected:
            if isinstance(item, dict):
                rows.append({
                    "hostname": hostname,
                    "role": as_str(item.get("role") or "unknown"),
                    "environment": as_str(item.get("environment")),
                    "source_file": as_str(item.get("source_file")),
                    "expected_service": as_str(item.get("service")),
                })
    return [row for row in rows if row.get("role")]


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            clean = {k: v for k, v in row.items() if v is not None}
            f.write(json.dumps(clean, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory containing raw Ansible fact JSON files")
    parser.add_argument("--output", required=True, help="Directory to write JSONL tables")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    collected = {table: [] for table in TABLES}

    for file_path in sorted(input_dir.glob("*.json")):
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        hostname, facts = get_facts(payload)
        collected["hosts"].append(normalize_host(hostname, facts))
        collected["services"].extend(normalize_services(hostname, facts))
        collected["packages"].extend(normalize_packages(hostname, facts))
        collected["mounts"].extend(normalize_mounts(hostname, facts))
        collected["interfaces"].extend(normalize_interfaces(hostname, facts))
        collected["security"].append(normalize_security(hostname, facts))
        collected["roles"].extend(normalize_roles(hostname, payload))

    for table, rows in collected.items():
        write_jsonl(output_dir / f"{table}.jsonl", rows)

    print(f"Wrote normalized tables to {output_dir}")
    for table, rows in collected.items():
        print(f"{table}: {len(rows)} rows")


if __name__ == "__main__":
    main()
