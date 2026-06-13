#!/usr/bin/env python3
"""Normalize selected Ansible collection JSON files into JSONL tables for Coral.

This script is intentionally allowlist-based. It does not export raw facts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

TABLES = ["hosts", "services", "packages", "mounts", "interfaces", "security", "roles"]


def fact_get(facts: dict[str, Any], name: str, default: Any = None) -> Any:
    value = facts.get(name, default)
    if value in (None, "", [], {}):
        return facts.get(f"ansible_{name}", default)
    return value


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
        hostname = fact_get(facts, "nodename") or fact_get(facts, "hostname")
    if not hostname:
        raise ValueError("could not determine hostname")
    return str(hostname), facts


def normalize_host(hostname: str, facts: dict[str, Any]) -> dict[str, Any]:
    python_fact = fact_get(facts, "python", {})
    python = python_fact if isinstance(python_fact, dict) else {}
    return {
        "hostname": hostname,
        "fqdn": as_str(fact_get(facts, "fqdn")),
        "distribution": as_str(fact_get(facts, "distribution")),
        "distribution_version": as_str(fact_get(facts, "distribution_version")),
        "distribution_major_version": as_str(fact_get(facts, "distribution_major_version")),
        "os_family": as_str(fact_get(facts, "os_family")),
        "kernel": as_str(fact_get(facts, "kernel")),
        "architecture": as_str(fact_get(facts, "architecture")),
        "system": as_str(fact_get(facts, "system")),
        "service_mgr": as_str(fact_get(facts, "service_mgr")),
        "pkg_mgr": as_str(fact_get(facts, "pkg_mgr")),
        "processor_vcpus": as_int(fact_get(facts, "processor_vcpus")),
        "processor_cores": as_int(fact_get(facts, "processor_cores")),
        "memtotal_mb": as_int(fact_get(facts, "memtotal_mb")),
        "uptime_seconds": as_int(fact_get(facts, "uptime_seconds")),
        "virtualization_type": as_str(fact_get(facts, "virtualization_type")),
        "virtualization_role": as_str(fact_get(facts, "virtualization_role")),
        "python_executable": as_str(python.get("executable")),
        "python_version": as_str(python.get("version", {}).get("string") if isinstance(python.get("version"), dict) else python.get("version")),
    }


def normalize_services(hostname: str, facts: dict[str, Any]) -> Iterable[dict[str, Any]]:
    services = fact_get(facts, "services", {})
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
    packages = fact_get(facts, "packages", {})
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
    mounts = fact_get(facts, "mounts", [])
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
    for iface in fact_get(facts, "interfaces", []) or []:
        if not isinstance(iface, str):
            continue
        details = fact_get(facts, iface.replace("-", "_"), {})
        if not isinstance(details, dict):
            continue
        ipv4 = details.get("ipv4", {}) if isinstance(details.get("ipv4"), dict) else {}
        ipv6 = details.get("ipv6", []) if isinstance(details.get("ipv6"), list) else []
        rows.append({
            "hostname": hostname,
            "interface": iface,
            "ipv4_address": as_str(ipv4.get("address")),
            "ipv6_addresses": [entry.get("address") for entry in ipv6 if isinstance(entry, dict) and entry.get("address")],
            "macaddress": as_str(details.get("macaddress")),
            "mtu": as_int(details.get("mtu")),
            "active": bool(details.get("active")) if details.get("active") is not None else None,
            "type": as_str(details.get("type")),
        })
    return rows


def normalize_security(hostname: str, facts: dict[str, Any]) -> dict[str, Any]:
    selinux_fact = fact_get(facts, "selinux", {})
    apparmor_fact = fact_get(facts, "apparmor", {})
    selinux = selinux_fact if isinstance(selinux_fact, dict) else {}
    apparmor = apparmor_fact if isinstance(apparmor_fact, dict) else {}
    firewall_hint = "unknown"
    packages_fact = fact_get(facts, "packages", {})
    packages = packages_fact if isinstance(packages_fact, dict) else {}
    if "firewalld" in packages:
        firewall_hint = "firewalld-present"
    elif "ufw" in packages:
        firewall_hint = "ufw-present"
    elif "nftables" in packages:
        firewall_hint = "nftables-present"
    fips = fact_get(facts, "fips")
    return {
        "hostname": hostname,
        "selinux_status": as_str(selinux.get("status")),
        "selinux_mode": as_str(selinux.get("mode")),
        "selinux_policy": as_str(selinux.get("type")),
        "apparmor_status": as_str(apparmor.get("status")),
        "fips": bool(fips) if fips is not None else None,
        "ssh_host_keys_collected": any(str(k).startswith(("ssh_host_key", "ansible_ssh_host_key")) for k in facts.keys()),
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
                    "expected_service": as_str(item.get("expected_service") or item.get("service")),
                })
    return [row for row in rows if row.get("role")]


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            clean = {k: v for k, v in row.items() if v is not None}
            f.write(json.dumps(clean, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory containing selected Ansible collection JSON files")
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
