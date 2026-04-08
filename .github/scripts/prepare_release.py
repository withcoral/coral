#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

SEMVER_RE = re.compile(
    r"^v?"
    r"(0|[1-9]\d*)\."
    r"(0|[1-9]\d*)\."
    r"(0|[1-9]\d*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class ReleaseError(Exception):
    pass


@dataclass(frozen=True)
class Version:
    major: int
    minor: int
    patch: int
    prerelease: tuple[tuple[int, int | str], ...]
    is_stable: bool

    @classmethod
    def parse(cls, raw: str) -> "Version | None":
        match = SEMVER_RE.fullmatch(raw)
        if match is None:
            return None
        major, minor, patch, prerelease = match.groups()
        prerelease_parts: tuple[tuple[int, int | str], ...]
        if prerelease is None:
            prerelease_parts = ()
            is_stable = True
        else:
            prerelease_parts = tuple(
                parse_prerelease_identifier(part) for part in prerelease.split(".")
            )
            is_stable = False
        return cls(
            major=int(major),
            minor=int(minor),
            patch=int(patch),
            prerelease=prerelease_parts,
            is_stable=is_stable,
        )

    def sort_key(self) -> tuple[int, int, int, int, tuple[tuple[int, int | str], ...]]:
        return (self.major, self.minor, self.patch, 1 if self.is_stable else 0, self.prerelease)


def parse_prerelease_identifier(part: str) -> tuple[int, int | str]:
    if part.isdigit():
        return (0, int(part))
    return (1, part)


def latest_version_tag(tags: Iterable[str]) -> str | None:
    ranked_tags: list[tuple[tuple[int, int, int, int, tuple[tuple[int, int | str], ...]], str]] = []
    for tag in tags:
        version = Version.parse(tag)
        if version is None:
            continue
        ranked_tags.append((version.sort_key(), tag))
    if not ranked_tags:
        return None
    return max(ranked_tags, key=lambda item: item[0])[1]


def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip()
        command = " ".join(["git", *args])
        raise ReleaseError(stderr or f"command failed: {command}")
    return result.stdout.strip()


def read_workspace_version(revision: str) -> str:
    cargo_toml = git("show", f"{revision}:Cargo.toml")
    data = tomllib.loads(cargo_toml)
    return data.get("workspace", {}).get("package", {}).get("version", "")


def maybe_read_workspace_version(revision: str) -> str:
    try:
        return read_workspace_version(revision)
    except ReleaseError:
        return ""


def append_outputs(outputs: dict[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in outputs.items()]
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
            handle.write("\n")
    else:
        print("\n".join(lines))


def require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ReleaseError(f"Missing required environment variable {name}.")
    return value


def prepare_outputs() -> dict[str, str]:
    event_name = require_env("GITHUB_EVENT_NAME")
    github_ref = require_env("GITHUB_REF")
    github_sha = require_env("GITHUB_SHA")
    target_ref = os.environ.get("TARGET_REF", "")
    push_before_sha = os.environ.get("PUSH_BEFORE_SHA", "")

    if event_name == "workflow_dispatch" and github_ref != "refs/heads/main":
        raise ReleaseError("Run this workflow from main.")

    source_ref = target_ref or github_sha
    resolved_sha = git("rev-parse", f"{source_ref}^{{commit}}", check=False)
    if not resolved_sha:
        raise ReleaseError(f"Could not resolve ref '{source_ref}' to a commit.")

    if event_name == "workflow_dispatch":
        on_main = subprocess.run(
            ["git", "merge-base", "--is-ancestor", resolved_sha, "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
        if on_main.returncode != 0:
            raise ReleaseError("target_ref must point to a commit on main.")

    current_version = read_workspace_version(resolved_sha)
    if not current_version:
        raise ReleaseError("Could not read [workspace.package].version from Cargo.toml")

    if event_name == "push":
        previous_ref = (
            push_before_sha
            if push_before_sha and set(push_before_sha) != {"0"}
            else f"{resolved_sha}^"
        )
        previous_version = maybe_read_workspace_version(previous_ref)
        if current_version == previous_version:
            print(f"Version unchanged ({current_version}), skipping release.")
            return {"should_release": "false"}
        print(f"Version changed: {previous_version} -> {current_version}")

    tag_name = f"v{current_version}"
    parsed_version = Version.parse(tag_name)
    if parsed_version is None:
        raise ReleaseError(f"version must look like v0.2.0, got {tag_name}")

    if event_name == "workflow_dispatch":
        tag_exists = subprocess.run(
            ["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag_name}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if tag_exists.returncode == 0:
            raise ReleaseError(f"Tag {tag_name} already exists.")

        latest_tag = latest_version_tag(git("tag", "--list", "v*").splitlines())
        if latest_tag is not None:
            latest_version = Version.parse(latest_tag)
            assert latest_version is not None
            if parsed_version.sort_key() <= latest_version.sort_key():
                raise ReleaseError(f"Version {tag_name} is not newer than latest tag {latest_tag}.")

    return {
        "should_release": "true",
        "build_ref": resolved_sha,
        "tag_name": tag_name,
        "release_name": tag_name,
        "artifact_prefix": "coral",
    }


def main() -> int:
    try:
        append_outputs(prepare_outputs())
    except ReleaseError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
