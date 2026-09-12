#!/usr/bin/env python3
"""Parse Claude Code memory files into JSONL for the Coral claude_code_sessions source.

Uses only Python stdlib. No external dependencies.

Usage:
    python3 memories-to-jsonl.py
    python3 memories-to-jsonl.py --output ~/.coral/claude_code_sessions

Reads all .md files (except MEMORY.md) from ~/.claude/projects/*/memory/
and outputs memories.jsonl with parsed YAML frontmatter + body content.
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

DEFAULT_CLAUDE_DIR = os.path.expanduser("~/.claude/projects")
DEFAULT_OUTPUT = os.path.expanduser("~/.coral/claude_code_sessions")


def parse_frontmatter(content):
    if not content.startswith("---"):
        return {}, content
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}, content
    front = parts[1].strip()
    body = parts[2].lstrip("\n")
    meta = {}
    current_key = None
    for line in front.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if ":" in stripped and not stripped.startswith("-"):
            key, _, val = stripped.partition(":")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key == "metadata":
                continue
            meta[key] = val if val else None
            current_key = key
        elif stripped.startswith("-") and current_key:
            pass
        elif ":" in stripped:
            key, _, val = stripped.partition(":")
            meta[key.strip()] = val.strip().strip('"').strip("'")
    return meta, body


def main():
    parser = argparse.ArgumentParser(
        description="Parse Claude Code memories to JSONL"
    )
    parser.add_argument(
        "--claude-dir", default=DEFAULT_CLAUDE_DIR,
        help=f"Claude projects directory (default: {DEFAULT_CLAUDE_DIR})",
    )
    parser.add_argument(
        "--output", "-o", default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    claude_dir = Path(args.claude_dir)
    if not claude_dir.exists():
        print(f"  ✗ Claude projects directory not found: {claude_dir}",
              file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "memories.jsonl"

    memory_files = sorted(
        f for f in claude_dir.rglob("memory/*.md")
        if f.name != "MEMORY.md"
    )

    if not memory_files:
        print("  ✗ No memory files found", file=sys.stderr)
        sys.exit(1)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, suffix=".jsonl", delete=False
    )

    count = 0
    try:
        for mf in memory_files:
            content = mf.read_text(encoding="utf-8", errors="replace")
            meta, body = parse_frontmatter(content)

            project_dir = str(mf.parent.parent.name)

            row = {
                "name": meta.get("name", mf.stem),
                "description": meta.get("description", ""),
                "type": meta.get("type", "unknown"),
                "origin_session_id": meta.get("originSessionId", ""),
                "project": project_dir,
                "body": body,
            }
            tmp.write(json.dumps(row) + "\n")
            count += 1

        tmp.close()
        shutil.move(tmp.name, output_path)

    except BaseException:
        tmp.close()
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise

    print(f"  ✓ {count} memories → {output_path}")


if __name__ == "__main__":
    main()
