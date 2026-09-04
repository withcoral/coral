#!/usr/bin/env python3
"""Export OpenCode session, message, and project data to JSONL for Coral.

Uses only Python stdlib (sqlite3). No external dependencies.

Usage:
    python3 opencode-to-jsonl.py
    python3 opencode-to-jsonl.py --db-path /path/to/opencode.db
    python3 opencode-to-jsonl.py --db-path /path/to/opencode.db --output /path/to/dir

OpenCode stores its data in a local SQLite database. By default the script
reads `~/.local/share/opencode/opencode.db` and writes:

    sessions.jsonl          — one row per session with flattened columns
                              and a metadata JSON blob for the raw
                              JSON-shaped fields
    messages.jsonl          — one row per message with id, session id,
                              timestamps, and the full payload as JSON
    session_messages.jsonl  — one row per session_message (alternative
                              message table indexed by session sequence)
    parts.jsonl             — one row per message part (text, tool calls,
                              images, etc.) — the actual transcript content
    todos.jsonl             — one row per session todo list entry
    session_inputs.jsonl    — one row per user prompt admitted to a session
    session_shares.jsonl    — one row per shared session URL
    projects.jsonl          — one row per project with id, worktree, name,
                              and timestamps
    project_directories.jsonl — one row per directory attached to a project
    workspaces.jsonl        — one row per workspace

Default output directory: `~/.coral/opencode/`.

The script reads the database in a read-only connection (`mode=ro`) so it
never touches OpenCode's live state. Re-run any time the on-disk database
changes to refresh Coral's view.
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

DEFAULT_DB_PATH = os.path.expanduser("~/.local/share/opencode/opencode.db")
DEFAULT_OUTPUT = os.path.expanduser("~/.coral/opencode")


def open_readonly(db_path: Path) -> sqlite3.Connection:
    """Open the SQLite database in read-only mode.

    Uses `Path.resolve().as_uri()` so paths containing URI-significant
    characters (`?`, `#`, `%`, spaces, non-ASCII) are percent-encoded
    correctly instead of breaking the SQLite URI parser.
    """
    if not db_path.is_file():
        raise FileNotFoundError(
            f"OpenCode database not found: {db_path}\n"
            f"  - Is OpenCode installed and has run at least once?\n"
            f"  - Or pass --db-path to point at a non-default location."
        )
    uri = db_path.resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def parse_model(model: str):
    """Split OpenCode's JSON-encoded model column into (provider, model_id).

    OpenCode stores the model as a JSON object shaped
    `{"id":"MiniMax-M3","providerID":"samagama","variant":"default"}`.
    Anything we cannot parse is returned as (None, None) so the caller can
    fall back to the raw value rather than guess.
    """
    if not model:
        return None, None
    try:
        obj = json.loads(model)
    except (TypeError, ValueError):
        return None, None
    if not isinstance(obj, dict):
        return None, None
    provider = obj.get("providerID") or None
    model_id = obj.get("id") or None
    return provider, model_id


def fetch_sessions(conn: sqlite3.Connection):
    sql = """
        SELECT id, project_id, parent_id, workspace_id, slug, title, directory,
               path, agent, model, version, share_url,
               tokens_input, tokens_output, tokens_reasoning,
               tokens_cache_read, tokens_cache_write, cost,
               time_created, time_updated, time_compacting, time_archived,
               metadata, summary_diffs, revert, permission
        FROM session
        ORDER BY time_updated DESC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        model_provider, model_id = parse_model(r["model"] or "")
        metadata_blob = {}
        for field, key in (
            ("metadata", "metadata"),
            ("summary_diffs", "summary_diffs"),
            ("revert", "revert"),
            ("permission", "permission"),
        ):
            raw = r[field]
            if raw is None or raw == "":
                continue
            try:
                metadata_blob[key] = json.loads(raw)
            except (TypeError, ValueError):
                metadata_blob[key] = raw
        rows.append(
            {
                "id": r["id"],
                "project_id": r["project_id"],
                "parent_id": r["parent_id"],
                "workspace_id": r["workspace_id"],
                "slug": r["slug"],
                "title": r["title"],
                "directory": r["directory"],
                "path": r["path"],
                "agent": r["agent"],
                "model": r["model"],
                "model_id": model_id,
                "model_provider": model_provider,
                "version": r["version"],
                "share_url": r["share_url"],
                "tokens_input": int(r["tokens_input"] or 0),
                "tokens_output": int(r["tokens_output"] or 0),
                "tokens_reasoning": int(r["tokens_reasoning"] or 0),
                "tokens_cache_read": int(r["tokens_cache_read"] or 0),
                "tokens_cache_write": int(r["tokens_cache_write"] or 0),
                "cost": float(r["cost"] or 0.0),
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
                "time_compacting": int(r["time_compacting"]) if r["time_compacting"] is not None else None,
                "time_archived": int(r["time_archived"]) if r["time_archived"] is not None else None,
                "metadata": metadata_blob or None,
            }
        )
    return rows


def fetch_messages(conn: sqlite3.Connection):
    sql = """
        SELECT id, session_id, time_created, time_updated, data
        FROM message
        ORDER BY session_id ASC, time_created ASC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        raw = r["data"]
        try:
            payload = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            payload = {"_raw": raw}
        rows.append(
            {
                "id": r["id"],
                "session_id": r["session_id"],
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
                "data": payload,
            }
        )
    return rows


def fetch_projects(conn: sqlite3.Connection):
    sql = """
        SELECT id, worktree, vcs, name, icon_url, icon_url_override,
               icon_color, time_created, time_updated, time_initialized
        FROM project
        ORDER BY time_created DESC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "worktree": r["worktree"],
                "vcs": r["vcs"],
                "name": r["name"],
                "icon_url": r["icon_url_override"] or r["icon_url"],
                "icon_color": r["icon_color"],
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
                "time_initialized": int(r["time_initialized"]) if r["time_initialized"] is not None else None,
            }
        )
    return rows


def fetch_project_directories(conn: sqlite3.Connection):
    sql = """
        SELECT project_id, directory, type, strategy, time_created
        FROM project_directory
        ORDER BY project_id ASC, directory ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "project_id": r["project_id"],
                "directory": r["directory"],
                "type": r["type"],
                "strategy": r["strategy"],
                "time_created": int(r["time_created"] or 0),
            }
        )
    return rows


def fetch_parts(conn: sqlite3.Connection):
    sql = """
        SELECT id, message_id, session_id, time_created, time_updated, data
        FROM part
        ORDER BY session_id ASC, time_created ASC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        raw = r["data"]
        try:
            payload = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            payload = {"_raw": raw}
        rows.append(
            {
                "id": r["id"],
                "message_id": r["message_id"],
                "session_id": r["session_id"],
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
                "data": payload,
            }
        )
    return rows


def fetch_todos(conn: sqlite3.Connection):
    sql = """
        SELECT session_id, content, status, priority, position,
               time_created, time_updated
        FROM todo
        ORDER BY session_id ASC, position ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "session_id": r["session_id"],
                "content": r["content"],
                "status": r["status"],
                "priority": r["priority"],
                "position": int(r["position"] or 0),
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_session_inputs(conn: sqlite3.Connection):
    sql = """
        SELECT id, session_id, prompt, delivery, admitted_seq, promoted_seq,
               time_created
        FROM session_input
        ORDER BY session_id ASC, admitted_seq ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "session_id": r["session_id"],
                "prompt": r["prompt"],
                "delivery": r["delivery"],
                "admitted_seq": int(r["admitted_seq"] or 0),
                "promoted_seq": int(r["promoted_seq"]) if r["promoted_seq"] is not None else None,
                "time_created": int(r["time_created"] or 0),
            }
        )
    return rows


def fetch_session_messages(conn: sqlite3.Connection):
    sql = """
        SELECT id, session_id, type, seq, time_created, time_updated, data
        FROM session_message
        ORDER BY session_id ASC, seq ASC
    """
    rows = []
    for r in conn.execute(sql):
        raw = r["data"]
        try:
            payload = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            payload = {"_raw": raw}
        rows.append(
            {
                "id": r["id"],
                "session_id": r["session_id"],
                "type": r["type"],
                "seq": int(r["seq"] or 0),
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
                "data": payload,
            }
        )
    return rows


def fetch_session_shares(conn: sqlite3.Connection):
    """Public share rows. The `secret` column is a credential and is
    intentionally **not** exported."""
    sql = """
        SELECT session_id, id, url, time_created, time_updated
        FROM session_share
        ORDER BY time_created DESC, session_id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "session_id": r["session_id"],
                "id": r["id"],
                "url": r["url"],
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_workspaces(conn: sqlite3.Connection):
    sql = """
        SELECT id, project_id, directory, type, name, branch, extra, time_used
        FROM workspace
        ORDER BY project_id ASC, directory ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "project_id": r["project_id"],
                "directory": r["directory"],
                "type": r["type"],
                "name": r["name"],
                "branch": r["branch"],
                "extra": r["extra"],
                "time_used": int(r["time_used"] or 0),
            }
        )
    return rows


def fetch_events(conn: sqlite3.Connection):
    """All domain events — the largest table (~1M rows). The `data` column
    is a JSON blob whose schema varies by `type`."""
    sql = """
        SELECT id, aggregate_id, seq, type, data
        FROM event
        ORDER BY aggregate_id ASC, seq ASC
    """
    rows = []
    for r in conn.execute(sql):
        raw = r["data"]
        try:
            payload = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            payload = {"_raw": raw}
        rows.append(
            {
                "id": r["id"],
                "aggregate_id": r["aggregate_id"],
                "seq": int(r["seq"] or 0),
                "type": r["type"],
                "data": payload,
            }
        )
    return rows


def fetch_event_sequences(conn: sqlite3.Connection):
    sql = """
        SELECT aggregate_id, seq, owner_id
        FROM event_sequence
        ORDER BY aggregate_id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "aggregate_id": r["aggregate_id"],
                "seq": int(r["seq"] or 0),
                "owner_id": r["owner_id"],
            }
        )
    return rows


def fetch_accounts(conn: sqlite3.Connection):
    """OpenCode account / OAuth rows. The `access_token` and
    `refresh_token` columns are live secrets and are intentionally **not**
    exported — same policy as `session_shares.secret`."""
    sql = """
        SELECT id, email, url, token_expiry, time_created, time_updated
        FROM account
        ORDER BY time_created DESC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "email": r["email"],
                "url": r["url"],
                "token_expiry": int(r["token_expiry"]) if r["token_expiry"] is not None else None,
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_account_states(conn: sqlite3.Connection):
    sql = """
        SELECT id, active_account_id, active_org_id
        FROM account_state
        ORDER BY id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": int(r["id"] or 0),
                "active_account_id": r["active_account_id"],
                "active_org_id": r["active_org_id"],
            }
        )
    return rows


def fetch_control_accounts(conn: sqlite3.Connection):
    """Control-plane account rows. The `access_token` and `refresh_token`
    columns are live secrets and are intentionally **not** exported."""
    sql = """
        SELECT email, url, token_expiry, active, time_created, time_updated
        FROM control_account
        ORDER BY time_created DESC, email ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "email": r["email"],
                "url": r["url"],
                "token_expiry": int(r["token_expiry"]) if r["token_expiry"] is not None else None,
                "active": int(r["active"] or 0),
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_credentials(conn: sqlite3.Connection):
    """Stored credential metadata. The `value` column is a live secret
    (API key / token) and is intentionally **not** exported."""
    sql = """
        SELECT id, integration_id, label, connector_id, method_id,
               active, time_created, time_updated
        FROM credential
        ORDER BY time_created DESC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "integration_id": r["integration_id"],
                "label": r["label"],
                "connector_id": r["connector_id"],
                "method_id": r["method_id"],
                "active": int(r["active"]) if r["active"] is not None else None,
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_data_migrations(conn: sqlite3.Connection):
    sql = """
        SELECT name, time_completed
        FROM data_migration
        ORDER BY name ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "name": r["name"],
                "time_completed": int(r["time_completed"] or 0),
            }
        )
    return rows


def fetch_migrations(conn: sqlite3.Connection):
    sql = """
        SELECT id, time_completed
        FROM migration
        ORDER BY time_completed ASC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "time_completed": int(r["time_completed"] or 0),
            }
        )
    return rows


def fetch_permissions(conn: sqlite3.Connection):
    sql = """
        SELECT id, project_id, action, resource, time_created, time_updated
        FROM permission
        ORDER BY project_id ASC, id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "id": r["id"],
                "project_id": r["project_id"],
                "action": r["action"],
                "resource": r["resource"],
                "time_created": int(r["time_created"] or 0),
                "time_updated": int(r["time_updated"] or 0),
            }
        )
    return rows


def fetch_session_context_epochs(conn: sqlite3.Connection):
    sql = """
        SELECT session_id, baseline, snapshot, baseline_seq
        FROM session_context_epoch
        ORDER BY session_id ASC
    """
    rows = []
    for r in conn.execute(sql):
        rows.append(
            {
                "session_id": r["session_id"],
                "baseline": r["baseline"],
                "snapshot": r["snapshot"],
                "baseline_seq": int(r["baseline_seq"] or 0),
            }
        )
    return rows


def write_jsonl_atomic(path: Path, rows):
    """Write `rows` to `path` as JSONL atomically via a temp file in the same dir."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", dir=str(path.parent), suffix=".jsonl", delete=False, encoding="utf-8"
    )
    try:
        for row in rows:
            tmp.write(json.dumps(row, separators=(",", ":")) + "\n")
        tmp.close()
        os.replace(tmp.name, path)
    except BaseException:
        tmp.close()
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise


# Registry of every table the manifest declares. The single source of
# truth for which JSONL files the converter writes. Each entry maps the
# table name to its fetch function.
TABLES = {
    "sessions":               fetch_sessions,
    "messages":               fetch_messages,
    "session_messages":       fetch_session_messages,
    "parts":                  fetch_parts,
    "todos":                  fetch_todos,
    "session_inputs":         fetch_session_inputs,
    "session_shares":         fetch_session_shares,
    "projects":               fetch_projects,
    "project_directories":    fetch_project_directories,
    "workspaces":             fetch_workspaces,
    "events":                 fetch_events,
    "event_sequences":        fetch_event_sequences,
    "accounts":               fetch_accounts,
    "account_states":         fetch_account_states,
    "control_accounts":       fetch_control_accounts,
    "credentials":            fetch_credentials,
    "data_migrations":        fetch_data_migrations,
    "migrations":             fetch_migrations,
    "permissions":            fetch_permissions,
    "session_context_epochs": fetch_session_context_epochs,
}


def main():
    parser = argparse.ArgumentParser(
        description="Export OpenCode session data to JSONL for Coral"
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to the OpenCode SQLite database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--only",
        choices=tuple(TABLES),
        default=None,
        help="Export only one table (default: export all).",
    )
    parser.add_argument(
        "--overwrite-placeholder",
        action="store_true",
        help=(
            "When --only is set, also overwrite any existing placeholder "
            "JSONL files for skipped tables with an empty file. Default "
            "(without this flag) is to leave existing JSONL files alone, "
            "which preserves data already exposed by other manifest tables."
        ),
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    output_dir = Path(args.output)

    selected = [args.only] if args.only else list(TABLES)
    output_dir.mkdir(parents=True, exist_ok=True)

    failures = []
    conn = open_readonly(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # ---- 1. Export every selected table (each in its own try/except). ----
        for table_name in selected:
            output_path = output_dir / f"{table_name}.jsonl"
            try:
                rows = TABLES[table_name](conn)
                write_jsonl_atomic(output_path, rows)
                print(f"  ✓ {len(rows):>6} {table_name:<18} → {output_path}")
            except Exception as exc:
                print(
                    f"  ✗ {table_name:<18} failed: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                failures.append((table_name, str(exc)))
                # Always materialize a (possibly empty) file at the expected
                # path so coral's file backend never has to handle a
                # missing/stale target.
                try:
                    write_jsonl_atomic(output_path, [])
                except OSError as write_exc:
                    print(
                        f"  ✗ {table_name:<18} placeholder write failed: {write_exc}",
                        file=sys.stderr,
                    )

        # ---- 2. For skipped tables, materialize placeholders only when the
        # target file is absent (or --overwrite-placeholder is set). This
        # preserves any pre-existing JSONL data when the user re-runs
        # `opencode-to-jsonl.py --only <table>` against an existing export. ----
        skipped = [t for t in TABLES if t not in selected]
        if skipped:
            placed = []
            kept = []
            for table_name in skipped:
                output_path = output_dir / f"{table_name}.jsonl"
                if output_path.exists() and not args.overwrite_placeholder:
                    kept.append(table_name)
                    continue
                try:
                    write_jsonl_atomic(output_path, [])
                    placed.append(table_name)
                except OSError as exc:
                    print(
                        f"  ✗ {table_name:<18} placeholder write failed: {exc}",
                        file=sys.stderr,
                    )
                    failures.append((table_name, f"placeholder write: {exc}"))
            if placed:
                print(
                    f"\n  --only={args.only!r}: created empty placeholder(s) "
                    f"for {len(placed)} other table(s): "
                    f"{', '.join(placed)}"
                )
            if kept:
                print(
                    f"  --only={args.only!r}: left existing JSONL untouched for "
                    f"{len(kept)} other table(s): {', '.join(kept)} "
                    f"(pass --overwrite-placeholder to clobber)"
                )
    finally:
        conn.close()

    if failures:
        print(
            f"\n  ✗ {len(failures)} table(s) failed; see stderr for details.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\n  OpenCode data exported from {db_path}")
    print(f"  Next: coral source add --file sources/community/opencode/manifest.yaml")

    print(f"\n  OpenCode data exported from {db_path}")
    print(f"  Next: coral source add --file sources/community/opencode/manifest.yaml")


if __name__ == "__main__":
    main()
