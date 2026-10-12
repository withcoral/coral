#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Run one benchmark repeat, or read the standard metrics of a finished one.

    uv run run.py run <agent> --mcp-url URL --coral-plugin DIR --model M --effort E \
        --workdir DIR --out DIR "<task prompt>"
    uv run run.py metrics <agent> <run dir>

Agents: claude-code, codex, opencode.

`run` writes into --out: run.jsonl (the agent's stdout), stderr.txt, and
meta.json (agent, model, effort, version, start time, wall time, exit code,
timeout). Every agent is wrapped in the same timeout, timed the same way, has
stdin closed, and is pinned to the given model and effort.

`metrics` reads a run directory and prints one JSON object: the standard
metrics, a tally of every tool the agent called, every exec and wait result
with its call number, state and failure code, and the agent's final message.
Reading the tracked metrics from that is the benchmark's own job.

The unit of progress is the tool call, numbered from 1 across the run. Neither
agent's transcript marks model rounds reliably: Claude Code splits one round
into several assistant messages and Codex marks none, so a count of calls is
the one unit both agents share.

With --coral-plugin, the arm's `plugins/coral` directory, an agent loads the
Coral plugin as a user who installed it would: Claude Code through
--plugin-dir, Codex through a scratch HOME whose `.agents/skills` holds only
the plugin's skills. Without it, the agent runs as a user who connected the
MCP server alone. OpenCode never loads it.

Agents keep their default tools. A user runs them that way, so a run that
answers through the shell or a web search instead of Coral is a finding about
the task prompt, not something to fence off here. The tool tally is how the
benchmark sees it.
"""
import argparse
import datetime as dt
from contextlib import ExitStack
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

AGENTS = ("claude-code", "codex", "opencode")
TIMEOUT_S = 15 * 60
MAX_TURNS = 25
FAILED_STATES = {"failed", "timed_out", "canceled", "interrupted", "tool_error"}


# ----------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------


def agent_version(binary: str) -> str | None:
    try:
        return subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return None


def claude_command(args: argparse.Namespace, out: Path) -> tuple[list[str], dict[str, str]]:
    """Only the MCP server, the Coral plugin and the model are pinned; tools
    stay at their defaults."""
    config = out / "mcp.json"
    config.write_text(json.dumps({"mcpServers": {"coral": {"type": "http", "url": args.mcp_url}}}) + "\n")
    command = [
        "claude", "-p", args.prompt,
        "--output-format", "stream-json", "--verbose",
        "--mcp-config", str(config), "--strict-mcp-config",
        "--setting-sources", "",
        *(["--plugin-dir", str(args.coral_plugin.resolve())] if args.coral_plugin else []),
        "--model", args.model, "--effort", args.effort,
        "--dangerously-skip-permissions",
        "--max-turns", str(args.max_turns),
    ]
    return command, {}


def codex_command(args: argparse.Namespace, out: Path, home: Path, user_home: Path) -> tuple[list[str], dict[str, str]]:
    """Codex runs from a scratch CODEX_HOME so the user's plugins and MCP servers
    do not load. Only auth.json is copied, into a temporary directory outside
    the run directory, and it is removed after the run. The account's hosted
    connectors and web search stay on, as they would for a user. Coral's exec
    tool carries destructive and open-world hints, so both gates are enabled
    for the server. Codex reads skills from `~/.agents/skills` whatever
    CODEX_HOME is, so HOME is a scratch directory too, holding only the Coral
    plugin's skills, or none without --coral-plugin."""
    shutil.copy(Path.home() / ".codex" / "auth.json", home / "auth.json")
    if args.coral_plugin:
        shutil.copytree(args.coral_plugin.resolve() / "skills", user_home / ".agents" / "skills")
    (home / "config.toml").write_text(f'model = "{args.model}"\nmodel_reasoning_effort = "{args.effort}"\n')
    command = [
        "codex", "exec", "--json", "--skip-git-repo-check", "--sandbox", "read-only",
        "-c", f'mcp_servers.coral.url="{args.mcp_url}"',
        "-c", "mcp_servers.coral.destructive_enabled=true",
        "-c", "mcp_servers.coral.open_world_enabled=true",
        "-c", 'mcp_servers.coral.default_tools_approval_mode="approve"',
        "-o", str(out / "last-message.txt"),
        args.prompt,
    ]
    return command, {"CODEX_HOME": str(home), "HOME": str(user_home)}


def opencode_command(args: argparse.Namespace, out: Path) -> tuple[list[str], dict[str, str]]:
    """Keep each run's MCP URL separate, even when workdirs are shared."""
    config = out / "opencode.json"
    config.write_text(json.dumps({
        "$schema": "https://opencode.ai/config.json",
        "mcp": {"coral": {"type": "remote", "url": args.mcp_url, "enabled": True}},
    }) + "\n")
    command = [
        "opencode", "run", "--format", "json", "--dangerously-skip-permissions",
        "--model", args.model, "--variant", args.effort,
        "--dir", str(args.workdir.resolve()), args.prompt,
    ]
    return command, {"OPENCODE_CONFIG": str(config)}


BINARIES = {"claude-code": "claude", "codex": "codex", "opencode": "opencode"}


def run(args: argparse.Namespace) -> int:
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    args.workdir.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        if args.agent == "codex":
            home = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="bench-codex-home-")))
            user_home = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="bench-home-")))
            command, extra_env = codex_command(args, out, home, user_home)
        elif args.agent == "opencode":
            command, extra_env = opencode_command(args, out)
        else:
            command, extra_env = claude_command(args, out)
        env = {**os.environ, **extra_env}
        started = dt.datetime.now(dt.timezone.utc)
        timed_out = False
        with open(out / "run.jsonl", "wb") as stdout, open(out / "stderr.txt", "wb") as stderr:
            clock = time.monotonic()
            process = subprocess.Popen(
                command, cwd=args.workdir, env=env, stdin=subprocess.DEVNULL,
                stdout=stdout, stderr=stderr, start_new_session=True,
            )
            try:
                exit_code = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, 9)
                exit_code = process.wait()
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, 9)
                    process.wait()
            wall_time_s = round(time.monotonic() - clock, 1)
    meta = {
        "agent": args.agent,
        "model": args.model,
        "effort": args.effort,
        "agent_version": agent_version(BINARIES[args.agent]),
        "started": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "wall_time_s": wall_time_s,
        "exit_code": exit_code,
        "timeout": timed_out,
        "coral_plugin": str(args.coral_plugin.resolve()) if args.coral_plugin and args.agent != "opencode" else None,
        "command": command,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=1) + "\n")
    print(json.dumps({k: meta[k] for k in ("agent", "wall_time_s", "exit_code", "timeout")}))
    return 0


# ----------------------------------------------------------------------------
# metrics
# ----------------------------------------------------------------------------


def run_update(value: object) -> tuple[str | None, str | None, str | None]:
    """(state, failure code, failure message) from a Coral exec or wait result.
    A tool-level error such as program_admission_failed reports state tool_error."""
    if not isinstance(value, dict):
        return None, None, None
    if "state" in value:
        failure = value.get("failure") or {}
        return value.get("state"), failure.get("code"), failure.get("message")
    if "code" in value:
        return "tool_error", value.get("code"), value.get("message")
    return None, None, None


def result_row(call: int, tool: str, arguments: object, value: object, text: str) -> dict:
    state, code, message = run_update(value)
    return {
        "call": call,
        "tool": tool,
        "arguments": json.dumps(arguments)[:400],
        "state": state,
        "code": code,
        "message": (message or "")[:200],
        "text": text,
    }


def read_claude(lines: list[dict]) -> tuple[dict, list[dict], dict[str, int], str | None]:
    standard: dict = {}
    results: list[dict] = []
    tool_calls: dict[str, int] = {}
    pending: dict[str, tuple[int, str, object]] = {}
    call = 0
    final = None
    for message in lines:
        kind = message.get("type")
        if kind == "assistant":
            for block in message["message"]["content"]:
                if block["type"] == "tool_use":
                    call += 1
                    pending[block["id"]] = (call, block["name"], block["input"])
                    tool_calls[block["name"]] = tool_calls.get(block["name"], 0) + 1
        elif kind == "user":
            for block in message["message"].get("content", []):
                if not (isinstance(block, dict) and block.get("type") == "tool_result"):
                    continue
                matched = pending.get(block.get("tool_use_id"))
                if not matched:
                    continue
                number, name, arguments = matched
                content = block.get("content")
                text = content if isinstance(content, str) else "".join(
                    part.get("text", "") for part in content if isinstance(part, dict))
                try:
                    value = json.loads(text)
                except ValueError:
                    value = None
                results.append(result_row(number, name.removeprefix("mcp__coral__"), arguments, value, text))
        elif kind == "result":
            usage = message.get("usage") or {}
            standard = {
                "input_tokens": sum(usage.get(k) or 0 for k in (
                    "input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")),
                "output_tokens": usage.get("output_tokens"),
                "cost_usd": message.get("total_cost_usd"),
                "tool_calls": call,
            }
            final = message.get("result")
    standard["tool_calls"] = call
    return standard, results, tool_calls, final


def read_codex(lines: list[dict]) -> tuple[dict, list[dict], dict[str, int], str | None]:
    """Codex names MCP tools `<server>.<tool>` on the item; shell commands,
    web searches and other tool kinds are tallied by their item type."""
    standard: dict = {}
    results: list[dict] = []
    tool_calls: dict[str, int] = {}
    call = 0
    calls = {}
    final = None
    for message in lines:
        if message.get("type") in ("item.started", "item.completed"):
            item = message["item"]
            if item.get("type") in ("mcp_tool_call", "command_execution", "web_search", "file_change"):
                if item["id"] not in calls:
                    call += 1
                    calls[item["id"]] = call
                    name = f'{item.get("server")}.{item.get("tool")}' if item.get("type") == "mcp_tool_call" else item["type"]
                    tool_calls[name] = tool_calls.get(name, 0) + 1
            if message["type"] == "item.started":
                continue
            if item.get("type") == "mcp_tool_call":
                value = (item.get("result") or {}).get("structured_content")
                if item.get("status") == "failed" and value is None:
                    value = {"code": "mcp_call_failed", "message": (item.get("error") or {}).get("message")}
                results.append(result_row(calls[item["id"]], item.get("tool"), item.get("arguments"), value, json.dumps(item.get("result"))))
            elif item.get("type") == "command_execution":
                results.append(result_row(calls[item["id"]], "shell", item.get("command"), None, item.get("aggregated_output") or ""))
            elif item.get("type") == "agent_message":
                final = item.get("text")
        elif message.get("type") == "turn.completed":
            usage = message.get("usage") or {}
            standard = {
                "input_tokens": usage.get("input_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "cost_usd": None,
                "tool_calls": call,
            }
    standard["tool_calls"] = call
    return standard, results, tool_calls, final


def read_opencode(lines: list[dict]) -> tuple[dict, list[dict], dict[str, int], str | None]:
    """OpenCode emits per-step usage and completed tool parts in JSON mode."""
    standard: dict = {}
    results: list[dict] = []
    tool_calls: dict[str, int] = {}
    final = None
    call = 0
    for message in lines:
        part = message.get("part") or {}
        if message.get("type") == "step_finish":
            tokens = part.get("tokens") or {}
            cache = tokens.get("cache") or {}
            for key, amount in (
                ("input_tokens", (tokens.get("input") or 0) + (cache.get("read") or 0) + (cache.get("write") or 0)),
                ("output_tokens", (tokens.get("output") or 0) + (tokens.get("reasoning") or 0)),
                ("cost_usd", part.get("cost")),
            ):
                if amount is not None:
                    standard[key] = standard.get(key, 0) + amount
        elif message.get("type") == "tool_use":
            call += 1
            name = part["tool"]
            tool_calls[name] = tool_calls.get(name, 0) + 1
            state = part.get("state") or {}
            text = state.get("output") or ""
            try:
                value = json.loads(text)
            except ValueError:
                value = None
            if state.get("status") == "error":
                value = {"code": "mcp_call_failed", "message": state.get("error")}
            tool = "shell" if name == "bash" else name.removeprefix("coral_")
            results.append(result_row(call, tool, state.get("input"), value, text))
        elif message.get("type") == "text":
            final = part.get("text")
    standard["tool_calls"] = call
    return standard, results, tool_calls, final


READERS = {"claude-code": read_claude, "codex": read_codex, "opencode": read_opencode}


def read_transcript(path: Path) -> list[dict]:
    lines = path.read_text(errors="replace").splitlines(keepends=True)
    messages = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            # A killed process can leave its final event only partly written.
            if index != len(lines) - 1 or line.endswith("\n"):
                raise
    return messages


def metrics(args: argparse.Namespace) -> int:
    run_dir: Path = args.run_dir
    meta = json.loads((run_dir / "meta.json").read_text())
    lines = read_transcript(run_dir / "run.jsonl")
    standard, results, tool_calls, final = READERS[args.agent](lines)
    standard["wall_time_s"] = meta["wall_time_s"]
    exec_tool = {"claude-code": "mcp__coral__exec", "codex": "coral.exec", "opencode": "coral_exec"}[args.agent]
    standard["exec_calls"] = tool_calls.get(exec_tool, 0)
    standard["failed_results"] = sum(1 for r in results if r["tool"] in ("exec", "wait") and r["state"] in FAILED_STATES)
    if args.agent == "opencode" and meta["timeout"]:
        # OpenCode does not emit pending tool calls in its JSON stream.
        standard["tool_calls"] = None
        standard["exec_calls"] = None
    for key in ("input_tokens", "output_tokens", "cost_usd", "tool_calls"):
        standard.setdefault(key, None)
    report = {
        "agent": args.agent,
        "model": meta["model"],
        "effort": meta["effort"],
        "agent_version": meta["agent_version"],
        "timeout": meta["timeout"],
        "standard": standard,
        "tool_calls": tool_calls,
        "results": [{k: v for k, v in r.items() if k != "text"} for r in results if r["tool"] in ("exec", "wait", "shell")],
        "final_message": final,
    }
    print(json.dumps(report, indent=1))
    return 0


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="run one repeat")
    run_parser.add_argument("agent", choices=AGENTS)
    run_parser.add_argument("prompt")
    run_parser.add_argument("--mcp-url", required=True)
    run_parser.add_argument("--coral-plugin", type=Path,
                            help="the arm's plugins/coral directory; Claude Code and Codex load it. Omit to run without the Coral skill")
    run_parser.add_argument("--model", required=True)
    run_parser.add_argument("--effort", required=True)
    run_parser.add_argument("--workdir", required=True, type=Path, help="empty directory the agent runs in")
    run_parser.add_argument("--out", required=True, type=Path, help="directory for this run's files")
    run_parser.add_argument("--timeout", type=int, default=TIMEOUT_S, help="seconds")
    run_parser.add_argument("--max-turns", type=int, default=MAX_TURNS, help="Claude Code only")
    run_parser.set_defaults(func=run)

    metrics_parser = commands.add_parser("metrics", help="print standard metrics for one run")
    metrics_parser.add_argument("agent", choices=AGENTS)
    metrics_parser.add_argument("run_dir", type=Path)
    metrics_parser.set_defaults(func=metrics)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
