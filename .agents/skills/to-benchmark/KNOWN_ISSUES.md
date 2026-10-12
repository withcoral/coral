# Known issues

Problems met while running benchmarks, with the workaround in use. Each entry names the agent and version it was seen on and the date. Read this before Setup. At the smoke run, check every entry that applies: delete the ones that no longer reproduce, and add any new problem here with its workaround.

## Claude Code 2.1.267, 2026-09-10

- `--setting-sources ""` changes the default model. `run.py` pins it.
- ToolSearch surfaces deferred built-ins such as `Monitor`, which runs a shell command, so `gh api` is one search away; the model took it in 5 of 10 runs of a GitHub question. Not a harness problem: the tool tally from `run.py metrics` shows it and the Run procedure reports it as a prompt finding.

## Codex 0.153.4, 2026-09-10

- `plugins.<name>.enabled=false` on the command line does not disable a plugin. `run.py` uses a scratch `CODEX_HOME` instead.
- MCP tools with destructive or open-world hints are refused under approval-never unless the server's `destructive_enabled` and `open_world_enabled` gates are set. `run.py` sets them.
- The account's hosted connectors (a `codex_apps` server with `github.fetch`) and web search stay on under a scratch `CODEX_HOME` and answered a GitHub question without Coral. Not a harness problem: the tool tally shows it and the Run procedure reports it as a prompt finding.

## Codex 0.160.0, 2026-10-05

- Codex loads skills from `~/.agents/skills` even under a scratch `CODEX_HOME`, so the user's personal skills reached every run. `run.py` sets `HOME` to a scratch directory holding only the arm's Coral skill.
- `gpt-6.1-sol` requires Codex CLI 0.160.0 or later with a ChatGPT account. Older clients (0.154.0) fail the first turn with "The 'gpt-6.1-sol' model is not supported when using Codex with a ChatGPT account". Upgrade the CLI before Setup.
- This version ignores `mcp_servers.coral.destructive_enabled` and `open_world_enabled` and logs an "unrecognized configuration settings" error item. Coral's `exec` ran without them. `run.py` still passes them; the warning is harmless.

## Claude Code 2.1.289, 2026-10-05

- Bash is not deferred, so `gh` is one call away: the model answered GitHub requests with `gh` without loading Coral's tools. Not a harness problem: the tool tally shows it and the Run procedure reports it as a prompt finding.

## Claude Code 2.1.294, 2026-10-08

- A server that once failed to connect is recorded by name in `~/.claude/mcp-needs-auth-cache.json`. Every later run sees the benchmark's `coral` server as `needs-auth`, even when the arm serves it, and answers in seconds without a tool call. The init message in `run.jsonl` shows the status. Remove the `coral` key from that file and repeat the run.

## Coral main at b5223a33, 2026-10-08

- Since #643, Workspace authority comes only from a Workspace Membership. An arm configured without `[auth]` serves MCP as the local principal `coral:local`, which has no Membership in the live Workspace, so every MCP request returns 404. Agents then report Coral as unavailable and answer another way. Insert a Membership for `coral:local` into each copied `coral.db`; see `bench-exec-result-size` Setup step 5.

## OpenCode 1.15.6, 2026-09-11

- Use `--dangerously-skip-permissions` and `--variant` for permission approval and reasoning effort. This version rejects `--auto`.
- The local database can fail with `NOT NULL constraint failed: session_message.seq` before a tool call. For verification, use a temporary `XDG_DATA_HOME` with a filesystem copy of `opencode/auth.json`. Remove that directory after the run. Keep the original database unchanged.
- JSON output reports tool calls only when they complete or fail. A killed run can omit pending calls. Treat its tool and exec counts as unavailable, rather than zero or complete totals.
