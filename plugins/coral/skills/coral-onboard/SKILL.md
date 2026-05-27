---
name: coral-onboard
description: "Install Coral, register it as an MCP server, configure a first source, and run a first query. Use when Coral is not yet running on this machine, or is running but not connected to this agent."
---

# Coral Onboard

## Overview

Use this skill exactly once per machine to take a user from no Coral to a returned SQL result. After onboarding, hand off to the `coral` skill for ongoing queries.

- This is the only Coral skill that uses the `coral` CLI as the primary surface.
- Stop after the first query returns rows. Do not extend into analytical queries or spec authoring.
- Do not run this skill on a machine where Coral is already installed, registered, and queryable.

## Support Checks

- Run before suggesting any step: `command -v coral`, `claude mcp list | grep -i coral`, `coral source list`.
- Resume the workflow from the first step that is not already satisfied; skip the steps that are.
- If the agent is not Claude Code, substitute the matching MCP config path (`.cursor/mcp.json`, `.vscode/mcp.json`, `claude_desktop_config.json`).

## Workflow

1. Detect state with the three commands in *Support Checks*.
2. Install Coral if the binary is missing.
   - macOS: `brew install withcoral/tap/coral`.
   - macOS / Linux: `curl -fsSL https://withcoral.com/install.sh | sh`.
   - Windows: download the latest release zip and place `coral.exe` on PATH.
   - Verify with `coral --version`.
3. Register the MCP server if the agent does not see it.
   - Claude Code: `claude mcp add --scope user coral -- coral mcp-stdio`.
   - Other agents: write the per-agent MCP config file pointing at `coral mcp-stdio`.
   - Tell the user to restart the agent. The MCP tools are not visible until restart.
4. Add a first source if `coral source list` is empty. Prefer the lowest-friction option that returns useful rows.
   - Zero-config: `coral source add claude` (queries `~/.claude/` transcripts).
   - Otherwise: whichever bundled source the user already has credentials for, via `coral source add --interactive <name>`.
   - Confirm with `coral source list` and `coral source test <name>`.
5. Run one verification query. Prefer the MCP `sql` tool once the agent has reconnected; otherwise `coral sql` directly. Use the smallest meaningful query for the chosen source.
6. Hand off. Report the source name, the row count, and one example row. Direct the user to the `coral` skill for further queries.

## Onboarding Rules

- Default to the system package manager (Homebrew on macOS, `install.sh` elsewhere). Do not build from source.
- After MCP registration, prompt for restart; do not assume auto-reload.
- Never store credentials in shell history or commits. Prefer `--interactive` over inline env vars when the secret is sensitive.
- If `claude mcp list` reports Coral as `Failed to connect`, re-add with the absolute binary path: `claude mcp add --scope user coral -- $(which coral) mcp-stdio`.
- If OAuth setup hangs, offer the paste-a-token path instead.
- One verification query is enough. Stop there.

## Boundaries

- Do not author custom source specs; defer to `coral-create-source-spec`.
- Do not review source-spec PRs; defer to `coral-review-source-spec`.
- Do not run multi-source queries, JOINs, or analytical workloads; defer to `coral`.
- Do not pre-install sources beyond the first; let the user grow their configuration over time.
