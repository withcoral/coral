---
name: coral
description: Use Coral's local-first SQL runtime to discover schemas, validate sources, and query installed data sources from the CLI or through MCP.
---

# Coral

Use this skill when you need to work with Coral data sources, inspect schemas, or answer questions with Coral SQL.

## When to use it

- You need to discover what sources are installed in Coral.
- You need to inspect available schemas or tables before writing a query.
- You need to run `coral sql` against one or more installed sources.
- You need to validate a source or troubleshoot source setup.
- You need to set up Coral for first use with `coral onboard`.

## Workflow

1. Confirm Coral is installed:
   - `coral --help`
2. If no sources are installed or the user is just getting started:
   - `coral onboard`
3. Inspect installed sources:
   - `coral source list`
4. Discover available schemas and tables:
   - `coral sql "SELECT schema_name, table_name FROM coral.tables ORDER BY 1, 2"`
5. Validate a specific source when setup looks wrong:
   - `coral source test <NAME>`
6. Run the actual query:
   - `coral sql "<SQL>"`

## Source setup

- Add a bundled source:
  - `coral source add <NAME>`
- Import a custom source manifest:
  - `coral source import /path/to/source.yaml`
- Validate the result:
  - `coral source test <NAME>`

Coral prompts interactively for required variables and secrets during add/import.

## Agent usage

If the user wants an MCP-capable client to call Coral directly, use:

- `coral mcp-stdio`

Then configure the client to launch that command.

## Querying guidance

- Start with `coral.tables` and `coral.columns` rather than guessing schema names.
- Keep the first query small and inspectable.
- If a query fails and setup may be the cause, validate the source before changing SQL.
- When in doubt, prefer showing the user the exact `coral sql` command you used.
