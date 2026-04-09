# Coral

One SQL interface over APIs, files, and live sources — built for agents.

Coral gives agents a local-first SQL runtime over APIs, files, and other live
sources. Query it from the CLI, inspect schemas and tables, or expose the same
runtime over MCP so agents can use it without bespoke tool glue.

> Coral is local-first today: add sources, query them through one runtime, and
> expose that runtime to agents over MCP. We’re actively expanding the product
> surface, including additional transports, deployment options, and broader
> source support.

## Why Coral

Most agent workflows access company data one tool at a time. That works, but it
tends to create:

- too many tool calls
- repeated auth, pagination, and retry logic
- poor cross-source reasoning
- high token traffic
- brittle glue code and prompts

Coral gives agents one query interface instead:

- query multiple live sources through SQL
- keep workflows inspectable and scriptable
- expose the same runtime over MCP
- answer cross-source questions without stitching tools together by hand

## What Coral does today

- onboard a local workspace with bundled or imported sources
- discover bundled sources
- add or import sources into a local workspace
- inspect schemas and tables through SQL
- run SQL queries from the CLI
- launch a local MCP stdio server for agent workflows

## Quickstart

### 1. Install Coral

```bash
brew install withcoral/tap/coral
coral --help
```

### 2. Run onboarding

```bash
coral onboard
```

`coral onboard` guides you through adding or importing a source and validating
it before you start querying.

If you prefer the low-level manual flow, you can still run:

```bash
coral source discover
coral source add github
coral source test github
```

### 3. Inspect available tables

Use `coral.tables` to see what Coral can query:

```bash
coral sql "SELECT * FROM coral.tables LIMIT 20"
```

### 4. Run a query

For example, to inspect recent GitHub releases:

```bash
coral sql "
  SELECT name, draft, prerelease, published_at
  FROM github.releases
  WHERE owner = 'withcoral' AND repo = 'coral'
  ORDER BY published_at DESC
  LIMIT 10
"
```

The exact schemas and tables depend on the sources you have installed. When in
doubt, inspect `coral.tables` first.

### 5. Use Coral with an agent

Coral can run as a local MCP server so agents can query your installed sources
through the same runtime.

#### Claude Code

```bash
claude mcp add coral -- coral mcp-stdio
```

#### Codex

```bash
codex mcp add coral -- coral mcp-stdio
```

#### OpenCode

Add a new MCP app configured to launch Coral with:

```bash
coral mcp-stdio
```

#### Claude Desktop

Open:

`Settings -> Developer -> Edit Config`

Then add Coral to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "coral": {
      "command": "/path/to/coral",
      "args": ["mcp-stdio"]
    }
  }
}
```

Use the full path to your `coral` binary. Once configured, your agent can use
Coral over MCP to inspect schemas, list tables, and query the sources installed
in your local workspace.

Coral also ships a reusable skill for agent workflows:

```bash
npx skills add withcoral/skills
```

For the full agent setup flow, including MCP examples and skills guidance, see
[Agent usage](https://withcoral.com/docs/guides/agent-usage).

## Core concepts

### Source

A source is an external system or local dataset that Coral can query, such as
GitHub, Slack, Stripe, local JSONL files, or Parquet data.

### Table

Coral exposes source data as SQL tables under source-owned schemas.

### Runtime

The CLI and MCP server use the same underlying runtime.

### Source spec

A source spec is the configuration Coral validates and installs for one source.
Coral ships bundled source specs and can also import custom ones.

## Local state

Coral stores local state in its platform-specific configuration directory.

You can override the config directory with:

```bash
export CORAL_CONFIG_DIR=/path/to/coral-config
```

Important files include:

- `config.toml` for installed-source metadata and non-secret variables
- installed source specs under `workspaces/<workspace>/sources/...`
- source secrets stored separately within the same local trust boundary

## Current focus

Coral is already usable for local agent workflows. We’re currently expanding:

- broader source coverage
- additional transports and network support
- richer deployment options
- smoother setup and ergonomics
- stronger source authoring and packaging

## Development

Run the workspace validation gate from the repository root:

```bash
make rust-checks
```

## Documentation

For setup guides, reference docs, and examples, visit
[withcoral.com/docs](https://withcoral.com/docs).

## Contributing

Contributions are welcome, especially bug fixes, tests, documentation
improvements, source improvements, and user-facing usability improvements.

Please read [`CONTRIBUTING.md`](./CONTRIBUTING.md) before opening a pull
request.

## Security

Please do not report security issues in public issues or pull requests. See
[`SECURITY.md`](./SECURITY.md).

## Licence

Coral is licensed under the Apache License 2.0. See [`LICENSE`](./LICENSE).
