# Coral Setup Plan

## Goal

Improve Coral's setup story using the product model that already exists in this repo:

1. install the CLI
2. configure at least one source locally
3. optionally connect an agent over MCP

This plan covers:

- `install.sh`
- `coral onboard`
- docs install and agent-usage flows

## What The Repo Already Says

The current repo is already internally consistent:

- installation is documented in `docs/getting-started/installation.mdx`
- first usage is documented in `docs/getting-started/quickstart.mdx`
- agent integration is documented in `docs/guides/use-coral-over-mcp.mdx`
- the CLI only exposes three top-level commands today: `sql`, `source`, and `mcp-stdio`

The code and docs do **not** currently imply a broad environment-setup product surface. They imply a local-first CLI with source management plus an MCP export.

That means the best plan is evolutionary, not expansive.

## Product Direction

Coral should sharpen the existing user journey instead of adding multiple new conceptual layers all at once.

The recommended user journey is:

1. install Coral
2. run `coral onboard`
3. query local data
4. if needed, connect an agent to `coral mcp-stdio`

This suggests:

- `install.sh` should stay narrow
- `coral onboard` should become the main guided source-setup flow
- docs should emphasize the install -> onboard -> agent usage progression

## Non-Goals

- Do not introduce a large setup subsystem before the product actually needs one.
- Do not mix shell configuration, source onboarding, and MCP usage into one command.
- Do not add external-config mutation flows unless there is a specific supported integration to automate.
- Do not replace the existing low-level `coral source ...` commands; `coral onboard` should sit above them.

## Workstream 1: `install.sh`

### Role

`install.sh` should only solve "get the Coral binary working locally."

### Responsibilities

- detect platform and architecture
- download the correct release artifact
- verify checksums
- place the binary in the install location
- print verification guidance
- print the next product step

### Install channels

Coral should continue to support at least two install channels:

- `brew install withcoral/tap/coral`
- `curl -fsSL https://withcoral.com/install.sh | sh`

Recommended positioning:

- Homebrew is the preferred managed install path
- `install.sh` is the alternative bootstrap path

This keeps package-managed lifecycle concerns with Homebrew while still supporting direct installation where Homebrew is unavailable or undesirable.

### Explicitly out of scope

- source onboarding
- MCP client configuration
- shell completion mutation
- agent skill installation

### Desired finish state

The installer should end with a minimal message like:

```text
Installed Coral.
Verify:
  coral --help
Next:
  coral onboard
Optional agent usage:
  withcoral.com/docs/guides/agent-usage
```

### Upgrade story

The install-channel behavior should be explicit in both docs and user messaging:

- Homebrew upgrades via `brew upgrade withcoral/tap/coral`
- `install.sh` upgrades by re-running the installer

Coral does not need to invent an update mechanism inside onboarding to compensate for the direct-install path.

Until Coral ships a dedicated self-update feature, the direct-install route should be documented as "reinstall to upgrade."

### Repo-specific rationale

The installation doc already frames Coral as a single local CLI in `docs/getting-started/installation.mdx`. The installer should reinforce that framing, not invent a second setup experience.

## Workstream 2: `coral onboard`

### Role

`coral onboard` should become the missing guided layer between installation and the existing low-level source commands.

### Why this is the best addition

The current docs ask users to manually do:

- `coral source discover`
- `coral source add <NAME>`
- `coral source test <NAME>`

That is correct, but it is procedural rather than productized. The repo already has all the underlying primitives in `coral-cli`, `coral-client`, `coral-app`, and `coral-spec`. A guided wrapper is the cleanest improvement.

### First iteration scope

- add top-level `coral onboard`
- require interactive terminal behavior
- detect whether any sources are already installed
- offer a simple guided flow:
  - discover bundled sources
  - select a bundled source to add
  - or import a custom source manifest
  - validate an installed source
- end with next steps:
  - `coral source list`
  - `coral sql "SELECT * FROM coral.tables ..."`
  - optional agent-usage docs

### Design constraints

- keep `coral-cli` as the terminal adapter
- reuse existing app RPCs rather than inventing new onboarding-only ones unless clearly needed
- keep source lifecycle behavior in `coral-app`
- keep input discovery in `coral-spec`
- make `coral onboard` a wrapper over existing primitives, not a second implementation of them

## Workstream 3: Documentation

### Current docs shape

The current docs navigation already has the right broad sections:

- install: `getting-started/installation`
- usage: `getting-started/quickstart`
- agent integration: `guides/use-coral-over-mcp`

The better plan is to tighten those pages around `coral onboard`, not to rebuild the IA from scratch.

### Install docs

Keep the existing install page at:

- `docs/getting-started/installation.mdx`

Update it to:

- verify install with `coral --help`
- make `coral onboard` the explicit next step
- position Homebrew as the preferred install path and `install.sh` as the manual alternative
- document the upgrade difference between Homebrew and `install.sh`
- move low-level local-state detail below the main path
- link clearly to agent usage as optional follow-on setup

### Agent usage docs

Add or rename toward:

- `docs/guides/agent-usage.mdx`

Recommended approach:

- keep `docs/guides/use-coral-over-mcp.mdx` as the source material
- either rename it to `agent-usage.mdx`
- or create `agent-usage.mdx` as the broader page and keep `use-coral-over-mcp.mdx` as an MCP-specific deep dive

### Recommended content model for agent usage

The agent-usage page should explain:

- prerequisites:
  - Coral installed
  - at least one source configured
- how agents connect:
  - `coral mcp-stdio`
  - client-specific MCP config examples
  - `npx skills add ...` for agent skill distribution if Coral publishes a skills endpoint/package
- how to verify:
  - ask for tables
  - run a simple SQL query
- how to troubleshoot:
  - `coral` not on `PATH`
  - no sources installed
  - source validation failures

### Quickstart docs

Update `docs/getting-started/quickstart.mdx` so that:

- the preferred path is `coral onboard`
- the manual `source discover` / `source add` / `source test` sequence remains documented as the transparent low-level flow

### README updates

Align the README with the same path:

1. install
2. `coral onboard`
3. query
4. optional agent integration via MCP and skills

## Proposed Delivery Sequence

### Phase 1: docs-first user journey cleanup

- update installation doc to point to `coral onboard`
- update quickstart framing around `coral onboard`
- add or rename the agent-usage guide
- update README to match

### Phase 2: CLI onboarding

- add `coral onboard`
- add tests for interactive and non-interactive behavior

### Phase 3: agent usage polish

- add `npx skills add ...` guidance once Coral has a stable skills distribution target
- keep agent integration primarily in docs unless a concrete automation surface is clearly warranted

## Verification Plan

- unit tests for onboard control-flow helpers
- integration test for non-interactive suppression
- docs navigation review for page naming and links
- run `make validate`

## Open Questions

- Should the docs land before the command, or should `coral onboard` land first to avoid documenting a not-yet-shipped command?
- Should `agent-usage` replace `use-coral-over-mcp`, or sit above it as the broader page?
- What is the canonical Coral target for `npx skills add ...` and how should that artifact be versioned and distributed?

## Recommendation

The better repo-native plan is:

1. keep `install.sh` narrow
2. make `coral onboard` the main new product addition
3. evolve the docs from install -> quickstart -> MCP into install -> onboard -> agent usage
4. add `npx skills add ...` to the agent-usage story once Coral has a stable skills distribution path

That plan fits the current code, current docs navigation, and current product boundaries without introducing extra surface area too early.
