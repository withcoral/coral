![Coral, with engraved orange corals](./apps/docs/images/cover.webp)

# Coral

**Execution rails for agents.**

Coral is an open-source, self-hosted gateway that sits between your agents and
the systems they work on. Agents call tools by writing short TypeScript
programs, and save the ones that work as custom tools to reuse. Coral checks
every call against your policy before it adds credentials and forwards the
call.

[![CI](https://github.com/withcoral/coral/actions/workflows/ci.yml/badge.svg)](https://github.com/withcoral/coral/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/withcoral/coral)](./LICENSE)
[![Docs](https://img.shields.io/badge/docs-withcoral.com-171717)](https://withcoral.com/docs)
[![Slack](https://img.shields.io/badge/chat-Slack-4A154B?logo=slack&logoColor=white)](https://withcoral.com/slack)

[Quickstart](https://withcoral.com/docs/getting-started/quickstart) ·
[Download](https://withcoral.com/download) ·
[Demo video](https://www.youtube.com/watch?v=p0PKOlHLyVI)

In this example, an agent posts unreviewed PRs to Slack for the first time. The
agent searches for the GitHub and Slack tools and writes code that calls them.
It isn't allowed to post to Slack without approval, so the user approves the
post. The agent then saves its code as a custom tool. The next day, the agent
runs the same task by calling that custom tool.

![In Claude Code, an agent posts unreviewed pull requests to Slack. It searches Coral for the GitHub and Slack tools, writes code that calls them, and waits for approval before posting. The Coral UI then shows the run, who approved it, and the custom tool the agent saved. The next day, the same request runs by calling that custom tool. A final card compares the two days: 35% fewer tokens and 31% faster, in line with our benchmarks.](./apps/docs/images/demo.gif)

**Control what agents can do, not just what they can reach.** Most MCP
gateways decide only which tools an agent can see. That is all or nothing: if
an agent can call `update_record`, it can update any record in any way the
tool allows. A custom tool is code, so it can hold the rule that Allow and
Block cannot: `closeStaleTicket` changes one field, only on tickets with no
activity for 30 days. You set Allow, Ask, or Block on the custom tool like any
other tool, and Coral still checks every provider call inside it against your
policy.

**Less duplicated agent work.** Agents start every task from scratch. They
re-read tool schemas, retry failed calls, and spend tokens rewriting code for
steps they got right yesterday. With Coral, agents save the code that worked
as custom tools they call again in one step, and workspace owners can share
them with the whole team.

Coral is used by thousands of developers at more than 450 companies for
safer, more efficient agent work in customer support, SecOps, business
intelligence, SRE, and more.

## Get started

1. **Install Coral.**
    - **For yourself:** download the desktop app from the
      [download page](https://withcoral.com/download). It runs on macOS
      (Apple silicon), Windows (x64), and Linux (x86_64).
    - **For your team:** [run a shared server](https://withcoral.com/docs/getting-started/self-host)
      with Docker Compose or Kubernetes. You need an OIDC identity provider, a
      reverse proxy for TLS, and SQLite or Postgres.
2. **Add a provider.** A provider is one service your agents can use, such as
   GitHub or Linear. Pick one from the catalog, or bring your own OpenAPI
   document or MCP server. By default, tools that read data run right away and
   everything else needs your approval. See
   [Add a provider](https://withcoral.com/docs/getting-started/quickstart#add-a-provider).
3. **Connect your agent.** Copy your workspace's MCP URL from the Coral UI and
   add it to your MCP client:

    ```bash
    claude mcp add --scope user --transport http coral <your-mcp-url>   # Claude Code
    codex mcp add coral --url <your-mcp-url>                            # Codex
    ```

    For Cursor, VS Code, OpenCode, and other clients, see
    [Connect an MCP client](https://withcoral.com/docs/getting-started/quickstart#connect-an-mcp-client).

4. **Ask a question.** Ask your agent something that uses that provider, such
   as "What are my open pull requests?"

## Why Coral

We started Coral while building an AI SRE. To investigate an incident, the
agent had to connect the dots across an ocean of telemetry and undocumented
relationships between internal and external systems. It needed broad
permissions and a lot of tokens, and it started from scratch the next time.

We built Coral so that the agent remembers the route. Once a task succeeds,
the agent saves the steps that worked as a custom tool. Next time, it finds
that tool in the same search as provider tools such as Datadog and Slack. It
started with SRE, and teams now use Coral for agent work across the
organization:

- **SRE.** The agent investigates alerts. Custom tools let it roll back only
  the services you name.
- **Customer support.** The agent issues refunds, and escalates to a person in
  the cases you define.
- **Business intelligence.** Custom tools aggregate warehouse, CRM, and billing
  data, and pass only the metrics to the agent.
- **SecOps.** The agent uses custom tools to isolate devices within approved
  groups.

What you get:

- **Precise control.** Set Allow, Ask, or Block for each provider tool and each
  custom tool. An approval shows the exact call and its arguments, so Security
  approves one refund lookup, not the whole payments API. See
  [Roles & Permissions](https://withcoral.com/docs/guides/permissions).
- **Shared tools, not shared tokens.** You can set each provider to use the
  caller's own account, so teammates share tools without sharing API tokens.
  Read how [Slackbot uses this](https://withcoral.com/blog/slackbot-works-better-with-coral).
- **Cheaper models, same accuracy.** With custom tools, a smaller model
  performs like a bigger one. For many tasks, this means Opus 5.5 accuracy at
  Haiku 5.5 cost.
- **Faster, leaner agents.** Across 34 tasks on GitHub, Linear, and Slack with
  Opus 5.5, custom tools cut tokens by 37%, cost by 31%, and time by 29%. See
  [every task](https://withcoral.com/docs/examples/custom-tools).
- **Ahead of other MCP gateways.** In blind benchmarks of 100 enterprise
  tasks, agents were more accurate and cheaper with Coral than with gateways
  from Kong, Composio, and Microsoft. See the
  [benchmarks](https://withcoral.com/#benchmarks).
- **Training data you own.** Each run leaves a structured trace of the goal,
  the tools used, and the outcome that works well for post-training your own
  model.

From one developer's laptop to an enterprise cloud, Coral fits your stack:

- **Runs on your infrastructure.** Coral is written in Rust and runs as a
  desktop app on your laptop or a shared server in your own cloud, with zero
  data egress.
- **Open source.** Coral is licensed under the Apache License 2.0, with no
  limits on users, agents, or volume. Everything in this README outside
  [Coral Enterprise](#for-teams-and-enterprise) is in the open-source edition.
- **Any agent, any system.** It works with any AI app or agent that supports
  MCP. You can connect anything with an MCP server or OpenAPI document,
  including more than 5,000 databases and SaaS tools.

## How Coral works

**Agents write programs.** Coral builds on code mode, and adds custom tools,
policy controls, and managed identities. To use tools, an agent writes a short
TypeScript program. One program can call several tools, filter the results,
and return only what the task needs. Less raw data passes through the model's
context.

**Coral runs them on rails.** Each program runs in a fresh V8 isolate with no
network access, no credentials, and no imports. Coral checks every tool call
against your workspace policy, then adds the credentials and forwards the call
to the provider. A policy can also require a person to approve the call. Every
run is attributed to the person or agent that started it.

```mermaid
graph LR
    Agent["Your agent"] -->|"exec: run a program"| MCP["Coral MCP server"]
    You["You"] -->|"providers, policy, approvals"| UI["Coral UI"]

    subgraph Coral["Coral (your laptop or server)"]
        MCP --> Run["Program runs in a V8 isolate"]
        Run <--> FN["Custom tools (private and shared)"]
        UI --> Gate["Policy check and credentials"]
        Run --> Gate
        Gate -.->|"approval request"| UI
    end

    Gate --> GH["GitHub"]
    Gate --> SL["Slack"]
    Gate --> Own["Your OpenAPI or MCP service"]
```

The agent sends Coral programs through the `exec` tool. A program calls
`coral.search` to find the tools the agent can use and read their
TypeScript signatures. Then the agent sends a program that does the work.
This one finds open pull requests that have no reviewer and posts them to
Slack:

```ts
const { data: pulls } = await coral.providers.github.pulls.list({
    owner: "withcoral",
    repo: "coral",
    state: "open",
});

const unreviewed = pulls.filter((pull) => pull.requested_reviewers.length === 0);

await coral.providers.slack.chat.postMessage({
    channel: "#engineering",
    text: unreviewed.map((pull) => `#${pull.number} ${pull.title}`).join("\n"),
});

return unreviewed.map((pull) => pull.number);
```

If posting to Slack needs approval, the run pauses there until you approve it.
Only the pull request numbers go back to the agent, not the full GitHub
response. The run happens on the Coral server, not in your agent, so it
carries on even if the agent disconnects.

**What works becomes a custom tool.** When a program succeeds, the agent saves
it as a private tool and calls it next time in one step as
`coral.private.<name>({ ... })`. Agents find custom tools in the same search
as provider tools, and reuse or edit them instead of starting over. A
workspace owner can publish a private tool as a shared tool under
`coral.shared`, so everyone in the workspace can call it.

To learn about providers, accounts, policy, and workspaces, read
[Concepts](https://withcoral.com/docs/guides/concepts).

## For teams and enterprise

Running Coral for a team? See
[Run a shared server](https://withcoral.com/docs/getting-started/self-host).

Coral Enterprise is coming soon under its own license. It adds
organization-wide governance and support: SSO with SAML, SCIM provisioning,
advanced RBAC, an immutable audit log, and support SLAs. See
[Coral Enterprise](https://withcoral.com/pricing) or
[book a demo](https://withcoral.com/contact).

## Development

To run Coral from a checkout, follow the
[development guide](./docs/development.md). To contribute, read
[`CONTRIBUTING.md`](./CONTRIBUTING.md). The
[release notes](https://github.com/withcoral/coral/releases) list what changed
in each version.

## Community

Ask questions, share ideas, and show what you built in our
[Slack community](https://withcoral.com/slack) or on
[GitHub issues](https://github.com/withcoral/coral/issues).

## Security

Do not report security issues in public issues or pull requests. See
[`SECURITY.md`](./SECURITY.md).

Coral is ISO/IEC 27001:2022 certified. Each program runs in its own V8
isolate, and Coral logs every approval.

## License

Coral is licensed under the Apache License 2.0. See [`LICENSE`](./LICENSE).

The [`ee/`](./ee) directory holds Enterprise Edition code under its own license,
[`ee/LICENSE`](./ee/LICENSE). A plain `cargo build` at the repository root
builds only code under the Apache License 2.0. [`NOTICE`](./NOTICE) sets out
which license covers which files.

## Upgrade from Coral 0.x

Coral v1 replaces the SQL interface with TypeScript programs. It starts from
fresh state and does not convert what 0.x stored. If Coral finds 0.x state, it
tells you at startup. See
[Upgrade from Coral 0.x](https://withcoral.com/docs/getting-started/self-host#upgrade-from-coral-0-x).
