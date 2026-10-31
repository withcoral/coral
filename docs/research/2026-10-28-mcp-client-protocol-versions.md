# MCP client harnesses: protocol version and elicitation support

Date: 2026-10-28

Question: which major MCP client harnesses negotiate protocol revision
`2026-07-28` (sessionless, per-request `_meta`, MRTR `input_required`) versus
`2025-11-25` (initialize handshake, `Mcp-Session-Id`, in-band
`elicitation/create`), and in which elicitation modes does each answer?

All GitHub sources were read from the `main` branch on 2026-10-28 unless a tag
or commit is named. All web pages were accessed 2026-10-28.

## The 2026-07-28 revision in one paragraph

The MCP blog post is dated "July 28, 2026" and says the release "officially
retired the `initialize`/`initialized` exchange along with the `Mcp-Session-Id`
header" and that "All four Tier 1 SDKs speak `2026-07-28` as of today", with
Rust in beta ([blog](https://blog.modelcontextprotocol.io/posts/2026-07-28/)).
The [changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)
attributes the pieces to three SEPs:

- **SEP-2567** (merged 2026-05-07, author pja-ant, milestone 2026-07-28-RC,
  [PR #2567](https://github.com/modelcontextprotocol/modelcontextprotocol/pull/2567)):
  "Remove protocol-level sessions and the `Mcp-Session-Id` header"; list
  endpoints "no longer vary per-connection"; cross-call state uses
  "explicit, server-minted handles". The SEP text says sessions are removed
  "with no deprecation window".
- **SEP-2575**: removes `initialize`/`notifications/initialized`; "Every request
  now carries its protocol version and client capabilities in `_meta`"
  (`io.modelcontextprotocol/protocolVersion`,
  `io.modelcontextprotocol/clientCapabilities`); adds mandatory
  `server/discover`; replaces the GET stream with `subscriptions/listen`;
  removes SSE resumability (`Last-Event-ID`).
- **SEP-2322** (MRTR): "replaces the previous approach of sending
  server-initiated requests" such as `elicitation/create`. Servers return
  `resultType: "input_required"` with `inputRequests`; clients retry the
  original request with `inputResponses`. Every result carries `resultType`;
  clients must treat a missing field from earlier-protocol servers as
  `"complete"`. Minor change 11 removes `notifications/elicitation/complete`
  and the URL-mode `elicitationId`; correlation moves to `requestState`.

Form and URL elicitation modes both survive in 2026-07-28; only the carrier
changed. The [elicitation page](https://modelcontextprotocol.io/specification/2026-07-28/client/elicitation)
says the capability is declared in
`_meta.io.modelcontextprotocol/clientCapabilities` on each request, that an
empty `elicitation: {}` "is equivalent to declaring support for `form` mode
only", and that for URL mode "the client retries the original request" and the
server decides from `requestState` whether the out-of-band step finished. URL
mode itself dates from 2025-11-25 (SEP-1036,
[2025-11-25 changelog](https://modelcontextprotocol.io/specification/2025-11-25/changelog)).

## Summary table

| Harness | Latest revision negotiated | Still sends 2025-11-25 (or older) `initialize`? | Elicitation form | Elicitation URL | In-band vs `input_required` | Source |
| --- | --- | --- | --- | --- | --- | --- |
| Claude Code CLI (v2 runtime, default since 2.1.274/2.1.299) | 2026-07-28 via `server/discover` probe | Yes: falls back to `initialize` when the probe fails; `MCP_PROTOCOL_NEGOTIATION=legacy` forces it | Yes | Yes (2026-07-28 since 2.1.281; legacy stdio/http/sse/ws since 2.1.287) | Both: in-band on legacy connections, MRTR retry on 2026-07-28 | [docs](https://code.claude.com/docs/en/mcp), [CHANGELOG](https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md) |
| Claude Desktop (remote custom connectors) | 2026-07-28 (user-observed `server/discover` + `subscriptions/listen`, Oct 2026) | Unknown (not documented) | No (tracker issue open) | No | n/a | [claude-ai-mcp #1182](https://github.com/anthropics/claude-ai-mcp/issues/1182), [#153](https://github.com/anthropics/claude-ai-mcp/issues/153) |
| Claude.ai web connectors (`Anthropic/ClaudeAI`) | 2026-07-28 (user-observed, Sep 2026) | Yes, observed Sep 2026 on an `initialize`-only server, without echoing `Mcp-Session-Id` | No (#153 open since 2026-04) | No | Does not retry an `InputRequiredResult`; surfaces an error (#1027) | [#1027](https://github.com/anthropics/claude-ai-mcp/issues/1027), [#975](https://github.com/anthropics/claude-ai-mcp/issues/975) |
| Claude Agent SDK | Same runtime as Claude Code | Same as Claude Code | Yes via `onElicitation`; declined by default | Yes via `onElicitation` | Same as Claude Code | [SDK reference](https://code.claude.com/docs/en/agent-sdk/typescript) |
| Cursor | 2025-11-25 (staff, 2026-09-21) | Yes, only path | Yes (since 1.5, 2025-08-21) | Unknown (undocumented) | In-band only | [forum, staff reply](https://forum.cursor.com/t/mcp-client-cannot-connect-to-modern-only-2026-07-28-streamable-http-servers-legacy-initialize-rejected/172536), [changelog 1.5](https://cursor.com/changelog/1-5) |
| VS Code / Copilot agent mode (1.145.0 main) | 2025-11-25 (`MCP.LATEST_PROTOCOL_VERSION`) | Yes, only path; no `2026-07-28` string in repo | Yes | Yes (`{form:{}, url:{}}`) | In-band only | [modelContextProtocol.ts](https://github.com/microsoft/vscode/blob/main/src/vs/platform/mcp/common/modelContextProtocol.ts), [mcpServerRequestHandler.ts](https://github.com/microsoft/vscode/blob/main/src/vs/workbench/contrib/mcp/common/mcpServerRequestHandler.ts) |
| ChatGPT apps / connectors | 2026-07-28 per OpenAI's extensions spec capability table (inferred; no OpenAI doc states the client's version) | Yes per the same table (`2025-11-25` or earlier → `initialize`) | Yes (core form plus `openai/elicitation` extension; Desktop and Work browser, not iOS/Android) | Unknown (undocumented) | MRTR on 2026-07-28; `openai/elicitation/create` in-band on legacy | [openai/mcp-extensions spec](https://github.com/openai/mcp-extensions/blob/main/docs/spec.md) |
| OpenAI Codex CLI (rmcp 3.5.1 pinned) | Default `Legacy` mode: `initialize` at **2025-06-18**; `mcp_2026_07_28` flag (default off) enables `Auto` with 2026-07-28 | Yes, default | Yes (PR #17043, 2026-04-08) | Capability declared `{form, url}` when `auth_elicitation` is on (default on) | In-band on legacy; `request_input.rs` drives `input_required` rounds when modern | [protocol_mode.rs](https://github.com/openai/codex/blob/main/codex-rs/rmcp-client/src/protocol_mode.rs), [features/src/lib.rs](https://github.com/openai/codex/blob/main/codex-rs/features/src/lib.rs) |
| OpenAI Agents SDK (Python) | With `mcp` 2.x: `mode="auto"` → 2026-07-28 probe; with `mcp` 1.30: 2025-11-25 | Yes (fallback, or 1.x install) | No handler found | No | n/a (no elicitation) | [_compat.py](https://github.com/openai/openai-agents-python/blob/main/src/agents/mcp/_compat.py), [docs/mcp.md](https://github.com/openai/openai-agents-python/blob/main/docs/mcp.md) |
| OpenAI Agents SDK (TypeScript) | `@modelcontextprotocol/client` ^2.0.0 with `versionNegotiation: { mode: 'auto' }` (since 0.15.0) | Yes (fallback) | No handler found | No | n/a | [node.ts](https://github.com/openai/openai-agents-js/blob/main/packages/agents-core/src/shims/mcp-server/node.ts) |
| Gemini CLI | 2025-06-18 (`@modelcontextprotocol/sdk` 1.23.0 pinned) | Yes, only path | No (capabilities are `roots` only) | No | n/a | [package.json](https://github.com/google-gemini/gemini-cli/blob/main/packages/core/package.json), [mcp-client.ts](https://github.com/google-gemini/gemini-cli/blob/main/packages/core/src/tools/mcp-client.ts) |
| Windsurf (now Devin Desktop) | 2026-07-28 since v3.10.67 (2026-10-12) | Presumably (inferred; changelog says "support for connecting", not exclusivity) | No (docs list tools, resources, prompts only) | No | n/a | [changelog](https://docs.devin.ai/desktop/changelog), [MCP docs](https://docs.devin.ai/desktop/cascade/mcp) |
| TypeScript SDK v2 (`@modelcontextprotocol/client` 2.4.0) | 2026-07-28 available; **default `versionNegotiation.mode = 'legacy'`** → `initialize` at 2025-11-25 | Yes, by default | Yes | Yes | Legacy era in-band; 2026 era `inputRequired(...)` | [versionNegotiation.ts](https://github.com/modelcontextprotocol/typescript-sdk/blob/main/packages/client/src/client/versionNegotiation.ts), [docs/protocol-versions.md](https://github.com/modelcontextprotocol/typescript-sdk/blob/main/docs/protocol-versions.md) |
| TypeScript SDK v1 (`@modelcontextprotocol/sdk` 1.32.2, `v1.x`) | 2025-11-25 | Yes, only path | Yes | Yes | In-band only | [v1.x types.ts](https://github.com/modelcontextprotocol/typescript-sdk/blob/v1.x/src/types.ts) |
| Python SDK v2 (2.3.0) | 2026-07-28; default `mode="auto"` probes `server/discover` | Yes (fallback to `initialize` at 2025-11-25) | Yes | Yes | Both | [version.py](https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp-types/mcp_types/version.py), [docs/protocol-versions.md](https://github.com/modelcontextprotocol/python-sdk/blob/main/docs/protocol-versions.md) |
| Python SDK v1 (1.30.0) | 2025-11-25 | Yes, only path | Yes | Yes | In-band only | [v1.30.0 types.py](https://github.com/modelcontextprotocol/python-sdk/blob/v1.30.0/src/mcp/types.py) |
| Rust SDK rmcp (3.5.1 in Coral; 3.8.0 latest) | `LATEST = V_2026_07_28`; `ServiceExt::serve` stays on the `Initialize` lifecycle; `Auto`/`Discover` opt-in | Yes, by default (`LATEST_WITH_INITIALIZE = V_2025_11_25`) | Yes | Yes | Both; client `call_tool` drives `input_required` rounds | local `rmcp-3.5.1/src/model.rs`, `service/client.rs`; [CHANGELOG](https://github.com/modelcontextprotocol/rust-sdk/blob/main/crates/rmcp/CHANGELOG.md) |
| Go SDK (1.8.0) | 2026-07-28 (`latestProtocolVersion`); `Connect` calls `server/discover` first | Yes (fallback) | Yes | Yes | Both; `clientMultiRoundTripMiddleware` retries | [shared.go](https://github.com/modelcontextprotocol/go-sdk/blob/main/mcp/shared.go), [client.go](https://github.com/modelcontextprotocol/go-sdk/blob/main/mcp/client.go), [mrtr.go](https://github.com/modelcontextprotocol/go-sdk/blob/main/mcp/mrtr.go) |
| Java SDK (2.x; 2.2.0-M1) | 2025-11-25 as a client (`ProtocolVersions`); 2026-07-28 exists only in a `modern` **server** package | Yes, only client path | Yes | Yes (2.x) | In-band only | [ProtocolVersions.java](https://github.com/modelcontextprotocol/java-sdk/blob/main/mcp-core/src/main/java/io/modelcontextprotocol/spec/ProtocolVersions.java), [ROADMAP.md](https://github.com/modelcontextprotocol/java-sdk/blob/main/ROADMAP.md) |
| C# SDK (2.3.0) | 2026-07-28; `ProtocolVersion == null` prefers it, probes `server/discover` | Yes (fallback; 2025-11-25 is the `initialize` default) | Yes | Yes | Both; MRTR retries automatically | [McpProtocolVersions.cs](https://github.com/modelcontextprotocol/csharp-sdk/blob/main/src/Common/McpProtocolVersions.cs), [McpClientOptions.cs](https://github.com/modelcontextprotocol/csharp-sdk/blob/main/src/ModelContextProtocol.Core/Client/McpClientOptions.cs), [docs/concepts/mrtr](https://github.com/modelcontextprotocol/csharp-sdk/blob/main/docs/concepts/mrtr/mrtr.md) |

## Per-harness evidence

### Claude Code (CLI)

Source: [Connect Claude Code to tools via MCP](https://code.claude.com/docs/en/mcp)
and the [CHANGELOG](https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md)
(top entry 2.1.312 on 2026-10-28).

- Two runtimes. "The v1 runtime is built on MCP TypeScript SDK 1.x. The v2
  runtime is the same code on MCP TypeScript SDK 2.0, which adds MCP protocol
  revision 2026-07-28." On v2, Claude Code "Asks HTTP, stdio, and claude.ai
  connector servers whether they support the newer revision" and "connects to
  every other server as v1 does". Env overrides: `MCP_SDK_GENERATION=v1|v2`,
  `MCP_PROTOCOL_NEGOTIATION=auto|legacy`.
- Rollout, from the changelog: 2.1.274 made Bedrock/Vertex/Foundry and
  telemetry-disabled installs use "the v2 MCP client and MCP 2026-07-28
  negotiation with direct HTTP servers by default"; 2.1.292 did the same for
  "local (stdio) MCP server connections"; 2.1.295 for "claude.ai connectors";
  2.1.299 for "cloud session and Remote Control MCP connectors". 2.1.302 fixed
  servers that negotiate 2026-07-28 "but omit `resultType`, `ttlMs` or
  `cacheScope`".
- Elicitation: 2.1.76 "Added MCP elicitation support" (form fields or browser
  URL). 2.1.281 "Added MCP URL-mode elicitation on 2026-07-28 protocol
  connections". 2.1.287 "Added URL prompts from MCP servers on the 2025-11-25
  protocol" with a `bareElicitationCapability` opt-out for servers that reject
  `{form, url}`. The docs: Claude Code "declares `elicitation: {form: {}, url:
  {}}`" on 2026-07-28 connections and on legacy connections to user-configured
  stdio/sse/http/ws servers (v2.1.287+); on other legacy connections "such as
  to claude.ai connectors" it declares "the bare `elicitation: {}`", form only.
- Mode of answering: on legacy connections it serves `elicitation/create`
  in-band. On 2026-07-28 the MRTR retry path is exercised: a user report shows
  "claude-code/2.1.284" in a terminal logging "Fulfilling input required by
  'tools/call' (round 1)" and completing
  ([claude-ai-mcp #1085](https://github.com/anthropics/claude-ai-mcp/issues/1085)).
  The docs add that `elicitation/create` "is served only in interactive
  terminal sessions"; print/SDK runs do not render a dialog.

### Claude Desktop

No Anthropic documentation states Claude Desktop's MCP protocol revision or
elicitation support. Evidence is from the public tracker
[anthropics/claude-ai-mcp](https://github.com/anthropics/claude-ai-mcp):

- [#1182](https://github.com/anthropics/claude-ai-mcp/issues/1182) (2026-10-24,
  user report, server on `@modelcontextprotocol/server` 2.3.1): "Negotiated
  protocol: `2026-07-28` (client sends `server/discover`, then opens
  `subscriptions/listen`)". So Desktop's remote custom connector path speaks
  2026-07-28. Local `claude_desktop_config.json` stdio servers: no source.
- Elicitation: [#153 "Elicitation Support"](https://github.com/anthropics/claude-ai-mcp/issues/153)
  (opened 2026-04-06) is still open with comments through 2026-10-07.
  [anthropics/claude-code #41110](https://github.com/anthropics/claude-code/issues/41110)
  (Desktop elicitation request) was closed the next day without a maintainer
  comment. The Cowork surface inside Desktop runs the Claude Code runtime and
  declares `elicitation: { form: {}, url: {} }` but "never renders the request"
  (#1085; also [#1046](https://github.com/anthropics/claude-ai-mcp/issues/1046):
  "Elicitation request received in print mode" then a 180 s hang, still
  reproducing on claude-code 2.1.310 per a 2026-10-25 comment).

### Claude.ai web connectors

Anthropic's [custom connectors article](https://support.claude.com/en/articles/11175166-getting-started-with-custom-connectors-using-remote-mcp)
does not name a protocol version or elicitation. Tracker evidence:

- [#1027](https://github.com/anthropics/claude-ai-mcp/issues/1027) (2026-09-13):
  server log shows `clientInfo=Anthropic/ClaudeAI/1.0.0 proto=2026-07-28` on
  `server/discover` and `tools/list`; a state-only `InputRequiredResult` is
  shown as "Error occurred during tool execution" and "never re-issues the
  request with `requestState`". The official TS client retried the same
  response and completed.
- [#975](https://github.com/anthropics/claude-ai-mcp/issues/975) (2026-09-02):
  against an `initialize`-only server the backend (UA `Claude-User`) ran
  `initialize` 23 times and "Requests where the client sent `Mcp-Session-Id`:
  0". Requests arrived from ten different backend IPs. So the legacy fallback
  exists but does not keep a session.
- [#859](https://github.com/anthropics/claude-ai-mcp/issues/859) (2026-08-15):
  a server omitting `resultType` is rejected client-side, i.e. the client
  validates 2026-07-28 shapes strictly.
- Elicitation: #153 open; no form or URL mode.

### Claude Agent SDK

[Agent SDK MCP page](https://code.claude.com/docs/en/agent-sdk/mcp) and the
[TypeScript reference](https://code.claude.com/docs/en/agent-sdk/typescript).
The SDK drives the same Claude Code process, so runtime and negotiation follow
the Claude Code section (the MCP docs say v2 is the default "in the sessions
where it doesn't fetch feature flags" on 2.1.274+). The reference lists
`onElicitation: (request: ElicitationRequest, options: { signal }) =>
Promise<ElicitationResult>`, "Called when an MCP server requests user input and
no hook handles it first. When not provided, unhandled elicitation requests
are declined automatically". VS Code's tracker describes the SDK's two modes,
"form-based and URL-based", from `@anthropic-ai/claude-agent-sdk@0.2.63`
([microsoft/vscode #299011](https://github.com/microsoft/vscode/issues/299011)).

### Cursor

Closed source. [Cursor MCP docs](https://cursor.com/docs/mcp) list Elicitation
as a supported capability ("Server-initiated requests for additional
information from users") with no mode or version detail; the
[1.5 changelog](https://cursor.com/changelog/1-5) (2025-08-21) added "Support
for MCP elicitation" with JSON-schema requests. On the forum thread
[MCP client cannot connect to modern-only 2026-07-28 servers](https://forum.cursor.com/t/mcp-client-cannot-connect-to-modern-only-2026-07-28-streamable-http-servers-legacy-initialize-rejected/172536)
(Cursor 3.21.16, 2026-09-21) staff member Colin wrote: "Cursor currently
supports MCP protocol revisions 2025-11-25, 2025-06-18, 2025-03-26 and
2024-11-05 for remote servers" and, on a timeline for 2026-07-28, "No timeline
at the moment!" (2026-09-25). URL mode is not mentioned anywhere official.

### VS Code / GitHub Copilot agent mode

Open source; `package.json` on main is 1.145.0.

- [`src/vs/platform/mcp/common/modelContextProtocol.ts`](https://github.com/microsoft/vscode/blob/main/src/vs/platform/mcp/common/modelContextProtocol.ts)
  line 43: `export const LATEST_PROTOCOL_VERSION = "2025-11-25";`
- [`mcpServerRequestHandler.ts`](https://github.com/microsoft/vscode/blob/main/src/vs/workbench/contrib/mcp/common/mcpServerRequestHandler.ts)
  sends `protocolVersion: MCP.LATEST_PROTOCOL_VERSION` in `initialize`,
  declares `elicitation: opts.elicitationRequestHandler ? { form: {}, url: {} }
  : undefined`, dispatches `request.method === 'elicitation/create'` in-band,
  and listens for `notifications/elicitation/complete`.
- [`mcpElicitationService.ts`](https://github.com/microsoft/vscode/blob/main/src/vs/workbench/contrib/mcp/browser/mcpElicitationService.ts)
  branches `isFormElicitation` → `_elicitForm` and `isUrlElicitation` →
  `_elicitUrl`, and accepts pre-2025-11-25 requests without `mode`.
- `gh search code "2026-07-28" --repo microsoft/vscode` returns nothing.
- The [VS Code MCP docs](https://code.visualstudio.com/docs/copilot/customization/mcp-servers)
  note: "Servers requiring interactive inputs aren't forwarded to Copilot
  harness sessions."

### ChatGPT (apps / connectors)

OpenAI's [Apps SDK MCP server page](https://developers.openai.com/apps-sdk/build/mcp-server)
and [Responses API MCP guide](https://developers.openai.com/api/docs/guides/tools-connectors-mcp)
name no protocol version; the former says "Use MCP elicitation when the server
needs structured information" and links the 2025-06-18 lifecycle spec. The
most specific primary source is OpenAI's own
[mcp-extensions spec](https://github.com/openai/mcp-extensions/blob/main/docs/spec.md):

- Capability table: "`2026-07-28` or newer → `server/discover`" and
  "`2025-11-25` or earlier → `initialize`", so the host handles both eras.
- "OpenAI-registered MCP servers require MCP `2026-07-28` or later with
  multi-round-trip requests (MRTR) for form elicitation. Direct MCP connections
  still support legacy forms." On legacy connections "MCP Servers MUST use
  `openai/elicitation/create`", described as "a superset of `elicitation/create`".
- The platform table marks "Extended forms" Supported on Desktop, Supported
  with limits on Web ("the Work browser, excluding classic ChatGPT"), Not
  supported on iOS and Android.
- URL mode is not mentioned in the extension spec or the Apps SDK docs.

Inference: ChatGPT negotiates 2026-07-28 where the server offers
`server/discover` and otherwise falls back to `initialize`; elicitation is
form-only as documented. No OpenAI page states the client's `protocolVersion`
directly.

### OpenAI Codex CLI

Open source, Rust, built on rmcp. `codex-rs/Cargo.toml` pins
`rmcp = { git = ".../rust-sdk", rev = "98a54789...", version = "=3.5.1" }`,
the same minor Coral uses.

- [`rmcp-client/src/protocol_mode.rs`](https://github.com/openai/codex/blob/main/codex-rs/rmcp-client/src/protocol_mode.rs):
  `enum McpProtocolMode { #[default] Legacy, V20260728, DiscoverLocalStdio }`.
  `Legacy` maps to `ClientLifecycleMode::Initialize` with preferred version
  `ProtocolVersion::V_2025_06_18`; the other two map to `Auto { preferred:
  [V_2026_07_28], legacy_version: Some(V_2025_06_18) }`.
- [`features/src/lib.rs`](https://github.com/openai/codex/blob/main/codex-rs/features/src/lib.rs):
  `mcp_2026_07_28` is `Stage::UnderDevelopment, default_enabled: false`;
  `codex_apps_mcp_2026_07_28` likewise. So a default Codex install sends
  `initialize` at **2025-06-18**, not 2025-11-25.
- Elicitation: [PR #17043](https://github.com/openai/codex/pull/17043) "[mcp]
  Support server-driven elicitations" merged 2026-04-08. `config/mod.rs`
  declares `ElicitationCapability::new().with_form(..).with_url(..)` when
  `auth_elicitation` is on (`Stage::Stable, default_enabled: true`; "Prompt
  Codex Apps connector auth failures through MCP URL elicitations"), else an
  empty object. The TUI (`mcp_server_elicitation.rs`) renders form requests;
  URL rendering is only evidenced for the Codex Apps auth path.
- MRTR: `rmcp-client/src/request_input.rs` matches
  `ServerResult::InputRequiredResult` and retries, bounded by
  `InputRequiredRoundsExceeded`; it also accepts `openai/elicitation/create`
  input requests. Active only when the modern mode is enabled.

### OpenAI Agents SDK

- Python ([pyproject](https://github.com/openai/openai-agents-python/blob/main/pyproject.toml)):
  `mcp>=1.30.0, <3, !=2.0.*, !=2.1.*`. [`_compat.py`](https://github.com/openai/openai-agents-python/blob/main/src/agents/mcp/_compat.py)
  builds `mcp.Client(transport, mode="auto", ...)` on v2. [docs/mcp.md](https://github.com/openai/openai-agents-python/blob/main/docs/mcp.md):
  "The client first sends a `server/discover` probe at the newest protocol
  version supported by the installed MCP SDK" and otherwise "falls back to the
  legacy `initialize` handshake". On `mcp` 1.30.0 the SDK's
  `LATEST_PROTOCOL_VERSION = "2025-11-25"`. No elicitation handler appears in
  `src/agents/mcp` (search for `elicit` hits only docs).
- TypeScript ([agents-core package.json](https://github.com/openai/openai-agents-js/blob/main/packages/agents-core/package.json)):
  `"@modelcontextprotocol/client": "^2.0.0"`;
  [`shims/mcp-server/node.ts`](https://github.com/openai/openai-agents-js/blob/main/packages/agents-core/src/shims/mcp-server/node.ts)
  constructs `new Client(..., { versionNegotiation: { mode: 'auto' } })` and
  branches on `client.getProtocolEra()`. CHANGELOG 0.15.0: "feat: support MCP
  2026-07-28 negotiation with legacy fallback". No `elicit` symbol in the
  package.

### Gemini CLI

[`packages/core/package.json`](https://github.com/google-gemini/gemini-cli/blob/main/packages/core/package.json)
pins `"@modelcontextprotocol/sdk": "1.23.0"`, whose `src/types.ts` has
`LATEST_PROTOCOL_VERSION = '2025-06-18'` and
`SUPPORTED_PROTOCOL_VERSIONS = ['2025-06-18', '2025-03-26', '2024-11-05',
'2024-10-07']`. [`mcp-client.ts`](https://github.com/google-gemini/gemini-cli/blob/main/packages/core/src/tools/mcp-client.ts)
registers only `roots: { listChanged: true }` and a `ListRootsRequestSchema`
handler; no elicitation handler. Tracker: [#22249](https://github.com/google-gemini/gemini-cli/issues/22249)
("Method not found" on `elicitation/create`) closed 2026-05-08 with "we don't
have immediate plans to address this specific issue"; [#15613](https://github.com/google-gemini/gemini-cli/issues/15613)
(URL elicitation) closed 2026-05-11. The `elicitation` symbols elsewhere in the
repo belong to Gemini CLI's own agent protocol, not MCP. The
[MCP docs page](https://geminicli.com/docs/tools/mcp-server/) links the
2025-06-18 spec and notes Gemini CLI was replaced by Antigravity CLI for
unpaid tiers on 2026-06-18.

### Windsurf (Devin Desktop)

`windsurf.com` redirects to `docs.devin.ai`. The
[changelog](https://docs.devin.ai/desktop/changelog) entry v3.10.67
(2026-10-12): "Added support for connecting to MCP servers on protocol
revision 2026-07-28 (`server/discover`, no `initialize`)." The
[MCP docs](https://docs.devin.ai/desktop/cascade/mcp): "We currently support an
MCP server's tools, resources, and prompts." No elicitation, sampling or roots
entries exist in either page.

### Official SDKs as clients

- **TypeScript v2** (`@modelcontextprotocol/client` 2.4.0, release 2026-10-23):
  `packages/core-internal/src/types/spec.types.2026-07-28.ts` has
  `LATEST_PROTOCOL_VERSION = '2026-07-28'`; the legacy-era
  `packages/core/src/constants.ts` keeps `LATEST_PROTOCOL_VERSION =
  '2025-11-25'`, `DEFAULT_NEGOTIATED_PROTOCOL_VERSION = '2025-03-26'`,
  `SUPPORTED_PROTOCOL_VERSIONS = ['2025-11-25', '2025-06-18', '2025-03-26',
  '2024-11-05', '2024-10-07']`. `versionNegotiation.ts`:
  `const DEFAULT_VERSION_NEGOTIATION_MODE: VersionNegotiationMode = 'legacy';`
  with the comment "changing the default later is a flip of this single line".
  `docs/protocol-versions.md`: `'auto'` "probes the server with
  `server/discover`"; `{ pin: '2026-07-28' }` never falls back. Legacy-era
  servers use `ctx.mcpReq.elicitInput`; 2026-era handlers `return
  inputRequired(...)`.
- **TypeScript v1** (`v1.x` branch; npm `latest` 1.32.2):
  `LATEST_PROTOCOL_VERSION = '2025-11-25'`. The `v1.x-2026-07-28` branch still
  reads 2025-11-25 in `types.ts`.
- **Python** (2.3.0, 2026-10-02): `KNOWN_PROTOCOL_VERSIONS = (..., "2025-11-25",
  "2026-07-28")`, `LATEST_PROTOCOL_VERSION = KNOWN[-1]`,
  `LATEST_HANDSHAKE_VERSION = "2025-11-25"` ("the client's offer ... default"),
  `LATEST_MODERN_VERSION = "2026-07-28"` ("the `server/discover` probe
  default"). `docs/protocol-versions.md`: default `mode="auto"` sends "a single
  `server/discover` probe at the newest version this SDK speaks" and falls back
  to `initialize`; on 2026-07-28 "The server *returns* its questions and you
  retry". v1.30.0 (2026-09-07): `LATEST_PROTOCOL_VERSION = "2025-11-25"`.
- **Rust rmcp** (3.5.1 local registry copy; 3.8.0 released 2026-10-26):
  `model.rs` lines 169-209: `V_2026_07_28`, `V_2025_11_25`, ..., `LATEST =
  V_2026_07_28`, `NO_INITIALIZE = V_2026_07_28`, `LATEST_WITH_INITIALIZE =
  V_2025_11_25`, `impl Default for ProtocolVersion → LATEST`;
  `InitializeRequestParams::default()` sets `protocol_version:
  ProtocolVersion::default()` (line 1443). `service/client.rs`:
  `ClientLifecycleMode::{Initialize, Discover{..}, Auto{..}}`, "Existing
  `ServiceExt::serve` behavior remains legacy initialization"; `Auto` probes
  `server/discover` with a 10 s timeout; after `initialize`, a server answer
  that names a no-handshake version seeds per-request `_meta`. The client's
  `call_tool` drives `input_required` rounds through the local `ClientHandler`
  (`InputRequiredRoundsExceeded` bound); `ElicitRequestParams` is
  `#[serde(tag = "mode")]` with `form` and `url` variants. CHANGELOG:
  3.0.0-beta.1 (2026-07-23) "serve draft-version requests statelessly per
  SEP-2567", "add MRTR behavior support (SEP-2322)", "add server discovery and
  negotiation (SEP-2575)"; `stateful_mode` renamed `legacy_session_mode`
  because "the `2026-07-28` draft version is always served statelessly";
  3.2.0 (2026-08-31) "keep initialize on legacy protocol versions".
- **Go** (1.8.0, 2026-09-14): `mcp/shared.go` `latestProtocolVersion =
  protocolVersion20260728`, `supportedProtocolVersions = [2026-07-28,
  2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05]`,
  `CodeUnsupportedProtocolVersion = -32022`. `client.go`: "try the stateless
  server/discover RPC first ... fall back to the legacy initialize";
  `ElicitationHandler` with `ElicitationCapabilities{Form, URL}`; `mrtr.go`
  `clientMultiRoundTripMiddleware` retries with `inputResponses` and
  `requestState`. `docs/client.md` has "Completing a URL elicitation".
- **Java** (2.2.0-M1 2026-10-16; 1.1.5/0.18.5 security lines):
  `spec/ProtocolVersions.java` ends at `MCP_2025_11_25 = "2025-11-25"` on both
  main and 2.2.0-M1. `modern/McpSchema.java` has `LATEST_PROTOCOL_VERSION =
  "2026-07-28"` but the `modern` package contains only `server` (no client
  directory). ROADMAP: 2.x implements "the 2025-11-25 MCP specification
  revision" including "URL mode elicitation (SEP-1036)"; "3.x, will implement
  the 2026-07-28 MCP specification revision" with "first 3.0.0 milestone
  releases ... planned for September 2026". Conformance results show a
  "Modern Server" passing 37/37 for 2026-07-28; no modern client.
- **C#** (2.3.0, 2026-10-20): `McpProtocolVersions.cs`
  `July2026ProtocolVersion = "2026-07-28"` ("clients prefer it by default"),
  `November2025ProtocolVersion = "2025-11-25"` ("the default version for the
  `initialize` and session-resume code paths"); supported list 2024-11-05
  through 2026-07-28. `McpClientOptions.ProtocolVersion`: when null "the client
  prefers the latest revision (`2026-07-28`)", probes `server/discover` and
  "automatically falls back to the `initialize` handshake". `docs/concepts/mrtr`:
  "The C# SDK client prefers `2026-07-28` by default" and retries with
  `inputResponses`; elicitation handler covers `Form` and `Url` capabilities.

## What this means for a server deciding to drop sessions

Of the harnesses surveyed, the ones that negotiate 2026-07-28 today are Claude
Code and the Claude Agent SDK (v2 runtime), Claude.ai web and Claude Desktop
remote connectors, Windsurf/Devin Desktop (since 2026-10-12), the OpenAI Agents
SDKs when built on the v2 MCP SDKs, and ChatGPT per OpenAI's extension spec.
The ones that still speak only the `initialize` handshake are Cursor
(2025-11-25, "No timeline"), VS Code/Copilot (2025-11-25), Gemini CLI
(2025-06-18), Codex CLI in its default configuration (2025-06-18), the Java SDK
client (2025-11-25), the TypeScript v2 client in its default `'legacy'` mode,
and every v1 TypeScript or Python SDK consumer. Of the 2026-07-28-capable
clients, the `initialize` fallback is confirmed for Claude Code and the Claude
Agent SDK (documented, with `MCP_PROTOCOL_NEGOTIATION=legacy`), Claude.ai web
(observed in September 2026, before its current rollout; see Gaps), the OpenAI
Agents SDKs (`mode: 'auto'`) and the Go, C#, Python and Rust SDK clients. It is
unconfirmed for Claude Desktop (undocumented) and Windsurf (inferred from a
changelog), and ChatGPT's exact behaviour is inferred from OpenAI's extension
spec. No client examined is known to have dropped the fallback.

Elicitation support correlates with the handshake path rather than the
revision: the clients that render elicitation in-band are Claude Code (both
modes), VS Code (both modes), Cursor (form), Codex CLI (form; URL for its own
auth path), ChatGPT (form), and the Go, C#, Python, TypeScript, Rust and Java
SDK clients. Of these, only Claude Code, the Go, C#, Python and Rust SDK
clients, and (when its flag is on) Codex are verified to drive the
`input_required` retry on 2026-07-28; Claude.ai web does not retry (#1027) and
Cowork does not render (#1085, #1046). Claude.ai web, Claude Desktop remote
connectors (local stdio servers are not covered by this survey; see Gaps),
Gemini CLI, Windsurf and both OpenAI Agents SDKs show no elicitation. A server
that serves 2026-07-28 statelessly and keeps `initialize` on 2025-11-25 for the
remaining clients matches what the C# SDK's hybrid mode and rmcp's
`legacy_session_mode` model; a server that answers only 2026-07-28 is
unreachable from Cursor, VS Code, Gemini CLI and default Codex as of this date.

## Gaps

- **Claude Desktop local stdio servers**: no primary or user-reported source
  on which revision Desktop negotiates with `claude_desktop_config.json`
  servers, nor whether it declares `elicitation`.
- **Claude.ai web fallback**: #975 (2026-09-02) shows `initialize` without
  session echo; whether that path still exists after the 2026-07-28 rollout
  observed in #1027 (2026-09-13) is not confirmed by Anthropic.
- **ChatGPT protocol version**: inferred from OpenAI's extension spec
  capability table; no OpenAI page states the client's `protocolVersion` or
  whether classic ChatGPT (as opposed to the Work browser) renders forms. URL
  mode is undocumented.
- **Cursor URL mode**: no official statement either way. Cursor's stance on
  2026-07-28 is a forum reply, not a changelog or docs entry.
- **Windsurf/Devin fallback and elicitation**: the changelog says 2026-07-28
  connecting is supported; it does not state whether `initialize` remains or
  which era is preferred. Docs list no elicitation.
- **Codex URL-mode rendering**: the capability is declared `{form, url}` by
  default, but the only URL-mode consumer found is the Codex Apps auth path;
  generic URL elicitation rendering in the TUI was not verified.
- **OpenAI Agents SDKs**: no elicitation code found; absence was established
  by code search, not by a documented statement.
- **Java client default `initialize` version**: `ProtocolVersions` tops out at
  2025-11-25, but the client's default offer was not read from the client
  class.
- **rmcp plain `serve()` offer**: `InitializeRequestParams::default()` carries
  `ProtocolVersion::default()` = 2026-07-28, a version with no handshake. How
  the `Initialize` lifecycle and 3.2.0's "keep initialize on legacy protocol
  versions" adjust the offer on the wire was not traced; Codex sidesteps this
  by setting 2025-06-18 explicitly.
- **VS Code**: evidence is the `main` branch source (1.145.0), not a release
  note; a shipped Insiders/Stable build could differ.
