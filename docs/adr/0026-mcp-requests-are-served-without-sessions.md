---
status: accepted
---

# MCP requests are served without sessions

Coral's MCP Adapter used to open one RMCP session per client that sent `initialize`, keyed by Workspace and Principal
and held in the memory of the Coral Instance that answered. Every later request had to reach that instance: the
session held the client's declared capabilities and, while a tool call waited on a human, the live connection that
carried an in-band elicitation and received its answer. With two Coral Instances behind a load balancer, a request that
landed elsewhere found no session. It was the last piece of per-instance state in the request path once the Run Inbox
([ADR 0025](0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md)) let any instance accept an outside
Run Request.

Coral now serves every MCP request on its own. No session id is issued and nothing is kept between requests: each
request's credential and endpoint name its Principal and Workspace (a Workspace MCP Endpoint fixes the Workspace in its
URL, the Organization MCP Endpoint takes it from the credential, and a credential-free request to a local Workspace
endpoint acts as the implicit local User, as before). A client on a protocol version before
2026-07-28 still gets its `initialize` answer and uses the same tools; what it no longer gets is an in-band prompt. Its
Run Update names each required action with its deadline and the Run Status Page, where the invoking User approves or
connects, and the agent observes the Run again with `wait`. A 2026-07-28 client, which carries its capabilities in
every request, is prompted as before through `input_required` results and retries, since that exchange never needed a
session.

The protocol made the same move: revision 2026-07-28 removed sessions and server-initiated requests (SEP-2567 and
SEP-2322). The research behind this decision, which harnesses negotiate which revision and how each handles
elicitation, is in
[docs/research/2026-10-28-mcp-client-protocol-versions.md](../research/2026-10-28-mcp-client-protocol-versions.md).

## Consequences

- **Any Coral Instance serves any MCP request.** A load balancer needs no affinity, and a rolling deploy ends no
  conversation, since no client holds a session that one instance owned.
- **Clients on a revision before 2026-07-28 approve in Reef.** Cursor, VS Code Copilot and Codex, which speak
  2025-11-25 or older and prompted in-band, now show the agent's relayed Run Status Page link instead of a form. Claude
  Code keeps its prompts on connections that negotiate 2026-07-28, its default since the v2 runtime; a connection it
  falls back to `initialize` on, or that `MCP_PROTOCOL_NEGOTIATION=legacy` forces, loses them too. Claude.ai and Claude
  Desktop never prompted.
- **No standalone stream.** The MCP endpoints serve only requests, never a stream a client opens on its own or a
  request to end a session. Nothing is announced to a client outside the response to its own request, so a prompt is no
  longer withdrawn when the action is decided elsewhere; the next Run Update shows the decision.
- **A stale session header is ignored.** A request that still names a session is served like any other.

## Rejected

- **Session affinity at the load balancer.** It keeps the in-band prompt for older clients, but couples the deployment
  to an ingress feature, and a deploy or a lost instance still drops every session it held.
- **A shared session store.** RMCP can restore a session on another instance from a store, but the pending in-band
  request lives in the first instance's memory, so an answer that lands elsewhere is lost until the prompt's deadline.
- **Routing client messages to the instance that holds the session.** A second Run Inbox for MCP traffic, built to keep
  a feature the current protocol revision removed.
