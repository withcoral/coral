---
status: accepted
---

# MCP requests are served without sessions, and a Required Action reaches its User through the assistant's link

Coral's MCP Adapter used to open one RMCP session per client that sent `initialize`, keyed by Workspace and Principal
and held in the memory of the Coral Instance that answered. Every later request had to reach that instance: the
session held the client's declared capabilities and, while a tool call waited on a human, the live connection that
carried an in-band elicitation and received its answer. With two Coral Instances behind a load balancer, a request that
landed elsewhere found no session. It was the last piece of per-instance state in the request path once the Run Inbox
([ADR 0025](0025-outside-run-requests-reach-the-run-owner-through-the-run-inbox.md)) let any instance accept an outside
Run Request. The session existed for the elicitation, and the elicitation is what a Run that a User invoked from their
AI app relied on when a Tool Invocation needed that User: an Approval Request under an `approval_required` Tool Access
State, or an Account Connection Request when the `caller` Credential Route finds no usable Account. Only that User can
decide, and the Run waits until they do. The
[client survey](https://github.com/withcoral/coral/issues/2734#issuecomment-6501368670) and the
[live check](https://github.com/withcoral/coral/issues/2735#issuecomment-6599973987) showed what elicitation delivers
today: one client in nine renders it, that client's model sees nothing of the prompt and narrates the Run wrongly
afterwards, an unanswered prompt outlives the Run's deadline, and one client declares the capability and shows nothing.

Coral now serves every MCP request on its own. No session id is issued and no MCP session state is kept between requests: each
request's credential and endpoint name its Principal and Workspace (a Workspace MCP Endpoint fixes the Workspace in its
URL, the Organization MCP Endpoint takes it from the credential, and a credential-free request to a local Workspace
endpoint acts as the implicit local User, as before). The protocol made the same move: revision 2026-07-28 removed
sessions and server-initiated requests (SEP-2567 and SEP-2322); the research behind that, which harnesses negotiate
which revision and how each handles elicitation, is in
[docs/research/2026-10-28-mcp-client-protocol-versions.md](../research/2026-10-28-mcp-client-protocol-versions.md).

With the session goes the prompt. Coral calls a request that a Run waits on a **Required Action** and the act of
putting it in front of the one User who can decide it a **Human Handoff**. Every Required Action of a Run that a User
invoked is offered through one **Handoff Route**, the assistant's link: the tool call that observes the Run returns the Run Update with the Required
Action and the Run Status Page link, the assistant passes the link and the Run identifier to the person, and the person
decides on the Run Status Page, signed in to Reef, with the exact Tool arguments in view. Reef is the standing place:
the Run Status Page holds every Required Action, and the Approvals Page every Approval Request, from the moment it
exists, and the first answer counts wherever it comes from. Coral sends no MCP elicitation and no notification, and it
offers the assistant no tool, argument or path by which to decide: the assistant can only pass the request on. Nothing
in the request (protocol revision, declared capabilities, client name) chooses anything, so Coral keeps no list of
known clients, and a client it has never seen gets the same handoff. The map that decided this is
[#2732](https://github.com/withcoral/coral/issues/2732); the parent of the implementation is #2825.

## What changes so the assistant's link is a Route to rely on

The assistant's link existed before this decision, as the fallback for clients that got no prompt, and the live check
showed why it was not yet a Route to rely on: the assistant stopped after one call and asked the person to say when
they had acted, named the Tool by guesswork, and left the person to find the request on a page that led with the Run's
activity. Making it the one Route means removing each of those
([what the assistant receives](https://github.com/withcoral/coral/issues/2736#issuecomment-6519311420)):

- **The Run Update says what is asked.** Each Required Action names its kind, the Provider, the waiting Tool where one
  Tool waits (a connection request shared by Invocations of different Tools names none), the actor, the deadline and
  the Run Status Page link, naming the request. The assistant phrases the request for the
  person from these fields alone; Coral supplies no ready-made sentence, and the link travels only in its own field,
  never inside prose.
- **The assistant's loop is written down, in the tools, not only in the instructions.** The server instructions state
  the rule in brief: a non-empty Required Action list means the Run waits for a person; show the link and the Run
  identifier and ask that person to act; never ask for credentials in the chat. The descriptions of the tools that
  observe a Run carry the loop itself, because those are the tools whose result the assistant is reading when a
  Required Action appears: tell the person what is asked and by when, the Run identifier, and the link exactly as
  given; then keep calling the observing tool call that waits, with the latest cursor, until no Required Action
  remains or the Run ends.
  The person's decision in Reef resumes the Run on its own, so the assistant does not ask the person to report back.
  The assistant makes no judgment about whether anyone is reading: the same loop runs in a chat and unattended. The
  assistant cannot decide and must not say it did. A decline reaches the Program as an error; the assistant reports it
  from the Run's output and does not run the Program again unasked. The assistant asks the person to connect an
  Account only when that Required Action appears, never ahead of a Run.
- **Waiting is exact.** From the cursor it was given, the observing tool call that waits holds, for at most one
  bounded call, until the Required Action list changes or the Run ends, then returns at once; ordinary output does
  not wake it, and a call without a cursor treats what it first reads as already seen
  ([#2731](https://github.com/withcoral/coral/issues/2731)). The Run Deadline bounds every deadline the assistant is
  told, so it is the loop's outer bound.
- **The page is the request.** The link opens the Run's page. The page is headed by the Run identifier the assistant
  said; each request is a card decided on its own, an approval card with the exact arguments; after the last decision
  the page tells the person to go back to their AI app. The Run Status Page section settles the page.

## Consequences

- **Any Coral Instance serves any MCP request.** A load balancer needs no affinity, and a rolling deploy breaks no
  client's connection to Coral, since no client holds a session that one instance owned.
- **No standalone stream.** The MCP endpoints serve only requests, never a stream a client opens on its own or a
  request to end a session. Nothing is announced to a client outside the response to its own request; the next Run
  Update shows a decision made elsewhere.
- **A stale session header is ignored.** A request that still names a session is served like any other.
- **The assistant is told, not asked.** Nothing from the arguments or the Account travels in the Run Update, and no
  field says whether the person saw anything; what the assistant receives is fixed above.
- **One clock.** The **Run Deadline**, the Run's accepted time plus its timeout, bounds every wait for a person;
  nothing pauses or extends it, and a Run that waits past it ends `TimedOut` for the person to run again. An Approval
  Request has no deadline of its own, so an approval never expires while its Run lives: a decline is the Program's
  error, silence is the timeout. An Account Connection Request keeps its own expiry across Runs; the deadline the Run
  Update shows for it is the earlier of that expiry and the Run Deadline. A decision that arrives after the Run ended
  does nothing for that Run. The maximum timeout is one deployment-wide constant with no operator setting; if one is ever added it is
  deployment-wide, never per Workspace ([time limits](https://github.com/withcoral/coral/issues/2738#issuecomment-6520221759)).
- **Where a Required Action reaches the person is never policy.** The Tool Access State keeps its three values: a Tool
  is enabled, needs approval, or is disabled, and nothing about it decides where or how the person is asked
  ([no setting](https://github.com/withcoral/coral/issues/2742#issuecomment-6521367372)).
- **The accepted cost.** A Required Action nobody reads waits to the Run Deadline, the Run ends `TimedOut`, and the
  person reruns. An assistant running unattended with a person's credential gets no other Route: the person changes the
  Tool's policy in Reef beforehand ([no notification](https://github.com/withcoral/coral/issues/2740#issuecomment-6521160022)).
- **Agent-invoked Runs have no Human Handoff.** Their Required Actions name the Workspace Owner, who acts in Reef, as
  before.
- **A Provider's own question to the User** (#2447) becomes a third kind of Required Action with the same shape: one
  exact person, one request with a deadline, the same Route and the same first-wins rule.

## The Run Status Page

The page a Human Handoff opens is where the person decides, so it is a handoff sheet, not an activity list
([the prototype decision](https://github.com/withcoral/coral/issues/2739#issuecomment-6520881303)):

- It is headed by the Run identifier, the name the assistant said, with the Run's intent under it. Each Required Action
  is its own card with its own decision; an approval card shows the exact arguments. The Program follows, folded. Cards
  are in the order the requests were made, and the request the link names is not marked, since every card is decided
  the same way.
- "Approve all" decides each open Approval Request on its own, and there is no "Deny all".
- From the card, a Workspace Owner can stop the Tool from asking again, through a confirmed change that sets the Tool's
  policy to `enabled`; the waiting call continues, as the glossary's Approval Request entry says.
- After the last decision the page tells the person to close the tab and go back to their AI app, and that the Run
  continues there. The app is named when Coral recorded its name with the Run, and is "your agent" otherwise. A Run that
  already ended says so and decides nothing.
- Each request shows how long it has waited and no countdown: the assistant already told the person the deadline, and
  a late arriver reads that the Run ended.
- A row on the Approvals Page names the Run and opens this page; it decides nothing, since a gist is not the arguments.

## Rejected

- **Session affinity at the load balancer.** It keeps the in-band prompt for older clients, but couples the deployment
  to an ingress feature, and a deploy or a lost instance still drops every session it held.
- **A shared session store.** RMCP can restore a session on another instance from a store, but the pending in-band
  request lives in the first instance's memory, so an answer that lands elsewhere is lost until the prompt's deadline.
- **Routing client messages to the instance that holds the session.** A second Run Inbox for MCP traffic, built to keep
  a feature the current protocol revision removed.
- **MCP elicitation as a Route, chosen per client.** It renders in one client today, blinds that client's model to what
  was asked and answered, and outlives the Run Deadline. It returns only when a client renders every elicitation mode
  and tells its model what was asked and answered ([the Route decision](https://github.com/withcoral/coral/issues/2737#issuecomment-6602262606)).
- **A notification Route** from Reef, Desktop, email or a chat app. The person is at the AI app they invoked the Run
  from, and a deadline measured in minutes leaves a notification little to do. It returns as a fresh effort if the
  timeout case hurts in use.
- **Pausing the Run Deadline while a person decides.** It would keep the isolate, the owner task and the lease alive
  for as long as a person is away and make the recorded deadline move.
- **Looking ahead at a Program to announce the approvals and connections it will need.** Coral asks only when an
  Invocation asks.
- **The assistant answering for the person**, and treating the AI app's own "allow this tool?" prompt as the approval.
  The assistant holds the User's credential, so the only trusted path to a decision is one a person operates: a
  signed-in Reef page.
