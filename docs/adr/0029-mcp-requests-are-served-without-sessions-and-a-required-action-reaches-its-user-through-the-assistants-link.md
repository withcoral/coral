---
status: accepted
---

# MCP requests are served without sessions, and a Required Action reaches its User through the assistant's link and the client's own prompt

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
sessions and server-initiated requests (SEP-2567 and SEP-2322).

Coral calls a request that a Run waits on a **Required Action**. Every Required Action of a Run that a User invoked
reaches that User through the assistant's link: the tool call that observes the Run returns the Run Update with the
Required Action and the link to the page where that User decides it, the assistant passes the link and the Run
identifier to the person, the waiting call shows the link to a client that asks for progress, a client that renders
MCP Apps shows the request as one, and the person decides on that page, signed in to Reef. An approval's page shows the
exact Tool arguments. Reef is the standing place: the Approvals Page holds every Approval Request, the person's account
pages their Account Connection Requests, and the Run Status Page every Required Action of its Run, from the moment it
exists, and the first answer counts wherever it comes from. A client that declares MCP elicitation on the request is
also asked in its own prompt, as the section below states; the link stays the way that works in every client. Coral
sends no notification apart from the waiting call's progress message, and it offers the assistant no tool, argument or
path by which to decide: the assistant can only pass the request on. Only the capabilities a request declares and one
deployment setting choose anything, so Coral keeps no list of known clients, and a client it has never seen is served
by what it declares. The map that decided the link is [#2732](https://github.com/withcoral/coral/issues/2732); the
parent of its implementation is #2825.

## A client that declares elicitation is asked in its own prompt

The link alone proved easy to miss: nothing stops the person, and a Required Action nobody reads waits until the Run
times out. Protocol revision 2026-07-28 lets a server ask a client without a session: each request declares the
client's capabilities, and a tool call can answer that it needs input, carrying the questions and an opaque state; the
client asks the person and repeats the call with the answers and the state. Any Coral Instance serves the repeat, so
the prompt keeps every request standing on its own. Claude Code negotiates that revision, declares both elicitation
modes on every tool call, shows the prompt and repeats the call; [#3415](https://github.com/withcoral/coral/issues/3415)
holds the check.

- **What the client declares chooses the prompt.** When the observing tool call that waits finds a Required Action that
  names the invoking User and the request declares elicitation, the call asks for input instead of returning the Run
  Update. The tool call that runs a Program never asks: a client answers by repeating the call, and a repeated run
  would run the Program again; its Run Update reaches the waiting call, which asks. An
  Approval Request is asked as a form with Approve and Decline when the client declares form mode and the deployment
  allows it; otherwise as a link to its page. An Account Connection Request is always asked as a link, because the
  connection happens at the Provider's sign-in. A client that declares neither mode gets the assistant's link alone.
- **What was asked travels with the client, and grants nothing.** The state of the repeated call and the cursor of
  every Run Update name the Required Actions already asked, so each is asked once while the assistant passes the
  latest cursor on. Neither is signed: they hold only what a client could send anyway, every observation is
  authorized as before, and an answer decides only through the same decision path as Reef's, which checks the
  person. A client that edits them can only be asked again, or not at all.
- **An answer decides only through the form.** An Approve or Decline from the form is that User's decision, recorded
  through the same decision path as Reef's, and the first answer wins. A link prompt's accept means only that the
  person opened the page or said they were done; the call goes on waiting for the decision made in Reef. A dismissed
  prompt falls back to the assistant's link, so a client that cannot show the prompt, such as Claude Code without a
  terminal, which answers at once, loses nothing.
- **The link prompt carries the link as text.** A client opens a link on the host where it runs, which over SSH is not
  the person's, so the message names the link too.
- **The assistant sees the outcome.** A request decided in a form leaves the Run Update as one decided in Reef does,
  and a decline reaches the Program as its error, so the assistant's account of the Run stays true. A dismissed
  prompt leaves the Run Update as it was: the action still waits there with its link. No field says that a prompt was
  asked or how it was answered, though #3415 asked for one: the assistant is told, not asked (below), and the Run
  Update already shows what still waits.
- **The deployment chooses how far to trust the client.** An approval decided in the form is recorded by the MCP
  client, not by a person signed in to Reef, and a client or one of its hooks can answer without anyone reading it. One
  operator setting chooses for the whole deployment: form prompts for approvals (the default), link prompts only, or
  no prompts. Form is the default because the client holds the User's own credential and the person asked for the
  prompt; an operator who does not accept that chooses link prompts. No prompts exists because a client can declare
  elicitation and then neither show the prompt nor repeat the call, which Coral cannot see:
  the Run then waits until its deadline. Remove that choice once no supported client does this.

## What changes so the assistant's link is reliable

The assistant's link existed before this decision, as the fallback for clients that got no prompt, and the live check
showed why it was not yet reliable: the assistant stopped after one call and asked the person to say when
they had acted, named the Tool by guesswork, and left the person to find the request on a page that led with the Run's
activity ([what the assistant receives](https://github.com/withcoral/coral/issues/2736#issuecomment-6519311420)). A
second live check, in Claude Code, showed that the assistant's text alone is not enough: told three ways to write the
link and then keep waiting, it wrote its message inside its own thinking, said "the link provided", and kept calling
the tool, because that client steers its model to plan privately between tool calls and shows thinking only as a
summary. Codex wrote the link as text before its first wait. Making the link the one way means fixing each of those:

- **The Run Update says what is asked.** Each Required Action names its kind, the Provider, the waiting Tool where one
  Tool waits (a connection request shared by Invocations of different Tools names none), the actor, the deadline and
  the link to the page where that person decides it. The assistant phrases the request for the person from these
  fields alone; Coral supplies the assistant no ready-made sentence, and the link travels to it only in its own field,
  never inside prose. The link is kept short, because a client shows it on one line.
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
- **The waiting call shows the link too.** When a request carries a progress token, the observing tool call that
  waits reads the Run once before it holds and, if a Required Action names the invoking User, sends one progress
  message on that call's own response stream for the first such entry, the link first and the request after it, so
  a client that cuts the line keeps the link. When that entry is decided the wait returns, and the next wait names
  the next entry. The message is for the person, not the assistant, which still reads the Run Update. A client that
  sends no token gets nothing but the Run Update, and nothing is sent outside the response to the request. Claude
  Code shows that line on the running call; Codex's terminal and Claude Desktop do not (checked by hand on
  2026-11-04, #2846), so the assistant's text stays the first way and the progress line the second.
- **A client that renders MCP Apps shows the request itself.** Claude Desktop condenses the assistant's text to one line
  while tools run and renders no progress, so neither way reached the person there. One tool, which the assistant calls
  to show a Run's Required Actions, links one `ui://` resource, an app the host renders with the tool result, in place,
  while the turn goes on: it shows the entries that name the invoking User, what is asked, the waiting Tool where one
  Tool waits, the Provider and the deadline, with one control that opens the entry's page. It decides nothing, calls no
  tool and holds no state: the tool's result, the Run's Required Actions as the Run Update lists them, is its whole
  input. The assistant calls the tool once each time a new Required Action names the invoking User, after it writes the
  link, and then goes on waiting. A card shows the entries the Run had at that call and does not change, so a later
  request gets its own call and its own card. The observing tools do not link the app. The extension links an app to a
  tool, not to one result, so a host renders it for every call of a linked tool, and Claude Desktop then tells the
  assistant that the person already sees the result (seen on 2026-11-22). Every client gets the same declaration; in one
  without the extension nothing renders, and the tool returns only the Run's identifier, its state and its Required
  Actions. Claude Desktop rendered the app when the observing tools linked it (checked by hand on 2026-11-04, #2846),
  and the [MCP Apps documentation](https://modelcontextprotocol.io/extensions/apps/overview#client-support) lists
  claude.ai and VS Code among its hosts. The link reaches the person when any of the three ways works, and only a client
  that shows none leaves it to the final reply.
- **Waiting is exact.** From the cursor it was given, the observing tool call that waits holds, for at most one
  bounded call, until the Required Action list changes or the Run ends, then returns at once; ordinary output does
  not wake it, and a call without a cursor treats what it first reads as already seen
  ([#2731](https://github.com/withcoral/coral/issues/2731)). The Run Deadline bounds every deadline the assistant is
  told, so it is the loop's outer bound.
- **The page is the request.** The link opens the request, not a list: an Approval Request's own address opens the
  Approvals Page with that request in review, its exact arguments in view; a connection request's link opens the
  person's own account page for the Provider, whose Connect control is the request. After the last decision the page
  tells the person to go back to their AI app. The section below settles the pages.

## Consequences

- **Any Coral Instance serves any MCP request.** A load balancer needs no affinity, and a rolling deploy breaks no
  client's connection to Coral, since no client holds a session that one instance owned.
- **No standalone stream.** The MCP endpoints serve only requests, never a stream a client opens on its own or a
  request to end a session. Nothing is announced to a client outside the response to its own request: the progress
  message rides the waiting call's response, and the next Run Update shows a decision made elsewhere.
- **A stale session header is ignored.** A request that still names a session is served like any other.
- **The assistant is told, not asked.** Nothing from the arguments or the Account travels in the Run Update, and no
  field says whether the person saw the link or a prompt; what the assistant receives is fixed above.
- **One clock.** The **Run Deadline**, the Run's accepted time plus its timeout, bounds every wait for a person;
  nothing pauses or extends it, and a Run that waits past it ends `TimedOut` for the person to run again. An Approval
  Request has no deadline of its own, so an approval never expires while its Run lives: a decline is the Program's
  error, silence is the timeout. An Account Connection Request keeps its own expiry across Runs; the deadline the Run
  Update shows for it is the earlier of that expiry and the Run Deadline. A decision that arrives after the Run ended
  does nothing for that Run. The maximum timeout is one deployment-wide constant with no operator setting; if one is ever added it is
  deployment-wide, never per Workspace ([time limits](https://github.com/withcoral/coral/issues/2738#issuecomment-6520221759)).
- **Where a Required Action reaches the person is never Tool policy.** The Tool Access State keeps its three values: a
  Tool is enabled, needs approval, or is disabled, and nothing about it decides where or how the person is asked
  ([no setting](https://github.com/withcoral/coral/issues/2742#issuecomment-6521367372)). Only the deployment's
  elicitation setting and what the client declares choose that, as stated above.
- **The accepted cost.** A Required Action nobody reads waits to the Run Deadline, the Run ends `TimedOut`, and the
  person reruns. An assistant running unattended with a person's credential gets no other way in: the person changes the
  Tool's policy in Reef beforehand ([no notification](https://github.com/withcoral/coral/issues/2740#issuecomment-6521160022)).
- **Agent-invoked Runs are unchanged.** Their Required Actions name the Workspace Owner, who acts in Reef; the Agent
  sees that its Run waits on an Owner, cannot inspect or decide the request, and has no person to pass the link to.
- **A Provider's own question to the User** (#2447) becomes a third kind of Required Action with the same shape: one
  exact person, one request with a deadline, the same link and the same first-wins rule.

## The pages the link opens

The page the assistant's link opens is where the person decides, so it opens the request, not an activity list
([the prototype decision](https://github.com/withcoral/coral/issues/2739#issuecomment-6520881303)). The Approvals Page
landed while this decision was being made (#2805) and already decides Approval Requests with the exact arguments in
view, so it is the page for an approval. The same change moved connection requests to the person's account pages, so
the account page for the Provider is the page for a connection, and the Run Status Page keeps the Run's record and
lists its Required Actions.

- An Approval Request has its own address, `/workspaces/<workspace>/approvals/<request>`, which opens the Approvals
  Page with that request in review: the exact arguments, a comment, Approve and Decline. Each request is decided on
  its own; the review moves to the Run's next pending request when there is one, in the order the requests were made.
- An Account Connection Request's link opens the person's own account page for the Provider,
  `/workspaces/<workspace>/accounts/<provider>`, whose one Connect control goes straight to the Provider's sign-in.
  Sign-in returns to the same account page, which shows the account connected.
- "Approve all" decides each open Approval Request of the Run on its own, and there is no "Deny all".
- From the review, a Workspace Owner can stop the Tool from asking again, through a confirmed change that sets the
  Tool's policy to `enabled`; the waiting call continues, as the glossary's Approval Request entry says.
- After the last decision the page tells the person to go back to their AI app, and that the Run continues there.
  The app is named when Coral recorded its name with the Run, and is "your agent" otherwise. A Run that already
  ended says so and decides nothing.
- Each request shows how long it has waited and no countdown: the assistant already told the person the deadline,
  and a late arriver reads that the Run ended.

## Rejected

- **Session affinity at the load balancer.** It keeps the in-band prompt for older clients, but couples the deployment
  to an ingress feature, and a deploy or a lost instance still drops every session it held.
- **A shared session store.** RMCP can restore a session on another instance from a store, but the pending in-band
  request lives in the first instance's memory, so an answer that lands elsewhere is lost until the prompt's deadline.
- **Routing client messages to the instance that holds the session.** A second Run Inbox for MCP traffic, built to keep
  a feature the current protocol revision removed.
- **MCP elicitation in protocol revisions before 2026-07-28.** There a client declares its capabilities only when it
  connects, and the prompt is a request from the server that the instance holding the connection must answer, so it
  needs the session this decision removed. A client on an earlier revision gets the assistant's link alone. The first
  look at elicitation also found that it blinded the client's model and outlived the Run Deadline
  ([the elicitation decision](https://github.com/withcoral/coral/issues/2737#issuecomment-6602262606)); the Run Update
  after a prompt and the fallback on a dismissed prompt answer the first, and an answer after the Run ended does
  nothing for that Run.
- **Ending the assistant's turn on each Required Action.** Tried on 2026-11-04 after the Claude Code check: the
  final reply is the one thing every client shows, so the link reached the person, but the Run then went on without
  its assistant until the person wrote back, and the person had to. It returns only for a client that shows no
  progress message and whose model writes nothing between tool calls.
- **A notification** from Reef, Desktop, email or a chat app. The person is at the AI app they invoked the Run
  from, and a deadline measured in minutes leaves a notification little to do. It returns as a fresh effort if the
  timeout case hurts in use.
- **Pausing the Run Deadline while a person decides.** It would keep the isolate, the owner task and the lease alive
  for as long as a person is away and make the recorded deadline move.
- **Looking ahead at a Program to announce the approvals and connections it will need.** Coral asks only when an
  Invocation asks.
- **The assistant answering for the person**, and treating the AI app's own "allow this tool?" prompt as the approval.
  The assistant holds the User's credential, so the only trusted paths to a decision are ones a person operates: a
  signed-in Reef page, and, where the deployment allows it, the form the client shows the person when Coral asks. The
  AI app's own prompt approves the client's call to Coral, not the Invocation the Program makes.
