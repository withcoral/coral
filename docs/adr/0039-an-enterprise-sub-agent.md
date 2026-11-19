---
status: accepted
---

# An Enterprise Sub-agent

Enterprises want data work kept in-house, for privacy and for cost. Today a lead agent such as Claude Code does that
work itself, finding Tools, writing Programs and reading results, and sends provider data to a hosted frontier model at
every step. The lead calls Coral's Sub-agent through one more MCP tool, `agent`, and watches or stops it with its
existing `wait` and `cancel` tools. The Sub-agent is an Enterprise Feature (ADR 0033): its code lives in `ee/`, and
core gains only general extension points.

## What we cared about

We held to these eleven points while we made the decisions below. Each decision names the points it serves.

1. We did not invent a new way to configure an external service. The model is configured the way the database
   connection is.
2. We did not interrupt normal service when no model is configured.
3. We did not wait for changes to the user interface.
4. We wanted something testable as soon as possible.
5. We used the patterns that `ee/` already has.
6. We made the Sub-agent robust to changes in Coral's MCP tools, so that a trained model does not stop working soon
   after it is trained.
7. We gave the Sub-agent no permissions of its own.
8. We kept Coral's rule that no instance holds session state.
9. We did not keep two copies of core's logic.
10. We did not add limits or settings that we could only guess at.
11. We made sure that the lead can check an answer from the Run itself.

## The design at a glance

```mermaid
flowchart LR
    lead["Lead agent<br/>e.g. Claude Code"]
    model["Model<br/>operator's URL"]
    db[("Database<br/>Turn state")]
    subgraph ee["ee/: works only under the Enterprise Edition"]
        eesurface["ee/ surface<br/>adds the agent tool"]
        subagent["Sub-agent<br/>runs each Turn"]
    end
    subgraph core["core"]
        shell["MCP shell<br/>authenticates"]
        coresurface["Core surface<br/>exec, wait, cancel"]
        modules["Core modules<br/>Tasks, Runs, Tool search"]
    end
    lead --> shell --> eesurface
    eesurface --> subagent
    eesurface --> coresurface
    coresurface --> modules
    subagent --> modules
    subagent --> model
    subagent --> db
```

The lead's request enters through core's MCP shell and reaches the `ee/` surface. A call to `agent` starts a Sub-agent
Turn, and every other call passes to the core surface. The Sub-agent sends the conversation to the model, and it
reaches Tasks, Runs and Tool search by calling core's modules directly, as core's own tools do.

The sections below give each decision, the points it serves, and the alternatives we rejected. The appendix at the end
explains how each part works in detail.

## Decisions

### 1. The Sub-agent is an Enterprise Feature that wraps core's MCP tools

**Serves points 2 and 5.**

We will keep all Sub-agent code in `ee/`. Core's MCP shell will ask a tool surface interface, `McpInterface`, for the
instructions, for the list of tools, and to run each tool call. Core will supply the default implementation, which
serves the tools that Coral serves today. `ee/` will supply a second implementation that wraps the first. It adds the
`agent` tool, lets `wait` and `cancel` accept a `sub_agent_id`, and passes every other call to core unchanged.

```mermaid
flowchart TD
    lead["Lead agent"]
    shell["Core: MCP shell<br/>protocol, authentication<br/>Workspace check<br/>span per tool call"]
    iface["Tool surface interface<br/>instructions, tools, call"]
    eesurface["ee/ surface<br/>adds the agent tool<br/>extends wait and cancel<br/>passes other calls to core"]
    coresurface["Core surface<br/>the tools served today"]
    lead --> shell --> iface --> eesurface
    eesurface -- wraps --> coresurface
```

`ee/` will register its implementation through `Extensions`, check the Edition on every call, and keep its tables in
core's migration chain. ADR 0033 sets these rules for every Enterprise Feature, and the Secret Store kinds in `ee/`
already register and check the Edition in this way.

The lead sees the Sub-agent only in the last row of this table. In every other case it receives exactly the tools and
instructions that a source build serves.

| Edition    | `[sub_agent]` in the config file | Tools the lead sees                                                           |
| ---------- | -------------------------------- | ----------------------------------------------------------------------------- |
| Community  | absent or present                | `exec`, `wait`, `cancel`, `end_task`                                          |
| Enterprise | absent                           | `exec`, `wait`, `cancel`, `end_task`                                          |
| Enterprise | present                          | the same tools plus `agent`; `wait` and `cancel` also accept a `sub_agent_id` |

**Rejected:** listing `agent` under the Community Edition and refusing every call to it. Secret Stores work that way
because an administrator creates each one, and a hidden store would look deleted. Nobody creates the `agent` tool, and
a listed tool that always refuses would add its description to every request the lead's model makes.

### 2. A Sub-agent runs with its caller's permissions

**Serves points 7 and 11.**

We will give a Sub-agent no identity and no permissions of its own. It runs with the permissions of the agent that
called it, inside that agent's Task. Each Run it starts is recorded as the calling agent's Run and is checked by the
same Policy Enforcement Point.

Because those Runs belong to the lead's own Task, the lead can check an answer for itself. Each result from `wait`
lists the steps the Sub-agent took. A step that started a Run holds the intent the Sub-agent gave for it and its
`run_id`, and nothing else. The lead reads that Run's Program and Final Value with `wait`.

**Rejected:**

- **A summary of evidence that the Sub-agent writes for the lead.** The lead can already read the Run itself.
- **A summary under the steps of what each Run called**, such as calls for each Provider, refused calls and pages not
  read. It would show a mistake at a glance, but core would need a new read of a Run's calls to supply it. The lead
  reads the Run when it wants that detail.

### 3. The Sub-agent calls core's Task and Run modules directly

**Serves point 6. It goes against point 9 in a small way, which is described below.**

Today, each of the MCP handlers `exec`, `wait` and `cancel` is one private function that does three jobs. It parses the
MCP arguments, it carries out the request, and it writes the result as MCP JSON.

```mermaid
flowchart TD
    lead["Lead agent"]
    subgraph handlers["MCP handlers for exec, wait and cancel, private to coral-mcp"]
        direction LR
        parse["Job 1: parse<br/>MCP JSON to arguments"]
        logic["Job 2: logic<br/>finds the Task<br/>applies limits<br/>submits and waits<br/>maps errors"]
        write["Job 3: write<br/>chooses the Events that fit<br/>result to MCP JSON"]
        parse --> logic --> write
    end
    lead -- MCP tool calls --> handlers
    handlers --> task["Task module"]
    handlers --> run["Run module"]
    handlers --> search["Tool search"]
```

The second job is small. It holds four rules:

- A Run with no time limit gets 15 minutes, and a call that starts a Run waits up to 30 seconds for it.
- The wait is checked before anything starts, so a refused wait starts no Run.
- A Run that started is always reported, even when Coral cannot read it back in the same call.
- Each failure becomes a code and a message for the agent.

The checks that decide what a caller may do are not in the handlers. They are in the Task and Run modules, and every
caller passes through them.

We will leave core's `exec`, `wait` and `cancel` handlers as they are. The Sub-agent's code in `ee/` will call the Task
and Run modules directly, as the handlers do. It will apply the same four rules in its own code, and it will choose its
own wording for each failure.

```mermaid
flowchart TD
    lead["Lead agent"]
    model["The Sub-agent's model"]
    coresurface["Core's MCP handlers<br/>unchanged<br/>jobs 1, 2 and 3"]
    subagent["Sub-agent code in ee/<br/>job 1: parses the tool call<br/>job 2: its own copy of the four rules<br/>job 3: writes its own JSON"]
    lead -- MCP tool calls --> coresurface
    model -- tool calls --> subagent
    coresurface --> task["Task module"]
    coresurface --> run["Run module"]
    coresurface --> search["Tool search"]
    subagent --> task
    subagent --> run
    subagent --> search
```

A Run that a Sub-agent starts passes the same permission checks and the same Policy Enforcement Point as a Run started
over MCP, because both go through the same modules. The wording of a failure joins the rest of what the Sub-agent's
model reads: `ee/` chooses it, so a change to the wording of core's MCP tools does not reach a trained model.

This goes against point 9 in a small way. The four rules exist twice, once in core's handlers and once in `ee/`. A
change to one of them in core is repeated in `ee/` by hand, and the two sets of tools can word the same failure
differently.

**Rejected:**

- **Move the second job into shared functions that both sets of tools call.** We decided this first, and built it as a
  crate with one function for each tool. It keeps one copy of the four rules and of the failure wording. But it needs a
  rewrite of core's MCP handlers in order to share four small rules, and it makes core's failure wording part of what a
  trained model reads.
- **Make core's existing MCP handlers public.** Their argument types define the MCP tool schemas, so a change to an MCP
  tool's arguments would break `ee/`.
- **Have `ee/` call the MCP tools and read fields from their JSON results.** The Sub-agent's model would then read
  core's MCP JSON, and a renamed field would fail only when a call ran.

### 4. The text that the Sub-agent's model reads is written in `ee/`

**Serves point 6.**

A model can be trained by reinforcement learning against the Sub-agent's tools. It learns the exact tool names,
argument names and result fields that it is shown, and it keeps using them after they change.

We will therefore write three things in `ee/`: the system prompt, the definition of each tool the model can call, and
the code that writes each tool result, including the wording of each failure. The Sub-agent's model will never receive
core's MCP instructions or core's MCP tool descriptions, which change to suit outside agents.

When a change in core alters a value that `ee/` reads, `ee/` stops compiling in the same pull request. The author then
updates `ee/` so that the model reads what it read before.

**Rejected:** a checked-in file of hashes of that text, verified in CI. It would only report a change, and the quickest
way to satisfy such a check is to update the file.

### 5. The Sub-agent has its own search tool, and its Programs cannot call Built-in Tools

**Serves point 6.**

Outside agents find Tools by calling the Built-in Tool `coral.search` from inside a Program (ADR 0026), and core
changes `coral.search` to improve how they do it. Search is also the only Tool an agent must know how to call in
advance, because an agent finds every other Tool by searching.

We will give the Sub-agent's model a `search` tool that `ee/` defines. It runs the same search by calling core's search
module directly. We will also refuse any Sub-agent Program that calls a Built-in Tool, including `coral.search`. If
`coral.search` worked inside a Program, a model could learn during training to call it there, and it would depend on
core's names again.

As a result, a Sub-agent cannot add a Provider, change policy or save a Private Tool, even when its caller is permitted
to. The lead does those things itself.

This does not reverse ADR 0026, which rejected serving search through two interfaces and choosing between them by
detecting the client. Core's MCP tools do not change, Coral does not detect the client, and core still has one
implementation of search.

**Rejected:**

- **Add code to the start of each Program that wraps `coral.search`.** The Program recorded for each Run would contain
  code that the model did not write, and the model could still call `coral.search` directly.
- **Forbid changes to `coral.search`.** ADR 0026 depends on changing it.

### 6. The operator configures the model in the config file, as it configures the database

**Serves points 1, 2, 3 and 10.**

We will add a `[sub_agent]` section to the config file that works as `[database]` does. The deployment owns the
section, a `*_env` key names the environment variable that holds the secret, and a change takes effect when Coral
restarts. The presence of the section turns the Sub-agent on. No key enables it separately, and no key sets the
Edition.

The model is any endpoint that implements OpenAI's Chat Completions API. Coral does not check which model answers, and
`ee/` has one model client. Coral does not call the model at startup, so an unreachable model does not stop Coral from
starting.

The settings exist only in the config file. Reef has no form for them, and Coral does not store them in its database.

We will add no reasoning setting, and no limit other than a fixed number of model calls in one Turn. The model server's
own defaults decide whether the model reasons. Limits on running Turns, tokens and duration wait until usage shows what
they should be.

**Rejected:**

- **A default model URL.** A config file that set only a model name would send provider data to a host that nobody
  chose.
- **A second model client for the Anthropic Messages API.** Claude can be reached through an OpenAI-compatible URL.
- **A `reasoning_effort` key.** On vLLM it turns thinking on for a Qwen model but does not keep the model's earlier
  reasoning in view. The model would lose that reasoning whenever the lead sent a follow-up.

### 7. A Turn runs in one Coral Instance and is never resumed

**Serves point 8.**

A Turn is the work a Sub-agent does between receiving one message and sending its response. We will run each Turn as a
background task in the Coral Instance that accepted the `agent` call. The Turn's state is stored in the database under
a lease, so any Coral Instance can answer a `wait` or a `cancel` for it.

We will not resume a Turn on another Coral Instance. A Turn runs with its caller's permissions, and Coral stores no
caller credential (ADR 0029), so another instance has nothing to continue with. When a Coral Instance shuts down, it
marks its Turns as interrupted. The lead then sends the Sub-agent a new message, and a new Turn starts with the stored
conversation.

**Rejected:** resuming an interrupted Turn on another Coral Instance. It would need a way to act as the caller with no
request from the caller, which is a separate design.

### 8. The first release changes nothing in Reef and adds no usage reporting

**Serves points 3, 4 and 5.**

We will ship the Sub-agent with no page and no form in Reef. The Runs that a Sub-agent starts appear in Reef as its
caller's Runs. We will add nothing to the Usage Report, because those Runs are already counted there.

We will test the Sub-agent with Rust tests in `ee/` that set the Edition directly and use a stub in place of the model.
The code will merge before a licence can set the Edition, so the Sub-agent stays off in every deployment until a real
licence turns it on. No test licence, special build or config key turns the Enterprise Edition on.

`ee/` will record a trace and metrics for each Turn through names that core's `coral-telemetry` crate declares. They
contain counts, token numbers and durations, and never text.

## Consequences

**What gets better**

- A change to core's MCP tools no longer changes what the Sub-agent's model reads.
- Core's `exec`, `wait` and `cancel` handlers do not change.
- A deployment with no `[sub_agent]` section, or under the Community Edition, behaves as it does today.
- An operator configures the model the way it already configures the database.

**What we accepted**

- A Workspace Owner cannot turn the Sub-agent off for one Workspace. It is on for the whole deployment.
- A Sub-agent cannot administer Coral, even when its caller can.
- An interrupted Turn does not continue. The lead must send its message again.
- Program diagnostics and Tool declarations still come from core, so a change to them changes what the model reads.
- The four rules behind `exec`, `wait` and `cancel` exist twice, in core's handlers and in `ee/`. A change to one of
  them in core is repeated in `ee/` by hand.
- Coral does not record which version of the prompt and tool definitions a model was trained on. A team that trains a
  model records the Coral release.
- With a hosted API, the operator cannot set the server's defaults, so the model reasons at the provider's default
  level.
- Reef shows no Sub-agents and no Turns.

**What we are not sure of**

- The Sub-agent's own `search` tool has not been measured. ADR 0026 measured search outside Programs as slower for
  outside agents.
- Sailfish, Coral's own model, was trained to call `coral.search` inside a Program. It will take extra steps until it
  is retrained against the Sub-agent's tools.

## How we will confirm it

- A test in `ee/` shows that the lead receives only core's tools under the Community Edition.
- A test in `ee/` shows that, under the Enterprise Edition, a Turn starts a Run in the lead's Task and `wait`
  returns the answer.
- Tests in `ee/` compare the system prompt, the tool definitions and a written result against the exact expected text.
- A developer runs a Turn with several steps and a compaction against Sailfish with reasoning turned on, before the
  Sub-agent is turned on for anyone.

## Related records

- ADR 0033 sets the rules for Enterprise code in `ee/`.
- ADR 0026 moved Tool search into Programs for outside agents.
- ADR 0029 says that Coral keeps no MCP session.
- ADR 0008 sets the operator's destination policy for outbound requests, which the Sub-agent's requests to the model
  follow.
- The tickets of [Decide how Coral's agent ships as an Enterprise
  Feature](https://github.com/withcoral/coral/issues/3198) hold the detail behind each decision.

## Appendix: how it works

This appendix explains how each part of the Sub-agent works. It describes the design at the time of this record, and
the code will later state much of it exactly.

### What the Sub-agent's tools call in core

Each of the Sub-agent's tools is code in `ee/` that calls a core module as the lead's caller:

| The Sub-agent's tool | What its code calls in core                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `exec`               | the Task module, which records the attempt and accepts the Program, then the Run module |
| `wait`               | the Run module, to read a Run after a cursor                                            |
| `cancel`             | the Run module, to cancel a Run and read it                                             |
| `search`             | the search module, for the discovery that the Built-in Tool `coral.search` runs         |

These are the calls that core's own MCP handlers and Built-in Tools make. `ee/` gets no entry point of its own.

`ee/` chooses how much of a Run's output fits in one result, and it chooses the wording of each failure. Where a core
module supplies text that is safe to show, such as a Program's diagnostics, `ee/` includes that text. For a failure
that `ee/` has no wording for, the model reads one general message.

### How the call stack changes

The three listings below follow one `exec` call. Indentation shows which function calls which. Each line is marked with
the job it belongs to: job 1 parses the arguments, job 2 is the logic, and job 3 writes the result. Job 2 is the part
that `ee/` writes for itself.

Today on `main`, the lead calls `exec`:

```text
McpServer::call_tool                     MCP shell: authenticated request, opens the span
  McpServer::dispatch                    picks the handler by tool name
    arguments::parse::<ExecArguments>    job 1: MCP JSON to arguments
    McpServer::exec                      job 2: finds the Task, applies defaults and limits
      commands.submit                    job 2: the Task module accepts the Program
      McpServer::observe                 job 2: the Run module waits for the Run
      projection::run_update             job 3: chooses the Events that fit in one result
                                         job 3: writes the result as MCP JSON
```

Decided, the lead calls `exec`:

```text
MCP shell: call_tool                     unchanged
  tool surface interface: call           the ee/ surface passes exec to the core surface
    core surface: exec                   unchanged: jobs 1, 2 and 3, as in the listing above
```

Decided, the Sub-agent's model calls `exec`:

```text
Turn loop, in ee/
  Sub-agent code: parse                  job 1: the model's tool call to values
  Sub-agent code: check the Program      refuses a Built-in Tool
  Sub-agent code: exec                   job 2: ee/'s own copy of the four rules
    Task module: submit                  the same call that core's handler makes
    Run module: observe
  Sub-agent code: write                  job 3: Run Update to the JSON the model reads
```

The function names in the "today" listing are the real ones. The two "decided" listings describe what each step does,
because that code is not written yet.

### What `ee/` receives at startup

`ee/` registers the factory for its tool surface through `Extensions`, in the same way that it registers a Secret Store
kind. At startup, when the config file has the factory's section, core calls the factory and gives it:

- the `[sub_agent]` section of the config file, as unparsed TOML;
- the database, and the Task, Workspace, Run and search modules;
- the operator's destination policy for outbound requests;
- a signal that fires when the Coral Instance shuts down.

Core does not know the name of the section. If the config file contains a section that no factory claims, startup
fails. A source build therefore rejects a config file that contains `[sub_agent]`.

The Sub-agent's database tables are created by core's migration chain, and their names start with `sub_agent_`. Every
query that reads or writes them is in `ee/`.

### What happens when the Edition changes

The `ee/` surface checks the Edition at the start of every call. While the Edition is Community, it passes the call to
the core surface without changing it. Core contains no Edition check for the Sub-agent.

The Edition can change while Coral runs. When it does:

- A Turn that is already running finishes. A Turn does not check the Edition again after it starts.
- No restart is needed. `ee/` builds its surface at startup under both Editions, so `agent` appears the next time the
  lead lists its tools after a licence sets the Edition to Enterprise.
- No Sub-agent data is deleted when the Edition becomes Community.

### What the Sub-agent's model reads, and where it is written

| What the Sub-agent's model reads | Where it is written | Learned in training |
| --- | --- | --- |
| The system prompt | `ee/` | Yes |
| The name, description and argument schema of `search`, `exec`, `wait` and `cancel` | `ee/` | Yes |
| The field names of each tool result | `ee/` | Yes |
| The declaration of a Provider, Private or Shared Tool, returned by `search` | core | No. The model reads it each time it searches. |
| The wording of each failure | `ee/` | No. The model reads each one when it occurs. |
| Program diagnostics, and other text that a core module marks as safe to show | core | No. The model reads each one when it occurs. |

The first version of the text in `ee/` starts from what core serves today:

- The descriptions of `exec`, `wait` and `cancel` are copies of core's current descriptions, with three parts removed:
  the `goal` argument, the instruction to show a required action's URL to the user, and the paragraph about
  `coral.search`.
- Each tool result uses the same field names as core's MCP result for the same tool.
- A tool result longer than 20 KiB is shortened to its first 10 KiB and its last 10 KiB.

### What a Sub-agent's Program may call

Before `ee/` starts a Run, it checks the text of the Program for the Tools it calls:

| The Program calls | What happens |
| --- | --- |
| A Provider Tool (`coral.providers.…`) | Allowed |
| A Private Tool (`coral.private.…`) | Allowed |
| A Shared Tool (`coral.shared.…`) | Allowed |
| `coral.emit` or `coral.ToolError` | Allowed |
| A Built-in Tool, such as `coral.search`, `coral.addCustomTool`, `coral.addProvider` or `coral.updatePolicy` | The Program is not run. The model receives an error that names the Tool. For `coral.search`, the error tells it to call `search`. |

A Built-in Tool that core adds later is refused in the same way, until someone changes `ee/` to allow it. A Sub-agent
can still call a Provider Tool that writes data, such as one that posts a comment, when its caller's policy allows it.

The check finds Call Paths that are written out in the Program. It does not find a Call Path that the Program builds
while it runs.

### How a change in core affects the Sub-agent's model

| Change in core | Effect on the Sub-agent's model |
| --- | --- |
| An MCP tool's name, description, argument schema or result field changes, or an MCP tool or argument is added | None. `ee/` does not use them. |
| The arguments or result fields of `coral.search` change | None. If the change alters a Rust type that `ee/` uses, `ee/` stops compiling, and the author updates `ee/` so that the model's `search` tool stays the same. |
| A Built-in Tool is added | None. A Sub-agent's Program cannot call it, and `search` does not return it. |
| A field is added to a Run Update | None, until someone changes `ee/` to include it in a result. |
| A call that `ee/` makes on the Task, Run or search module gains an input | `ee/` stops compiling. The author decides what value `ee/` passes. |
| A value that `ee/` reads is removed, or its type changes | `ee/` stops compiling. The author updates `ee/`. |
| A Run state, Event kind or required-action kind is added | `ee/` stops compiling. The author writes the text the model reads for it, or leaves that Event out of results. |
| The wording of a failure in core's MCP tools changes | None. `ee/` chooses its own wording. |
| A kind of failure is added in core | The model reads `ee/`'s general message for it, until someone gives it its own wording in `ee/`. |
| A Program diagnostic or a Tool declaration changes | The model reads the new text. |

`ee/` handles every Run state, Event kind and required-action kind by name and has no default case. That is why adding
one stops `ee/` compiling, in the same pull request that adds it.

### The `[sub_agent]` section of the config file

```toml
[sub_agent]
base_url = "https://sailfish.example/v1"   # required, no default
model = "sailfish-1"                        # required
api_key_env = "CORAL_SUB_AGENT_API_KEY"     # optional: the environment variable that holds the token
context_window = 131072                     # optional: the model's context window, in tokens
```

- `api_key_env` names an environment variable, as every other secret in the config file does. If it is set and the
  variable is missing, startup fails. If it is not set, Coral sends no `Authorization` header.
- Requests to the model go through `coral-outbound` and follow the operator's `[outbound]` destination policy, with no
  exception for the Sub-agent. A model on loopback or on a private network must be allowed there.
- The Sub-agent is on for every Workspace in the deployment.
- The party that runs the deployment writes the section. For a self-hosted deployment, that is the operator. For a SaaS
  deployment, it is Coral's control plane.

### What the model server must do

`ee/` has one model client, built and tested against vLLM. Other servers that implement Chat Completions and return
tool calls in the `tool_calls` field should work, but they are not tested.

| Requirement | Reason | vLLM flags |
| --- | --- | --- |
| Return tool calls in the `tool_calls` field of the reply | Coral does not parse tool calls out of the reply's text. | `--enable-auto-tool-choice` and `--tool-call-parser` |
| For a reasoning model, return reasoning in the `reasoning` or `reasoning_content` field of the reply | Coral does not parse `<think>` tags out of the reply's text. Without this, Coral treats the reasoning as part of the Sub-agent's answer and can show it to the lead. | `--reasoning-parser` |
| Turn reasoning on or off in the server's own defaults | Coral sends no reasoning setting in its requests. | `--default-chat-template-kwargs` |

Coral stores each Sub-agent's conversation in its own format, as a list of typed blocks: text, a tool call, a tool
result with an error flag, reasoning, and a compaction summary. The client converts between this format and Chat
Completions messages on every request. Chat Completions messages are not used for storage because they cannot hold a
tool result's error flag, reasoning or a compaction summary.

Coral keeps reasoning in the stored conversation. It sends reasoning back to the model only when the model that wrote
it is the model named in the config file. It never includes reasoning in the response to the lead.

### The steps of one Turn

When the lead calls `agent`, the Coral Instance that receives the call starts the Turn as a background task and returns
the `sub_agent_id`. The lead then calls `wait` with that `sub_agent_id` to read the Sub-agent's progress and its
answer.

During a Turn, the Sub-agent repeats two steps. It sends the conversation to the model. It then runs the tool calls in
the model's reply and adds their results to the conversation. The Turn ends when the model replies without a tool
call, and that reply is the Sub-agent's answer.

```mermaid
flowchart TD
    ask["The lead calls agent<br/>with a message"]
    start["Coral starts the Turn<br/>in the background and<br/>returns the sub_agent_id"]
    send["The Turn sends the<br/>conversation to the model"]
    tools{"Tool calls<br/>in the reply?"}
    run["The Turn runs each tool call<br/>and adds the results"]
    answer["The reply is the answer.<br/>The Turn ends."]
    wait["The lead calls wait with<br/>the sub_agent_id and reads<br/>the steps and the answer"]
    ask --> start --> send --> tools
    tools -- yes --> run
    run -- next model call --> send
    tools -- no --> answer --> wait
```

| Situation | What happens |
| --- | --- |
| The lead calls `cancel` with the `sub_agent_id`, on any Coral Instance | The call records the cancellation in the database. The Turn stops when it next renews its lease, and the Coral Instance that runs it cancels the Runs it started that have not ended. |
| The lead ends the Task | A running Turn in it stops as canceled, at its next model call or when it next renews its lease, and its unfinished Runs are cancelled. |
| The lead sends another message while the Turn is running | The Sub-agent reads the message before its next model call. |
| A Run that the Turn started waits for a person to approve a Tool call or connect an account | `wait` returns `needs_action` with that Run's required actions, and the lead shows them to the user. |
| The conversation comes close to the model's context window | The Sub-agent asks the model to summarize the older messages. It continues with the summary and the most recent messages. |
| The model server returns HTTP 429 or a 5xx status, or the request fails to send | The client retries three times. If the request still fails, the Turn fails with the model server's error, and the lead can send the message again. |
| The Turn makes 60 model calls without finishing | The Turn fails. The error tells the lead to send a narrower message. |
| The Coral Instance shuts down | It marks its running Turns as interrupted before it exits. |
| The Coral Instance stops without shutting down | The Turn's lease in the database expires. The next request that reads the Turn marks it as interrupted. |

A Run that an interrupted or failed Turn started continues under its Run Owner. Only a canceled Turn cancels its Runs.
A Turn itself does not use a Run Owner or a Run Executor, because those run Programs. A Turn only calls the model and
core's Task, Run and search modules.

### What telemetry records

Each Turn produces one trace. The root span of that trace links to the span of the `agent` call that started the Turn.
Inside the trace there is one span for each model call, each tool call and each compaction. Each Run that the Turn
starts has its own Run Trace, which links to the tool-call span that started it.

| Signal | What it contains |
| --- | --- |
| Turn span | the Workspace, the Principal, the Task, the `sub_agent_id`, how the Turn ended, and the number of model calls, tool calls and compactions |
| Model-call span | the model name from the config file, the model host, token counts, the finish reason, the number of retries, and the HTTP status of a failed call |
| Tool-call span | the tool name, how the call ended, and the `run_id` if it started a Run |
| Compaction span | the token count before and after |
| Metrics named `coral.sub_agent.*` | Turns ended, Turn duration, Turns running, model calls per Turn, model-call duration, tokens per model call, and compactions |

No span or metric contains a message, a prompt, tool arguments, a tool result, the model's text, the model's reasoning
or the model server's error text. No metric has the model name, a Workspace or an ID as an attribute.

The names of these fields and metrics are declared in core's `coral-telemetry` crate, and `ee/` records values through
them. A test in `ee/` runs core's telemetry source check over the code in `ee/`.

This sets a rule for later Enterprise Features:

- If the feature's work passes through an interface that core owns, core records the telemetry at that interface, and
  `ee/` records nothing. Reads from a Secret Store already work this way.
- `ee/` adds names to core's `coral-telemetry` crate only for work that happens entirely inside `ee/`.
- If Enterprise names become most of that crate's catalog, they move to a catalog that `ee/` owns.

### The order of rollout

1. The code merges to `main` before a licence can set the Edition. Every deployment still runs as the Community
   Edition, so the Sub-agent is off everywhere.
2. A developer runs a Turn with several steps and a compaction against Sailfish with reasoning turned on. This checks
   the model's chat template and the server's parsers, which a stub cannot check. The developer uses a local build that
   is changed to start as the Enterprise Edition, and does not commit that change.
3. The Coral team's self-hosted deployment turns the Sub-agent on, with a real licence that Coral issues to itself and
   a `[sub_agent]` section that points at Sailfish.
4. SaaS deployments get the Sub-agent when Coral's control plane can write the section and the token. That work is
   outside this repository.

The Sub-agent has no Reel scenario. It changes nothing in Reef, and Reel runs the released binary, which runs as the
Community Edition until a licence is installed.
