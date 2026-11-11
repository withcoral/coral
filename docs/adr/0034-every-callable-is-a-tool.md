---
status: proposed
---

# Every callable is a Tool: Private and Shared Tools are Invocations that Coral performs

A Program calls three kinds of Tool, and every Tool call has one contract: it is asynchronous, takes one object of
data and returns data, is one Tool Invocation, and has a Tool Access State.

- A **Provider Tool** is at `coral.providers.<namespace>.<path>`. The Built-in Provider's Tools now sit directly on
  `coral`: `coral.search`, `coral.addProvider`, `coral.addTool`.
- A **Private Tool** is what a Saved Function was. It is at `coral.private.<path>` and belongs to the Principal who
  saved it.
- A **Shared Tool** is at `coral.shared.<path>`. A Workspace Owner publishes it from a Private Tool, and the Workspace
  owns it.

Private Tools belong to a Private Provider and Shared Tools to a Shared Provider. Each Workspace has one of each,
always present, and Coral performs their Tools, as it does the Built-in Provider's. So both kinds reuse the policy,
approvals, Invocations, search and descriptions that Provider Tools already have.

We wanted Members to reuse each other's work without a second governance model. We also wanted sharing to change a call
site's prefix and nothing else. A tool that takes callbacks privately but data publicly would break every caller at the
moment it is shared. Function Grants also need a tool that can run in its own isolated context, so only data may cross
a call.

## Decisions

- **Roots and verbs.** `coral.functions` becomes `coral.private`, and `coral.workspace.*` moves onto `coral`.
  `coral.providers` keeps its name.
  - There are no aliases and no pointers. An old path fails at admission like any path that names no Tool. Retained
    Runs keep the paths they recorded, and Reef reads them under the new roots.
  - `saveFunction` and `deleteFunction` become `coral.addTool` (create only), `coral.updateTool` (replace only, with an
    optional expected revision) and `coral.removeTool`.
  - `addTool` and `updateTool` take an optional `policy`, the tool's default Tool Access State. It is `enabled` when
    `addTool` omits it, and an update that omits it keeps the current one. Owner rules override it, as for any Tool.
- **A Private Tool call is an Invocation.** Each call crosses to Coral and back, so the call is always asynchronous and
  takes and returns only data.
  - **One object in.** A Private or Shared Tool takes one argument, an object, as a Provider Tool does. Its default
    export declares at most one parameter, and that parameter's type is an object; a save or publish refuses anything
    else. A call with no argument passes `{}`. Field paths start at `input`, whatever the parameter is called.
  - **JSON data only, never converted.** Arguments and results hold only `null`, booleans, finite numbers, strings,
    arrays and plain objects. Coral refuses anything else and names where it is: a function, a symbol, a `bigint`,
    `NaN` or an infinity, a `Date`, `Map`, `Set` or other class instance, or a cycle. A call refuses bad arguments
    with `invalid_input` before it becomes an Invocation, and a body that returns a bad value ends its call with
    `invalid_tool_result`. A property set to `undefined` is absent, and an `undefined` array element is `null`.
  - The Tool calls its body makes are child Invocations linked to it. The parent ends only after its children, and Coral
    refuses a child call made after its parent ended.
  - A throw in the body reaches the caller as a Tool Error with code `tool_failed`. A Tool Error from a nested call
    keeps its own code.
  - Tool calls made by top-level module code belong to the module, sit under no parent Invocation, and never receive a
    grant.
- **Private Tool policy.** Each Principal sees only their own part of the Private Provider. A Workspace Owner can
  disable the whole Private Provider. An Owner can also set exact rules on the Private Tools they can read: their own and
  every Agent's. A Member's Private Tool uses its declared default, because Owners cannot read Members' private work.
- **Programs read and set policy by complete Call Path.** One read returns every rule that applies to a Tool in
  every root: every Provider's Tools, the Built-in and Shared Tools, and the caller's own Private Tools. An Owner's
  Program can set rules on any Provider's Tools, the Shared Tools and the Owner's own Private Tools, in one batch. It
  never sets a Built-in Tool's rule, so a Program cannot loosen the approval on the Tools that change the Workspace.
  An Owner reads and sets the rules on an Agent's Private Tools only in Reef, where the Owner can name whose tool it
  is.
- **Shared Tools.**
  - **Publishing:** only a Workspace Owner publishes, and only in Reef. The Owner can publish their own Private Tool or
    an Agent's, never a Member's. Publishing copies the tool; the Private Tool stays as it was.
  - **Revisions:** each publish is an immutable, numbered revision, and the newest one is current. The publish request
    carries the exact source and description the Owner reviewed, plus the revision the Owner expects to be current. So
    Coral never publishes content that nobody reviewed.
  - **Callers:** every Run links the current revision and records which one. Callers cannot pin a revision.
  - **Policy at publish:** the Owner chooses the Shared Tool's default Tool Access State.
  - **Running:** a Shared Tool runs with the caller's authority, policy and Provider Accounts, and returns its own value
    rather than a Tool Result. Its body may call any Tool. Inside it, `coral.private.x` is the caller's `x`.
  - **Ownership:** the author is attribution only. Removing, disabling or deleting the author leaves the Shared Tool
    unchanged.
  - **Removal:** only an Owner removes a Shared Tool. Removal frees the path and keeps the revisions. It is refused while
    another Shared Tool's current revision calls the tool.
  - **Visibility:** every Member and Agent can read a Shared Tool's source and current revision. Every Member User can
    read its history; Agents cannot.
- **Function Grants come later, on this shape.** An Owner will be able to grant any tool the Owner can read: their own
  Private Tools, Agents' Private Tools and every Shared Tool. Grants need the tool to run in an isolated context, with a
  JSON-only bridge and an origin bound to the call. This ADR keeps that path open:
  - every boundary is data-only;
  - nested calls carry their parent Invocation, never a stack frame;
  - a Run pins each revision at admission.
- **Reef shows one "Tools" page.** It replaces "Providers" and "Functions" and groups Coral, Shared, each Provider, and
  Private. "Provider" still names a connected service.

## Considered options

- **A Workspace library of shared functions at `coral.library`, with private functions unchanged.** It would ship
  faster, but it kept two contracts and two governance models. Sharing a function would also have changed how callers
  call it.
- **Shared functions as Tools, and private functions as plain linked functions.** We measured the cost first. A plain
  linked function can be synchronous, aliased or passed as a callback, so it has no single call boundary to record. The
  product owner chose consistency, so policy and grants apply to every tool.
- **Shared Tools inside the Built-in Provider.** Coral replaces that catalog on every boot, and nobody may reconfigure
  it. Owner-published Tools would have given one catalog two writers.
- **`coral.tools` or `coral.library` as the shared root, `coral.mine` and `coral.ours`, or renaming `coral.providers`.**
  `private` and `shared` name the scope, which is how people describe the act of sharing.
- **Positional parameters, as Saved Functions took.** They kept every existing Saved Function working, but a tool
  would take a different argument shape from a Provider Tool. Its Invocation would record an array, and its field
  paths would need a root of their own.
- **Copying as `JSON.stringify` does.** It needs no new code and matches what an isolated tool would receive, but it
  loses data silently: a `Map` arrives as `{}`, a function disappears and `NaN` becomes `null`. A refusal that names
  the field is cheaper for an agent to fix than wrong output it never notices.
- **Permanent aliases for the old roots, or refusals that name the new path.** Either is code that Coral keeps for
  names no Program should still use.
- **Banning some calls from Shared Tool bodies:** the authoring Tools, Owner-only Built-ins, Private Tools, `globalThis`
  and computed paths. Every call inside a Shared Tool already runs under the caller's authority and policy, and only
  Owners publish. A ban would surprise authors and add no protection that policy does not already give.
- **Members publishing, or a proposal queue, in v1.** Owner-only publishing adds no new trust relationship, and
  loosening it later is additive.

## Consequences

- **Every Private Tool call is asynchronous, and its arguments and results are copied as data.** We checked 232
  distinct Saved Function sources from local development and evaluation databases: all are already asynchronous, none
  takes a callback or returns a function, a class instance or a `Date`, and one references another function. An
  `undefined` array element now arrives as `null`. A caller now catches a `ToolError`, not the original error object.
- **A Saved Function with positional parameters stops working.** Admission refuses a Program that links one, naming
  the tool, until its author saves it again with one object parameter. A Shared Tool revision published that way
  needs a new revision. A caller that passed a `Date` or a `Map` now converts it first, for example with
  `toISOString()` or `Object.fromEntries()`.
- **A Tool Invocation can perform no dispatch and have children.** This needs:
  - a parent link in the Invocation schema;
  - a call that resolves when it is permitted, then completes when its body ends;
  - an outer call that does not hold a Fan-out Limit slot while its children wait for one;
  - Run Executor messages that permit and complete a code-performed call.

  Add these messages to the Run stream design before it ships.
- **Run history grows.** Every Private Tool call is now a recorded Invocation, including calls in loops.
- **Saved Function rows become Private Tools of the Private Provider.** Sources that still name `coral.functions` or
  `coral.workspace` fail at their next admission, like any path that names no Tool.
- **Block keeps its meaning until grants ship.** When grants ship, Reef's wording for `disabled` must change in the
  same release. It will mean "blocked for direct calls".
- This ADR amends [ADR 0020](0020-saved-functions-are-typed-from-the-callers-declarations.md) (the roots that
  typing reads), [ADR 0026](0026-agents-discover-tools-inside-programs.md) (`coral.search`) and
  [ADR 0031](0031-agents-save-functions-inside-programs.md) (the authoring Tools and their names).
