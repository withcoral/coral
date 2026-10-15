# Workspace middleware exploration

Status: design interview complete; user confirmed shared understanding. Proceed to implementation planning. No implementation, commit, push, or PR authorized by that confirmation. Keep this exploration document only; do not add an ADR.

## Confirmed direction

- Workspace-level Koa-style around-invocation interceptor chain.
- Middleware can apply to Program Runs, Saved Function calls, and Operation Invocations. Workspace UI has one scope checkbox for each level.
- Middleware can inspect and modify arguments, approve or decline execution, and inspect, transform, redact, or withhold results. Withholding a result does not undo external effects.
- V1 Middleware may call ordinary Coral Provider Operations, including JEV inference and Sentry integration, but may not reference Saved Functions. Example call paths from the discussion were illustrative, not verified APIs.
- Middleware is trusted Workspace infrastructure. Its helper calls, including transitive helper calls, bypass Workspace middleware and invocation policies. Tenant isolation, Provider authorization, and native audit logging remain.
- The continuation resumes the original invocation without granting it the infrastructure bypass.
- There are mutable Run, Saved Function, and Operation context scopes. An operation-count middleware should support counts across the Run and inside/outside Saved Functions.
- Add a save_middleware MCP authoring tool analogous to save_function. Every successful creation or update leaves the Middleware disabled. Failed saves must not replace the accepted definition.
- Clicking a Middleware in the workspace UI opens a modal displaying its source.

## Examples

- Sentry: observe invocation execution, timing, failures, and policy outcomes.
- JEV: evaluate inputs before execution and outputs before returning them.
- Operation counter: count Operation Invocations in a Run and within/outside Saved Function calls.

## Settled interview decisions

- Middleware is a separate Workspace-owned resource that reuses TypeScript authoring machinery, not an attachment to a Principal's private Saved Function.
- Middleware can explicitly override Workspace invocation policy for the original call, including approval-required and disabled Operation states. Platform and Provider boundaries remain distinct.
- Returning/withholding is ordinary onion behavior, not a special JEV-specific security feature. Activity shows each Middleware execution's final value and Operations executed by Middleware. Do not add a special raw-versus-filtered-result comparison view. No new privileged-result reader ACL was requested; exact persistence of intermediate wrapped return values remains to be clarified.
- Caller-routed helper Operations use the original Invoking Principal's Provider Account.
- Context stores are isolated by Middleware identity as well as invocation scope: (Middleware, Run), (Middleware, Saved Function call), and (Middleware, Operation Invocation). Enter/exit share the same scope instance. Three calls of the same Operation have three Operation contexts, while that Middleware's Run context is shared across the Run.
- For a given Middleware, Saved Function and Operation interception can read/write its current Run context; Operation interception within a Saved Function can also read/write its nearest Saved Function context. Different Middleware do not share these stores.
- Context scopes exist regardless of interception toggles: toggles select execution points, not context availability. Operation-only Middleware still has its own Run and nearest Saved Function contexts.
- Contexts are ordinary mutable JavaScript objects with author-defined contents. Counting is only an example, not built-in behavior. Middleware authors coordinate updates across asynchronous interleavings; no runtime atomic-store API is required.
- Middleware Activity follows the enclosing Run's existing visibility and retention rules, showing Middleware final return values and helper Operation records. There is no new privileged audit view.
- Only Workspace Owners may create, update, activate, deactivate, reorder, or delete Middleware.
- Each Run pins the Middleware chain, source, scopes, and order at admission. Later edits affect later Runs. V1 Middleware cannot reference Saved Functions, so there is no Middleware helper-function dependency closure to pin.
- Unhandled Middleware exceptions/timeouts fail the invocation closed. Middleware may explicitly handle its own helper errors.
- Middleware can access only the current Run context, the current/nearest Saved Function context, and the current Operation context where applicable. With A -> B -> Operation, Operation Middleware sees B's context, not A's. There is no automatic ancestor-context exposure or inclusive counting. Context propagation up/down is authored Middleware behavior; its exact API remains open.

- The continuation may be called zero or one time. Middleware can return a replacement value without executing the inner invocation. Repeated continuation calls are rejected.
- If the Operation succeeds and Middleware subsequently fails, the caller receives the Middleware error; Activity preserves the successful Operation and failed Middleware separately. No automatic retry or rollback.
- Every successful edit, including description, source, configuration, scopes, or order, disables the changed Middleware until an Owner re-enables it. In-progress Runs keep their pinned chain.
- Continuing and overriding policy are distinct: next() alone preserves current policy; an explicit override changes it. Later Middleware sees both the immutable original policy and the current effective policy and may change the latter again. Passing the chain onward does not implicitly approve. Exact API names remain illustrative.
- Policy uses existing Operation Access States: enabled, approval_required, disabled. Setting current policy to disabled does not short-circuit; later Middleware may change it again. Policy is enforced at the center of the onion immediately before execution. Throwing is the separate immediate-stop mechanism (subject to ordinary outer error handling).
- Policy overrides affect only the wrapped invocation, not nested Run/Function/Operation invocations. Nested business calls have their own middleware and policy checks.

- Authoring API exposes mutable ctx.args and ctx.currentPolicy, immutable ctx.originalPolicy, and parameterless next(). Downstream Middleware sees edits; return values flow outward normally.
- Outer Middleware may catch policy-denial errors and return fallbacks without executing the denied invocation.
- Human approval occurs at the center after enter-phase Middleware, against final modified arguments. Approval binds to those exact arguments and does not rerun Middleware or duplicate execution.

- Saved Function calls intercepted by enabled Middleware become async; callers must await them. Unintercepted synchronous calls retain existing behavior.
- Run Middleware wraps all linked-module initialization and the submitted Program. Saved Function Middleware wraps exported callable execution only. Operations directly in module initialization have Run context but no Saved Function context; actual Function calls during initialization get normal call scopes.
- originalPolicy/currentPolicy exist only for Operation interception. Runs and Saved Functions have no new policy states or approval feature; their Middleware can throw or short-circuit.
- Workspace Operation policies are frozen for each Run, alongside Saved Function sources and Middleware configuration. This is a deliberate change from this checkout's live policy rechecks (crates/coral-run/src/invocation.rs:818-828, 921-962). Original policy comes from the Run snapshot; Middleware may modify it per Operation Invocation. Live authority, Provider availability, and credential authorization checks remain distinct.

- V1 source modal is read-only. Code authoring uses save_middleware; Owners manage activation, scopes, and order in the table.
- Middleware modules cannot perform top-level effects; Provider calls belong inside the exported Middleware function.
- Middleware/helper work shares the enclosing Run's deadline, cancellation, and resource limits. Policy bypass is not a limits bypass, and cancellation does not initiate new cleanup Operations.
- Middleware Activity appears as separate top-level entries, not a nested tree under the wrapped invocation. Each execution has one row with enter/exit phase and status indicators, not two phase rows.
- ctx.args is a positional array for Saved Function calls and the normal input object for Operations. Run Middleware receives immutable submitted Program metadata rather than editable source.
- Workspace table order is outermost-to-innermost; provide accessible move-up/move-down controls. Moving an item disables the moved Middleware; shifted neighbors stay enabled.

## Interview outcome

Shared understanding confirmed. Keep decisions in this exploration document; no ADR requested. Next phase is implementation planning.

## Implementation constraints

- Finalize exported TypeScript declarations consistent with the settled context scopes and argument shapes.
- Validate final Operation arguments at the center, before approval/dispatch; never execute malformed rewritten inputs.
- Enforce no top-level effects and no Saved Function references in V1 Middleware.
- Ensure infrastructure capability lifetime cannot leak into the wrapped continuation or ordinary Program.
- Preserve shared Run limits and account authorization, and present Activity as the agreed flat phase/status rows.

## Compatibility and implementation facts

- Saved Functions remain Principal-owned/private; Middleware is a separate Workspace-owned resource. V1 forbids Saved Function references in Middleware.
- Invocation Approval currently means a human decision. Middleware changes Operation Access State before that existing approval gate rather than impersonating a human approver.
- Saved Functions are currently linked ordinary JavaScript callables without a separate durable invocation record. Wrappers must cover aliases as well as direct calls; intercepted calls become async by explicit design decision.
- Saved Function Middleware preserves ordinary JavaScript arguments/results, including non-JSON values. Activity uses bounded non-executing previews where faithful serialization is impossible.
- Middleware must await or return next(); finishing while the continuation is still running is an explicit error, not supported detached execution. Already-started effects cannot be assumed undone.

Assistant suggestions from earlier discussion are not settled unless explicitly confirmed by the user. Update this document as interview decisions settle; add glossary definitions when terminology is resolved, and propose ADRs only for consequential trade-offs.

## Design update: `coral.emit` is an intercepted Operation

User decision, recorded after V1 implementation started. It supersedes any assumption that only Provider calls are Operations for interception.

- A Program or Saved Function call to `coral.emit` is an Operation-level invocation. Enabled Operation Middleware wraps it.
- Each wrapping Middleware gets its own Operation context for that emit, plus its current Run context and nearest Saved Function context.
- Middleware can inspect and modify the emitted payload. Through ordinary onion semantics it can block the emit or replace its result.
- `coral.emit` must not bypass output-control Middleware.
- A Middleware-originated `coral.emit` follows the existing trusted infrastructure bypass. It is not intercepted, but it stays attributed to its Middleware execution and observable.

### Implications found during implementation

- V1 first forbade `coral.emit` in Middleware. Middleware now has its own `coral.emit`, which carries the trusted bypass and records its Middleware execution on the Emission Event.
- An unintercepted Emission becomes durable when it is called. Intercepted emits run asynchronously through the chain, so the runtime queues them to keep the Program's call order.
- Payload validation (Program Value and size limits) moves to the center of the onion, after Middleware edits.
- Emits during linked-module initialization have a Run context but no Saved Function context, as Operations do.

### Settled emit decisions (confirmed through Hermes)

- `coral.emit` has no `originalPolicy` or `currentPolicy`. Middleware blocks an emit by throwing or by not calling `next()`.
- An emit shows Middleware `ctx.kind === "operation"`, `ctx.path === "coral.emit"`, and `ctx.args` as the emitted value, which Middleware can edit. The Program's `coral.emit` Promise still resolves to `undefined`, even when Middleware returns a replacement value.
- Program emits keep their call order, including emits the Program does not await.
- Middleware-originated emits bypass interception. They appear in Run Updates (MCP `wait`) and in Activity, attributed to their Middleware execution.
