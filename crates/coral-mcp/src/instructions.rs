//! Server `instructions`: the agent workflow every Coral session needs first.
//!
//! Claude Code keeps only the first 2048 characters of server instructions,
//! so the text stays within that budget. Agents learn discovery from
//! `exec`'s example, the types and the errors; the rest of the text is
//! kept as it was before discovery moved into Programs.

/// Instructions sent in `InitializeResult` and `DiscoverResult`.
pub(crate) const INSTRUCTIONS: &str = "\
Coral runs TypeScript Programs that call the user's connected providers.

## Workflow
1. Pass `goal` to the first `exec` of a unit of work, then its `task_id` to later calls.
2. Find provider tools and Saved Functions with `coral.workspace.search` in a Program; `exec` shows how.
3. Call `exec` with one TypeScript async-function body. Each `exec` starts a new Run and can repeat external effects.
4. While `state` is `running` or `has_more` is true, call `wait` with the same `run_id` and the latest `next_cursor`. The last page carries the Final Value or failure.
5. When the answering Run succeeds and other values can reuse its work (several calls, paging, joins, filters), save it with `save_function`. Skip one call, one-off writes and exploration. Tell the user its path.
6. When the work is done or cannot continue, call `end_task` once with the outcome.

## Program rules
- Write each call as the complete literal Call Path that search returns, such as `coral.providers.github.getRepo(...)`. Keep `coral` and its sub-paths out of variables.
- Only provider tools reach the network; `fetch`, `setTimeout` and other ambient APIs are absent.
- Keep credentials and account choice out of Program source; Coral supplies both.
- Publish interim output with `await coral.emit(value)`. The Program's return value is the Final Value.
- Error responses throw `coral.ToolError` with their `data`. Coral never pages or retries: follow continuation data.

## Results
- `isError: true` means Coral rejected the call. Fix what `code` and `message` name, then call again.
- Report a failed, canceled, timed-out or interrupted Run to the user as its outcome.

## Human handoff
A non-empty `required_actions` means a person must act before the Run goes on.
- Show the user the full `run_status_url`. Ask the person that `required_actor` names to act before `deadline_at`.
- For `account_connection`, ask only when this action appears.
- After the person acts, call `wait` for the same Run. Keep the Task open until it ends.
";

// Clients add instructions to every turn; Claude Code drops what follows 2048.
const _: () = assert!(INSTRUCTIONS.len() <= 2048);
