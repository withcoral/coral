//! Server `instructions`: the agent workflow every Coral session needs first.
//!
//! Claude Code keeps only the first 2048 characters of server instructions,
//! so the text stays within that budget. It holds only the workflow steps:
//! tool-specific detail belongs in the tool's own description, and agents
//! learn discovery from `exec`'s example, the types and the errors.

/// Instructions sent in `InitializeResult` and `DiscoverResult`.
pub(crate) const INSTRUCTIONS: &str = "\
Coral runs TypeScript Programs that call the user's connected providers.

## Workflow
1. Call `start_task` once per unit of user work. Pass its `task_id` to every later call.
2. Find provider tools and Saved Functions with `coral.workspace.search` in a Program; `exec` shows how.
3. Call `exec` with one TypeScript async-function body. Write each call as its complete literal Call Path, such as `coral.providers.github.getRepo(...)`. Coral supplies credentials and accounts. Only provider tools reach the network; `fetch` is absent. Each `exec` starts a new Run and can repeat external effects.
4. While `state` is `running` or `has_more` is true, call `wait` with the same `run_id` and the latest `next_cursor`. The last page carries the Final Value or failure. Report a failed, canceled, timed-out or interrupted Run to the user as its outcome.
5. A non-empty `required_actions` means a person must act. Show the user the full `run_status_url`, ask the person that `required_actor` names to act before `deadline_at`, then call `wait` for the same Run. Ask for an `account_connection` only when one appears.
6. After the answering Run succeeds, call `save_function` unless its Program made one provider tool call. Use the request's values as inputs, a path like `github.pulls.listStale`, and search words in the description. Tell the user its path.
7. When the work is done or cannot continue, call `end_task` once with the outcome.
";

// Clients add instructions to every turn; Claude Code drops what follows 2048.
const _: () = assert!(INSTRUCTIONS.len() <= 2048);
