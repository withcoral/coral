//! Server `instructions`: the agent workflow every Coral session needs first.
//!
//! Claude Code keeps only the first 2048 characters of server instructions,
//! so the text stays within that budget. Tool-specific detail belongs in the
//! tool's own description. Both variants share every section but the workflow.

macro_rules! introduction {
    () => {
        "Coral runs TypeScript Programs that call the user's connected providers.\n\n"
    };
}

macro_rules! rules {
    () => {
        "\
## Program rules
- Write each call as the complete literal Call Path that `search` returns, such as `coral.providers.github.getRepo(...)`. Keep `coral` and its sub-paths out of variables.
- Only Operations reach the network; `fetch`, `setTimeout` and other ambient APIs are absent.
- Keep credentials and account choice out of Program source; Coral supplies both.
- Publish interim output with `await coral.emit(value)`. The Program's return value is the Final Value.
- Error responses throw `coral.OperationError` with their `data`. Coral never pages or retries: follow continuation data.

## Results
- `isError: true` means Coral rejected the call. Fix what `code` and `message` name, then call again.
- Report a failed, canceled, timed-out or interrupted Run to the user as its outcome.

## Human handoff
A non-empty `required_actions` means a person must act before the Run goes on.
- Show the user the full `run_status_url`. Ask the person that `required_actor` names to act before `deadline_at`.
- For `account_connection`, ask only when this action appears.
- After the person acts, call `wait` for the same Run. Keep the Task open until it ends.
"
    };
}

/// Instructions sent in `InitializeResult` and `DiscoverResult`.
pub(crate) const INSTRUCTIONS: &str = concat!(
    introduction!(),
    "\
## Workflow
1. Call `start_task` once per unit of user work. Pass its `task_id` to every later call.
2. Call `search` to find Operations and Saved Functions, and `describe` for a Call Path's declaration.
3. Call `exec` with one TypeScript async-function body. Each `exec` starts a new Run and can repeat external effects.
4. While `state` is `running` or `has_more` is true, call `wait` with the same `run_id` and the latest `next_cursor`. The last page carries the Final Value or failure.
5. After the answering Run succeeds, call `save_function` unless its Program made one Operation call. Tell the user its path.
6. When the work is done or cannot continue, call `end_task` once with the outcome.

",
    rules!()
);

/// Instructions when Coral's own agent is available through `agent`.
pub(crate) const AGENT_INSTRUCTIONS: &str = concat!(
    introduction!(),
    "\
## Workflow
1. Call `start_task` once per unit of user work. Pass its `task_id` to every later call.
2. For data work you can state in one message, call `agent`, then `wait` on its `turn_id`. Its `final` message answers or asks one question; reply with `agent`. Go to step 5.
3. Otherwise, call `search` and `describe`, then `exec` one TypeScript async-function body. Each `exec` can repeat external effects. While `state` is `running` or `has_more` is true, call `wait` with the `run_id` and latest `next_cursor`.
4. After the answering Run succeeds, call `save_function` unless its Program made one Operation call. Tell the user its path.
5. When the work is done or cannot continue, call `end_task` once with the outcome.

",
    rules!()
);

// Clients add instructions to every turn; Claude Code drops what follows 2048.
const _: () = assert!(INSTRUCTIONS.len() <= 2048);
const _: () = assert!(AGENT_INSTRUCTIONS.len() <= 2048);
