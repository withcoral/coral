//! Server `instructions`: the agent workflow every Coral session needs first.
//!
//! Claude Code 2.1.292 kept only the first 2048 characters of server
//! instructions, and 2.1.296 and later keep 4096. The text stays within 2048,
//! so a client that cuts at either limit reads it whole. Codex copies it into
//! each tool, and pi
//! shows the model its first line or none of it, so what an agent needs on every
//! call, such as its namespaces, goes in `exec`'s description instead. Agents
//! learn discovery from `exec`'s example, the types and the errors; the rest
//! of the text is kept as it was before discovery moved into Programs.

/// Instructions sent in `InitializeResult` and `DiscoverResult`.
pub(crate) const INSTRUCTIONS: &str = "\
Coral runs TypeScript Programs that call the user's connected providers.

## Workflow
1. Pass `goal` to the first `exec` of a unit of work, then its `task_id` to later calls.
2. Find provider tools and Saved Functions with `coral.search` in a Program; `exec` shows how.
3. Call `exec` with one TypeScript async-function body. Each `exec` starts a new Run and can repeat external effects.
4. While `state` is `running` or `has_more` is true, call `wait` with the same `run_id` and the latest `next_cursor`. The last page carries the Final Value or failure.
5. If other values can reuse the work (many calls, paging, joins, filters), write a function, call it, then pass it as `source` to `coral.addTool` in that Run. Skip one call, one-off writes and exploration. Tell the user its path.
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
A non-empty `required_actions` means the Run waits for a person.
- For `invoking_user`, write the full `url` as reply text, then keep calling `wait`: it can report the link as progress while it holds, and the decision resumes the Run.
- Never ask for a token, password or account in the chat: a connection happens on that page.
";

// Clients add instructions to every turn; Claude Code before 2.1.296 drops
// what follows 2048.
const _: () = assert!(INSTRUCTIONS.len() <= 2048);
