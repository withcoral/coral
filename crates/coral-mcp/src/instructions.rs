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

/// Writes the instructions sent in `InitializeResult` and `DiscoverResult`
/// around the steps of their Workflow section, as a string literal.
///
/// Clients add instructions to every turn, and Claude Code before 2.1.296 drops
/// what follows 2048, so longer instructions do not compile.
macro_rules! instructions {
    ($steps:literal) => {{
        const INSTRUCTIONS: &str = concat!(
            "\
Coral runs TypeScript programs that call the user's connected providers.

## Workflow
",
            $steps,
            "
## Program rules
- Write each call as the complete literal call path that search returns, such as `coral.providers.github.getRepo(...)`. Keep `coral` and its sub-paths out of variables.
- Only provider tools reach the network; `fetch`, `setTimeout` and other ambient APIs are absent.
- Keep credentials and account choice out of program source; Coral supplies both.
- Publish interim output with `await coral.emit(value)`. The program's return value is the final value.
- Error responses throw `coral.ToolError` with their `data`. Coral never pages or retries: follow continuation data.

## Results
- `isError: true` means Coral rejected the call. Fix what `code` and `message` name, then call again.
- Report a failed, canceled, timed-out or interrupted run to the user as its outcome.

## Human handoff
A non-empty `required_actions` means the run waits for a person.
- For each new `invoking_user` entry, write the full `url` as reply text, call `show_required_actions` once, then keep calling `wait`: the decision resumes the run.
- Never ask for a token, password or account in the chat: a connection happens on that page.
"
        );
        const _: () = assert!(INSTRUCTIONS.len() <= 2048);
        INSTRUCTIONS
    }};
}

/// The instructions of an agent that works with core's own tools.
pub(crate) const INSTRUCTIONS: &str = instructions!(
    "\
1. Pass `goal` to the first `exec` of a unit of work, then its `task_id` to later calls.
2. Find provider and custom tools with `coral.search` in a program; `exec` shows how.
3. Call `exec` with one TypeScript async-function body. Each `exec` starts a new run and can repeat external effects.
4. While `state` is `running` or `has_more` is true, call `wait` with the same `run_id` and the latest `next_cursor`. The last page carries the final value or failure.
5. If other values can reuse the work (many calls, paging, joins, filters), write a function, call it, then pass it as `source` to `coral.addCustomTool` in that run (`coral.updateCustomTool` replaces one). Skip one call, one-off writes and exploration. Tell the user its path.
6. When the work is done or cannot continue, call `end_task` once with the outcome.
"
);

/// The instructions of an agent that can also hand work to a Sub-agent:
/// policy enables `coral.agent` in its Workspace (ADR 0040).
pub(crate) const DELEGATING_INSTRUCTIONS: &str = instructions!(
    "\
1. Pass `goal` to the first `exec` of a unit of work, then its `task_id` to later calls.
2. For data work you can describe in a message, brief one of Coral's agents from a program, one brief per question: `return await coral.agent({ message })`. While `state` is `running` or `needs_action`, call again with `sub_agent_id`. Review `answer` and `steps`; to reply, add `message`. Go to step 5.
3. Otherwise, call `exec` with one TypeScript async-function body; it shows how to find provider tools with `coral.search`. While `state` is `running` or `has_more` is true, call `wait` with the `run_id` and latest `next_cursor`.
4. If other values can reuse the work, add it with `coral.addCustomTool` in a run. Tell the user its path.
5. When the work is done or cannot continue, call `end_task` once per Task with the outcome.
"
);

#[cfg(test)]
mod tests {
    use super::{DELEGATING_INSTRUCTIONS, INSTRUCTIONS};

    // Catches: a lead that can delegate served without the call that does it,
    // and delegating steps served without the sections of core that follow them.
    #[test]
    fn the_delegating_instructions_show_the_call_and_keep_cores_other_sections() {
        let after_workflow = |text: &'static str| text.split_once("## Program rules").unzip().1;
        let call = "`return await coral.agent({ message })`";
        assert!(DELEGATING_INSTRUCTIONS.contains(call));
        assert!(!INSTRUCTIONS.contains("coral.agent"));
        assert!(after_workflow(INSTRUCTIONS).is_some());
        assert_eq!(
            after_workflow(DELEGATING_INSTRUCTIONS),
            after_workflow(INSTRUCTIONS)
        );
    }
}
