use super::{AGENT_DESCRIPTION, INSTRUCTIONS};

// Catches: a change to what the lead reads about `agent` that nobody meant.
#[test]
fn the_agent_description_is_the_text_the_lead_reads() {
    assert_eq!(
        AGENT_DESCRIPTION,
        "Delegate data retrieval to Coral's agents. An agent reads the user's connected providers and can make one change you specify exactly. It runs programs with your permissions, in your Task. Add providers, change policy and save custom tools yourself. You remain responsible for the answer.\n\
         \n\
         One brief per question. An agent observes only your messages. Include the user's question verbatim, today's date, and, if you have one, a guess at where to look first, stated as a guess.\n\
         \n\
         An agent's turn ends when it sends a message without a tool call: an answer, or a report of where it is blocked. Reply in one or two sentences with `agent`, `sub_agent_id` and `message`; a message sent during a turn is read before the agent's next step. `wait` on the `sub_agent_id` until it ends; it returns its `state` (`running`, `completed`, `failed`, `canceled` or `interrupted`), its `answer` once `completed`, and its steps. A step that started a run holds the agent's intent and that run's `run_id`; to check the step, call `wait` with that `run_id`."
    );
}

// Catches: a change to the lead's steps that nobody meant, and the steps
// served without the sections of core that follow them.
#[test]
fn the_instructions_are_cores_with_the_leads_steps_as_their_workflow() {
    let workflow = "## Workflow\n\
        1. Start a unit of work with `agent` or `exec` with `goal`, then pass the `task_id` to later calls.\n\
        2. For data work you can describe in messages, brief Coral's agents with `agent`, one brief per question, and `wait` on each `sub_agent_id`. Correct one when its steps show it going wrong, review each answer, and follow up with its `sub_agent_id`. Go to step 5.\n\
        3. Otherwise, call `exec` with one TypeScript async-function body; it shows how to find provider tools with `coral.search`. While `state` is `running` or `has_more` is true, call `wait` with the `run_id` and latest `next_cursor`.\n\
        4. If other values can reuse the work, add it with `coral.addCustomTool` in a run. Tell the user its path.\n\
        5. When the work is done or cannot continue, call `end_task` once per Task with the outcome.\n\
        \n\
        ## Program rules\n";
    assert!(INSTRUCTIONS.contains(workflow), "{INSTRUCTIONS}");
}
