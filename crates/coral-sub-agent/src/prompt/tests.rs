use std::collections::BTreeSet;

use coral_catalog::{
    AuthenticationKind, BuiltinCredentialRoute, ListedProvider, ListedProviderState, ProviderFacts,
};
use serde::Deserialize;
use serde_json::Value;

use super::{TOOLS, system_prompt};

// Catches: any change to the words of the system prompt, with or without Providers.
#[test]
fn the_system_prompt_reads_as_written() {
    let providers = [
        provider("github", "GitHub", 3, ListedProviderState::Active, false),
        provider("linear", "Linear", 1, ListedProviderState::Disabled, true),
    ];
    for (providers, expected) in [
        (
            &[][..],
            format!(
                "You are Coral's data agent. A lead agent wrote the brief in this thread and receives your final message.\n\n\
                 Coral listed no providers for this workspace. Leave `provider` out of `coral.search`.\n\n\
                 {SECTIONS}"
            ),
        ),
        (
            &providers[..],
            format!(
                "You are Coral's data agent. A lead agent wrote the brief in this thread and receives your final message.\n\n\
                 Providers in this workspace (the values `coral.search` accepts as `provider`):\n\n\
                 - github: GitHub, 3 tools\n\
                 - linear: Linear, 1 tool, disabled, needs an account connected\n\n\
                 {SECTIONS}"
            ),
        ),
    ] {
        assert_eq!(system_prompt(providers), expected);
    }
}

// Catches: any change to a tool's name, description or argument schema.
#[test]
fn each_tool_reads_as_written() {
    let tools = TOOLS.map(|tool| (tool.name, tool.description, tool.parameters));
    assert_eq!(
        tools,
        [
            ("exec", EXEC_DESCRIPTION, EXEC_PARAMETERS),
            ("wait", WAIT_DESCRIPTION, WAIT_PARAMETERS),
            (
                "cancel",
                "Request cancellation of one program run.",
                CANCEL_PARAMETERS
            ),
        ]
    );
}

// Catches: a schema that is not JSON, or that names an argument other than
// the ones the tools' code reads.
#[test]
fn each_schema_is_json_and_names_the_arguments_the_tools_read() {
    for (name, required, optional) in [
        (
            "exec",
            &["task_id", "intent", "program"][..],
            &["run_timeout_ms", "wait_ms"][..],
        ),
        ("wait", &["run_id"][..], &["cursor", "wait_ms"][..]),
        ("cancel", &["run_id"][..], &["cursor"][..]),
    ] {
        let tool = TOOLS
            .iter()
            .find(|tool| tool.name == name)
            .expect("the tool");
        let schema: Schema = serde_json::from_str(tool.parameters).expect("JSON");
        let names = schema.properties.keys().map(String::as_str);
        let listed = required.iter().chain(optional).copied();
        assert_eq!(
            names.collect::<BTreeSet<_>>(),
            listed.collect::<BTreeSet<_>>(),
            "{name}"
        );
        assert_eq!(schema.required, required, "{name}");
    }
}

/// The parts of a tool's argument schema that name its arguments.
#[derive(Deserialize)]
struct Schema {
    properties: serde_json::Map<String, Value>,
    required: Vec<String>,
}

fn provider(
    namespace: &str,
    display_name: &str,
    tools: usize,
    state: ListedProviderState,
    account_required: bool,
) -> ListedProvider {
    ListedProvider {
        call_root: format!("coral.providers.{namespace}"),
        namespace: Some(namespace.to_owned()),
        state,
        facts: ProviderFacts {
            display_name: display_name.to_owned(),
            credential_route: BuiltinCredentialRoute::None,
            authentication: AuthenticationKind::None,
            tools,
            account_required,
        },
    }
}

const SECTIONS: &str = "\
## Tools
- exec: run one TypeScript async-function body as a new run. Include this thread's task_id.
- wait: read a run whose state is `running` or that has more output.
- cancel: stop a run.
- A tool result over 20 KiB is truncated.

## Programs
- `coral.search({ query, provider, limit })` returns tools matching a description of what they do, each with `call_path` and `signature_line`; `coral.search({ paths })` returns full declarations. It does not search records.
- Call tools by complete call path: `coral.providers.<name>.…` for providers, `coral.…` for Coral.
- Only provider tools reach the network. Coral does not paginate or retry. Concurrent calls may return HTTP 429.
- A failed call throws `coral.ToolError`; its `status` and `data` describe the failure, its message does not.
- The return value is the run's final value.

## Lead
- The lead observes each run: intent, calls, refusals, results.
- A message with no tool call ends your turn and is delivered to the lead.
- The lead holds context you lack, including the user's conversation. It can reply after your turn or message you during it; a message during a turn arrives before your next step. This thread persists across turns.
- One lead message costs about as much time as two of your steps.
- A run with `required_actions` waits for a person; the lead shows them.
- Make no change the brief does not specify exactly. The current date is in the brief. Do not add custom tools.
";

const EXEC_DESCRIPTION: &str = r#"Start a new Program Run from one TypeScript async-function body. Observe it with `wait`. Return only the values the answer needs: select fields and filter items inside the Program, and do not return or emit a whole provider tool result. Log diagnostics with `console.log`. A thrown `coral.ToolError` means the call produced no declared success. When Coral rejects the upstream's response, it carries `data` and `metadata`, plus `status` for HTTP, so catch it to treat a 404 as an answer. Without `data`, it carries only a Coral failure code, and the call may or may not have taken effect.

Find provider, shared and private tools with `coral.search`. It finds tools by what they do, never records: query with the action, such as `search messages`, and keep people, titles, IDs and dates for the tool call. No hit never means that the record is missing: search again with other words for the action. A `query` returns `hits`. Call a read from a hit when it is `complete`. Otherwise, and before a write, pass its `call_path` in `paths` to read its declaration. A path that names nothing comes back in `missing`, with `suggestions`. When the user names a service, pass all its namespaces in `provider`, as one string or an array; otherwise leave it out. `coral` holds Coral's own tools. A wrong namespace is refused with the full list.
```ts
const { data } = await coral.search({ query: "list pull requests", provider: "acme_code", limit: 5 });
const paths = data.hits.filter((hit) => !hit.complete).map((hit) => hit.call_path);
const read = paths.length ? (await coral.search({ paths })).data : undefined;
return {
  hits: data.hits.map((hit) => [hit.call_path, hit.signature_line, hit.description_snippet]),
  declarations: read?.descriptions.map((description) => description.typescript_declaration),
  missing: read?.missing.map((path) => [path.call_path, path.suggestions]),
};
```
Then call it in a later Program by its literal Call Path:
```ts
return (await coral.providers.acme_code.pulls.list({ owner: "acme", repo: "api" })).data.map((pull) => pull.title);
```"#;

const WAIT_DESCRIPTION: &str = "Observe one program run: its authored emissions and logs, current `required_actions`, and `state`. Returns when `required_actions` change, the run ends, or `wait_ms` runs out, with the output that arrived by then. New output alone does not end the wait. Observing consumes nothing, so an earlier cursor reads the same output again. Human handoff: a non-empty `required_actions` means the run waits for a person. Keep calling `wait` with `wait_ms` 30000 and the latest `next_cursor` until `required_actions` is empty or the run ends. While it holds, the call can report the link as its progress, and the person's decision on that page resumes the run on its own, so do not ask them to report back. You cannot decide an entry and must not say you did. A connection happens on that page, so never ask for a token, password or account in the chat. A decline or a missed `deadline_at` reaches the program as `coral.ToolError`, and a run past its own deadline ends `timed_out`: report what happened from the run's output, and do not start the program again unless asked. For `account_connection`, ask only when the entry appears.";

const EXEC_PARAMETERS: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "task_id": {
      "description": "The task_id of the Task this call continues.",
      "type": "string",
      "pattern": "^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
      "maxLength": 104
    },
    "intent": {
      "description": "Why this call is needed, in one short phrase.",
      "type": "string",
      "minLength": 1,
      "maxLength": 80
    },
    "program": {
      "description": "TypeScript async-function body to run. At most 64 KiB.",
      "type": "string"
    },
    "run_timeout_ms": {
      "description": "Run timeout in milliseconds, counted from when the Program starts. Default and maximum 900000 (15 minutes). Waiting and human action do not extend it.",
      "type": ["integer", "null"],
      "default": 900000,
      "minimum": 1,
      "maximum": 900000
    },
    "wait_ms": {
      "description": "Longest this call waits for the run to end or for `required_actions` to change, in milliseconds. New output does not end the wait. Default 30000. 0 returns at once.",
      "type": ["integer", "null"],
      "default": 30000,
      "minimum": 0,
      "maximum": 30000
    }
  },
  "required": ["task_id", "intent", "program"],
  "additionalProperties": false
}"#;

const WAIT_PARAMETERS: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "run_id": {
      "description": "The run_id from exec.",
      "type": "string",
      "pattern": "^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
      "maxLength": 104
    },
    "cursor": {
      "description": "The next_cursor from the previous exec or wait on this run. Without it, the response returns the run's events from the start.",
      "type": ["string", "null"],
      "minLength": 1
    },
    "wait_ms": {
      "description": "Longest this call waits for the run to end or for `required_actions` to change, in milliseconds. New output does not end the wait. Default 0, which returns at once.",
      "type": ["integer", "null"],
      "default": 0,
      "minimum": 0,
      "maximum": 30000
    }
  },
  "required": ["run_id"],
  "additionalProperties": false
}"#;

const CANCEL_PARAMETERS: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "run_id": {
      "description": "The run_id from exec.",
      "type": "string",
      "pattern": "^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
      "maxLength": 104
    },
    "cursor": {
      "description": "The next_cursor from the previous exec or wait on this run. Without it, the response returns the run's events from the start.",
      "type": ["string", "null"],
      "minLength": 1
    }
  },
  "required": ["run_id"],
  "additionalProperties": false
}"#;
