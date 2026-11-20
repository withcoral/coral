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
                 Coral listed no providers for this workspace. Leave `provider` out of `search`.\n\n\
                 {SECTIONS}"
            ),
        ),
        (
            &providers[..],
            format!(
                "You are Coral's data agent. A lead agent wrote the brief in this thread and receives your final message.\n\n\
                 Providers in this workspace (the values `search` accepts as `provider`):\n\n\
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
            ("search", SEARCH_DESCRIPTION, SEARCH_PARAMETERS),
            ("end_task", END_TASK_DESCRIPTION, END_TASK_PARAMETERS),
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
        (
            "search",
            &[][..],
            &["query", "provider", "limit", "offset", "paths", "types"][..],
        ),
        ("end_task", &["task_id", "outcome"][..], &["feedback"][..]),
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
    #[serde(default)]
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
- search: find the tools a program can call by what they do, and read their declarations.
- exec: run one TypeScript async-function body as a new run. Include this thread's task_id.
- wait: read a run whose state is `running` or that has more output.
- cancel: stop a run.
- end_task: record the outcome once the work is done or cannot continue, then reply with your answer.
- A tool result over 20 KiB is truncated.

## Programs
- Call tools by the complete call path that `search` returns: `coral.providers.<name>.…`, `coral.shared.…` or `coral.private.…`.
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

const EXEC_DESCRIPTION: &str = "Start a new Program Run from one TypeScript async-function body. Observe it with `wait`. Return only the values the answer needs: select fields and filter items inside the Program, and do not return or emit a whole provider tool result. Log diagnostics with `console.log`. A thrown `coral.ToolError` means the call produced no declared success. When Coral rejects the upstream's response, it carries `data` and `metadata`, plus `status` for HTTP, so catch it to treat a 404 as an answer. Without `data`, it carries only a Coral failure code, and the call may or may not have taken effect.";

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

const END_TASK_DESCRIPTION: &str = "Record whether your work succeeded or failed. The lead's Task stays open: the lead ends it. Then reply with your answer, with no tool call.";

const END_TASK_PARAMETERS: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "task_id": {
      "description": "The task_id of the Task this call continues.",
      "type": "string",
      "pattern": "^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
      "maxLength": 104
    },
    "outcome": {
      "description": "Whether the work succeeded.",
      "type": "string",
      "enum": ["success", "failure"]
    },
    "feedback": {
      "description": "Optional notes on how the work went.",
      "type": ["string", "null"]
    }
  },
  "required": ["task_id", "outcome"],
  "additionalProperties": false
}"#;

const SEARCH_DESCRIPTION: &str = r#"Find provider, shared and private tools. It finds tools by what they do, never records: query with the action, such as `search messages`, and keep people, titles, IDs and dates for the tool call. No hit never means that the record is missing: search again with other words for the action. A `query` returns `hits`. Call a read from a hit when it is `complete`. Otherwise, and before a write, pass its `call_path` in `paths` to read its declaration. A path that names nothing comes back in `missing`, with `suggestions`. When the user names a service, pass all its namespaces in `provider`, as one string or an array; otherwise leave it out. A wrong namespace is refused with the full list.
```json
{"query": "list pull requests", "provider": "acme_code", "limit": 5}
```
returns `hits`, each with `call_path`, `signature_line`, `complete` and `description_snippet`.
```json
{"paths": ["coral.providers.acme_code.pulls.list"]}
```
returns `descriptions`, each with its `typescript_declaration`, and `missing`, each with `suggestions`.
Then call it in a Program by its literal Call Path:
```ts
return (await coral.providers.acme_code.pulls.list({ owner: "acme", repo: "api" })).data.map((pull) => pull.title);
```"#;

const SEARCH_PARAMETERS: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "query": {
      "description": "Keywords for the capability you need, such as list pull requests. Leave out people, record titles, IDs, dates and other record values. Omit it to list what you can call.",
      "type": "string",
      "minLength": 1
    },
    "provider": {
      "description": "When the user names a service, all its namespaces. Otherwise leave it out. It narrows provider tools to those providers; shared tools and your private tools stay.",
      "anyOf": [
        { "type": "string" },
        { "type": "array", "items": { "type": "string" }, "minItems": 1, "maxItems": 32 }
      ]
    },
    "limit": {
      "description": "Maximum hits. Default 10.",
      "type": "integer",
      "minimum": 1,
      "maximum": 50
    },
    "offset": {
      "description": "The next_offset of the previous page, with the same query and provider.",
      "type": "integer",
      "minimum": 0
    },
    "paths": {
      "description": "1 to 10 complete call paths whose declarations to read. Pass it without query, provider, limit and offset.",
      "type": "array",
      "items": { "type": "string", "maxLength": 1024 },
      "minItems": 1,
      "maxItems": 10
    },
    "types": {
      "description": "Type names from the declaration of the one path in paths, returned whole instead of the declaration.",
      "type": "array",
      "items": { "type": "string" },
      "maxItems": 32
    }
  },
  "additionalProperties": false
}"#;
