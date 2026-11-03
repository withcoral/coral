use std::any::Any;

use rmcp::ErrorData;
use rmcp::model::{MetaObject, Tool, ToolAnnotations};
use schemars::transform::{RestrictFormats, Transform as _};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use coral_run::RUN_TIMEOUT_MAX_MS;
use coral_task::{TASK_FEEDBACK_MAX_CHARS, TASK_INTENT_MAX_CHARS};

const TASK_ID_DESCRIPTION: &str = "The task_id from start_task.";
const RUN_ID_DESCRIPTION: &str = "The run_id from exec.";
const CALL_INTENT_DESCRIPTION: &str = "Why this call is needed, in one short phrase.";
const FUNCTION_PATH_DESCRIPTION: &str =
    "Dotted path under coral.functions, without that prefix, such as reports.summarize.";
const RUN_CURSOR_DESCRIPTION: &str = "The next_cursor from the previous exec or wait on this Run. Without it, the response returns the Run's events from the start.";
/// `exec`'s description, with the one example of discovery an agent reads.
/// A test runs its first Program against a real `coral.workspace.search` and
/// saves it as a Saved Function to type-check it, so keep that Program whole.
pub(crate) const EXEC_TOOL_DESCRIPTION: &str = r#"Start a new Program Run from one TypeScript async-function body. Observe it with `wait`. Return only the values the answer needs: select fields and filter items inside the Program, and do not return or emit a whole provider tool result. Log diagnostics with `console.log`. A thrown `coral.ToolError` means the call produced no declared success. When Coral rejects the upstream's response, it carries `data` and `metadata`, plus `status` for HTTP, so catch it to treat a 404 as an answer. Without `data`, it carries only a Coral failure code, and the call may or may not have taken effect.

Find provider tools and Saved Functions with `coral.workspace.search`. A `query` returns `hits`. Call a read from a hit when it is `complete`. Otherwise, and before a write, pass its `call_path` in `paths` to read its declaration. A path that names nothing comes back in `missing`, with `suggestions`.
```ts
const { data } = await coral.workspace.search({ query: "list pull requests", limit: 5 });
const paths = data.hits.filter((hit) => !hit.complete).map((hit) => hit.call_path);
const read = paths.length ? (await coral.workspace.search({ paths })).data : undefined;
return {
  hits: data.hits.map((hit) => [hit.call_path, hit.signature_line, hit.description_snippet]),
  declarations: read?.descriptions.map((description) => description.typescript_declaration),
  missing: read?.missing.map((path) => [path.call_path, path.suggestions]),
};
```
Then call it in a later Program by its literal Call Path:
```ts
return (await coral.providers.github.pulls.list({ owner: "acme", repo: "api" })).data.map((pull) => pull.title);
```"#;

// Claude Code 2.1.296 keeps the first 4096 characters of a tool's
// description. `exec` stays within 2048, the budget the server instructions
// keep, so a client with a smaller limit still reads the example whole.
const _: () = assert!(EXEC_TOOL_DESCRIPTION.len() <= 2048);
pub(crate) const DEFAULT_RUN_TIMEOUT_MS: u64 = 900_000;
pub(crate) const DEFAULT_EXEC_WAIT_MS: u64 = 30_000;
pub(crate) const DEFAULT_WAIT_MS: u64 = 0;
pub(crate) const MAX_WAIT_MS: u64 = 30_000;

/// Claude Code sends a tool with this `_meta` key in its first request instead
/// of deferring it behind tool search. Other clients ignore the key.
const ALWAYS_LOAD_META_KEY: &str = "anthropic/alwaysLoad";

pub(crate) trait ToolArguments: DeserializeOwned + JsonSchema + Any {
    const NAME: &'static str;
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct StartTaskArguments {
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = "What the user asked for, in one short phrase."
    )]
    pub(crate) intent: String,
}

impl ToolArguments for StartTaskArguments {
    const NAME: &'static str = "start_task";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct SaveFunctionArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = FUNCTION_PATH_DESCRIPTION
    )]
    pub(crate) path: String,
    #[schemars(
        length(min = 1),
        description = "One line, one or two sentences: what the function does and when to use it. Do not restate input or return types; the declaration is shown beside it."
    )]
    pub(crate) description: String,
    #[schemars(
        length(min = 1),
        description = "TypeScript module with one default callable export and no imports. At most 64 KiB."
    )]
    pub(crate) source: String,
}

impl ToolArguments for SaveFunctionArguments {
    const NAME: &'static str = "save_function";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct DeleteFunctionArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = FUNCTION_PATH_DESCRIPTION
    )]
    pub(crate) path: String,
}

impl ToolArguments for DeleteFunctionArguments {
    const NAME: &'static str = "delete_function";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct ExecArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(description = "TypeScript async-function body to run. At most 64 KiB.")]
    pub(crate) program: String,
    #[schemars(
        range(min = 1, max = RUN_TIMEOUT_MAX_MS),
        extend("default" = DEFAULT_RUN_TIMEOUT_MS),
        description = format!("Run timeout in milliseconds. Default and maximum {DEFAULT_RUN_TIMEOUT_MS} (15 minutes). Waiting and human action do not extend it.")
    )]
    pub(crate) run_timeout_ms: Option<u64>,
    #[schemars(
        range(min = 0, max = MAX_WAIT_MS),
        extend("default" = DEFAULT_EXEC_WAIT_MS),
        description = format!("Longest this call waits for the Run to end or for `required_actions` to change, in milliseconds. New output does not end the wait. Default {DEFAULT_EXEC_WAIT_MS}. 0 returns at once.")
    )]
    pub(crate) wait_ms: Option<u64>,
}

impl ToolArguments for ExecArguments {
    const NAME: &'static str = "exec";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct WaitArguments {
    #[schemars(
        schema_with = "identifier_schema",
        description = RUN_ID_DESCRIPTION
    )]
    pub(crate) run_id: String,
    #[schemars(
        length(min = 1),
        description = RUN_CURSOR_DESCRIPTION
    )]
    pub(crate) cursor: Option<String>,
    #[schemars(
        range(min = 0, max = MAX_WAIT_MS),
        extend("default" = DEFAULT_WAIT_MS),
        description = format!("Longest this call waits for the Run to end or for `required_actions` to change, in milliseconds. New output does not end the wait. Default {DEFAULT_WAIT_MS}, which returns at once.")
    )]
    pub(crate) wait_ms: Option<u64>,
}

impl ToolArguments for WaitArguments {
    const NAME: &'static str = "wait";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct CancelArguments {
    #[schemars(
        schema_with = "identifier_schema",
        description = RUN_ID_DESCRIPTION
    )]
    pub(crate) run_id: String,
    #[schemars(
        length(min = 1),
        description = RUN_CURSOR_DESCRIPTION
    )]
    pub(crate) cursor: Option<String>,
}

impl ToolArguments for CancelArguments {
    const NAME: &'static str = "cancel";
}

#[derive(Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(rename_all = "snake_case")]
pub(crate) enum TaskOutcomeArgument {
    Success,
    Failure,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct EndTaskArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(description = "Terminal Task outcome.")]
    pub(crate) outcome: TaskOutcomeArgument,
    #[schemars(
        length(min = 1, max = TASK_FEEDBACK_MAX_CHARS),
        description = format!("Optional notes on how the Task went. At most {TASK_FEEDBACK_MAX_CHARS} characters.")
    )]
    pub(crate) feedback: Option<String>,
}

impl ToolArguments for EndTaskArguments {
    const NAME: &'static str = "end_task";
}

pub(crate) fn parse<T>(arguments: Option<Map<String, Value>>) -> Result<T, ErrorData>
where
    T: ToolArguments,
{
    let arguments = Value::Object(arguments.unwrap_or_default());
    T::deserialize(&arguments).ok().ok_or_else(|| {
        ErrorData::invalid_params(
            format!(
                "invalid arguments for '{}': {}",
                T::NAME,
                argument_problem(T::NAME, &arguments)
            ),
            None,
        )
    })
}

/// What is wrong with `arguments` to tool `name`, read from the schema Coral
/// advertises. A deserializer's message quotes the value or key it rejected,
/// which can be private, so none reaches the caller.
fn argument_problem(name: &str, arguments: &Value) -> String {
    let required = TOOLS
        .iter()
        .find(|tool| tool.name == name)
        .and_then(|tool| tool.input_schema.get("required"))
        .and_then(Value::as_array);
    let missing = required
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find(|field| arguments.get(field).is_none());
    match missing {
        Some(field) => format!("missing field `{field}`"),
        None => "a value does not match the tool's input schema".to_owned(),
    }
}

/// Every tool this Adapter serves, as `tools/list` advertises them.
pub(crate) fn tools() -> Vec<Tool> {
    TOOLS.clone()
}

/// The served tool named `name`. The name returned is Coral's own, never the
/// caller's text.
pub(crate) fn served_name(name: &str) -> Option<&'static str> {
    TOOLS
        .iter()
        .map(|tool| tool.name.as_ref())
        .find(|served| *served == name)
}

static TOOLS: std::sync::LazyLock<Vec<Tool>> = std::sync::LazyLock::new(|| {
    vec![
        always_loaded(tool::<StartTaskArguments, crate::projection::StartedTask>(
            "Start one Coral Task for one unit of user work.",
            mutation_annotations("Start Task", false, false),
        )),
        tool::<SaveFunctionArguments, crate::projection::SavedFunctionResult<'_>>(
            "Atomically create or replace one Saved Function. Programs call it as `coral.functions.<path>(...)`.",
            mutation_annotations("Save Function", true, false),
        ),
        tool::<DeleteFunctionArguments, crate::projection::DeletedFunction>(
            "Delete one Saved Function.",
            mutation_annotations("Delete Function", true, false),
        ),
        always_loaded(
            tool::<ExecArguments, crate::projection::RunUpdateResult<'_>>(
                EXEC_TOOL_DESCRIPTION,
                mutation_annotations("Execute Program", true, true),
            ),
        ),
        always_loaded(
            tool::<WaitArguments, crate::projection::RunUpdateResult<'_>>(
                "Observe one Program Run: its authored Emissions and Logs, current `required_actions`, and `state`. Returns when `required_actions` change, the Run ends, or `wait_ms` runs out, with the output that arrived by then. New output alone does not end the wait. Observing consumes nothing, so an earlier cursor reads the same output again.",
                read_annotations("Wait for Run"),
            ),
        ),
        tool::<CancelArguments, crate::projection::RunUpdateResult<'_>>(
            "Request cancellation of one Program Run.",
            mutation_annotations("Cancel Run", true, false),
        ),
        always_loaded(tool::<EndTaskArguments, crate::projection::EndedTask>(
            "Record one Task's success or failure outcome. The first call wins.",
            mutation_annotations("End Task", false, false),
        )),
    ]
});

fn tool<T, Output: JsonSchema>(description: &'static str, annotations: ToolAnnotations) -> Tool
where
    T: ToolArguments,
{
    let input_schema = rmcp::handler::server::tool::schema_for_input::<T>()
        .unwrap_or_else(|error| panic!("invalid MCP input schema for '{}': {error}", T::NAME));
    // Normalize only Coral-owned advertisements, never authoritative Provider schemas.
    // This visits schema positions (including $defs), not example/default JSON data.
    let mut input_schema = Schema::from(input_schema.as_ref().clone());
    RestrictFormats::default().transform(&mut input_schema);
    let mut tool = Tool::new(T::NAME, description, input_schema.ensure_object().clone())
        .with_annotations(annotations);
    tool.output_schema = Some(std::sync::Arc::new(crate::projection::output_schema::<
        Output,
    >()));
    tool
}

/// Marks a tool every Task uses, so its schema arrives before the first call.
fn always_loaded(tool: Tool) -> Tool {
    let mut meta = MetaObject::new();
    meta.0
        .insert(ALWAYS_LOAD_META_KEY.into(), Value::Bool(true));
    tool.with_meta(meta)
}

pub(crate) fn uuid_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = String::json_schema(generator);
    schema.insert("format".into(), json!("uuid"));
    schema.insert("minLength".into(), json!(36));
    schema.insert("maxLength".into(), json!(36));
    schema
}

/// A Task or Run identifier: four or five lowercase words joined by hyphens,
/// or the UUID of one issued before Coral used words.
pub(crate) fn identifier_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = String::json_schema(generator);
    schema.insert("pattern".into(), json!(coral_identifier::SCHEMA_PATTERN));
    schema.insert("maxLength".into(), json!(coral_identifier::MAX_LENGTH));
    schema
}

fn read_annotations(title: &'static str) -> ToolAnnotations {
    ToolAnnotations::with_title(title)
        .read_only(true)
        .destructive(false)
        .idempotent(true)
        .open_world(false)
}

fn mutation_annotations(
    title: &'static str,
    destructive: bool,
    open_world: bool,
) -> ToolAnnotations {
    ToolAnnotations::with_title(title)
        .read_only(false)
        .destructive(destructive)
        .idempotent(false)
        .open_world(open_world)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A caller learns which field is missing, but a rejected value or key
    /// does not come back in the error.
    #[test]
    fn invalid_arguments_name_a_missing_field_and_never_quote_the_caller() {
        const SCHEMA: &str = "a value does not match the tool's input schema";
        for (arguments, problem) in [
            (json!({}), "missing field `task_id`"),
            (json!({"task_id": "t", "outcome": "value-canary"}), SCHEMA),
            (
                json!({"task_id": ["value-canary"], "outcome": "success"}),
                SCHEMA,
            ),
            (
                json!({"task_id": "t", "outcome": "success", "key-canary": 1}),
                SCHEMA,
            ),
        ] {
            let Value::Object(arguments) = arguments else {
                panic!("object arguments");
            };
            let Err(error) = parse::<EndTaskArguments>(Some(arguments)) else {
                panic!("rejected: {problem}");
            };
            assert_eq!(
                error.message,
                format!("invalid arguments for 'end_task': {problem}")
            );
        }
    }

    /// The tools every Task uses load in the first request, and the rest stay
    /// deferred. A new tool fails here until it picks a side.
    #[test]
    fn only_the_core_tools_load_in_the_first_request() {
        let advertised: BTreeMap<String, Option<Value>> = tools()
            .iter()
            .map(|tool| {
                let advertised = serde_json::to_value(tool).expect("tool as JSON");
                let always_load = advertised
                    .get("_meta")
                    .and_then(|meta| meta.get(ALWAYS_LOAD_META_KEY))
                    .cloned();
                (tool.name.to_string(), always_load)
            })
            .collect();
        let expected = [
            ("start_task", Some(json!(true))),
            ("exec", Some(json!(true))),
            ("wait", Some(json!(true))),
            ("end_task", Some(json!(true))),
            ("save_function", None),
            ("delete_function", None),
            ("cancel", None),
        ]
        .map(|(name, always_load)| (name.to_owned(), always_load));
        assert_eq!(advertised, BTreeMap::from(expected));
    }

    /// Task history keeps the `search` and `describe` responses agents
    /// received before discovery moved into Programs, and Reef's Activity
    /// detail parses them by these keys; a Reef test reads the same two
    /// files. No tool advertises them now, so the schemas come from the
    /// types Task history retains. A difference rewrites the file and fails:
    /// review the diff, change Reef's parser to match, and commit both.
    #[test]
    fn retained_search_and_describe_schemas_match_their_snapshots() {
        let mut changed = Vec::new();
        for (name, schema) in [
            (
                "search",
                crate::projection::output_schema::<coral_catalog::SearchResult>(),
            ),
            (
                "describe",
                crate::projection::output_schema::<coral_catalog::Description>(),
            ),
        ] {
            let retained = Value::Object(schema);
            let path = format!(
                "{}/tests/output-schema/{name}.json",
                env!("CARGO_MANIFEST_DIR")
            );
            let snapshot = std::fs::read_to_string(&path)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok());
            if snapshot.as_ref() != Some(&retained) {
                let mut text = serde_json::to_string_pretty(&retained).expect("schema as JSON");
                text.push('\n');
                std::fs::write(&path, text).expect("write the output schema snapshot");
                changed.push(path);
            }
        }
        assert!(
            changed.is_empty(),
            "the retained output schema differs from its snapshot; rewrote {changed:?}"
        );
    }

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[expect(dead_code, reason = "schema-generation fixture")]
    struct NumericArguments {
        count: u16,
        entries: Vec<NumericFields>,
        optional: Option<NumericFields>,
        #[schemars(extend("default" = {"type": "integer", "format": "uint64"}))]
        metadata: Option<Value>,
    }

    impl ToolArguments for NumericArguments {
        const NAME: &'static str = "numeric_fixture";
    }

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[expect(dead_code, reason = "schema-generation fixture")]
    struct NumericFields {
        unsigned: u64,
        signed: i32,
        floating: f64,
        #[schemars(schema_with = "uuid_schema")]
        id: String,
    }

    #[test]
    fn generated_nested_inputs_remove_only_nonstandard_formats() {
        let advertised = tool::<NumericArguments, crate::projection::StartedTask>(
            "Numeric fixture",
            read_annotations("Fixture"),
        );
        let schema = Value::Object(advertised.input_schema.as_ref().clone());
        let validator = jsonschema::draft202012::options()
            .should_validate_formats(true)
            .should_ignore_unknown_formats(false)
            .build(&schema)
            .expect("nested numeric schemas compile with standard formats only");
        let mut expected = Value::Object(
            rmcp::handler::server::tool::schema_for_input::<NumericArguments>()
                .unwrap()
                .as_ref()
                .clone(),
        );
        // The only permitted changes are these generated numeric annotations.
        // In particular, keep UUID, local references, required fields and bounds.
        for (path, format) in [
            ("/properties/count", "uint16"),
            ("/$defs/NumericFields/properties/unsigned", "uint64"),
            ("/$defs/NumericFields/properties/signed", "int32"),
            ("/$defs/NumericFields/properties/floating", "double"),
        ] {
            assert_eq!(
                expected
                    .pointer_mut(path)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove("format"),
                Some(json!(format))
            );
        }
        assert_eq!(schema, expected);

        let mut input = json!({
            "count": 1,
            "entries": [{"unsigned": 2, "signed": -3, "floating": 4.5,
                "id": "12345678-1234-4123-8123-123456789abc"}],
            "optional": null
        });
        assert!(validator.is_valid(&input));
        *input.pointer_mut("/entries/0/id").expect("nested UUID") =
            json!("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
        assert!(
            !validator.is_valid(&input),
            "UUID validation survives normalization"
        );
    }
}
