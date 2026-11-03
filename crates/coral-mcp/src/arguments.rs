use std::any::Any;

use rmcp::ErrorData;
use rmcp::model::{Tool, ToolAnnotations};
use schemars::transform::{RestrictFormats, Transform as _};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use coral_run::RUN_TIMEOUT_MAX_MS;
use coral_search::SEARCH_DEFAULT_LIMIT;
use coral_task::{TASK_FEEDBACK_MAX_CHARS, TASK_INTENT_MAX_CHARS};

const TASK_ID_DESCRIPTION: &str = "The task_id from start_task.";
const RUN_ID_DESCRIPTION: &str = "The run_id from exec.";
const CALL_INTENT_DESCRIPTION: &str = "Why this call is needed, in one short phrase.";
const DESCRIBE_INTENT_DESCRIPTION: &str =
    "What the search result did not show that you need from this declaration.";
const FUNCTION_PATH_DESCRIPTION: &str =
    "Dotted path under coral.functions, without that prefix, such as reports.summarize.";
const SEARCH_TOOL_DESCRIPTION: &str = "Search current visible provider tools and Saved Functions. In `query`, give keywords for the capability you need. Leave out people, record titles, IDs, dates and other record values. Each result is a summary with a signature line. Call a read directly when its signature line is complete. Call describe first when the line was cut, its input names a type you do not see, or before a write. When the user names the service, pass its namespace in `provider`, or `coral.workspace` for the Built-in Provider's tools. Otherwise, leave `provider` out. `start_task` lists the namespaces you can search. The filter narrows provider tools to one Provider; Saved Functions stay. A page lists `retriever_statuses` only when results may be missing; retry shortly or list without a query.";
const RUN_CURSOR_DESCRIPTION: &str = "The next_cursor from the previous exec or wait on this Run. Without it, the response returns the Run's events from the start.";
pub(crate) const DEFAULT_RUN_TIMEOUT_MS: u64 = 900_000;
pub(crate) const DEFAULT_EXEC_WAIT_MS: u64 = 10_000;
pub(crate) const DEFAULT_WAIT_MS: u64 = 0;
pub(crate) const MAX_WAIT_MS: u64 = 30_000;

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
pub(crate) struct SearchArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = format!("Keywords for the capability you need, such as `list pull requests`. Omit only to page through visible items, {SEARCH_DEFAULT_LIMIT} per page.")
    )]
    pub(crate) query: Option<String>,
    #[schemars(
        length(min = 1, max = 128),
        regex(pattern = r"^([A-Za-z_$][A-Za-z0-9_$]*|coral\.workspace)$"),
        description = "Namespace from coral.providers.<namespace>.<tool>, as listed by start_task, or coral.workspace for the Built-in Provider's tools. Omit unless the user names the service."
    )]
    pub(crate) provider: Option<String>,
    #[schemars(
        range(min = 1, max = 50),
        extend("default" = SEARCH_DEFAULT_LIMIT),
        description = format!("Maximum results. Default {SEARCH_DEFAULT_LIMIT}.")
    )]
    pub(crate) limit: Option<u16>,
    #[schemars(
        length(min = 1),
        description = "The next_cursor from the previous page, with the same query and provider."
    )]
    pub(crate) cursor: Option<String>,
}

impl ToolArguments for SearchArguments {
    const NAME: &'static str = "search";
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub(crate) struct DescribeArguments {
    #[schemars(schema_with = "identifier_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = DESCRIBE_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = "Complete Call Path: coral.providers.<namespace>.<tool>, coral.workspace.<tool> or coral.functions.<path>."
    )]
    pub(crate) path: String,
    #[schemars(
        length(max = 32),
        description = "Type names from this path's declaration. Returns those types whole instead of the inputs and the function."
    )]
    pub(crate) types: Option<Vec<String>>,
}

impl ToolArguments for DescribeArguments {
    const NAME: &'static str = "describe";
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
        description = format!("How long this call waits for output before returning, in milliseconds. Default {DEFAULT_EXEC_WAIT_MS}. 0 returns at once.")
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
        description = format!("How long to wait for new output, a required action, or the end of the Run, in milliseconds. Default {DEFAULT_WAIT_MS}.")
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
        tool::<StartTaskArguments, crate::projection::StartedTask>(
            "Start one Coral Task for one unit of user work.",
            mutation_annotations("Start Task", false, false),
        ),
        tool::<SearchArguments, crate::projection::SearchResult>(
            SEARCH_TOOL_DESCRIPTION,
            read_annotations("Search"),
        ),
        tool::<
            DescribeArguments,
            crate::projection::Description<Option<crate::projection::SavedDetail>>,
        >(
            "Describe one exact Call Path: the inputs and result types needed to call it. Result types beyond a size limit appear as field names only or are not shown; describe the same path with types to see them. To compare candidates, call describe for each of them in parallel.",
            read_annotations("Describe"),
        ),
        tool::<SaveFunctionArguments, crate::projection::SavedFunctionResult<'_>>(
            "Atomically create or replace one Saved Function. Programs call it as `coral.functions.<path>(...)`.",
            mutation_annotations("Save Function", true, false),
        ),
        tool::<DeleteFunctionArguments, crate::projection::DeletedFunction>(
            "Delete one Saved Function.",
            mutation_annotations("Delete Function", true, false),
        ),
        tool::<ExecArguments, crate::projection::RunUpdateResult<'_>>(
            "Start a new Program Run from one TypeScript async-function body. Observe it with `wait`. Return only the values the answer needs: select fields and filter items inside the Program, and do not return or emit a whole provider tool result. Log diagnostics with `console.log`. A thrown `coral.ToolError` means the call produced no declared success. When Coral rejects the upstream's response, it carries `data` and `metadata`, plus `status` for HTTP, so catch it to treat a 404 as an answer. Without `data`, it carries only a Coral failure code, and the call may or may not have taken effect.",
            mutation_annotations("Execute Program", true, true),
        ),
        tool::<WaitArguments, crate::projection::RunUpdateResult<'_>>(
            "Observe one Program Run: its authored Emissions and Logs, current `required_actions`, and `state`. Observing consumes nothing, so an earlier cursor reads the same output again.",
            read_annotations("Wait for Run"),
        ),
        tool::<CancelArguments, crate::projection::RunUpdateResult<'_>>(
            "Request cancellation of one Program Run.",
            mutation_annotations("Cancel Run", true, false),
        ),
        tool::<EndTaskArguments, crate::projection::EndedTask>(
            "Record one Task's success or failure outcome. The first call wins.",
            mutation_annotations("End Task", false, false),
        ),
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

    /// The `types` schema caps the item count; the per-name byte limit is
    /// domain-validated and not advertised.
    #[test]
    fn describe_types_schema_caps_the_item_count() {
        let tools = tools();
        let describe = tools
            .iter()
            .find(|tool| tool.name == "describe")
            .expect("advertised tool");
        let types = describe
            .input_schema
            .get("properties")
            .and_then(|properties| properties.get("types"))
            .expect("types schema");
        assert_eq!(
            types.get("maxItems").and_then(Value::as_u64),
            u64::try_from(coral_search::DESCRIBE_TYPES_MAX).ok()
        );
    }

    /// Reef's Activity detail parses retained `search` and `describe`
    /// responses by these keys, and a Reef test reads the same two files. A
    /// difference rewrites the file and fails: review the diff, change Reef's
    /// parser to match, and commit both.
    #[test]
    fn search_and_describe_output_schemas_match_their_snapshots() {
        let tools = tools();
        let mut changed = Vec::new();
        for name in ["search", "describe"] {
            let tool = tools
                .iter()
                .find(|tool| tool.name == name)
                .expect("advertised tool");
            let advertised = Value::Object(
                tool.output_schema
                    .as_deref()
                    .expect("advertised output schema")
                    .clone(),
            );
            let path = format!(
                "{}/tests/output-schema/{name}.json",
                env!("CARGO_MANIFEST_DIR")
            );
            let snapshot = std::fs::read_to_string(&path)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok());
            if snapshot.as_ref() != Some(&advertised) {
                let mut text = serde_json::to_string_pretty(&advertised).expect("schema as JSON");
                text.push('\n');
                std::fs::write(&path, text).expect("write the output schema snapshot");
                changed.push(path);
            }
        }
        assert!(
            changed.is_empty(),
            "the advertised output schema differs from its snapshot; rewrote {changed:?}"
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
