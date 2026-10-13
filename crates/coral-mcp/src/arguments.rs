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

const TASK_ID_DESCRIPTION: &str = "Task identifier returned by start_task for this unit of attributed work; canonical lowercase hyphenated UUID.";
const RUN_ID_DESCRIPTION: &str =
    "Coral-generated Program Run identifier; canonical lowercase hyphenated UUID.";
const CALL_INTENT_DESCRIPTION: &str = "Nonblank natural-language reason this call is needed for the attributed Task. No null characters. At most 80 Unicode characters, including whitespace; not trimmed.";
const DESCRIBE_INTENT_DESCRIPTION: &str = "What you need to learn from this contract that the search result did not show. Nonblank, with no null characters. At most 80 Unicode characters, including whitespace; not trimmed.";
const FUNCTION_PATH_DESCRIPTION: &str = "Dotted path relative to coral.functions (omit that prefix), such as reports.summarize. Use 1 to 16 safe JavaScript identifier segments; Unicode identifiers are supported. Reserved segments __proto__, prototype, constructor and then are forbidden. At most 256 UTF-8 bytes (not characters), checked by the domain.";
const SEARCH_TOOL_DESCRIPTION: &str = "Search current visible Operations and Saved Functions. In `query`, describe the capability you need. Leave out people, record titles, IDs, dates and other record values. Each result is a summary with evidence of why it matched and a signature line. Call a read directly when its signature line is complete. Call describe first when the line was cut, its input names a type you do not see, or before a write. When the user names the service, pass its namespace in `provider`, or `coral.admin` for Coral's own operations. Otherwise, leave `provider` out. `start_task` lists the namespaces you can search. The filter narrows Operations to one Provider; Saved Functions stay. A retriever status of `partial` or `error` means results may be missing; retry shortly or list without a query.
Retriever states:
- `results_found`: the retriever ranked at least one result.
- `empty`: the retriever searched everything and nothing matched.
- `partial`: some Providers or Saved Functions are not indexed yet, or failed; results may be missing.
- `error`: the retriever could not rank; its results are missing.";
const RUN_CURSOR_DESCRIPTION: &str =
    "Optional opaque Run cursor. Reuse the returned next_cursor unchanged for the same Run.";
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
        description = "Nonblank natural-language description of the work this Task should group. No null characters. At most 80 Unicode characters, including whitespace; not trimmed."
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = "Optional nonblank catalog discovery query, not a request for upstream records. Describe the capability you need. Leave out people, record titles, IDs, dates and other record values. No null characters; not trimmed. At most 4096 UTF-8 bytes (not characters), including whitespace, checked by the domain. Omit or use null to list visible items; an empty string is invalid."
    )]
    pub(crate) query: Option<String>,
    #[schemars(
        length(min = 1, max = 128),
        regex(pattern = r"^([A-Za-z_$][A-Za-z0-9_$]*|coral\.admin)$"),
        description = "Optional Provider namespace filter: the second segment of the Provider's Call Paths, as in coral.providers.<namespace>.<operation>, or coral.admin for Coral's own operations. Not a display name or id. Pass it when the user names the service. Otherwise, leave it out. The start_task result lists the namespaces you can search; Coral refuses any other value. It narrows Operations only; Saved Functions stay."
    )]
    pub(crate) provider: Option<String>,
    #[schemars(
        range(min = 1, max = 50),
        extend("default" = SEARCH_DEFAULT_LIMIT),
        description = format!("Maximum complete Authoring Descriptions to return. Omit or use null for {SEARCH_DEFAULT_LIMIT}. The server applies this default; this annotation does not require clients to insert a value.")
    )]
    pub(crate) limit: Option<u16>,
    #[schemars(
        length(min = 1),
        description = "Optional opaque query-bound continuation cursor, at most 8192 UTF-8 bytes. Reuse the returned next_cursor unchanged with the same query and Provider."
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = DESCRIBE_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        length(min = 1),
        description = "Complete Call Path: coral.providers.<namespace>.<operation>, coral.admin.<operation> or coral.functions.<path>, not a relative path. Every segment must be a safe JavaScript identifier; Unicode identifiers are supported. For Saved Functions, the relative path has at most 16 segments and 256 UTF-8 bytes (not characters); reserved segments __proto__, prototype, constructor and then are forbidden. The complete path is at most 1024 UTF-8 bytes (not characters). Identifier and byte constraints are checked by the domain."
    )]
    pub(crate) path: String,
    #[schemars(
        length(max = 32),
        description = "Names of types in this path's declaration, as a describe result's comments name them. Returns those types whole instead of the inputs and the function, and a Saved Function without its source. At most 32 names of at most 256 UTF-8 bytes each, checked by the domain."
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
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
        description = "One or two sentences on what the function does and when to use it. Coral shows its TypeScript declaration beside the description, so do not restate input or return types. One line: no line breaks, surrounding whitespace, or other control characters (including null). At most 4 KiB (4096 UTF-8 bytes, not characters), checked by the domain."
    )]
    pub(crate) description: String,
    #[schemars(
        length(min = 1),
        description = "Nonempty no-import TypeScript module with one default callable export. No null characters. At most 64 KiB (65536 UTF-8 bytes, not characters), checked by the domain."
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(
        length(min = 1, max = TASK_INTENT_MAX_CHARS),
        description = CALL_INTENT_DESCRIPTION
    )]
    pub(crate) intent: String,
    #[schemars(
        description = "TypeScript async-function body to execute. Empty input is valid. No null characters. At most 64 KiB (65536 UTF-8 bytes, not characters), checked by the domain."
    )]
    pub(crate) program: String,
    #[schemars(
        range(min = 1, max = RUN_TIMEOUT_MAX_MS),
        extend("default" = DEFAULT_RUN_TIMEOUT_MS),
        description = format!("Optional positive Run timeout in milliseconds, up to 15 minutes. Omit or use null for {DEFAULT_RUN_TIMEOUT_MS} milliseconds. One wall-clock deadline starts at durable acceptance; observation and human action do not renew it. The server applies this default; this annotation does not require clients to insert a value.")
    )]
    pub(crate) run_timeout_ms: Option<u64>,
    #[schemars(
        range(min = 0, max = MAX_WAIT_MS),
        extend("default" = DEFAULT_EXEC_WAIT_MS),
        description = format!("Initial observation wait in milliseconds, independent of the Run lifetime. Omit or use null for {DEFAULT_EXEC_WAIT_MS} milliseconds. Zero returns immediately. Batches authored output until the Run ends, human action is required, or this wait expires. The server applies this default; this annotation does not require clients to insert a value.")
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
        schema_with = "uuid_schema",
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
        description = format!("Optional wait in milliseconds for authored output, changed required actions, or terminal state. Omit or use null for {DEFAULT_WAIT_MS} milliseconds. Zero reads immediately. The server applies this default; this annotation does not require clients to insert a value.")
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
        schema_with = "uuid_schema",
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
    #[schemars(schema_with = "uuid_schema", description = TASK_ID_DESCRIPTION)]
    pub(crate) task_id: String,
    #[schemars(description = "Terminal Task outcome.")]
    pub(crate) outcome: TaskOutcomeArgument,
    #[schemars(
        length(min = 1, max = TASK_FEEDBACK_MAX_CHARS),
        description = "Optional completion feedback for either outcome. Nonblank, without null characters. At most 4096 Unicode characters, including whitespace; not trimmed. Domain-validated and private with the Task."
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
    serde_json::from_value(Value::Object(arguments.unwrap_or_default())).map_err(|error| {
        ErrorData::invalid_params(
            format!("invalid arguments for '{}': {error}", T::NAME),
            None,
        )
    })
}

pub(crate) fn tools() -> Vec<Tool> {
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
            "Start a new Program Run from one TypeScript async-function body. Observe it with `wait`. Return only the values the answer needs: select fields and filter items inside the Program, and do not return or emit a whole Operation Result. Log diagnostics with `console.log`. A thrown `coral.OperationError` is a known Coral failure; you can report its code and trace identifier.",
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
}

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

    /// The `types` schema and description state the limits the domain
    /// enforces. The byte limit stays in the description: JSON Schema's
    /// `maxLength` counts characters, not bytes.
    #[test]
    fn describe_types_schema_states_the_domain_limits() {
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
        let description = types
            .get("description")
            .and_then(Value::as_str)
            .expect("types description");
        let limits = format!(
            "At most {} names of at most {} UTF-8 bytes each",
            coral_search::DESCRIBE_TYPES_MAX,
            coral_search::DESCRIBE_TYPE_NAME_MAX_BYTES
        );
        assert!(description.contains(&limits), "{description}");
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
