use std::sync::Arc;

use rmcp::{
    ErrorData,
    model::{CallToolResult, Tool, ToolAnnotations},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value, json};

use coral_api::{CORAL_EPISODE_ID_MAX_LEN, CORAL_EPISODE_INTENT_MAX_CHARS};

use super::{
    CatalogToolKind, DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET,
    DEFAULT_SEARCH_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MAX_SEARCH_PAGINATION_LIMIT,
    MIN_PAGINATION_LIMIT, Pagination, connected_source_names_text, describe_table_output_schema,
    list_catalog_output_schema, list_columns_output_schema, parse_pagination,
    parse_search_pagination, schema::json_schema_value, schema::tool_input_schema,
    schema::tool_output_schema, search_catalog_output_schema, sql_input_schema, sql_output_schema,
};

const EPISODE_ID_ARGUMENT_DESCRIPTION: &str = "Optional episode id returned by open_episode. Pass it on subsequent Coral tool calls for the same task so Coral can attribute the call to that episode.";
const EPISODE_ID_JSON_SCHEMA_PATTERN: &str = "^[!-~]+$";
const DEFAULT_IGNORE_CASE: bool = true;
const DEFAULT_REQUIRED_ONLY: bool = false;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolDescriptionContext {
    pub(crate) visible_table_count: usize,
    pub(crate) visible_function_count: usize,
    connected_source_names: Vec<String>,
}

impl ToolDescriptionContext {
    pub(crate) fn new(
        visible_table_count: usize,
        visible_function_count: usize,
        mut connected_source_names: Vec<String>,
    ) -> Self {
        connected_source_names.sort();
        connected_source_names.dedup();
        Self {
            visible_table_count,
            visible_function_count,
            connected_source_names,
        }
    }

    fn connected_sources_sentence(&self) -> String {
        connected_source_names_text(&self.connected_source_names).map_or_else(
            || "No connected user sources are currently configured.".to_string(),
            |names| format!("Connected sources/schemas include: {names}."),
        )
    }
}

#[derive(JsonSchema)]
pub(crate) struct ListCatalogArguments {
    #[schemars(description = "Optional exact SQL schema name to list.")]
    pub(crate) schema: Option<String>,
    #[schemars(
        description = "Optional item kind to list. Omit or pass null to list all catalog items."
    )]
    pub(crate) kind: Option<CatalogToolKind>,
    #[schemars(flatten, with = "DefaultPaginationInput")]
    pub(crate) pagination: Pagination,
}

#[derive(JsonSchema)]
pub(crate) struct SearchCatalogArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Rust regex pattern to match database catalog metadata."
    )]
    pub(crate) pattern: String,
    #[schemars(description = "Optional exact SQL schema name to search.")]
    pub(crate) schema: Option<String>,
    #[schemars(
        description = "Optional item kind to search. Omit or pass null to search all catalog items."
    )]
    pub(crate) kind: Option<CatalogToolKind>,
    #[serde(default = "default_ignore_case")]
    #[schemars(description = "Whether regex matching is case-insensitive. Defaults to true.")]
    pub(crate) ignore_case: bool,
    #[schemars(flatten, with = "SearchPaginationInput")]
    pub(crate) pagination: Pagination,
}

#[derive(JsonSchema)]
pub(crate) struct DescribeTableArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact SQL schema name."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
}

#[derive(JsonSchema)]
pub(crate) struct ListColumnsArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact SQL schema name."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Optional Rust regex matched against column names, descriptions, and data types."
    )]
    pub(crate) pattern: Option<String>,
    #[serde(default = "default_ignore_case")]
    #[schemars(description = "Whether regex matching is case-insensitive. Defaults to true.")]
    pub(crate) ignore_case: bool,
    #[serde(default = "default_required_only")]
    #[schemars(description = "Only return columns that are required filters. Defaults to false.")]
    pub(crate) required_only: bool,
    #[schemars(flatten, with = "DefaultPaginationInput")]
    pub(crate) pagination: Pagination,
}

#[derive(JsonSchema)]
pub(crate) struct FeedbackArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "What you were trying to do."
    )]
    pub(crate) trying_to_do: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "What you already tried."
    )]
    pub(crate) tried: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Where you got blocked."
    )]
    pub(crate) stuck: String,
}

#[derive(JsonSchema)]
pub(crate) struct OpenEpisodeArguments {
    #[schemars(
        length(min = 1, max = CORAL_EPISODE_INTENT_MAX_CHARS),
        pattern(r"\S"),
        description = "Natural-language description of the task this episode should group."
    )]
    pub(crate) intent: String,
    #[schemars(
        with = "Option<EpisodeIdSchema>",
        description = "Optional parent episode id when this task is a child of an existing episode."
    )]
    pub(crate) parent_episode_id: Option<String>,
}

#[derive(JsonSchema)]
#[expect(
    dead_code,
    reason = "schema-only struct for flattened default pagination inputs"
)]
struct DefaultPaginationInput {
    #[serde(default = "default_pagination_limit")]
    #[schemars(
        range(min = MIN_PAGINATION_LIMIT, max = MAX_PAGINATION_LIMIT),
        description = "Maximum items to return, from 1 to 200. Defaults to 50."
    )]
    limit: u32,
    #[serde(default = "default_pagination_offset")]
    #[schemars(
        range(min = DEFAULT_PAGINATION_OFFSET, max = u32::MAX),
        description = "Number of matching items to skip. Defaults to 0."
    )]
    offset: u32,
}

#[derive(JsonSchema)]
#[expect(
    dead_code,
    reason = "schema-only struct for flattened search pagination inputs"
)]
struct SearchPaginationInput {
    #[serde(default = "default_search_pagination_limit")]
    #[schemars(
        range(min = MIN_PAGINATION_LIMIT, max = MAX_SEARCH_PAGINATION_LIMIT),
        description = "Maximum catalog items to return, from 1 to 100. Defaults to 20."
    )]
    limit: u32,
    #[serde(default = "default_pagination_offset")]
    #[schemars(
        range(min = DEFAULT_PAGINATION_OFFSET, max = u32::MAX),
        description = "Number of matching catalog items to skip. Defaults to 0."
    )]
    offset: u32,
}

#[derive(JsonSchema)]
#[schemars(transparent, inline)]
#[expect(dead_code, reason = "schema-only type for episode id constraints")]
struct EpisodeIdSchema(
    #[schemars(
        length(min = 1, max = CORAL_EPISODE_ID_MAX_LEN),
        regex(pattern = EPISODE_ID_JSON_SCHEMA_PATTERN)
    )]
    String,
);

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct FeedbackStoredValue {
    pub(crate) feedback_id: String,
    pub(crate) created_at: String,
    pub(crate) message: &'static str,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct EpisodeOpenedValue {
    #[schemars(with = "EpisodeIdSchema")]
    pub(crate) episode_id: String,
    #[schemars(with = "Option<EpisodeIdSchema>")]
    pub(crate) parent_episode_id: Option<String>,
    pub(crate) message: &'static str,
    pub(crate) instructions: &'static str,
}

pub(crate) fn sql_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new("sql", sql_tool_description(context), sql_input_schema())
        .with_raw_output_schema(sql_output_schema())
        .with_annotations(
            ToolAnnotations::with_title("Run SQL")
                .read_only(true)
                .destructive(false)
                .idempotent(true)
                .open_world(true),
        )
}

pub(crate) fn list_catalog_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        "list_catalog",
        format!(
            "List database catalog items for Coral sources. {} {} table(s) and {} table function(s) are currently visible.",
            context.connected_sources_sentence(),
            context.visible_table_count,
            context.visible_function_count
        ),
        tool_input_schema::<ListCatalogArguments>(),
    )
    .with_raw_output_schema(list_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Catalog")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn search_catalog_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        "search_catalog",
        search_catalog_description(context),
        tool_input_schema::<SearchCatalogArguments>(),
    )
    .with_raw_output_schema(search_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Search Catalog")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn describe_table_tool() -> Tool {
    Tool::new(
        "describe_table",
        "Describe one database table without returning full column definitions.",
        tool_input_schema::<DescribeTableArguments>(),
    )
    .with_raw_output_schema(describe_table_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Describe Table")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn list_columns_tool() -> Tool {
    Tool::new(
        "list_columns",
        "List columns for one database table with optional regex and required-filter narrowing.",
        tool_input_schema::<ListColumnsArguments>(),
    )
    .with_raw_output_schema(list_columns_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Columns")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn feedback_tool() -> Tool {
    Tool::new(
        "feedback",
        "Submit feedback when you are blocked. Coral stores the report locally and uploads an anonymous copy, without user identifiers, to Coral's hosted feedback service to improve Coral's performance.",
        tool_input_schema::<FeedbackArguments>(),
    )
    .with_raw_output_schema(feedback_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Store Feedback Report")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true),
    )
}

pub(crate) fn open_episode_tool() -> Tool {
    Tool::new(
        "open_episode",
        "Open a Coral episode for the current task. Call this once at the start of a task, then pass the returned episode_id on subsequent Coral tool calls for that task.",
        tool_input_schema::<OpenEpisodeArguments>(),
    )
    .with_raw_output_schema(open_episode_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Open Episode")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

pub(crate) fn required_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<String, ErrorData> {
    let value = arguments
        .and_then(|arguments| arguments.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("missing string argument '{key}'"), None)
        })?;
    Ok(value.to_string())
}

pub(crate) fn open_episode_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<OpenEpisodeArguments, ErrorData> {
    Ok(OpenEpisodeArguments {
        intent: required_string_argument(arguments, "intent")?,
        parent_episode_id: optional_episode_id_argument(arguments, "parent_episode_id")?,
    })
}

pub(crate) fn feedback_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<FeedbackArguments, ErrorData> {
    Ok(FeedbackArguments {
        trying_to_do: required_string_argument(arguments, "trying_to_do")?,
        tried: required_string_argument(arguments, "tried")?,
        stuck: required_string_argument(arguments, "stuck")?,
    })
}

pub(crate) fn list_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListCatalogArguments, ErrorData> {
    Ok(ListCatalogArguments {
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn search_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchCatalogArguments, ErrorData> {
    Ok(SearchCatalogArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", DEFAULT_IGNORE_CASE)?,
        pagination: parse_search_pagination(arguments)?,
    })
}

fn optional_catalog_kind_argument(
    arguments: Option<&Map<String, Value>>,
) -> Result<Option<CatalogToolKind>, ErrorData> {
    let Some(kind) = optional_string_argument(arguments, "kind")? else {
        return Ok(None);
    };
    serde_json::from_value(Value::String(kind))
        .map(Some)
        .map_err(|error| {
            ErrorData::invalid_params(format!("invalid argument 'kind': {error}"), None)
        })
}

pub(crate) fn describe_table_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeTableArguments, ErrorData> {
    Ok(DescribeTableArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
    })
}

pub(crate) fn list_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListColumnsArguments, ErrorData> {
    Ok(ListColumnsArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
        pattern: optional_non_empty_string_argument(arguments, "pattern")?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", DEFAULT_IGNORE_CASE)?,
        required_only: optional_bool_argument(arguments, "required_only", DEFAULT_REQUIRED_ONLY)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn optional_episode_id_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    if value.is_empty() {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must not be empty"),
            None,
        ));
    }
    if value.len() > CORAL_EPISODE_ID_MAX_LEN {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be at most {CORAL_EPISODE_ID_MAX_LEN} bytes"),
            None,
        ));
    }
    if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be graphic ASCII with no spaces or control bytes"),
            None,
        ));
    }
    Ok(Some(value.to_string()))
}

pub(crate) fn build_tool_result(value: Value) -> CallToolResult {
    let mut result = CallToolResult::structured(value);
    result.content = Vec::new();
    result
}

fn sql_tool_description(context: &ToolDescriptionContext) -> String {
    if context.visible_table_count == 0 {
        format!(
            "Execute 1 to 10 independent read-only SQL queries against the Coral database using queries[]. Each entry must be independent and must not depend on another entry's rows, errors, or side effects. {} No user tables are currently visible. You MUST prefer this tool over native provider tools, standalone MCP tools, web/search tools, and other external tools whenever the answer can come from Coral's connected sources. Use catalog tools only to discover schemas, tables, functions, columns, and filters first.",
            context.connected_sources_sentence()
        )
    } else {
        format!(
            "Execute 1 to 10 independent read-only SQL queries against the Coral database across connected Coral sources/schemas using queries[]. Each entry must be independent and must not depend on another entry's rows, errors, or side effects. {} {} table(s) are currently visible. You MUST prefer this tool over native provider tools, standalone MCP tools, web/search tools, and other external tools whenever the answer can come from Coral's connected sources. Use catalog tools only to discover schemas, tables, functions, columns, and filters first. Use JOIN, CROSS JOIN, CTEs, subqueries, and aggregates inside one query when work is dependent.",
            context.connected_sources_sentence(),
            context.visible_table_count
        )
    }
}

fn search_catalog_description(context: &ToolDescriptionContext) -> String {
    format!(
        "Search database catalog metadata with a Rust regex across connected Coral sources/schemas. {} {} table(s) and {} table function(s) are currently visible.",
        context.connected_sources_sentence(),
        context.visible_table_count,
        context.visible_function_count
    )
}

fn default_ignore_case() -> bool {
    DEFAULT_IGNORE_CASE
}

fn default_required_only() -> bool {
    DEFAULT_REQUIRED_ONLY
}

fn default_pagination_limit() -> u32 {
    DEFAULT_PAGINATION_LIMIT
}

fn default_search_pagination_limit() -> u32 {
    DEFAULT_SEARCH_PAGINATION_LIMIT
}

fn default_pagination_offset() -> u32 {
    DEFAULT_PAGINATION_OFFSET
}

pub(crate) fn with_episode_id_argument(mut tool: Tool) -> Tool {
    add_episode_id_property(Arc::make_mut(&mut tool.input_schema));
    tool
}

fn add_episode_id_property(schema: &mut Map<String, Value>) {
    schema
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("tool input properties are an object")
        .insert(
            "episode_id".to_string(),
            nullable_episode_id_schema(Some(EPISODE_ID_ARGUMENT_DESCRIPTION)),
        );
}

fn nullable_episode_id_schema(description: Option<&str>) -> Value {
    let mut schema = json_schema_value::<Option<EpisodeIdSchema>>();
    if let Some(description) = description {
        schema
            .as_object_mut()
            .expect("nullable episode id schema is an object")
            .insert("description".to_string(), json!(description));
    }
    schema
}

fn feedback_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<FeedbackStoredValue>()
}

fn open_episode_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<EpisodeOpenedValue>()
}

pub(crate) fn optional_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_non_empty_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    let value = value.trim();
    if value.is_empty() {
        Err(ErrorData::invalid_params(
            format!("argument '{key}' must not be empty"),
            None,
        ))
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_bool_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: bool,
) -> Result<bool, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    value.as_bool().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a boolean"), None)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value, json};

    use super::{
        DEFAULT_IGNORE_CASE, DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET,
        DEFAULT_REQUIRED_ONLY, DEFAULT_SEARCH_PAGINATION_LIMIT, EPISODE_ID_ARGUMENT_DESCRIPTION,
        ToolDescriptionContext, build_tool_result, connected_source_names_text,
        list_catalog_arguments, search_catalog_arguments, search_catalog_tool, sql_tool,
        with_episode_id_argument,
    };

    #[test]
    fn success_tool_result_uses_structured_content_only() {
        let value = json!({
            "rows": [
                {
                    "id": 1,
                    "text": "hello"
                },
                {
                    "id": 2,
                    "text": "world"
                }
            ]
        });

        let result = build_tool_result(value.clone());

        assert!(result.content.is_empty());
        assert_eq!(
            result.structured_content.expect("structured content"),
            value
        );
    }

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);
        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");
        assert_eq!(list.kind, None);

        arguments.insert("pattern".to_string(), Value::String("issue".to_string()));
        let search = search_catalog_arguments(Some(&arguments)).expect("search arguments");
        assert_eq!(search.kind, None);
    }

    #[test]
    fn list_columns_pattern_accepts_null_as_omitted() {
        let arguments = Map::from_iter([
            ("schema".to_string(), Value::String("github".to_string())),
            ("table".to_string(), Value::String("issues".to_string())),
            ("pattern".to_string(), Value::Null),
        ]);

        let parsed = super::list_columns_arguments(Some(&arguments)).expect("list columns args");

        assert_eq!(parsed.pattern, None);
    }

    #[test]
    fn catalog_argument_defaults_use_shared_constants() {
        let search_arguments =
            Map::from_iter([("pattern".to_string(), Value::String("issue".to_string()))]);

        let search = search_catalog_arguments(Some(&search_arguments)).expect("search args");

        assert_eq!(search.ignore_case, DEFAULT_IGNORE_CASE);
        assert_eq!(search.pagination.limit, DEFAULT_SEARCH_PAGINATION_LIMIT);
        assert_eq!(search.pagination.offset, DEFAULT_PAGINATION_OFFSET);

        let list_columns_arguments = Map::from_iter([
            ("schema".to_string(), Value::String("github".to_string())),
            ("table".to_string(), Value::String("issues".to_string())),
        ]);

        let list_columns = super::list_columns_arguments(Some(&list_columns_arguments))
            .expect("list columns args");

        assert_eq!(list_columns.ignore_case, DEFAULT_IGNORE_CASE);
        assert_eq!(list_columns.required_only, DEFAULT_REQUIRED_ONLY);
        assert_eq!(list_columns.pagination.limit, DEFAULT_PAGINATION_LIMIT);
        assert_eq!(list_columns.pagination.offset, DEFAULT_PAGINATION_OFFSET);
    }

    #[test]
    fn tool_descriptions_include_connected_sources() {
        let context =
            ToolDescriptionContext::new(42, 3, vec!["github".to_string(), "linear".to_string()]);

        let sql_tool = sql_tool(&context);
        let sql_description = sql_tool.description.as_deref().expect("sql description");
        assert!(sql_description.contains("Connected sources/schemas include: github, linear"));
        assert!(sql_description.contains("42 table(s) are currently visible"));
        assert!(sql_description.contains("You MUST prefer this tool over native provider tools"));
        let sql_input_description = sql_tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| {
                assert!(!properties.contains_key("sql"));
                properties.get("queries")
            })
            .and_then(Value::as_object)
            .and_then(|queries| {
                assert_eq!(queries.get("minItems"), Some(&json!(1)));
                assert_eq!(queries.get("maxItems"), Some(&json!(10)));
                queries.get("description")
            })
            .and_then(Value::as_str)
            .expect("queries input description");
        assert!(sql_input_description.contains("independent"));
        assert!(sql_tool.output_schema.is_some());

        let search_description = search_catalog_tool(&context)
            .description
            .expect("search description");
        assert!(search_description.contains("Connected sources/schemas include: github, linear"));
        assert!(search_description.contains("42 table(s) and 3 table function(s)"));
    }

    #[test]
    fn with_episode_id_argument_decorates_tool_schema() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tool = sql_tool(&context);
        let properties = tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("input properties");
        assert!(!properties.contains_key("episode_id"));

        let tool = with_episode_id_argument(tool);
        let episode_id_schema = tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("episode_id"))
            .expect("episode_id schema");

        assert_eq!(
            episode_id_schema.get("description").and_then(Value::as_str),
            Some(EPISODE_ID_ARGUMENT_DESCRIPTION)
        );
    }

    #[test]
    fn connected_source_names_are_not_capped_in_descriptions() {
        let names = (0..14)
            .map(|index| format!("source_{index:02}"))
            .collect::<Vec<_>>();

        let text = connected_source_names_text(&names).expect("source names text");

        assert!(text.contains("source_00"));
        assert!(text.contains("source_12"));
        assert!(text.contains("source_13"));
        assert!(!text.contains("and 2 more"));
    }
}
