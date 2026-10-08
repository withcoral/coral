use std::sync::Arc;

use coral_client::SearchResponseValue;
use rmcp::ErrorData;
use rmcp::model::{Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde_json::{Map, Value};

use super::arguments::{optional_u32_argument, required_string_argument};
use super::context::ToolDescriptionContext;
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;

const DEFAULT_SEARCH_LIMIT: u32 = 10;
const MIN_SEARCH_LIMIT: u32 = 1;
const MAX_SEARCH_LIMIT: u32 = 50;
const CATALOG_QUERY_DESCRIPTION: &str =
    "Natural language text for finding relevant Coral catalog entries.";
const OBSERVED_VALUES_QUERY_DESCRIPTION: &str = "Natural language text for finding relevant Coral catalog entries or values observed during earlier queries.";

#[derive(JsonSchema)]
pub(crate) struct SearchArguments {
    #[schemars(
        length(min = 1),
        description = CATALOG_QUERY_DESCRIPTION
    )]
    pub(crate) query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(
        range(min = MIN_SEARCH_LIMIT, max = MAX_SEARCH_LIMIT),
        description = "Maximum search results to return, from 1 to 50. Defaults to 10."
    )]
    pub(crate) limit: u32,
}

pub(crate) fn search_tool(
    context: &ToolDescriptionContext,
    observed_values_search_enabled: bool,
) -> Tool {
    let mut input_schema = tool_input_schema::<SearchArguments>();
    let query_schema = Arc::make_mut(&mut input_schema)
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .and_then(|properties| properties.get_mut("query"))
        .and_then(Value::as_object_mut)
        .expect("search query input schema");
    query_schema.insert(
        "description".to_string(),
        Value::String(
            if observed_values_search_enabled {
                OBSERVED_VALUES_QUERY_DESCRIPTION
            } else {
                CATALOG_QUERY_DESCRIPTION
            }
            .to_string(),
        ),
    );

    Tool::new(
        ToolName::Search.as_str(),
        search_description(context, observed_values_search_enabled),
        input_schema,
    )
    .with_raw_output_schema(tool_output_schema::<SearchResponseValue<'static>>())
    .with_annotations(
        ToolAnnotations::with_title("Search Coral")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn search_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchArguments, ErrorData> {
    Ok(SearchArguments {
        query: required_string_argument(arguments, "query")?,
        limit: optional_u32_argument(
            arguments,
            "limit",
            DEFAULT_SEARCH_LIMIT,
            MIN_SEARCH_LIMIT,
            MAX_SEARCH_LIMIT,
        )?,
    })
}

fn search_description(
    context: &ToolDescriptionContext,
    observed_values_search_enabled: bool,
) -> String {
    let search_scope = if observed_values_search_enabled {
        "tables, table functions, columns, filters, and locally observed values"
    } else {
        "tables, table functions, columns, and filters in Coral's local catalog"
    };
    format!(
        "Find relevant Coral {search_scope}. {} {} table(s) and {} table function(s) are currently visible. Runtime preparation may read stored credentials, initialize source providers, and inspect file metadata in local or object storage, but it does not execute your data query or return source rows. Returns typed results plus provider statuses; use `sql` to query current data.",
        context.connected_sources_sentence(),
        context.visible_table_count,
        context.visible_function_count
    )
}

fn default_search_limit() -> u32 {
    DEFAULT_SEARCH_LIMIT
}
