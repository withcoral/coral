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

#[derive(JsonSchema)]
pub(crate) struct SearchArguments {
    #[schemars(
        length(min = 1),
        description = "Plain-language text for finding relevant Coral schemas, tables, functions, columns, or filters."
    )]
    pub(crate) query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(
        range(min = MIN_SEARCH_LIMIT, max = MAX_SEARCH_LIMIT),
        description = "Maximum search results to return, from 1 to 50. Defaults to 10."
    )]
    pub(crate) limit: u32,
}

pub(crate) fn search_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        ToolName::Search.as_str(),
        search_description(context),
        tool_input_schema::<SearchArguments>(),
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

fn search_description(context: &ToolDescriptionContext) -> String {
    format!(
        "Find relevant Coral tables, table functions, columns, and filters across connected sources/schemas. {} {} table(s) and {} table function(s) are currently visible. Returns typed results plus provider statuses; use `sql` to query the data.",
        context.connected_sources_sentence(),
        context.visible_table_count,
        context.visible_function_count
    )
}

fn default_search_limit() -> u32 {
    DEFAULT_SEARCH_LIMIT
}
