use std::{collections::HashMap, sync::Arc};

use rmcp::ErrorData;
use rmcp::model::{Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use super::context::ToolDescriptionContext;
use super::errors::{ToolError, ToolErrorWithData};
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;

pub(crate) const MAX_SQL_BATCH_QUERIES: usize = 10;
const MAX_SQL_BATCH_RESULT_INDEX: usize = MAX_SQL_BATCH_QUERIES - 1;

#[derive(JsonSchema)]
pub(crate) struct SqlArguments {
    #[schemars(
        length(min = 1, max = MAX_SQL_BATCH_QUERIES),
        inner(length(min = 1)),
        description = "One to ten independent read-only SQL statements to execute against Coral. Entries must not depend on one another's rows, errors, or side effects."
    )]
    pub(crate) queries: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct SqlBatchValue {
    #[schemars(range(min = 1, max = MAX_SQL_BATCH_QUERIES))]
    total_count: usize,
    #[schemars(range(min = 0, max = MAX_SQL_BATCH_QUERIES))]
    success_count: usize,
    #[schemars(range(min = 0, max = MAX_SQL_BATCH_QUERIES))]
    error_count: usize,
    #[schemars(length(min = 1, max = MAX_SQL_BATCH_QUERIES))]
    results: Vec<SqlQueryResultValue>,
}

#[derive(Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct SqlGuideValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) catalog: Option<String>,
    pub(crate) schema: String,
    pub(crate) resource: String,
    guide: String,
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) id: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct SqlGuideBlockValue {
    message: &'static str,
    #[schemars(length(min = 1))]
    guides: Vec<SqlGuideValue>,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "status")]
pub(crate) enum SqlQueryResultValue {
    #[serde(rename = "success")]
    Success {
        #[schemars(range(min = 0, max = MAX_SQL_BATCH_RESULT_INDEX))]
        index: usize,
        #[schemars(schema_with = "json_object_array_schema")]
        rows: Vec<Value>,
    },
    #[serde(rename = "error")]
    Error {
        #[schemars(range(min = 0, max = MAX_SQL_BATCH_RESULT_INDEX))]
        index: usize,
        error: ToolError,
    },
    #[serde(rename = "guide_required")]
    GuideRequired {
        #[schemars(range(min = 0, max = MAX_SQL_BATCH_RESULT_INDEX))]
        index: usize,
        #[schemars(length(min = 1))]
        guides: Vec<SqlGuideValue>,
    },
}

#[derive(JsonSchema)]
#[serde(untagged)]
#[schemars(extend("type" = "object"))]
#[expect(
    dead_code,
    reason = "schema-only enum for the SQL tool output contract"
)]
enum SqlToolOutputSchema {
    Success(SqlBatchValue),
    PartialFailure(ToolErrorWithData<SqlBatchValue>),
    GuideBlock(SqlGuideBlockValue),
}

impl SqlGuideValue {
    pub(crate) fn new(
        catalog: Option<String>,
        schema: String,
        resource: String,
        guide: String,
        id: String,
    ) -> Self {
        Self {
            catalog,
            schema,
            resource,
            guide,
            id,
        }
    }
}

impl SqlGuideBlockValue {
    pub(crate) fn new(guides: Vec<SqlGuideValue>) -> Self {
        Self {
            message: "Coral blocked this SQL call because one or more referenced resources have require_guide_read enabled. No queries in this call were executed. Read the guidance below. These guide versions are now unblocked for the remainder of this task. Retry the SQL unchanged if it follows the guidance, or revise it before trying again.",
            guides,
        }
    }
}

impl SqlBatchValue {
    pub(crate) fn from_unordered(mut results: Vec<SqlQueryResultValue>) -> Self {
        results.sort_by_key(SqlQueryResultValue::index);

        for (expected_index, result) in results.iter().enumerate() {
            debug_assert_eq!(result.index(), expected_index);
        }

        let success_count = results
            .iter()
            .filter(|result| matches!(result, SqlQueryResultValue::Success { .. }))
            .count();
        let error_count = results
            .iter()
            .filter(|result| matches!(result, SqlQueryResultValue::Error { .. }))
            .count();
        Self {
            total_count: results.len(),
            success_count,
            error_count,
            results,
        }
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.error_count > 0
    }

    pub(crate) fn partial_failure_error(&self) -> ToolError {
        ToolError {
            summary: "One or more SQL queries failed".to_string(),
            detail: "Inspect `data.results` for per-query successes and errors.".to_string(),
            hint: None,
            grpc_code: tonic::Code::Unknown.to_string(),
            reason: Some("SQL_BATCH_PARTIAL_FAILURE".to_string()),
            retryable: self.results.iter().any(SqlQueryResultValue::is_retryable),
            metadata: HashMap::from([
                ("total_count".to_string(), self.total_count.to_string()),
                ("success_count".to_string(), self.success_count.to_string()),
                ("error_count".to_string(), self.error_count.to_string()),
            ]),
        }
    }
}

impl SqlQueryResultValue {
    fn index(&self) -> usize {
        match self {
            Self::Success { index, .. }
            | Self::Error { index, .. }
            | Self::GuideRequired { index, .. } => *index,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Error { error, .. } => error.retryable,
            Self::Success { .. } | Self::GuideRequired { .. } => false,
        }
    }

    pub(crate) fn required_guides(&self) -> &[SqlGuideValue] {
        match self {
            Self::GuideRequired { guides, .. } => guides,
            Self::Success { .. } | Self::Error { .. } => &[],
        }
    }
}

pub(crate) fn sql_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SqlArguments, ErrorData> {
    let queries = arguments
        .and_then(|arguments| arguments.get("queries"))
        .and_then(Value::as_array)
        .ok_or_else(|| ErrorData::invalid_params("argument 'queries' must be an array", None))?;
    if queries.is_empty() {
        return Err(ErrorData::invalid_params(
            "argument 'queries' must contain at least 1 query",
            None,
        ));
    }
    if queries.len() > MAX_SQL_BATCH_QUERIES {
        return Err(ErrorData::invalid_params(
            format!("argument 'queries' must contain at most {MAX_SQL_BATCH_QUERIES} queries"),
            None,
        ));
    }
    let queries = queries
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let query = value.as_str().ok_or_else(|| {
                ErrorData::invalid_params(
                    format!("argument 'queries[{index}]' must be a string"),
                    None,
                )
            })?;
            let query = query.trim();
            if query.is_empty() {
                return Err(ErrorData::invalid_params(
                    format!("argument 'queries[{index}]' must not be empty"),
                    None,
                ));
            }
            Ok(query.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlArguments { queries })
}

pub(crate) fn sql_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        ToolName::Sql.as_str(),
        sql_tool_description(context),
        sql_input_schema(),
    )
    .with_raw_output_schema(sql_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Run SQL")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(true),
    )
}

pub(crate) fn sql_input_schema() -> Arc<Map<String, Value>> {
    tool_input_schema::<SqlArguments>()
}

pub(crate) fn sql_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<SqlToolOutputSchema>()
}

fn json_object_array_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": { "type": "object" }
    })
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::SqlGuideValue;

    #[test]
    fn required_guide_value_keeps_catalog_identity() {
        let guide = SqlGuideValue::new(
            Some("github_v4".to_string()),
            "issues".to_string(),
            "list_for_repo".to_string(),
            "Use repository lookup.".to_string(),
            "guide-id".to_string(),
        );

        assert_eq!(
            serde_json::to_value(guide).expect("guide serializes"),
            json!({
                "catalog": "github_v4",
                "schema": "issues",
                "resource": "list_for_repo",
                "guide": "Use repository lookup."
            })
        );
    }
}
