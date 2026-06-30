use std::{collections::HashMap, sync::Arc};

use rmcp::ErrorData;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use super::{ToolError, schema::tool_input_schema, schema::tool_output_schema};

pub(crate) const MAX_SQL_BATCH_QUERIES: usize = 10;
const MAX_SQL_BATCH_RESULT_INDEX: usize = MAX_SQL_BATCH_QUERIES - 1;

#[derive(JsonSchema)]
pub(crate) struct SqlArguments {
    #[schemars(
        length(min = 1, max = MAX_SQL_BATCH_QUERIES),
        inner(length(min = 1), pattern(r"\S")),
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
    PartialFailure(SqlPartialFailureOutput),
}

#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
#[expect(
    dead_code,
    reason = "schema-only struct for the SQL partial-failure envelope"
)]
struct SqlPartialFailureOutput {
    error: ToolError,
    data: SqlBatchValue,
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
        Self {
            total_count: results.len(),
            success_count,
            error_count: results.len() - success_count,
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
            Self::Success { index, .. } | Self::Error { index, .. } => *index,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Success { .. } => false,
            Self::Error { error, .. } => error.retryable,
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
