use std::{collections::HashMap, sync::Arc};

use rmcp::ErrorData;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{ToolError, schema::json_object_schema, tool_error_output_schema};

pub(crate) const MAX_SQL_BATCH_QUERIES: usize = 10;
const MAX_SQL_BATCH_RESULT_INDEX: usize = MAX_SQL_BATCH_QUERIES - 1;

pub(crate) struct SqlArguments {
    pub(crate) queries: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct SqlBatchValue {
    total_count: usize,
    success_count: usize,
    error_count: usize,
    results: Vec<SqlQueryResultValue>,
}

#[derive(Serialize)]
#[serde(tag = "status")]
pub(crate) enum SqlQueryResultValue {
    #[serde(rename = "success")]
    Success { index: usize, rows: Vec<Value> },
    #[serde(rename = "error")]
    Error { index: usize, error: ToolError },
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
    json_object_schema(&json!({
        "type": "object",
        "required": ["queries"],
        "properties": {
            "queries": {
                "type": "array",
                "description": "One to ten independent read-only SQL statements to execute against Coral. Entries must not depend on one another's rows, errors, or side effects.",
                "minItems": 1,
                "maxItems": MAX_SQL_BATCH_QUERIES,
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "pattern": r"\S"
                }
            }
        }
    }))
}

pub(crate) fn sql_output_schema() -> Arc<Map<String, Value>> {
    let result_index_max = MAX_SQL_BATCH_RESULT_INDEX;
    json_object_schema(&json!({
        "type": "object",
        "oneOf": [
            { "$ref": "#/$defs/sql_batch" },
            {
                "type": "object",
                "required": ["error", "data"],
                "additionalProperties": false,
                "properties": {
                    "error": { "$ref": "#/$defs/tool_error" },
                    "data": { "$ref": "#/$defs/sql_batch" }
                }
            }
        ],
        "$defs": {
            "sql_batch": sql_batch_output_schema(result_index_max),
            "tool_error": tool_error_output_schema()
        }
    }))
}

fn sql_batch_output_schema(result_index_max: usize) -> Value {
    json!({
        "type": "object",
        "required": ["total_count", "success_count", "error_count", "results"],
        "additionalProperties": false,
        "properties": {
            "total_count": { "type": "integer", "minimum": 1, "maximum": MAX_SQL_BATCH_QUERIES },
            "success_count": { "type": "integer", "minimum": 0, "maximum": MAX_SQL_BATCH_QUERIES },
            "error_count": { "type": "integer", "minimum": 0, "maximum": MAX_SQL_BATCH_QUERIES },
            "results": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_SQL_BATCH_QUERIES,
                "items": {
                    "oneOf": [
                        {
                            "type": "object",
                            "required": ["index", "status", "rows"],
                            "additionalProperties": false,
                            "properties": {
                                "index": { "type": "integer", "minimum": 0, "maximum": result_index_max },
                                "status": { "const": "success" },
                                "rows": { "type": "array", "items": { "type": "object" } }
                            }
                        },
                        {
                            "type": "object",
                            "required": ["index", "status", "error"],
                            "additionalProperties": false,
                            "properties": {
                                "index": { "type": "integer", "minimum": 0, "maximum": result_index_max },
                                "status": { "const": "error" },
                                "error": { "$ref": "#/$defs/tool_error" }
                            }
                        }
                    ]
                }
            }
        },
    })
}
