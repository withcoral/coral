use std::sync::Arc;

use rmcp::ErrorData;
use serde::Serialize;
use serde_json::{Map, Value, json};

pub(crate) const MAX_SQL_BATCH_QUERIES: usize = 10;

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
}

impl SqlBatchValue {
    pub(crate) fn from_successes(results: Vec<SqlQueryResultValue>) -> Self {
        Self {
            total_count: results.len(),
            success_count: results.len(),
            error_count: 0,
            results,
        }
    }

    pub(crate) fn from_unordered(
        mut results: Vec<SqlQueryResultValue>,
    ) -> Result<Self, tonic::Status> {
        results.sort_by_key(SqlQueryResultValue::index);

        for (expected_index, result) in results.iter().enumerate() {
            let actual_index = result.index();
            if actual_index != expected_index {
                return Err(tonic::Status::internal(format!(
                    "SQL batch result index mismatch: expected {expected_index}, found {actual_index}"
                )));
            }
        }

        Ok(Self::from_successes(results))
    }
}

impl SqlQueryResultValue {
    fn index(&self) -> usize {
        match self {
            Self::Success { index, .. } => *index,
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
                    "minLength": 1
                }
            }
        }
    }))
}

pub(crate) fn sql_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
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
                    "type": "object",
                    "required": ["index", "status", "rows"],
                    "additionalProperties": false,
                    "properties": {
                        "index": { "type": "integer", "minimum": 0, "maximum": 9 },
                        "status": { "const": "success" },
                        "rows": { "type": "array", "items": { "type": "object" } }
                    }
                }
            }
        }
    }))
}

fn json_object_schema(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(
        value
            .as_object()
            .cloned()
            .expect("tool schemas should be JSON objects"),
    )
}
