//! MCP result-handle response shaping.
//!
//! This module owns the MCP-facing large-result policy: when `sql` stays inline,
//! when it returns a process-local handle, and how `result_get` renders pages.
//! The lower-level handle lifecycle lives in `result_store`; Arrow slicing and
//! JSON-safe row rendering live in `coral-client`.

use std::sync::Arc;

use coral_client::{
    CollectedQueryResult, QueryResultError, batches_to_json_rows_json_safe_numbers,
    result_slice::{
        ColumnSummary, ResultPage, ResultSliceRequest, has_duplicate_column_names, schema_summary,
        slice_result,
    },
};
use rmcp::ErrorData;
use serde::Serialize;
use serde_json::Value;

use crate::{
    result_store::{ResultStore, ResultStoreError},
    telemetry,
};

const SQL_PREVIEW_ROWS: usize = 20;
const SQL_INLINE_BYTE_CHECK_MAX_ROWS: usize = 100;
const SQL_INLINE_MAX_BYTES: usize = 8192;
const SQL_PREVIEW_RESPONSE_MAX_BYTES: usize = 16 * 1024;
const LARGE_RESULT_GUIDANCE_MIN_ROWS: usize = 1_000;
const LARGE_RESULT_GUIDANCE: &str = "Result is large; answer from row_count or rerun the SQL with filters or aggregates instead of paging every row. If raw rows are required, call result_get with limit 500 and a columns projection.";
const DUPLICATE_COLUMN_NAMES_WARNING: &str = "Result contains duplicate column names and cannot be rendered as JSON object rows without losing data. Alias duplicate columns in the SQL query, for example SELECT a.id AS a_id, b.id AS b_id, then rerun the query.";
const PREVIEW_OMITTED_GUIDANCE: &str = "Preview omitted because it exceeds Coral's MCP response budget; call result_get with a columns projection or rerun the SQL with filters, LIMIT, or smaller expressions.";
const PREVIEW_OMITTED_LARGE_RESULT_GUIDANCE: &str = "Preview omitted because it exceeds Coral's MCP response budget. Result is large; answer from row_count or rerun the SQL with filters or aggregates instead of paging every row. If raw rows are required, call result_get with limit 500 and a columns projection.";
const RESULT_TOO_LARGE_WARNING: &str = "Result exceeded the in-memory handle limit; rerun the SQL with LIMIT, filters, or a smaller column set.";
const RESULT_TOO_LARGE_PREVIEW_OMITTED_WARNING: &str = "Result exceeded the in-memory handle limit, and its preview exceeds Coral's MCP response budget; rerun the SQL with LIMIT, filters, or a smaller column set.";

pub(crate) const RESULT_GET_DEFAULT_LIMIT: usize = 200;
pub(crate) const RESULT_GET_MAX_LIMIT: usize = 500;

#[derive(Clone, Default)]
pub(crate) struct ResultHandles {
    store: ResultStore,
}

#[derive(Serialize)]
struct SqlRowsValue {
    rows: Vec<Value>,
}

#[derive(Serialize)]
struct SqlHandledValue {
    result_id: String,
    row_count: usize,
    column_count: usize,
    columns: Vec<ColumnSummary>,
    preview: ResultPreviewValue,
    next_call: Option<NextCallValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guidance: Option<&'static str>,
}

#[derive(Serialize)]
struct SqlPreviewOnlyValue {
    preview_only: bool,
    row_count: usize,
    column_count: usize,
    columns: Vec<ColumnSummary>,
    preview: ResultPreviewValue,
    warning: &'static str,
}

#[derive(Serialize)]
struct SqlDuplicateColumnsValue {
    duplicate_column_names: bool,
    rows_omitted: bool,
    row_count: usize,
    column_count: usize,
    columns: Vec<ColumnSummary>,
    warning: &'static str,
}

#[derive(Serialize)]
struct ResultPreviewValue {
    offset: usize,
    limit: usize,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<usize>,
    rows: Vec<Value>,
}

#[derive(Serialize)]
struct NextCallValue {
    tool: &'static str,
    arguments: NextCallArgumentsValue,
}

#[derive(Serialize)]
struct NextCallArgumentsValue {
    result_id: String,
    offset: usize,
    limit: usize,
}

#[derive(Serialize)]
struct ResultGetValue {
    result_id: String,
    #[serde(flatten)]
    page: ResultPage,
}

impl ResultHandles {
    pub(crate) fn new() -> Self {
        Self {
            store: ResultStore::new(),
        }
    }

    pub(crate) fn sql_value(&self, result: CollectedQueryResult) -> Result<Value, tonic::Status> {
        // JSON object rows collapse duplicate column names, so do not render
        // any rows for these results. Require aliases instead of returning
        // corrupt data or bypassing the large-result policy.
        if has_duplicate_column_names(&result) {
            let columns = schema_summary(&result);
            telemetry::record_sql_result(
                &tracing::Span::current(),
                "duplicate_named_metadata",
                result.row_count(),
                columns.len(),
                0,
                None,
                true,
            );
            return serialize_tool_value(SqlDuplicateColumnsValue {
                duplicate_column_names: true,
                rows_omitted: true,
                row_count: result.row_count(),
                column_count: columns.len(),
                columns,
                warning: DUPLICATE_COLUMN_NAMES_WARNING,
            });
        }

        if let Some(value) = inline_sql_value_if_small(&result)? {
            return Ok(value);
        }

        let result = Arc::new(result);
        let preview = preview_page(&result)?;
        let columns = schema_summary(&result);
        match self.store.insert_result(Arc::clone(&result)) {
            Ok(result_id) => {
                let preview_rows = preview.rows.len();
                let preview_has_more = preview.has_more;
                let guidance = result_guidance(result.row_count(), false);
                let value = serialize_handled_sql_value(
                    result_id.clone(),
                    &result,
                    columns.clone(),
                    preview,
                    guidance,
                )?;
                if serialized_value_len(&value)? <= SQL_PREVIEW_RESPONSE_MAX_BYTES {
                    telemetry::record_sql_result(
                        &tracing::Span::current(),
                        "handle",
                        result.row_count(),
                        columns.len(),
                        preview_rows,
                        Some(preview_has_more),
                        guidance.is_some(),
                    );
                    return Ok(value);
                }

                let preview = metadata_only_preview();
                let guidance = result_guidance(result.row_count(), true);
                telemetry::record_sql_result(
                    &tracing::Span::current(),
                    "handle",
                    result.row_count(),
                    columns.len(),
                    0,
                    Some(preview.has_more),
                    true,
                );
                serialize_handled_sql_value(result_id, &result, columns, preview, guidance)
            }
            Err(ResultStoreError::TooLarge { .. }) => {
                let preview_rows = preview.rows.len();
                let preview_has_more = preview.has_more;
                let value = serialize_preview_only_sql_value(
                    &result,
                    columns.clone(),
                    preview,
                    RESULT_TOO_LARGE_WARNING,
                )?;
                if serialized_value_len(&value)? <= SQL_PREVIEW_RESPONSE_MAX_BYTES {
                    telemetry::record_sql_result(
                        &tracing::Span::current(),
                        "preview_only",
                        result.row_count(),
                        columns.len(),
                        preview_rows,
                        Some(preview_has_more),
                        false,
                    );
                    return Ok(value);
                }

                let preview = metadata_only_preview();
                telemetry::record_sql_result(
                    &tracing::Span::current(),
                    "preview_only",
                    result.row_count(),
                    columns.len(),
                    0,
                    Some(preview.has_more),
                    false,
                );
                serialize_preview_only_sql_value(
                    &result,
                    columns,
                    preview,
                    RESULT_TOO_LARGE_PREVIEW_OMITTED_WARNING,
                )
            }
            Err(error) => Err(result_store_status(&error)),
        }
    }

    pub(crate) fn page_value(
        &self,
        result_id: String,
        offset: usize,
        limit: usize,
        columns: Option<&[String]>,
    ) -> Result<Value, ErrorData> {
        telemetry::record_result_get_request(
            &tracing::Span::current(),
            offset,
            limit,
            columns.map(<[String]>::len),
        );
        let result = self
            .store
            .get(&result_id)
            .map_err(|error| result_store_error_data(&error))?;
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset,
                limit,
                columns,
            },
        )
        .map_err(result_page_error_data)?;
        telemetry::record_result_get_page(
            &tracing::Span::current(),
            page.row_count,
            page.rows.len(),
            page.columns.len(),
            page.has_more,
            page.next_offset,
        );
        serde_json::to_value(ResultGetValue { result_id, page })
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }
}

pub(crate) fn query_result_status(error: &QueryResultError) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

fn inline_sql_value_if_small(
    result: &CollectedQueryResult,
) -> Result<Option<Value>, tonic::Status> {
    if result.row_count() > SQL_INLINE_BYTE_CHECK_MAX_ROWS {
        return Ok(None);
    }
    let rows = batches_to_json_rows_json_safe_numbers(result.batches())
        .map_err(|error| query_result_status(&error))?;
    let rows_len = rows.len();
    let value = serialize_tool_value(SqlRowsValue { rows })?;
    let compact_len = serde_json::to_string(&value)
        .map_err(|error| tonic::Status::internal(error.to_string()))?
        .len();
    if compact_len <= SQL_INLINE_MAX_BYTES {
        telemetry::record_sql_result(
            &tracing::Span::current(),
            "inline_rows",
            result.row_count(),
            result.schema().fields().len(),
            rows_len,
            Some(false),
            false,
        );
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn preview_page(result: &CollectedQueryResult) -> Result<ResultPreviewValue, tonic::Status> {
    let page = slice_result(
        result,
        ResultSliceRequest {
            offset: 0,
            limit: SQL_PREVIEW_ROWS,
            columns: None,
        },
    )
    .map_err(|error| query_result_status(&error))?;
    Ok(ResultPreviewValue {
        offset: page.offset,
        limit: page.limit,
        has_more: page.has_more,
        next_offset: page.next_offset,
        rows: page.rows,
    })
}

fn metadata_only_preview() -> ResultPreviewValue {
    ResultPreviewValue {
        offset: 0,
        limit: 0,
        has_more: false,
        next_offset: None,
        rows: Vec::new(),
    }
}

fn result_guidance(row_count: usize, preview_omitted: bool) -> Option<&'static str> {
    match (preview_omitted, row_count >= LARGE_RESULT_GUIDANCE_MIN_ROWS) {
        (true, true) => Some(PREVIEW_OMITTED_LARGE_RESULT_GUIDANCE),
        (true, false) => Some(PREVIEW_OMITTED_GUIDANCE),
        (false, true) => Some(LARGE_RESULT_GUIDANCE),
        (false, false) => None,
    }
}

fn serialize_handled_sql_value(
    result_id: String,
    result: &CollectedQueryResult,
    columns: Vec<ColumnSummary>,
    preview: ResultPreviewValue,
    guidance: Option<&'static str>,
) -> Result<Value, tonic::Status> {
    // Advertise the maximum page size: agents tend to copy these
    // arguments verbatim, and small pages multiply round trips.
    let next_call_offset = if preview.limit == 0 && result.row_count() > preview.offset {
        Some(preview.offset)
    } else {
        preview.next_offset
    };
    let next_call = next_call_offset.map(|offset| NextCallValue {
        tool: "result_get",
        arguments: NextCallArgumentsValue {
            result_id: result_id.clone(),
            offset,
            limit: RESULT_GET_MAX_LIMIT,
        },
    });
    serialize_tool_value(SqlHandledValue {
        result_id,
        row_count: result.row_count(),
        column_count: columns.len(),
        columns,
        preview,
        next_call,
        guidance,
    })
}

fn serialize_preview_only_sql_value(
    result: &CollectedQueryResult,
    columns: Vec<ColumnSummary>,
    preview: ResultPreviewValue,
    warning: &'static str,
) -> Result<Value, tonic::Status> {
    serialize_tool_value(SqlPreviewOnlyValue {
        preview_only: true,
        row_count: result.row_count(),
        column_count: columns.len(),
        columns,
        preview,
        warning,
    })
}

fn serialized_value_len(value: &Value) -> Result<usize, tonic::Status> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|error| tonic::Status::internal(error.to_string()))
}

fn result_store_error_data(error: &ResultStoreError) -> ErrorData {
    match error {
        ResultStoreError::NotFound(_) | ResultStoreError::Expired(_) => {
            ErrorData::invalid_params(error.to_string(), None)
        }
        ResultStoreError::TooLarge { .. } | ResultStoreError::Unavailable => {
            ErrorData::internal_error(error.to_string(), None)
        }
    }
}

fn result_store_status(error: &ResultStoreError) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

fn result_page_error_data(error: QueryResultError) -> ErrorData {
    match error {
        QueryResultError::InvalidSliceRequest(message) => ErrorData::invalid_params(message, None),
        other => ErrorData::internal_error(other.to_string(), None),
    }
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    fn single_string_result(value: String) -> CollectedQueryResult {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .expect("batch");
        CollectedQueryResult::new(schema, vec![batch], 1).expect("result")
    }

    fn handles_with_rejecting_store() -> ResultHandles {
        ResultHandles {
            store: ResultStore::with_test_limits(0, usize::MAX),
        }
    }

    #[test]
    fn single_row_over_inline_budget_returns_handle() {
        let value = ResultHandles::new()
            .sql_value(single_string_result("x".repeat(SQL_INLINE_MAX_BYTES + 512)))
            .expect("sql value");

        assert!(value.get("rows").is_none());
        assert!(
            value["result_id"]
                .as_str()
                .expect("result id")
                .starts_with("res_")
        );
        assert_eq!(value["row_count"], 1);
        assert_eq!(
            value["preview"]["rows"]
                .as_array()
                .expect("preview rows")
                .len(),
            1
        );
        assert!(
            serialized_value_len(&value).expect("serialized length")
                <= SQL_PREVIEW_RESPONSE_MAX_BYTES
        );
    }

    #[test]
    fn preview_response_over_budget_uses_metadata_only_preview() {
        let value = ResultHandles::new()
            .sql_value(single_string_result(
                "x".repeat(SQL_PREVIEW_RESPONSE_MAX_BYTES + 512),
            ))
            .expect("sql value");

        assert!(value.get("rows").is_none());
        assert!(
            value["result_id"]
                .as_str()
                .expect("result id")
                .starts_with("res_")
        );
        assert_eq!(value["preview"]["limit"], 0);
        assert_eq!(value["preview"]["has_more"], false);
        assert!(value["preview"].get("next_offset").is_none());
        assert!(
            value["preview"]["rows"]
                .as_array()
                .expect("preview rows")
                .is_empty()
        );
        assert_eq!(value["next_call"]["arguments"]["offset"], 0);
        assert!(
            value["guidance"]
                .as_str()
                .expect("guidance")
                .contains("Preview omitted")
        );
        assert!(
            serialized_value_len(&value).expect("serialized length")
                <= SQL_PREVIEW_RESPONSE_MAX_BYTES
        );
    }

    #[test]
    fn preview_only_result_exposes_contract_shape() {
        let body = "x".repeat(SQL_INLINE_MAX_BYTES + 512);
        let value = handles_with_rejecting_store()
            .sql_value(single_string_result(body.clone()))
            .expect("sql value");

        assert_eq!(value["preview_only"], true);
        assert!(value.get("result_id").is_none());
        assert!(value.get("next_call").is_none());
        assert_eq!(value["row_count"], 1);
        assert_eq!(value["column_count"], 1);
        assert_eq!(value["columns"][0]["name"], "body");
        assert_eq!(value["preview"]["offset"], 0);
        assert_eq!(value["preview"]["limit"], SQL_PREVIEW_ROWS);
        assert_eq!(value["preview"]["has_more"], false);
        assert!(value["preview"].get("next_offset").is_none());
        assert_eq!(
            value["preview"]["rows"][0]["body"]
                .as_str()
                .expect("preview body")
                .len(),
            body.len()
        );
        assert_eq!(value["warning"], RESULT_TOO_LARGE_WARNING);
    }

    #[test]
    fn preview_only_result_can_omit_oversized_preview_rows() {
        let value = handles_with_rejecting_store()
            .sql_value(single_string_result(
                "x".repeat(SQL_PREVIEW_RESPONSE_MAX_BYTES + 512),
            ))
            .expect("sql value");

        assert_eq!(value["preview_only"], true);
        assert!(value.get("result_id").is_none());
        assert!(value.get("next_call").is_none());
        assert_eq!(value["row_count"], 1);
        assert_eq!(value["column_count"], 1);
        assert_eq!(value["columns"][0]["name"], "body");
        assert_eq!(value["preview"]["offset"], 0);
        assert_eq!(value["preview"]["limit"], 0);
        assert_eq!(value["preview"]["has_more"], false);
        assert!(value["preview"].get("next_offset").is_none());
        assert!(
            value["preview"]["rows"]
                .as_array()
                .expect("preview rows")
                .is_empty()
        );
        assert_eq!(value["warning"], RESULT_TOO_LARGE_PREVIEW_OMITTED_WARNING);
        assert!(
            serialized_value_len(&value).expect("serialized length")
                <= SQL_PREVIEW_RESPONSE_MAX_BYTES
        );
    }
}
