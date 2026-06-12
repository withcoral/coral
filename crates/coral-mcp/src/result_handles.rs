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
const LARGE_RESULT_GUIDANCE_MIN_ROWS: usize = 1_000;
const LARGE_RESULT_GUIDANCE: &str = "Result is large; answer from row_count or rerun the SQL with filters or aggregates instead of paging every row. If raw rows are required, call result_get with limit 500 and a columns projection.";

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
        if let Some(value) = inline_sql_value_if_small(&result)? {
            return Ok(value);
        }

        // JSON object rows collapse duplicate column names and result_get
        // projects columns by name, so duplicate-named results cannot be
        // paged through a handle. Keep the legacy inline shape for them.
        if has_duplicate_column_names(&result) {
            let rows = batches_to_json_rows_json_safe_numbers(result.batches())
                .map_err(|error| query_result_status(&error))?;
            telemetry::record_sql_result(
                &tracing::Span::current(),
                "duplicate_named_inline",
                result.row_count(),
                result.schema().fields().len(),
                rows.len(),
                Some(false),
                false,
            );
            return serialize_tool_value(SqlRowsValue { rows });
        }

        let result = Arc::new(result);
        let preview = preview_page(&result)?;
        let columns = schema_summary(&result);
        match self.store.insert_result(Arc::clone(&result)) {
            Ok(result_id) => {
                // Advertise the maximum page size: agents tend to copy these
                // arguments verbatim, and small pages multiply round trips.
                let next_call = preview.next_offset.map(|offset| NextCallValue {
                    tool: "result_get",
                    arguments: NextCallArgumentsValue {
                        result_id: result_id.clone(),
                        offset,
                        limit: RESULT_GET_MAX_LIMIT,
                    },
                });
                let guidance = (result.row_count() >= LARGE_RESULT_GUIDANCE_MIN_ROWS)
                    .then_some(LARGE_RESULT_GUIDANCE);
                telemetry::record_sql_result(
                    &tracing::Span::current(),
                    "handle",
                    result.row_count(),
                    columns.len(),
                    preview.rows.len(),
                    Some(preview.has_more),
                    guidance.is_some(),
                );
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
            Err(ResultStoreError::TooLarge { .. }) => {
                telemetry::record_sql_result(
                    &tracing::Span::current(),
                    "preview_only",
                    result.row_count(),
                    columns.len(),
                    preview.rows.len(),
                    Some(preview.has_more),
                    false,
                );
                serialize_tool_value(SqlPreviewOnlyValue {
                    preview_only: true,
                    row_count: result.row_count(),
                    column_count: columns.len(),
                    columns,
                    preview,
                    warning: "Result exceeded the in-memory handle limit; rerun the SQL with LIMIT, filters, or a smaller column set.",
                })
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
    let preview_would_include_full_result = result.row_count() <= SQL_PREVIEW_ROWS;
    if preview_would_include_full_result || compact_len <= SQL_INLINE_MAX_BYTES {
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
