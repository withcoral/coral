//! Thin local transport bootstrap and shared query-result helpers for Coral.
//!
//! `coral-client` intentionally stays narrow today. It owns:
//!
//! - endpoint dialing into the generated gRPC transport surface
//! - lightweight shared Arrow IPC decoding helpers
//! - lightweight shared result-format rendering used by CLI and MCP
//!
//! It does **not** currently try to present a richer domain SDK. Callers that
//! need more abstraction should add it above this crate rather than widening
//! the transport/bootstrap seam here.
//!
//! For tests or embedding scenarios that need explicit control over local server
//! configuration or lifecycle, use [`local`] rather than treating those
//! bootstrap seams as the default client surface.

mod client;
mod error;
mod grpc;
pub mod local;
mod propagation;
mod status_error;

use std::io::Cursor;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::json::writer::{JsonArray, WriterBuilder};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use coral_api::v1::ExecuteSqlResponse;
use serde_json::Value;

pub use client::{
    AppClient, DEFAULT_WORKSPACE_ID, FeedbackClient, QueryClient, SourceClient, default_workspace,
};
pub use error::{ClientError, QueryResultError};
pub use status_error::{
    CORAL_ERROR_DOMAIN, CoralQueryError, DecodedStatusError, decode_status_error,
};

/// Fully decoded unary query response.
#[derive(Debug, Clone)]
pub struct CollectedQueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl CollectedQueryResult {
    /// Builds a collected query result and validates the declared row count.
    ///
    /// # Errors
    ///
    /// Returns [`QueryResultError::InvalidResponse`] if the declared row count
    /// does not match the actual number of rows in `batches`.
    pub fn new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        declared_row_count: usize,
    ) -> Result<Self, QueryResultError> {
        let actual_row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if actual_row_count != declared_row_count {
            return Err(QueryResultError::InvalidResponse(format!(
                "row_count mismatch: declared {declared_row_count}, actual {actual_row_count}"
            )));
        }
        Ok(Self {
            schema,
            batches,
            row_count: actual_row_count,
        })
    }

    #[must_use]
    /// Returns the Arrow schema for the decoded query result.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[must_use]
    /// Returns the Arrow record batches in query result order.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    /// Returns the total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

/// Decodes one unary API response into Arrow batches and schema.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the Arrow IPC payload is invalid or if the
/// declared row count does not match the decoded batches.
pub fn decode_execute_sql_response(
    response: &ExecuteSqlResponse,
) -> Result<CollectedQueryResult, QueryResultError> {
    let (schema, batches) = decode_arrow_ipc_stream(&response.arrow_ipc_stream)?;
    let row_count = usize::try_from(response.row_count).map_err(|_err| {
        QueryResultError::InvalidResponse("row_count must not be negative".into())
    })?;
    CollectedQueryResult::new(schema, batches, row_count)
}

fn decode_arrow_ipc_stream(
    bytes: &[u8],
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// Formats batches as an ASCII table.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the batches cannot be rendered.
pub fn format_batches_table(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    pretty_format_batches(batches)
        .map(|table| table.to_string())
        .map_err(Into::into)
}

/// Formats batches as a JSON array string.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the batches cannot be encoded as JSON.
pub fn format_batches_json(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    let mut bytes = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    String::from_utf8(bytes).map_err(Into::into)
}

/// Converts batches into JSON row objects.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the batches cannot be encoded as JSON rows.
pub fn batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<Value>, QueryResultError> {
    let json = format_batches_json(batches)?;
    serde_json::from_str(&json).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use coral_api::v1::ExecuteSqlResponse;
    use serde_json::Value;

    use super::{
        CollectedQueryResult, batches_to_json_rows, decode_execute_sql_response,
        format_batches_json, format_batches_table,
    };

    fn response() -> ExecuteSqlResponse {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as _,
                Arc::new(StringArray::from(vec![Some("a"), None])) as _,
            ],
        )
        .expect("batch");

        ExecuteSqlResponse {
            arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode"),
            row_count: 2,
        }
    }

    fn encode_arrow_ipc_stream(
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<Vec<u8>, arrow::error::ArrowError> {
        let mut bytes = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, schema)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        Ok(bytes)
    }

    #[test]
    fn execute_sql_response_round_trips_batches() {
        let decoded = decode_execute_sql_response(&response()).expect("decode");
        assert_eq!(decoded.row_count(), 2);
        assert_eq!(decoded.schema().fields().len(), 2);
        assert_eq!(decoded.batches().len(), 1);
        let batch = decoded.batches().first().expect("decoded batch");
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn execute_sql_response_preserves_empty_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let response = ExecuteSqlResponse {
            arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[]).expect("encode"),
            row_count: 0,
        };
        let decoded = decode_execute_sql_response(&response).expect("decode");
        assert_eq!(decoded.row_count(), 0);
        assert_eq!(decoded.schema(), &schema);
        assert!(decoded.batches().is_empty());
    }

    #[test]
    fn formatting_helpers_render_batches() {
        let decoded = decode_execute_sql_response(&response()).expect("decode");
        let table = format_batches_table(decoded.batches()).expect("table");
        assert!(table.contains("id"));
        let json = format_batches_json(decoded.batches()).expect("json");
        assert!(json.contains("\"name\":\"a\""));
        assert!(json.contains("\"name\":null"));
        let rows = batches_to_json_rows(decoded.batches()).expect("rows");
        assert_eq!(rows.len(), 2);
        let row = rows.get(1).expect("second row");
        assert!(row.get("name").is_some_and(Value::is_null));
    }

    #[test]
    fn collected_query_result_rejects_row_count_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as _],
        )
        .expect("batch");

        let error =
            CollectedQueryResult::new(schema, vec![batch], 2).expect_err("expected mismatch");
        let super::QueryResultError::InvalidResponse(detail) = error else {
            panic!("expected invalid response");
        };
        assert!(detail.contains("row_count mismatch"));
    }
}
