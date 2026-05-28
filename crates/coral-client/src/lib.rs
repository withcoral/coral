//! Thin local transport bootstrap and shared query-result helpers for Coral.
//!
//! `coral-client` intentionally stays narrow today. It owns:
//!
//! - endpoint dialing into the generated gRPC transport surface
//! - lightweight shared Arrow IPC decoding helpers
//! - lightweight shared result-format rendering used by CLI and MCP
//! - lightweight client-side decoding helpers for shared transport DTOs
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
mod sources;
mod status_error;

use std::io::Cursor;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::json::writer::{JsonArray, WriterBuilder};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use coral_api::v1::ExecuteSqlResponse;
use serde_json::Value;

pub use client::{
    AppClient, CatalogClient, DEFAULT_WORKSPACE_ID, FeedbackClient, QueryClient, SourceClient,
    default_workspace,
};
pub use error::{ClientError, QueryResultError};
pub use sources::{SourceInputDecodeError, manifest_input_from_proto};
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
/// Returns [`QueryResultError`] if Arrow's JSON writer rejects a column type
/// in the batch (e.g. an unsupported `Union` or extension type).
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
/// Returns [`QueryResultError`] if Arrow's JSON writer rejects a column type
/// in the batch (e.g. an unsupported `Union` or extension type).
pub fn batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<Value>, QueryResultError> {
    let json = format_batches_json(batches)?;
    serde_json::from_str(&json).map_err(Into::into)
}

/// Converts batches into JSON row objects, stringifying `Int64` and `UInt64`
/// columns so the exact value survives consumers that decode JSON numbers as
/// IEEE-754 doubles (e.g. JS `JSON.parse`) and would silently truncate values
/// past 2^53.
///
/// # Errors
///
/// Returns [`QueryResultError`] if Arrow's JSON writer rejects a column type
/// in the batch (e.g. an unsupported `Union` or extension type).
pub fn batches_to_json_rows_int64_safe(
    batches: &[RecordBatch],
) -> Result<Vec<Value>, QueryResultError> {
    let mut rows = batches_to_json_rows(batches)?;
    stringify_int64_columns(batches, &mut rows);
    Ok(rows)
}

fn stringify_int64_columns(batches: &[RecordBatch], rows: &mut [Value]) {
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return;
    };
    for row in rows {
        for field in schema.fields() {
            stringify_field(field.data_type(), row, field.name());
        }
    }
}

fn stringify_field(data_type: &DataType, parent: &mut Value, field_name: &str) {
    let Value::Object(obj) = parent else { return };
    let Some(value) = obj.get_mut(field_name) else {
        return;
    };
    stringify_value(data_type, value);
}

fn stringify_value(data_type: &DataType, value: &mut Value) {
    match data_type {
        DataType::Int64 | DataType::UInt64 => {
            if let Value::Number(n) = value {
                *value = Value::String(n.to_string());
            }
        }
        DataType::Struct(fields) => {
            for field in fields {
                stringify_field(field.data_type(), value, field.name());
            }
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            if let Value::Array(items) = value {
                for item in items {
                    stringify_value(field.data_type(), item);
                }
            }
        }
        DataType::Dictionary(_, value_type) => {
            stringify_value(value_type, value);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use coral_api::v1::ExecuteSqlResponse;
    use serde_json::Value;

    use super::{
        CollectedQueryResult, batches_to_json_rows, batches_to_json_rows_int64_safe,
        decode_execute_sql_response, format_batches_json, format_batches_table, stringify_value,
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
    fn batches_to_json_rows_int64_safe_stringifies_int64_values() {
        use arrow::array::UInt64Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("snowflake_id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![-8_504_475_857_937_456_387_i64, 1])) as _,
                Arc::new(UInt64Array::from(vec![18_446_744_073_709_551_000_u64, 2])) as _,
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as _,
            ],
        )
        .expect("batch");

        let rows = batches_to_json_rows_int64_safe(&[batch]).expect("rows");
        let first = rows.first().expect("first row");
        assert_eq!(
            first.get("user_id"),
            Some(&Value::String("-8504475857937456387".to_string())),
        );
        assert_eq!(
            first.get("snowflake_id"),
            Some(&Value::String("18446744073709551000".to_string())),
        );
        assert_eq!(first.get("name"), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn stringify_value_rewrites_int64_inside_struct() {
        let dt = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]));
        let mut value =
            serde_json::json!({"id": -8_504_475_857_937_456_387_i64, "name": "a", "count": 3});
        stringify_value(&dt, &mut value);
        assert_eq!(
            value,
            serde_json::json!({"id": "-8504475857937456387", "name": "a", "count": 3}),
        );
    }

    #[test]
    fn stringify_value_rewrites_int64_inside_list() {
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let mut value = serde_json::json!([1_i64, -8_504_475_857_937_456_387_i64, null]);
        stringify_value(&dt, &mut value);
        assert_eq!(
            value,
            serde_json::json!(["1", "-8504475857937456387", null]),
        );
    }

    #[test]
    fn stringify_value_rewrites_int64_inside_list_of_structs() {
        let item = Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ])),
            false,
        );
        let dt = DataType::List(Arc::new(item));
        let mut value = serde_json::json!([{"id": 1_i64, "tag": "a"}, {"id": -8_504_475_857_937_456_387_i64, "tag": "b"}]);
        stringify_value(&dt, &mut value);
        assert_eq!(
            value,
            serde_json::json!([
                {"id": "1", "tag": "a"},
                {"id": "-8504475857937456387", "tag": "b"},
            ]),
        );
    }

    #[test]
    fn stringify_value_leaves_non_int64_scalars_untouched() {
        let dt = DataType::Int32;
        let mut value = serde_json::json!(42);
        stringify_value(&dt, &mut value);
        assert_eq!(value, serde_json::json!(42));
    }

    #[test]
    fn batches_to_json_rows_int64_safe_rewrites_struct_columns() {
        use arrow::array::{ArrayRef, StructArray};

        let inner_int = Arc::new(Field::new("id", DataType::Int64, false));
        let inner_str = Arc::new(Field::new("tag", DataType::Utf8, false));
        let struct_field = Field::new(
            "event",
            DataType::Struct(Fields::from(vec![
                inner_int.as_ref().clone(),
                inner_str.as_ref().clone(),
            ])),
            false,
        );

        let id_values =
            Arc::new(Int64Array::from(vec![-8_504_475_857_937_456_387_i64])) as ArrayRef;
        let tag_values = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
        let event_array = StructArray::from(vec![(inner_int, id_values), (inner_str, tag_values)]);

        let schema = Arc::new(Schema::new(vec![struct_field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(event_array) as ArrayRef]).expect("batch");

        let rows = batches_to_json_rows_int64_safe(&[batch]).expect("rows");
        let first = rows.first().expect("first row");
        assert_eq!(
            first.get("event"),
            Some(&serde_json::json!({"id": "-8504475857937456387", "tag": "a"})),
        );
    }

    #[test]
    fn batches_to_json_rows_keeps_int64_as_number() {
        let decoded = decode_execute_sql_response(&response()).expect("decode");
        let rows = batches_to_json_rows(decoded.batches()).expect("rows");
        let first = rows.first().expect("first row");
        assert_eq!(first.get("id"), Some(&serde_json::json!(1)));
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
