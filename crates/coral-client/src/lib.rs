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

use std::io::{Cursor, Write};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::json::writer::{
    Encoder, EncoderFactory, EncoderOptions, JsonArray, NullableEncoder, WriterBuilder,
};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
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

/// Converts batches into JSON row objects, encoding numeric SQL types whose
/// exact value cannot survive a JSON-number round-trip as JSON strings.
///
/// Concretely, `Int64`, `UInt64`, and `Decimal{32,64,128,256}` leaves anywhere
/// in the schema (including inside `Struct`, `List`, `Map`, and `Dictionary`)
/// are written as quoted strings. Clients that decode JSON numbers as
/// IEEE-754 doubles (e.g. JS `JSON.parse`) would otherwise silently truncate
/// values past 2^53 or lose digits of precision on wide decimals.
///
/// The column's declared SQL type is unchanged — only the JSON encoding of the
/// value differs. This is the contract MCP relies on.
///
/// # Errors
///
/// Returns [`QueryResultError`] if Arrow's JSON writer rejects a column type
/// in the batch (e.g. an unsupported `Union` or extension type).
pub fn batches_to_json_rows_json_safe_numbers(
    batches: &[RecordBatch],
) -> Result<Vec<Value>, QueryResultError> {
    let json = format_batches_json_safe_numbers(batches)?;
    serde_json::from_str(&json).map_err(Into::into)
}

fn format_batches_json_safe_numbers(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    let mut bytes = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(JsonSafeNumbersFactory))
            .build::<_, JsonArray>(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    String::from_utf8(bytes).map_err(Into::into)
}

#[derive(Debug)]
struct JsonSafeNumbersFactory;

impl EncoderFactory for JsonSafeNumbersFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        match array.data_type() {
            DataType::Int64
            | DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {
                let options = FormatOptions::new().with_display_error(true);
                let formatter = ArrayFormatter::try_new(array, &options)?;
                let nulls = array.nulls().cloned();
                let encoder: Box<dyn Encoder + 'a> = Box::new(QuotedFormatterEncoder { formatter });
                Ok(Some(NullableEncoder::new(encoder, nulls)))
            }
            _ => Ok(None),
        }
    }
}

struct QuotedFormatterEncoder<'a> {
    formatter: ArrayFormatter<'a>,
}

impl Encoder for QuotedFormatterEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        write!(out, "{}", self.formatter.value(idx)).expect("writing into Vec<u8> is infallible");
        out.push(b'"');
    }
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
        CollectedQueryResult, batches_to_json_rows, batches_to_json_rows_json_safe_numbers,
        decode_execute_sql_response, format_batches_json, format_batches_table,
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

    const HUGE_I64: i64 = -8_504_475_857_937_456_387_i64;
    const HUGE_U64: u64 = 18_446_744_073_709_551_000_u64;

    #[test]
    fn json_safe_numbers_stringifies_top_level_int64_and_uint64() {
        use arrow::array::UInt64Array;
        use arrow::datatypes::Fields;

        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("snowflake_id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ])));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![HUGE_I64, 1])) as _,
                Arc::new(UInt64Array::from(vec![HUGE_U64, 2])) as _,
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as _,
            ],
        )
        .expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        let first = rows.first().expect("first row");
        assert_eq!(
            first.get("user_id"),
            Some(&Value::String(HUGE_I64.to_string()))
        );
        assert_eq!(
            first.get("snowflake_id"),
            Some(&Value::String(HUGE_U64.to_string())),
        );
        assert_eq!(first.get("name"), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn json_safe_numbers_stringifies_int64_inside_struct() {
        use arrow::array::{ArrayRef, StructArray};
        use arrow::datatypes::Fields;

        let inner_int = Arc::new(Field::new("id", DataType::Int64, false));
        let inner_str = Arc::new(Field::new("tag", DataType::Utf8, false));
        let event_array = StructArray::from(vec![
            (
                inner_int.clone(),
                Arc::new(Int64Array::from(vec![HUGE_I64])) as ArrayRef,
            ),
            (
                inner_str.clone(),
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event",
            DataType::Struct(Fields::from(vec![
                inner_int.as_ref().clone(),
                inner_str.as_ref().clone(),
            ])),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(event_array) as ArrayRef]).expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        assert_eq!(
            rows.first().and_then(|row| row.get("event")),
            Some(&serde_json::json!({"id": HUGE_I64.to_string(), "tag": "a"})),
        );
    }

    #[test]
    fn json_safe_numbers_stringifies_int64_inside_list() {
        use arrow::array::{ArrayRef, ListArray};
        use arrow::buffer::OffsetBuffer;

        let item_field = Arc::new(Field::new("item", DataType::Int64, true));
        let values = Arc::new(Int64Array::from(vec![Some(1), Some(HUGE_I64), None])) as ArrayRef;
        let offsets = OffsetBuffer::<i32>::from_lengths([3]);
        let list = ListArray::new(item_field.clone(), offsets, values, None);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ids",
            DataType::List(item_field),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        assert_eq!(
            rows.first().and_then(|row| row.get("ids")),
            Some(&serde_json::json!(["1", HUGE_I64.to_string(), null])),
        );
    }

    /// Regression: the previous walker treated `DataType::Map` as a list shape
    /// and only descended into `Value::Array`, but Arrow serializes maps as JSON
    /// objects — so int64 map values were emitted as lossy JSON numbers.
    #[test]
    fn json_safe_numbers_stringifies_int64_inside_map() {
        use arrow::array::{ArrayRef, MapArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Fields;

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int64, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let keys = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let values = Arc::new(Int64Array::from(vec![1_i64, HUGE_I64])) as ArrayRef;
        let entries = StructArray::from(vec![(key_field, keys), (value_field, values)]);
        let offsets = OffsetBuffer::<i32>::from_lengths([2]);
        let map = MapArray::new(entries_field.clone(), offsets, entries, None, false);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "counts",
            DataType::Map(entries_field, false),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(map) as ArrayRef]).expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        assert_eq!(
            rows.first().and_then(|row| row.get("counts")),
            Some(&serde_json::json!({"a": "1", "b": HUGE_I64.to_string()})),
        );
    }

    /// Regression: dictionary arrays unwrap to their value array, so the
    /// factory must match on the inner type — covered here by a `Dictionary`
    /// of `Int64` values that previously could have rendered as a JSON number.
    #[test]
    fn json_safe_numbers_stringifies_int64_inside_dictionary() {
        use arrow::array::{ArrayRef, DictionaryArray, Int32Array};

        let keys = Int32Array::from(vec![0_i32, 1, 0]);
        let values = Arc::new(Int64Array::from(vec![HUGE_I64, 7])) as ArrayRef;
        let dict = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(keys, values)
            .expect("dictionary");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict) as ArrayRef]).expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        let expected = HUGE_I64.to_string();
        let ids: Vec<_> = rows.iter().map(|row| row.get("id").cloned()).collect();
        assert_eq!(
            ids,
            vec![
                Some(Value::String(expected.clone())),
                Some(Value::String("7".to_string())),
                Some(Value::String(expected)),
            ],
        );
    }

    /// Regression: `Decimal128` was emitted by arrow-json as a raw, unquoted
    /// JSON number, which loses precision under `JSON.parse`. The factory now
    /// quotes decimal leaves so the exact value survives.
    #[test]
    fn json_safe_numbers_stringifies_decimal128() {
        use arrow::array::Decimal128Array;

        let array = Decimal128Array::from(vec![123_456_789_012_345_678_901_i128])
            .with_precision_and_scale(38, 9)
            .expect("decimal");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 9),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array) as _]).expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        assert_eq!(
            rows.first().and_then(|row| row.get("amount")),
            Some(&Value::String("123456789012.345678901".to_string())),
        );
    }

    #[test]
    fn json_safe_numbers_leaves_other_scalars_alone() {
        use arrow::array::{Float64Array, Int32Array};
        use arrow::datatypes::Fields;

        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("small_int", DataType::Int32, false),
            Field::new("ratio", DataType::Float64, false),
        ])));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![42])) as _,
                Arc::new(Float64Array::from(vec![2.5])) as _,
            ],
        )
        .expect("batch");

        let rows = batches_to_json_rows_json_safe_numbers(&[batch]).expect("rows");
        let first = rows.first().expect("first row");
        assert_eq!(first.get("small_int"), Some(&serde_json::json!(42)));
        assert_eq!(first.get("ratio"), Some(&serde_json::json!(2.5)));
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
