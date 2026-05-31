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

use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::json::writer::{JsonArray, LineDelimited, WriterBuilder};
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

/// Formats batches as newline-delimited JSON (one object per row).
///
/// Each row is a self-contained JSON object terminated by `\n`, matching the
/// [ndjson.org](https://ndjson.org/) convention. The output is empty when there
/// are no rows. Suitable for streaming consumers and shell pipelines.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the batches cannot be encoded as JSON.
pub fn format_batches_ndjson(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    let mut bytes = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    String::from_utf8(bytes).map_err(Into::into)
}

/// Formats batches as RFC 4180-compliant CSV with a header row.
///
/// The header row is emitted from the first batch's schema; if there are no
/// batches the output is empty. Null values are rendered as empty fields.
///
/// # Errors
///
/// Returns [`QueryResultError`] if the batches cannot be encoded as CSV.
pub fn format_batches_csv(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    let mut bytes = Vec::new();
    {
        let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    String::from_utf8(bytes).map_err(Into::into)
}

/// Formats batches as a GitHub-flavored markdown table.
///
/// The header is taken from the first batch's schema; if `batches` is empty
/// the output is empty. Cell values use the same display formatting as the
/// pretty-print table renderer, with markdown-significant characters
/// (`|`, `\`, newlines) escaped so the table stays well-formed when pasted
/// into PR comments, issues, or docs.
///
/// # Errors
///
/// Returns [`QueryResultError`] if a cell value cannot be formatted.
pub fn format_batches_markdown(batches: &[RecordBatch]) -> Result<String, QueryResultError> {
    let Some(first) = batches.first() else {
        return Ok(String::new());
    };
    let schema = first.schema();
    let options = FormatOptions::default().with_null("");

    let header_cells: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| escape_markdown_cell(field.name()))
        .collect();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;

        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(formatters.len());
            for formatter in &formatters {
                let cell = formatter.value(row_idx).try_to_string()?;
                row.push(escape_markdown_cell(&cell));
            }
            rows.push(row);
        }
    }

    let widths: Vec<usize> = header_cells
        .iter()
        .enumerate()
        .map(|(col_idx, header)| {
            let max_data = rows
                .iter()
                .map(|row| row.get(col_idx).map_or(0, String::len))
                .max()
                .unwrap_or(0);
            // Separator must have at least three dashes per GFM.
            header.len().max(max_data).max(3)
        })
        .collect();

    let mut out = String::new();
    push_markdown_row(&mut out, &header_cells, &widths);
    push_markdown_separator(&mut out, &widths);
    for row in &rows {
        push_markdown_row(&mut out, row, &widths);
    }
    Ok(out)
}

fn push_markdown_row(out: &mut String, cells: &[String], widths: &[usize]) {
    out.push('|');
    for (idx, width) in widths.iter().enumerate() {
        let empty = String::new();
        let cell = cells.get(idx).unwrap_or(&empty);
        out.push(' ');
        out.push_str(cell);
        for _ in 0..width.saturating_sub(cell.len()) {
            out.push(' ');
        }
        out.push(' ');
        out.push('|');
    }
    out.push('\n');
}

fn push_markdown_separator(out: &mut String, widths: &[usize]) {
    out.push('|');
    for width in widths {
        out.push(' ');
        for _ in 0..*width {
            out.push('-');
        }
        out.push(' ');
        out.push('|');
    }
    out.push('\n');
}

fn escape_markdown_cell(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '|' => out.push_str("\\|"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("<br>"),
            '\r' => {}
            _ => out.push(ch),
        }
    }
    out
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
        format_batches_csv, format_batches_json, format_batches_markdown, format_batches_ndjson,
        format_batches_table,
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
    fn ndjson_emits_one_object_per_row_with_explicit_nulls() {
        let decoded = decode_execute_sql_response(&response()).expect("decode");
        let ndjson = format_batches_ndjson(decoded.batches()).expect("ndjson");

        let rows: Vec<Value> = ndjson
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is json"))
            .collect();
        assert_eq!(rows.len(), 2, "one line per row, got {ndjson:?}");
        let first = rows.first().expect("first row");
        let second = rows.get(1).expect("second row");
        assert_eq!(first.get("id"), Some(&Value::from(1_i64)));
        assert_eq!(first.get("name"), Some(&Value::from("a")));
        assert!(
            second.get("name").is_some_and(Value::is_null),
            "explicit null should be preserved"
        );
        assert!(
            ndjson.ends_with('\n'),
            "ndjson should be newline-terminated"
        );
    }

    #[test]
    fn ndjson_on_empty_input_is_empty() {
        let ndjson = format_batches_ndjson(&[]).expect("ndjson");
        assert!(ndjson.is_empty());
    }

    #[test]
    fn csv_renders_header_and_quotes_special_characters() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as _,
                Arc::new(StringArray::from(vec![
                    Some("has,comma and \"quote\""),
                    None,
                ])) as _,
            ],
        )
        .expect("batch");

        let csv = format_batches_csv(&[batch]).expect("csv");
        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("id,title"));
        assert_eq!(
            lines.next(),
            Some("1,\"has,comma and \"\"quote\"\"\""),
            "embedded comma and quote must be RFC 4180 escaped"
        );
        assert_eq!(lines.next(), Some("2,"), "null cell renders as empty field");
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn csv_on_empty_input_is_empty() {
        let csv = format_batches_csv(&[]).expect("csv");
        assert!(csv.is_empty());
    }

    #[test]
    fn markdown_renders_table_and_escapes_pipes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as _,
                Arc::new(StringArray::from(vec![Some("a|b"), None])) as _,
            ],
        )
        .expect("batch");

        let md = format_batches_markdown(&[batch]).expect("markdown");
        let lines: Vec<&str> = md.lines().collect();
        assert_eq!(lines.len(), 4, "header + separator + 2 data rows: {md}");
        let header = lines.first().expect("header row");
        let separator = lines.get(1).expect("separator row");
        let first_data = lines.get(2).expect("first data row");
        let second_data = lines.get(3).expect("second data row");
        assert!(header.starts_with("| id"), "header row");
        assert!(
            separator.contains("---"),
            "separator row must have at least three dashes per column"
        );
        assert!(
            first_data.contains("a\\|b"),
            "pipe in cell must be escaped, got: {first_data}"
        );
        assert!(
            second_data.ends_with('|'),
            "null cell still emits a closing column delimiter"
        );
    }

    #[test]
    fn markdown_on_empty_input_is_empty() {
        let md = format_batches_markdown(&[]).expect("markdown");
        assert!(md.is_empty());
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
