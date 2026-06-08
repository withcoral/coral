//! Query-result slicing helpers shared by MCP result handles.

use std::collections::HashSet;

use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use crate::{CollectedQueryResult, QueryResultError, batches_to_json_rows_json_safe_numbers};

/// One query-result column rendered for agent-facing metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnSummary {
    /// Column name as returned by the SQL query.
    pub name: String,
    /// Arrow data type rendered with its display representation.
    pub data_type: String,
    /// Whether the column allows null values.
    pub is_nullable: bool,
    /// Zero-based ordinal position in the full query result schema.
    pub ordinal_position: usize,
}

/// Request for one page from a collected query result.
#[derive(Debug, Clone, Copy)]
pub struct ResultSliceRequest<'a> {
    /// Zero-based row offset.
    pub offset: usize,
    /// Maximum rows to return. `0` returns metadata only.
    pub limit: usize,
    /// Optional projected column names. Omitted means all columns.
    pub columns: Option<&'a [String]>,
}

/// One page of query-result rows plus schema and pagination metadata.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResultPage {
    /// Total rows in the stored result.
    pub row_count: usize,
    /// Columns included in this page.
    pub columns: Vec<ColumnSummary>,
    /// Zero-based row offset used for this page.
    pub offset: usize,
    /// Maximum rows requested for this page.
    pub limit: usize,
    /// Whether there are more rows after this page.
    pub has_more: bool,
    /// Next offset to request when `has_more` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
    /// JSON-safe row objects for this page.
    pub rows: Vec<Value>,
}

/// Returns metadata for every column in the query result.
#[must_use]
pub fn schema_summary(result: &CollectedQueryResult) -> Vec<ColumnSummary> {
    result
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| column_summary(idx, field))
        .collect()
}

/// Returns approximate in-memory bytes held by the decoded Arrow batches.
#[must_use]
pub fn result_estimated_bytes(result: &CollectedQueryResult) -> usize {
    result
        .batches()
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum()
}

/// Returns one projected and paginated JSON-safe page from a query result.
///
/// # Errors
///
/// Returns [`QueryResultError`] when column projection is invalid or Arrow/JSON
/// rendering fails.
pub fn slice_result(
    result: &CollectedQueryResult,
    request: ResultSliceRequest<'_>,
) -> Result<ResultPage, QueryResultError> {
    let indices = projection_indices(result, request.columns)?;
    let columns = indices
        .iter()
        .map(|idx| {
            let field = result.schema().fields().get(*idx).ok_or_else(|| {
                invalid_result_request("validated projection index was outside the result schema")
            })?;
            Ok::<ColumnSummary, QueryResultError>(column_summary(*idx, field))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let sliced = slice_batches(result.batches(), request.offset, request.limit);
    let projected = if all_columns_selected(result, &indices) {
        sliced
    } else {
        sliced
            .into_iter()
            .map(|batch| batch.project(&indices))
            .collect::<Result<Vec<_>, _>>()?
    };
    let rows = batches_to_json_rows_json_safe_numbers(&projected)?;
    let next_offset = next_offset(result.row_count(), request.offset, request.limit);
    Ok(ResultPage {
        row_count: result.row_count(),
        columns,
        offset: request.offset,
        limit: request.limit,
        has_more: next_offset.is_some(),
        next_offset,
        rows,
    })
}

fn column_summary(idx: usize, field: &arrow::datatypes::FieldRef) -> ColumnSummary {
    ColumnSummary {
        name: field.name().clone(),
        data_type: field.data_type().to_string(),
        is_nullable: field.is_nullable(),
        ordinal_position: idx,
    }
}

fn projection_indices(
    result: &CollectedQueryResult,
    columns: Option<&[String]>,
) -> Result<Vec<usize>, QueryResultError> {
    let fields = result.schema().fields();
    let Some(columns) = columns else {
        return Ok((0..fields.len()).collect());
    };
    if columns.is_empty() {
        return Err(invalid_result_request(
            "columns must be omitted or contain at least one column name",
        ));
    }

    let mut requested = HashSet::with_capacity(columns.len());
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        if !requested.insert(column.as_str()) {
            return Err(invalid_result_request(format!(
                "duplicate requested column '{column}'"
            )));
        }
        let matches = fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| (field.name() == column).then_some(idx))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => {
                return Err(invalid_result_request(format!(
                    "result does not contain column '{column}'"
                )));
            }
            [idx] => indices.push(*idx),
            _ => {
                return Err(invalid_result_request(format!(
                    "column '{column}' is ambiguous because it appears more than once"
                )));
            }
        }
    }
    Ok(indices)
}

fn slice_batches(batches: &[RecordBatch], offset: usize, limit: usize) -> Vec<RecordBatch> {
    if limit == 0 {
        return Vec::new();
    }

    let mut remaining_offset = offset;
    let mut remaining_limit = limit;
    let mut out = Vec::new();
    for batch in batches {
        let batch_rows = batch.num_rows();
        if remaining_offset >= batch_rows {
            remaining_offset -= batch_rows;
            continue;
        }

        let start = remaining_offset;
        remaining_offset = 0;
        let len = (batch_rows - start).min(remaining_limit);
        if len == 0 {
            break;
        }
        out.push(batch.slice(start, len));
        remaining_limit -= len;
        if remaining_limit == 0 {
            break;
        }
    }
    out
}

fn next_offset(row_count: usize, offset: usize, limit: usize) -> Option<usize> {
    if limit == 0 {
        return None;
    }
    if offset >= row_count {
        return None;
    }
    let next = offset.saturating_add(limit);
    (next < row_count).then_some(next)
}

fn all_columns_selected(result: &CollectedQueryResult, indices: &[usize]) -> bool {
    indices.len() == result.schema().fields().len()
        && indices
            .iter()
            .copied()
            .eq(0..result.schema().fields().len())
}

fn invalid_result_request(message: impl Into<String>) -> QueryResultError {
    QueryResultError::InvalidResponse(message.into())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        reason = "test code: assertion-style indexing keeps expected row shapes readable"
    )]

    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use serde_json::{Value, json};

    use super::{ResultSliceRequest, result_estimated_bytes, schema_summary, slice_result};
    use crate::CollectedQueryResult;

    fn two_batch_result() -> CollectedQueryResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as _,
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as _,
            ],
        )
        .expect("first batch");
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3_i64, 4])) as _,
                Arc::new(StringArray::from(vec![Some("c"), None])) as _,
            ],
        )
        .expect("second batch");
        CollectedQueryResult::new(schema, vec![first, second], 4).expect("result")
    }

    #[test]
    fn schema_summary_returns_column_metadata() {
        let result = two_batch_result();
        let columns = schema_summary(&result);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, "Int64");
        assert!(!columns[0].is_nullable);
        assert_eq!(columns[0].ordinal_position, 0);
        assert_eq!(columns[1].name, "name");
        assert!(columns[1].is_nullable);
    }

    #[test]
    fn slice_result_pages_across_batches() {
        let result = two_batch_result();
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset: 1,
                limit: 2,
                columns: None,
            },
        )
        .expect("slice");
        assert_eq!(page.row_count, 4);
        assert_eq!(page.offset, 1);
        assert_eq!(page.limit, 2);
        assert!(page.has_more);
        assert_eq!(page.next_offset, Some(3));
        assert_eq!(
            page.rows,
            vec![
                json!({"id": "2", "name": "b"}),
                json!({"id": "3", "name": "c"})
            ]
        );
    }

    #[test]
    fn slice_result_projects_columns_in_requested_order() {
        let result = two_batch_result();
        let columns = vec!["name".to_string(), "id".to_string()];
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: Some(&columns),
            },
        )
        .expect("slice");
        assert_eq!(
            page.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["name", "id"]
        );
        assert_eq!(page.rows, vec![json!({"name": "a", "id": "1"})]);
    }

    #[test]
    fn slice_result_limit_zero_returns_metadata_only() {
        let result = two_batch_result();
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 0,
                columns: None,
            },
        )
        .expect("slice");
        assert!(page.rows.is_empty());
        assert!(!page.has_more);
        assert_eq!(page.next_offset, None);
    }

    #[test]
    fn slice_result_offset_past_end_returns_empty_page() {
        let result = two_batch_result();
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset: 99,
                limit: 10,
                columns: None,
            },
        )
        .expect("slice");
        assert!(page.rows.is_empty());
        assert!(!page.has_more);
        assert_eq!(page.next_offset, None);
    }

    #[test]
    fn slice_result_rejects_empty_duplicate_missing_and_ambiguous_columns() {
        let result = two_batch_result();
        let empty: Vec<String> = Vec::new();
        slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: Some(&empty),
            },
        )
        .expect_err("empty projection should fail");

        let duplicate = vec!["id".to_string(), "id".to_string()];
        slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: Some(&duplicate),
            },
        )
        .expect_err("duplicate projection should fail");

        let missing = vec!["missing".to_string()];
        slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: Some(&missing),
            },
        )
        .expect_err("missing projection should fail");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as _,
                Arc::new(Int64Array::from(vec![2_i64])) as _,
            ],
        )
        .expect("batch");
        let ambiguous = CollectedQueryResult::new(schema, vec![batch], 1).expect("result");
        let selected = vec!["id".to_string()];
        slice_result(
            &ambiguous,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: Some(&selected),
            },
        )
        .expect_err("ambiguous projection should fail");
    }

    #[test]
    fn slice_result_preserves_json_safe_numbers_for_decimals() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(38, 9), false),
        ]));
        let amount = Decimal128Array::from(vec![123_456_789_012_345_678_901_i128])
            .with_precision_and_scale(38, 9)
            .expect("decimal");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![-8_504_475_857_937_456_387_i64])) as _,
                Arc::new(amount) as _,
            ],
        )
        .expect("batch");
        let result = CollectedQueryResult::new(schema, vec![batch], 1).expect("result");
        let page = slice_result(
            &result,
            ResultSliceRequest {
                offset: 0,
                limit: 1,
                columns: None,
            },
        )
        .expect("slice");
        assert_eq!(
            page.rows.first().and_then(|row| row.get("id")),
            Some(&Value::String("-8504475857937456387".to_string()))
        );
        assert_eq!(
            page.rows.first().and_then(|row| row.get("amount")),
            Some(&Value::String("123456789012.345678901".to_string()))
        );
    }

    #[test]
    fn estimated_bytes_uses_arrow_batch_memory() {
        let result = two_batch_result();
        assert!(result_estimated_bytes(&result) > 0);
    }
}
