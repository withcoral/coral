//! Tracks whether planted needle rows appear in query results.

use std::io::Cursor;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::SecondsFormat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;

use super::{NeedleSpec, error::NeedleError};

/// A single overlapping column is too collision-prone to count as retrieval.
const MIN_MATCHING_COLUMNS: usize = 2;

pub(crate) struct NeedleTracker {
    log_path: PathBuf,
    specs: Vec<NeedleSpec>,
}

#[derive(Debug)]
struct MatchCheck {
    column_index: usize,
    expected_value: ScalarValue,
}

#[derive(serde::Serialize)]
struct NeedleLogEntry<'a> {
    ts: String,
    schema: &'a str,
    table: &'a str,
    needle: &'a serde_json::Map<String, serde_json::Value>,
    sql: &'a str,
}

impl NeedleTracker {
    pub(crate) fn new(log_path: PathBuf, specs: Vec<NeedleSpec>) -> Self {
        Self { log_path, specs }
    }

    pub(crate) fn check_and_log(
        &self,
        sql: &str,
        batches: &[RecordBatch],
    ) -> Result<(), NeedleError> {
        let matching_specs = self
            .specs
            .iter()
            .filter(|spec| result_contains_needle(batches, &spec.column_values))
            .collect::<Vec<_>>();
        if matching_specs.is_empty() {
            return Ok(());
        }

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .map_err(|e| NeedleError::io(&self.log_path, e))?;
        let ts = chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

        for spec in matching_specs {
            let line = NeedleLogEntry {
                ts: ts.clone(),
                schema: &spec.schema,
                table: &spec.table,
                needle: &spec.column_values,
                sql,
            };
            let mut encoded = serde_json::to_vec(&line)
                .map_err(|e| NeedleError::JsonConversion(e.to_string()))?;
            encoded.push(b'\n');
            file.write_all(&encoded)
                .map_err(|e| NeedleError::io(&self.log_path, e))?;
        }

        Ok(())
    }
}

fn result_contains_needle(
    batches: &[RecordBatch],
    column_values: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    batches
        .iter()
        .any(|batch| batch_contains_needle(batch, column_values))
}

fn batch_contains_needle(
    batch: &RecordBatch,
    column_values: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let Some(checks) = build_match_checks(batch, column_values) else {
        return false;
    };
    if checks.len() < MIN_MATCHING_COLUMNS {
        return false;
    }

    (0..batch.num_rows()).any(|row| row_matches_needle(batch, row, &checks))
}

fn build_match_checks(
    batch: &RecordBatch,
    column_values: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<MatchCheck>> {
    let schema = batch.schema();
    let mut checks = Vec::new();

    for (column_name, json_value) in column_values {
        let Ok(column_index) = schema.index_of(column_name) else {
            continue;
        };
        let data_type = schema.field(column_index).data_type();
        let expected_value = json_to_scalar(json_value, data_type)?;

        checks.push(MatchCheck {
            column_index,
            expected_value,
        });
    }

    Some(checks)
}

fn row_matches_needle(batch: &RecordBatch, row: usize, checks: &[MatchCheck]) -> bool {
    checks.iter().all(|check| {
        ScalarValue::try_from_array(batch.column(check.column_index), row)
            .ok()
            .as_ref()
            == Some(&check.expected_value)
    })
}

/// Converts one JSON value into the batch column's scalar type by round-tripping
/// through Arrow's NDJSON reader instead of hand-implementing JSON -> `ScalarValue`.
fn json_to_scalar(value: &serde_json::Value, data_type: &DataType) -> Option<ScalarValue> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        data_type.clone(),
        true,
    )]));
    let mut line = serde_json::to_vec(&serde_json::json!({ "value": value })).ok()?;
    line.push(b'\n');

    let mut reader = ReaderBuilder::new(schema).build(Cursor::new(line)).ok()?;
    let batch = reader.next()?.ok()?;
    ScalarValue::try_from_array(batch.column(0), 0).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use serde_json::json;

    use super::*;

    fn query_results(ids: &[&str], values: &[i32]) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(ids.to_vec())),
                    Arc::new(Int32Array::from(values.to_vec())),
                ],
            )
            .expect("query results batch should build"),
        ]
    }

    #[test]
    fn json_to_scalar_supports_uint64() {
        let scalar = json_to_scalar(&json!(42), &DataType::UInt64);
        assert_eq!(scalar, Some(ScalarValue::UInt64(Some(42))));
    }

    #[test]
    fn json_to_scalar_supports_timestamps() {
        let scalar = json_to_scalar(
            &json!("2024-01-02T03:04:05Z"),
            &DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
        );
        assert_eq!(
            scalar,
            Some(ScalarValue::TimestampMicrosecond(
                Some(1_704_164_645_000_000),
                Some("UTC".into()),
            ))
        );
    }

    #[test]
    fn batch_contains_needle_matches_and_rejects_non_matching_rows() {
        let batches = query_results(&["needle-1", "other"], &[42, 1]);
        let needle = json!({"id": "needle-1", "value": 42});
        let miss = json!({"id": "needle-1", "value": 999});

        assert!(batch_contains_needle(
            &batches[0],
            needle.as_object().expect("object")
        ));
        assert!(!batch_contains_needle(
            &batches[0],
            miss.as_object().expect("object")
        ));
    }

    #[test]
    fn single_column_overlap_is_ignored() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["needle-1"]))])
                .expect("single-column batch should build");
        let needle = json!({"id": "needle-1", "value": 42});

        assert!(!batch_contains_needle(
            &batch,
            needle.as_object().expect("needle should be object")
        ));
    }

    #[test]
    fn conversion_failure_prevents_match() {
        let batches = query_results(&["needle-1"], &[42]);
        let needle = json!({"id": "needle-1", "value": "not-an-int"});

        assert!(!batch_contains_needle(
            &batches[0],
            needle.as_object().expect("needle should be object")
        ));
    }

    #[test]
    fn check_and_log_writes_ndjson_for_matches() {
        let dir = tempfile::TempDir::new().expect("tempdir should be created");
        let log_path = dir.path().join("needles.yaml.log");
        let tracker = NeedleTracker::new(
            log_path.clone(),
            vec![
                NeedleSpec {
                    schema: "test".to_string(),
                    table: "items".to_string(),
                    column_values: json!({"id": "needle-1", "value": 42})
                        .as_object()
                        .expect("object")
                        .clone(),
                },
                NeedleSpec {
                    schema: "test".to_string(),
                    table: "items".to_string(),
                    column_values: json!({"id": "needle-2", "value": 99})
                        .as_object()
                        .expect("object")
                        .clone(),
                },
            ],
        );

        tracker
            .check_and_log(
                "SELECT id, value FROM test.items",
                &query_results(&["needle-1"], &[42]),
            )
            .expect("matching tracker write should succeed");

        let log = std::fs::read_to_string(&log_path).expect("log should be readable");
        let lines = log.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);

        let entry: serde_json::Value =
            serde_json::from_str(lines[0]).expect("log entry should parse as JSON");
        assert_eq!(entry["schema"], "test");
        assert_eq!(entry["table"], "items");
        assert_eq!(entry["needle"]["id"], "needle-1");
        assert_eq!(entry["sql"], "SELECT id, value FROM test.items");
    }
}
