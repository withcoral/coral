//! Shared column-mapping logic for turning backend JSON rows into `Arrow` batches.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::arrow_type_for_column;
use crate::backends::shared::json_path::get_path_value;
use coral_spec::{ColumnSpec, ExprSpec};

#[allow(
    clippy::implicit_hasher,
    reason = "This helper operates on caller-provided HashMaps that always use the default hasher"
)]
/// Convert backend `JSON` rows into a typed `RecordBatch`.
///
/// # Errors
///
/// Returns a `DataFusionError` if any column expression cannot be evaluated or
/// if the resulting arrays cannot be assembled into the requested schema.
pub(crate) fn convert_items(
    columns: &[ColumnSpec],
    schema: SchemaRef,
    filters: &HashMap<String, String>,
    items: &[Value],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());

    for column in columns {
        let expr = column.resolved_expr();
        let data_type = arrow_type_for_column(column)?;

        match data_type {
            DataType::Utf8 => {
                let array: StringArray = items
                    .iter()
                    .map(|row| to_utf8(eval_expr(&expr, row, filters)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            DataType::Int64 => {
                let array: Int64Array = items
                    .iter()
                    .map(|row| to_i64(eval_expr(&expr, row, filters)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            DataType::Boolean => {
                // Preserve prior behavior: missing bool values are rendered as false.
                let array: BooleanArray = items
                    .iter()
                    .map(|row| Some(to_bool(eval_expr(&expr, row, filters)).unwrap_or(false)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            DataType::Float64 => {
                let array: Float64Array = items
                    .iter()
                    .map(|row| to_f64(eval_expr(&expr, row, filters)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported Arrow type in mapping: {other:?}"
                )));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn eval_expr(expr: &ExprSpec, row: &Value, filters: &HashMap<String, String>) -> Option<Value> {
    match expr {
        ExprSpec::Path { path } => get_path_value(row, path).cloned(),
        ExprSpec::Coalesce { exprs } => {
            for nested in exprs {
                let value = eval_expr(nested, row, filters);
                if value.as_ref().is_some_and(|v| !v.is_null()) {
                    return value;
                }
            }
            None
        }
        ExprSpec::FromFilter { key } => filters.get(key).map(|v| Value::String(v.clone())),
        ExprSpec::Literal { value } => Some(value.clone()),
        ExprSpec::Null => None,
        ExprSpec::JoinArray { path, separator } => {
            let values = get_path_value(row, path)?.as_array()?;
            let joined = values
                .iter()
                .filter_map(value_to_string_for_join)
                .collect::<Vec<_>>()
                .join(separator);
            Some(Value::String(joined))
        }
        ExprSpec::TagValue {
            path,
            key,
            key_field,
            value_field,
        } => {
            let tags = get_path_value(row, path)?.as_array()?;
            for item in tags {
                let is_match = item
                    .get(key_field)
                    .and_then(Value::as_str)
                    .is_some_and(|k| k == key);
                if is_match && let Some(v) = item.get(value_field) {
                    return Some(v.clone());
                }
            }
            None
        }
        ExprSpec::IfPresent { check, then_value } => {
            let value = eval_expr(check, row, filters);
            if value.as_ref().is_some_and(|v| !v.is_null()) {
                Some(Value::String(then_value.clone()))
            } else {
                None
            }
        }
        ExprSpec::JoinTagValues {
            path,
            key,
            key_field,
            value_field,
            separator,
        } => {
            let entries = get_path_value(row, path)?.as_array()?;
            let parts: Vec<String> = entries
                .iter()
                .filter(|item| {
                    item.get(key_field)
                        .and_then(Value::as_str)
                        .is_some_and(|k| k == key)
                })
                .filter_map(|item| item.get(value_field).and_then(value_to_string_for_join))
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(Value::String(parts.join(separator)))
            }
        }
        ExprSpec::FirstArrayItemPath { path, item_path } => {
            let first = get_path_value(row, path)?.as_array()?.first()?;
            get_path_value(first, item_path).cloned()
        }
        ExprSpec::ObjectFilterPath {
            path,
            filter_key,
            item_path,
        } => {
            let key = filters.get(filter_key)?;
            let selected = get_path_value(row, path)?.as_object()?.get(key)?;
            if item_path.is_empty() {
                Some(selected.clone())
            } else {
                get_path_value(selected, item_path).cloned()
            }
        }
        ExprSpec::CurrentRow => Some(row.clone()),
    }
}

fn value_to_string_for_join(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(v) => Some(v.clone()),
        Value::Bool(v) => Some(v.to_string()),
        Value::Number(v) => Some(v.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).ok(),
    }
}

fn to_utf8(value: Option<Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(v) => Some(v),
        Value::Bool(v) => Some(v.to_string()),
        Value::Number(v) => Some(v.to_string()),
        Value::Array(v) => serde_json::to_string(&v).ok(),
        Value::Object(v) => serde_json::to_string(&v).ok(),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    reason = "JSON numeric coercion intentionally accepts lossy conversions into i64 for downstream consumers"
)]
fn to_i64(value: Option<Value>) -> Option<i64> {
    match value? {
        Value::Number(v) => v
            .as_i64()
            .or_else(|| v.as_f64().map(|f| f as i64))
            .or_else(|| v.as_u64().map(|u| u as i64)),
        Value::String(v) => v.parse::<i64>().ok(),
        Value::Bool(v) => Some(i64::from(v)),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "JSON numeric coercion intentionally permits i64-to-f64 precision loss"
)]
fn to_f64(value: Option<Value>) -> Option<f64> {
    match value? {
        Value::Number(v) => v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)),
        Value::String(v) => v.parse::<f64>().ok(),
        Value::Bool(v) => Some(if v { 1.0 } else { 0.0 }),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn to_bool(value: Option<Value>) -> Option<bool> {
    match value? {
        Value::Bool(v) => Some(v),
        Value::Number(v) => v.as_i64().map(|n| n != 0),
        Value::String(v) => {
            let v = v.trim().to_ascii_lowercase();
            match v.as_str() {
                "true" | "t" | "1" | "yes" => Some(true),
                "false" | "f" | "0" | "no" => Some(false),
                _ => None,
            }
        }
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::convert_items;
    use crate::backends::schema_from_columns;
    use coral_spec::backends::http::HttpTableSpec;
    use coral_spec::{ExprSpec, RequestSpec, parse_source_manifest_value};
    use datafusion::arrow::array::{Array, StringArray};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn table_with_expr(name: &str, expr: &ExprSpec) -> HttpTableSpec {
        parse_source_manifest_value(serde_json::json!({
            "dsl_version": 3,
            "name": "test",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "tables": [{
                "name": "t",
                "description": "test",
                "request": request_json(&RequestSpec::default()),
                "columns": [{
                    "name": name,
                    "type": "Utf8",
                    "expr": expr_json(expr),
                }]
            }]
        }))
        .expect("manifest should parse")
        .as_http()
        .expect("http manifest")
        .tables[0]
            .clone()
    }

    fn request_json(request: &RequestSpec) -> Value {
        let path = if request.path.is_empty() {
            "/items"
        } else {
            request.path.raw()
        };
        json!({
            "method": format!("{:?}", request.method),
            "path": path,
            "query": [],
            "body": [],
            "headers": [],
        })
    }

    fn expr_json(expr: &ExprSpec) -> Value {
        match expr {
            ExprSpec::Path { path } => json!({ "kind": "path", "path": path }),
            ExprSpec::IfPresent { check, then_value } => json!({
                "kind": "if_present",
                "check": expr_json(check),
                "then_value": then_value,
            }),
            ExprSpec::JoinTagValues {
                path,
                key,
                key_field,
                value_field,
                separator,
            } => json!({
                "kind": "join_tag_values",
                "path": path,
                "key": key,
                "key_field": key_field,
                "value_field": value_field,
                "separator": separator,
            }),
            other => panic!("unsupported test expr: {other:?}"),
        }
    }

    #[test]
    fn if_present_returns_value_when_check_succeeds() {
        let table = table_with_expr(
            "label",
            &ExprSpec::IfPresent {
                check: Box::new(ExprSpec::Path {
                    path: vec!["status".into()],
                }),
                then_value: "has_status".into(),
            },
        );
        let schema = schema_from_columns(table.columns(), "test", table.name()).unwrap();
        let items = vec![
            serde_json::json!({"status": "ok"}),
            serde_json::json!({"other": "field"}),
        ];
        let batch = convert_items(table.columns(), schema, &HashMap::new(), &items).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "has_status");
        assert!(col.is_null(1));
    }

    #[test]
    fn if_present_treats_explicit_null_as_absent() {
        let table = table_with_expr(
            "label",
            &ExprSpec::IfPresent {
                check: Box::new(ExprSpec::Path {
                    path: vec!["status".into()],
                }),
                then_value: "present".into(),
            },
        );
        let schema = schema_from_columns(table.columns(), "test", table.name()).unwrap();
        let items = vec![serde_json::json!({"status": null})];
        let batch = convert_items(table.columns(), schema, &HashMap::new(), &items).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.is_null(0), "explicit null should not trigger IfPresent");
    }

    #[test]
    fn join_tag_values_concatenates_matching_items() {
        let table = table_with_expr(
            "texts",
            &ExprSpec::JoinTagValues {
                path: vec!["content".into()],
                key: "text".into(),
                key_field: "type".into(),
                value_field: "text".into(),
                separator: "|".into(),
            },
        );
        let schema = schema_from_columns(table.columns(), "test", table.name()).unwrap();
        let items = vec![
            serde_json::json!({
                "content": [
                    {"type": "thinking", "thinking": "hmm"},
                    {"type": "text", "text": "hello"},
                    {"type": "text", "text": "world"}
                ]
            }),
            serde_json::json!({"content": [{"type": "tool_use", "name": "Read"}]}),
            serde_json::json!({"content": "plain string"}),
        ];
        let batch = convert_items(table.columns(), schema, &HashMap::new(), &items).unwrap();
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello|world");
        assert!(col.is_null(1), "no matching tags should yield null");
        assert!(col.is_null(2), "non-array content should yield null");
    }

    #[test]
    fn join_tag_values_single_match_no_separator() {
        let table = table_with_expr(
            "text",
            &ExprSpec::JoinTagValues {
                path: vec!["content".into()],
                key: "text".into(),
                key_field: "type".into(),
                value_field: "text".into(),
                separator: "|".into(),
            },
        );
        let schema = schema_from_columns(table.columns(), "test", table.name()).unwrap();
        let items = vec![serde_json::json!({
            "content": [{"type": "text", "text": "only one"}]
        })];
        let batch = convert_items(table.columns(), schema, &HashMap::new(), &items).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "only one");
    }
}
