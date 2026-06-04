//! Shared column-mapping logic for turning backend JSON rows into `Arrow` batches.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::DateTime;
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::shared::json_path::get_path_value;
use coral_spec::{
    ColumnSpec, ExprSpec, ManifestDataType, ParsedTemplate, TemplateNamespace, TemplatePart,
    TimestampInput,
};

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
    args: &HashMap<String, String>,
    items: &[Value],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());

    for column in columns {
        let expr = column.resolved_expr();
        let data_type = column
            .manifest_data_type()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;

        match data_type {
            ManifestDataType::Utf8 => {
                let array: StringArray = items
                    .iter()
                    .map(|row| to_utf8(eval_expr(&expr, row, filters, args)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            ManifestDataType::Json => {
                let array: StringArray = items
                    .iter()
                    .map(|row| {
                        eval_expr(&expr, row, filters, args)
                            .and_then(|value| json_value_to_text(&value))
                    })
                    .collect();
                arrays.push(Arc::new(array));
            }
            ManifestDataType::Int64 => {
                let array: Int64Array = items
                    .iter()
                    .map(|row| to_i64(eval_expr(&expr, row, filters, args)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            ManifestDataType::Boolean => {
                let array: BooleanArray = items
                    .iter()
                    .map(|row| to_bool(eval_expr(&expr, row, filters, args)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            ManifestDataType::Float64 => {
                let array: Float64Array = items
                    .iter()
                    .map(|row| to_f64(eval_expr(&expr, row, filters, args)))
                    .collect();
                arrays.push(Arc::new(array));
            }
            ManifestDataType::Timestamp => {
                let array: TimestampMicrosecondArray = items
                    .iter()
                    .map(|row| to_i64(eval_expr(&expr, row, filters, args)))
                    .collect();
                let array = array.with_timezone("+00:00");
                arrays.push(Arc::new(array));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn eval_expr(
    expr: &ExprSpec,
    row: &Value,
    filters: &HashMap<String, String>,
    args: &HashMap<String, String>,
) -> Option<Value> {
    match expr {
        ExprSpec::Path { path } => get_path_value(row, path).cloned(),
        ExprSpec::Coalesce { exprs } => {
            for nested in exprs {
                let value = eval_expr(nested, row, filters, args);
                if value.as_ref().is_some_and(|v| !v.is_null()) {
                    return value;
                }
            }
            None
        }
        ExprSpec::FromFilter { key } => filters.get(key).map(|v| Value::String(v.clone())),
        ExprSpec::FromArg { key } => args.get(key).map(|v| Value::String(v.clone())),
        ExprSpec::Literal { value } => Some(value.clone()),
        ExprSpec::Null => None,
        ExprSpec::JoinArray { path, separator } => eval_join_array(row, path, separator),
        ExprSpec::JoinArrayPath {
            path,
            item_path,
            separator,
        } => eval_join_array_path(row, path, item_path, separator),
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
            let value = eval_expr(check, row, filters, args);
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
        ExprSpec::FormatTimestamp { expr, input } => {
            eval_format_timestamp(expr, input, row, filters, args)
        }
        ExprSpec::Base64Decode { expr } => eval_base64_decode(expr, row, filters, args),
        ExprSpec::Replace { expr, from, to } => {
            let raw = to_utf8(eval_expr(expr, row, filters, args))?;
            Some(Value::String(raw.replace(from, to)))
        }
        ExprSpec::Template { template, values } => {
            eval_template(template, values, row, filters, args)
        }
    }
}

/// Evaluate a `FormatTimestamp` expression, returning epoch **microseconds** as
/// a `Value::Number` suitable for an Arrow `TimestampMicrosecondArray`.
fn eval_format_timestamp(
    expr: &ExprSpec,
    input: &TimestampInput,
    row: &Value,
    filters: &HashMap<String, String>,
    args: &HashMap<String, String>,
) -> Option<Value> {
    let value = eval_expr(expr, row, filters, args)?;
    let micros = match &value {
        Value::String(s) => parse_timestamp_micros(s, input),
        Value::Number(n) => {
            // serde_json stores numbers as f64 internally, so precision may
            // already be reduced. Try the string representation first to
            // recover as much precision as possible.
            let s = n.to_string();
            parse_timestamp_micros(&s, input)
        }
        _ => None,
    }?;
    Some(Value::Number(micros.into()))
}

fn parse_timestamp_micros(s: &str, input: &TimestampInput) -> Option<i64> {
    match input {
        TimestampInput::Seconds | TimestampInput::Milliseconds => parse_epoch_micros(s, input),
        TimestampInput::Iso8601 => parse_iso8601_micros(s),
    }
}

/// Parse an ISO 8601 / RFC 3339 timestamp string into epoch microseconds.
fn parse_iso8601_micros(s: &str) -> Option<i64> {
    Some(DateTime::parse_from_rfc3339(s).ok()?.timestamp_micros())
}

/// Parse a timestamp string into epoch microseconds without intermediate
/// floating-point arithmetic, preserving full microsecond precision.
fn parse_epoch_micros(s: &str, input: &TimestampInput) -> Option<i64> {
    let (int_str, frac_str) = s.split_once('.').unwrap_or((s, ""));
    let whole: i64 = int_str.parse().ok()?;
    let frac_width: usize = match input {
        TimestampInput::Seconds => 6,
        TimestampInput::Milliseconds => 3,
        TimestampInput::Iso8601 => return None,
    };
    let padded = format!("{frac_str:0<frac_width$}");
    let frac: i64 = padded.get(..frac_width)?.parse().ok()?;
    let multiplier: i64 = match input {
        TimestampInput::Seconds => 1_000_000,
        TimestampInput::Milliseconds => 1_000,
        TimestampInput::Iso8601 => return None,
    };
    Some(whole * multiplier + frac)
}

fn eval_base64_decode(
    expr: &ExprSpec,
    row: &Value,
    filters: &HashMap<String, String>,
    args: &HashMap<String, String>,
) -> Option<Value> {
    let raw = to_utf8(eval_expr(expr, row, filters, args))?;
    let compact = raw
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let decoded = BASE64_STANDARD.decode(compact).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    Some(Value::String(text))
}

fn eval_template(
    template: &ParsedTemplate,
    values: &HashMap<String, ExprSpec>,
    row: &Value,
    filters: &HashMap<String, String>,
    args: &HashMap<String, String>,
) -> Option<Value> {
    // Pre-evaluate every key so replacements are deterministic regardless of
    // HashMap iteration order.
    let evaluated: HashMap<&str, Option<String>> = values
        .iter()
        .map(|(key, expr)| {
            let raw = eval_expr(expr, row, filters, args).and_then(|v| to_utf8(Some(v)));
            (key.as_str(), raw)
        })
        .collect();

    let mut result = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => result.push_str(part),
            TemplatePart::Token(token) => match token.namespace() {
                TemplateNamespace::Expr => {
                    let rendered = evaluated
                        .get(token.key())
                        .and_then(Clone::clone)
                        .or_else(|| token.default_value().map(ToString::to_string))
                        .unwrap_or_default();
                    result.push_str(&rendered);
                }
                TemplateNamespace::Filter => {
                    let rendered = filters
                        .get(token.key())
                        .cloned()
                        .or_else(|| token.default_value().map(ToString::to_string))
                        .unwrap_or_default();
                    result.push_str(&rendered);
                }
                TemplateNamespace::Arg => {
                    let rendered = args
                        .get(token.key())
                        .cloned()
                        .or_else(|| token.default_value().map(ToString::to_string))
                        .unwrap_or_default();
                    result.push_str(&rendered);
                }
                TemplateNamespace::Input
                | TemplateNamespace::State
                | TemplateNamespace::Other(_) => return None,
            },
        }
    }

    Some(Value::String(result))
}

fn eval_join_array(row: &Value, path: &[String], separator: &str) -> Option<Value> {
    let values = get_path_value(row, path)?.as_array()?;
    let joined = join_values(values.iter(), separator);
    Some(Value::String(joined))
}

fn eval_join_array_path(
    row: &Value,
    path: &[String],
    item_path: &[String],
    separator: &str,
) -> Option<Value> {
    let values = get_path_value(row, path)?.as_array()?;
    let joined = join_values(
        values
            .iter()
            .filter_map(|item| get_path_value(item, item_path)),
        separator,
    );
    Some(Value::String(joined))
}

fn join_values<'a>(values: impl Iterator<Item = &'a Value>, separator: &str) -> String {
    values
        .filter_map(value_to_string_for_join)
        .collect::<Vec<_>>()
        .join(separator)
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

pub(crate) fn json_value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) if is_json_text(value) => Some(value.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

fn is_json_text(value: &str) -> bool {
    let trimmed = value.trim_start();
    if !matches!(trimmed.as_bytes().first(), Some(b'{' | b'[' | b'"')) {
        return false;
    }
    serde_json::from_str::<Value>(value).is_ok()
}

fn to_i64(value: Option<Value>) -> Option<i64> {
    match value? {
        Value::Number(v) => v
            .as_i64()
            .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok())),
        Value::String(v) => v.parse::<i64>().ok(),
        Value::Bool(v) => Some(i64::from(v)),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[expect(
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
    use super::{convert_items, eval_template, parse_iso8601_micros};
    use crate::backends::schema_from_columns;
    use coral_spec::{ColumnSpec, ExprSpec, ParsedTemplate, TimestampInput};
    use datafusion::arrow::array::{
        Array, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn convert_expr_items(data_type: &str, expr: ExprSpec, items: &[Value]) -> RecordBatch {
        let columns = [ColumnSpec {
            name: "value".into(),
            data_type: data_type.into(),
            nullable: true,
            r#virtual: false,
            description: String::new(),
            expr: Some(expr),
        }];
        let schema = schema_from_columns(&columns, "test", "t").unwrap();
        convert_items(&columns, schema, &HashMap::new(), &HashMap::new(), items).unwrap()
    }

    fn path(name: &str) -> ExprSpec {
        ExprSpec::Path {
            path: vec![name.into()],
        }
    }

    fn first_column<T: Array + 'static>(batch: &RecordBatch) -> &T {
        batch.column(0).as_any().downcast_ref::<T>().unwrap()
    }

    #[test]
    fn format_timestamp_parses_iso8601_strings() {
        let items = vec![serde_json::json!({
            "created_time": "2026-03-11T12:34:56.123456Z"
        })];
        let batch = convert_expr_items(
            "Timestamp",
            ExprSpec::FormatTimestamp {
                input: TimestampInput::Iso8601,
                expr: Box::new(path("created_time")),
            },
            &items,
        );
        let col = first_column::<TimestampMicrosecondArray>(&batch);

        assert_eq!(col.value(0), 1_773_232_496_123_456);
    }

    #[test]
    fn parse_iso8601_micros_returns_epoch_microseconds() {
        assert_eq!(
            parse_iso8601_micros("2026-03-11T12:34:56.123456Z"),
            Some(1_773_232_496_123_456)
        );
    }

    #[test]
    fn if_present_returns_value_when_check_succeeds() {
        let items = vec![
            serde_json::json!({"status": "ok"}),
            serde_json::json!({"other": "field"}),
        ];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::IfPresent {
                check: Box::new(path("status")),
                then_value: "has_status".into(),
            },
            &items,
        );
        assert_eq!(batch.num_rows(), 2);
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "has_status");
        assert!(col.is_null(1));
    }

    #[test]
    fn if_present_treats_explicit_null_as_absent() {
        let items = vec![serde_json::json!({"status": null})];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::IfPresent {
                check: Box::new(path("status")),
                then_value: "present".into(),
            },
            &items,
        );
        let col = first_column::<StringArray>(&batch);
        assert!(col.is_null(0), "explicit null should not trigger IfPresent");
    }

    #[test]
    fn join_tag_values_concatenates_matching_items() {
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
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::JoinTagValues {
                path: vec!["content".into()],
                key: "text".into(),
                key_field: "type".into(),
                value_field: "text".into(),
                separator: "|".into(),
            },
            &items,
        );
        assert_eq!(batch.num_rows(), 3);
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "hello|world");
        assert!(col.is_null(1), "no matching tags should yield null");
        assert!(col.is_null(2), "non-array content should yield null");
    }

    #[test]
    fn join_tag_values_single_match_no_separator() {
        let items = vec![serde_json::json!({
            "content": [{"type": "text", "text": "only one"}]
        })];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::JoinTagValues {
                path: vec!["content".into()],
                key: "text".into(),
                key_field: "type".into(),
                value_field: "text".into(),
                separator: "|".into(),
            },
            &items,
        );
        assert_eq!(batch.num_rows(), 1);
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "only one");
    }

    #[test]
    fn join_array_path_concatenates_nested_scalar_values() {
        let items = vec![
            serde_json::json!({
                "labels": {
                    "nodes": [
                        {"name": "bug"},
                        {"name": "customer"},
                        {"color": "#fff"}
                    ]
                }
            }),
            serde_json::json!({"labels": {"nodes": []}}),
            serde_json::json!({"labels": {"nodes": [{"name": null}]}}),
        ];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::JoinArrayPath {
                path: vec!["labels".into(), "nodes".into()],
                item_path: vec!["name".into()],
                separator: ",".into(),
            },
            &items,
        );
        assert_eq!(batch.num_rows(), 3);
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "bug,customer");
        assert_eq!(col.value(1), "");
        assert_eq!(col.value(2), "");
    }

    #[test]
    fn boolean_columns_preserve_nulls_and_false_values() {
        let items = vec![
            serde_json::json!({"enabled": true}),
            serde_json::json!({"enabled": false}),
            serde_json::json!({"enabled": null}),
            serde_json::json!({}),
            serde_json::json!({"enabled": "not-a-bool"}),
        ];
        let batch = convert_expr_items("Boolean", path("enabled"), &items);
        assert_eq!(batch.num_rows(), 5);
        let col = first_column::<BooleanArray>(&batch);
        assert!(col.value(0));
        assert!(!col.value(1));
        assert!(col.is_null(2));
        assert!(col.is_null(3));
        assert!(col.is_null(4));
    }

    #[test]
    fn int64_columns_reject_lossy_json_numbers() {
        let items = vec![
            serde_json::from_str(r#"{"id": 1.9}"#).unwrap(),
            serde_json::from_str(r#"{"id": -1.9}"#).unwrap(),
            serde_json::from_str(r#"{"id": 1.0}"#).unwrap(),
            serde_json::from_str(r#"{"id": 1e3}"#).unwrap(),
            serde_json::from_str(r#"{"id": 1.0000000000000001}"#).unwrap(),
            json!({"id": i64::MAX}),
            json!({"id": 9_223_372_036_854_775_808_u64}),
            json!({"id": u64::MAX}),
            json!({"id": i64::MAX.to_string()}),
            json!({"id": "9223372036854775808"}),
        ];

        let batch = convert_expr_items("Int64", path("id"), &items);
        let col = first_column::<Int64Array>(&batch);

        let expected = [
            None,
            None,
            None,
            None,
            None,
            Some(i64::MAX),
            None,
            None,
            Some(i64::MAX),
            None,
        ];
        assert_eq!(col.len(), expected.len());
        for (idx, expected) in expected.into_iter().enumerate() {
            match expected {
                Some(value) => assert_eq!(col.value(idx), value, "row {idx}"),
                None => assert!(col.is_null(idx), "row {idx} should be null"),
            }
        }
    }

    #[test]
    fn replace_expr_rewrites_string_values() {
        let items = vec![serde_json::json!({"title": "hello world"})];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::Replace {
                expr: Box::new(path("title")),
                from: " ".into(),
                to: "-".into(),
            },
            &items,
        );
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "hello-world");
    }

    #[test]
    fn base64_decode_expr_decodes_wrapped_utf8_text() {
        let items = vec![
            serde_json::json!({"content": "aGVs\nbG8="}),
            serde_json::json!({"content": "not base64"}),
        ];
        let batch = convert_expr_items(
            "Utf8",
            ExprSpec::Base64Decode {
                expr: Box::new(path("content")),
            },
            &items,
        );
        let col = first_column::<StringArray>(&batch);
        assert_eq!(col.value(0), "hello");
        assert!(col.is_null(1));
    }

    #[test]
    fn template_renders_expr_and_filter_tokens_with_defaults() {
        let rendered = eval_template(
            &ParsedTemplate::parse("{{filter.org|default-org}}/{{expr.slug|untitled}}")
                .expect("template"),
            &HashMap::from([(
                "slug".to_string(),
                ExprSpec::Replace {
                    expr: Box::new(path("title")),
                    from: " ".into(),
                    to: "-".into(),
                },
            )]),
            &json!({"title": "hello world"}),
            &HashMap::from([("org".to_string(), "acme".to_string())]),
            &HashMap::new(),
        );

        assert_eq!(
            rendered,
            Some(Value::String("acme/hello-world".to_string()))
        );
    }

    #[test]
    fn template_uses_default_for_missing_expr_value() {
        let rendered = eval_template(
            &ParsedTemplate::parse("{{expr.slug|untitled}}").expect("template"),
            &HashMap::from([("slug".to_string(), path("missing"))]),
            &json!({"title": "hello world"}),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert_eq!(rendered, Some(Value::String("untitled".to_string())));
    }

    #[test]
    fn json_type_serializes_values_as_valid_json_strings() {
        let items = vec![
            json!({"properties": {"country": "US", "count": 3}}),
            json!({"properties": "hello"}),
            json!({"properties": "{\"country\":\"DE\",\"count\":7}"}),
            json!({"properties": true}),
            json!({"properties": 3}),
            json!({"properties": null}),
            json!({}),
        ];
        let batch = convert_expr_items("Json", path("properties"), &items);

        let col = first_column::<StringArray>(&batch);
        assert_eq!(
            serde_json::from_str::<Value>(col.value(0)).unwrap(),
            json!({"country": "US", "count": 3}),
        );
        assert_eq!(
            serde_json::from_str::<Value>(col.value(1)).unwrap(),
            json!("hello")
        );
        assert_eq!(
            serde_json::from_str::<Value>(col.value(2)).unwrap(),
            json!({"country": "DE", "count": 7}),
        );
        assert_eq!(
            serde_json::from_str::<Value>(col.value(3)).unwrap(),
            json!(true)
        );
        assert_eq!(
            serde_json::from_str::<Value>(col.value(4)).unwrap(),
            json!(3)
        );
        assert!(col.is_null(5));
        assert!(col.is_null(6));
    }
}
