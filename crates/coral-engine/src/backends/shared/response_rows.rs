//! Shared row extraction for backends that materialize rows as JSON values.
//!
//! HTTP and MCP backends both produce a JSON payload that needs to be turned
//! into a flat row list driven by [`ResponseSpec`]. This module owns the
//! `RowStrategy` switch so the two backends do not drift apart.

use serde_json::{Map, Value, json};

use coral_spec::{ResponseSpec, RowStrategy};

use crate::backends::shared::json_path::get_path_value;

/// Extracts the flat row list from `payload` according to `response`.
pub(crate) fn extract_rows(response: &ResponseSpec, payload: &Value) -> Vec<Value> {
    match response.row_strategy {
        RowStrategy::Direct => extract_direct(response, payload),
        RowStrategy::DictEntries => extract_dict_entries(response, payload),
        RowStrategy::SeriesPointList => extract_series_point_list(response, payload),
    }
}

fn extract_direct(response: &ResponseSpec, payload: &Value) -> Vec<Value> {
    match response_root(response, payload) {
        Value::Array(items) => items.clone(),
        Value::Null => Vec::new(),
        other => vec![other.clone()],
    }
}

fn extract_dict_entries(response: &ResponseSpec, payload: &Value) -> Vec<Value> {
    let Value::Object(map) = response_root(response, payload) else {
        return Vec::new();
    };
    map.iter()
        .map(|(key, value)| {
            let mut row = if let Value::Object(obj) = value {
                obj.clone()
            } else {
                let mut row = Map::new();
                row.insert("_value".to_string(), value.clone());
                row
            };
            row.insert("_key".to_string(), Value::String(key.clone()));
            Value::Object(row)
        })
        .collect()
}

fn extract_series_point_list(response: &ResponseSpec, payload: &Value) -> Vec<Value> {
    let root = response_root(response, payload);
    let series = root
        .as_array()
        .cloned()
        .or_else(|| {
            get_path_value(root, &["series".to_string()])
                .and_then(Value::as_array)
                .cloned()
        })
        .unwrap_or_default();

    let mut rows = Vec::new();
    for item in series {
        let metric = item
            .get("metric")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let scope = item
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let Some(pointlist) = item.get("pointlist").and_then(Value::as_array) else {
            continue;
        };
        for point in pointlist {
            let Some(pair) = point.as_array() else {
                continue;
            };
            let Some(raw_timestamp) = pair.first().and_then(Value::as_f64) else {
                continue;
            };
            let Some(value) = pair.get(1).and_then(Value::as_f64) else {
                continue;
            };
            // Range-check before the i64 cast. i64::MIN is exactly representable
            // in f64; i64::MAX rounds up to 2^63, so use it as an *exclusive*
            // upper bound — values ≥ 2^63 cannot be represented in i64.
            #[expect(
                clippy::cast_precision_loss,
                reason = "i64::MAX rounds up to 2^63, used as exclusive upper bound"
            )]
            let upper_exclusive = i64::MAX as f64;
            #[expect(
                clippy::cast_precision_loss,
                reason = "i64::MIN is exactly representable as f64"
            )]
            let lower_inclusive = i64::MIN as f64;
            if !raw_timestamp.is_finite()
                || !(lower_inclusive..upper_exclusive).contains(&raw_timestamp)
            {
                continue;
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "Series timestamps are integral epoch values; range checked above"
            )]
            let timestamp = raw_timestamp as i64;
            rows.push(json!({
                "metric": metric,
                "scope": scope,
                "timestamp": timestamp,
                "value": value
            }));
        }
    }
    rows
}

fn response_root<'a>(response: &ResponseSpec, payload: &'a Value) -> &'a Value {
    if response.rows_path.is_empty() {
        payload
    } else {
        get_path_value(payload, &response.rows_path).unwrap_or(&Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::extract_rows;
    use coral_spec::{ResponseSpec, RowStrategy};
    use serde_json::{Value, json};

    fn response(rows_path: &[&str], row_strategy: RowStrategy) -> ResponseSpec {
        ResponseSpec {
            rows_path: rows_path.iter().map(|s| (*s).to_string()).collect(),
            row_strategy,
            ..ResponseSpec::default()
        }
    }

    #[test]
    fn direct_returns_array_at_rows_path() {
        let payload = json!({ "items": [{ "id": 1 }, { "id": 2 }] });
        let rows = extract_rows(&response(&["items"], RowStrategy::Direct), &payload);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn direct_wraps_scalar_in_single_row() {
        let payload = json!({ "value": 42 });
        let rows = extract_rows(&response(&["value"], RowStrategy::Direct), &payload);
        assert_eq!(rows, vec![json!(42)]);
    }

    #[test]
    fn direct_returns_empty_for_missing_path() {
        let payload = json!({ "items": [] });
        let rows = extract_rows(&response(&["missing"], RowStrategy::Direct), &payload);
        assert!(rows.is_empty());
    }

    #[test]
    fn dict_entries_flattens_objects_with_key() {
        let payload = json!({
            "result": {
                "a": { "open": 1 },
                "b": { "open": 2 }
            }
        });
        let rows = extract_rows(&response(&["result"], RowStrategy::DictEntries), &payload);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(row.get("_key").is_some());
            assert!(row.get("open").is_some());
        }
    }

    #[test]
    fn dict_entries_wraps_scalar_values_in_value_field() {
        let payload = json!({ "result": { "a": 1.5, "b": 2.5 } });
        let rows = extract_rows(&response(&["result"], RowStrategy::DictEntries), &payload);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(row.get("_key").is_some());
            assert!(row.get("_value").is_some());
        }
    }

    #[test]
    fn dict_entries_returns_empty_for_non_object_root() {
        let payload = json!({ "result": [1, 2, 3] });
        let rows = extract_rows(&response(&["result"], RowStrategy::DictEntries), &payload);
        assert!(rows.is_empty());
    }

    #[test]
    fn series_point_list_skips_malformed_points() {
        let payload = json!({
            "series": [{
                "metric": "cpu",
                "scope": "host:demo",
                "pointlist": [
                    [1_710_000_000, 42.5],
                    [1_710_000_060],
                    [null, 1.0],
                    ["1710000120", 2.0]
                ]
            }]
        });
        let rows = extract_rows(&response(&[], RowStrategy::SeriesPointList), &payload);
        assert_eq!(
            rows,
            vec![json!({
                "metric": "cpu",
                "scope": "host:demo",
                "timestamp": 1_710_000_000_i64,
                "value": 42.5
            })]
        );
    }

    #[test]
    fn series_point_list_honors_rows_path() {
        let payload = json!({
            "result": {
                "series": [{
                    "metric": "cpu",
                    "scope": "host:demo",
                    "pointlist": [[1_710_000_000, 42.5]]
                }]
            }
        });
        let rows = extract_rows(
            &response(&["result"], RowStrategy::SeriesPointList),
            &payload,
        );
        assert_eq!(
            rows,
            vec![json!({
                "metric": "cpu",
                "scope": "host:demo",
                "timestamp": 1_710_000_000_i64,
                "value": 42.5
            })]
        );
    }

    #[test]
    fn series_point_list_accepts_rows_path_to_series_array() {
        let payload = json!({
            "result": {
                "series": [{
                    "metric": "cpu",
                    "scope": "host:demo",
                    "pointlist": [[1_710_000_000, 42.5]]
                }]
            }
        });
        let rows = extract_rows(
            &response(&["result", "series"], RowStrategy::SeriesPointList),
            &payload,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.first().and_then(|row| row.get("metric")),
            Some(&json!("cpu"))
        );
    }

    #[test]
    fn series_point_list_returns_empty_when_series_missing() {
        let payload = json!({});
        let rows: Vec<Value> = extract_rows(&response(&[], RowStrategy::SeriesPointList), &payload);
        assert!(rows.is_empty());
    }
}
