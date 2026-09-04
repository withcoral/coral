//! Small helpers for reading nested values from JSON payloads.

use serde_json::Value;

/// Resolves object keys and numeric array indices along a path from `root`.
pub(crate) fn get_path_value<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(root);
    }

    let mut current = root;
    for segment in path {
        if let Value::Array(values) = current {
            current = values.get(segment.parse::<usize>().ok()?)?;
        } else {
            current = current.get(segment)?;
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::get_path_value;

    fn path(segments: &[&str]) -> Vec<String> {
        segments
            .iter()
            .map(|segment| (*segment).to_string())
            .collect()
    }

    #[test]
    fn numeric_segments_select_object_keys() {
        let payload = json!({
            "0": "root",
            "nested": {"1": "nested"}
        });

        assert_eq!(
            get_path_value(&payload, &path(&["0"])).and_then(Value::as_str),
            Some("root")
        );
        assert_eq!(
            get_path_value(&payload, &path(&["nested", "1"])).and_then(Value::as_str),
            Some("nested")
        );
    }

    #[test]
    fn numeric_segments_keep_selecting_array_indices() {
        let payload = json!({"items": ["first", "second"]});

        assert_eq!(
            get_path_value(&payload, &path(&["items", "1"])).and_then(Value::as_str),
            Some("second")
        );
    }
}
