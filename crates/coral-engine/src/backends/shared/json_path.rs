//! Small helpers for reading nested values from JSON payloads.

use serde_json::Value;

/// Resolves a nested JSON path from `root`.
pub(crate) fn get_path_value<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(root);
    }

    let mut current = root;
    for segment in path {
        if let Ok(index) = segment.parse::<usize>() {
            current = current.as_array()?.get(index)?;
        } else {
            current = current.get(segment)?;
        }
    }
    Some(current)
}
