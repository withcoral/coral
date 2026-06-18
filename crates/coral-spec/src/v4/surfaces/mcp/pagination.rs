use serde_json::Value;

use crate::v4::surfaces::json_schema::json_schema_type_contains;

pub(super) fn find_response_cursor_path(schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    for (name, property) in properties {
        if is_response_cursor_property(name, property) {
            return Some(vec![name.clone()]);
        }
    }
    for (name, property) in properties {
        if !json_schema_type_contains(property, "object") {
            continue;
        }
        if let Some(mut path) = find_response_cursor_path(property) {
            path.insert(0, name.clone());
            return Some(path);
        }
    }
    None
}

fn is_response_cursor_property(name: &str, schema: &Value) -> bool {
    const RESPONSE_CURSORS: &[&str] = &["nextcursor", "nextpagetoken", "nexttoken", "endcursor"];
    RESPONSE_CURSORS.contains(&cursor_token(name).as_str())
        && (json_schema_type_contains(schema, "string") || schema.get("type").is_none())
}

fn cursor_token(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}
