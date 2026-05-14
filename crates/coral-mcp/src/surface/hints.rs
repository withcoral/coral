use serde_json::{Value, json};

pub(crate) struct DiscoveryHint {
    pub(crate) message: &'static str,
    pub(crate) suggested_tools: &'static [&'static str],
}

pub(crate) const TABLE_FUNCTIONS_ARE_SEPARATE: DiscoveryHint = DiscoveryHint {
    message: "Table discovery only returns ordinary tables. If you do not find the capability you need, also use list_table_functions or search_table_functions; provider-native search, lookup, and range capabilities may exist only as table functions.",
    suggested_tools: &["list_table_functions", "search_table_functions"],
};

pub(crate) fn add_hints(value: &mut Value, hints: &[DiscoveryHint]) {
    if hints.is_empty() {
        return;
    }
    let object = value
        .as_object_mut()
        .expect("hinted MCP result should be a JSON object");
    let existing_hints = object.entry("hints").or_insert_with(|| json!([]));
    existing_hints
        .as_array_mut()
        .expect("hints should be a JSON array")
        .extend(hints.iter().map(hint_value));
}

pub(crate) fn hints_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "required": ["message", "suggested_tools"],
            "additionalProperties": false,
            "properties": {
                "message": { "type": "string" },
                "suggested_tools": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        }
    })
}

fn hint_value(hint: &DiscoveryHint) -> Value {
    json!({
        "message": hint.message,
        "suggested_tools": hint.suggested_tools,
    })
}
