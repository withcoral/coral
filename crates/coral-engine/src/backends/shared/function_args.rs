//! Shared source-function argument formatting.

use serde_json::Value;

/// String view used only by legacy `from_arg` result expressions and enum
/// validation. Request construction keeps the original [`Value`].
pub(crate) fn function_argument_display_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}
