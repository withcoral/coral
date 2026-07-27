//! Shared source-function argument formatting.

use std::collections::HashMap;

use coral_spec::ManifestDataType;
use serde_json::Value;

/// Typed and textual views of one HTTP function call's bound arguments.
///
/// Structured request bodies consume `values`; textual request surfaces
/// consume `text_values`.
#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionArgumentValues {
    values: HashMap<String, Value>,
    text_values: HashMap<String, String>,
}

impl FunctionArgumentValues {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            values: HashMap::with_capacity(capacity),
            text_values: HashMap::with_capacity(capacity),
        }
    }

    pub(crate) fn insert(&mut self, name: String, value: Value, data_type: ManifestDataType) {
        self.text_values.insert(
            name.clone(),
            function_argument_transport_text(&value, data_type),
        );
        self.values.insert(name, value);
    }

    pub(crate) fn values(&self) -> &HashMap<String, Value> {
        &self.values
    }

    pub(crate) fn text_values(&self) -> &HashMap<String, String> {
        &self.text_values
    }
}

/// String view used only by legacy `from_arg` result expressions and enum
/// validation. Request construction keeps the original [`Value`].
pub(crate) fn function_argument_display_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

/// Text representation used when an HTTP argument is rendered into a
/// textual request surface such as a query parameter, path, or header.
///
/// JSON strings retain their serialized quotes while structured request
/// bodies continue to consume the original [`Value`].
fn function_argument_transport_text(value: &Value, data_type: ManifestDataType) -> String {
    if data_type == ManifestDataType::Json {
        value.to_string()
    } else {
        function_argument_display_value(value)
    }
}
