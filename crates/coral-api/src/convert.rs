//! Hand-written conversions from generated `coral.v1` protobuf messages into
//! foreign types such as [`serde_json::Value`].
//!
//! The protobuf package defines two byte-identical JSON value messages,
//! [`JsonValue`](crate::v1::JsonValue) (resources) and
//! [`CodeModeJsonValue`](crate::v1::CodeModeJsonValue) (Code Mode events).
//! Both walk into the same [`serde_json::Value`] shape; the conversions live
//! here so every transport surface shares one canonical implementation.

use serde_json::Value;

use crate::v1::{
    CodeModeJsonValue, JsonValue, code_mode_json_value, json_value as proto_json_value,
};

impl From<JsonValue> for Value {
    fn from(value: JsonValue) -> Self {
        match value.kind {
            Some(proto_json_value::Kind::NullValue(_)) | None => Value::Null,
            Some(proto_json_value::Kind::BoolValue(value)) => Value::Bool(value),
            Some(proto_json_value::Kind::IntegerValue(value)) => Value::from(value),
            Some(proto_json_value::Kind::UnsignedIntegerValue(value)) => Value::from(value),
            Some(proto_json_value::Kind::DoubleValue(value)) => Value::from(value),
            Some(proto_json_value::Kind::StringValue(value)) => Value::String(value),
            Some(proto_json_value::Kind::ObjectValue(object)) => Value::Object(
                object
                    .fields
                    .into_iter()
                    .map(|(key, value)| (key, Value::from(value)))
                    .collect(),
            ),
            Some(proto_json_value::Kind::ArrayValue(array)) => {
                Value::Array(array.values.into_iter().map(Value::from).collect())
            }
        }
    }
}

impl From<CodeModeJsonValue> for Value {
    fn from(value: CodeModeJsonValue) -> Self {
        match value.kind {
            Some(code_mode_json_value::Kind::NullValue(_)) | None => Value::Null,
            Some(code_mode_json_value::Kind::BoolValue(value)) => Value::Bool(value),
            Some(code_mode_json_value::Kind::IntegerValue(value)) => Value::from(value),
            Some(code_mode_json_value::Kind::UnsignedIntegerValue(value)) => Value::from(value),
            Some(code_mode_json_value::Kind::DoubleValue(value)) => Value::from(value),
            Some(code_mode_json_value::Kind::StringValue(value)) => Value::String(value),
            Some(code_mode_json_value::Kind::ObjectValue(object)) => Value::Object(
                object
                    .fields
                    .into_iter()
                    .map(|(key, value)| (key, Value::from(value)))
                    .collect(),
            ),
            Some(code_mode_json_value::Kind::ArrayValue(array)) => {
                Value::Array(array.values.into_iter().map(Value::from).collect())
            }
        }
    }
}
