//! Declared-type-aware source-function argument binding.

use coral_spec::ManifestDataType;
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use serde_json::Value;

use crate::backends::shared::scalar::timestamp_to_rfc3339;

/// Returns whether an expression is a SQL NULL literal, including through a
/// cast inserted by parameter binding.
pub(crate) fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => is_null_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => is_null_literal(cast.expr.as_ref()),
        _ => false,
    }
}

/// Converts one bound literal using the source function's declared type.
///
/// JSON parameters travel through `DataFusion` as serialized UTF-8 so they can
/// participate in ordinary parameter binding. This is the boundary that
/// restores their JSON type before an HTTP body or MCP tool argument is built.
pub(crate) fn literal_to_declared_json_value(
    expr: &Expr,
    data_type: ManifestDataType,
) -> Option<Value> {
    match expr {
        Expr::Literal(value, _) => scalar_to_declared_json_value(value, data_type),
        Expr::Cast(cast) => literal_to_declared_json_value(cast.expr.as_ref(), data_type),
        Expr::TryCast(cast) => literal_to_declared_json_value(cast.expr.as_ref(), data_type),
        _ => None,
    }
}

fn scalar_to_declared_json_value(
    value: &ScalarValue,
    data_type: ManifestDataType,
) -> Option<Value> {
    match data_type {
        ManifestDataType::Utf8 => string_scalar(value).map(Value::String),
        ManifestDataType::Json => {
            let serialized = string_scalar(value)?;
            serde_json::from_str(&serialized).ok()
        }
        ManifestDataType::Int64 => signed_integer_scalar(value).map(Value::from),
        ManifestDataType::Float64 => float_scalar(value)
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        ManifestDataType::Boolean => match value {
            ScalarValue::Boolean(Some(value)) => Some(Value::Bool(*value)),
            _ => None,
        },
        ManifestDataType::Timestamp => timestamp_to_rfc3339(value).map(Value::String),
    }
}

fn string_scalar(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn signed_integer_scalar(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        _ => None,
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "matches DataFusion's existing numeric coercion from Int64 to Float64"
)]
fn float_scalar(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Int8(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_json_restores_objects_and_keeps_utf8_strings_distinct() {
        let expr = Expr::Literal(
            ScalarValue::Utf8(Some(r#"{"sort":"recent"}"#.to_string())),
            None,
        );

        assert_eq!(
            literal_to_declared_json_value(&expr, ManifestDataType::Json),
            Some(serde_json::json!({"sort": "recent"}))
        );
        assert_eq!(
            literal_to_declared_json_value(&expr, ManifestDataType::Utf8),
            Some(Value::String(r#"{"sort":"recent"}"#.to_string()))
        );
    }

    #[test]
    fn declared_json_preserves_null_as_json_data() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("null".to_string())), None);

        assert_eq!(
            literal_to_declared_json_value(&expr, ManifestDataType::Json),
            Some(Value::Null)
        );
        assert!(!is_null_literal(&expr));
    }
}
