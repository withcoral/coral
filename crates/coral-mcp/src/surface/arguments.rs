use rmcp::ErrorData;
use serde_json::{Map, Value};

pub(crate) fn required_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<String, ErrorData> {
    let value = arguments
        .and_then(|arguments| arguments.get(key))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("missing string argument '{key}'"), None)
        })?;
    Ok(value.to_string())
}

pub(crate) fn optional_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    Ok(Some(value.to_string()))
}

pub(crate) fn optional_non_empty_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    if value.trim().is_empty() {
        Err(ErrorData::invalid_params(
            format!("argument '{key}' must not be empty"),
            None,
        ))
    } else {
        Ok(Some(value.to_string()))
    }
}

pub(crate) fn optional_bool_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: bool,
) -> Result<bool, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    value.as_bool().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a boolean"), None)
    })
}

pub(crate) fn optional_u32_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    let value = value.as_i64().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be an integer"), None)
    })?;
    if value < i64::from(min) || value > i64::from(max) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        ));
    }
    Ok(value as u32)
}

#[cfg(test)]
mod tests {
    use super::{
        optional_non_empty_string_argument, optional_string_argument, optional_u32_argument,
        required_string_argument,
    };
    use serde_json::{Map, Value};

    fn string_arguments(key: &str, value: &str) -> Map<String, Value> {
        Map::from_iter([(key.to_string(), Value::String(value.to_string()))])
    }

    #[test]
    fn required_string_argument_preserves_surrounding_whitespace() {
        let arguments = string_arguments("pattern", " ^messages$ ");

        let parsed =
            required_string_argument(Some(&arguments), "pattern").expect("argument should parse");

        assert_eq!(parsed, " ^messages$ ");
    }

    #[test]
    fn required_string_argument_rejects_whitespace_only_values() {
        let arguments = string_arguments("schema", "   ");

        required_string_argument(Some(&arguments), "schema")
            .expect_err("whitespace-only required argument should fail");
    }

    #[test]
    fn optional_string_argument_preserves_exact_value() {
        let arguments = string_arguments("schema", " local_messages ");

        let parsed =
            optional_string_argument(Some(&arguments), "schema").expect("argument should parse");

        assert_eq!(parsed.as_deref(), Some(" local_messages "));
    }

    #[test]
    fn optional_non_empty_string_argument_preserves_surrounding_whitespace() {
        let arguments = string_arguments("pattern", " ^id$ ");

        let parsed = optional_non_empty_string_argument(Some(&arguments), "pattern")
            .expect("argument should parse");

        assert_eq!(parsed.as_deref(), Some(" ^id$ "));
    }

    #[test]
    fn optional_non_empty_string_argument_rejects_whitespace_only_values() {
        let arguments = string_arguments("pattern", "   ");

        optional_non_empty_string_argument(Some(&arguments), "pattern")
            .expect_err("whitespace-only non-empty argument should fail");
    }

    #[test]
    fn optional_u32_argument_enforces_bounds() {
        let arguments = Map::from_iter([("limit".to_string(), Value::from(51))]);

        optional_u32_argument(Some(&arguments), "limit", 10, 1, 50)
            .expect_err("out-of-range integer should fail");
    }
}
