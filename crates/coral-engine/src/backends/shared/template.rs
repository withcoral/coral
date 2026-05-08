//! Backend-agnostic template and value-source rendering.

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use datafusion::error::{DataFusionError, Result};
use serde_json::{Value, json};

use coral_spec::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken, ValueSourceSpec};

/// Shared empty filter/state map for source-scoped rendering.
pub(crate) static EMPTY_MAP: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);

/// Resolve one declarative value source into an optional JSON value.
pub(crate) fn resolve_value_source(
    value: &ValueSourceSpec,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<Option<Value>> {
    match value {
        ValueSourceSpec::Template { template } => {
            let rendered = render_template(template, filters, state, resolved_inputs)?;
            Ok(Some(Value::String(rendered)))
        }
        ValueSourceSpec::Literal { value } => Ok(Some(value.clone())),
        ValueSourceSpec::Filter { key, default } => Ok(filters
            .get(key)
            .map(|v| Value::String(v.clone()))
            .or_else(|| default.clone())),
        ValueSourceSpec::FilterInt { key, default } => {
            let value = if let Some(filter) = filters.get(key) {
                let parsed = filter.parse::<i64>().map_err(|error| {
                    DataFusionError::Execution(format!(
                        "filter '{key}' value '{filter}' is not a valid i64: {error}"
                    ))
                })?;
                Some(json!(parsed))
            } else {
                default.map(|value| json!(value))
            };
            Ok(value)
        }
        ValueSourceSpec::FilterBool { key, default } => {
            let value = if let Some(filter) = filters.get(key) {
                let parsed = filter.parse::<bool>().map_err(|error| {
                    DataFusionError::Execution(format!(
                        "filter '{key}' value '{filter}' is not a valid bool: {error}"
                    ))
                })?;
                Some(json!(parsed))
            } else {
                default.map(|value| json!(value))
            };
            Ok(value)
        }
        ValueSourceSpec::FilterSplit {
            key,
            separator,
            part,
        } => {
            split_filter_part(filters, key, separator, *part).map(|value| value.map(Value::String))
        }
        ValueSourceSpec::FilterSplitInt {
            key,
            separator,
            part,
        } => {
            let Some(raw) = split_filter_part(filters, key, separator, *part)? else {
                return Ok(None);
            };
            let parsed = raw.parse::<i64>().map_err(|error| {
                DataFusionError::Execution(format!(
                    "filter '{key}' split part {part} value '{raw}' is not a valid i64: {error}"
                ))
            })?;
            Ok(Some(json!(parsed)))
        }
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. } => Err(DataFusionError::Execution(format!(
            "function argument '{key}' cannot be resolved outside a function request"
        ))),
        ValueSourceSpec::Input { key } => Ok(resolved_inputs.get(key).cloned().map(Value::String)),
        ValueSourceSpec::State { key } => Ok(state.get(key).map(|v| Value::String(v.clone()))),
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "Current Unix epoch seconds fit within i64 for centuries"
            )]
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let value = now.saturating_sub(*seconds);
            Ok(Some(json!(value)))
        }
    }
}

fn split_filter_part(
    filters: &HashMap<String, String>,
    key: &str,
    separator: &str,
    part: usize,
) -> Result<Option<String>> {
    let Some(filter) = filters.get(key) else {
        return Ok(None);
    };
    filter
        .split(separator)
        .nth(part)
        .map_or_else(|| {
            Err(DataFusionError::Execution(format!(
                "filter '{key}' value '{filter}' does not contain split part {part} using separator '{separator}'"
            )))
        }, |value| Ok(Some(value.to_string())))
}

/// Render a parsed template into a concrete string.
pub(crate) fn render_template(
    template: &ParsedTemplate,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<String> {
    let mut out = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => out.push_str(part),
            TemplatePart::Token(token) => {
                out.push_str(&resolve_template_token(
                    token,
                    filters,
                    state,
                    resolved_inputs,
                )?);
            }
        }
    }
    Ok(out)
}

fn resolve_template_token(
    token: &TemplateToken,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<String> {
    let default = token.default_value().map(ToString::to_string);

    if token.namespace() == &TemplateNamespace::Input {
        return resolved_inputs
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "missing source input '{}' for template token",
                    token.key()
                ))
            });
    }

    if token.namespace() == &TemplateNamespace::Filter {
        return filters
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("missing filter '{}'", token.key()))
            });
    }

    if token.namespace() == &TemplateNamespace::State {
        return state.get(token.key()).cloned().or(default).ok_or_else(|| {
            DataFusionError::Execution(format!("missing state value '{}'", token.key()))
        });
    }

    Err(DataFusionError::Execution(format!(
        "unsupported template token '{}'",
        token.raw()
    )))
}

/// Flatten a JSON value into a plain string suitable for header/query use.
pub(crate) fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Validate only the input-token dependencies for a template.
pub(crate) fn validate_input_dependencies(
    template: &ParsedTemplate,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    for part in template.parts() {
        if let TemplatePart::Token(token) = part
            && token.namespace() == &TemplateNamespace::Input
            && token.default_value().is_none()
            && !resolved_inputs.contains_key(token.key())
        {
            return Err(DataFusionError::Execution(format!(
                "missing source input '{}' for template token",
                token.key()
            )));
        }
    }
    Ok(())
}

/// Validate only the input-token dependencies for a value source.
pub(crate) fn validate_value_source_inputs(
    value: &ValueSourceSpec,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    match value {
        ValueSourceSpec::Template { template } => {
            validate_input_dependencies(template, resolved_inputs)
        }
        ValueSourceSpec::Input { key } => {
            if resolved_inputs.contains_key(key) {
                Ok(())
            } else {
                Err(DataFusionError::Execution(format!(
                    "missing source input '{key}' for `from: input` value source"
                )))
            }
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Filter { .. }
        | ValueSourceSpec::FilterInt { .. }
        | ValueSourceSpec::FilterBool { .. }
        | ValueSourceSpec::FilterSplit { .. }
        | ValueSourceSpec::FilterSplitInt { .. }
        | ValueSourceSpec::Arg { .. }
        | ValueSourceSpec::ArgInt { .. }
        | ValueSourceSpec::ArgBool { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => Ok(()),
    }
}
