//! Backend-agnostic template and value-source rendering.
//!
//! Source manifests use `ParsedTemplate` strings and `ValueSourceSpec` values
//! to describe how request parts (headers, query params, bodies, URLs, auth
//! tokens) pull their runtime data. The helpers here turn those declarative
//! shapes into concrete strings / JSON values given a set of filters,
//! pagination state, and resolved input values. They have no HTTP-specific
//! behavior and can be reused across any backend that wants the same
//! interpolation semantics.

use std::collections::{BTreeMap, HashMap};

use datafusion::error::{DataFusionError, Result};
use serde_json::{Value, json};

use coral_spec::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken, ValueSourceSpec};

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
        ValueSourceSpec::Input { key } => Ok(resolved_inputs.get(key).cloned().map(Value::String)),
        ValueSourceSpec::State { key } => Ok(state.get(key).map(|v| Value::String(v.clone()))),
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => {
            #[allow(
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
