//! Backend-agnostic template and value-source rendering.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::LazyLock;

use datafusion::error::{DataFusionError, Result};
use serde::Serialize;
use serde_json::{Value, json};

use coral_spec::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken, ValueSourceSpec};

/// Shared empty filter/state map for source-scoped rendering.
pub(crate) static EMPTY_MAP: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);

/// Runtime values available while rendering one backend request.
#[derive(Clone, Copy)]
pub(crate) struct RenderContext<'a> {
    pub(crate) filters: &'a HashMap<String, String>,
    pub(crate) args: &'a HashMap<String, String>,
    pub(crate) state: &'a HashMap<String, String>,
    pub(crate) resolved_inputs: &'a BTreeMap<String, String>,
}

impl<'a> RenderContext<'a> {
    pub(crate) fn new(
        filters: &'a HashMap<String, String>,
        args: &'a HashMap<String, String>,
        state: &'a HashMap<String, String>,
        resolved_inputs: &'a BTreeMap<String, String>,
    ) -> Self {
        Self {
            filters,
            args,
            state,
            resolved_inputs,
        }
    }

    pub(crate) fn source_scoped(resolved_inputs: &'a BTreeMap<String, String>) -> Self {
        Self::new(&EMPTY_MAP, &EMPTY_MAP, &EMPTY_MAP, resolved_inputs)
    }
}

#[derive(Clone, Copy)]
enum RuntimeValueNamespace {
    Filter,
    FunctionArgument,
}

impl RuntimeValueNamespace {
    fn values<'a>(self, context: &'a RenderContext<'_>) -> &'a HashMap<String, String> {
        match self {
            Self::Filter => context.filters,
            Self::FunctionArgument => context.args,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::FunctionArgument => "function argument",
        }
    }
}

/// Resolve one declarative value source into an optional JSON value.
pub(crate) fn resolve_value_source(
    value: &ValueSourceSpec,
    context: &RenderContext<'_>,
) -> Result<Option<Value>> {
    use RuntimeValueNamespace::{Filter, FunctionArgument};

    match value {
        ValueSourceSpec::Template { template } => {
            let rendered = render_template(template, context)?;
            Ok(Some(Value::String(rendered)))
        }
        ValueSourceSpec::OneOf { values } => resolve_one_of(values, context),
        ValueSourceSpec::Literal { value } => Ok(Some(value.clone())),
        ValueSourceSpec::Filter { key, default } => {
            Ok(string_runtime_value(context, Filter, key, default.as_ref()))
        }
        ValueSourceSpec::Arg { key, default } => Ok(string_runtime_value(
            context,
            FunctionArgument,
            key,
            default.as_ref(),
        )),
        ValueSourceSpec::FilterInt { key, default } => {
            parse_runtime_value(context, Filter, key, *default, "i64")
        }
        ValueSourceSpec::ArgInt { key, default } => {
            parse_runtime_value(context, FunctionArgument, key, *default, "i64")
        }
        ValueSourceSpec::FilterBool { key, default } => {
            parse_runtime_value(context, Filter, key, *default, "bool")
        }
        ValueSourceSpec::FilterSplit {
            key,
            separator,
            part,
        } => split_value_part(context, Filter, key, separator, *part)
            .map(|value| value.map(Value::String)),
        ValueSourceSpec::FilterSplitInt {
            key,
            separator,
            part,
        } => parse_split_i64_value(context, Filter, key, separator, *part),
        ValueSourceSpec::ArgBool { key, default } => {
            parse_runtime_value(context, FunctionArgument, key, *default, "bool")
        }
        ValueSourceSpec::ArgSplit {
            key,
            separator,
            part,
        } => split_value_part(context, FunctionArgument, key, separator, *part)
            .map(|value| value.map(Value::String)),
        ValueSourceSpec::ArgSplitInt {
            key,
            separator,
            part,
        } => parse_split_i64_value(context, FunctionArgument, key, separator, *part),
        ValueSourceSpec::Input { key } => {
            Ok(context.resolved_inputs.get(key).cloned().map(Value::String))
        }
        ValueSourceSpec::Bearer { key } => Ok(context
            .resolved_inputs
            .get(key)
            .filter(|value| !value.is_empty())
            .map(|value| Value::String(format!("Bearer {value}")))),
        ValueSourceSpec::State { key } => {
            Ok(context.state.get(key).map(|v| Value::String(v.clone())))
        }
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => Ok(Some(now_minus_seconds(*seconds))),
    }
}

fn resolve_one_of(
    values: &[ValueSourceSpec],
    context: &RenderContext<'_>,
) -> Result<Option<Value>> {
    for value in values {
        let Some(resolved) = resolve_value_source(value, context)? else {
            continue;
        };
        if !value_to_string(&resolved).is_empty() {
            return Ok(Some(resolved));
        }
    }
    Ok(None)
}

fn string_runtime_value(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    default: Option<&Value>,
) -> Option<Value> {
    namespace
        .values(context)
        .get(key)
        .map(|value| Value::String(value.clone()))
        .or_else(|| default.cloned())
}

fn now_minus_seconds(seconds: i64) -> Value {
    #[expect(
        clippy::cast_possible_wrap,
        reason = "Current Unix epoch seconds fit within i64 for centuries"
    )]
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    json!(now.saturating_sub(seconds))
}

fn parse_runtime_value<T>(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    default: Option<T>,
    type_label: &str,
) -> Result<Option<Value>>
where
    T: FromStr + Serialize,
    T::Err: Display,
{
    let Some(raw) = namespace.values(context).get(key) else {
        return Ok(default.map(|value| json!(value)));
    };
    let parsed = raw.parse::<T>().map_err(|error| {
        let label = namespace.label();
        DataFusionError::Execution(format!(
            "{label} '{key}' value '{raw}' is not a valid {type_label}: {error}"
        ))
    })?;
    Ok(Some(json!(parsed)))
}

fn parse_split_i64_value(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    separator: &str,
    part: usize,
) -> Result<Option<Value>> {
    let Some(raw) = split_value_part(context, namespace, key, separator, part)? else {
        return Ok(None);
    };
    let parsed = raw.parse::<i64>().map_err(|error| {
        let label = namespace.label();
        DataFusionError::Execution(format!(
            "{label} '{key}' split part {part} value '{raw}' is not a valid i64: {error}"
        ))
    })?;
    Ok(Some(json!(parsed)))
}

fn split_value_part(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    separator: &str,
    part: usize,
) -> Result<Option<String>> {
    let Some(value) = namespace.values(context).get(key) else {
        return Ok(None);
    };
    value
        .split(separator)
        .nth(part)
        .map_or_else(|| {
            let label = namespace.label();
            Err(DataFusionError::Execution(format!(
                "{label} '{key}' value '{value}' does not contain split part {part} using separator '{separator}'"
            )))
        }, |value| Ok(Some(value.to_string())))
}

/// Render a parsed template into a concrete string.
pub(crate) fn render_template(
    template: &ParsedTemplate,
    context: &RenderContext<'_>,
) -> Result<String> {
    let mut out = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => out.push_str(part),
            TemplatePart::Token(token) => {
                out.push_str(&resolve_template_token(token, context)?);
            }
        }
    }
    Ok(out)
}

fn resolve_template_token(token: &TemplateToken, context: &RenderContext<'_>) -> Result<String> {
    let value = match token.namespace() {
        TemplateNamespace::Input => context.resolved_inputs.get(token.key()),
        TemplateNamespace::Filter => context.filters.get(token.key()),
        TemplateNamespace::Arg => context.args.get(token.key()),
        TemplateNamespace::State => context.state.get(token.key()),
        TemplateNamespace::Expr | TemplateNamespace::Other(_) => {
            return Err(DataFusionError::Execution(format!(
                "unsupported template token '{}'",
                token.raw()
            )));
        }
    };
    value
        .cloned()
        .or_else(|| token.default_value().map(ToString::to_string))
        .ok_or_else(|| missing_template_token_error(token))
}

fn missing_template_token_error(token: &TemplateToken) -> DataFusionError {
    let message = match token.namespace() {
        TemplateNamespace::Input => {
            format!("missing source input '{}' for template token", token.key())
        }
        TemplateNamespace::Filter => format!("missing filter '{}'", token.key()),
        TemplateNamespace::Arg => format!("missing request argument '{}'", token.key()),
        TemplateNamespace::State => format!("missing state value '{}'", token.key()),
        TemplateNamespace::Expr | TemplateNamespace::Other(_) => {
            format!("unsupported template token '{}'", token.raw())
        }
    };
    DataFusionError::Execution(message)
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
        ValueSourceSpec::OneOf { values } => {
            let mut last_error = None;
            for value in values {
                match validate_value_source_inputs(value, resolved_inputs) {
                    Ok(()) => return Ok(()),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(last_error.unwrap_or_else(|| {
                DataFusionError::Execution("`from: one_of` value source has no values".to_string())
            }))
        }
        ValueSourceSpec::Input { key } => validate_required_input(resolved_inputs, key, "input"),
        ValueSourceSpec::Bearer { key } => validate_required_input(resolved_inputs, key, "bearer"),
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Filter { .. }
        | ValueSourceSpec::FilterInt { .. }
        | ValueSourceSpec::FilterBool { .. }
        | ValueSourceSpec::FilterSplit { .. }
        | ValueSourceSpec::FilterSplitInt { .. }
        | ValueSourceSpec::Arg { .. }
        | ValueSourceSpec::ArgInt { .. }
        | ValueSourceSpec::ArgBool { .. }
        | ValueSourceSpec::ArgSplit { .. }
        | ValueSourceSpec::ArgSplitInt { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => Ok(()),
    }
}

fn validate_required_input(
    resolved_inputs: &BTreeMap<String, String>,
    key: &str,
    source: &str,
) -> Result<()> {
    if resolved_inputs.contains_key(key) {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "missing source input '{key}' for `from: {source}` value source"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use coral_spec::ValueSourceSpec;
    use serde_json::json;

    use super::{EMPTY_MAP, RenderContext, resolve_value_source, validate_value_source_inputs};

    fn inputs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn runtime_values(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn test_render_context<'a>(
        filters: &'a HashMap<String, String>,
        args: &'a HashMap<String, String>,
        resolved_inputs: &'a BTreeMap<String, String>,
    ) -> RenderContext<'a> {
        RenderContext::new(filters, args, &EMPTY_MAP, resolved_inputs)
    }

    fn resolve(
        value: &ValueSourceSpec,
        filter_pairs: &[(&str, &str)],
        arg_pairs: &[(&str, &str)],
        input_pairs: &[(&str, &str)],
    ) -> datafusion::error::Result<Option<serde_json::Value>> {
        let filters = runtime_values(filter_pairs);
        let args = runtime_values(arg_pairs);
        let resolved_inputs = inputs(input_pairs);
        resolve_value_source(
            value,
            &test_render_context(&filters, &args, &resolved_inputs),
        )
    }

    fn resolve_source_scoped(
        value: &ValueSourceSpec,
        input_pairs: &[(&str, &str)],
    ) -> datafusion::error::Result<Option<serde_json::Value>> {
        let resolved_inputs = inputs(input_pairs);
        resolve_value_source(value, &RenderContext::source_scoped(&resolved_inputs))
    }

    fn input(key: &str) -> ValueSourceSpec {
        ValueSourceSpec::Input {
            key: key.to_string(),
        }
    }

    fn bearer(key: &str) -> ValueSourceSpec {
        ValueSourceSpec::Bearer {
            key: key.to_string(),
        }
    }

    fn filter_int(key: &str) -> ValueSourceSpec {
        ValueSourceSpec::FilterInt {
            key: key.to_string(),
            default: None,
        }
    }

    fn filter_bool(key: &str) -> ValueSourceSpec {
        ValueSourceSpec::FilterBool {
            key: key.to_string(),
            default: None,
        }
    }

    fn filter_split(part: usize) -> ValueSourceSpec {
        ValueSourceSpec::FilterSplit {
            key: "issue_identifier".to_string(),
            separator: "-".to_string(),
            part,
        }
    }

    fn filter_split_int(part: usize) -> ValueSourceSpec {
        ValueSourceSpec::FilterSplitInt {
            key: "issue_identifier".to_string(),
            separator: "-".to_string(),
            part,
        }
    }

    fn arg_split(part: usize) -> ValueSourceSpec {
        ValueSourceSpec::ArgSplit {
            key: "issue".to_string(),
            separator: "-".to_string(),
            part,
        }
    }

    fn arg_split_int(part: usize) -> ValueSourceSpec {
        ValueSourceSpec::ArgSplitInt {
            key: "issue".to_string(),
            separator: "-".to_string(),
            part,
        }
    }

    fn one_of(values: Vec<ValueSourceSpec>) -> ValueSourceSpec {
        ValueSourceSpec::OneOf { values }
    }

    fn assert_error_contains(error: &datafusion::error::DataFusionError, expected: &str) {
        let error = error.to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    #[test]
    fn resolve_value_source_uses_provider_scoped_credentials() {
        let value = resolve(&input("API_KEY"), &[], &[], &[("API_KEY", "alpha-secret")])
            .expect("input lookup should succeed");

        assert_eq!(value, Some(json!("alpha-secret")));
    }

    #[test]
    fn resolve_value_source_uses_declared_store_without_fallback() {
        let value = resolve(&input("API_KEY"), &[], &[], &[]).expect("input lookup should succeed");

        assert_eq!(value, None);
    }

    #[test]
    fn resolve_value_source_parses_filter_ints_as_numbers() {
        let value = resolve(
            &filter_int("start_time"),
            &[("start_time", "1700000000000000")],
            &[],
            &[],
        )
        .expect("integer filter should resolve");

        assert_eq!(value, Some(json!(1_700_000_000_000_000_i64)));
    }

    #[test]
    fn resolve_value_source_rejects_invalid_filter_ints() {
        let error = resolve(
            &filter_int("start_time"),
            &[("start_time", "not-a-number")],
            &[],
            &[],
        )
        .expect_err("invalid integer filter should fail");

        assert_error_contains(
            &error,
            "filter 'start_time' value 'not-a-number' is not a valid i64",
        );
    }

    #[test]
    fn resolve_value_source_splits_runtime_parts() {
        for (case, value, filters, args, expected) in [
            (
                "filter string",
                filter_split(0),
                &[("issue_identifier", "SOURCE-496")][..],
                &[][..],
                json!("SOURCE"),
            ),
            (
                "filter integer",
                filter_split_int(1),
                &[("issue_identifier", "SOURCE-496")][..],
                &[][..],
                json!(496),
            ),
            (
                "argument string",
                arg_split(0),
                &[][..],
                &[("issue", "SOURCE-496")][..],
                json!("SOURCE"),
            ),
            (
                "argument integer",
                arg_split_int(1),
                &[][..],
                &[("issue", "SOURCE-496")][..],
                json!(496),
            ),
        ] {
            let actual = resolve(&value, filters, args, &[])
                .unwrap_or_else(|error| panic!("{case} should resolve: {error}"));
            assert_eq!(actual, Some(expected), "{case}");
        }
    }

    #[test]
    fn resolve_value_source_rejects_invalid_runtime_split_parts() {
        for (case, value, filters, args, expected) in [
            (
                "missing filter string part",
                filter_split(1),
                &[("issue_identifier", "SOURCE496")][..],
                &[][..],
                "filter 'issue_identifier' value 'SOURCE496' does not contain split part 1",
            ),
            (
                "missing argument string part",
                arg_split(1),
                &[][..],
                &[("issue", "SOURCE496")][..],
                "function argument 'issue' value 'SOURCE496' does not contain split part 1",
            ),
            (
                "missing filter integer part",
                filter_split_int(1),
                &[("issue_identifier", "SOURCE496")][..],
                &[][..],
                "filter 'issue_identifier' value 'SOURCE496' does not contain split part 1",
            ),
            (
                "invalid argument integer part",
                arg_split_int(1),
                &[][..],
                &[("issue", "SOURCE-abc")][..],
                "function argument 'issue' split part 1 value 'abc' is not a valid i64",
            ),
        ] {
            let error = match resolve(&value, filters, args, &[]) {
                Ok(value) => panic!("{case} should fail, got {value:?}"),
                Err(error) => error,
            };
            assert_error_contains(&error, expected);
        }
    }

    #[test]
    fn resolve_value_source_parses_filter_bools_as_bools() {
        let value = resolve(
            &filter_bool("descending"),
            &[("descending", "false")],
            &[],
            &[],
        )
        .expect("bool filter should resolve");

        assert_eq!(value, Some(json!(false)));
    }

    #[test]
    fn one_of_prefers_first_present_value() {
        let value = resolve_source_scoped(
            &one_of(vec![input("API_KEY"), bearer("OAUTH_TOKEN")]),
            &[("API_KEY", "lin_api_key"), ("OAUTH_TOKEN", "oauth")],
        )
        .expect("one_of should resolve");

        assert_eq!(value, Some(json!("lin_api_key")));
    }

    #[test]
    fn one_of_uses_bearer_fallback_value() {
        let value = resolve_source_scoped(
            &one_of(vec![input("API_KEY"), bearer("OAUTH_TOKEN")]),
            &[("OAUTH_TOKEN", "oauth")],
        )
        .expect("one_of should resolve");

        assert_eq!(value, Some(json!("Bearer oauth")));
    }

    #[test]
    fn one_of_ignores_empty_bearer_values() {
        let value =
            resolve_source_scoped(&one_of(vec![bearer("OAUTH_TOKEN")]), &[("OAUTH_TOKEN", "")])
                .expect("one_of should resolve");

        assert_eq!(value, None);
    }

    #[test]
    fn one_of_input_dependency_validation_accepts_any_resolved_branch() {
        let value = one_of(vec![input("API_KEY"), bearer("OAUTH_TOKEN")]);

        validate_value_source_inputs(&value, &inputs(&[("API_KEY", "lin_api_key")]))
            .expect("api key should satisfy one_of");
        validate_value_source_inputs(&value, &inputs(&[("OAUTH_TOKEN", "oauth_access")]))
            .expect("oauth token should satisfy one_of");
        assert_error_contains(
            &validate_value_source_inputs(&value, &inputs(&[]))
                .expect_err("missing both should fail"),
            "missing source input 'OAUTH_TOKEN' for `from: bearer` value source",
        );
    }
}
