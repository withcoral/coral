//! Backend-agnostic template and value-source rendering.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::LazyLock;

use datafusion::error::{DataFusionError, Result};
use serde_json::{Value, json};

use coral_spec::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken, ValueSourceSpec};

/// Shared empty filter/state map for source-scoped rendering.
pub(crate) static EMPTY_MAP: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
static EMPTY_SECRET_INPUT_NAMES: LazyLock<BTreeSet<String>> = LazyLock::new(BTreeSet::new);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved<T> {
    pub(crate) value: T,
    pub(crate) depends_on_secret: bool,
}

impl<T> Resolved<T> {
    fn public(value: T) -> Self {
        Self {
            value,
            depends_on_secret: false,
        }
    }

    fn map<U>(self, map: impl FnOnce(T) -> U) -> Resolved<U> {
        Resolved {
            value: map(self.value),
            depends_on_secret: self.depends_on_secret,
        }
    }
}

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
    Ok(
        resolve_value_source_with_secret_provenance(value, context, &EMPTY_SECRET_INPUT_NAMES)?
            .map(|resolved| resolved.value),
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "One exhaustive pass must keep value resolution and secret provenance identical"
)]
pub(crate) fn resolve_value_source_with_secret_provenance(
    value: &ValueSourceSpec,
    context: &RenderContext<'_>,
    secret_input_names: &BTreeSet<String>,
) -> Result<Option<Resolved<Value>>> {
    match value {
        ValueSourceSpec::Template { template } => {
            let rendered =
                render_template_with_secret_provenance(template, context, secret_input_names)?;
            Ok(Some(rendered.map(Value::String)))
        }
        ValueSourceSpec::OneOf { values } => resolve_one_of(values, context, secret_input_names),
        ValueSourceSpec::Literal { value } => Ok(Some(Resolved::public(value.clone()))),
        ValueSourceSpec::Filter { key, default } => Ok(string_runtime_value(
            context,
            RuntimeValueNamespace::Filter,
            key,
            default.as_ref(),
        )
        .map(Resolved::public)),
        ValueSourceSpec::Arg { key, default } => Ok(string_runtime_value(
            context,
            RuntimeValueNamespace::FunctionArgument,
            key,
            default.as_ref(),
        )
        .map(Resolved::public)),
        ValueSourceSpec::FilterInt { key, default } => {
            parse_i64_value(context, RuntimeValueNamespace::Filter, key, *default)
                .map(|value| value.map(Resolved::public))
        }
        ValueSourceSpec::ArgInt { key, default } => parse_i64_value(
            context,
            RuntimeValueNamespace::FunctionArgument,
            key,
            *default,
        )
        .map(|value| value.map(Resolved::public)),
        ValueSourceSpec::FilterBool { key, default } => {
            parse_bool_value(context, RuntimeValueNamespace::Filter, key, *default)
                .map(|value| value.map(Resolved::public))
        }
        ValueSourceSpec::FilterStringArray { key, default } => {
            parse_filter_strings(context, key, default.as_deref())
                .map(|value| value.map(Resolved::public))
        }
        ValueSourceSpec::FilterSplit {
            key,
            separator,
            part,
        } => split_value_part(
            context,
            RuntimeValueNamespace::Filter,
            key,
            separator,
            *part,
        )
        .map(|value| value.map(|value| Resolved::public(Value::String(value)))),
        ValueSourceSpec::FilterSplitInt {
            key,
            separator,
            part,
        } => parse_split_i64_value(
            context,
            RuntimeValueNamespace::Filter,
            key,
            separator,
            *part,
        )
        .map(|value| value.map(Resolved::public)),
        ValueSourceSpec::ArgBool { key, default } => parse_bool_value(
            context,
            RuntimeValueNamespace::FunctionArgument,
            key,
            *default,
        )
        .map(|value| value.map(Resolved::public)),
        ValueSourceSpec::ArgSplit {
            key,
            separator,
            part,
        } => split_value_part(
            context,
            RuntimeValueNamespace::FunctionArgument,
            key,
            separator,
            *part,
        )
        .map(|value| value.map(|value| Resolved::public(Value::String(value)))),
        ValueSourceSpec::ArgSplitInt {
            key,
            separator,
            part,
        } => parse_split_i64_value(
            context,
            RuntimeValueNamespace::FunctionArgument,
            key,
            separator,
            *part,
        )
        .map(|value| value.map(Resolved::public)),
        ValueSourceSpec::Input { key } => {
            Ok(context
                .resolved_inputs
                .get(key)
                .cloned()
                .map(|value| Resolved {
                    value: Value::String(value),
                    depends_on_secret: secret_input_names.contains(key),
                }))
        }
        ValueSourceSpec::Bearer { key } => Ok(context
            .resolved_inputs
            .get(key)
            .filter(|value| !value.is_empty())
            .map(|value| Resolved {
                value: Value::String(format!("Bearer {value}")),
                depends_on_secret: true,
            })),
        ValueSourceSpec::State { key } => Ok(context
            .state
            .get(key)
            .map(|value| Resolved::public(Value::String(value.clone())))),
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => {
            Ok(Some(Resolved::public(now_minus_seconds(*seconds))))
        }
    }
}

fn resolve_one_of(
    values: &[ValueSourceSpec],
    context: &RenderContext<'_>,
    secret_input_names: &BTreeSet<String>,
) -> Result<Option<Resolved<Value>>> {
    for value in values {
        let Some(resolved) =
            resolve_value_source_with_secret_provenance(value, context, secret_input_names)?
        else {
            continue;
        };
        if !value_to_string(&resolved.value).is_empty() {
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

fn parse_i64_value(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    default: Option<i64>,
) -> Result<Option<Value>> {
    let Some(raw) = namespace.values(context).get(key) else {
        return Ok(default.map(|value| json!(value)));
    };
    let parsed = raw.parse::<i64>().map_err(|error| {
        let label = namespace.label();
        DataFusionError::Execution(format!(
            "{label} '{key}' value '{raw}' is not a valid i64: {error}"
        ))
    })?;
    Ok(Some(json!(parsed)))
}

fn parse_bool_value(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    default: Option<bool>,
) -> Result<Option<Value>> {
    let Some(raw) = namespace.values(context).get(key) else {
        return Ok(default.map(|value| json!(value)));
    };
    let parsed = raw.parse::<bool>().map_err(|error| {
        let label = namespace.label();
        DataFusionError::Execution(format!(
            "{label} '{key}' value '{raw}' is not a valid bool: {error}"
        ))
    })?;
    Ok(Some(json!(parsed)))
}

fn parse_string_array_value(
    context: &RenderContext<'_>,
    namespace: RuntimeValueNamespace,
    key: &str,
    default: Option<&[String]>,
) -> Result<Option<Value>> {
    let Some(raw) = namespace.values(context).get(key) else {
        return Ok(default.map(|values| json!(values)));
    };
    let parsed = serde_json::from_str::<Vec<String>>(raw).map_err(|error| {
        let label = namespace.label();
        DataFusionError::Execution(format!(
            "{label} '{key}' value '{raw}' is not a valid JSON array of strings: {error}"
        ))
    })?;
    Ok(Some(json!(parsed)))
}

fn parse_filter_strings(
    context: &RenderContext<'_>,
    key: &str,
    default: Option<&[String]>,
) -> Result<Option<Value>> {
    parse_string_array_value(context, RuntimeValueNamespace::Filter, key, default)
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
    Ok(render_template_with_secret_provenance(template, context, &EMPTY_SECRET_INPUT_NAMES)?.value)
}

pub(crate) fn render_template_with_secret_provenance(
    template: &ParsedTemplate,
    context: &RenderContext<'_>,
    secret_input_names: &BTreeSet<String>,
) -> Result<Resolved<String>> {
    let mut out = String::with_capacity(template.raw().len());
    let mut depends_on_secret = false;
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => out.push_str(part),
            TemplatePart::Token(token) => {
                out.push_str(&resolve_template_token(token, context)?);
                depends_on_secret |= token.namespace() == &TemplateNamespace::Input
                    && context.resolved_inputs.contains_key(token.key())
                    && secret_input_names.contains(token.key());
            }
        }
    }
    Ok(Resolved {
        value: out,
        depends_on_secret,
    })
}

fn resolve_template_token(token: &TemplateToken, context: &RenderContext<'_>) -> Result<String> {
    let default = token.default_value().map(ToString::to_string);

    if token.namespace() == &TemplateNamespace::Input {
        return context
            .resolved_inputs
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
        return context
            .filters
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("missing filter '{}'", token.key()))
            });
    }

    if token.namespace() == &TemplateNamespace::Arg {
        return context
            .args
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("missing request argument '{}'", token.key()))
            });
    }

    if token.namespace() == &TemplateNamespace::State {
        return context
            .state
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
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
        ValueSourceSpec::Input { key } => {
            if resolved_inputs.contains_key(key) {
                Ok(())
            } else {
                Err(DataFusionError::Execution(format!(
                    "missing source input '{key}' for `from: input` value source"
                )))
            }
        }
        ValueSourceSpec::Bearer { key } => {
            if resolved_inputs.contains_key(key) {
                Ok(())
            } else {
                Err(DataFusionError::Execution(format!(
                    "missing source input '{key}' for `from: bearer` value source"
                )))
            }
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Filter { .. }
        | ValueSourceSpec::FilterInt { .. }
        | ValueSourceSpec::FilterBool { .. }
        | ValueSourceSpec::FilterStringArray { .. }
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use coral_spec::{ParsedTemplate, ValueSourceSpec};
    use serde_json::json;

    use super::{
        EMPTY_MAP, RenderContext, render_template_with_secret_provenance, resolve_value_source,
        resolve_value_source_with_secret_provenance, validate_value_source_inputs,
    };

    fn inputs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
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

    #[test]
    fn resolve_value_source_uses_provider_scoped_credentials() {
        let resolved_inputs = BTreeMap::from([("API_KEY".to_string(), "alpha-secret".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::Input {
                key: "API_KEY".to_string(),
            },
            &test_render_context(&HashMap::new(), &HashMap::new(), &resolved_inputs),
        )
        .expect("input lookup should succeed");

        assert_eq!(value, Some(json!("alpha-secret")));
    }

    #[test]
    fn resolve_value_source_uses_declared_store_without_fallback() {
        let resolved_inputs = BTreeMap::new();

        let value = resolve_value_source(
            &ValueSourceSpec::Input {
                key: "API_KEY".to_string(),
            },
            &test_render_context(&HashMap::new(), &HashMap::new(), &resolved_inputs),
        )
        .expect("input lookup should succeed");

        assert_eq!(value, None);
    }

    #[test]
    fn resolve_value_source_parses_filter_ints_as_numbers() {
        let filters = HashMap::from([("start_time".to_string(), "1700000000000000".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::FilterInt {
                key: "start_time".to_string(),
                default: None,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect("integer filter should resolve");

        assert_eq!(value, Some(json!(1_700_000_000_000_000_i64)));
    }

    #[test]
    fn resolve_value_source_rejects_invalid_filter_ints() {
        let filters = HashMap::from([("start_time".to_string(), "not-a-number".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::FilterInt {
                key: "start_time".to_string(),
                default: None,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect_err("invalid integer filter should fail");

        assert!(
            error
                .to_string()
                .contains("filter 'start_time' value 'not-a-number' is not a valid i64")
        );
    }

    #[test]
    fn resolve_value_source_splits_filter_parts() {
        let filters = HashMap::from([("issue_identifier".to_string(), "SOURCE-496".to_string())]);

        let team = resolve_value_source(
            &ValueSourceSpec::FilterSplit {
                key: "issue_identifier".to_string(),
                separator: "-".to_string(),
                part: 0,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect("split filter should resolve");
        let number = resolve_value_source(
            &ValueSourceSpec::FilterSplitInt {
                key: "issue_identifier".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect("split integer filter should resolve");

        assert_eq!(team, Some(json!("SOURCE")));
        assert_eq!(number, Some(json!(496)));
    }

    #[test]
    fn resolve_value_source_splits_function_argument_parts() {
        let args = HashMap::from([("issue".to_string(), "SOURCE-496".to_string())]);

        let team = resolve_value_source(
            &ValueSourceSpec::ArgSplit {
                key: "issue".to_string(),
                separator: "-".to_string(),
                part: 0,
            },
            &test_render_context(&HashMap::new(), &args, &BTreeMap::new()),
        )
        .expect("split function argument should resolve");
        let number = resolve_value_source(
            &ValueSourceSpec::ArgSplitInt {
                key: "issue".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&HashMap::new(), &args, &BTreeMap::new()),
        )
        .expect("split integer function argument should resolve");

        assert_eq!(team, Some(json!("SOURCE")));
        assert_eq!(number, Some(json!(496)));
    }

    #[test]
    fn resolve_value_source_rejects_missing_filter_split_part() {
        let filters = HashMap::from([("issue_identifier".to_string(), "SOURCE496".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::FilterSplit {
                key: "issue_identifier".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect_err("missing split part should fail");

        assert!(
            error.to_string().contains(
                "filter 'issue_identifier' value 'SOURCE496' does not contain split part 1"
            )
        );
    }

    #[test]
    fn resolve_value_source_rejects_missing_function_argument_split_part() {
        let args = HashMap::from([("issue".to_string(), "SOURCE496".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::ArgSplit {
                key: "issue".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&HashMap::new(), &args, &BTreeMap::new()),
        )
        .expect_err("missing split function argument part should fail");

        assert!(
            error.to_string().contains(
                "function argument 'issue' value 'SOURCE496' does not contain split part 1"
            )
        );
    }

    #[test]
    fn resolve_value_source_rejects_missing_filter_split_int_part() {
        let filters = HashMap::from([("issue_identifier".to_string(), "SOURCE496".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::FilterSplitInt {
                key: "issue_identifier".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect_err("missing split integer part should fail");

        assert!(
            error.to_string().contains(
                "filter 'issue_identifier' value 'SOURCE496' does not contain split part 1"
            )
        );
    }

    #[test]
    fn resolve_value_source_rejects_invalid_function_argument_split_int_part() {
        let args = HashMap::from([("issue".to_string(), "SOURCE-abc".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::ArgSplitInt {
                key: "issue".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
            &test_render_context(&HashMap::new(), &args, &BTreeMap::new()),
        )
        .expect_err("invalid split function argument int should fail");

        assert!(
            error
                .to_string()
                .contains("function argument 'issue' split part 1 value 'abc' is not a valid i64")
        );
    }

    #[test]
    fn resolve_value_source_parses_filter_bools_as_bools() {
        let filters = HashMap::from([("descending".to_string(), "false".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::FilterBool {
                key: "descending".to_string(),
                default: None,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect("bool filter should resolve");

        assert_eq!(value, Some(json!(false)));
    }

    #[test]
    fn resolve_value_source_parses_filter_string_arrays_as_json_arrays() {
        let filters = HashMap::from([(
            "log_stream_names".to_string(),
            r#"["stream-a","stream-b"]"#.to_string(),
        )]);

        let value = resolve_value_source(
            &ValueSourceSpec::FilterStringArray {
                key: "log_stream_names".to_string(),
                default: None,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect("string array filter should resolve");

        assert_eq!(value, Some(json!(["stream-a", "stream-b"])));
    }

    #[test]
    fn resolve_value_source_uses_filter_string_array_default() {
        let value = resolve_value_source(
            &ValueSourceSpec::FilterStringArray {
                key: "log_stream_names".to_string(),
                default: Some(vec!["stream-a".to_string()]),
            },
            &test_render_context(&HashMap::new(), &HashMap::new(), &BTreeMap::new()),
        )
        .expect("string array filter default should resolve");

        assert_eq!(value, Some(json!(["stream-a"])));
    }

    #[test]
    fn resolve_value_source_rejects_invalid_filter_string_arrays() {
        let filters = HashMap::from([(
            "log_stream_names".to_string(),
            r#"["stream-a",42]"#.to_string(),
        )]);

        let error = resolve_value_source(
            &ValueSourceSpec::FilterStringArray {
                key: "log_stream_names".to_string(),
                default: None,
            },
            &test_render_context(&filters, &HashMap::new(), &BTreeMap::new()),
        )
        .expect_err("non-string array entries should fail");

        assert!(
            error.to_string().contains(
                "filter 'log_stream_names' value '[\"stream-a\",42]' is not a valid JSON array of strings"
            )
        );
    }

    #[test]
    fn one_of_prefers_first_present_value() {
        let resolved_inputs = inputs(&[("API_KEY", "lin_api_key"), ("OAUTH_TOKEN", "oauth")]);

        let value = resolve_value_source(
            &ValueSourceSpec::OneOf {
                values: vec![
                    ValueSourceSpec::Input {
                        key: "API_KEY".to_string(),
                    },
                    ValueSourceSpec::Bearer {
                        key: "OAUTH_TOKEN".to_string(),
                    },
                ],
            },
            &RenderContext::source_scoped(&resolved_inputs),
        )
        .expect("one_of should resolve");

        assert_eq!(value, Some(json!("lin_api_key")));
    }

    #[test]
    fn one_of_uses_bearer_fallback_value() {
        let resolved_inputs = inputs(&[("OAUTH_TOKEN", "oauth")]);

        let value = resolve_value_source(
            &ValueSourceSpec::OneOf {
                values: vec![
                    ValueSourceSpec::Input {
                        key: "API_KEY".to_string(),
                    },
                    ValueSourceSpec::Bearer {
                        key: "OAUTH_TOKEN".to_string(),
                    },
                ],
            },
            &RenderContext::source_scoped(&resolved_inputs),
        )
        .expect("one_of should resolve");

        assert_eq!(value, Some(json!("Bearer oauth")));
    }

    #[test]
    fn one_of_ignores_empty_bearer_values() {
        let resolved_inputs = inputs(&[("OAUTH_TOKEN", "")]);

        let value = resolve_value_source(
            &ValueSourceSpec::OneOf {
                values: vec![ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                }],
            },
            &RenderContext::source_scoped(&resolved_inputs),
        )
        .expect("one_of should resolve");

        assert_eq!(value, None);
    }

    #[test]
    fn one_of_provenance_follows_only_the_selected_branch() {
        let resolved_inputs = inputs(&[
            ("EMPTY_SECRET", ""),
            ("PUBLIC", "visible"),
            ("OAUTH_TOKEN", "secret"),
        ]);
        let secret_names = BTreeSet::from(["EMPTY_SECRET".to_string(), "OAUTH_TOKEN".to_string()]);
        let context = RenderContext::source_scoped(&resolved_inputs);

        let public = resolve_value_source_with_secret_provenance(
            &ValueSourceSpec::OneOf {
                values: vec![
                    ValueSourceSpec::Input {
                        key: "EMPTY_SECRET".to_string(),
                    },
                    ValueSourceSpec::Input {
                        key: "PUBLIC".to_string(),
                    },
                    ValueSourceSpec::Bearer {
                        key: "OAUTH_TOKEN".to_string(),
                    },
                ],
            },
            &context,
            &secret_names,
        )
        .expect("one_of should resolve")
        .expect("public fallback should be selected");
        assert_eq!(public.value, json!("visible"));
        assert!(!public.depends_on_secret);

        let secret = resolve_value_source_with_secret_provenance(
            &ValueSourceSpec::OneOf {
                values: vec![
                    ValueSourceSpec::Input {
                        key: "MISSING".to_string(),
                    },
                    ValueSourceSpec::Bearer {
                        key: "OAUTH_TOKEN".to_string(),
                    },
                ],
            },
            &context,
            &secret_names,
        )
        .expect("one_of should resolve")
        .expect("secret fallback should be selected");
        assert_eq!(secret.value, json!("Bearer secret"));
        assert!(secret.depends_on_secret);
    }

    #[test]
    fn template_provenance_tracks_rendered_secret_tokens() {
        let resolved_inputs = inputs(&[("HOST", "api.example.test"), ("PATH", "items")]);
        let secret_names = BTreeSet::from(["HOST".to_string()]);
        let context = RenderContext::source_scoped(&resolved_inputs);

        let secret = render_template_with_secret_provenance(
            &ParsedTemplate::parse("https://{{input.HOST}}/{{input.PATH}}")
                .expect("secret template"),
            &context,
            &secret_names,
        )
        .expect("template should render");
        assert_eq!(secret.value, "https://api.example.test/items");
        assert!(secret.depends_on_secret);

        let public = render_template_with_secret_provenance(
            &ParsedTemplate::parse("/{{input.PATH}}").expect("public template"),
            &context,
            &secret_names,
        )
        .expect("template should render");
        assert!(!public.depends_on_secret);
    }

    #[test]
    fn one_of_input_dependency_validation_accepts_any_resolved_branch() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Input {
                    key: "API_KEY".to_string(),
                },
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
            ],
        };

        validate_value_source_inputs(&value, &inputs(&[("API_KEY", "lin_api_key")]))
            .expect("api key should satisfy one_of");
        validate_value_source_inputs(&value, &inputs(&[("OAUTH_TOKEN", "oauth_access")]))
            .expect("oauth token should satisfy one_of");
        assert!(
            validate_value_source_inputs(&value, &inputs(&[]))
                .expect_err("missing both should fail")
                .to_string()
                .contains("missing source input 'OAUTH_TOKEN' for `from: bearer` value source")
        );
    }
}
