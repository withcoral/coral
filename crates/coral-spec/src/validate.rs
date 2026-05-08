//! Shared manifest validation helpers.

use std::collections::{HashMap, HashSet};

use crate::common::{
    BodySpec, ColumnSpec, ExprSpec, FilterSpec, FunctionArgBinding, PaginationSpec,
    RequestRouteSpec, RequestSpec, SourceTableFunctionSpec, ValueSourceSpec,
};
use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

pub(crate) fn validate_table_names<'a>(
    schema: &str,
    table_names: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let mut seen_tables = HashSet::new();
    for table_name in table_names {
        let key = table_name.to_ascii_lowercase();
        if seen_tables.contains(&key) {
            return Err(ManifestError::validation(format!(
                "source '{schema}' has duplicate table '{key}'"
            )));
        }
        seen_tables.insert(key);
    }

    Ok(())
}

pub(crate) fn validate_http_table(
    schema: &str,
    table_name: &str,
    filters: &[FilterSpec],
    columns: &[ColumnSpec],
    request: &RequestSpec,
    requests: &[RequestRouteSpec],
    pagination: &PaginationSpec,
) -> Result<()> {
    if request.path.raw().trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "{schema}.{table_name} has an empty request.path"
        )));
    }

    validate_columns(columns, schema, table_name)?;
    let known_filters = validate_filters_and_column_exprs(filters, columns, schema, table_name)?;

    validate_request_bindings(schema, table_name, request, &known_filters)?;

    for route in requests {
        for filter_name in &route.when_filters {
            if !known_filters.contains(filter_name) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table_name} requests.when_filters references unknown filter '{filter_name}'"
                )));
            }
        }
        validate_request_bindings(schema, table_name, &route.request, &known_filters)?;
    }

    for filter in filters.iter().filter(|f| f.required) {
        if !known_filters.contains(&filter.name) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table_name} required filter '{}' is not declared",
                filter.name
            )));
        }
    }

    pagination.validate(schema, table_name)
}

pub(crate) fn validate_http_function_names(
    source_name: &str,
    table_names: impl IntoIterator<Item = impl AsRef<str>>,
    functions: &[SourceTableFunctionSpec],
) -> Result<()> {
    let table_names = table_names
        .into_iter()
        .map(|name| name.as_ref().to_string())
        .collect::<HashSet<_>>();
    let mut function_names = HashSet::new();

    for function in functions {
        validate_identifier(
            &function.name,
            &format!("source '{source_name}' function name"),
        )?;
        if table_names.contains(&function.name) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' declares both a table and function named '{}'",
                function.name
            )));
        }
        if !function_names.insert(function.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{}' is declared more than once",
                function.name
            )));
        }
    }

    Ok(())
}

pub(crate) fn validate_http_function(
    source_name: &str,
    function: &SourceTableFunctionSpec,
) -> Result<()> {
    validate_identifier(
        &function.name,
        &format!("source '{source_name}' function name"),
    )?;

    let mut arg_names = HashSet::new();
    let mut request_arg_names = HashSet::new();

    for arg in &function.args {
        validate_identifier(
            &arg.name,
            &format!(
                "source '{source_name}' function '{}' argument",
                function.name
            ),
        )?;
        if !arg_names.insert(arg.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{}' argument '{}' is declared more than once",
                function.name, arg.name
            )));
        }
        validate_unique_values(
            &arg.values,
            &format!(
                "source '{source_name}' function '{}' argument '{}'",
                function.name, arg.name
            ),
        )?;
        validate_function_binding(
            source_name,
            &function.name,
            &arg.bind,
            &mut request_arg_names,
        )?;
    }

    validate_filters_and_column_exprs(
        &[],
        &function.columns,
        source_name,
        &format!("function '{}'", function.name),
    )?;
    validate_function_request_bindings(source_name, function, &request_arg_names)?;
    function
        .pagination
        .validate(source_name, &format!("function '{}'", function.name))?;

    Ok(())
}

pub(crate) fn validate_filters_and_column_exprs(
    filters: &[FilterSpec],
    columns: &[ColumnSpec],
    schema: &str,
    table: &str,
) -> Result<HashSet<String>> {
    let mut known_filters = HashSet::new();
    for filter in filters {
        if !known_filters.insert(filter.name.clone()) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} has duplicate filter '{}'",
                filter.name
            )));
        }
    }

    for col in columns {
        if let Some(expr) = &col.expr {
            validate_expr(
                expr,
                &known_filters,
                &format!("{schema}.{table} column '{}'", col.name),
            )?;
        }
    }

    Ok(known_filters)
}

pub(crate) fn validate_unique_values(values: &[String], context: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "{context} values must not contain empty strings"
            )));
        }
        if !seen.insert(value.as_str()) {
            return Err(ManifestError::validation(format!(
                "{context} value '{value}' is declared more than once"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_columns(columns: &[ColumnSpec], schema: &str, table: &str) -> Result<()> {
    let mut seen_columns = HashSet::new();
    for col in columns {
        if !seen_columns.insert(col.name.clone()) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} has duplicate column '{}'",
                col.name
            )));
        }
    }

    Ok(())
}

fn validate_request_bindings(
    schema: &str,
    table_name: &str,
    request: &RequestSpec,
    known_filters: &HashSet<String>,
) -> Result<()> {
    validate_template(
        &request.path,
        known_filters,
        &format!("{schema}.{table_name}"),
    )?;

    for header in &request.headers {
        validate_value_source(
            &header.value,
            known_filters,
            &format!("{schema}.{table_name} request header '{}'", header.name),
        )?;
    }

    for param in &request.query {
        validate_value_source(
            &param.value,
            known_filters,
            &format!("{schema}.{table_name} query param '{}'", param.name),
        )?;
    }

    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                validate_value_source(
                    &field.value,
                    known_filters,
                    &format!(
                        "{schema}.{table_name} request body path '{}'",
                        field.path.join(".")
                    ),
                )?;
            }
        }
        BodySpec::Text { content } => {
            validate_value_source(
                content,
                known_filters,
                &format!("{schema}.{table_name} request body text"),
            )?;
        }
    }

    Ok(())
}

fn validate_value_source(
    source: &ValueSourceSpec,
    known_filters: &HashSet<String>,
    context: &str,
) -> Result<()> {
    match source {
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. }
            if !known_filters.contains(key) =>
        {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{key}'"
            )));
        }
        ValueSourceSpec::Template { template } => {
            validate_template(template, known_filters, context)?;
        }
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. } => {
            return Err(ManifestError::validation(format!(
                "{context} uses function argument '{key}' outside a function request"
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validate_function_binding<'a>(
    source_name: &str,
    function_name: &str,
    binding: &'a FunctionArgBinding,
    request_arg_names: &mut HashSet<&'a str>,
) -> Result<()> {
    if !request_arg_names.insert(binding.arg.as_str()) {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' function '{function_name}' has multiple bindings for request arg '{}'",
            binding.arg
        )));
    }

    Ok(())
}

fn validate_function_request_bindings(
    source_name: &str,
    function: &SourceTableFunctionSpec,
    request_arg_names: &HashSet<&str>,
) -> Result<()> {
    if function.request.path.raw().trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' function '{}' has an empty request.path",
            function.name
        )));
    }

    validate_arg_template(
        &function.request.path,
        request_arg_names,
        &format!("source '{source_name}' function '{}'", function.name),
    )?;

    for header in &function.request.headers {
        validate_arg_value_source(
            &header.value,
            request_arg_names,
            &format!(
                "source '{source_name}' function '{}' request header '{}'",
                function.name, header.name
            ),
        )?;
    }

    for param in &function.request.query {
        validate_arg_value_source(
            &param.value,
            request_arg_names,
            &format!(
                "source '{source_name}' function '{}' query param '{}'",
                function.name, param.name
            ),
        )?;
    }

    match &function.request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                validate_arg_value_source(
                    &field.value,
                    request_arg_names,
                    &format!(
                        "source '{source_name}' function '{}' request body path '{}'",
                        function.name,
                        field.path.join(".")
                    ),
                )?;
            }
        }
        BodySpec::Text { content } => {
            validate_arg_value_source(
                content,
                request_arg_names,
                &format!(
                    "source '{source_name}' function '{}' request body text",
                    function.name
                ),
            )?;
        }
    }

    Ok(())
}

fn validate_arg_value_source(
    source: &ValueSourceSpec,
    request_arg_names: &HashSet<&str>,
    context: &str,
) -> Result<()> {
    match source {
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. }
            if !request_arg_names.contains(key.as_str()) =>
        {
            return Err(ManifestError::validation(format!(
                "{context} references unknown request arg '{key}'"
            )));
        }
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => {
            return Err(ManifestError::validation(format!(
                "{context} uses table filter '{key}' inside a function request"
            )));
        }
        ValueSourceSpec::Template { template } => {
            validate_arg_template(template, request_arg_names, context)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_arg_template(
    template: &ParsedTemplate,
    request_arg_names: &HashSet<&str>,
    context: &str,
) -> Result<()> {
    for token in template.tokens() {
        match token.namespace() {
            TemplateNamespace::Arg => {
                if !request_arg_names.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown request arg '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            TemplateNamespace::Input | TemplateNamespace::State => {}
            TemplateNamespace::Filter | TemplateNamespace::Expr | TemplateNamespace::Other(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported function request template token '{}'",
                    token.raw()
                )));
            }
        }
    }
    Ok(())
}

fn validate_identifier(value: &str, context: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(ManifestError::validation(format!(
            "{context} must not be empty"
        )));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ManifestError::validation(format!(
            "{context} '{value}' must start with a letter or underscore"
        )));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(ManifestError::validation(format!(
            "{context} '{value}' may only contain letters, numbers, and underscores"
        )));
    }
    Ok(())
}

fn validate_expr(expr: &ExprSpec, known_filters: &HashSet<String>, context: &str) -> Result<()> {
    match expr {
        ExprSpec::FromFilter { key } if !known_filters.contains(key) => {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{key}'"
            )));
        }
        ExprSpec::Coalesce { exprs } => {
            for nested in exprs {
                validate_expr(nested, known_filters, context)?;
            }
        }
        ExprSpec::IfPresent { check, .. } => {
            validate_expr(check, known_filters, context)?;
        }
        ExprSpec::ObjectFilterPath { filter_key, .. } if !known_filters.contains(filter_key) => {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{filter_key}'"
            )));
        }
        ExprSpec::FormatTimestamp { expr, .. } => {
            validate_expr(expr, known_filters, context)?;
        }
        ExprSpec::Replace { expr, from, .. } => {
            if from.is_empty() {
                return Err(ManifestError::validation(format!(
                    "{context} has replace expression with empty 'from' value"
                )));
            }
            validate_expr(expr, known_filters, context)?;
        }
        ExprSpec::Template { template, values } => {
            for (key, value_expr) in values {
                validate_expr(
                    value_expr,
                    known_filters,
                    &format!("{context} template value '{key}'"),
                )?;
            }
            validate_expr_template(template, values, known_filters, context)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_expr_template(
    template: &ParsedTemplate,
    values: &HashMap<String, ExprSpec>,
    known_filters: &HashSet<String>,
    context: &str,
) -> Result<()> {
    for token in template.tokens() {
        match token.namespace() {
            TemplateNamespace::Expr => {
                if !values.contains_key(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown expr '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            TemplateNamespace::Filter => {
                if !known_filters.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown filter '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            TemplateNamespace::Input
            | TemplateNamespace::Arg
            | TemplateNamespace::State
            | TemplateNamespace::Other(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported expr template token '{}'",
                    token.raw()
                )));
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_template(
    template: &ParsedTemplate,
    known_filters: &HashSet<String>,
    context: &str,
) -> Result<()> {
    for token in template.tokens() {
        match token.namespace() {
            TemplateNamespace::Filter => {
                if !known_filters.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown filter '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            TemplateNamespace::Input | TemplateNamespace::State => {}
            TemplateNamespace::Arg => {
                return Err(ManifestError::validation(format!(
                    "{context} uses function argument token '{}' outside a function request",
                    token.raw()
                )));
            }
            TemplateNamespace::Expr | TemplateNamespace::Other(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported template token '{}'",
                    token.raw()
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        validate_filters_and_column_exprs, validate_http_function, validate_http_function_names,
        validate_http_table, validate_table_names,
    };
    use crate::common::{
        ColumnSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding, PaginationSpec,
        QueryParamSpec, RequestRouteSpec, RequestSpec, SourceTableFunctionSpec,
        TableFunctionArgSpec, ValueSourceSpec,
    };
    use crate::template::ParsedTemplate;

    fn test_column() -> ColumnSpec {
        ColumnSpec {
            name: "id".to_string(),
            data_type: "Utf8".to_string(),
            nullable: true,
            r#virtual: false,
            description: String::new(),
            expr: None,
        }
    }

    fn test_filters() -> Vec<FilterSpec> {
        vec![FilterSpec {
            name: "id".to_string(),
            required: false,
            mode: FilterMode::Equality,
        }]
    }

    fn column_with_expr(expr: ExprSpec) -> ColumnSpec {
        let mut column = test_column();
        column.expr = Some(expr);
        column
    }

    fn base_request() -> RequestSpec {
        RequestSpec {
            path: ParsedTemplate::parse("/messages").expect("request path"),
            ..RequestSpec::default()
        }
    }

    fn function_with_request_value(value: ValueSourceSpec) -> SourceTableFunctionSpec {
        SourceTableFunctionSpec {
            name: "search".to_string(),
            description: String::new(),
            fetch_limit_default: None,
            args: vec![TableFunctionArgSpec {
                name: "query".to_string(),
                required: true,
                values: vec![],
                bind: FunctionArgBinding {
                    arg: "q".to_string(),
                },
            }],
            request: RequestSpec {
                path: ParsedTemplate::parse("/search").expect("request path"),
                query: vec![QueryParamSpec {
                    name: "q".to_string(),
                    value,
                }],
                ..RequestSpec::default()
            },
            response: crate::ResponseSpec::default(),
            pagination: PaginationSpec::default(),
            columns: vec![],
        }
    }

    #[test]
    fn validate_table_names_rejects_duplicate_table_names() {
        let schema = "github";
        let table_names = ["issues", "prs", "Issues"];

        let error = validate_table_names(schema, table_names)
            .expect_err("expected duplicate table to be rejected");

        assert!(
            error
                .to_string()
                .contains("source 'github' has duplicate table 'issues'")
        );
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_in_default_request_bindings() {
        let request = RequestSpec {
            query: vec![QueryParamSpec {
                name: "user_id".to_string(),
                value: ValueSourceSpec::Filter {
                    key: "missing".to_string(),
                    default: None,
                },
            }],
            ..base_request()
        };

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &request,
            &[],
            &PaginationSpec::default(),
        )
        .expect_err("default request should reject unknown filters");

        assert!(
            error
                .to_string()
                .contains("references unknown filter 'missing'")
        );
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_in_route_request_bindings() {
        let route = RequestRouteSpec {
            when_filters: vec!["id".to_string()],
            request: RequestSpec {
                query: vec![QueryParamSpec {
                    name: "cursor".to_string(),
                    value: ValueSourceSpec::Filter {
                        key: "missing".to_string(),
                        default: None,
                    },
                }],
                ..base_request()
            },
        };

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &base_request(),
            &[route],
            &PaginationSpec::default(),
        )
        .expect_err("route request should reject unknown filters");

        assert!(
            error
                .to_string()
                .contains("references unknown filter 'missing'")
        );
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_split_bindings() {
        let request = RequestSpec {
            query: vec![QueryParamSpec {
                name: "team_key".to_string(),
                value: ValueSourceSpec::FilterSplit {
                    key: "missing".to_string(),
                    separator: "-".to_string(),
                    part: 0,
                },
            }],
            ..base_request()
        };

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &request,
            &[],
            &PaginationSpec::default(),
        )
        .expect_err("filter_split should reject unknown filters");

        assert!(
            error
                .to_string()
                .contains("references unknown filter 'missing'")
        );
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_split_int_bindings() {
        let request = RequestSpec {
            query: vec![QueryParamSpec {
                name: "issue_number".to_string(),
                value: ValueSourceSpec::FilterSplitInt {
                    key: "missing".to_string(),
                    separator: "-".to_string(),
                    part: 1,
                },
            }],
            ..base_request()
        };

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &request,
            &[],
            &PaginationSpec::default(),
        )
        .expect_err("filter_split_int should reject unknown filters");

        assert!(
            error
                .to_string()
                .contains("references unknown filter 'missing'")
        );
    }

    #[test]
    fn validate_http_table_rejects_function_arg_value_sources() {
        let cases = [
            ValueSourceSpec::Arg {
                key: "query".to_string(),
                default: None,
            },
            ValueSourceSpec::ArgInt {
                key: "limit".to_string(),
                default: None,
            },
            ValueSourceSpec::ArgBool {
                key: "archived".to_string(),
                default: None,
            },
        ];

        for value in cases {
            let request = RequestSpec {
                query: vec![QueryParamSpec {
                    name: "value".to_string(),
                    value,
                }],
                ..base_request()
            };

            let error = validate_http_table(
                "demo",
                "messages",
                &test_filters(),
                &[test_column()],
                &request,
                &[],
                &PaginationSpec::default(),
            )
            .expect_err("table requests should reject function arguments");

            assert!(
                error.to_string().contains("uses function argument"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn validate_http_table_rejects_function_arg_template_tokens() {
        let request = RequestSpec {
            path: ParsedTemplate::parse("/search/{{arg.q}}").expect("template"),
            ..RequestSpec::default()
        };

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &request,
            &[],
            &PaginationSpec::default(),
        )
        .expect_err("table request templates should reject function arguments");

        assert!(
            error
                .to_string()
                .contains("uses function argument token 'arg.q' outside a function request")
        );
    }

    #[test]
    fn validate_http_function_rejects_table_filter_value_sources() {
        let cases = [
            ValueSourceSpec::Filter {
                key: "q".to_string(),
                default: None,
            },
            ValueSourceSpec::FilterInt {
                key: "limit".to_string(),
                default: None,
            },
            ValueSourceSpec::FilterBool {
                key: "archived".to_string(),
                default: None,
            },
            ValueSourceSpec::FilterSplit {
                key: "repo".to_string(),
                separator: "/".to_string(),
                part: 0,
            },
            ValueSourceSpec::FilterSplitInt {
                key: "issue_key".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
        ];

        for value in cases {
            let function = function_with_request_value(value);
            let error = validate_http_function("demo", &function)
                .expect_err("function requests should reject table filters");

            assert!(
                error.to_string().contains("uses table filter"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn validate_http_function_names_rejects_table_name_collisions() {
        let function = SourceTableFunctionSpec {
            name: "messages".to_string(),
            description: String::new(),
            fetch_limit_default: None,
            args: vec![],
            request: base_request(),
            response: crate::ResponseSpec::default(),
            pagination: PaginationSpec::default(),
            columns: vec![],
        };

        let error = validate_http_function_names("demo", ["messages"], &[function])
            .expect_err("function should not share a table name");

        assert!(
            error
                .to_string()
                .contains("declares both a table and function named 'messages'")
        );
    }

    #[test]
    fn validate_http_function_rejects_filter_column_exprs() {
        let mut function = function_with_request_value(ValueSourceSpec::Arg {
            key: "q".to_string(),
            default: None,
        });
        function.columns = vec![column_with_expr(ExprSpec::FromFilter {
            key: "q".to_string(),
        })];

        let error = validate_http_function("demo", &function)
            .expect_err("function columns should not reference table filters");

        assert!(error.to_string().contains("references unknown filter 'q'"));
    }

    #[test]
    fn validate_column_template_accepts_expr_and_filter_tokens() {
        let column = column_with_expr(ExprSpec::Template {
            template: ParsedTemplate::parse("{{filter.id|default-id}}/{{expr.slug|unknown}}")
                .expect("template"),
            values: HashMap::from([(
                "slug".to_string(),
                ExprSpec::Replace {
                    expr: Box::new(ExprSpec::Path {
                        path: vec!["name".to_string()],
                    }),
                    from: " ".to_string(),
                    to: "-".to_string(),
                },
            )]),
        });

        validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
            .expect("expr template should validate");
    }

    #[test]
    fn validate_column_template_rejects_unknown_expr_token() {
        let column = column_with_expr(ExprSpec::Template {
            template: ParsedTemplate::parse("{{expr.slug|unknown}}").expect("template"),
            values: HashMap::new(),
        });

        let error =
            validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
                .expect_err("unknown expr token should fail");

        assert!(error.to_string().contains("references unknown expr 'slug'"));
    }

    #[test]
    fn validate_column_template_rejects_secret_tokens() {
        let column = column_with_expr(ExprSpec::Template {
            template: ParsedTemplate::parse("{{secret.API_KEY}}").expect("template"),
            values: HashMap::new(),
        });

        let error =
            validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
                .expect_err("secret token should fail");

        assert!(
            error
                .to_string()
                .contains("uses unsupported expr template token 'secret.API_KEY'")
        );
    }

    #[test]
    fn validate_replace_rejects_empty_from() {
        let column = column_with_expr(ExprSpec::Replace {
            expr: Box::new(ExprSpec::Path {
                path: vec!["name".to_string()],
            }),
            from: String::new(),
            to: "-".to_string(),
        });

        let error =
            validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
                .expect_err("empty replace source should fail");

        assert!(
            error
                .to_string()
                .contains("has replace expression with empty 'from' value")
        );
    }
}
