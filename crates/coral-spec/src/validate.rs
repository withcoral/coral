//! Shared manifest validation helpers.

use std::collections::{HashMap, HashSet};

use crate::common::{
    BodySpec, ColumnSpec, ExprSpec, FilterSpec, PaginationSpec, RequestRouteSpec, RequestSpec,
    ValueSourceSpec,
};
use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

pub(crate) fn validate_table_names<'a>(
    schema: &str,
    table_names: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let mut seen_tables = HashSet::new();
    for table_name in table_names {
        if !seen_tables.insert(table_name) {
            return Err(ManifestError::validation(format!(
                "source '{schema}' has duplicate table '{table_name}'"
            )));
        }
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
            if !known_filters.contains(key) =>
        {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{key}'"
            )));
        }
        ValueSourceSpec::Template { template } => {
            validate_template(template, known_filters, context)?;
        }
        _ => {}
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
            TemplateNamespace::Input | TemplateNamespace::State | TemplateNamespace::Other(_) => {
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

    use crate::common::{
        ColumnSpec, ExprSpec, FilterMode, FilterSpec, PaginationSpec, QueryParamSpec,
        RequestRouteSpec, RequestSpec, ValueSourceSpec,
    };
    use crate::template::ParsedTemplate;

    use super::{validate_filters_and_column_exprs, validate_http_table};

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
