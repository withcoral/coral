//! Shared manifest validation helpers.

use std::collections::HashSet;

use crate::common::{
    ColumnSpec, ExprSpec, FilterSpec, PaginationSpec, RequestRouteSpec, RequestSpec,
    ValueSourceSpec,
};
use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

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

    validate_template(
        &request.path,
        &known_filters,
        &format!("{schema}.{table_name}"),
    )?;

    for header in &request.headers {
        validate_value_source(
            &header.value,
            &known_filters,
            &format!("{schema}.{table_name} request header '{}'", header.name),
        )?;
    }

    for param in &request.query {
        validate_value_source(
            &param.value,
            &known_filters,
            &format!("{schema}.{table_name} query param '{}'", param.name),
        )?;
    }

    for field in &request.body {
        validate_value_source(
            &field.value,
            &known_filters,
            &format!(
                "{schema}.{table_name} request body path '{}'",
                field.path.join(".")
            ),
        )?;
    }

    for route in requests {
        let known: HashSet<&str> = filters.iter().map(|f| f.name.as_str()).collect();
        for filter_name in &route.when_filters {
            if !known.contains(filter_name.as_str()) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table_name} requests.when_filters references unknown filter '{filter_name}'"
                )));
            }
        }
        validate_http_binding(
            schema,
            table_name,
            filters,
            &route.request,
            columns,
            pagination,
        )?;
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

fn validate_http_binding(
    schema: &str,
    table_name: &str,
    filters: &[FilterSpec],
    request: &RequestSpec,
    _columns: &[ColumnSpec],
    _pagination: &PaginationSpec,
) -> Result<()> {
    let known_filters: HashSet<String> = filters.iter().map(|f| f.name.clone()).collect();

    validate_template(
        &request.path,
        &known_filters,
        &format!("{schema}.{table_name}"),
    )?;

    for header in &request.headers {
        validate_value_source(
            &header.value,
            &known_filters,
            &format!("{schema}.{table_name} request header '{}'", header.name),
        )?;
    }

    for param in &request.query {
        validate_value_source(
            &param.value,
            &known_filters,
            &format!("{schema}.{table_name} query param '{}'", param.name),
        )?;
    }

    for field in &request.body {
        validate_value_source(
            &field.value,
            &known_filters,
            &format!(
                "{schema}.{table_name} request body path '{}'",
                field.path.join(".")
            ),
        )?;
    }

    Ok(())
}

fn validate_value_source(
    source: &ValueSourceSpec,
    known_filters: &HashSet<String>,
    context: &str,
) -> Result<()> {
    match source {
        ValueSourceSpec::Filter { key, .. } => {
            if !known_filters.contains(key) {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown filter '{key}'"
                )));
            }
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
        ExprSpec::FromFilter { key } => {
            if !known_filters.contains(key) {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown filter '{key}'"
                )));
            }
        }
        ExprSpec::Coalesce { exprs } => {
            for nested in exprs {
                validate_expr(nested, known_filters, context)?;
            }
        }
        ExprSpec::IfPresent { check, .. } => {
            validate_expr(check, known_filters, context)?;
        }
        ExprSpec::ObjectFilterPath { filter_key, .. } => {
            if !known_filters.contains(filter_key) {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown filter '{filter_key}'"
                )));
            }
        }
        _ => {}
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
            TemplateNamespace::Secret | TemplateNamespace::Variable | TemplateNamespace::State => {}
            TemplateNamespace::Env | TemplateNamespace::Other(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported template token '{}'",
                    token.raw()
                )));
            }
        }
    }

    Ok(())
}
