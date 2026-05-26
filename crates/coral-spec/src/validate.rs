//! Shared manifest validation helpers.

use std::collections::{HashMap, HashSet};

use crate::common::{
    BodySpec, ColumnSpec, DetailHintSpec, ExprSpec, FilterSpec, FunctionArgBinding,
    MAX_SEARCH_CALLS_PER_QUERY, MAX_SEARCH_CANDIDATES_PER_QUERY, MAX_SEARCH_TOP_K, PaginationSpec,
    RequestRouteSpec, RequestSpec, SearchLimitsSpec, SourceTableFunctionKind,
    SourceTableFunctionSpec, ValueSourceSpec,
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

#[expect(
    clippy::too_many_arguments,
    reason = "HTTP table validation mirrors the source-spec fields it validates."
)]
pub(crate) fn validate_http_table(
    schema: &str,
    table_name: &str,
    filters: &[FilterSpec],
    columns: &[ColumnSpec],
    request: &RequestSpec,
    requests: &[RequestRouteSpec],
    pagination: &PaginationSpec,
    search_limits: Option<&SearchLimitsSpec>,
    detail_hints: &[DetailHintSpec],
) -> Result<()> {
    if request.path.raw().trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "{schema}.{table_name} has an empty request.path"
        )));
    }

    validate_columns(columns, schema, table_name)?;
    let known_filters = validate_filters_and_column_exprs(filters, columns, schema, table_name)?;
    // Table-level search metadata is optional for legacy table surfaces; new
    // provider-native retrieval surfaces should use search table functions.
    validate_search_metadata(
        schema,
        table_name,
        false,
        search_limits,
        detail_hints,
        columns,
    )?;

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

    validate_columns(
        &function.columns,
        source_name,
        &format!("function '{}'", function.name),
    )?;
    validate_column_exprs(
        &function.columns,
        &HashSet::new(),
        &request_arg_names,
        source_name,
        &format!("function '{}'", function.name),
    )?;
    validate_search_metadata(
        source_name,
        &format!("function '{}'", function.name),
        function.kind == SourceTableFunctionKind::Search,
        function.search_limits.as_ref(),
        &function.detail_hints,
        &function.columns,
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
        filter.manifest_data_type()?;
    }

    validate_column_exprs(columns, &known_filters, &HashSet::new(), schema, table)?;

    Ok(known_filters)
}

fn validate_column_exprs(
    columns: &[ColumnSpec],
    known_filters: &HashSet<String>,
    known_args: &HashSet<&str>,
    schema: &str,
    table: &str,
) -> Result<()> {
    for col in columns {
        if let Some(expr) = &col.expr {
            validate_expr(
                expr,
                known_filters,
                known_args,
                &format!("{schema}.{table} column '{}'", col.name),
            )?;
        }
    }
    Ok(())
}

pub(crate) struct DetailHintTargetTable<'a> {
    pub(crate) name: &'a str,
    pub(crate) filters: &'a [FilterSpec],
}

pub(crate) struct DetailHintDeclaringSurface<'a> {
    pub(crate) surface_kind: &'static str,
    pub(crate) surface_name: &'a str,
    pub(crate) hints: &'a [DetailHintSpec],
    pub(crate) columns: &'a [ColumnSpec],
}

pub(crate) fn validate_detail_hint_references(
    schema: &str,
    targets: &[DetailHintTargetTable<'_>],
    sources: &[DetailHintDeclaringSurface<'_>],
) -> Result<()> {
    for source in sources {
        for hint in source.hints {
            let context = format!(
                "{schema}.{} '{}' detail_hints",
                source.surface_kind, source.surface_name
            );
            let Some(target) = resolve_detail_hint_target(schema, targets, &hint.table) else {
                return Err(ManifestError::validation(format!(
                    "{context} target table '{}' does not match any table in source '{schema}'",
                    hint.table
                )));
            };
            let Some(search_result_column) = source
                .columns
                .iter()
                .find(|column| column.name == hint.search_result_column)
            else {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown search_result_column '{}'",
                    hint.search_result_column
                )));
            };
            let Some(detail_filter) = target
                .filters
                .iter()
                .find(|filter| filter.name == hint.detail_filter)
            else {
                return Err(ManifestError::validation(format!(
                    "{context} target table '{}' does not declare detail_filter '{}'",
                    hint.table, hint.detail_filter
                )));
            };
            let search_result_type = search_result_column.manifest_data_type()?;
            let detail_filter_type = detail_filter.manifest_data_type()?;
            if search_result_type != detail_filter_type {
                return Err(ManifestError::validation(format!(
                    "{context} search_result_column '{}' type '{}' does not match target table '{}' detail_filter '{}' type '{}'",
                    hint.search_result_column,
                    search_result_column.data_type,
                    hint.table,
                    hint.detail_filter,
                    detail_filter.data_type
                )));
            }
        }
    }

    Ok(())
}

fn resolve_detail_hint_target<'a>(
    schema: &str,
    targets: &'a [DetailHintTargetTable<'a>],
    hint_table: &str,
) -> Option<&'a DetailHintTargetTable<'a>> {
    let qualified_prefix = format!("{schema}.");
    let unqualified = hint_table
        .strip_prefix(&qualified_prefix)
        .unwrap_or(hint_table);

    targets.iter().find(|target| target.name == unqualified)
}

fn validate_search_metadata(
    schema: &str,
    table: &str,
    require_search_limits: bool,
    search_limits: Option<&SearchLimitsSpec>,
    detail_hints: &[DetailHintSpec],
    columns: &[ColumnSpec],
) -> Result<()> {
    if require_search_limits && search_limits.is_none() {
        return Err(ManifestError::validation(format!(
            "{schema}.{table} is a search surface and must define search_limits"
        )));
    }
    if let Some(limits) = search_limits {
        validate_search_limits(limits, &format!("{schema}.{table} search_limits"))?;
    }
    validate_detail_hints(
        detail_hints,
        columns,
        &format!("{schema}.{table} detail_hints"),
    )
}

fn validate_search_limits(limits: &SearchLimitsSpec, context: &str) -> Result<()> {
    if limits.default_top_k == 0 {
        return Err(ManifestError::validation(format!(
            "{context}.default_top_k must be > 0"
        )));
    }
    if limits.max_top_k == 0 {
        return Err(ManifestError::validation(format!(
            "{context}.max_top_k must be > 0"
        )));
    }
    if limits.max_top_k > MAX_SEARCH_TOP_K {
        return Err(ManifestError::validation(format!(
            "{context}.max_top_k must be <= {MAX_SEARCH_TOP_K}"
        )));
    }
    if limits.default_top_k > limits.max_top_k {
        return Err(ManifestError::validation(format!(
            "{context}.default_top_k must be <= max_top_k"
        )));
    }
    if limits.max_calls_per_query == 0 {
        return Err(ManifestError::validation(format!(
            "{context}.max_calls_per_query must be > 0"
        )));
    }
    if limits.max_calls_per_query > MAX_SEARCH_CALLS_PER_QUERY {
        return Err(ManifestError::validation(format!(
            "{context}.max_calls_per_query must be <= {MAX_SEARCH_CALLS_PER_QUERY}"
        )));
    }
    let Some(candidate_budget) = limits.max_top_k.checked_mul(limits.max_calls_per_query) else {
        return Err(ManifestError::validation(format!(
            "{context}.max_top_k * max_calls_per_query exceeds supported range"
        )));
    };
    if candidate_budget > MAX_SEARCH_CANDIDATES_PER_QUERY {
        return Err(ManifestError::validation(format!(
            "{context}.max_top_k * max_calls_per_query must be <= {MAX_SEARCH_CANDIDATES_PER_QUERY}"
        )));
    }
    Ok(())
}

fn validate_detail_hints(
    detail_hints: &[DetailHintSpec],
    columns: &[ColumnSpec],
    context: &str,
) -> Result<()> {
    let column_names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();

    for hint in detail_hints {
        if hint.table.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "{context} must not contain an empty table"
            )));
        }
        if hint.search_result_column.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "{context} must not contain an empty search_result_column"
            )));
        }
        if !column_names.contains(hint.search_result_column.as_str()) {
            return Err(ManifestError::validation(format!(
                "{context} references unknown search_result_column '{}'",
                hint.search_result_column
            )));
        }
        if hint.detail_filter.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "{context} must not contain an empty detail_filter"
            )));
        }
        if hint.purpose.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "{context} must not contain an empty purpose"
            )));
        }
    }

    Ok(())
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
        col.manifest_data_type().map_err(|error| {
            ManifestError::validation(format!(
                "{schema}.{table} column '{}' has invalid type '{}': {error}",
                col.name, col.data_type
            ))
        })?;
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
                if let Some(arg) = &field.when_arg {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table_name} request body path '{}' uses function argument condition '{arg}' outside a function request",
                        field.path.join(".")
                    )));
                }
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
        | ValueSourceSpec::ArgBool { key, .. }
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. } => {
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
                if let Some(arg) = &field.when_arg
                    && !request_arg_names.contains(arg.as_str())
                {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' function '{}' request body path '{}' references unknown request arg '{arg}' in when_arg",
                        function.name,
                        field.path.join(".")
                    )));
                }
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
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. }
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

fn validate_expr(
    expr: &ExprSpec,
    known_filters: &HashSet<String>,
    known_args: &HashSet<&str>,
    context: &str,
) -> Result<()> {
    match expr {
        ExprSpec::FromFilter { key } if !known_filters.contains(key) => {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{key}'"
            )));
        }
        ExprSpec::FromArg { key } if !known_args.contains(key.as_str()) => {
            return Err(ManifestError::validation(format!(
                "{context} references unknown request arg '{key}'"
            )));
        }
        ExprSpec::Coalesce { exprs } => {
            for nested in exprs {
                validate_expr(nested, known_filters, known_args, context)?;
            }
        }
        ExprSpec::IfPresent { check, .. } => {
            validate_expr(check, known_filters, known_args, context)?;
        }
        ExprSpec::ObjectFilterPath { filter_key, .. } if !known_filters.contains(filter_key) => {
            return Err(ManifestError::validation(format!(
                "{context} references unknown filter '{filter_key}'"
            )));
        }
        ExprSpec::FormatTimestamp { expr, .. } | ExprSpec::Base64Decode { expr } => {
            validate_expr(expr, known_filters, known_args, context)?;
        }
        ExprSpec::Replace { expr, from, .. } => {
            if from.is_empty() {
                return Err(ManifestError::validation(format!(
                    "{context} has replace expression with empty 'from' value"
                )));
            }
            validate_expr(expr, known_filters, known_args, context)?;
        }
        ExprSpec::Template { template, values } => {
            for (key, value_expr) in values {
                validate_expr(
                    value_expr,
                    known_filters,
                    known_args,
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
        validate_columns, validate_filters_and_column_exprs, validate_http_function,
        validate_http_function_names, validate_http_table, validate_table_names,
    };
    use crate::common::{
        ColumnSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding,
        MAX_SEARCH_CANDIDATES_PER_QUERY, MAX_SEARCH_TOP_K, PaginationSpec, QueryParamSpec,
        RequestRouteSpec, RequestSpec, SearchLimitsSpec, SourceTableFunctionKind,
        SourceTableFunctionSpec, TableFunctionArgSpec, ValueSourceSpec,
    };
    use crate::parse_source_manifest_value;
    use crate::template::ParsedTemplate;
    use serde_json::{Value, json};

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
            data_type: "Utf8".to_string(),
            required: false,
            mode: FilterMode::Equality,
            description: String::new(),
        }]
    }

    fn column_with_expr(expr: ExprSpec) -> ColumnSpec {
        let mut column = test_column();
        column.expr = Some(expr);
        column
    }

    #[test]
    fn validate_columns_rejects_invalid_column_type() {
        let mut column = test_column();
        column.data_type = "Banana".to_string();

        let error = validate_columns(&[column], "demo", "messages")
            .expect_err("column types should be validated");

        assert!(
            error
                .to_string()
                .contains("demo.messages column 'id' has invalid type 'Banana'"),
            "unexpected error: {error}"
        );
    }

    fn base_request() -> RequestSpec {
        RequestSpec {
            path: ParsedTemplate::parse("/messages").expect("request path"),
            ..RequestSpec::default()
        }
    }

    fn table_detail_hint_manifest(target_table: &str, detail_filter: &str) -> Value {
        json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "tables": [
                {
                    "name": "search",
                    "description": "Search candidates",
                    "filters": [{ "name": "query", "mode": "contains" }],
                    "search_limits": {
                        "default_top_k": 10,
                        "max_top_k": 100,
                        "max_calls_per_query": 1
                    },
                    "detail_hints": [{
                        "table": target_table,
                        "search_result_column": "id",
                        "detail_filter": detail_filter,
                        "purpose": "Fetch full item details."
                    }],
                    "request": { "path": "/search" },
                    "columns": [{ "name": "id", "type": "Utf8" }]
                },
                {
                    "name": "items",
                    "description": "Item details",
                    "filters": [{ "name": "item_id", "required": true }],
                    "request": { "path": "/items/{{filter.item_id}}" },
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }
            ]
        })
    }

    fn function_detail_hint_manifest(detail_filter: &str) -> Value {
        json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "tables": [{
                "name": "items",
                "description": "Item details",
                "filters": [{ "name": "item_id", "required": true }],
                "request": { "path": "/items/{{filter.item_id}}" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }],
            "functions": [{
                "name": "search_items",
                "kind": "search",
                "search_limits": {
                    "default_top_k": 10,
                    "max_top_k": 100,
                    "max_calls_per_query": 1
                },
                "detail_hints": [{
                    "table": "demo.items",
                    "search_result_column": "id",
                    "detail_filter": detail_filter,
                    "purpose": "Fetch full item details."
                }],
                "args": [{
                    "name": "query",
                    "required": true,
                    "bind": { "arg": "query" }
                }],
                "request": {
                    "path": "/search",
                    "query": [{ "name": "q", "from": "arg", "key": "query" }]
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        })
    }

    fn function_with_request_value(value: ValueSourceSpec) -> SourceTableFunctionSpec {
        SourceTableFunctionSpec {
            name: "search".to_string(),
            kind: SourceTableFunctionKind::Table,
            description: String::new(),
            fetch_limit_default: None,
            search_limits: None,
            detail_hints: Vec::new(),
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
            None,
            &[],
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
            None,
            &[],
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
            None,
            &[],
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
            None,
            &[],
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
            ValueSourceSpec::ArgSplit {
                key: "issue_key".to_string(),
                separator: "-".to_string(),
                part: 0,
            },
            ValueSourceSpec::ArgSplitInt {
                key: "issue_key".to_string(),
                separator: "-".to_string(),
                part: 1,
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
                None,
                &[],
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
            None,
            &[],
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
    fn validate_http_function_rejects_unknown_arg_split_bindings() {
        for value in [
            ValueSourceSpec::ArgSplit {
                key: "missing".to_string(),
                separator: "-".to_string(),
                part: 0,
            },
            ValueSourceSpec::ArgSplitInt {
                key: "missing".to_string(),
                separator: "-".to_string(),
                part: 1,
            },
        ] {
            let function = function_with_request_value(value);
            let error = validate_http_function("demo", &function)
                .expect_err("function requests should reject unknown split args");

            assert!(
                error
                    .to_string()
                    .contains("references unknown request arg 'missing'"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn validate_http_function_names_rejects_table_name_collisions() {
        let function = SourceTableFunctionSpec {
            name: "messages".to_string(),
            kind: SourceTableFunctionKind::Table,
            description: String::new(),
            fetch_limit_default: None,
            search_limits: None,
            detail_hints: Vec::new(),
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
    fn validate_http_table_allows_contains_filters_without_search_limits() {
        let filters = vec![FilterSpec {
            name: "query".to_string(),
            data_type: "Utf8".to_string(),
            required: false,
            mode: FilterMode::Contains,
            description: String::new(),
        }];

        validate_http_table(
            "demo",
            "search",
            &filters,
            &[test_column()],
            &base_request(),
            &[],
            &PaginationSpec::default(),
            None,
            &[],
        )
        .expect("contains filters should not force search metadata");
    }

    #[test]
    fn validate_http_function_requires_search_limits_for_search_kind() {
        let mut function = function_with_request_value(ValueSourceSpec::Arg {
            key: "q".to_string(),
            default: None,
        });
        function.kind = SourceTableFunctionKind::Search;
        function.columns = vec![test_column()];

        let error = validate_http_function("demo", &function)
            .expect_err("search function should require bounded search metadata");

        assert!(
            error.to_string().contains("must define search_limits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_search_metadata_accepts_limits_and_detail_hints() {
        let search_limits = SearchLimitsSpec {
            default_top_k: 10,
            max_top_k: 100,
            max_calls_per_query: 1,
        };
        let detail_hints = [crate::DetailHintSpec {
            table: "demo.items".to_string(),
            search_result_column: "id".to_string(),
            detail_filter: "item_id".to_string(),
            purpose: "Fetch full item details.".to_string(),
        }];
        let filters = vec![FilterSpec {
            name: "query".to_string(),
            data_type: "Utf8".to_string(),
            required: false,
            mode: FilterMode::Contains,
            description: String::new(),
        }];

        validate_http_table(
            "demo",
            "search",
            &filters,
            &[test_column()],
            &base_request(),
            &[],
            &PaginationSpec::default(),
            Some(&search_limits),
            &detail_hints,
        )
        .expect("search metadata should validate");
    }

    #[test]
    fn validate_search_limits_rejects_default_above_max() {
        let mut function = function_with_request_value(ValueSourceSpec::Arg {
            key: "q".to_string(),
            default: None,
        });
        function.kind = SourceTableFunctionKind::Search;
        function.search_limits = Some(SearchLimitsSpec {
            default_top_k: 101,
            max_top_k: 100,
            max_calls_per_query: 1,
        });
        function.columns = vec![test_column()];

        let error = validate_http_function("demo", &function)
            .expect_err("invalid search limits should fail");

        assert!(
            error
                .to_string()
                .contains("default_top_k must be <= max_top_k"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_search_limits_rejects_max_top_k_above_cap() {
        let mut function = function_with_request_value(ValueSourceSpec::Arg {
            key: "q".to_string(),
            default: None,
        });
        function.kind = SourceTableFunctionKind::Search;
        function.search_limits = Some(SearchLimitsSpec {
            default_top_k: 10,
            max_top_k: MAX_SEARCH_TOP_K + 1,
            max_calls_per_query: 1,
        });
        function.columns = vec![test_column()];

        let error = validate_http_function("demo", &function)
            .expect_err("overlarge search top-k cap should fail");

        assert!(
            error
                .to_string()
                .contains(&format!("max_top_k must be <= {MAX_SEARCH_TOP_K}")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_search_limits_rejects_aggregate_candidate_budget_above_cap() {
        let mut function = function_with_request_value(ValueSourceSpec::Arg {
            key: "q".to_string(),
            default: None,
        });
        function.kind = SourceTableFunctionKind::Search;
        function.search_limits = Some(SearchLimitsSpec {
            default_top_k: 10,
            max_top_k: 1_000,
            max_calls_per_query: (MAX_SEARCH_CANDIDATES_PER_QUERY / 1_000) + 1,
        });
        function.columns = vec![test_column()];

        let error = validate_http_function("demo", &function)
            .expect_err("overlarge aggregate search budget should fail");

        assert!(
            error.to_string().contains(&format!(
                "max_top_k * max_calls_per_query must be <= {MAX_SEARCH_CANDIDATES_PER_QUERY}"
            )),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_detail_hints_rejects_unknown_result_column() {
        let detail_hints = [crate::DetailHintSpec {
            table: "demo.items".to_string(),
            search_result_column: "missing".to_string(),
            detail_filter: "item_id".to_string(),
            purpose: "Fetch full item details.".to_string(),
        }];

        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            &base_request(),
            &[],
            &PaginationSpec::default(),
            None,
            &detail_hints,
        )
        .expect_err("unknown detail hint result column should fail");

        assert!(
            error
                .to_string()
                .contains("references unknown search_result_column 'missing'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_detail_hints_reject_empty_fields() {
        let cases = [
            (
                "table",
                crate::DetailHintSpec {
                    table: String::new(),
                    search_result_column: "id".to_string(),
                    detail_filter: "item_id".to_string(),
                    purpose: "Fetch full item details.".to_string(),
                },
            ),
            (
                "search_result_column",
                crate::DetailHintSpec {
                    table: "demo.items".to_string(),
                    search_result_column: String::new(),
                    detail_filter: "item_id".to_string(),
                    purpose: "Fetch full item details.".to_string(),
                },
            ),
            (
                "detail_filter",
                crate::DetailHintSpec {
                    table: "demo.items".to_string(),
                    search_result_column: "id".to_string(),
                    detail_filter: String::new(),
                    purpose: "Fetch full item details.".to_string(),
                },
            ),
            (
                "purpose",
                crate::DetailHintSpec {
                    table: "demo.items".to_string(),
                    search_result_column: "id".to_string(),
                    detail_filter: "item_id".to_string(),
                    purpose: String::new(),
                },
            ),
        ];

        for (field_name, detail_hint) in cases {
            let error = validate_http_table(
                "demo",
                "messages",
                &test_filters(),
                &[test_column()],
                &base_request(),
                &[],
                &PaginationSpec::default(),
                None,
                &[detail_hint],
            )
            .expect_err("empty detail hint fields should fail");

            assert!(
                error.to_string().contains(&format!("empty {field_name}")),
                "unexpected error for {field_name}: {error}"
            );
        }
    }

    #[test]
    fn parse_http_manifest_accepts_qualified_detail_hint_target() {
        parse_source_manifest_value(table_detail_hint_manifest("demo.items", "item_id"))
            .expect("qualified detail hint target should validate");
    }

    #[test]
    fn parse_http_manifest_accepts_unqualified_detail_hint_target() {
        parse_source_manifest_value(table_detail_hint_manifest("items", "item_id"))
            .expect("unqualified same-source detail hint target should validate");
    }

    #[test]
    fn parse_http_manifest_rejects_detail_hint_unknown_target_table() {
        let error =
            parse_source_manifest_value(table_detail_hint_manifest("demo.missing", "item_id"))
                .expect_err("unknown target table should fail");

        assert!(
            error
                .to_string()
                .contains("target table 'demo.missing' does not match any table"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_http_manifest_rejects_detail_hint_unknown_target_filter() {
        let error =
            parse_source_manifest_value(table_detail_hint_manifest("demo.items", "missing_filter"))
                .expect_err("unknown target filter should fail");

        assert!(
            error.to_string().contains(
                "target table 'demo.items' does not declare detail_filter 'missing_filter'"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_http_manifest_rejects_detail_hint_type_mismatch() {
        let mut manifest = table_detail_hint_manifest("demo.items", "item_id");
        let tables = manifest
            .get_mut("tables")
            .and_then(Value::as_array_mut)
            .expect("manifest tables");
        let detail_table = tables.get_mut(1).expect("detail table");
        let filters = detail_table
            .get_mut("filters")
            .and_then(Value::as_array_mut)
            .expect("detail filters");
        let detail_filter = filters.get_mut(0).expect("detail filter");
        detail_filter
            .as_object_mut()
            .expect("detail filter object")
            .insert("type".to_string(), json!("Int64"));

        let error = parse_source_manifest_value(manifest)
            .expect_err("detail hint type mismatch should fail");

        assert!(
            error.to_string().contains(
                "search_result_column 'id' type 'Utf8' does not match target table 'demo.items' detail_filter 'item_id' type 'Int64'"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_http_manifest_rejects_function_detail_hint_unknown_target_filter() {
        let error = parse_source_manifest_value(function_detail_hint_manifest("missing_filter"))
            .expect_err("unknown function detail target filter should fail");

        assert!(
            error.to_string().contains(
                "target table 'demo.items' does not declare detail_filter 'missing_filter'"
            ),
            "unexpected error: {error}"
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

    #[test]
    fn validate_base64_decode_propagates_inner_expr_errors() {
        let column = column_with_expr(ExprSpec::Base64Decode {
            expr: Box::new(ExprSpec::FromFilter {
                key: "missing".to_string(),
            }),
        });

        let error =
            validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
                .expect_err("unknown filter in base64_decode should fail");

        assert!(
            error
                .to_string()
                .contains("references unknown filter 'missing'")
        );
    }
}
