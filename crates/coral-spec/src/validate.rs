//! Shared manifest validation helpers.

use std::collections::{HashMap, HashSet};

use crate::common::{
    BodySpec, ColumnSpec, DetailHintSpec, ExprSpec, FilterSpec, MAX_SEARCH_CALLS_PER_QUERY,
    MAX_SEARCH_CANDIDATES_PER_QUERY, MAX_SEARCH_TOP_K, PaginationSpec, RequestRouteSpec,
    RequestSpec, SearchLimitsSpec, SourceTableFunctionKind, SourceTableFunctionSpec,
    TableFunctionArgSpec, ValueSourceSpec,
};
use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclaredRelationKind {
    Table,
    Function,
}

impl DeclaredRelationKind {
    fn validate_name(self, source_name: &str, name: &str) -> Result<()> {
        match self {
            Self::Table => {
                if name.trim().is_empty() {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' table name must not be empty"
                    )));
                }
                Ok(())
            }
            Self::Function => {
                validate_identifier(name, &format!("source '{source_name}' function name"))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeclaredRelation<'a> {
    kind: DeclaredRelationKind,
    name: &'a str,
}

impl<'a> DeclaredRelation<'a> {
    pub(crate) fn table(name: &'a str) -> Self {
        Self {
            kind: DeclaredRelationKind::Table,
            name,
        }
    }

    pub(crate) fn function(name: &'a str) -> Self {
        Self {
            kind: DeclaredRelationKind::Function,
            name,
        }
    }
}

pub(crate) fn validate_declared_relation_namespace<'a>(
    source_name: &str,
    relations: impl IntoIterator<Item = DeclaredRelation<'a>>,
) -> Result<()> {
    let mut namespace = HashMap::new();
    for relation in relations {
        relation.kind.validate_name(source_name, relation.name)?;

        let key = relation.name.to_ascii_lowercase();
        if let Some(previous_kind) = namespace.get(&key) {
            return match (*previous_kind, relation.kind) {
                (DeclaredRelationKind::Table, DeclaredRelationKind::Table) => {
                    Err(ManifestError::validation(format!(
                        "source '{source_name}' table '{}' is declared more than once",
                        relation.name
                    )))
                }
                (DeclaredRelationKind::Function, DeclaredRelationKind::Function) => {
                    Err(ManifestError::validation(format!(
                        "source '{source_name}' function '{}' is declared more than once",
                        relation.name
                    )))
                }
                _ => Err(ManifestError::validation(format!(
                    "source '{source_name}' declares both a table and function named '{}'",
                    relation.name
                ))),
            };
        }
        namespace.insert(key, relation.kind);
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
    let table_context = format!("{schema}.{table_name}");
    if request.path.raw().trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "{table_context} has an empty request.path"
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

    validate_request_bindings(
        &table_context,
        request,
        RequestBindingScope::Table(&known_filters),
    )?;

    for route in requests {
        for filter_name in &route.when_filters {
            if !known_filters.contains(filter_name) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table_name} requests.when_filters references unknown filter '{filter_name}'"
                )));
            }
        }
        validate_request_bindings(
            &table_context,
            &route.request,
            RequestBindingScope::Table(&known_filters),
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

pub(crate) fn validate_http_function(
    source_name: &str,
    function: &SourceTableFunctionSpec,
) -> Result<()> {
    validate_identifier(
        &function.name,
        &format!("source '{source_name}' function name"),
    )?;

    let request_arg_names =
        validate_table_function_args(source_name, &function.name, &function.args, "request arg")?;

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
    context: &str,
    request: &RequestSpec,
    scope: RequestBindingScope<'_, '_>,
) -> Result<()> {
    validate_request_template(&request.path, scope, context)?;

    for header in &request.headers {
        validate_request_value_source(
            &header.value,
            scope,
            &format!("{context} request header '{}'", header.name),
        )?;
    }

    for param in &request.query {
        validate_request_value_source(
            &param.value,
            scope,
            &format!("{context} query param '{}'", param.name),
        )?;
    }

    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                validate_when_arg(scope, field.when_arg.as_ref(), context, &field.path)?;
                validate_request_value_source(
                    &field.value,
                    scope,
                    &format!("{context} request body path '{}'", field.path.join(".")),
                )?;
            }
        }
        BodySpec::Text { content } => {
            validate_request_value_source(content, scope, &format!("{context} request body text"))?;
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum RequestBindingScope<'a, 'b> {
    Table(&'a HashSet<String>),
    Function(&'a HashSet<&'b str>),
}

fn validate_when_arg(
    scope: RequestBindingScope<'_, '_>,
    when_arg: Option<&String>,
    context: &str,
    path: &[String],
) -> Result<()> {
    let Some(arg) = when_arg else {
        return Ok(());
    };
    match scope {
        RequestBindingScope::Table(_) => Err(ManifestError::validation(format!(
            "{context} request body path '{}' uses function argument condition '{arg}' outside a function request",
            path.join(".")
        ))),
        RequestBindingScope::Function(request_arg_names)
            if !request_arg_names.contains(arg.as_str()) =>
        {
            Err(ManifestError::validation(format!(
                "{context} request body path '{}' references unknown request arg '{arg}' in when_arg",
                path.join(".")
            )))
        }
        RequestBindingScope::Function(_) => Ok(()),
    }
}

fn validate_request_value_source(
    source: &ValueSourceSpec,
    scope: RequestBindingScope<'_, '_>,
    context: &str,
) -> Result<()> {
    if let Some(key) = filter_key(source) {
        match scope {
            RequestBindingScope::Table(known_filters) if !known_filters.contains(key) => {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown filter '{key}'"
                )));
            }
            RequestBindingScope::Function(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses table filter '{key}' inside a function request"
                )));
            }
            RequestBindingScope::Table(_) => {}
        }
    }
    if let Some(key) = arg_key(source) {
        match scope {
            RequestBindingScope::Function(request_arg_names)
                if !request_arg_names.contains(key) =>
            {
                return Err(ManifestError::validation(format!(
                    "{context} references unknown request arg '{key}'"
                )));
            }
            RequestBindingScope::Table(_) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses function argument '{key}' outside a function request"
                )));
            }
            RequestBindingScope::Function(_) => {}
        }
    }

    match source {
        ValueSourceSpec::Template { template } => {
            validate_request_template(template, scope, context)?;
        }
        ValueSourceSpec::OneOf { values } => {
            if values.is_empty() {
                return Err(ManifestError::validation(format!(
                    "{context} one_of values must not be empty"
                )));
            }
            for (index, value) in values.iter().enumerate() {
                validate_request_value_source(
                    value,
                    scope,
                    &format!("{context} one_of values[{index}]"),
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn filter_key(source: &ValueSourceSpec) -> Option<&str> {
    match source {
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => Some(key),
        _ => None,
    }
}

fn arg_key(source: &ValueSourceSpec) -> Option<&str> {
    match source {
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. }
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. } => Some(key),
        _ => None,
    }
}

pub(crate) fn validate_table_function_args<'a>(
    source_name: &str,
    function_name: &str,
    args: &'a [TableFunctionArgSpec],
    binding_label: &str,
) -> Result<HashSet<&'a str>> {
    let mut arg_names = HashSet::new();
    let mut request_arg_names = HashSet::new();

    for arg in args {
        validate_identifier(
            &arg.name,
            &format!("source '{source_name}' function '{function_name}' argument"),
        )?;
        if !arg_names.insert(arg.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{function_name}' argument '{}' is declared more than once",
                arg.name
            )));
        }
        validate_unique_values(
            &arg.values,
            &format!(
                "source '{source_name}' function '{function_name}' argument '{}'",
                arg.name
            ),
        )?;
        if !request_arg_names.insert(arg.bind.arg.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{function_name}' has multiple bindings for {binding_label} '{}'",
                arg.bind.arg
            )));
        }
    }

    Ok(request_arg_names)
}

fn validate_function_request_bindings(
    source_name: &str,
    function: &SourceTableFunctionSpec,
    request_arg_names: &HashSet<&str>,
) -> Result<()> {
    let function_context = format!("source '{source_name}' function '{}'", function.name);
    if function.request.path.raw().trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "{function_context} has an empty request.path"
        )));
    }

    validate_request_bindings(
        &function_context,
        &function.request,
        RequestBindingScope::Function(request_arg_names),
    )
}

fn validate_request_template(
    template: &ParsedTemplate,
    scope: RequestBindingScope<'_, '_>,
    context: &str,
) -> Result<()> {
    for token in template.tokens() {
        match (scope, token.namespace()) {
            (RequestBindingScope::Table(known_filters), TemplateNamespace::Filter) => {
                if !known_filters.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown filter '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            (RequestBindingScope::Table(_), TemplateNamespace::Arg) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses function argument token '{}' outside a function request",
                    token.raw()
                )));
            }
            (
                RequestBindingScope::Table(_),
                TemplateNamespace::Expr | TemplateNamespace::Other(_),
            ) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported template token '{}'",
                    token.raw()
                )));
            }
            (RequestBindingScope::Function(request_arg_names), TemplateNamespace::Arg) => {
                if !request_arg_names.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown request arg '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            (
                RequestBindingScope::Function(_),
                TemplateNamespace::Filter | TemplateNamespace::Expr | TemplateNamespace::Other(_),
            ) => {
                return Err(ManifestError::validation(format!(
                    "{context} uses unsupported function request template token '{}'",
                    token.raw()
                )));
            }
            (_, TemplateNamespace::Input | TemplateNamespace::State) => {}
        }
    }
    Ok(())
}

pub(crate) fn validate_identifier(value: &str, context: &str) -> Result<()> {
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
    validate_request_template(template, RequestBindingScope::Table(known_filters), context)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        DeclaredRelation, validate_columns, validate_declared_relation_namespace,
        validate_filters_and_column_exprs, validate_http_function, validate_http_table,
    };
    use crate::common::{
        ColumnSpec, DetailHintSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding,
        MAX_SEARCH_CANDIDATES_PER_QUERY, MAX_SEARCH_TOP_K, PaginationSpec, QueryParamSpec,
        RequestRouteSpec, RequestSpec, SearchLimitsSpec, SourceTableFunctionKind,
        SourceTableFunctionSpec, TableFunctionArgSpec, ValueSourceSpec,
    };
    use crate::parse_source_manifest_value;
    use crate::template::ParsedTemplate;
    use crate::test_support::{assert_error_contains, insert_field};
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
        vec![filter_spec("id", FilterMode::Equality)]
    }

    fn filter_spec(name: &str, mode: FilterMode) -> FilterSpec {
        FilterSpec {
            name: name.to_string(),
            data_type: "Utf8".to_string(),
            required: false,
            mode,
            description: String::new(),
        }
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

        assert_error_contains(
            &error,
            "demo.messages column 'id' has invalid type 'Banana'",
        );
    }

    fn base_request() -> RequestSpec {
        RequestSpec {
            path: ParsedTemplate::parse("/messages").expect("request path"),
            ..RequestSpec::default()
        }
    }

    fn request_with_query_value(name: &str, value: ValueSourceSpec) -> RequestSpec {
        RequestSpec {
            query: vec![QueryParamSpec {
                name: name.to_string(),
                value,
            }],
            ..base_request()
        }
    }

    fn input(key: &str) -> ValueSourceSpec {
        ValueSourceSpec::Input {
            key: key.to_string(),
        }
    }

    macro_rules! value_source {
        ($variant:ident, $key:expr) => {
            ValueSourceSpec::$variant {
                key: $key.to_string(),
                default: None,
            }
        };
        ($variant:ident, $key:expr, $separator:expr, $part:expr) => {
            ValueSourceSpec::$variant {
                key: $key.to_string(),
                separator: $separator.to_string(),
                part: $part,
            }
        };
    }

    fn expect_table_validation_error(
        request: &RequestSpec,
        routes: &[RequestRouteSpec],
        expectation: &str,
        expected: &str,
    ) {
        let error = validate_http_table(
            "demo",
            "messages",
            &test_filters(),
            &[test_column()],
            request,
            routes,
            &PaginationSpec::default(),
            None,
            &[],
        )
        .expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    fn expect_table_request_value_error(value: ValueSourceSpec, expected: &str) {
        expect_table_validation_error(
            &request_with_query_value("value", value),
            &[],
            "table request value source should fail",
            expected,
        );
    }

    fn expect_function_validation_error(
        function: &SourceTableFunctionSpec,
        expectation: &str,
        expected: &str,
    ) {
        let error = validate_http_function("demo", function).expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    fn expect_function_request_value_error(value: ValueSourceSpec, expected: &str) {
        expect_function_validation_error(
            &function_with_request_value(value),
            "function request value source should fail",
            expected,
        );
    }

    fn expect_column_expr_error(expr: ExprSpec, expectation: &str, expected: &str) {
        let column = column_with_expr(expr);
        let error =
            validate_filters_and_column_exprs(&test_filters(), &[column], "demo", "messages")
                .expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    fn expect_detail_hint_table_error(
        detail_hint: DetailHintSpec,
        expectation: &str,
        expected: &str,
    ) {
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
        .expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    fn expect_manifest_error(manifest: Value, expectation: &str, expected: &str) {
        let error = parse_source_manifest_value(manifest).expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    const DETAIL_PURPOSE: &str = "Fetch full item details.";

    fn search_limits() -> SearchLimitsSpec {
        SearchLimitsSpec {
            default_top_k: 10,
            max_top_k: 100,
            max_calls_per_query: 1,
        }
    }

    fn search_limits_json() -> Value {
        json!(search_limits())
    }

    fn detail_hint_spec(
        table: &str,
        search_result_column: &str,
        detail_filter: &str,
        purpose: &str,
    ) -> DetailHintSpec {
        DetailHintSpec {
            table: table.to_string(),
            search_result_column: search_result_column.to_string(),
            detail_filter: detail_filter.to_string(),
            purpose: purpose.to_string(),
        }
    }

    fn detail_hint_json(table: &str, detail_filter: &str) -> Value {
        json!(detail_hint_spec(table, "id", detail_filter, DETAIL_PURPOSE))
    }

    fn http_manifest(fields: Value) -> Value {
        let Value::Object(mut manifest) = json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
        }) else {
            unreachable!("base HTTP manifest fixture is an object");
        };
        let Value::Object(fields) = fields else {
            unreachable!("HTTP manifest fixture overrides must be an object");
        };
        manifest.extend(fields);
        Value::Object(manifest)
    }

    fn detail_items_table() -> Value {
        json!({
            "name": "items",
            "description": "Item details",
            "filters": [{ "name": "item_id", "required": true }],
            "request": { "path": "/items/{{filter.item_id}}" },
            "columns": [{ "name": "id", "type": "Utf8" }]
        })
    }

    fn table_detail_hint_manifest(target_table: &str, detail_filter: &str) -> Value {
        http_manifest(json!({
            "tables": [
                {
                    "name": "search",
                    "description": "Search candidates",
                    "filters": [{ "name": "query", "mode": "contains" }],
                    "search_limits": search_limits_json(),
                    "detail_hints": [detail_hint_json(target_table, detail_filter)],
                    "request": { "path": "/search" },
                    "columns": [{ "name": "id", "type": "Utf8" }]
                },
                detail_items_table()
            ]
        }))
    }

    fn function_detail_hint_manifest(detail_filter: &str) -> Value {
        http_manifest(json!({
            "tables": [detail_items_table()],
            "functions": [{
                "name": "search_items",
                "kind": "search",
                "search_limits": search_limits_json(),
                "detail_hints": [detail_hint_json("demo.items", detail_filter)],
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
        }))
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

    fn search_function(search_limits: Option<SearchLimitsSpec>) -> SourceTableFunctionSpec {
        let mut function = function_with_request_value(value_source!(Arg, "q"));
        function.kind = SourceTableFunctionKind::Search;
        function.search_limits = search_limits;
        function.columns = vec![test_column()];
        function
    }

    #[test]
    fn validate_declared_relation_namespace_rejects_duplicate_tables_that_differ_only_by_case() {
        let relations = [
            DeclaredRelation::table("issues"),
            DeclaredRelation::table("prs"),
            DeclaredRelation::table("Issues"),
        ];

        let error = validate_declared_relation_namespace("github", relations)
            .expect_err("expected duplicate table to be rejected");

        assert_error_contains(
            &error,
            "source 'github' table 'Issues' is declared more than once",
        );
    }

    #[test]
    fn validate_declared_relation_namespace_rejects_duplicate_functions_that_differ_only_by_case() {
        let relations = [
            DeclaredRelation::function("search"),
            DeclaredRelation::function("Search"),
        ];

        let error = validate_declared_relation_namespace("github", relations)
            .expect_err("expected duplicate function to be rejected");

        assert!(
            error
                .to_string()
                .contains("source 'github' function 'Search' is declared more than once")
        );
    }

    #[test]
    fn validate_declared_relation_namespace_rejects_table_function_case_collisions() {
        let relations = [
            DeclaredRelation::table("Messages"),
            DeclaredRelation::function("messages"),
        ];

        let error = validate_declared_relation_namespace("demo", relations)
            .expect_err("expected table/function collision to be rejected");

        assert!(
            error
                .to_string()
                .contains("source 'demo' declares both a table and function named 'messages'")
        );
    }

    #[test]
    fn validate_declared_relation_namespace_reports_earlier_collisions_first() {
        let relations = [
            DeclaredRelation::table("issues"),
            DeclaredRelation::function("issues"),
            DeclaredRelation::function("bad-name"),
        ];

        let error = validate_declared_relation_namespace("demo", relations)
            .expect_err("expected first namespace collision to be rejected");

        assert_eq!(
            error.to_string(),
            "source 'demo' declares both a table and function named 'issues'"
        );
    }

    #[test]
    fn validate_declared_relation_namespace_allows_quoted_sql_table_names() {
        let relations = [
            DeclaredRelation::table("player.stats"),
            DeclaredRelation::table("message-events"),
            DeclaredRelation::function("search"),
        ];

        validate_declared_relation_namespace("demo", relations)
            .expect("table names that require SQL quoting should remain valid");
    }

    #[test]
    fn validate_declared_relation_namespace_rejects_empty_table_names() {
        let relations = [DeclaredRelation::table("  ")];

        let error = validate_declared_relation_namespace("demo", relations)
            .expect_err("expected empty table name to be rejected");

        assert_eq!(
            error.to_string(),
            "source 'demo' table name must not be empty"
        );
    }

    #[test]
    fn validate_declared_relation_namespace_rejects_invalid_function_names() {
        let relations = [DeclaredRelation::function("1search")];

        let error = validate_declared_relation_namespace("demo", relations)
            .expect_err("expected invalid function name to be rejected");

        assert!(error.to_string().contains(
            "source 'demo' function name '1search' must start with a letter or underscore"
        ));
    }

    #[test]
    fn http_manifest_rejects_table_function_names_that_differ_only_by_case() {
        let error = parse_source_manifest_value(json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "tables": [{
                "name": "Messages",
                "description": "Messages",
                "request": { "path": "/messages" }
            }],
            "functions": [{
                "name": "messages",
                "request": { "path": "/messages/search" }
            }]
        }))
        .expect_err("HTTP table/function case collision should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' declares both a table and function named 'messages'"
        );
    }

    #[test]
    fn http_manifest_rejects_duplicate_function_names_that_differ_only_by_case() {
        let error = parse_source_manifest_value(json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "functions": [
                {
                    "name": "Search",
                    "request": { "path": "/search" }
                },
                {
                    "name": "search",
                    "request": { "path": "/search" }
                }
            ]
        }))
        .expect_err("HTTP function case duplicate should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' function 'search' is declared more than once"
        );
    }

    #[test]
    fn http_backend_accepts_quoted_sql_table_names() {
        crate::backends::http::HttpSourceManifest::parse_manifest_value(json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "tables": [{
                "name": "message-events",
                "description": "Events",
                "request": { "path": "/events" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("HTTP table names that require SQL quoting should remain valid");
    }

    #[test]
    fn file_backend_accepts_quoted_sql_table_names() {
        crate::backends::file::FileSourceManifest::parse_manifest_value(json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "file",
            "tables": [{
                "name": "message-events",
                "description": "Events",
                "format": "jsonl",
                "source": { "location": "file:///tmp/coral/events/" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("file table names that require SQL quoting should remain valid");
    }

    #[test]
    fn mcp_backend_accepts_quoted_sql_table_names() {
        crate::backends::mcp::McpSourceManifest::parse_manifest_value(json!({
            "name": "demo",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "message-events",
                "tool": "list_events",
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("MCP table names that require SQL quoting should remain valid");
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_bindings() {
        for (name, request, routes) in [
            (
                "default request",
                request_with_query_value("user_id", value_source!(Filter, "missing")),
                Vec::new(),
            ),
            (
                "route request",
                base_request(),
                vec![RequestRouteSpec {
                    when_filters: vec!["id".to_string()],
                    request: request_with_query_value("cursor", value_source!(Filter, "missing")),
                }],
            ),
            (
                "filter_split",
                request_with_query_value("team_key", value_source!(FilterSplit, "missing", "-", 0)),
                Vec::new(),
            ),
            (
                "filter_split_int",
                request_with_query_value(
                    "issue_number",
                    value_source!(FilterSplitInt, "missing", "-", 1),
                ),
                Vec::new(),
            ),
        ] {
            expect_table_validation_error(
                &request,
                &routes,
                name,
                "references unknown filter 'missing'",
            );
        }
    }

    #[test]
    fn validate_http_table_rejects_function_arg_value_sources() {
        let cases = [
            value_source!(Arg, "query"),
            value_source!(ArgInt, "limit"),
            value_source!(ArgBool, "archived"),
            value_source!(ArgSplit, "issue_key", "-", 0),
            value_source!(ArgSplitInt, "issue_key", "-", 1),
        ];

        for value in cases {
            expect_table_request_value_error(value, "uses function argument");
        }
    }

    #[test]
    fn validate_http_table_rejects_function_arg_template_tokens() {
        let request = RequestSpec {
            path: ParsedTemplate::parse("/search/{{arg.q}}").expect("template"),
            ..base_request()
        };

        expect_table_validation_error(
            &request,
            &[],
            "table request templates should reject function arguments",
            "uses function argument token 'arg.q' outside a function request",
        );
    }

    #[test]
    fn validate_http_table_rejects_function_arg_one_of_value_sources() {
        expect_table_request_value_error(
            ValueSourceSpec::OneOf {
                values: vec![input("API_KEY"), value_source!(Arg, "q")],
            },
            "uses function argument 'q' outside a function request",
        );
    }

    #[test]
    fn validate_http_table_rejects_unknown_filter_one_of_value_sources() {
        expect_table_request_value_error(
            ValueSourceSpec::OneOf {
                values: vec![input("API_KEY"), value_source!(Filter, "missing")],
            },
            "references unknown filter 'missing'",
        );
    }

    #[test]
    fn validate_http_function_rejects_table_filter_value_sources() {
        let cases = [
            value_source!(Filter, "q"),
            value_source!(FilterInt, "limit"),
            value_source!(FilterBool, "archived"),
            value_source!(FilterSplit, "repo", "/", 0),
            value_source!(FilterSplitInt, "issue_key", "-", 1),
        ];

        for value in cases {
            expect_function_request_value_error(value, "uses table filter");
        }
    }

    #[test]
    fn validate_http_function_rejects_unknown_arg_split_bindings() {
        for value in [
            value_source!(ArgSplit, "missing", "-", 0),
            value_source!(ArgSplitInt, "missing", "-", 1),
        ] {
            expect_function_request_value_error(value, "references unknown request arg 'missing'");
        }
    }

    #[test]
    fn validate_http_function_accepts_arg_one_of_value_sources() {
        let function = function_with_request_value(ValueSourceSpec::OneOf {
            values: vec![value_source!(Arg, "q"), input("API_KEY")],
        });

        validate_http_function("demo", &function)
            .expect("function request one_of should accept declared args");
    }

    #[test]
    fn validate_http_function_rejects_unknown_arg_one_of_value_sources() {
        let function = function_with_request_value(ValueSourceSpec::OneOf {
            values: vec![value_source!(Arg, "missing"), input("API_KEY")],
        });

        expect_function_validation_error(
            &function,
            "function request one_of should reject unknown args",
            "references unknown request arg 'missing'",
        );
    }

    #[test]
    fn validate_http_table_allows_contains_filters_without_search_limits() {
        let filters = vec![filter_spec("query", FilterMode::Contains)];

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
        expect_function_validation_error(
            &search_function(None),
            "search function should require bounded search metadata",
            "must define search_limits",
        );
    }

    #[test]
    fn validate_search_metadata_accepts_limits_and_detail_hints() {
        let search_limits = search_limits();
        let detail_hints = [detail_hint_spec(
            "demo.items",
            "id",
            "item_id",
            DETAIL_PURPOSE,
        )];
        let filters = vec![filter_spec("query", FilterMode::Contains)];

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
    fn validate_search_limits_reject_invalid_bounds() {
        for (name, limits, expected) in [
            (
                "default_top_k above max_top_k",
                SearchLimitsSpec {
                    default_top_k: 101,
                    max_top_k: 100,
                    max_calls_per_query: 1,
                },
                "default_top_k must be <= max_top_k".to_string(),
            ),
            (
                "max_top_k above cap",
                SearchLimitsSpec {
                    default_top_k: 10,
                    max_top_k: MAX_SEARCH_TOP_K + 1,
                    max_calls_per_query: 1,
                },
                format!("max_top_k must be <= {MAX_SEARCH_TOP_K}"),
            ),
            (
                "aggregate candidate budget above cap",
                SearchLimitsSpec {
                    default_top_k: 10,
                    max_top_k: 1_000,
                    max_calls_per_query: (MAX_SEARCH_CANDIDATES_PER_QUERY / 1_000) + 1,
                },
                format!(
                    "max_top_k * max_calls_per_query must be <= {MAX_SEARCH_CANDIDATES_PER_QUERY}"
                ),
            ),
        ] {
            expect_function_validation_error(
                &search_function(Some(limits)),
                name,
                expected.as_str(),
            );
        }
    }

    #[test]
    fn validate_detail_hints_rejects_unknown_result_column() {
        expect_detail_hint_table_error(
            detail_hint_spec("demo.items", "missing", "item_id", DETAIL_PURPOSE),
            "unknown detail hint result column should fail",
            "references unknown search_result_column 'missing'",
        );
    }

    #[test]
    fn validate_detail_hints_reject_empty_fields() {
        let cases = [
            (
                "table",
                detail_hint_spec("", "id", "item_id", DETAIL_PURPOSE),
            ),
            (
                "search_result_column",
                detail_hint_spec("demo.items", "", "item_id", DETAIL_PURPOSE),
            ),
            (
                "detail_filter",
                detail_hint_spec("demo.items", "id", "", DETAIL_PURPOSE),
            ),
            (
                "purpose",
                detail_hint_spec("demo.items", "id", "item_id", ""),
            ),
        ];

        for (field_name, detail_hint) in cases {
            expect_detail_hint_table_error(
                detail_hint,
                "empty detail hint fields should fail",
                &format!("empty {field_name}"),
            );
        }
    }

    #[test]
    fn parse_http_manifest_accepts_detail_hint_targets() {
        for (name, target) in [
            ("qualified detail hint target", "demo.items"),
            ("unqualified same-source detail hint target", "items"),
        ] {
            parse_source_manifest_value(table_detail_hint_manifest(target, "item_id"))
                .unwrap_or_else(|error| panic!("{name} should validate: {error}"));
        }
    }

    #[test]
    fn parse_http_manifest_rejects_detail_hint_unknown_target_table() {
        expect_manifest_error(
            table_detail_hint_manifest("demo.missing", "item_id"),
            "unknown target table should fail",
            "target table 'demo.missing' does not match any table",
        );
    }

    #[test]
    fn parse_http_manifest_rejects_detail_hint_unknown_target_filter() {
        expect_manifest_error(
            table_detail_hint_manifest("demo.items", "missing_filter"),
            "unknown target filter should fail",
            "target table 'demo.items' does not declare detail_filter 'missing_filter'",
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
        insert_field(
            filters.get_mut(0).expect("detail filter"),
            "type",
            json!("Int64"),
        );

        expect_manifest_error(
            manifest,
            "detail hint type mismatch should fail",
            "search_result_column 'id' type 'Utf8' does not match target table 'demo.items' detail_filter 'item_id' type 'Int64'",
        );
    }

    #[test]
    fn parse_http_manifest_rejects_function_detail_hint_unknown_target_filter() {
        expect_manifest_error(
            function_detail_hint_manifest("missing_filter"),
            "unknown function detail target filter should fail",
            "target table 'demo.items' does not declare detail_filter 'missing_filter'",
        );
    }

    #[test]
    fn validate_http_function_rejects_filter_column_exprs() {
        let mut function = function_with_request_value(value_source!(Arg, "q"));
        function.columns = vec![column_with_expr(ExprSpec::FromFilter {
            key: "q".to_string(),
        })];

        let error = validate_http_function("demo", &function)
            .expect_err("function columns should not reference table filters");

        assert_error_contains(&error, "references unknown filter 'q'");
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
        expect_column_expr_error(
            ExprSpec::Template {
                template: ParsedTemplate::parse("{{expr.slug|unknown}}").expect("template"),
                values: HashMap::new(),
            },
            "unknown expr token should fail",
            "references unknown expr 'slug'",
        );
    }

    #[test]
    fn validate_column_template_rejects_secret_tokens() {
        expect_column_expr_error(
            ExprSpec::Template {
                template: ParsedTemplate::parse("{{secret.API_KEY}}").expect("template"),
                values: HashMap::new(),
            },
            "secret token should fail",
            "uses unsupported expr template token 'secret.API_KEY'",
        );
    }

    #[test]
    fn validate_replace_rejects_empty_from() {
        expect_column_expr_error(
            ExprSpec::Replace {
                expr: Box::new(ExprSpec::Path {
                    path: vec!["name".to_string()],
                }),
                from: String::new(),
                to: "-".to_string(),
            },
            "empty replace source should fail",
            "has replace expression with empty 'from' value",
        );
    }

    #[test]
    fn validate_base64_decode_propagates_inner_expr_errors() {
        expect_column_expr_error(
            ExprSpec::Base64Decode {
                expr: Box::new(ExprSpec::FromFilter {
                    key: "missing".to_string(),
                }),
            },
            "unknown filter in base64_decode should fail",
            "references unknown filter 'missing'",
        );
    }
}
