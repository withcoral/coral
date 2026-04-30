#![allow(
    missing_docs,
    reason = "This module defines many field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for HTTP sources.
//!
//! HTTP manifests describe request templating, response-row extraction, filter
//! binding, and pagination. These types are normalized and validated here, but
//! they are still engine-neutral; no runtime HTTP client or execution concerns
//! live in this crate.

use std::collections::{BTreeSet, HashSet};

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    ColumnSpec, FilterSpec, FunctionArgBinding, HeaderSpec, ManifestError, ManifestInputKind,
    ManifestInputSpec, PaginationSpec, ParsedTemplate, RequestRouteSpec, RequestSpec, ResponseSpec,
    Result, SourceBackend, SourceManifestCommon, SourceTableFunctionSpec, TableCommon,
    inputs::collect_source_inputs_value,
    validate::{validate_columns, validate_template, validate_unique_values},
    validate_http_table, validate_test_queries,
};

/// Source-level authentication requirements for HTTP-backed source specs.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum AuthSpec {
    /// HTTP Basic authentication; runtime base64-encodes `username:password`.
    #[serde(rename = "BasicAuth")]
    BasicAuth(BasicAuthSpec),
    /// Declarative list of auth headers to attach to the request.
    #[serde(rename = "HeaderAuth")]
    HeaderAuth(HeaderAuthSpec),
    /// Dispatches auth header resolution to a runtime-registered authenticator.
    #[serde(rename = "CustomAuth")]
    CustomAuth(CustomAuthSpec),
}

impl Default for AuthSpec {
    fn default() -> Self {
        Self::HeaderAuth(HeaderAuthSpec::default())
    }
}

/// HTTP Basic authenticator with separate username and password templates.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuthSpec {
    pub username: ParsedTemplate,
    pub password: ParsedTemplate,
}

/// Declarative authenticator that injects one or more headers.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HeaderAuthSpec {
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
}

/// Dispatches to a runtime-registered request authenticator by name.
#[derive(Debug, Clone, Deserialize)]
pub struct CustomAuthSpec {
    pub authenticator: String,
    #[serde(flatten)]
    pub config: Map<String, Value>,
}

/// Provider-specific response hints for classifying and delaying rate-limit retries.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RateLimitSpec {
    #[serde(default)]
    pub extra_statuses: Vec<u16>,
    #[serde(default)]
    pub retry_after_header: Option<String>,
    #[serde(default)]
    pub remaining_header: Option<String>,
    #[serde(default)]
    pub reset_header: Option<String>,
}

/// Validated top-level manifest for an HTTP-backed source.
#[derive(Debug, Clone)]
pub struct HttpSourceManifest {
    pub common: SourceManifestCommon,
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub request_headers: Vec<HeaderSpec>,
    pub rate_limit: RateLimitSpec,
    pub tables: Vec<HttpTableSpec>,
    pub functions: Vec<SourceTableFunctionSpec>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    backend: SourceBackend,
    #[serde(default)]
    base_url: ParsedTemplate,
    #[serde(default)]
    auth: AuthSpec,
    #[serde(default)]
    request_headers: Vec<HeaderSpec>,
    #[serde(default)]
    rate_limit: RateLimitSpec,
    #[serde(default)]
    inputs: Option<Value>,
    tables: Vec<RawHttpTableSpec>,
    #[serde(default)]
    functions: Vec<SourceTableFunctionSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpTableSpec {
    name: String,
    description: String,
    #[serde(default)]
    guide: String,
    #[serde(default)]
    filters: Vec<FilterSpec>,
    #[serde(default)]
    fetch_limit_default: Option<usize>,
    #[serde(default)]
    request: RequestSpec,
    #[serde(default)]
    requests: Vec<RequestRouteSpec>,
    #[serde(default)]
    response: ResponseSpec,
    #[serde(default)]
    pagination: PaginationSpec,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
}

/// One validated HTTP table declaration.
#[derive(Debug, Clone)]
pub struct HttpTableSpec {
    pub common: TableCommon,
    pub request: RequestSpec,
    pub requests: Vec<RequestRouteSpec>,
    pub response: ResponseSpec,
    pub pagination: PaginationSpec,
}

impl HttpTableSpec {
    #[must_use]
    /// Returns the stable table name.
    pub fn name(&self) -> &str {
        &self.common.name
    }

    #[must_use]
    /// Returns the declared SQL filters that may influence request selection.
    pub fn filters(&self) -> &[FilterSpec] {
        &self.common.filters
    }

    #[must_use]
    /// Returns the declared output columns for this table.
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.common.columns
    }

    #[must_use]
    /// Returns the default fetch limit declared by the manifest, if any.
    pub fn fetch_limit_default(&self) -> Option<usize> {
        self.common.fetch_limit_default
    }

    #[must_use]
    /// Selects the most specific request route that matches the provided
    /// filter set, or falls back to the default request.
    pub fn resolve_request(&self, provided_filters: &HashSet<String>) -> &RequestSpec {
        let mut best_match: Option<&RequestRouteSpec> = None;
        let mut best_specificity = 0usize;

        for route in &self.requests {
            if route
                .when_filters
                .iter()
                .all(|f| provided_filters.contains(f))
            {
                let specificity = route.when_filters.len();
                if best_match.is_none() || specificity > best_specificity {
                    best_match = Some(route);
                    best_specificity = specificity;
                }
            }
        }

        best_match.map_or(&self.request, |route| &route.request)
    }
}

impl HttpSourceManifest {
    /// Returns the source secrets required by this manifest.
    ///
    /// In the new input model, every declared input with `kind: secret` is
    /// required because secrets cannot carry defaults.
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        self.declared_inputs
            .iter()
            .filter(|input| input.kind == ManifestInputKind::Secret)
            .map(|input| input.key.clone())
            .collect()
    }
}

impl RawHttpTableSpec {
    fn into_validated(self, schema: &str) -> Result<HttpTableSpec> {
        validate_http_table(
            schema,
            &self.name,
            &self.filters,
            &self.columns,
            &self.request,
            &self.requests,
            &self.pagination,
        )?;

        Ok(HttpTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                self.filters,
                self.fetch_limit_default,
                self.columns,
            ),
            request: self.request,
            requests: self.requests,
            response: self.response,
            pagination: self.pagination,
        })
    }
}

impl HttpSourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let declared_inputs = collect_source_inputs_value(&value)?;
        let raw: RawHttpSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawHttpSourceManifest {
            dsl_version,
            name,
            version,
            description,
            test_queries,
            backend: _backend,
            base_url,
            auth,
            request_headers,
            rate_limit,
            inputs: _inputs,
            tables,
            functions,
        } = raw;
        validate_test_queries(&name, &test_queries)?;
        let common =
            SourceManifestCommon::new(dsl_version, name, version, description, test_queries);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;
        validate_http_function_names(&common.name, &functions)?;
        let functions = functions
            .into_iter()
            .map(|function| function.into_validated_http(&common.name))
            .collect::<Result<Vec<_>>>()?;
        if base_url.raw().trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{}' must define a non-empty base_url",
                common.name
            )));
        }
        validate_template(
            &base_url,
            &HashSet::new(),
            &format!("source '{}'", common.name),
        )?;

        Ok(Self {
            common,
            base_url,
            auth,
            request_headers,
            rate_limit,
            tables,
            functions,
            declared_inputs,
        })
    }
}

impl SourceTableFunctionSpec {
    fn into_validated_http(self, source_name: &str) -> Result<Self> {
        validate_http_function(source_name, &self)?;
        Ok(self)
    }
}

fn validate_http_function_names(
    source_name: &str,
    functions: &[SourceTableFunctionSpec],
) -> Result<()> {
    let mut function_names = HashSet::new();

    for function in functions {
        validate_identifier(
            &function.name,
            &format!("source '{source_name}' function name"),
        )?;
        if !function_names.insert(function.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{}' is declared more than once",
                function.name
            )));
        }
    }

    Ok(())
}

fn validate_http_function(source_name: &str, function: &SourceTableFunctionSpec) -> Result<()> {
    validate_identifier(
        &function.name,
        &format!("source '{source_name}' function name"),
    )?;

    let mut arg_names = HashSet::new();
    let mut request_arg_names = HashSet::new();
    let mut direct_targets = HashSet::new();

    for fixed in &function.fixed {
        validate_function_binding(
            source_name,
            &function.name,
            &fixed.bind,
            &mut request_arg_names,
            &mut direct_targets,
        )?;
    }

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
            &mut direct_targets,
        )?;
    }

    validate_columns(
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

fn binding_target(binding: &FunctionArgBinding) -> &str {
    match binding {
        FunctionArgBinding::RequestArg { arg } => arg,
    }
}

fn validate_function_binding<'a>(
    source_name: &str,
    function_name: &str,
    binding: &'a FunctionArgBinding,
    request_arg_names: &mut HashSet<&'a str>,
    direct_targets: &mut HashSet<&'a str>,
) -> Result<()> {
    let target = binding_target(binding);
    request_arg_names.insert(target);

    match binding {
        FunctionArgBinding::RequestArg { arg } => {
            if !direct_targets.insert(arg.as_str()) {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' function '{function_name}' has multiple direct bindings for request arg '{arg}'"
                )));
            }
        }
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

    for field in &function.request.body {
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

    Ok(())
}

fn validate_arg_value_source(
    source: &crate::ValueSourceSpec,
    request_arg_names: &HashSet<&str>,
    context: &str,
) -> Result<()> {
    match source {
        crate::ValueSourceSpec::Arg { key, .. }
        | crate::ValueSourceSpec::ArgInt { key, .. }
        | crate::ValueSourceSpec::ArgBool { key, .. }
            if !request_arg_names.contains(key.as_str()) =>
        {
            return Err(ManifestError::validation(format!(
                "{context} references unknown request arg '{key}'"
            )));
        }
        crate::ValueSourceSpec::Filter { key, .. }
        | crate::ValueSourceSpec::FilterInt { key, .. }
        | crate::ValueSourceSpec::FilterBool { key, .. } => {
            return Err(ManifestError::validation(format!(
                "{context} uses table filter '{key}' inside a function request"
            )));
        }
        crate::ValueSourceSpec::Template { template } => {
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
            crate::TemplateNamespace::Arg => {
                if !request_arg_names.contains(token.key()) {
                    return Err(ManifestError::validation(format!(
                        "{context} references unknown request arg '{}' in template '{}'",
                        token.key(),
                        template.raw()
                    )));
                }
            }
            crate::TemplateNamespace::Input | crate::TemplateNamespace::State => {}
            crate::TemplateNamespace::Filter
            | crate::TemplateNamespace::Expr
            | crate::TemplateNamespace::Other(_) => {
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

#[cfg(test)]
pub(crate) fn test_http_table_spec(
    name: &str,
    columns: Vec<ColumnSpec>,
    filters: Vec<FilterSpec>,
    request: RequestSpec,
) -> HttpTableSpec {
    HttpTableSpec {
        common: TableCommon::new(
            name.to_string(),
            "test".to_string(),
            String::new(),
            filters,
            None,
            columns,
        ),
        request,
        requests: vec![],
        response: ResponseSpec::default(),
        pagination: PaginationSpec::default(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        FunctionArgBinding, QueryParamSpec, RequestSpec, SourceTableFunctionSpec,
        TableFunctionArgSpec, ValueSourceSpec,
    };

    use super::validate_http_function;

    fn function_with_request_value(value: ValueSourceSpec) -> SourceTableFunctionSpec {
        SourceTableFunctionSpec {
            name: "search".to_string(),
            kind: String::new(),
            description: String::new(),
            fetch_limit_default: None,
            fixed: vec![],
            args: vec![TableFunctionArgSpec {
                name: "query".to_string(),
                required: true,
                values: vec![],
                bind: FunctionArgBinding::RequestArg {
                    arg: "q".to_string(),
                },
            }],
            request: RequestSpec {
                path: crate::ParsedTemplate::parse("/search").expect("request path"),
                query: vec![QueryParamSpec {
                    name: "q".to_string(),
                    value,
                }],
                ..RequestSpec::default()
            },
            response: crate::ResponseSpec::default(),
            pagination: crate::PaginationSpec::default(),
            columns: vec![],
        }
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
}
