#![allow(
    missing_docs,
    reason = "This module defines field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for MCP-backed sources.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    ColumnSpec, DeclaredRelation, FilterMode, FilterSpec, ManifestError, ManifestInputKind,
    ManifestInputSpec, PaginationSpec, RequestSpec, ResponseSpec, Result, SourceBackend,
    SourceManifestCommon, SourceTableFunctionKind, SourceTableFunctionSpec, TableCommon,
    TableFunctionArgSpec, ValueSourceSpec,
    inputs::{
        collect_source_inputs_value, declared_secret_input_names, required_secret_input_names,
    },
    validate_columns, validate_declared_relation_namespace, validate_filters_and_column_exprs,
    validate_table_function_args, validate_test_queries,
};

/// Validated top-level manifest for a Model Context Protocol-backed source.
#[derive(Debug, Clone)]
pub struct McpSourceManifest {
    pub common: SourceManifestCommon,
    pub server: McpServerSpec,
    pub functions: Vec<McpTableFunctionSpec>,
    pub tables: Vec<McpTableSpec>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    backend: SourceBackend,
    #[serde(default)]
    inputs: Option<Value>,
    server: McpServerSpec,
    #[serde(default)]
    functions: Vec<RawMcpTableFunctionSpec>,
    #[serde(default)]
    tables: Vec<RawMcpTableSpec>,
}

/// MCP server connection settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpServerSpec {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: Vec<McpEnvSpec>,
    },
    StreamableHttp {
        url: String,
        #[serde(default)]
        auth: Option<McpHttpAuthSpec>,
    },
}

/// One environment variable passed to a stdio MCP server process.
#[derive(Debug, Clone, Deserialize)]
pub struct McpEnvSpec {
    pub name: String,
    #[serde(flatten)]
    pub value: crate::ValueSourceSpec,
}

/// HTTP authentication for Streamable HTTP MCP servers.
#[derive(Debug, Clone, Deserialize)]
pub struct McpHttpAuthSpec {
    #[serde(rename = "type")]
    kind: McpHttpAuthKind,
    #[serde(flatten)]
    token: crate::ValueSourceSpec,
}

/// Supported Streamable HTTP MCP auth schemes.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpHttpAuthKind {
    Bearer,
}

impl McpHttpAuthSpec {
    #[must_use]
    pub fn bearer_token(&self) -> &crate::ValueSourceSpec {
        match self.kind {
            McpHttpAuthKind::Bearer => &self.token,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpTableFunctionSpec {
    name: String,
    tool: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    fetch_limit_default: Option<usize>,
    #[serde(default)]
    args: Vec<TableFunctionArgSpec>,
    #[serde(default)]
    pagination: Option<McpPaginationSpec>,
    #[serde(default)]
    response: ResponseSpec,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
}

/// One source-scoped table-valued function backed by an MCP tool call.
#[derive(Debug, Clone)]
pub struct McpTableFunctionSpec {
    pub common: SourceTableFunctionSpec,
    pub tool: String,
    pub pagination: Option<McpPaginationSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpTableSpec {
    name: String,
    tool: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    guide: String,
    #[serde(default)]
    pub fetch_limit_default: Option<usize>,
    #[serde(default)]
    tool_args: BTreeMap<String, ValueSourceSpec>,
    #[serde(default)]
    filters: Vec<McpTableFilterSpec>,
    #[serde(default)]
    limit_binding: Option<McpLimitBinding>,
    #[serde(default)]
    pagination: Option<McpPaginationSpec>,
    #[serde(default)]
    response: ResponseSpec,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
}

/// One SQL table backed by an MCP tool call.
#[derive(Debug, Clone)]
pub struct McpTableSpec {
    pub common: TableCommon,
    pub tool: String,
    pub tool_args: BTreeMap<String, ValueSourceSpec>,
    pub filter_bindings: Vec<McpTableFilterBinding>,
    pub limit_binding: Option<McpLimitBinding>,
    pub pagination: Option<McpPaginationSpec>,
    pub response: ResponseSpec,
}

/// How `LIMIT` pushes into an MCP tool argument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpLimitBinding {
    pub tool_arg: String,
    #[serde(default)]
    pub max: Option<usize>,
}

/// Cursor pagination for MCP tool results.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPaginationSpec {
    pub cursor_arg: String,
    pub response_cursor_path: Vec<String>,
    #[serde(default)]
    pub max_pages: Option<usize>,
}

/// One SQL filter declared on an MCP table that may bind into an MCP tool argument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTableFilterSpec {
    pub name: String,
    #[serde(rename = "type", default = "default_mcp_filter_data_type")]
    pub data_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mode: FilterMode,
    #[serde(default)]
    pub description: String,
    pub tool_arg: String,
}

#[derive(Debug, Clone)]
pub struct McpTableFilterBinding {
    pub name: String,
    pub tool_arg: String,
}

impl McpTableSpec {
    #[must_use]
    /// Returns the stable table name.
    pub fn name(&self) -> &str {
        &self.common.name
    }

    #[must_use]
    /// Returns the declared SQL filters that may bind into MCP tool arguments.
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
    /// Returns the MCP tool argument name bound to a declared SQL filter.
    pub fn tool_arg_for_filter(&self, filter_name: &str) -> Option<&str> {
        self.filter_bindings
            .iter()
            .find(|binding| binding.name == filter_name)
            .map(|binding| binding.tool_arg.as_str())
    }
}

impl McpTableFunctionSpec {
    #[must_use]
    /// Returns the stable function name.
    pub fn name(&self) -> &str {
        &self.common.name
    }

    #[must_use]
    /// Returns the function arguments.
    pub fn args(&self) -> &[TableFunctionArgSpec] {
        &self.common.args
    }

    #[must_use]
    /// Returns the declared output columns for this function.
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.common.columns
    }

    #[must_use]
    /// Returns the default fetch limit declared by the manifest, if any.
    pub fn fetch_limit_default(&self) -> Option<usize> {
        self.common.fetch_limit_default
    }
}

fn default_mcp_filter_data_type() -> String {
    "Utf8".to_string()
}

impl McpTableFilterSpec {
    fn filter_spec(&self) -> FilterSpec {
        FilterSpec {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            required: self.required,
            mode: self.mode,
            description: self.description.clone(),
        }
    }

    fn binding(&self) -> McpTableFilterBinding {
        McpTableFilterBinding {
            name: self.name.clone(),
            tool_arg: self.tool_arg.clone(),
        }
    }
}

impl RawMcpTableFunctionSpec {
    fn into_validated(self, source_name: &str) -> Result<McpTableFunctionSpec> {
        validate_mcp_function(source_name, &self)?;
        Ok(McpTableFunctionSpec {
            tool: self.tool,
            pagination: self.pagination,
            common: SourceTableFunctionSpec {
                name: self.name,
                kind: SourceTableFunctionKind::default(),
                description: self.description,
                fetch_limit_default: self.fetch_limit_default,
                search_limits: None,
                detail_hints: Vec::new(),
                args: self.args,
                request: RequestSpec::default(),
                response: self.response,
                pagination: PaginationSpec::default(),
                columns: self.columns,
            },
        })
    }
}

impl RawMcpTableSpec {
    fn into_validated(self, source_name: &str) -> Result<McpTableSpec> {
        let filters: Vec<FilterSpec> = self
            .filters
            .iter()
            .map(McpTableFilterSpec::filter_spec)
            .collect();
        validate_mcp_table(source_name, &self, &filters)?;
        let filter_bindings = self
            .filters
            .iter()
            .map(McpTableFilterSpec::binding)
            .collect();
        Ok(McpTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                filters,
                self.fetch_limit_default,
                None,
                Vec::new(),
                self.columns,
            ),
            tool: self.tool,
            tool_args: self.tool_args,
            filter_bindings,
            limit_binding: self.limit_binding,
            pagination: self.pagination,
            response: self.response,
        })
    }
}

impl McpSourceManifest {
    /// Returns all source secrets declared by this manifest.
    pub fn declared_secret_names(&self) -> BTreeSet<String> {
        declared_secret_input_names(&self.declared_inputs)
    }

    /// Returns the source secrets required by this manifest.
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        required_secret_input_names(&self.declared_inputs)
    }

    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let declared_inputs = collect_source_inputs_value(&value)?;
        let raw: RawMcpSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawMcpSourceManifest {
            dsl_version,
            name,
            version,
            description,
            test_queries,
            backend: _backend,
            inputs: _inputs,
            server,
            functions,
            tables,
        } = raw;

        if functions.is_empty() && tables.is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{name}' must define at least one function or table"
            )));
        }
        validate_test_queries(&name, &test_queries)?;
        validate_server(&name, &server, &declared_inputs)?;
        validate_declared_relation_namespace(
            &name,
            tables
                .iter()
                .map(|table| DeclaredRelation::table(table.name.as_str()))
                .chain(
                    functions
                        .iter()
                        .map(|function| DeclaredRelation::function(function.name.as_str())),
                ),
        )?;
        let common =
            SourceManifestCommon::new(dsl_version, name, version, description, test_queries);
        let functions = functions
            .into_iter()
            .map(|function| function.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            common,
            server,
            functions,
            tables,
            declared_inputs,
        })
    }
}

fn validate_server(
    source_name: &str,
    server: &McpServerSpec,
    declared_inputs: &[ManifestInputSpec],
) -> Result<()> {
    match server {
        McpServerSpec::Stdio { command, env, .. } => {
            validate_stdio_server(source_name, command, env)
        }
        McpServerSpec::StreamableHttp { url, auth } => {
            validate_streamable_http_server(source_name, url, auth.as_ref(), declared_inputs)
        }
    }
}

fn validate_stdio_server(source_name: &str, command: &str, env: &[McpEnvSpec]) -> Result<()> {
    if command.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP server command must not be empty"
        )));
    }

    let mut env_names = HashSet::new();
    for env in env {
        if env.name.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP server env name must not be empty"
            )));
        }
        if !env_names.insert(env.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP server env '{}' is declared more than once",
                env.name
            )));
        }
        validate_server_env_value_source(source_name, env)?;
    }
    Ok(())
}

fn validate_streamable_http_server(
    source_name: &str,
    raw_url: &str,
    auth: Option<&McpHttpAuthSpec>,
    declared_inputs: &[ManifestInputSpec],
) -> Result<()> {
    let url = url::Url::parse(raw_url).map_err(|error| {
        ManifestError::validation(format!(
            "source '{source_name}' MCP streamable_http server url is invalid: {error}"
        ))
    })?;
    match url.scheme() {
        "https" => {}
        "http" if is_local_http_url(&url) => {}
        "http" => {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP streamable_http server url must use https unless it targets localhost"
            )));
        }
        scheme => {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP streamable_http server url has unsupported scheme '{scheme}'"
            )));
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP streamable_http server url must not embed credentials in userinfo; use the `auth` block with a secret input instead"
        )));
    }
    if let Some(auth) = auth {
        validate_streamable_http_auth_token(source_name, auth.bearer_token(), declared_inputs)?;
    }
    Ok(())
}

/// Streamable HTTP bearer tokens are credentials: enforce that they
/// resolve from a declared `kind: secret` input rather than a literal,
/// template, variable input, or any of the request-scoped sources.
fn validate_streamable_http_auth_token(
    source_name: &str,
    token: &ValueSourceSpec,
    declared_inputs: &[ManifestInputSpec],
) -> Result<()> {
    let ValueSourceSpec::Input { key } = token else {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP streamable_http server auth token must use `from: input` referencing a secret input"
        )));
    };
    let declared = declared_inputs
        .iter()
        .find(|input| input.key == *key)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "source '{source_name}' MCP streamable_http server auth token references input '{key}' which is not declared under `inputs`"
            ))
        })?;
    if declared.kind != ManifestInputKind::Secret {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP streamable_http server auth token must reference a `kind: secret` input; '{key}' is a variable"
        )));
    }
    Ok(())
}

fn is_local_http_url(url: &url::Url) -> bool {
    // Use the typed `Host` enum so IPv4/IPv6 literals are matched by their
    // parsed address (`is_loopback()`) rather than a textual prefix check —
    // a hostname like `127.example.com` shares the `127.` prefix but is not
    // loopback, and IPv6 literals in URLs arrive bracketed.
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(addr)) => addr.is_loopback(),
        Some(url::Host::Ipv6(addr)) => addr.is_loopback(),
        None => false,
    }
}

fn validate_server_env_value_source(source_name: &str, env: &McpEnvSpec) -> Result<()> {
    let context = format!("source '{source_name}' MCP server env '{}'", env.name);
    validate_source_scoped_value_source(&env.value, SourceScopedValueContext::source(&context))
}

fn validate_mcp_function(source_name: &str, function: &RawMcpTableFunctionSpec) -> Result<()> {
    if function.tool.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' function '{}' must define a non-empty tool",
            function.name
        )));
    }
    if function.columns.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' function '{}' must define columns",
            function.name
        )));
    }

    let mut request_arg_names =
        validate_table_function_args(source_name, &function.name, &function.args, "tool arg")?;
    if let Some(pagination) = &function.pagination {
        validate_pagination(
            source_name,
            "function",
            &function.name,
            pagination,
            &mut request_arg_names,
        )?;
    }

    validate_columns(
        &function.columns,
        source_name,
        &format!("function '{}'", function.name),
    )?;
    validate_filters_and_column_exprs(
        &[],
        &function.columns,
        source_name,
        &format!("function '{}'", function.name),
    )?;
    Ok(())
}

fn validate_mcp_table(
    source_name: &str,
    table: &RawMcpTableSpec,
    filters: &[FilterSpec],
) -> Result<()> {
    if table.tool.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' table '{}' must define a non-empty tool",
            table.name
        )));
    }
    if table.columns.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' table '{}' must define columns",
            table.name
        )));
    }

    validate_columns(
        &table.columns,
        source_name,
        &format!("table '{}'", table.name),
    )?;
    validate_filters_and_column_exprs(
        filters,
        &table.columns,
        source_name,
        &format!("table '{}'", table.name),
    )?;

    let mut bound_tool_args: HashSet<&str> = table.tool_args.keys().map(String::as_str).collect();
    for (name, source) in &table.tool_args {
        if name.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' tool_args has an empty key",
                table.name
            )));
        }
        validate_table_tool_arg_value_source(source_name, &table.name, name, source)?;
    }

    for filter in &table.filters {
        if filter.tool_arg.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' filter '{}' must define a non-empty tool_arg",
                table.name, filter.name
            )));
        }
        if !bound_tool_args.insert(filter.tool_arg.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' filter '{}' binds tool arg '{}' that is already bound",
                table.name, filter.name, filter.tool_arg
            )));
        }
    }

    if let Some(limit_binding) = &table.limit_binding {
        if limit_binding.tool_arg.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' limit_binding.tool_arg must not be empty",
                table.name
            )));
        }
        if !bound_tool_args.insert(limit_binding.tool_arg.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' limit_binding binds tool arg '{}' that is already bound",
                table.name, limit_binding.tool_arg
            )));
        }
        if matches!(limit_binding.max, Some(0)) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' limit_binding.max must be greater than 0",
                table.name
            )));
        }
    }
    if let Some(pagination) = &table.pagination {
        validate_pagination(
            source_name,
            "table",
            &table.name,
            pagination,
            &mut bound_tool_args,
        )?;
    }

    Ok(())
}

fn validate_pagination<'a>(
    source_name: &str,
    relation_kind: &str,
    relation_name: &str,
    pagination: &'a McpPaginationSpec,
    bound_tool_args: &mut HashSet<&'a str>,
) -> Result<()> {
    if pagination.cursor_arg.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {relation_kind} '{relation_name}' pagination.cursor_arg must not be empty"
        )));
    }
    if pagination.response_cursor_path.is_empty()
        || pagination
            .response_cursor_path
            .iter()
            .any(|segment| segment.trim().is_empty())
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {relation_kind} '{relation_name}' pagination.response_cursor_path must not be empty"
        )));
    }
    if matches!(pagination.max_pages, Some(0)) {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {relation_kind} '{relation_name}' pagination.max_pages must be greater than 0"
        )));
    }
    if !bound_tool_args.insert(pagination.cursor_arg.as_str()) {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' {relation_kind} '{relation_name}' pagination binds tool arg '{}' that is already bound",
            pagination.cursor_arg
        )));
    }
    Ok(())
}

fn validate_table_tool_arg_value_source(
    source_name: &str,
    table_name: &str,
    arg_name: &str,
    source: &ValueSourceSpec,
) -> Result<()> {
    let context = format!("source '{source_name}' table '{table_name}' tool_args.{arg_name}");
    validate_source_scoped_value_source(source, SourceScopedValueContext::table_tool_arg(&context))
}

#[derive(Clone, Copy)]
struct SourceScopedValueContext<'a> {
    context: &'a str,
    source_scope_text: &'static str,
    arg_scope_text: &'static str,
    direct_filter_text: &'static str,
    template_filter_text: &'static str,
    filter_suffix: &'static str,
}

impl<'a> SourceScopedValueContext<'a> {
    fn table_tool_arg(context: &'a str) -> Self {
        Self {
            context,
            source_scope_text: "MCP table tool_args are source-scoped",
            arg_scope_text: "tables do not take arguments",
            direct_filter_text: "references filter",
            template_filter_text: "template references filter",
            filter_suffix: "; bind filters through filters[].tool_arg instead",
        }
    }

    fn source(context: &'a str) -> Self {
        Self {
            context,
            source_scope_text: "the value is source-scoped",
            arg_scope_text: "the value is source-scoped",
            direct_filter_text: "uses table filter",
            template_filter_text: "template references table filter",
            filter_suffix: " but the value is source-scoped",
        }
    }

    fn direct_filter_error(self, key: &str) -> ManifestError {
        ManifestError::validation(format!(
            "{} {} '{key}'{}",
            self.context, self.direct_filter_text, self.filter_suffix
        ))
    }

    fn template_filter_error(self, key: &str) -> ManifestError {
        ManifestError::validation(format!(
            "{} {} '{key}'{}",
            self.context, self.template_filter_text, self.filter_suffix
        ))
    }
}

fn validate_source_scoped_value_source(
    source: &ValueSourceSpec,
    context: SourceScopedValueContext<'_>,
) -> Result<()> {
    let context_text = context.context;
    match source {
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => Err(context.direct_filter_error(key)),
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. }
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. } => Err(ManifestError::validation(format!(
            "{context_text} uses function argument '{key}' but {}",
            context.arg_scope_text,
        ))),
        ValueSourceSpec::State { key } => Err(ManifestError::validation(format!(
            "{context_text} uses state value '{key}' but {}",
            context.source_scope_text,
        ))),
        ValueSourceSpec::Template { template } => {
            for token in template.tokens() {
                match token.namespace() {
                    crate::TemplateNamespace::Filter => {
                        return Err(context.template_filter_error(token.key()));
                    }
                    crate::TemplateNamespace::Arg => {
                        return Err(ManifestError::validation(format!(
                            "{context_text} template references function argument '{}' but {}",
                            token.key(),
                            context.arg_scope_text,
                        )));
                    }
                    crate::TemplateNamespace::State => {
                        return Err(ManifestError::validation(format!(
                            "{context_text} template references state value '{}' but {}",
                            token.key(),
                            context.source_scope_text,
                        )));
                    }
                    crate::TemplateNamespace::Expr | crate::TemplateNamespace::Other(_) => {
                        return Err(ManifestError::validation(format!(
                            "{context_text} uses unsupported template token '{}'",
                            token.raw()
                        )));
                    }
                    crate::TemplateNamespace::Input => {}
                }
            }
            Ok(())
        }
        ValueSourceSpec::OneOf { values } => {
            if values.is_empty() {
                return Err(ManifestError::validation(format!(
                    "{context_text} one_of values must not be empty"
                )));
            }
            for value in values {
                validate_source_scoped_value_source(value, context)?;
            }
            Ok(())
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Input { .. }
        | ValueSourceSpec::Bearer { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{Value, json};

    use super::{McpServerSpec, McpSourceManifest};
    use crate::test_support::assert_error_contains;

    fn parse_mcp(body: Value) -> crate::Result<McpSourceManifest> {
        let Value::Object(mut manifest) = body else {
            return McpSourceManifest::parse_manifest_value(body);
        };

        for (key, value) in [
            ("dsl_version", json!(3)),
            ("name", json!("demo")),
            ("version", json!("0.1.0")),
            ("backend", json!("mcp")),
            ("server", stdio_server()),
        ] {
            manifest.entry(key.to_string()).or_insert(value);
        }

        McpSourceManifest::parse_manifest_value(Value::Object(manifest))
    }

    fn stdio_server() -> Value {
        json!({ "transport": "stdio", "command": "demo-mcp-server" })
    }

    fn id_columns() -> Value {
        json!([{ "name": "id", "type": "Utf8" }])
    }

    fn title_columns() -> Value {
        json!([{ "name": "title", "type": "Utf8" }])
    }

    fn issues_table() -> Value {
        json!({
            "name": "issues",
            "tool": "list_issues",
            "columns": title_columns()
        })
    }

    fn issues_table_with(fields: Value) -> Value {
        let Value::Object(mut table) = issues_table() else {
            unreachable!("issues_table fixture is an object");
        };
        let Value::Object(fields) = fields else {
            unreachable!("test fixture overrides must be an object");
        };
        table.extend(fields);
        Value::Object(table)
    }

    fn parse_issues_table_with(fields: Value) -> crate::Result<McpSourceManifest> {
        parse_mcp(json!({ "tables": [issues_table_with(fields)] }))
    }

    fn table_named(name: &str) -> Value {
        json!({
            "name": name,
            "tool": "list_issues",
            "columns": id_columns()
        })
    }

    fn function_named(name: &str) -> Value {
        json!({
            "name": name,
            "tool": "search_issues",
            "args": [],
            "columns": id_columns()
        })
    }

    fn mcp_with_server(server: &Value) -> Value {
        json!({
            "server": server,
            "tables": [issues_table()]
        })
    }

    fn streamable_http_server(url: &str) -> Value {
        json!({
            "transport": "streamable_http",
            "url": url
        })
    }

    fn parse_streamable_http_url(url: &str) -> crate::Result<McpSourceManifest> {
        parse_mcp(mcp_with_server(&streamable_http_server(url)))
    }

    fn assert_parse_error_contains(
        result: crate::Result<McpSourceManifest>,
        expectation: &str,
        expected: &str,
    ) {
        let error = result.expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    #[test]
    fn parses_mcp_manifest_with_secret_input() {
        let manifest = parse_mcp(json!({
            "name": "github_mcp",
            "inputs": {
                "GITHUB_TOKEN": { "kind": "secret" },
                "OPTIONAL_TOKEN": { "kind": "secret", "required": false }
            },
            "server": {
                "transport": "stdio",
                "command": "github-mcp-server",
                "env": [{
                    "name": "GITHUB_TOKEN",
                    "from": "input",
                    "key": "GITHUB_TOKEN"
                }]
            },
            "functions": [{
                "name": "search_issues",
                "tool": "search_issues",
                "args": [{
                    "name": "query",
                    "required": true,
                    "bind": { "arg": "query" }
                }],
                "columns": title_columns()
            }]
        }))
        .expect("mcp manifest should parse");

        assert_eq!(manifest.common.name, "github_mcp");
        let function = manifest.functions.first().expect("function should parse");
        assert_eq!(function.tool, "search_issues");
        assert_eq!(
            manifest.declared_secret_names(),
            BTreeSet::from(["GITHUB_TOKEN".to_string(), "OPTIONAL_TOKEN".to_string()])
        );
        assert_eq!(
            manifest.required_secret_names(),
            BTreeSet::from(["GITHUB_TOKEN".to_string()])
        );
    }

    #[test]
    fn rejects_mcp_surfaces_without_columns() {
        for (name, manifest, expected) in [
            (
                "function",
                json!({ "functions": [{ "name": "lookup", "tool": "lookup" }] }),
                "source 'demo' function 'lookup' must define columns",
            ),
            (
                "table",
                json!({ "tables": [{ "name": "issues", "tool": "list_issues" }] }),
                "source 'demo' table 'issues' must define columns",
            ),
        ] {
            let error = parse_mcp(manifest).expect_err(name);
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn parses_streamable_http_mcp_server_with_input_backed_bearer_auth() {
        let manifest = parse_mcp(json!({
            "inputs": {
                "MCP_ACCESS_TOKEN": { "kind": "secret" }
            },
            "server": {
                "transport": "streamable_http",
                "url": "https://mcp.example.com/mcp",
                "auth": {
                    "type": "bearer",
                    "from": "input",
                    "key": "MCP_ACCESS_TOKEN"
                }
            },
            "tables": [issues_table()]
        }))
        .expect("streamable_http mcp manifest should parse");

        assert!(matches!(
            manifest.server,
            McpServerSpec::StreamableHttp { .. }
        ));
        assert!(
            manifest
                .required_secret_names()
                .contains("MCP_ACCESS_TOKEN")
        );
    }

    #[test]
    fn rejects_streamable_http_server_with_stdio_fields() {
        assert_parse_error_contains(
            parse_mcp(mcp_with_server(&json!({
                "transport": "streamable_http",
                "url": "https://mcp.example.com/mcp",
                "command": "mcp-server"
            }))),
            "stdio fields on streamable_http should fail",
            "unknown field `command`",
        );
    }

    #[test]
    fn rejects_insecure_non_loopback_streamable_http_urls() {
        for (url, expectation) in [
            ("http://mcp.example.com/mcp", "non-local http should fail"),
            (
                "http://127.example.com/mcp",
                "loopback-lookalike host should still fail",
            ),
        ] {
            assert_parse_error_contains(
                parse_streamable_http_url(url),
                expectation,
                "must use https unless it targets localhost",
            );
        }
    }

    #[test]
    fn allows_http_for_real_loopback_addresses() {
        for url in [
            "http://127.0.0.1:8080/mcp",
            "http://localhost:8080/mcp",
            "http://[::1]:8080/mcp",
        ] {
            parse_streamable_http_url(url)
                .unwrap_or_else(|err| panic!("loopback url `{url}` should parse: {err}"));
        }
    }

    #[test]
    fn rejects_streamable_http_urls_with_userinfo() {
        for (url, expectation) in [
            (
                "https://alice:s3cret@mcp.example.com/mcp",
                "userinfo credentials should be rejected",
            ),
            (
                "https://api-token@mcp.example.com/mcp",
                "username-only userinfo should be rejected",
            ),
        ] {
            assert_parse_error_contains(
                parse_streamable_http_url(url),
                expectation,
                "must not embed credentials in userinfo",
            );
        }
    }

    #[test]
    fn rejects_streamable_http_auth_token_from_literal() {
        assert_parse_error_contains(
            parse_mcp(mcp_with_server(&json!({
                "transport": "streamable_http",
                "url": "https://mcp.example.com/mcp",
                "auth": {
                    "type": "bearer",
                    "from": "literal",
                    "value": "hunter2"
                }
            }))),
            "literal bearer token should be rejected",
            "must use `from: input` referencing a secret input",
        );
    }

    #[test]
    fn rejects_streamable_http_auth_token_from_variable_input() {
        // Use a non-credential-like name so the early credential-like
        // safety net does not fire before our explicit check.
        assert_parse_error_contains(
            parse_mcp(json!({
                "inputs": {
                    "MCP_OPAQUE_VALUE": { "kind": "variable", "default": "x" }
                },
                "server": {
                    "transport": "streamable_http",
                    "url": "https://mcp.example.com/mcp",
                    "auth": {
                        "type": "bearer",
                        "from": "input",
                        "key": "MCP_OPAQUE_VALUE"
                    }
                },
                "tables": [issues_table()]
            })),
            "variable-kind input as bearer token should be rejected",
            "must reference a `kind: secret` input",
        );
    }

    #[test]
    fn parses_minimal_mcp_table() {
        let manifest = parse_issues_table_with(json!({
                "tool_args": {
                    "owner": { "from": "literal", "value": "acme" }
                },
                "filters": [{
                    "name": "state",
                    "tool_arg": "state"
                }],
                "response": { "rows_path": ["issues"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "title", "type": "Utf8" }
                ]
        }))
        .expect("table-only mcp manifest should parse");

        assert_eq!(manifest.tables.len(), 1);
        let table = manifest.tables.first().expect("one table");
        assert_eq!(table.name(), "issues");
        assert_eq!(table.tool, "list_issues");
        assert_eq!(table.filters().len(), 1);
        assert_eq!(
            table
                .filter_bindings
                .first()
                .expect("one filter binding")
                .tool_arg,
            "state"
        );
    }

    #[test]
    fn rejects_duplicate_mcp_surface_names() {
        for (name, manifest, expected) in [
            (
                "table/function name collision",
                json!({
                    "tables": [table_named("issues")],
                    "functions": [function_named("issues")]
                }),
                "source 'demo' declares both a table and function named 'issues'",
            ),
            (
                "case-insensitive table/function name collision",
                json!({
                    "tables": [table_named("Issues")],
                    "functions": [function_named("issues")]
                }),
                "source 'demo' declares both a table and function named 'issues'",
            ),
            (
                "duplicate table names",
                json!({
                    "tables": [table_named("issues"), table_named("issues")]
                }),
                "source 'demo' table 'issues' is declared more than once",
            ),
            (
                "case-insensitive duplicate table names",
                json!({
                    "tables": [table_named("Issues"), table_named("issues")]
                }),
                "source 'demo' table 'issues' is declared more than once",
            ),
            (
                "case-insensitive duplicate function names",
                json!({
                    "functions": [function_named("Search"), function_named("search")]
                }),
                "source 'demo' function 'search' is declared more than once",
            ),
        ] {
            let error = parse_mcp(manifest).expect_err(name);
            assert_eq!(error.to_string(), expected, "unexpected error for {name}");
        }
    }

    #[test]
    fn rejects_request_scoped_mcp_server_env_sources() {
        for (name, env, expected) in [
            (
                "state reference in server env",
                json!({ "name": "CURSOR", "from": "state", "key": "cursor" }),
                "MCP server env 'CURSOR' uses state value 'cursor'",
            ),
            (
                "filter template reference in server env",
                json!({
                    "name": "FILTERED",
                    "from": "template",
                    "template": "{{filter.state}}"
                }),
                "MCP server env 'FILTERED' template references table filter 'state'",
            ),
        ] {
            assert_parse_error_contains(
                parse_mcp(json!({
                    "server": {
                        "transport": "stdio",
                        "command": "demo-mcp-server",
                        "env": [env]
                    },
                    "tables": [issues_table()]
                })),
                name,
                expected,
            );
        }
    }

    #[test]
    fn rejects_invalid_tool_arg_bindings() {
        for (name, fields, expected) in [
            (
                "filter reference in tool_args",
                json!({
                    "tool_args": {
                        "state": { "from": "filter", "key": "state" }
                    },
                    "filters": [{ "name": "state", "tool_arg": "state" }]
                }),
                "references filter 'state'; bind filters through filters[].tool_arg",
            ),
            (
                "state reference in tool_args",
                json!({ "tool_args": { "cursor": { "from": "state", "key": "cursor" } } }),
                "tool_args.cursor uses state value 'cursor'",
            ),
            (
                "state template reference in tool_args",
                json!({
                    "tool_args": {
                        "cursor": { "from": "template", "template": "{{state.cursor}}" }
                    }
                }),
                "tool_args.cursor template references state value 'cursor'",
            ),
            (
                "duplicate filter and tool_args binding",
                json!({
                    "tool_args": {
                        "state": { "from": "literal", "value": "open" }
                    },
                    "filters": [{ "name": "state", "tool_arg": "state" }]
                }),
                "binds tool arg 'state' that is already bound",
            ),
        ] {
            assert_parse_error_contains(parse_issues_table_with(fields), name, expected);
        }
    }

    #[test]
    fn parses_table_with_limit_binding() {
        let manifest = parse_issues_table_with(json!({
            "limit_binding": { "tool_arg": "page_size", "max": 200 }
        }))
        .expect("manifest with limit_binding should parse");

        let table = manifest.tables.first().expect("one table");
        let binding = table.limit_binding.as_ref().expect("binding present");
        assert_eq!(binding.tool_arg, "page_size");
        assert_eq!(binding.max, Some(200));
    }

    #[test]
    fn parses_table_with_cursor_pagination() {
        let manifest = parse_issues_table_with(json!({
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 5
            }
        }))
        .expect("manifest with pagination should parse");

        let table = manifest.tables.first().expect("one table");
        let pagination = table.pagination.as_ref().expect("pagination present");
        assert_eq!(pagination.cursor_arg, "cursor");
        assert_eq!(pagination.response_cursor_path, ["meta", "nextCursor"]);
        assert_eq!(pagination.max_pages, Some(5));
    }

    #[test]
    fn rejects_table_tool_arg_collisions() {
        for (name, fields, expected) in [
            (
                "pagination cursor colliding with filter",
                json!({
                    "filters": [{ "name": "state", "tool_arg": "cursor" }],
                    "pagination": {
                        "cursor_arg": "cursor",
                        "response_cursor_path": ["nextCursor"]
                    }
                }),
                "pagination binds tool arg 'cursor' that is already bound",
            ),
            (
                "limit_binding colliding with filter",
                json!({
                    "filters": [{ "name": "state", "tool_arg": "state" }],
                    "limit_binding": { "tool_arg": "state" }
                }),
                "limit_binding binds tool arg 'state' that is already bound",
            ),
        ] {
            assert_parse_error_contains(parse_issues_table_with(fields), name, expected);
        }
    }

    #[test]
    fn rejects_manifest_without_tables_or_functions() {
        let error = parse_mcp(json!({})).expect_err("manifest needs at least one tool surface");

        assert_eq!(
            error.to_string(),
            "source 'demo' must define at least one function or table"
        );
    }
}
