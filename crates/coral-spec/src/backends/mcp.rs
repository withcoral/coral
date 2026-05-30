#![allow(
    missing_docs,
    reason = "This module defines field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for MCP-backed sources.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    ColumnSpec, FilterMode, FilterSpec, FunctionArgBinding, ManifestError, ManifestInputSpec,
    PaginationSpec, RequestSpec, ResponseSpec, Result, SourceBackend, SourceManifestCommon,
    SourceTableFunctionKind, SourceTableFunctionSpec, TableCommon, TableFunctionArgSpec,
    ValueSourceSpec,
    inputs::{
        collect_source_inputs_value, declared_secret_input_names, required_secret_input_names,
    },
    validate_columns, validate_filters_and_column_exprs, validate_identifier,
    validate_test_queries, validate_unique_values,
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
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    pub transport: McpTransport,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<McpEnvSpec>,
}

/// Supported MCP transports.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    Stdio,
}

/// One environment variable passed to a stdio MCP server process.
#[derive(Debug, Clone, Deserialize)]
pub struct McpEnvSpec {
    pub name: String,
    #[serde(flatten)]
    pub value: crate::ValueSourceSpec,
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
        validate_mcp_table(source_name, &self)?;
        let filters = self
            .filters
            .iter()
            .map(McpTableFilterSpec::filter_spec)
            .collect();
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

    fn filter_specs(&self) -> Vec<FilterSpec> {
        self.filters
            .iter()
            .map(McpTableFilterSpec::filter_spec)
            .collect()
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
        validate_server(&name, &server)?;
        validate_table_and_function_names(&name, &tables, &functions)?;
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

fn validate_server(source_name: &str, server: &McpServerSpec) -> Result<()> {
    if server.command.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP server command must not be empty"
        )));
    }

    let mut env_names = HashSet::new();
    for env in &server.env {
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

fn validate_server_env_value_source(source_name: &str, env: &McpEnvSpec) -> Result<()> {
    validate_source_scoped_value_source(
        &env.value,
        &format!("source '{source_name}' MCP server env '{}'", env.name),
    )
}

fn validate_table_and_function_names(
    source_name: &str,
    tables: &[RawMcpTableSpec],
    functions: &[RawMcpTableFunctionSpec],
) -> Result<()> {
    let mut table_names = HashSet::new();
    for table in tables {
        validate_identifier(&table.name, &format!("source '{source_name}' table name"))?;
        if !table_names.insert(table.name.to_ascii_lowercase()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' is declared more than once",
                table.name
            )));
        }
    }

    let mut function_names = HashSet::new();
    for function in functions {
        validate_identifier(
            &function.name,
            &format!("source '{source_name}' function name"),
        )?;
        let normalized_name = function.name.to_ascii_lowercase();
        if table_names.contains(&normalized_name) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' declares both a table and function named '{}'",
                function.name
            )));
        }
        if !function_names.insert(normalized_name) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' function '{}' is declared more than once",
                function.name
            )));
        }
    }
    Ok(())
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

fn validate_mcp_table(source_name: &str, table: &RawMcpTableSpec) -> Result<()> {
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
        &table.filter_specs(),
        &table.columns,
        source_name,
        &format!("table '{}'", table.name),
    )?;

    let mut bound_tool_args: HashSet<&str> = HashSet::new();
    for (name, source) in &table.tool_args {
        if name.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' tool_args has an empty key",
                table.name
            )));
        }
        if !bound_tool_args.insert(name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' has multiple bindings for tool arg '{name}'",
                table.name
            )));
        }
        validate_table_tool_arg_value_source(source_name, &table.name, name, source)?;
    }

    let mut filter_names: HashSet<&str> = HashSet::new();
    for filter in &table.filters {
        if filter.tool_arg.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' filter '{}' must define a non-empty tool_arg",
                table.name, filter.name
            )));
        }
        if !filter_names.insert(filter.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' table '{}' has duplicate filter '{}'",
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
    match source {
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => Err(ManifestError::validation(format!(
            "{context} references filter '{key}'; bind filters through filters[].tool_arg instead",
        ))),
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. }
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. } => Err(ManifestError::validation(format!(
            "{context} uses function argument '{key}' but tables do not take arguments",
        ))),
        ValueSourceSpec::State { key } => Err(ManifestError::validation(format!(
            "{context} uses state value '{key}' but MCP table tool_args are source-scoped",
        ))),
        ValueSourceSpec::Template { template } => {
            for token in template.tokens() {
                match token.namespace() {
                    crate::TemplateNamespace::Filter => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references filter '{}'; bind filters through filters[].tool_arg instead",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::Arg => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references function argument '{}' but tables do not take arguments",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::State => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references state value '{}' but MCP table tool_args are source-scoped",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::Expr | crate::TemplateNamespace::Other(_) => {
                        return Err(ManifestError::validation(format!(
                            "{context} uses unsupported template token '{}'",
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
                    "{context} one_of values must not be empty"
                )));
            }
            for value in values {
                validate_table_tool_arg_value_source(source_name, table_name, arg_name, value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_source_scoped_value_source(source: &ValueSourceSpec, context: &str) -> Result<()> {
    match source {
        ValueSourceSpec::Filter { key, .. }
        | ValueSourceSpec::FilterInt { key, .. }
        | ValueSourceSpec::FilterBool { key, .. }
        | ValueSourceSpec::FilterSplit { key, .. }
        | ValueSourceSpec::FilterSplitInt { key, .. } => Err(ManifestError::validation(format!(
            "{context} uses table filter '{key}' but the value is source-scoped",
        ))),
        ValueSourceSpec::Arg { key, .. }
        | ValueSourceSpec::ArgInt { key, .. }
        | ValueSourceSpec::ArgBool { key, .. }
        | ValueSourceSpec::ArgSplit { key, .. }
        | ValueSourceSpec::ArgSplitInt { key, .. } => Err(ManifestError::validation(format!(
            "{context} uses function argument '{key}' but the value is source-scoped",
        ))),
        ValueSourceSpec::State { key } => Err(ManifestError::validation(format!(
            "{context} uses state value '{key}' but the value is source-scoped",
        ))),
        ValueSourceSpec::Template { template } => {
            for token in template.tokens() {
                match token.namespace() {
                    crate::TemplateNamespace::Input => {}
                    crate::TemplateNamespace::Filter => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references table filter '{}' but the value is source-scoped",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::Arg => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references function argument '{}' but the value is source-scoped",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::State => {
                        return Err(ManifestError::validation(format!(
                            "{context} template references state value '{}' but the value is source-scoped",
                            token.key()
                        )));
                    }
                    crate::TemplateNamespace::Expr | crate::TemplateNamespace::Other(_) => {
                        return Err(ManifestError::validation(format!(
                            "{context} uses unsupported template token '{}'",
                            token.raw()
                        )));
                    }
                }
            }
            Ok(())
        }
        ValueSourceSpec::OneOf { values } => {
            if values.is_empty() {
                return Err(ManifestError::validation(format!(
                    "{context} one_of values must not be empty"
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

fn validate_function_binding<'a>(
    source_name: &str,
    function_name: &str,
    binding: &'a FunctionArgBinding,
    request_arg_names: &mut HashSet<&'a str>,
) -> Result<()> {
    if !request_arg_names.insert(binding.arg.as_str()) {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' function '{function_name}' has multiple bindings for tool arg '{}'",
            binding.arg
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::McpSourceManifest;
    use serde_json::json;

    #[test]
    fn parses_mcp_manifest_with_secret_input() {
        let manifest = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "github_mcp",
            "version": "0.1.0",
            "backend": "mcp",
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
                "columns": [{ "name": "title", "type": "Utf8" }]
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
    fn rejects_mcp_function_without_columns() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": {
                "transport": "stdio",
                "command": "demo-mcp-server"
            },
            "functions": [{
                "name": "lookup",
                "tool": "lookup"
            }]
        }))
        .expect_err("missing columns should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' function 'lookup' must define columns"
        );
    }

    #[test]
    fn parses_minimal_mcp_table() {
        let manifest = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
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
            }]
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
    fn rejects_table_and_function_with_same_name() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "columns": [{ "name": "id", "type": "Utf8" }]
            }],
            "functions": [{
                "name": "issues",
                "tool": "search_issues",
                "args": [],
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("table/function name collision should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' declares both a table and function named 'issues'"
        );
    }

    #[test]
    fn rejects_table_and_function_names_that_differ_only_by_case() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "Issues",
                "tool": "list_issues",
                "columns": [{ "name": "id", "type": "Utf8" }]
            }],
            "functions": [{
                "name": "issues",
                "tool": "search_issues",
                "args": [],
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("case-insensitive table/function name collision should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' declares both a table and function named 'issues'"
        );
    }

    #[test]
    fn rejects_duplicate_mcp_table_names() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [
                {
                    "name": "issues",
                    "tool": "list_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                },
                {
                    "name": "issues",
                    "tool": "list_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }
            ]
        }))
        .expect_err("duplicate table names should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' table 'issues' is declared more than once"
        );
    }

    #[test]
    fn rejects_duplicate_mcp_table_names_that_differ_only_by_case() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [
                {
                    "name": "Issues",
                    "tool": "list_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                },
                {
                    "name": "issues",
                    "tool": "list_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }
            ]
        }))
        .expect_err("case-insensitive duplicate table names should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' table 'issues' is declared more than once"
        );
    }

    #[test]
    fn rejects_duplicate_mcp_function_names_that_differ_only_by_case() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "functions": [
                {
                    "name": "Search",
                    "tool": "search_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                },
                {
                    "name": "search",
                    "tool": "search_issues",
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }
            ]
        }))
        .expect_err("case-insensitive duplicate function names should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' function 'search' is declared more than once"
        );
    }

    #[test]
    fn rejects_mcp_server_env_referencing_state() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": {
                "transport": "stdio",
                "command": "demo-mcp-server",
                "env": [{
                    "name": "CURSOR",
                    "from": "state",
                    "key": "cursor"
                }]
            },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("state reference in server env should fail");

        assert!(
            error
                .to_string()
                .contains("MCP server env 'CURSOR' uses state value 'cursor'"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_mcp_server_env_template_referencing_filter() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": {
                "transport": "stdio",
                "command": "demo-mcp-server",
                "env": [{
                    "name": "FILTERED",
                    "from": "template",
                    "template": "{{filter.state}}"
                }]
            },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("filter template reference in server env should fail");

        assert!(
            error
                .to_string()
                .contains("MCP server env 'FILTERED' template references table filter 'state'"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_tool_args_referencing_filters() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "tool_args": {
                    "state": { "from": "filter", "key": "state" }
                },
                "filters": [{
                    "name": "state",
                    "tool_arg": "state"
                }],
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("filter reference in tool_args should fail");

        assert!(
            error
                .to_string()
                .contains("references filter 'state'; bind filters through filters[].tool_arg"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_tool_args_referencing_state() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "tool_args": {
                    "cursor": { "from": "state", "key": "cursor" }
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("state reference in tool_args should fail");

        assert!(
            error
                .to_string()
                .contains("tool_args.cursor uses state value 'cursor'"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_tool_args_template_referencing_state() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "tool_args": {
                    "cursor": { "from": "template", "template": "{{state.cursor}}" }
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("state template reference in tool_args should fail");

        assert!(
            error
                .to_string()
                .contains("tool_args.cursor template references state value 'cursor'"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_duplicate_tool_arg_bindings_across_filters_and_tool_args() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "tool_args": {
                    "state": { "from": "literal", "value": "open" }
                },
                "filters": [{
                    "name": "state",
                    "tool_arg": "state"
                }],
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("duplicate tool arg bindings should fail");

        assert!(
            error
                .to_string()
                .contains("binds tool arg 'state' that is already bound"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_table_without_columns() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues"
            }]
        }))
        .expect_err("missing columns should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' table 'issues' must define columns"
        );
    }

    #[test]
    fn parses_table_with_limit_binding() {
        let manifest = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "limit_binding": { "tool_arg": "page_size", "max": 200 },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("manifest with limit_binding should parse");

        let table = manifest.tables.first().expect("one table");
        let binding = table.limit_binding.as_ref().expect("binding present");
        assert_eq!(binding.tool_arg, "page_size");
        assert_eq!(binding.max, Some(200));
    }

    #[test]
    fn parses_table_with_cursor_pagination() {
        let manifest = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "pagination": {
                    "cursor_arg": "cursor",
                    "response_cursor_path": ["meta", "nextCursor"],
                    "max_pages": 5
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("manifest with pagination should parse");

        let table = manifest.tables.first().expect("one table");
        let pagination = table.pagination.as_ref().expect("pagination present");
        assert_eq!(pagination.cursor_arg, "cursor");
        assert_eq!(pagination.response_cursor_path, ["meta", "nextCursor"]);
        assert_eq!(pagination.max_pages, Some(5));
    }

    #[test]
    fn rejects_pagination_colliding_with_filter() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "filters": [{
                    "name": "state",
                    "tool_arg": "cursor"
                }],
                "pagination": {
                    "cursor_arg": "cursor",
                    "response_cursor_path": ["nextCursor"]
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("pagination cursor colliding with filter should fail");

        assert!(
            error
                .to_string()
                .contains("pagination binds tool arg 'cursor' that is already bound"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_limit_binding_colliding_with_filter() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" },
            "tables": [{
                "name": "issues",
                "tool": "list_issues",
                "filters": [{
                    "name": "state",
                    "tool_arg": "state"
                }],
                "limit_binding": { "tool_arg": "state" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("limit_binding colliding with filter should fail");

        assert!(
            error
                .to_string()
                .contains("limit_binding binds tool arg 'state' that is already bound"),
            "got: {error}"
        );
    }

    #[test]
    fn rejects_manifest_without_tables_or_functions() {
        let error = McpSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "demo-mcp-server" }
        }))
        .expect_err("manifest needs at least one tool surface");

        assert_eq!(
            error.to_string(),
            "source 'demo' must define at least one function or table"
        );
    }
}
