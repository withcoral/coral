//! MCP-backed source runtime pieces.

pub(crate) mod error;

pub(crate) use error::McpProviderQueryError;

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::{
    McpServerSpec, McpSourceManifest, McpTableFunctionSpec, McpTableSpec,
};
use coral_spec::{FilterMode, ResponseSpec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientInfo, Implementation, JsonObject};
use rmcp::transport::ConfigureCommandExt;
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use tokio::process::Command;

use crate::backends::shared::filter_expr::{extract_filter_values, literal_to_string};
use crate::backends::shared::json_exec::{JsonExec, RowFetcher};
use crate::backends::shared::mapping::convert_items;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{RenderContext, resolve_value_source};
use crate::backends::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, RegisteredTableFunction, SourceTableFunctions, build_registered_inputs,
    internal_table_function_name, registered_columns_from_specs, schema_from_columns,
};

#[derive(Debug, Clone)]
struct McpCompiledSource {
    manifest: McpSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
    resolved_inputs: Arc<BTreeMap<String, String>>,
    caller: McpSourceClient,
}

#[derive(Clone)]
struct McpSourceClient {
    caller: Arc<dyn McpToolCaller>,
}

impl std::fmt::Debug for McpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpSourceClient").finish_non_exhaustive()
    }
}

#[async_trait]
trait McpToolCaller: std::fmt::Debug + Send + Sync {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value>;
}

#[derive(Debug)]
struct StdioMcpToolCaller {
    source_name: String,
    server: McpServerSpec,
    resolved_inputs: Arc<BTreeMap<String, String>>,
}

#[derive(Clone)]
struct McpSourceTableFunction {
    spec: Arc<McpTableFunctionSpec>,
    state: Arc<McpFunctionState>,
}

struct McpFunctionState {
    backend: McpSourceClient,
    source_schema: String,
    function_name: String,
    tool_name: String,
    schema: SchemaRef,
    response: ResponseSpec,
    columns: Arc<[coral_spec::ColumnSpec]>,
    fetch_limit_default: Option<usize>,
}

impl std::fmt::Debug for McpSourceTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpSourceTableFunction")
            .field("source_schema", &self.state.source_schema)
            .field("function", &self.state.function_name)
            .field("tool", &self.state.tool_name)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for McpFunctionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpFunctionState")
            .field("source_schema", &self.source_schema)
            .field("function", &self.function_name)
            .field("tool", &self.tool_name)
            .finish_non_exhaustive()
    }
}

pub(crate) fn compile_manifest(
    manifest: &McpSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        &manifest.declared_inputs,
        &request.source_secrets,
        &request.source_variables,
    ));
    let caller = Arc::new(StdioMcpToolCaller {
        source_name: manifest.common.name.clone(),
        server: manifest.server.clone(),
        resolved_inputs: Arc::clone(&resolved_inputs),
    });
    compile_source_with_caller(
        manifest.clone(),
        request.source_secrets.clone(),
        request.source_variables.clone(),
        resolved_inputs,
        caller,
    )
}

fn compile_source_with_caller(
    manifest: McpSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
    resolved_inputs: Arc<BTreeMap<String, String>>,
    caller: Arc<dyn McpToolCaller>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(McpCompiledSource {
        manifest,
        source_secrets,
        source_variables,
        resolved_inputs,
        caller: McpSourceClient { caller },
    })
}

#[async_trait]
impl CompiledBackendSource for McpCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    async fn register(
        &self,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> Result<BackendRegistration> {
        let mut table_functions =
            SourceTableFunctions::with_capacity(self.manifest.functions.len());
        let mut table_function_infos = Vec::with_capacity(self.manifest.functions.len());

        for function in &self.manifest.functions {
            let internal_name =
                internal_table_function_name(&self.manifest.common.name, &function.name);
            let function_impl: Arc<dyn TableFunctionImpl> = Arc::new(McpSourceTableFunction::new(
                self.caller.clone(),
                self.manifest.common.name.clone(),
                function.clone(),
            )?);
            table_functions.insert(internal_name.clone(), function_impl);
            table_function_infos.push(registered_table_function(
                &self.manifest.common.name,
                function,
                internal_name,
            ));
        }

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.manifest.tables.len());
        for table in &self.manifest.tables {
            let provider: Arc<dyn TableProvider> = Arc::new(McpTableProvider::new(
                self.caller.clone(),
                self.manifest.common.name.clone(),
                Arc::clone(&self.resolved_inputs),
                table.clone(),
            )?);
            tables.insert(table.name.clone(), provider);
            table_infos.push(registered_table(table));
        }

        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs = build_registered_inputs(
            &self.manifest.declared_inputs,
            &self.source_variables,
            &secret_keys,
        );

        Ok(BackendRegistration {
            tables,
            table_functions,
            source: RegisteredSource {
                schema_name: self.manifest.common.name.clone(),
                tables: table_infos,
                table_functions: table_function_infos,
                inputs,
            },
        })
    }
}

impl McpSourceTableFunction {
    fn new(
        backend: McpSourceClient,
        source_schema: String,
        function: McpTableFunctionSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(&function.columns, &source_schema, &function.name)?;
        let function_name = function.name.clone();
        let tool_name = function.tool.clone();
        Ok(Self {
            spec: Arc::new(function.clone()),
            state: Arc::new(McpFunctionState {
                backend,
                source_schema,
                function_name,
                tool_name,
                schema,
                response: function.response,
                columns: Arc::from(function.columns),
                fetch_limit_default: function.fetch_limit_default,
            }),
        })
    }
}

impl TableFunctionImpl for McpSourceTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let arg_values = bind_function_args(&self.state.source_schema, &self.spec, args)?;
        Ok(Arc::new(McpFunctionCallTableProvider {
            state: Arc::clone(&self.state),
            arg_values,
        }))
    }
}

struct McpFunctionCallTableProvider {
    state: Arc<McpFunctionState>,
    arg_values: HashMap<String, String>,
}

impl std::fmt::Debug for McpFunctionCallTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpFunctionCallTableProvider")
            .field("source_schema", &self.state.source_schema)
            .field("function", &self.state.function_name)
            .field("arg_values", &self.arg_values.keys())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for McpFunctionCallTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.state.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let arguments = self
            .arg_values
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<serde_json::Map<_, _>>();
        let fetcher = Arc::new(McpFetchPlan {
            backend: self.state.backend.clone(),
            relation: self.state.function_name.clone(),
            tool_name: self.state.tool_name.clone(),
            arguments,
            response: self.state.response.clone(),
            limit: limit.or(self.state.fetch_limit_default),
        });
        let converter = {
            let columns = Arc::clone(&self.state.columns);
            let schema = self.state.schema.clone();
            Arc::new(move |items: &[Value]| {
                convert_items(&columns, schema.clone(), &HashMap::new(), items)
            })
        };
        let exec = JsonExec::new(
            &self.state.source_schema,
            &self.state.function_name,
            self.state.schema.clone(),
            fetcher,
            converter,
            projection.cloned(),
        )?;
        Ok(Arc::new(exec))
    }
}

struct McpTableProvider {
    backend: McpSourceClient,
    source_schema: String,
    resolved_inputs: Arc<BTreeMap<String, String>>,
    table: Arc<McpTableSpec>,
    schema: SchemaRef,
}

impl std::fmt::Debug for McpTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpTableProvider")
            .field("source_schema", &self.source_schema)
            .field("table", &self.table.name)
            .field("tool", &self.table.tool)
            .finish_non_exhaustive()
    }
}

impl McpTableProvider {
    fn new(
        backend: McpSourceClient,
        source_schema: String,
        resolved_inputs: Arc<BTreeMap<String, String>>,
        table: McpTableSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(&table.columns, &source_schema, &table.name)?;
        Ok(Self {
            backend,
            source_schema,
            resolved_inputs,
            table: Arc::new(table),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for McpTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let allowed: std::collections::HashSet<&str> =
            self.table.filters.iter().map(|f| f.name.as_str()).collect();
        let filter_modes: HashMap<&str, FilterMode> = self
            .table
            .filters
            .iter()
            .map(|f| (f.name.as_str(), f.mode))
            .collect();

        Ok(filters
            .iter()
            .map(|expr| classify_filter(expr, &allowed, &filter_modes))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let declared_filters = self.table.filter_specs();
        let filter_values = extract_filter_values(filters, &declared_filters);

        let mut arguments = JsonObject::new();

        let render_context = RenderContext::source_scoped(&self.resolved_inputs);
        for (name, source) in &self.table.tool_args {
            if let Some(value) = resolve_value_source(source, &render_context)? {
                arguments.insert(name.clone(), value);
            }
        }

        for filter in &self.table.filters {
            match filter_values.get(&filter.name) {
                Some(value) => {
                    arguments.insert(filter.tool_arg.clone(), Value::String(value.clone()));
                }
                None if filter.required => {
                    return Err(DataFusionError::External(Box::new(
                        McpProviderQueryError::MissingRequiredFilter {
                            schema: self.source_schema.clone(),
                            table: self.table.name.clone(),
                            column: filter.name.clone(),
                        },
                    )));
                }
                None => {}
            }
        }

        let effective_limit = effective_limit(
            limit,
            self.table.fetch_limit_default,
            self.table.limit_binding.as_ref().and_then(|b| b.max),
        );

        if let (Some(binding), Some(limit)) = (self.table.limit_binding.as_ref(), effective_limit) {
            arguments.insert(binding.tool_arg.clone(), Value::from(limit));
        }

        let fetcher = Arc::new(McpFetchPlan {
            backend: self.backend.clone(),
            relation: self.table.name.clone(),
            tool_name: self.table.tool.clone(),
            arguments,
            response: self.table.response.clone(),
            limit: effective_limit,
        });
        let columns: Arc<[coral_spec::ColumnSpec]> = Arc::from(self.table.columns.clone());
        let schema = self.schema.clone();
        let filter_values_arc = Arc::new(filter_values);
        let converter = {
            let columns = Arc::clone(&columns);
            let schema = schema.clone();
            let filter_values = Arc::clone(&filter_values_arc);
            Arc::new(move |items: &[Value]| {
                convert_items(&columns, schema.clone(), &filter_values, items)
            })
        };

        let exec = JsonExec::new(
            &self.source_schema,
            &self.table.name,
            schema,
            fetcher,
            converter,
            projection.cloned(),
        )?;
        Ok(Arc::new(exec))
    }
}

/// Combines SQL `LIMIT`, the manifest's `fetch_limit_default`, and an
/// optional `limit_binding.max` cap into a single effective row count.
fn effective_limit(
    sql_limit: Option<usize>,
    fetch_limit_default: Option<usize>,
    binding_max: Option<usize>,
) -> Option<usize> {
    let base = sql_limit.or(fetch_limit_default);
    match (base, binding_max) {
        (Some(base), Some(max)) => Some(base.min(max)),
        (Some(base), None) => Some(base),
        (None, Some(max)) => Some(max),
        (None, None) => None,
    }
}

fn classify_filter(
    expr: &Expr,
    allowed: &std::collections::HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
) -> TableProviderFilterPushDown {
    if let Expr::Column(col) = expr
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::Not(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::IsTrue(inner) | Expr::IsFalse(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
        && let Expr::Column(col) = binary.left.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(binary.right.as_ref()).is_some()
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::Like(like) = expr
        && !like.negated
        && let Expr::Column(col) = like.expr.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(like.pattern.as_ref()).is_some()
    {
        let mode = filter_modes.get(col.name()).copied().unwrap_or_default();
        if matches!(mode, FilterMode::Search | FilterMode::Contains) {
            return TableProviderFilterPushDown::Inexact;
        }
    }
    TableProviderFilterPushDown::Unsupported
}

#[derive(Debug)]
struct McpFetchPlan {
    backend: McpSourceClient,
    relation: String,
    tool_name: String,
    arguments: JsonObject,
    response: ResponseSpec,
    limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for McpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        let payload = self
            .backend
            .caller
            .call_tool(&self.relation, &self.tool_name, self.arguments.clone())
            .await?;
        let mut rows = extract_rows(&self.response, &payload);
        if let Some(limit) = self.limit
            && rows.len() > limit
        {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait]
impl McpToolCaller for StdioMcpToolCaller {
    async fn call_tool(
        &self,
        relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        let mut command = Command::new(&self.server.command);
        command.args(&self.server.args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let render_context = RenderContext::source_scoped(&self.resolved_inputs);
        for env in &self.server.env {
            let Some(value) = resolve_value_source(&env.value, &render_context)? else {
                continue;
            };
            command.env(&env.name, value_to_env_string(value));
        }

        let transport = rmcp::transport::TokioChildProcess::new(command.configure(|cmd| {
            cmd.kill_on_drop(true);
        }))
        .map_err(|error| {
            DataFusionError::External(Box::new(McpProviderQueryError::ServerStart {
                source_schema: self.source_name.clone(),
                detail: error.to_string(),
            }))
        })?;
        let client = McpClientHandler::new(&self.source_name)
            .serve(transport)
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::Initialize {
                    source_schema: self.source_name.clone(),
                    detail: error.to_string(),
                }))
            })?;
        let result = client
            .call_tool(CallToolRequestParams::new(tool_name.to_string()).with_arguments(arguments))
            .await
            .map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::ToolCall {
                    source_schema: self.source_name.clone(),
                    relation: relation.to_string(),
                    tool: tool_name.to_string(),
                    detail: error.to_string(),
                }))
            })?;
        normalize_tool_result(&self.source_name, relation, tool_name, result)
    }
}

#[derive(Debug, Clone)]
struct McpClientHandler {
    client_info: ClientInfo,
}

impl McpClientHandler {
    fn new(source_name: &str) -> Self {
        let mut client_info = ClientInfo::default();
        client_info.client_info = Implementation::new(
            format!("coral-engine/{source_name}"),
            env!("CARGO_PKG_VERSION"),
        );
        Self { client_info }
    }
}

impl ClientHandler for McpClientHandler {
    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

fn normalize_tool_result(
    source_schema: &str,
    relation: &str,
    tool_name: &str,
    result: CallToolResult,
) -> Result<Value> {
    if result.is_error.unwrap_or(false) {
        let detail = result
            .content
            .iter()
            .find_map(|content| content.as_text().map(|text| text.text.clone()))
            .unwrap_or_else(|| "tool reported isError=true with no content".to_string());
        return Err(DataFusionError::External(Box::new(
            McpProviderQueryError::ToolReturnedError {
                source_schema: source_schema.to_string(),
                relation: relation.to_string(),
                tool: tool_name.to_string(),
                detail,
            },
        )));
    }
    if let Some(value) = result.structured_content {
        return Ok(value);
    }
    for content in &result.content {
        if let Some(text) = content.as_text() {
            return serde_json::from_str(&text.text).map_err(|error| {
                DataFusionError::External(Box::new(McpProviderQueryError::ResultDecode {
                    source_schema: source_schema.to_string(),
                    relation: relation.to_string(),
                    tool: tool_name.to_string(),
                    detail: error.to_string(),
                }))
            });
        }
    }
    Ok(Value::Null)
}

fn value_to_env_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}

fn registered_table(table: &McpTableSpec) -> RegisteredTable {
    let required_filters: Vec<String> = table
        .filters
        .iter()
        .filter(|filter| filter.required)
        .map(|filter| filter.name.clone())
        .collect();
    let columns = registered_columns_from_specs(&table.columns, &required_filters);
    RegisteredTable {
        table_name: table.name.clone(),
        description: table.description.clone(),
        guide: table.guide.clone(),
        columns,
        required_filters,
    }
}

fn registered_table_function(
    schema_name: &str,
    function: &McpTableFunctionSpec,
    internal_name: String,
) -> RegisteredTableFunction {
    let arguments = function
        .args
        .iter()
        .map(|arg| {
            serde_json::json!({
                "name": arg.name,
                "required": arg.required,
                "values": arg.values,
            })
        })
        .collect::<Vec<_>>();
    let result_columns = registered_columns_from_specs(&function.columns, &[])
        .into_iter()
        .map(|column| {
            serde_json::json!({
                "name": column.name,
                "type": column.data_type,
                "nullable": column.nullable,
                "description": column.description,
            })
        })
        .collect::<Vec<_>>();

    RegisteredTableFunction {
        schema_name: schema_name.to_string(),
        function_name: function.name.clone(),
        internal_name,
        description: function.description.clone(),
        arguments_json: serde_json::to_string(&arguments).expect("arguments json"),
        result_columns_json: serde_json::to_string(&result_columns).expect("result columns json"),
        arg_names: function.args.iter().map(|arg| arg.name.clone()).collect(),
    }
}

struct FunctionCallContext<'a> {
    source_schema: &'a str,
    function_name: &'a str,
}

fn bind_function_args(
    source_schema: &str,
    function: &McpTableFunctionSpec,
    args: &[Expr],
) -> Result<HashMap<String, String>> {
    let context = FunctionCallContext {
        source_schema,
        function_name: function.name.as_str(),
    };
    ensure_no_extra_args(&context, function.args.len(), args.len())?;

    let mut required_missing = Vec::new();
    let mut arg_values = HashMap::with_capacity(function.args.len());

    for (index, spec) in function.args.iter().enumerate() {
        let Some(value) = resolve_call_arg_literal(&context, spec.name.as_str(), args.get(index))?
        else {
            if spec.required {
                required_missing.push(spec.name.as_str());
            }
            continue;
        };
        ensure_call_arg_allowed_value(&context, spec.name.as_str(), &value, &spec.values)?;
        arg_values.insert(spec.bind.arg.clone(), value);
    }

    if !required_missing.is_empty() {
        return Err(DataFusionError::External(Box::new(
            McpProviderQueryError::MissingRequiredFunctionArg {
                schema: context.source_schema.to_string(),
                function: context.function_name.to_string(),
                args: required_missing.iter().map(ToString::to_string).collect(),
            },
        )));
    }

    Ok(arg_values)
}

fn ensure_no_extra_args(
    context: &FunctionCallContext<'_>,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual > expected {
        return Err(DataFusionError::Plan(format!(
            "{}.{} expected at most {} arguments, got {}",
            context.source_schema, context.function_name, expected, actual
        )));
    }
    Ok(())
}

fn resolve_call_arg_literal(
    context: &FunctionCallContext<'_>,
    arg_name: &str,
    expr: Option<&Expr>,
) -> Result<Option<String>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    if is_null_literal(expr) {
        return Ok(None);
    }
    let Some(value) = literal_to_string(expr) else {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{}' must be a literal",
            context.source_schema, context.function_name, arg_name
        )));
    };
    Ok(Some(value))
}

fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => is_null_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => is_null_literal(cast.expr.as_ref()),
        _ => false,
    }
}

fn ensure_call_arg_allowed_value(
    context: &FunctionCallContext<'_>,
    arg: &str,
    value: &str,
    allowed_values: &[String],
) -> Result<()> {
    if !allowed_values.is_empty() && !allowed_values.iter().any(|allowed| allowed == value) {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{arg}' has invalid value '{value}'; expected one of: {}",
            context.source_schema,
            context.function_name,
            allowed_values.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QuerySource;
    use crate::runtime::catalog;
    use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
    use crate::runtime::source_functions::SourceFunctionRegistry;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionContext;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FakeMcpCaller {
        calls: Mutex<Vec<(String, JsonObject)>>,
    }

    #[async_trait]
    impl McpToolCaller for FakeMcpCaller {
        async fn call_tool(
            &self,
            _relation: &str,
            tool_name: &str,
            arguments: JsonObject,
        ) -> Result<Value> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((tool_name.to_string(), arguments.clone()));
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Ok(json!({
                "items": [
                    { "title": format!("{query} one"), "url": "https://example.com/1" },
                    { "title": format!("{query} two"), "url": "https://example.com/2" }
                ]
            }))
        }
    }

    /// Records each MCP tool call and returns a fixed payload for table tests.
    #[derive(Debug)]
    struct FakeMcpTableCaller {
        calls: Mutex<Vec<(String, JsonObject)>>,
    }

    #[async_trait]
    impl McpToolCaller for FakeMcpTableCaller {
        async fn call_tool(
            &self,
            _relation: &str,
            tool_name: &str,
            arguments: JsonObject,
        ) -> Result<Value> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((tool_name.to_string(), arguments.clone()));
            Ok(json!({
                "issues": [
                    { "id": "1", "title": "Bug A", "state": "open" },
                    { "id": "2", "title": "Bug B", "state": "open" },
                    { "id": "3", "title": "Bug C", "state": "closed" }
                ]
            }))
        }
    }

    fn mcp_manifest() -> coral_spec::ValidatedSourceManifest {
        coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": {
                "transport": "stdio",
                "command": "unused"
            },
            "functions": [{
                "name": "search",
                "tool": "search_tool",
                "args": [{
                    "name": "query",
                    "required": true,
                    "bind": { "arg": "query" }
                }],
                "response": {
                    "rows_path": ["items"]
                },
                "columns": [
                    { "name": "title", "type": "Utf8" },
                    { "name": "url", "type": "Utf8" }
                ]
            }]
        }))
        .expect("mcp manifest should parse")
    }

    fn compile_sources(
        manifest: coral_spec::ValidatedSourceManifest,
        caller: Arc<dyn McpToolCaller>,
    ) -> Vec<CompiledQuerySource> {
        let mcp_manifest = manifest.as_mcp().expect("mcp manifest").clone();
        let variables = BTreeMap::new();
        let secrets = BTreeMap::new();
        let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
            &mcp_manifest.declared_inputs,
            &secrets,
            &variables,
        ));
        let compiled = compile_source_with_caller(
            mcp_manifest,
            secrets.clone(),
            variables.clone(),
            resolved_inputs,
            caller,
        );
        vec![CompiledQuerySource {
            source: QuerySource::new(manifest, variables, secrets),
            compiled,
        }]
    }

    #[tokio::test]
    async fn executes_mcp_table_function_with_bound_args() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(&ctx, compile_sources(mcp_manifest(), caller.clone()));

        let batches = ctx
            .sql("SELECT title, url FROM test_mcp.search(query => 'issue') ORDER BY title")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| issue one"));
        assert!(rendered.contains("| issue two"));

        let calls = caller.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        let call = calls.first().expect("one MCP call should be recorded");
        assert_eq!(call.0, "search_tool");
        assert_eq!(
            call.1.get("query"),
            Some(&Value::String("issue".to_string()))
        );
    }

    #[tokio::test]
    async fn missing_required_function_arg_fails_planning() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(&ctx, compile_sources(mcp_manifest(), caller));

        let error = ctx
            .sql("SELECT title FROM test_mcp.search()")
            .await
            .expect_err("missing required arg should fail");

        assert!(
            error
                .to_string()
                .contains("test_mcp.search missing required argument(s): query"),
            "unexpected error: {error}"
        );
    }

    fn register_test_sources(ctx: &SessionContext, sources: Vec<CompiledQuerySource>) {
        let registration =
            register_sources_blocking(ctx, sources).expect("mcp source should register");
        let source_functions = SourceFunctionRegistry::new(
            registration
                .active_sources
                .iter()
                .flat_map(|source| source.table_functions.iter()),
        );
        ctx.register_relation_planner(Arc::new(source_functions))
            .expect("source function planner should register");
    }

    fn register_test_sources_with_catalog(ctx: &SessionContext, sources: Vec<CompiledQuerySource>) {
        let registration =
            register_sources_blocking(ctx, sources).expect("mcp source should register");
        catalog::register(ctx, &registration.active_sources).expect("catalog should register");
        let source_functions = SourceFunctionRegistry::new(
            registration
                .active_sources
                .iter()
                .flat_map(|source| source.table_functions.iter()),
        );
        ctx.register_relation_planner(Arc::new(source_functions))
            .expect("source function planner should register");
    }

    fn mcp_table_manifest() -> coral_spec::ValidatedSourceManifest {
        coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "unused" },
            "tables": [{
                "name": "issues",
                "description": "Open issues",
                "tool": "list_issues",
                "tool_args": {
                    "owner": { "from": "literal", "value": "acme" }
                },
                "filters": [{
                    "name": "state",
                    "required": false,
                    "tool_arg": "state"
                }],
                "response": { "rows_path": ["issues"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "title", "type": "Utf8" },
                    { "name": "state", "type": "Utf8" }
                ]
            }]
        }))
        .expect("mcp table manifest should parse")
    }

    fn mcp_table_required_filter_manifest() -> coral_spec::ValidatedSourceManifest {
        coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "unused" },
            "tables": [{
                "name": "issues",
                "description": "Open issues",
                "tool": "list_issues",
                "filters": [{
                    "name": "state",
                    "required": true,
                    "tool_arg": "state"
                }],
                "response": { "rows_path": ["issues"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "title", "type": "Utf8" },
                    { "name": "state", "type": "Utf8" }
                ]
            }]
        }))
        .expect("required-filter manifest should parse")
    }

    #[tokio::test]
    async fn scans_mcp_table_with_manifest_tool_args_and_no_filters() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller.clone()));

        let batches = ctx
            .sql("SELECT id, title, state FROM test_mcp.issues ORDER BY id")
            .await
            .expect("table query should plan")
            .collect()
            .await
            .expect("table query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| Bug A"));
        assert!(rendered.contains("| Bug B"));
        assert!(rendered.contains("| Bug C"));

        let calls = caller.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        let call = calls.first().expect("one MCP call");
        assert_eq!(call.0, "list_issues");
        assert_eq!(
            call.1.get("owner"),
            Some(&Value::String("acme".to_string()))
        );
        assert!(
            call.1.get("state").is_none(),
            "unbound optional filter should not be passed: {:?}",
            call.1
        );
    }

    #[tokio::test]
    async fn pushes_equality_filter_into_mcp_tool_arg() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller.clone()));

        let _ = ctx
            .sql("SELECT id FROM test_mcp.issues WHERE state = 'open'")
            .await
            .expect("filter query should plan")
            .collect()
            .await
            .expect("filter query should execute");

        let calls = caller.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        let call = calls.first().expect("one MCP call");
        assert_eq!(
            call.1.get("state"),
            Some(&Value::String("open".to_string()))
        );
    }

    #[tokio::test]
    async fn missing_required_filter_fails_planning() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(
            &ctx,
            compile_sources(mcp_table_required_filter_manifest(), caller.clone()),
        );

        let error = ctx
            .sql("SELECT id FROM test_mcp.issues")
            .await
            .expect("planning succeeds before scan")
            .collect()
            .await
            .expect_err("missing required filter should fail");

        let message = error.to_string();
        assert!(
            message.contains("test_mcp.issues table requires a constant equality filter"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("WHERE state = <constant>"),
            "missing column hint in error: {message}"
        );

        let root = error.find_root();
        match root {
            DataFusionError::External(inner) => {
                let provider = inner
                    .downcast_ref::<McpProviderQueryError>()
                    .expect("error should downcast to McpProviderQueryError");
                match provider {
                    McpProviderQueryError::MissingRequiredFilter {
                        schema,
                        table,
                        column,
                    } => {
                        assert_eq!(schema, "test_mcp");
                        assert_eq!(table, "issues");
                        assert_eq!(column, "state");
                    }
                    other => panic!("unexpected MCP error variant: {other:?}"),
                }
                let structured = provider.to_structured();
                assert_eq!(structured.reason(), "MISSING_REQUIRED_FILTER");
            }
            other => panic!("expected External error, got {other:?}"),
        }

        assert!(
            caller.calls.lock().expect("calls lock").is_empty(),
            "no MCP call should be made when planning fails"
        );
    }

    #[tokio::test]
    async fn applies_projection_and_limit_to_mcp_table_scan() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller));

        let batches = ctx
            .sql("SELECT title FROM test_mcp.issues ORDER BY id LIMIT 1")
            .await
            .expect("limit query should plan")
            .collect()
            .await
            .expect("limit query should execute");

        let total_rows: usize = batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(total_rows, 1);
        let schema = batches.first().expect("at least one batch").schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "title");
    }

    fn mcp_table_with_limit_binding_manifest(
        max: Option<usize>,
    ) -> coral_spec::ValidatedSourceManifest {
        let mut binding = serde_json::Map::new();
        binding.insert(
            "tool_arg".to_string(),
            Value::String("page_size".to_string()),
        );
        if let Some(max) = max {
            binding.insert("max".to_string(), serde_json::json!(max));
        }
        let binding = Value::Object(binding);
        coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_mcp",
            "version": "0.1.0",
            "backend": "mcp",
            "server": { "transport": "stdio", "command": "unused" },
            "tables": [{
                "name": "issues",
                "description": "issues with limit binding",
                "tool": "list_issues",
                "limit_binding": binding,
                "response": { "rows_path": ["issues"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "title", "type": "Utf8" },
                    { "name": "state", "type": "Utf8" }
                ]
            }]
        }))
        .expect("limit-binding manifest should parse")
    }

    #[tokio::test]
    async fn limit_binding_pushes_sql_limit_into_tool_arg() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(
            &ctx,
            compile_sources(mcp_table_with_limit_binding_manifest(None), caller.clone()),
        );

        let _ = ctx
            .sql("SELECT id FROM test_mcp.issues LIMIT 2")
            .await
            .expect("limit query should plan")
            .collect()
            .await
            .expect("limit query should execute");

        let calls = caller.calls.lock().expect("calls lock");
        let call = calls.first().expect("one MCP call");
        assert_eq!(call.1.get("page_size"), Some(&Value::from(2u64)));
    }

    #[tokio::test]
    async fn limit_binding_caps_sql_limit_at_manifest_max() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(
            &ctx,
            compile_sources(
                mcp_table_with_limit_binding_manifest(Some(2)),
                caller.clone(),
            ),
        );

        let batches = ctx
            .sql("SELECT id FROM test_mcp.issues LIMIT 1000")
            .await
            .expect("limit query should plan")
            .collect()
            .await
            .expect("limit query should execute");

        let calls = caller.calls.lock().expect("calls lock");
        let call = calls.first().expect("one MCP call");
        assert_eq!(
            call.1.get("page_size"),
            Some(&Value::from(2u64)),
            "expected SQL LIMIT to be capped at manifest max"
        );

        let total_rows: usize = batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(
            total_rows, 2,
            "post-response truncation should still apply at the manifest cap"
        );
    }

    #[tokio::test]
    async fn limit_binding_omits_arg_when_no_limit_set() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources(
            &ctx,
            compile_sources(mcp_table_with_limit_binding_manifest(None), caller.clone()),
        );

        let _ = ctx
            .sql("SELECT id FROM test_mcp.issues")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let calls = caller.calls.lock().expect("calls lock");
        let call = calls.first().expect("one MCP call");
        assert!(
            call.1.get("page_size").is_none(),
            "unbounded scan should not pass page_size: {:?}",
            call.1
        );
    }

    #[tokio::test]
    async fn mcp_table_appears_in_catalog_metadata() {
        let ctx = SessionContext::new();
        let caller = Arc::new(FakeMcpTableCaller {
            calls: Mutex::new(Vec::new()),
        });
        register_test_sources_with_catalog(&ctx, compile_sources(mcp_table_manifest(), caller));

        let batches = ctx
            .sql(
                "SELECT column_name FROM coral.columns \
                 WHERE schema_name = 'test_mcp' AND table_name = 'issues' \
                 ORDER BY column_name",
            )
            .await
            .expect("metadata query should plan")
            .collect()
            .await
            .expect("metadata query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| id"));
        assert!(rendered.contains("| title"));
        assert!(rendered.contains("| state"));
    }
}
