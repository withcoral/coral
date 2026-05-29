use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::ResponseSpec;
use coral_spec::backends::mcp::{McpPaginationSpec, McpTableFunctionSpec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use serde_json::Value;

use super::client::McpSourceClient;
use super::error::McpProviderQueryError;
use super::fetch::McpFetchPlan;
use crate::backends::schema_from_columns;
use crate::backends::shared::json_exec::JsonExec;
use crate::backends::shared::mapping::convert_items;

#[derive(Clone)]
pub(super) struct McpSourceTableFunction {
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
    pagination: Option<McpPaginationSpec>,
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

impl McpSourceTableFunction {
    pub(super) fn new(
        backend: McpSourceClient,
        source_schema: String,
        function: McpTableFunctionSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(function.columns(), &source_schema, function.name())?;
        let function_name = function.name().to_string();
        let tool_name = function.tool.clone();
        let response = function.common.response.clone();
        let columns = function.common.columns.clone();
        let fetch_limit_default = function.fetch_limit_default();
        let pagination = function.pagination.clone();
        Ok(Self {
            spec: Arc::new(function),
            state: Arc::new(McpFunctionState {
                backend,
                source_schema,
                function_name,
                tool_name,
                schema,
                response,
                pagination,
                columns: Arc::from(columns),
                fetch_limit_default,
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
    arg_values: HashMap<String, Value>,
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
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        let fetcher = Arc::new(McpFetchPlan {
            backend: self.state.backend.clone(),
            source_schema: self.state.source_schema.clone(),
            relation: self.state.function_name.clone(),
            tool_name: self.state.tool_name.clone(),
            arguments,
            source_inputs: None,
            source_tool_args: Arc::new(BTreeMap::default()),
            response: self.state.response.clone(),
            pagination: self.state.pagination.clone(),
            limit: limit.or(self.state.fetch_limit_default),
        });
        let arg_strings: Arc<HashMap<String, String>> =
            Arc::new(arg_values_as_strings(&self.arg_values));
        let converter = {
            let columns = Arc::clone(&self.state.columns);
            let schema = self.state.schema.clone();
            let args = Arc::clone(&arg_strings);
            Arc::new(move |items: &[Value]| {
                convert_items(&columns, schema.clone(), &HashMap::new(), &args, items)
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

struct FunctionCallContext<'a> {
    source_schema: &'a str,
    function_name: &'a str,
}

fn bind_function_args(
    source_schema: &str,
    function: &McpTableFunctionSpec,
    args: &[Expr],
) -> Result<HashMap<String, Value>> {
    let context = FunctionCallContext {
        source_schema,
        function_name: function.name(),
    };
    ensure_no_extra_args(&context, function.args().len(), args.len())?;

    let mut required_missing = Vec::new();
    let mut arg_values = HashMap::with_capacity(function.args().len());

    for (index, spec) in function.args().iter().enumerate() {
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
) -> Result<Option<Value>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    if is_null_literal(expr) {
        return Ok(None);
    }
    let Some(value) = literal_to_json_value(expr) else {
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

fn literal_to_json_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(value, _) => scalar_value_to_json(value),
        Expr::Cast(cast) => literal_to_json_value(cast.expr.as_ref()),
        Expr::TryCast(cast) => literal_to_json_value(cast.expr.as_ref()),
        _ => None,
    }
}

/// Renders the typed function call args as the `String` map `convert_items`
/// expects when resolving `expr.kind: from_arg`. Strings pass through as-is;
/// other JSON scalars are stringified so a column reading `from_arg` sees the
/// same surface text the user typed in SQL.
fn arg_values_as_strings(arg_values: &HashMap<String, Value>) -> HashMap<String, String> {
    arg_values
        .iter()
        .map(|(key, value)| {
            let text = match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            (key.clone(), text)
        })
        .collect()
}

fn scalar_value_to_json(value: &ScalarValue) -> Option<Value> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(Value::String(value.clone())),
        ScalarValue::Boolean(Some(value)) => Some(Value::Bool(*value)),
        ScalarValue::Int8(Some(value)) => Some(Value::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(Value::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(Value::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(Value::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(Value::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(Value::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(Value::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(Value::from(*value)),
        ScalarValue::Float32(Some(value)) => {
            serde_json::Number::from_f64(f64::from(*value)).map(Value::Number)
        }
        ScalarValue::Float64(Some(value)) => {
            serde_json::Number::from_f64(*value).map(Value::Number)
        }
        _ => None,
    }
}

fn ensure_call_arg_allowed_value(
    context: &FunctionCallContext<'_>,
    arg: &str,
    value: &Value,
    allowed_values: &[String],
) -> Result<()> {
    let comparable_value = value_for_allowed_value_check(value);
    if !allowed_values.is_empty()
        && !allowed_values
            .iter()
            .any(|allowed| allowed == comparable_value.as_str())
    {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{arg}' has invalid value '{value}'; expected one of: {}",
            context.source_schema,
            context.function_name,
            allowed_values.join(", ")
        )));
    }
    Ok(())
}

fn value_for_allowed_value_check(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}
