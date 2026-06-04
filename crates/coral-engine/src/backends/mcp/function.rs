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

use super::McpToolCaller;
use super::error::McpProviderQueryError;
use super::fetch::McpFetchPlan;
use crate::backends::schema_from_columns;
use crate::backends::shared::function_args::{FunctionCallContext, bind_table_function_args};
use crate::backends::shared::json_exec::mapped_json_exec;

#[derive(Clone)]
pub(super) struct McpSourceTableFunction {
    spec: Arc<McpTableFunctionSpec>,
    state: Arc<McpFunctionState>,
}

struct McpFunctionState {
    backend: Arc<dyn McpToolCaller>,
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
        backend: Arc<dyn McpToolCaller>,
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
        mapped_json_exec(
            &self.state.source_schema,
            &self.state.function_name,
            self.state.schema.clone(),
            Arc::clone(&self.state.columns),
            fetcher,
            (Arc::new(HashMap::new()), arg_strings),
            projection.cloned(),
        )
    }
}

fn bind_function_args(
    source_schema: &str,
    function: &McpTableFunctionSpec,
    args: &[Expr],
) -> Result<HashMap<String, Value>> {
    bind_table_function_args(
        &FunctionCallContext {
            source_schema,
            function_name: function.name(),
        },
        function.args(),
        args,
        literal_to_json_value,
        value_for_allowed_value_check,
        |context, args| {
            DataFusionError::External(Box::new(
                McpProviderQueryError::MissingRequiredFunctionArg {
                    schema: context.source_schema.to_string(),
                    function: context.function_name.to_string(),
                    args,
                },
            ))
        },
    )
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

fn value_for_allowed_value_check(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}
