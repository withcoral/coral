use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::ManifestDataType;
use coral_spec::backends::mcp::McpTableSpec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use rmcp::model::JsonObject;
use serde_json::Value;

use super::McpSourceInputs;
use super::client::McpSourceClient;
use super::error::McpProviderQueryError;
use super::fetch::McpFetchPlan;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{classify_filter_pushdown, extract_filter_values};
use crate::backends::shared::json_exec::JsonExec;
use crate::backends::shared::mapping::convert_items;

pub(super) struct McpTableProvider {
    backend: McpSourceClient,
    source_schema: String,
    source_inputs: Arc<McpSourceInputs>,
    table: Arc<McpTableSpec>,
    schema: SchemaRef,
}

impl std::fmt::Debug for McpTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpTableProvider")
            .field("source_schema", &self.source_schema)
            .field("table", &self.table.name())
            .field("tool", &self.table.tool)
            .finish_non_exhaustive()
    }
}

impl McpTableProvider {
    pub(super) fn new(
        backend: McpSourceClient,
        source_schema: String,
        source_inputs: Arc<McpSourceInputs>,
        table: McpTableSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(table.columns(), &source_schema, table.name())?;
        Ok(Self {
            backend,
            source_schema,
            source_inputs,
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
        Ok(classify_filter_pushdown(filters, self.table.filters()))
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_values = extract_filter_values(filters, self.table.filters());

        let mut arguments = JsonObject::new();

        for filter in self.table.filters() {
            match filter_values.get(&filter.name) {
                Some(value) => {
                    let tool_arg =
                        self.table
                            .tool_arg_for_filter(&filter.name)
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "{}.{} filter '{}' is missing its MCP tool binding",
                                    self.source_schema,
                                    self.table.name(),
                                    filter.name
                                ))
                            })?;
                    let typed = coerce_filter_value(
                        &self.source_schema,
                        self.table.name(),
                        &filter.name,
                        filter
                            .manifest_data_type()
                            .map_err(|error| DataFusionError::Plan(error.to_string()))?,
                        value,
                    )?;
                    arguments.insert(tool_arg.to_string(), typed);
                }
                None if filter.required => {
                    return Err(DataFusionError::External(Box::new(
                        McpProviderQueryError::MissingRequiredFilter {
                            schema: self.source_schema.clone(),
                            table: self.table.name().to_string(),
                            column: filter.name.clone(),
                        },
                    )));
                }
                None => {}
            }
        }

        let limits = resolve_limits(
            limit,
            self.table.fetch_limit_default(),
            self.table.limit_binding.as_ref().and_then(|b| b.max),
        );

        if let (Some(binding), Some(push)) = (self.table.limit_binding.as_ref(), limits.push) {
            arguments.insert(binding.tool_arg.clone(), Value::from(push));
        }

        let fetcher = Arc::new(McpFetchPlan {
            backend: self.backend.clone(),
            source_schema: self.source_schema.clone(),
            relation: self.table.name().to_string(),
            tool_name: self.table.tool.clone(),
            arguments,
            source_inputs: Some(Arc::clone(&self.source_inputs)),
            source_tool_args: Arc::new(self.table.tool_args.clone()),
            response: self.table.response.clone(),
            pagination: self.table.pagination.clone(),
            limit: limits.truncate,
        });
        let columns: Arc<[coral_spec::ColumnSpec]> = Arc::from(self.table.columns().to_vec());
        let schema = self.schema.clone();
        let filter_values_arc = Arc::new(filter_values);
        let empty_args: Arc<HashMap<String, String>> = Arc::new(HashMap::new());
        let converter = {
            let columns = Arc::clone(&columns);
            let schema = schema.clone();
            let filter_values = Arc::clone(&filter_values_arc);
            let args = Arc::clone(&empty_args);
            Arc::new(move |items: &[Value]| {
                convert_items(&columns, schema.clone(), &filter_values, &args, items)
            })
        };

        let exec = JsonExec::new(
            &self.source_schema,
            self.table.name(),
            schema,
            fetcher,
            converter,
            projection.cloned(),
        )?;
        Ok(Arc::new(exec))
    }
}

/// Per-request pushdown vs. final row-count truncation for an MCP table scan.
///
/// `push` is the value to send via `limit_binding.tool_arg` (a per-request
/// cap or page size). `truncate` is the upper bound on the total rows the
/// scan returns, applied after accumulating across pages. `limit_binding.max`
/// only enters `push` — it must not cap the final row count, otherwise a
/// paginated table with `max = N` would make row `N + 1` unreachable even
/// when the server returns a next cursor.
struct ResolvedLimits {
    push: Option<usize>,
    truncate: Option<usize>,
}

fn resolve_limits(
    sql_limit: Option<usize>,
    fetch_limit_default: Option<usize>,
    binding_max: Option<usize>,
) -> ResolvedLimits {
    let truncate = sql_limit.or(fetch_limit_default);
    let push = match (truncate, binding_max) {
        (Some(base), Some(max)) => Some(base.min(max)),
        (Some(base), None) => Some(base),
        (None, Some(max)) => Some(max),
        (None, None) => None,
    };
    ResolvedLimits { push, truncate }
}

/// Parses the stringified filter value back into a JSON scalar that matches
/// the manifest's declared filter type. MCP tools whose `inputSchema` requires
/// `integer`, `boolean`, or `number` arguments reject string-wrapped values
/// like `"10"` or `"true"` — function args already preserve JSON scalar
/// types, and pushed table filters should too.
fn coerce_filter_value(
    source_schema: &str,
    table_name: &str,
    filter_name: &str,
    data_type: ManifestDataType,
    value: &str,
) -> Result<Value> {
    match data_type {
        ManifestDataType::Utf8 | ManifestDataType::Timestamp => {
            Ok(Value::String(value.to_string()))
        }
        ManifestDataType::Json => serde_json::from_str(value).map_err(|_unused| {
            invalid_filter_value_plan_error(source_schema, table_name, filter_name, "Json", value)
        }),
        ManifestDataType::Int64 => value.parse::<i64>().map(Value::from).map_err(|_unused| {
            invalid_filter_value_plan_error(source_schema, table_name, filter_name, "Int64", value)
        }),
        ManifestDataType::Float64 => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| {
                invalid_filter_value_plan_error(
                    source_schema,
                    table_name,
                    filter_name,
                    "Float64",
                    value,
                )
            }),
        ManifestDataType::Boolean => match value {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(invalid_filter_value_plan_error(
                source_schema,
                table_name,
                filter_name,
                "Boolean",
                value,
            )),
        },
    }
}

fn invalid_filter_value_plan_error(
    source_schema: &str,
    table_name: &str,
    filter_name: &str,
    declared: &str,
    value: &str,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{source_schema}.{table_name} filter '{filter_name}' is declared as {declared} but value \
         '{value}' could not be parsed as {declared}"
    ))
}
