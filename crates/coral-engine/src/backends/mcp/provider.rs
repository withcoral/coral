use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::FilterMode;
use coral_spec::backends::mcp::McpTableSpec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use rmcp::model::JsonObject;
use serde_json::Value;

use super::client::McpSourceClient;
use super::error::McpProviderQueryError;
use super::fetch::McpFetchPlan;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{extract_filter_values, literal_to_string};
use crate::backends::shared::json_exec::JsonExec;
use crate::backends::shared::mapping::convert_items;
use crate::backends::shared::template::{RenderContext, resolve_value_source};

pub(super) struct McpTableProvider {
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
            .field("table", &self.table.name())
            .field("tool", &self.table.tool)
            .finish_non_exhaustive()
    }
}

impl McpTableProvider {
    pub(super) fn new(
        backend: McpSourceClient,
        source_schema: String,
        resolved_inputs: Arc<BTreeMap<String, String>>,
        table: McpTableSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(table.columns(), &source_schema, table.name())?;
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
        let allowed: std::collections::HashSet<&str> = self
            .table
            .filters()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let filter_modes: HashMap<&str, FilterMode> = self
            .table
            .filters()
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
        let filter_values = extract_filter_values(filters, self.table.filters());

        let mut arguments = JsonObject::new();

        let render_context = RenderContext::source_scoped(&self.resolved_inputs);
        for (name, source) in &self.table.tool_args {
            if let Some(value) = resolve_value_source(source, &render_context)? {
                arguments.insert(name.clone(), value);
            }
        }

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
                    arguments.insert(tool_arg.to_string(), Value::String(value.clone()));
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

        let effective_limit = effective_limit(
            limit,
            self.table.fetch_limit_default(),
            self.table.limit_binding.as_ref().and_then(|b| b.max),
        );

        if let (Some(binding), Some(limit)) = (self.table.limit_binding.as_ref(), effective_limit) {
            arguments.insert(binding.tool_arg.clone(), Value::from(limit));
        }

        let fetcher = Arc::new(McpFetchPlan {
            backend: self.backend.clone(),
            source_schema: self.source_schema.clone(),
            relation: self.table.name().to_string(),
            tool_name: self.table.tool.clone(),
            arguments,
            response: self.table.response.clone(),
            pagination: self.table.pagination.clone(),
            limit: effective_limit,
        });
        let columns: Arc<[coral_spec::ColumnSpec]> = Arc::from(self.table.columns().to_vec());
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
            self.table.name(),
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
