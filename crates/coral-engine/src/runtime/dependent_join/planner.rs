use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::backends::http::HttpSourceTableProvider;
use crate::runtime::dependent_join::exec::{DependentJoinExec, DependentJoinExecConfig};
use crate::runtime::dependent_join::logical::DependentJoinNode;

#[derive(Debug, Default)]
pub(crate) struct DependentJoinExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DependentJoinExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DependentJoinNode>() else {
            return Ok(None);
        };

        let [resolver] = physical_inputs else {
            return plan_err!("DependentJoinNode expected one physical resolver input");
        };

        let provider = resolve_http_provider(session_state, node).await?;
        let exec = DependentJoinExec::new(DependentJoinExecConfig {
            resolver: Arc::clone(resolver),
            dependent: provider.client,
            dependent_source_schema: provider.source_schema,
            table: provider.table,
            binding_keys: Arc::from(node.binding_keys.clone()),
            literal_filters: Arc::new(node.literal_filters.clone()),
            dependent_projection: Arc::from(node.dependent_projection.clone()),
            dependent_first: node.dependent_first,
            max_bindings: node.max_bindings,
            max_resolver_rows: node.max_resolver_rows,
            max_rows_per_binding: node.max_rows_per_binding,
            max_concurrency: node.max_concurrency,
            page_hint: node.page_hint,
            output_schema: Arc::new(node.schema.as_arrow().clone()),
        });

        Ok(Some(Arc::new(exec)))
    }
}

struct ResolvedHttpDependent {
    client: crate::backends::http::HttpSourceClient,
    source_schema: String,
    table: Arc<coral_spec::backends::http::HttpTableSpec>,
}

async fn resolve_http_provider(
    session_state: &SessionState,
    node: &DependentJoinNode,
) -> Result<ResolvedHttpDependent> {
    let catalog_options = &session_state.config_options().catalog;
    let table_ref = node.dependent_table.clone().resolve(
        &catalog_options.default_catalog,
        &catalog_options.default_schema,
    );
    let catalog = session_state
        .catalog_list()
        .catalog(&table_ref.catalog)
        .ok_or_else(|| {
            plan_datafusion_err!(
                "dependent table '{}' catalog '{}' is not registered",
                node.dependent_table,
                table_ref.catalog
            )
        })?;
    let schema = catalog.schema(&table_ref.schema).ok_or_else(|| {
        plan_datafusion_err!(
            "dependent table '{}' schema '{}' is not registered",
            node.dependent_table,
            table_ref.schema
        )
    })?;
    let provider = schema
        .table(&table_ref.table)
        .await?
        .ok_or_else(|| plan_datafusion_err!("dependent table '{}' is not registered", table_ref))?;

    let provider = provider
        .as_any()
        .downcast_ref::<HttpSourceTableProvider>()
        .ok_or_else(|| {
            plan_datafusion_err!(
                "dependent table '{}' is no longer HTTP-backed",
                node.dependent_table
            )
        })?;

    Ok(ResolvedHttpDependent {
        client: provider.client().clone(),
        source_schema: provider.source_schema().to_string(),
        table: Arc::clone(provider.table_spec()),
    })
}
