use std::path::{Path, PathBuf};
use std::sync::Arc;

use coral_capabilities::Capability;
use coral_exports::{SqlBinding, SqlBindingKind};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_contains_udf, json_from_scalar_udf, json_get_array_udf,
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_json_udf, json_get_str_udf,
    json_get_udf, json_length_udf, json_object_keys_udf,
};

use crate::SqlResult;
use crate::info::{table_function_info_from_binding, table_info_from_binding};
use crate::metadata::{
    QueryExecution, QueryPlan, SqlMetadataInfo, SqlTableLookup, TableFunctionInfo, TableInfo,
};
use crate::table_provider::register_runtime_table;
use crate::validation::validate_read_only_sql;

/// App-resolved SQL runtime binding. The app owns export composition,
/// credentials, and installed artifact paths; this crate only executes SQL.
#[derive(Debug, Clone)]
pub struct SqlRuntimeBinding {
    pub capability: Capability,
    pub binding: SqlBinding,
    pub source_materialized_dir: PathBuf,
}

/// App-owned provider invocation hook used by provider-backed SQL scans.
#[async_trait::async_trait]
pub trait SqlProviderInvoker: std::fmt::Debug + Send + Sync {
    async fn invoke_provider(
        &self,
        request: SqlProviderInvocation<'_>,
    ) -> SqlResult<serde_json::Value>;
}

/// Provider invocation requested by a SQL scan.
pub struct SqlProviderInvocation<'a> {
    pub capability: &'a Capability,
    pub binding: &'a SqlBinding,
    pub source_materialized_dir: &'a Path,
    pub args: serde_json::Map<String, serde_json::Value>,
}

/// SQL runtime over app-resolved SQL bindings.
#[derive(Debug, Clone)]
pub struct SqlWorkspace {
    bindings: Vec<SqlRuntimeBinding>,
    provider_invoker: Option<Arc<dyn SqlProviderInvoker>>,
}

impl SqlWorkspace {
    #[must_use]
    pub fn new(bindings: Vec<SqlRuntimeBinding>) -> Self {
        Self {
            bindings,
            provider_invoker: None,
        }
    }

    #[must_use]
    pub fn with_provider_invoker(mut self, invoker: Arc<dyn SqlProviderInvoker>) -> Self {
        self.provider_invoker = Some(invoker);
        self
    }

    #[must_use]
    pub fn list_tables(
        &self,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Vec<TableInfo> {
        self.table_infos()
            .into_iter()
            .filter(|table| schema_filter.is_none_or(|filter| table.schema_name == filter))
            .filter(|table| table_filter.is_none_or(|filter| table.table_name == filter))
            .collect()
    }

    #[must_use]
    pub fn sql_metadata(&self, schema_filter: Option<&str>) -> SqlMetadataInfo {
        SqlMetadataInfo {
            tables: self.list_tables(schema_filter, None),
            table_functions: self
                .table_function_infos()
                .into_iter()
                .filter(|function| {
                    schema_filter.is_none_or(|filter| function.schema_name == filter)
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn lookup_sql_table(&self, schema_name: &str, table_name: &str) -> SqlTableLookup {
        let tables = self.table_infos();
        let table = tables
            .iter()
            .find(|table| table.schema_name == schema_name && table.table_name == table_name)
            .cloned();
        SqlTableLookup {
            table,
            missing_context_tables: tables,
        }
    }

    /// Executes one read-only SQL statement against the registered SQL bindings.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SqlError::InvalidInput`] when the statement is not a single
    /// read-only query or `DataFusion` rejects planning/execution. Returns
    /// [`crate::SqlError::Internal`] when Arrow result collection fails.
    pub async fn execute_sql(&self, sql: &str) -> SqlResult<QueryExecution> {
        validate_read_only_sql(sql)?;
        let ctx = self.session_context().await?;
        let dataframe = ctx.sql(sql).await?;
        let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
        let batches = dataframe.collect().await?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }

    /// Builds unoptimized, optimized, and physical plans for one read-only SQL statement.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SqlError::InvalidInput`] when the statement is not a single
    /// read-only query or `DataFusion` rejects planning. Returns other SQL
    /// errors if runtime table registration fails.
    pub async fn explain_sql(&self, sql: &str) -> SqlResult<QueryPlan> {
        validate_read_only_sql(sql)?;
        let ctx = self.session_context().await?;
        let dataframe = ctx.sql(sql).await?;
        let unoptimized = dataframe.logical_plan().display_indent_schema().to_string();
        let optimized = dataframe
            .clone()
            .into_optimized_plan()?
            .display_indent_schema()
            .to_string();
        let physical = format!("{:?}", dataframe.create_physical_plan().await?);
        Ok(QueryPlan::new(unoptimized, optimized, physical))
    }

    async fn session_context(&self) -> SqlResult<SessionContext> {
        let mut ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        register_json_functions(&mut ctx)?;
        for binding in &self.bindings {
            if binding.binding.kind != SqlBindingKind::Table {
                continue;
            }
            register_runtime_table(&ctx, binding, self.provider_invoker.clone()).await?;
        }
        Ok(ctx)
    }

    fn table_infos(&self) -> Vec<TableInfo> {
        let mut tables = self
            .bindings
            .iter()
            .filter(|binding| binding.binding.kind == SqlBindingKind::Table)
            .filter_map(table_info_from_binding)
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| {
            (&left.schema_name, &left.table_name).cmp(&(&right.schema_name, &right.table_name))
        });
        tables
    }

    fn table_function_infos(&self) -> Vec<TableFunctionInfo> {
        let mut functions = self
            .bindings
            .iter()
            .filter(|binding| binding.binding.kind == SqlBindingKind::Function)
            .filter_map(table_function_info_from_binding)
            .collect::<Vec<_>>();
        functions.sort_by(|left, right| {
            (&left.schema_name, &left.function_name)
                .cmp(&(&right.schema_name, &right.function_name))
        });
        functions
    }
}

fn register_json_functions(ctx: &mut SessionContext) -> SqlResult<()> {
    let functions: [Arc<ScalarUDF>; 12] = [
        json_get_udf(),
        json_get_bool_udf(),
        json_get_float_udf(),
        json_get_int_udf(),
        json_get_json_udf(),
        json_get_array_udf(),
        json_as_text_udf(),
        json_get_str_udf(),
        json_contains_udf(),
        json_length_udf(),
        json_object_keys_udf(),
        json_from_scalar_udf(),
    ];
    for udf in functions {
        FunctionRegistry::register_udf(ctx, udf)?;
    }
    Ok(())
}
