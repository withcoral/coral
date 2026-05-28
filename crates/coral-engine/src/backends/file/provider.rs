//! Native `DataFusion` listing-table provider for file tables that need no Coral-specific row mapping.

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::backends::schema_from_columns;
use coral_spec::backends::file::{FileFormat, FileTableSpec};

use super::listing::{PreparedListingTable, prepare_listing_table};
use super::parquet_schema::infer_schema_expand_dicts;

#[derive(Debug)]
pub(crate) struct FileTableProvider {
    inner: ListingTable,
}

impl FileTableProvider {
    /// Build a file-backed table provider from a source manifest.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the file source configuration is
    /// invalid or the listing table cannot be constructed.
    #[cfg(test)]
    pub(crate) fn try_new(
        ctx: &SessionContext,
        source_schema: &str,
        table: FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Self> {
        futures::executor::block_on(Self::try_new_async(
            ctx,
            source_schema,
            table,
            home_dir,
            resolved_inputs,
        ))
    }

    pub(crate) async fn try_new_async(
        ctx: &SessionContext,
        source_schema: &str,
        table: FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let inner = Self::build_listing_table(
            ctx.clone(),
            source_schema,
            &table,
            home_dir,
            resolved_inputs,
        )
        .await?;
        Ok(Self { inner })
    }

    async fn build_listing_table(
        ctx: SessionContext,
        source_schema: &str,
        table: &FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<ListingTable> {
        let format = table.format;
        let PreparedListingTable {
            table_path,
            listing_options,
            partition_columns,
            ..
        } = prepare_listing_table(&ctx, source_schema, table, home_dir, resolved_inputs).await?;

        let mut file_schema = if format == FileFormat::Parquet && !table.has_explicit_columns() {
            infer_schema_expand_dicts(&ctx, &listing_options, &table_path).await?
        } else {
            schema_from_columns(table.columns(), source_schema, table.name())?
        };

        // Strip partition columns from the file schema. If an older writer
        // stored partition columns (e.g. `_part_id`) inside the Parquet files,
        // schema inference will include them here. DataFusion's ListingTable
        // adds partition columns from the hive directory path; having them in
        // both the file schema and the partition config produces a
        // duplicate-field schema that causes queries to return zero rows.
        let partition_names: HashSet<&str> = partition_columns.names().collect();
        if !partition_names.is_empty() {
            let fields: Vec<_> = file_schema
                .fields()
                .iter()
                .filter(|f| !partition_names.contains(&f.name().as_str()))
                .cloned()
                .collect();
            file_schema = Arc::new(Schema::new_with_metadata(
                fields,
                file_schema.metadata().clone(),
            ));
        }

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(file_schema);

        Ok(ListingTable::try_new(config)?
            .with_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache()))
    }
}

#[async_trait]
impl TableProvider for FileTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}
