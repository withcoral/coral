//! Native file table providers built on `DataFusion` file scan primitives.

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat as DataFusionFileFormat;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_datasource::table_schema::TableSchema;
use futures::TryStreamExt as _;
use object_store::ObjectStore;

use crate::backends::schema_from_columns;
use coral_spec::backends::file::{FileFormat, FileTableSpec};

use super::file_groups::{FileGroupsForScan, file_groups_for_scan};
use super::listing::{PreparedListingTable, prepare_listing_table};
use super::metadata::FileMetadataColumns;
use super::parquet_schema::infer_schema_expand_dicts;
use super::partitions::{
    PartitionColumns, filter_is_supported_partition_filter, filter_references_partition,
};

#[derive(Debug)]
pub(crate) struct FileTableProvider {
    inner: FileTableProviderInner,
}

#[derive(Debug)]
enum FileTableProviderInner {
    Listing(ListingTable),
    Metadata(MetadataFileTableProvider),
}

#[derive(Debug)]
struct MetadataFileTableProvider {
    table_path: datafusion::datasource::listing::ListingTableUrl,
    object_store: Arc<dyn ObjectStore>,
    file_extension: String,
    format: Arc<dyn DataFusionFileFormat>,
    schema: SchemaRef,
    table_schema: TableSchema,
    metadata_columns: FileMetadataColumns,
    partition_columns: PartitionColumns,
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
        let inner = Self::build_provider(
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
        let PreparedListingTable {
            table_path,
            listing_options,
            partition_columns,
            ..
        } = prepare_listing_table(&ctx, source_schema, table, home_dir, resolved_inputs).await?;

        let file_schema =
            file_schema_for_table(&ctx, table, source_schema, &listing_options, &table_path)
                .await?;
        let file_schema = strip_partition_columns(file_schema, &partition_columns);

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(file_schema);

        Ok(ListingTable::try_new(config)?
            .with_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache()))
    }

    async fn build_provider(
        ctx: SessionContext,
        source_schema: &str,
        table: &FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<FileTableProviderInner> {
        let metadata_columns = FileMetadataColumns::try_new(&table.source.metadata)?;
        if metadata_columns.has_row_columns() {
            return Err(DataFusionError::Plan(
                "row-scoped file metadata is only supported by JSONL file tables".to_string(),
            ));
        }
        if metadata_columns.has_file_columns() {
            return MetadataFileTableProvider::try_new(
                ctx,
                source_schema,
                table,
                home_dir,
                resolved_inputs,
                metadata_columns,
            )
            .await
            .map(FileTableProviderInner::Metadata);
        }

        Self::build_listing_table(ctx, source_schema, table, home_dir, resolved_inputs)
            .await
            .map(FileTableProviderInner::Listing)
    }
}

impl MetadataFileTableProvider {
    async fn try_new(
        ctx: SessionContext,
        source_schema: &str,
        table: &FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
        metadata_columns: FileMetadataColumns,
    ) -> Result<Self> {
        let PreparedListingTable {
            table_path,
            object_store,
            listing_options,
            partition_columns,
        } = prepare_listing_table(&ctx, source_schema, table, home_dir, resolved_inputs).await?;

        let file_schema =
            file_schema_for_table(&ctx, table, source_schema, &listing_options, &table_path)
                .await?;
        let file_schema = strip_partition_columns(file_schema, &partition_columns);
        metadata_columns.reject_schema_collisions(&file_schema, source_schema, table.name())?;
        let table_schema = TableSchema::new(file_schema, partition_columns.arrow_fields());
        let table_schema = metadata_columns.extend_table_schema_if_present(table_schema);
        let schema = Arc::clone(table_schema.table_schema());

        Ok(Self {
            table_path,
            object_store,
            file_extension: listing_options.file_extension,
            format: listing_options.format,
            schema,
            table_schema,
            metadata_columns,
            partition_columns,
        })
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let files = self
            .table_path
            .list_all_files(state, self.object_store.as_ref(), &self.file_extension)
            .await?
            .try_collect()
            .await?;

        let FileGroupsForScan {
            groups,
            grouped_by_partition,
        } = file_groups_for_scan(
            &self.table_path,
            &self.partition_columns,
            files,
            &self.metadata_columns,
            filters,
            state.config().target_partitions(),
            state.config_options().optimizer.preserve_file_partitions,
        )?;

        let file_source = self.format.file_source(self.table_schema.clone());
        let config = FileScanConfigBuilder::new(self.table_path.object_store(), file_source)
            .with_file_groups(groups)
            .with_statistics(Statistics::new_unknown(&self.schema))
            .with_limit(limit)
            .with_partitioned_by_file_group(grouped_by_partition)
            .with_projection_indices(projection.cloned())?
            .build();

        self.format.create_physical_plan(state, config).await
    }
}

async fn file_schema_for_table(
    ctx: &SessionContext,
    table: &FileTableSpec,
    source_schema: &str,
    listing_options: &datafusion::datasource::listing::ListingOptions,
    table_path: &datafusion::datasource::listing::ListingTableUrl,
) -> Result<SchemaRef> {
    if table.format == FileFormat::Parquet && !table.has_explicit_columns() {
        infer_schema_expand_dicts(ctx, listing_options, table_path).await
    } else {
        schema_from_columns(table.columns(), source_schema, table.name())
    }
}

fn strip_partition_columns(
    file_schema: SchemaRef,
    partition_columns: &PartitionColumns,
) -> SchemaRef {
    // Strip partition columns from the file schema. If an older writer stored
    // partition columns (e.g. `_part_id`) inside the Parquet files, schema
    // inference will include them here. DataFusion's file scan adds partition
    // columns from the path; having them in both places produces a
    // duplicate-field schema that causes queries to return zero rows.
    let partition_names: HashSet<&str> = partition_columns.names().collect();
    if partition_names.is_empty() {
        return file_schema;
    }

    let fields: Vec<_> = file_schema
        .fields()
        .iter()
        .filter(|f| !partition_names.contains(&f.name().as_str()))
        .cloned()
        .collect();
    Arc::new(Schema::new_with_metadata(
        fields,
        file_schema.metadata().clone(),
    ))
}

#[async_trait]
impl TableProvider for FileTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match &self.inner {
            FileTableProviderInner::Listing(inner) => inner.schema(),
            FileTableProviderInner::Metadata(inner) => inner.schema.clone(),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        match &self.inner {
            FileTableProviderInner::Listing(inner) => inner.supports_filters_pushdown(filters),
            FileTableProviderInner::Metadata(inner) => Ok(filters
                .iter()
                .map(|filter| {
                    if filter_is_supported_partition_filter(filter, &inner.partition_columns) {
                        TableProviderFilterPushDown::Exact
                    } else if filter_references_partition(filter, &inner.partition_columns) {
                        TableProviderFilterPushDown::Inexact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect()),
        }
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &self.inner {
            FileTableProviderInner::Listing(inner) => {
                inner.scan(state, projection, filters, limit).await
            }
            FileTableProviderInner::Metadata(inner) => {
                inner.scan(state, projection, filters, limit).await
            }
        }
    }
}
