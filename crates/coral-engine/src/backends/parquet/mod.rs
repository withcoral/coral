//! `Parquet` table provider backed by local files or object-store URLs.

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::io::Cursor;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::ObjectStore;
use object_store::ObjectStoreExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{ChunkReader, Length};

use crate::backends::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, build_registered_inputs, build_registered_table, partition_columns_to_arrow,
    registered_columns_from_schema, registered_columns_from_specs, required_filter_names,
    schema_from_columns,
};
use coral_spec::backends::file::{FileTableSpec, ParquetSourceManifest};

const DEFAULT_PARQUET_EXTENSION: &str = ".parquet";

// Parquet footer layout (end of file):
//   [thrift-encoded FileMetaData: metadata_len bytes]
//   [4-byte LE int32: metadata_len]
//   [4-byte magic: "PAR1"]
const PARQUET_FOOTER_SIZE: u64 = 8;

#[derive(Debug, Clone)]
struct ParquetCompiledSource {
    manifest: ParquetSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
}

pub(crate) fn compile_source(
    manifest: ParquetSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(ParquetCompiledSource {
        manifest,
        source_secrets,
        source_variables,
    })
}

pub(crate) fn compile_manifest(
    manifest: &ParquetSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    compile_source(
        manifest.clone(),
        request.source_secrets.clone(),
        request.source_variables.clone(),
    )
}

#[derive(Debug)]
pub(crate) struct ParquetTableProvider {
    inner: ListingTable,
}

impl ParquetTableProvider {
    /// Build a Parquet-backed table provider from a source manifest.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the `Parquet` source configuration is
    /// invalid or the listing table cannot be constructed.
    #[cfg(test)]
    pub(crate) fn try_new(
        ctx: &SessionContext,
        source_schema: &str,
        table: FileTableSpec,
        source_secrets: &BTreeMap<String, String>,
    ) -> Result<Self> {
        futures::executor::block_on(Self::try_new_async(
            ctx,
            source_schema,
            table,
            source_secrets,
        ))
    }

    pub(crate) async fn try_new_async(
        ctx: &SessionContext,
        source_schema: &str,
        table: FileTableSpec,
        source_secrets: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let inner =
            Self::build_listing_table(ctx.clone(), source_schema, &table, source_secrets).await?;
        Ok(Self { inner })
    }

    async fn build_listing_table(
        ctx: SessionContext,
        source_schema: &str,
        table: &FileTableSpec,
        source_secrets: &BTreeMap<String, String>,
    ) -> Result<ListingTable> {
        let source = table.source.clone();
        let mut table_path = ListingTableUrl::parse(&source.location).map_err(|error| {
            DataFusionError::Plan(format!(
                "{source_schema}.{} has invalid source.location '{}': {error}",
                table.name(),
                source.location
            ))
        })?;

        if table_path.is_collection() {
            table_path = table_path.with_glob(source.parquet_glob_or_default())?;
        }

        let object_store = build_object_store(source_schema, &table_path, source_secrets)?;
        ctx.register_object_store(table_path.object_store().as_ref(), object_store);

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_session_config_options(ctx.state().config())
            .with_file_extension(DEFAULT_PARQUET_EXTENSION)
            .with_table_partition_cols(partition_columns_to_arrow(&source.partitions)?);

        listing_options
            .validate_partitions(&ctx.state(), &table_path)
            .await?;

        let mut file_schema = if table.has_explicit_columns() {
            schema_from_columns(table.columns(), source_schema, table.name())?
        } else {
            infer_schema_expand_dicts(&ctx, &listing_options, &table_path).await?
        };

        // Strip partition columns from the file schema. If an older writer
        // stored partition columns (e.g. `_part_id`) inside the Parquet files,
        // schema inference will include them here. DataFusion's ListingTable
        // adds partition columns from the hive directory path; having them in
        // both the file schema and the partition config produces a
        // duplicate-field schema that causes queries to return zero rows.
        let partition_names: Vec<&str> = listing_options
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
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
impl TableProvider for ParquetTableProvider {
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

#[async_trait]
impl CompiledBackendSource for ParquetCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    async fn register(&self, ctx: &SessionContext) -> Result<BackendRegistration> {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.manifest.tables.len());

        for table in &self.manifest.tables {
            let provider = ParquetTableProvider::try_new_async(
                ctx,
                &self.manifest.common.name,
                table.clone(),
                &self.source_secrets,
            )
            .await?;
            let schema = provider.schema();
            let table_name = table.name().to_string();
            let metadata = registered_table(table, &schema);
            tables.insert(table_name, Arc::new(provider));
            table_infos.push(metadata);
        }

        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs = build_registered_inputs(
            &self.manifest.declared_inputs,
            &self.source_variables,
            &secret_keys,
        );

        Ok(BackendRegistration {
            tables,
            source: RegisteredSource {
                schema_name: self.manifest.common.name.clone(),
                tables: table_infos,
                table_functions: vec![],
                inputs,
            },
        })
    }
}

fn registered_table(table: &FileTableSpec, inferred_schema: &SchemaRef) -> RegisteredTable {
    let required_filters = required_filter_names(table.filters());
    let columns = if table.columns().is_empty() {
        registered_columns_from_schema(inferred_schema, &required_filters)
    } else {
        registered_columns_from_specs(table.columns(), &required_filters)
    };

    build_registered_table(&table.common, columns, required_filters)
}

fn build_object_store(
    source_schema: &str,
    table_path: &ListingTableUrl,
    source_secrets: &BTreeMap<String, String>,
) -> Result<Arc<dyn ObjectStore>> {
    match table_path.scheme() {
        "file" => Ok(Arc::new(LocalFileSystem::new())),
        "s3" => {
            let bucket = table_path.get_url().host_str().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "parquet source '{source_schema}' is missing an S3 bucket in '{}'",
                    table_path.as_str()
                ))
            })?;

            let use_instance_profile =
                lookup_source_setting(source_secrets, "use_instance_profile")
                    .as_deref()
                    .map(parse_bool)
                    .transpose()?
                    .unwrap_or(false);

            let access_key_id = lookup_source_setting(source_secrets, "aws_access_key_id");
            let secret_access_key = lookup_source_setting(source_secrets, "aws_secret_access_key");
            let session_token = lookup_source_setting(source_secrets, "aws_session_token");
            let region = lookup_source_setting(source_secrets, "aws_region");

            if access_key_id.is_some() ^ secret_access_key.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "parquet source '{source_schema}' must define both aws_access_key_id and aws_secret_access_key"
                )));
            }

            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            if let Some(region) = region {
                builder = builder.with_region(region);
            }

            if let (Some(access_key_id), Some(secret_access_key)) =
                (access_key_id, secret_access_key)
            {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
            } else if !use_instance_profile {
                return Err(DataFusionError::Plan(format!(
                    "parquet source '{source_schema}' must define aws_access_key_id and aws_secret_access_key, or set use_instance_profile"
                )));
            }

            if let Some(session_token) = session_token {
                builder = builder.with_token(session_token);
            }

            builder
                .build()
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "failed to configure S3 object store for source '{source_schema}': {error}"
                    ))
                })
        }
        other => Err(DataFusionError::Plan(format!(
            "parquet source '{source_schema}' uses unsupported scheme '{other}'"
        ))),
    }
}

fn lookup_source_setting(
    source_secrets: &BTreeMap<String, String>,
    provider_key: &str,
) -> Option<String> {
    source_secrets.get(provider_key).cloned()
}

/// A [`ChunkReader`] backed by a sub-range of a parquet file held in memory.
///
/// `file_len` is the full logical file size; `data` holds bytes starting at
/// `start_offset` within the file. [`ParquetMetaDataReader`] calls
/// [`ChunkReader::get_bytes`] with absolute file offsets, which we translate
/// to relative offsets into `data`.
///
/// [`ParquetMetaDataReader`]: parquet::file::metadata::ParquetMetaDataReader
struct FooterBytes {
    file_len: u64,
    data: Bytes,
    start_offset: u64,
}

impl Length for FooterBytes {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for FooterBytes {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let relative = self.relative_offset(start)?;
        if relative > self.data.len() {
            return Err(parquet::errors::ParquetError::General(format!(
                "requested offset {start} is past end of buffered footer (len={})",
                self.data.len()
            )));
        }
        Ok(Cursor::new(self.data.slice(relative..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let relative = self.relative_offset(start)?;
        let end = relative.checked_add(length).ok_or_else(|| {
            parquet::errors::ParquetError::General(
                "footer offset + length overflows usize".to_string(),
            )
        })?;
        if end > self.data.len() {
            return Err(parquet::errors::ParquetError::General(format!(
                "requested range {start}..{} is outside buffered footer (len={})",
                start + length as u64,
                self.data.len()
            )));
        }
        Ok(self.data.slice(relative..end))
    }
}

impl FooterBytes {
    fn relative_offset(&self, start: u64) -> parquet::errors::Result<usize> {
        if start < self.start_offset {
            return Err(parquet::errors::ParquetError::General(format!(
                "requested offset {start} is before buffered footer start {}",
                self.start_offset
            )));
        }
        usize::try_from(start - self.start_offset)
            .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
    }
}

/// Infer the schema for a parquet listing table, expanding dictionary types
/// per file before merging.
///
/// `DataFusion`'s built-in `infer_schema` merges file schemas internally using
/// `Arrow`'s `Schema::try_merge`, which fails when the same column is
/// `Dictionary(K, V)` in one file and plain `V` in another — a pattern used
/// by `OTel` `Arrow`'s adaptive encoding. This function reads each file's schema
/// from its footer individually, expands dictionary types on each, then merges
/// the already-expanded schemas so the merge always sees compatible types.
async fn infer_schema_expand_dicts(
    ctx: &SessionContext,
    listing_options: &ListingOptions,
    table_path: &ListingTableUrl,
) -> Result<SchemaRef> {
    // Fast path: standard inference works when all files share identical encoding.
    if let Ok(inferred) = listing_options.infer_schema(&ctx.state(), table_path).await {
        if inferred.fields().is_empty() {
            return Err(DataFusionError::Execution(format!(
                "no parquet files found at {table_path}"
            )));
        }
        let expanded = expand_dictionary_types(&inferred);
        // Strip schema-level metadata for the same reason as the slow path below.
        return Ok(Arc::new(Schema::new_with_metadata(
            expanded.fields().clone(),
            HashMap::default(),
        )));
    }

    // Slow path: read each file's footer, expand dictionaries, then merge.
    let store = ctx.runtime_env().object_store(table_path)?;

    let parquet_objects: Vec<_> = table_path
        .list_all_files(
            &ctx.state(),
            store.as_ref(),
            &listing_options.file_extension,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to list {table_path}: {e}")))?
        .try_collect()
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to list {table_path}: {e}")))?;

    if parquet_objects.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "no parquet files found at {table_path}"
        )));
    }

    let mut merged: Option<Schema> = None;

    for obj_meta in &parquet_objects {
        let file_size: u64 = obj_meta.size;
        if file_size < PARQUET_FOOTER_SIZE {
            return Err(DataFusionError::Execution(format!(
                "parquet file {} is too small ({file_size} bytes)",
                obj_meta.location
            )));
        }

        // First range read: last 8 bytes to decode the footer metadata length.
        let tail = store
            .get_range(
                &obj_meta.location,
                (file_size - PARQUET_FOOTER_SIZE)..file_size,
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "footer tail read failed for {}: {e}",
                    obj_meta.location
                ))
            })?;

        let metadata_tail = tail.get(..4).ok_or_else(|| {
            DataFusionError::Execution("invalid parquet footer tail bytes".to_string())
        })?;
        let metadata_len = i32::from_le_bytes(metadata_tail.try_into().map_err(|_err| {
            DataFusionError::Execution("invalid parquet footer tail bytes".to_string())
        })?);
        if metadata_len < 0 {
            return Err(DataFusionError::Execution(format!(
                "negative parquet metadata length in {}",
                obj_meta.location
            )));
        }
        let metadata_len = u64::try_from(metadata_len).expect("checked non-negative above");
        if metadata_len + PARQUET_FOOTER_SIZE > file_size {
            return Err(DataFusionError::Execution(format!(
                "parquet metadata length {metadata_len} exceeds file size in {}",
                obj_meta.location
            )));
        }

        // Second range read: thrift-encoded metadata + 8-byte tail.
        let footer_start = file_size - PARQUET_FOOTER_SIZE - metadata_len;
        let footer_data = store
            .get_range(&obj_meta.location, footer_start..file_size)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "footer metadata read failed for {}: {e}",
                    obj_meta.location
                ))
            })?;

        let footer_reader = FooterBytes {
            file_len: file_size,
            data: footer_data,
            start_offset: footer_start,
        };

        let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(footer_reader)
            .map_err(|e| DataFusionError::Execution(format!("parquet schema read failed: {e}")))?
            .schema()
            .clone();

        let expanded = expand_dictionary_types(&arrow_schema);
        // Strip schema-level metadata before merging: OTel Arrow embeds
        // per-file values (e.g. _part_id UUIDs) in schema metadata, which
        // causes Schema::try_merge to fail with "conflicting metadata".
        let expanded_no_meta =
            Schema::new_with_metadata(expanded.fields().clone(), HashMap::default());

        merged = Some(match merged {
            None => expanded_no_meta,
            Some(s) => Schema::try_merge(vec![s, expanded_no_meta])?,
        });
    }

    Ok(Arc::new(merged.expect("metadata items were processed")))
}

/// Expand dictionary-encoded fields to their plain value types.
///
/// `OTel` `Arrow` uses adaptive encoding: the same column may be
/// `Dictionary(K, V)` in one file and plain `V` in another. Using the
/// plain value type as the logical schema lets the Parquet reader decode
/// both variants without a type mismatch at `RecordBatch` validation time.
fn expand_dictionary_types(schema: &SchemaRef) -> SchemaRef {
    fn expand(dt: &DataType) -> DataType {
        match dt {
            DataType::Dictionary(_, value_type) => expand(value_type),
            DataType::List(field) => DataType::List(Arc::new(expand_field(field.as_ref()))),
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(|f| Arc::new(expand_field(f))).collect())
            }
            other => other.clone(),
        }
    }

    fn expand_field(field: &Field) -> Field {
        Field::new(field.name(), expand(field.data_type()), field.is_nullable())
            .with_metadata(field.metadata().clone())
    }

    let expanded: Vec<Field> = schema.fields().iter().map(|f| expand_field(f)).collect();
    Arc::new(Schema::new_with_metadata(
        expanded,
        schema.metadata().clone(),
    ))
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(DataFusionError::Plan(format!(
            "invalid boolean value '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::ParquetTableProvider;
    use crate::backends::compile_source_manifest;
    use crate::runtime::catalog;
    use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
    use crate::{QueryRuntimeContext, QuerySource};
    use coral_spec::backends::file::FileTableSpec;
    use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};
    use datafusion::arrow::array::{
        DictionaryArray, Float64Array, Int64Array, StringArray, UInt16Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt16Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn compile_sources(manifests: Vec<ValidatedSourceManifest>) -> Vec<CompiledQuerySource> {
        manifests
            .into_iter()
            .map(|manifest| {
                let variables = BTreeMap::new();
                let secrets = BTreeMap::new();
                CompiledQuerySource {
                    source: QuerySource::new(manifest.clone(), variables.clone(), secrets.clone()),
                    compiled: compile_source_manifest(
                        &manifest,
                        variables,
                        secrets,
                        &QueryRuntimeContext::default(),
                    )
                    .expect("manifest should compile"),
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn parquet_provider_reads_local_files_with_partitions() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        write_metrics_fixture(fixture_dir.path());

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = parquet_manifest(&location);

        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("parquet source should register");

        let provider = ctx
            .catalog("datafusion")
            .expect("catalog should exist")
            .schema("otel")
            .expect("schema should exist")
            .table("metrics")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert!(
            provider
                .as_any()
                .downcast_ref::<ParquetTableProvider>()
                .is_some()
        );

        let batches = ctx
            .sql("SELECT metric, value, date FROM otel.metrics ORDER BY metric")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();

        assert!(rendered.contains("cpu.usage"));
        assert!(rendered.contains("memory.usage"));
        assert!(rendered.contains("2026-03-10"));
    }

    #[tokio::test]
    async fn parquet_provider_exposes_inferred_schema_in_coral_columns() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        write_metrics_fixture(fixture_dir.path());

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = parquet_manifest(&location);

        let active_sources = register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("parquet source should register");
        catalog::register(&ctx, &active_sources.active_sources)
            .expect("metadata tables should register");

        let batches = ctx
            .sql(
                "SELECT column_name, data_type \
                 FROM coral.columns \
                 WHERE schema_name = 'otel' AND table_name = 'metrics' \
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

        assert!(rendered.contains("date"));
        assert!(rendered.contains("Utf8"));
        assert!(rendered.contains("metric"));
        assert!(rendered.contains("value"));
        assert!(rendered.contains("Float64"));
    }

    #[tokio::test]
    async fn parquet_provider_relists_files_within_same_context_when_cache_is_disabled() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        write_metrics_fixture(fixture_dir.path());

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_list_cache_limit(0)
                .build()
                .expect("runtime should build"),
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), runtime);
        let location = format!("file://{}/", fixture_dir.path().display());
        let manifest = parquet_manifest(&location);

        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("parquet plugin should register");

        let before = ctx
            .sql("SELECT COUNT(*) AS count FROM otel.metrics")
            .await
            .expect("initial count should plan")
            .collect()
            .await
            .expect("initial count should execute");
        let before_rendered = pretty_format_batches(&before)
            .expect("initial count should render")
            .to_string();
        assert!(before_rendered.contains('2'));

        write_metrics_fixture_for_day(
            fixture_dir.path(),
            "2026-03-11",
            &[("disk.usage", 55.0), ("net.in", 100.0)],
            "export-2.parquet",
        );

        let after = ctx
            .sql("SELECT COUNT(*) AS count FROM otel.metrics")
            .await
            .expect("updated count should plan")
            .collect()
            .await
            .expect("updated count should execute");
        let after_rendered = pretty_format_batches(&after)
            .expect("updated count should render")
            .to_string();
        assert!(after_rendered.contains('4'));
    }

    fn parquet_manifest(location: &str) -> ValidatedSourceManifest {
        parquet_manifest_with_glob_and_partitions(
            location,
            "**/*.parquet",
            &[json!({
                "name": "date",
                "type": "Utf8",
            })],
        )
    }

    // ── infer_schema_expand_dicts tests ──────────────────────────────────────

    /// Simulates `OTel` `Arrow` adaptive encoding where two files written for the
    /// same logical column differ in physical schema: one uses
    /// `Dictionary(UInt16, Int64)` and the other plain `Int64`. `DataFusion`'s
    /// built-in schema merge fails on the mismatch, so the slow path must
    /// expand dictionaries per-file and then merge.
    #[tokio::test]
    async fn infer_schema_slow_path_merges_mixed_dictionary_and_plain_columns() {
        let dir = tempdir().expect("tempdir should be created");

        // File 1: "val" column is Dictionary(UInt16, Int64).
        {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int64)),
                false,
            )]));
            let keys = UInt16Array::from(vec![0u16]);
            let values = Arc::new(Int64Array::from(vec![100i64]));
            let col =
                Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values).expect("dict array"));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).expect("batch");
            let file = std::fs::File::create(dir.path().join("dict.parquet"))
                .expect("create dict.parquet");
            let mut w = ArrowWriter::try_new(file, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }

        // File 2: "val" column is plain Int64.
        {
            let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
            let col = Arc::new(Int64Array::from(vec![200i64]));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).expect("batch");
            let file = std::fs::File::create(dir.path().join("plain.parquet"))
                .expect("create plain.parquet");
            let mut w = ArrowWriter::try_new(file, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }

        let location = format!("file://{}/", dir.path().display());
        let manifest = parquet_manifest_no_partitions(&location);
        let ctx = SessionContext::new();
        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("mixed-encoding plugin should register via slow path");

        let batches = ctx
            .sql("SELECT val FROM otel.metrics ORDER BY val")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(
            rendered.contains("100"),
            "dictionary-encoded row should be present"
        );
        assert!(
            rendered.contains("200"),
            "plain-encoded row should be present"
        );
    }

    #[tokio::test]
    async fn infer_schema_slow_path_respects_table_glob() {
        let dir = tempdir().expect("tempdir should be created");

        {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int64)),
                false,
            )]));
            let keys = UInt16Array::from(vec![0u16]);
            let values = Arc::new(Int64Array::from(vec![100i64]));
            let col =
                Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values).expect("dict array"));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).expect("batch");
            let file = std::fs::File::create(dir.path().join("matching-dict.parquet"))
                .expect("create dict.parquet");
            let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
            writer.write(&batch).expect("write");
            writer.close().expect("close");
        }

        {
            let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
            let col = Arc::new(Int64Array::from(vec![200i64]));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).expect("batch");
            let file = std::fs::File::create(dir.path().join("matching-plain.parquet"))
                .expect("create plain.parquet");
            let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
            writer.write(&batch).expect("write");
            writer.close().expect("close");
        }

        std::fs::write(dir.path().join("ignored.parquet"), b"not a parquet file")
            .expect("ignored file should be written");

        let location = format!("file://{}/", dir.path().display());
        let manifest = parquet_manifest_no_partitions_with_glob(&location, "matching-*.parquet");
        let ctx = SessionContext::new();
        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("glob should ignore non-matching parquet files during schema inference");

        let batches = ctx
            .sql("SELECT val FROM otel.metrics ORDER BY val")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(
            rendered.contains("100"),
            "dictionary-encoded row should be present"
        );
        assert!(
            rendered.contains("200"),
            "plain-encoded row should be present"
        );
    }

    #[test]
    fn infer_schema_slow_path_returns_error_for_corrupt_parquet_footer() {
        let dir = tempdir().expect("tempdir should be created");
        std::fs::write(dir.path().join("data.parquet"), b"not a parquet file")
            .expect("write corrupt file");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", dir.path().display());
        let table = parquet_table_spec(&location);
        let result = ParquetTableProvider::try_new(&ctx, "otel", table, &BTreeMap::default());
        assert!(
            result.is_err(),
            "corrupt parquet should cause provider construction failure"
        );
    }

    #[test]
    fn infer_schema_slow_path_returns_error_for_too_small_parquet_file() {
        let dir = tempdir().expect("tempdir should be created");
        // 4 bytes is below PARQUET_FOOTER_SIZE (8).
        std::fs::write(dir.path().join("tiny.parquet"), b"PAR1").expect("write too-small file");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", dir.path().display());
        let table = parquet_table_spec(&location);
        let result = ParquetTableProvider::try_new(&ctx, "otel", table, &BTreeMap::default());
        assert!(
            result.is_err(),
            "too-small parquet should cause provider construction failure"
        );
    }

    /// Regression test for the case where an older writer stored the partition
    /// column (`_part_id`) both as a hive-style directory prefix
    /// (`_part_id=<uuid>/`) **and** as a physical column inside the Parquet
    /// file. When `DataFusion`'s `ListingTable` sees `_part_id` in both the
    /// file schema and in `table_partition_cols`, it produces a broken
    /// duplicate-field schema that returns zero rows.  The fix strips partition
    /// column names from the inferred file schema before passing it to
    /// `ListingTableConfig`.
    #[tokio::test]
    async fn partition_column_in_file_schema_is_stripped_and_data_is_queryable() {
        let dir = tempdir().expect("tempdir should be created");

        // Write a parquet file that contains `_part_id` as a physical column
        // (the old buggy writer behaviour) inside a hive partition directory.
        let part_dir = dir.path().join("_part_id=abc-123");
        std::fs::create_dir_all(&part_dir).expect("partition dir should exist");
        {
            // File schema deliberately includes `_part_id` — this is the
            // defect we are guarding against.
            let schema = Arc::new(Schema::new(vec![
                Field::new("metric", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
                Field::new("_part_id", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["cpu.usage", "mem.usage"])),
                    Arc::new(Float64Array::from(vec![0.42_f64, 12.5_f64])),
                    Arc::new(StringArray::from(vec!["abc-123", "abc-123"])),
                ],
            )
            .expect("batch should build");
            let file =
                std::fs::File::create(part_dir.join("data.parquet")).expect("file should create");
            let mut w = ArrowWriter::try_new(file, schema, None).expect("writer should init");
            w.write(&batch).expect("batch should write");
            w.close().expect("writer should close");
        }

        // Use a manifest that declares `_part_id` as the partition column,
        // matching the hive directory written above.
        let location = format!("file://{}/", dir.path().display());
        let manifest = parquet_manifest_with_partition(&location, "_part_id");
        let ctx = SessionContext::new();
        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("plugin should register even when file schema contains partition column");

        // The provider schema must contain `_part_id` exactly once.
        let provider = ctx
            .catalog("datafusion")
            .expect("catalog should exist")
            .schema("otel")
            .expect("schema should exist")
            .table("metrics")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let schema = provider.schema();
        let part_id_fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|f| f.name() == "_part_id")
            .collect();
        assert_eq!(
            part_id_fields.len(),
            1,
            "_part_id must appear exactly once in the table schema, got: {schema:?}"
        );

        // Non-partition data columns must still be present.
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"metric"),
            "non-partition field `metric` must be preserved; schema: {schema:?}"
        );
        assert!(
            field_names.contains(&"value"),
            "non-partition field `value` must be preserved; schema: {schema:?}"
        );

        // The table must actually return rows when queried.
        let batches = ctx
            .sql("SELECT metric, value, _part_id FROM otel.metrics ORDER BY metric")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");
        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();
        assert!(
            rendered.contains("cpu.usage"),
            "data row should be queryable after partition-strip fix"
        );
        assert!(
            rendered.contains("abc-123"),
            "_part_id value from hive directory should be visible"
        );
    }

    fn parquet_table_spec(location: &str) -> FileTableSpec {
        parquet_table_spec_with_glob(location, "**/*.parquet")
    }

    fn parquet_table_spec_with_glob(location: &str, glob: &str) -> FileTableSpec {
        let source_manifest = parquet_manifest_with_glob_and_partitions(location, glob, &[]);
        let manifest = source_manifest.as_parquet().expect("parquet manifest");
        manifest.tables.first().expect("parquet table").clone()
    }

    fn parquet_manifest_no_partitions(location: &str) -> ValidatedSourceManifest {
        parquet_manifest_no_partitions_with_glob(location, "**/*.parquet")
    }

    fn parquet_manifest_no_partitions_with_glob(
        location: &str,
        glob: &str,
    ) -> ValidatedSourceManifest {
        parquet_manifest_with_glob_and_partitions(location, glob, &[])
    }

    fn parquet_manifest_with_partition(location: &str, partition: &str) -> ValidatedSourceManifest {
        parquet_manifest_with_glob_and_partitions(
            location,
            "**/*.parquet",
            &[json!({
                "name": partition,
                "type": "Utf8",
            })],
        )
    }

    fn parquet_manifest_with_glob_and_partitions(
        location: &str,
        glob: &str,
        partitions: &[serde_json::Value],
    ) -> ValidatedSourceManifest {
        parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "otel",
            "version": "0.1.0",
            "backend": "parquet",
            "tables": [{
                "name": "metrics",
                "description": "metrics",
                "source": {
                    "location": location,
                    "glob": glob,
                    "partitions": partitions,
                },
                "columns": [],
            }]
        }))
        .expect("parquet manifest should parse")
    }

    fn write_metrics_fixture(root: &std::path::Path) {
        write_metrics_fixture_for_day(
            root,
            "2026-03-10",
            &[("cpu.usage", 0.42), ("memory.usage", 12.5)],
            "metrics.parquet",
        );
    }

    fn write_metrics_fixture_for_day(
        root: &std::path::Path,
        day: &str,
        rows: &[(&str, f64)],
        file_name: &str,
    ) {
        let partition_dir = root.join(format!("date={day}"));
        std::fs::create_dir_all(&partition_dir).expect("partition dir should exist");
        let file = std::fs::File::create(partition_dir.join(file_name))
            .expect("fixture file should be created");

        let schema = Arc::new(Schema::new(vec![
            Field::new("metric", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(metric, _)| *metric).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("record batch should be created");

        let mut writer =
            ArrowWriter::try_new(file, schema, None).expect("writer should be created");
        writer.write(&batch).expect("batch should be written");
        writer.close().expect("writer should close");
    }
}
