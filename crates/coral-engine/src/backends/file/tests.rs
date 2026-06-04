use super::file_groups::file_groups_for_scan;
use super::metadata::FileMetadataColumns;
use super::partitions::{
    PartitionColumns, partition_filter_constraints, partition_values_for_path,
};
use super::provider;
use crate::backends::compile_source_manifest;
use crate::runtime::catalog;
use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
use crate::{QueryRuntimeContext, QuerySource};
use coral_spec::backends::file::{
    FilePartitionDataType, FileTableSpec, PartitionColumnSpec, PartitionPathSpec,
};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};
use datafusion::arrow::array::{
    DictionaryArray, Float64Array, Int64Array, StringArray, UInt16Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use object_store::ObjectMeta;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn file_url_from_directory_path(path: &std::path::Path) -> String {
    url::Url::from_directory_path(path)
        .expect("path should convert to file URL")
        .to_string()
}

fn file_url_from_file_path(path: &std::path::Path) -> String {
    url::Url::from_file_path(path)
        .expect("path should convert to file URL")
        .to_string()
}

fn write_events_file(path: impl AsRef<std::path::Path>, contents: &str) {
    fs::write(path, contents).expect("events fixture should be written");
}

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

fn register_file_source(ctx: &SessionContext, manifest: ValidatedSourceManifest) {
    register_sources_blocking(ctx, compile_sources(vec![manifest]))
        .expect("file source should register");
}

fn register_file_source_with_catalog(ctx: &SessionContext, manifest: ValidatedSourceManifest) {
    let active_sources = register_sources_blocking(ctx, compile_sources(vec![manifest]))
        .expect("file source should register");
    catalog::register(ctx, &active_sources.active_sources)
        .expect("metadata tables should register");
}

fn file_context(manifest: ValidatedSourceManifest) -> SessionContext {
    let ctx = SessionContext::new();
    register_file_source(&ctx, manifest);
    ctx
}

async fn collect_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute")
}

async fn render_query(ctx: &SessionContext, sql: &str) -> String {
    pretty_format_batches(&collect_query(ctx, sql).await)
        .expect("batches should render")
        .to_string()
}

async fn registered_provider(
    ctx: &SessionContext,
    schema: &str,
    table: &str,
) -> Arc<dyn TableProvider> {
    ctx.catalog("datafusion")
        .expect("catalog should exist")
        .schema(schema)
        .expect("schema should exist")
        .table(table)
        .await
        .expect("table lookup should succeed")
        .expect("table should exist")
}

#[test]
fn hive_partition_extraction_requires_declared_layout_order() {
    let table_path = ListingTableUrl::parse("s3://bucket/events/")
        .expect("table path")
        .with_glob("**/*.jsonl")
        .expect("table glob");
    let partitions = vec![
        PartitionColumnSpec {
            name: "year".to_string(),
            data_type: FilePartitionDataType::Int64,
            path: PartitionPathSpec::Hive,
        },
        PartitionColumnSpec {
            name: "month".to_string(),
            data_type: FilePartitionDataType::Int64,
            path: PartitionPathSpec::Hive,
        },
    ];
    let partition_columns =
        PartitionColumns::try_new(&partitions).expect("partition columns should parse");

    let valid =
        ObjectPath::parse("events/year=2026/month=05/users.jsonl").expect("valid object path");
    let values = partition_values_for_path(&table_path, &valid, &partition_columns)
        .expect("valid hive path should match");
    assert_eq!(
        values.into_scalars(),
        vec![ScalarValue::Int64(Some(2026)), ScalarValue::Int64(Some(5))]
    );

    let out_of_order = ObjectPath::parse("events/month=05/year=2026/users.jsonl")
        .expect("out-of-order object path");
    let error = partition_values_for_path(&table_path, &out_of_order, &partition_columns)
        .expect_err("out-of-order hive path should fail");
    assert!(error.to_string().contains("expected hive partition 'year'"));
}

#[test]
fn partition_in_pruning_ignores_nonliteral_items() {
    let table_path = ListingTableUrl::parse("s3://bucket/events/")
        .expect("table path")
        .with_glob("**/*.jsonl")
        .expect("table glob");
    let partitions = vec![PartitionColumnSpec {
        name: "year".to_string(),
        data_type: FilePartitionDataType::Int64,
        path: PartitionPathSpec::Hive,
    }];
    let partition_columns =
        PartitionColumns::try_new(&partitions).expect("partition columns should parse");
    let path = ObjectPath::parse("events/year=2027/users.jsonl").expect("object path");
    let values = partition_values_for_path(&table_path, &path, &partition_columns)
        .expect("partition values should parse");

    let filters = vec![col("year").in_list(vec![lit(2026_i64), col("other_year")], false)];
    let constraints = partition_filter_constraints(&filters, &partition_columns);

    assert!(
        constraints.matches(&values),
        "mixed literal/non-literal IN predicates must not prune partitions"
    );
}

#[test]
fn partition_pruning_canonicalizes_literals_by_partition_type() {
    let table_path = ListingTableUrl::parse("s3://bucket/events/")
        .expect("table path")
        .with_glob("**/*.jsonl")
        .expect("table glob");
    let partitions = vec![PartitionColumnSpec {
        name: "month".to_string(),
        data_type: FilePartitionDataType::Int64,
        path: PartitionPathSpec::Hive,
    }];
    let partition_columns =
        PartitionColumns::try_new(&partitions).expect("partition columns should parse");
    let path = ObjectPath::parse("events/month=05/users.jsonl").expect("object path");
    let values = partition_values_for_path(&table_path, &path, &partition_columns)
        .expect("partition values should parse");

    let filters = vec![col("month").eq(lit("05"))];
    let constraints = partition_filter_constraints(&filters, &partition_columns);
    assert!(constraints.matches(&values));
    let other_path = ObjectPath::parse("events/month=06/users.jsonl").expect("object path");
    let other_values = partition_values_for_path(&table_path, &other_path, &partition_columns)
        .expect("partition values should parse");
    assert!(!constraints.matches(&other_values));

    let filters = vec![col("month").eq(lit("5.0"))];
    let constraints = partition_filter_constraints(&filters, &partition_columns);
    assert!(
        constraints.matches(&values),
        "non-canonical literals must not become exact pruning constraints"
    );
}

#[test]
fn file_groups_split_partitioned_files_unless_preservation_is_enabled() {
    let table_path = ListingTableUrl::parse("s3://bucket/sessions/")
        .expect("table path")
        .with_glob("20??/**/*.jsonl")
        .expect("table glob");
    let partitions = vec![
        PartitionColumnSpec {
            name: "year".to_string(),
            data_type: FilePartitionDataType::Int64,
            path: PartitionPathSpec::Segment { index: 0 },
        },
        PartitionColumnSpec {
            name: "month".to_string(),
            data_type: FilePartitionDataType::Int64,
            path: PartitionPathSpec::Segment { index: 1 },
        },
        PartitionColumnSpec {
            name: "day".to_string(),
            data_type: FilePartitionDataType::Int64,
            path: PartitionPathSpec::Segment { index: 2 },
        },
    ];
    let partition_columns =
        PartitionColumns::try_new(&partitions).expect("partition columns should parse");
    let files = (0..4)
        .map(|index| object_meta(&format!("sessions/2026/05/14/session-{index}.jsonl")))
        .collect::<Vec<_>>();
    let metadata_columns = FileMetadataColumns::try_new(&[]).expect("empty metadata should parse");

    let split = file_groups_for_scan(
        &table_path,
        &partition_columns,
        files.clone(),
        &metadata_columns,
        &[],
        4,
        0,
    )
    .expect("file groups should build");
    assert_eq!(split.groups.len(), 4);
    assert!(
        !split.grouped_by_partition,
        "split groups must not claim DataFusion partition-preserving layout"
    );

    let preserved = file_groups_for_scan(
        &table_path,
        &partition_columns,
        files,
        &metadata_columns,
        &[],
        4,
        1,
    )
    .expect("file groups should build");
    assert_eq!(preserved.groups.len(), 1);
    assert!(preserved.grouped_by_partition);
}

#[tokio::test]
async fn parquet_provider_reads_local_files_with_partitions() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture(fixture_dir.path());

    let ctx = file_context(parquet_manifest_for_dir(
        fixture_dir.path(),
        "**/*.parquet",
        &[parquet_partition("date")],
    ));

    let _provider = registered_provider(&ctx, "otel", "metrics").await;

    let rendered = render_query(
        &ctx,
        "SELECT metric, value, date FROM otel.metrics ORDER BY metric",
    )
    .await;

    assert!(rendered.contains("cpu.usage"));
    assert!(rendered.contains("memory.usage"));
    assert!(rendered.contains("2026-03-10"));
}

#[tokio::test]
async fn parquet_provider_exposes_inferred_schema_in_coral_columns() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture(fixture_dir.path());

    let ctx = SessionContext::new();
    register_file_source_with_catalog(
        &ctx,
        parquet_manifest_for_dir(
            fixture_dir.path(),
            "**/*.parquet",
            &[parquet_partition("date")],
        ),
    );

    let rendered = render_query(
        &ctx,
        "SELECT column_name, data_type \
         FROM coral.columns \
         WHERE schema_name = 'otel' AND table_name = 'metrics' \
         ORDER BY column_name",
    )
    .await;

    assert!(rendered.contains("date"));
    assert!(rendered.contains("Utf8"));
    assert!(rendered.contains("metric"));
    assert!(rendered.contains("value"));
    assert!(rendered.contains("Float64"));
}

#[tokio::test]
async fn parquet_inferred_schema_rejects_metadata_name_collision() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    let file = std::fs::File::create(fixture_dir.path().join("events.parquet"))
        .expect("fixture file should be created");
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["from-file"])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .expect("record batch should be created");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer should be created");
    writer.write(&batch).expect("batch should be written");
    writer.close().expect("writer should close");

    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "parquet_collision",
        "version": "0.1.0",
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "events",
            "format": "parquet",
            "source": {
                "location": location,
                "glob": "**/*.parquet",
                "metadata": [{ "name": "file_path", "kind": "relative_path" }]
            },
            "columns": [],
        }]
    }))
    .expect("manifest should parse");

    let ctx = SessionContext::new();
    let registration = register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("source registration should collect provider failures");

    assert!(registration.active_sources.is_empty());
    let failure = registration
        .failures
        .first()
        .expect("metadata collision should fail source registration");
    assert!(
        failure.detail.contains(
            "parquet_collision.events metadata column 'file_path' duplicates a file column"
        ),
        "{}",
        failure.detail
    );
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
    register_file_source(
        &ctx,
        parquet_manifest_for_dir(
            fixture_dir.path(),
            "**/*.parquet",
            &[parquet_partition("date")],
        ),
    );

    let before_rendered = render_query(&ctx, "SELECT COUNT(*) AS count FROM otel.metrics").await;
    assert!(before_rendered.contains('2'));

    write_metrics_fixture_for_day(
        fixture_dir.path(),
        "2026-03-11",
        &[("disk.usage", 55.0), ("net.in", 100.0)],
        "export-2.parquet",
    );

    let after_rendered = render_query(&ctx, "SELECT COUNT(*) AS count FROM otel.metrics").await;
    assert!(after_rendered.contains('4'));
}

#[tokio::test]
async fn file_provider_honors_custom_glob_extension() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_events_file(
        fixture_dir.path().join("events.ndjson"),
        r#"{"id":1,"kind":"user"}
{"id":2,"kind":"assistant"}
"#,
    );

    let ctx = file_context(events_manifest_for_dir(
        "custom_ext_demo",
        "jsonl",
        fixture_dir.path(),
        "**/*.ndjson",
        None,
    ));

    let rendered = render_query(&ctx, "SELECT COUNT(*) AS rows FROM custom_ext_demo.events").await;

    assert!(rendered.contains('2'));
}

#[tokio::test]
async fn file_provider_honors_explicit_file_without_default_extension() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    let file_path = fixture_dir.path().join("events.data");
    write_events_file(
        &file_path,
        r#"{"id":1,"kind":"user"}
{"id":2,"kind":"assistant"}
"#,
    );

    let location = file_url_from_file_path(&file_path);
    let ctx = file_context(events_manifest(
        "explicit_file_demo",
        "jsonl",
        &location,
        "**/*.jsonl",
        None,
    ));

    let rendered = render_query(
        &ctx,
        "SELECT kind FROM explicit_file_demo.events ORDER BY id",
    )
    .await;

    assert!(rendered.contains("assistant"));
    assert!(rendered.contains("user"));
}

#[tokio::test]
async fn file_provider_reads_csv_with_format_options() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_events_file(
        fixture_dir.path().join("events.csv"),
        "id|kind\n1|user\n2|assistant\n",
    );

    let ctx = file_context(events_manifest_for_dir(
        "csv_demo",
        "csv",
        fixture_dir.path(),
        "**/*.csv",
        Some(json!({ "has_header": true, "delimiter": "|" })),
    ));

    let rendered = render_query(&ctx, "SELECT kind FROM csv_demo.events ORDER BY id").await;

    assert!(rendered.contains("assistant"));
    assert!(rendered.contains("user"));
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

    write_dictionary_i64_parquet(dir.path(), "dict.parquet", 100);
    write_plain_i64_parquet(dir.path(), "plain.parquet", 200);

    let ctx = file_context(parquet_manifest_for_dir(dir.path(), "**/*.parquet", &[]));

    let rendered = render_query(&ctx, "SELECT val FROM otel.metrics ORDER BY val").await;
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

    write_dictionary_i64_parquet(dir.path(), "matching-dict.parquet", 100);
    write_plain_i64_parquet(dir.path(), "matching-plain.parquet", 200);

    std::fs::write(dir.path().join("ignored.parquet"), b"not a parquet file")
        .expect("ignored file should be written");

    let ctx = file_context(parquet_manifest_for_dir(
        dir.path(),
        "matching-*.parquet",
        &[],
    ));

    let rendered = render_query(&ctx, "SELECT val FROM otel.metrics ORDER BY val").await;
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
    let table = parquet_table_spec_for_dir(dir.path());
    let result =
        provider::FileTableProvider::try_new(&ctx, "otel", table, None, &BTreeMap::default());
    let error = result.expect_err("corrupt parquet should cause provider construction failure");
    assert!(
        error.to_string().contains("data.parquet"),
        "corrupt parquet error should include the object path: {error}"
    );
}

#[test]
fn infer_schema_slow_path_returns_error_for_too_small_parquet_file() {
    let dir = tempdir().expect("tempdir should be created");
    // 4 bytes is below PARQUET_FOOTER_SIZE (8).
    std::fs::write(dir.path().join("tiny.parquet"), b"PAR1").expect("write too-small file");

    let ctx = SessionContext::new();
    let table = parquet_table_spec_for_dir(dir.path());
    let result =
        provider::FileTableProvider::try_new(&ctx, "otel", table, None, &BTreeMap::default());
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
    let ctx = file_context(parquet_manifest_for_dir(
        dir.path(),
        "**/*.parquet",
        &[parquet_partition("_part_id")],
    ));

    // The provider schema must contain `_part_id` exactly once.
    let provider = registered_provider(&ctx, "otel", "metrics").await;
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
    let rendered = render_query(
        &ctx,
        "SELECT metric, value, _part_id FROM otel.metrics ORDER BY metric",
    )
    .await;
    assert!(
        rendered.contains("cpu.usage"),
        "data row should be queryable after partition-strip fix"
    );
    assert!(
        rendered.contains("abc-123"),
        "_part_id value from hive directory should be visible"
    );
}

fn parquet_table_spec_for_dir(root: &Path) -> FileTableSpec {
    let location = file_url_from_directory_path(root);
    parquet_table_spec(&location)
}

fn parquet_table_spec(location: &str) -> FileTableSpec {
    let source_manifest = parquet_manifest(location, "**/*.parquet", &[]);
    let manifest = source_manifest.as_file().expect("file manifest");
    manifest.tables.first().expect("parquet table").clone()
}

fn parquet_partition(name: &str) -> serde_json::Value {
    json!({
        "name": name,
        "type": "Utf8",
    })
}

fn parquet_manifest(
    location: &str,
    glob: &str,
    partitions: &[serde_json::Value],
) -> ValidatedSourceManifest {
    parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "otel",
        "version": "0.1.0",
        "backend": "file",
        "tables": [{
            "name": "metrics",
            "description": "metrics",
            "format": "parquet",
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

fn parquet_manifest_for_dir(
    root: &Path,
    glob: &str,
    partitions: &[serde_json::Value],
) -> ValidatedSourceManifest {
    parquet_manifest(&file_url_from_directory_path(root), glob, partitions)
}

fn file_manifest_with_columns(
    source_name: &str,
    format: &str,
    location: &str,
    glob: &str,
    columns: &[serde_json::Value],
    format_options: Option<serde_json::Value>,
) -> ValidatedSourceManifest {
    let mut table = json!({
        "name": "events",
        "description": "events",
        "format": format,
        "source": {
            "location": location,
            "glob": glob,
        },
        "columns": columns,
    });
    if let Some(format_options) = format_options {
        table
            .as_object_mut()
            .expect("table object")
            .insert("format_options".to_string(), format_options);
    }
    parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": source_name,
        "version": "0.1.0",
        "backend": "file",
        "tables": [table],
    }))
    .expect("file manifest should parse")
}

fn events_manifest(
    source_name: &str,
    format: &str,
    location: &str,
    glob: &str,
    format_options: Option<serde_json::Value>,
) -> ValidatedSourceManifest {
    file_manifest_with_columns(
        source_name,
        format,
        location,
        glob,
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        format_options,
    )
}

fn events_manifest_for_dir(
    source_name: &str,
    format: &str,
    root: &Path,
    glob: &str,
    format_options: Option<serde_json::Value>,
) -> ValidatedSourceManifest {
    events_manifest(
        source_name,
        format,
        &file_url_from_directory_path(root),
        glob,
        format_options,
    )
}

fn write_metrics_fixture(root: &Path) {
    write_metrics_fixture_for_day(
        root,
        "2026-03-10",
        &[("cpu.usage", 0.42), ("memory.usage", 12.5)],
        "metrics.parquet",
    );
}

fn write_metrics_fixture_for_day(root: &Path, day: &str, rows: &[(&str, f64)], file_name: &str) {
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

    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer should be created");
    writer.write(&batch).expect("batch should be written");
    writer.close().expect("writer should close");
}

fn write_dictionary_i64_parquet(root: &Path, file_name: &str, value: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int64)),
        false,
    )]));
    let keys = UInt16Array::from(vec![0_u16]);
    let values = Arc::new(Int64Array::from(vec![value]));
    let column =
        Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values).expect("dict array"));
    write_val_parquet(root, file_name, schema, column);
}

fn write_plain_i64_parquet(root: &Path, file_name: &str, value: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
    let column = Arc::new(Int64Array::from(vec![value]));
    write_val_parquet(root, file_name, schema, column);
}

fn write_val_parquet(
    root: &Path,
    file_name: &str,
    schema: Arc<Schema>,
    column: Arc<dyn datafusion::arrow::array::Array>,
) {
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("batch");
    let file = std::fs::File::create(root.join(file_name)).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn object_meta(path: &str) -> ObjectMeta {
    ObjectMeta {
        location: ObjectPath::parse(path).expect("object path should parse"),
        last_modified: chrono::Utc::now(),
        size: 1,
        e_tag: None,
        version: None,
    }
}
