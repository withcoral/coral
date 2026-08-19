use super::file_groups::file_groups_for_scan;
use super::metadata::FileMetadataColumns;
use super::partitions::{
    PartitionColumns, partition_filter_constraints, partition_values_for_path,
};
use super::provider::FileTableProvider;
use crate::backends::shared::source_observation::test_support::{
    RecordedSourceObservation, RecordingSourceObservationPublisher,
};
use crate::backends::{BackendCompileRequest, compile_source_manifest, compile_validated_manifest};
use crate::runtime::catalog;
use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
use crate::{QueryRuntimeContext, QuerySource, SourceObservationPublisher};
use coral_spec::backends::file::{
    FilePartitionDataType, FileTableSpec, PartitionColumnSpec, PartitionPathSpec,
};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};
use datafusion::arrow::array::{
    Array as _, DictionaryArray, Float64Array, Int64Array, StringArray, UInt16Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use object_store::ObjectMeta;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::fs;
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

    let ctx = SessionContext::new();
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = parquet_manifest(&location);

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("file source should register");

    let provider = ctx
        .catalog("datafusion")
        .expect("catalog should exist")
        .schema("otel")
        .expect("schema should exist")
        .table("metrics")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert!(provider.downcast_ref::<FileTableProvider>().is_some());

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
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = parquet_manifest(&location);

    let active_sources = register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("file source should register");
    catalog::register(
        &ctx,
        &active_sources.active_sources,
        &active_sources.column_fetchers,
        &[],
        catalog::CatalogColumnFetchFailures::default(),
    )
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
    let location = file_url_from_directory_path(fixture_dir.path());
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

#[tokio::test]
async fn file_provider_reads_jsonl_with_listing_table() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    fs::write(
        fixture_dir.path().join("events.jsonl"),
        r#"{"id":1,"kind":"user"}
{"id":2,"kind":"assistant"}
"#,
    )
    .expect("jsonl fixture should be written");

    let ctx = SessionContext::new();
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = file_manifest_with_columns(
        "jsonl_demo",
        "jsonl",
        &location,
        "**/*.jsonl",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        None,
    );

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("jsonl source should register");

    let provider = ctx
        .catalog("datafusion")
        .expect("catalog should exist")
        .schema("jsonl_demo")
        .expect("schema should exist")
        .table("events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert!(
        provider.downcast_ref::<FileTableProvider>().is_some(),
        "plain JSONL should use DataFusion's native listing-table provider"
    );

    let batches = ctx
        .sql("SELECT id, kind FROM jsonl_demo.events ORDER BY id")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();

    assert!(rendered.contains("assistant"));
    assert!(rendered.contains("user"));
}

#[tokio::test]
async fn file_provider_honors_custom_glob_extension() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    fs::write(
        fixture_dir.path().join("events.ndjson"),
        r#"{"id":1,"kind":"user"}
{"id":2,"kind":"assistant"}
"#,
    )
    .expect("jsonl fixture should be written");

    let ctx = SessionContext::new();
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = file_manifest_with_columns(
        "custom_ext_demo",
        "jsonl",
        &location,
        "**/*.ndjson",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        None,
    );

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("jsonl source should register");

    let batches = ctx
        .sql("SELECT COUNT(*) AS rows FROM custom_ext_demo.events")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();

    assert!(rendered.contains('2'));
}

#[tokio::test]
async fn file_provider_honors_explicit_file_without_default_extension() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    let file_path = fixture_dir.path().join("events.data");
    fs::write(
        &file_path,
        r#"{"id":1,"kind":"user"}
{"id":2,"kind":"assistant"}
"#,
    )
    .expect("jsonl fixture should be written");

    let ctx = SessionContext::new();
    let location = file_url_from_file_path(&file_path);
    let manifest = file_manifest_with_columns(
        "explicit_file_demo",
        "jsonl",
        &location,
        "**/*.jsonl",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        None,
    );

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("jsonl source should register");

    let batches = ctx
        .sql("SELECT kind FROM explicit_file_demo.events ORDER BY id")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();

    assert!(rendered.contains("assistant"));
    assert!(rendered.contains("user"));
}

#[tokio::test]
async fn file_provider_reads_json_array_with_listing_table() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    fs::write(
        fixture_dir.path().join("events.json"),
        r#"[{"id":1,"kind":"user"},{"id":2,"kind":"assistant"}]"#,
    )
    .expect("json fixture should be written");

    let ctx = SessionContext::new();
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = file_manifest_with_columns(
        "json_demo",
        "json",
        &location,
        "**/*.json",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        None,
    );

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("json source should register");

    let batches = ctx
        .sql("SELECT COUNT(*) AS rows FROM json_demo.events")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();

    assert!(rendered.contains('2'));
}

#[tokio::test]
async fn file_provider_reads_csv_with_format_options() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    fs::write(
        fixture_dir.path().join("events.csv"),
        "id|kind\n1|user\n2|assistant\n",
    )
    .expect("csv fixture should be written");

    let ctx = SessionContext::new();
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = file_manifest_with_columns(
        "csv_demo",
        "csv",
        &location,
        "**/*.csv",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        Some(json!({ "has_header": true, "delimiter": "|" })),
    );

    register_sources_blocking(&ctx, compile_sources(vec![manifest]))
        .expect("csv source should register");

    let batches = ctx
        .sql("SELECT kind FROM csv_demo.events ORDER BY id")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();

    assert!(rendered.contains("assistant"));
    assert!(rendered.contains("user"));
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
        let file =
            std::fs::File::create(dir.path().join("dict.parquet")).expect("create dict.parquet");
        let mut w = ArrowWriter::try_new(file, schema, None).expect("writer");
        w.write(&batch).expect("write");
        w.close().expect("close");
    }

    // File 2: "val" column is plain Int64.
    {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from(vec![200i64]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).expect("batch");
        let file =
            std::fs::File::create(dir.path().join("plain.parquet")).expect("create plain.parquet");
        let mut w = ArrowWriter::try_new(file, schema, None).expect("writer");
        w.write(&batch).expect("write");
        w.close().expect("close");
    }

    let location = file_url_from_directory_path(dir.path());
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

    let location = file_url_from_directory_path(dir.path());
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
    let location = file_url_from_directory_path(dir.path());
    let table = parquet_table_spec(&location);
    let result = FileTableProvider::try_new(&ctx, "otel", table, None, &BTreeMap::default());
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
    let location = file_url_from_directory_path(dir.path());
    let table = parquet_table_spec(&location);
    let result = FileTableProvider::try_new(&ctx, "otel", table, None, &BTreeMap::default());
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
    let location = file_url_from_directory_path(dir.path());
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

/// Compiles sources the way the engine does when observed-value capture is on.
///
/// `compile_sources` above is the capture-off path: it leaves the publisher
/// list empty, so nothing wraps the file providers.
fn compile_sources_with_observation_publishers(
    manifests: Vec<ValidatedSourceManifest>,
    publishers: &[Arc<dyn SourceObservationPublisher>],
) -> Vec<CompiledQuerySource> {
    let runtime_context = QueryRuntimeContext::default();
    let request_authenticators = HashMap::new();
    let request_identity_http_authenticators = HashMap::new();
    manifests
        .into_iter()
        .map(|manifest| {
            let source = QuerySource::new(manifest.clone(), BTreeMap::new(), BTreeMap::new());
            let compiled = compile_validated_manifest(
                &manifest,
                &BackendCompileRequest {
                    source: &source,
                    runtime_context: &runtime_context,
                    database_pool_registry: Arc::new(crate::DatabasePoolRegistry::new()),
                    source_secrets: BTreeMap::new(),
                    source_variables: BTreeMap::new(),
                    request_authenticators: &request_authenticators,
                    source_input_resolver: None,
                    source_observation_publishers: publishers,
                    request_identity_http_authenticators: &request_identity_http_authenticators,
                },
            )
            .expect("manifest should compile");
            CompiledQuerySource { source, compiled }
        })
        .collect()
}

/// Registers `manifest` with observed-value capture on, runs `sql`, and
/// returns the rendered rows next to what the scan published.
async fn observed_query(
    manifest: ValidatedSourceManifest,
    sql: &str,
) -> (String, Vec<RecordedSourceObservation>) {
    let ctx = SessionContext::new();
    let publisher = Arc::new(RecordingSourceObservationPublisher::default());
    register_sources_blocking(
        &ctx,
        compile_sources_with_observation_publishers(
            vec![manifest],
            &[Arc::clone(&publisher) as Arc<dyn SourceObservationPublisher>],
        ),
    )
    .expect("observed file source should register");

    let batches = ctx
        .sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    (rendered, publisher.observations())
}

fn observed_rows(observations: &[RecordedSourceObservation]) -> usize {
    observations
        .iter()
        .map(|observation| observation.row_count)
        .sum()
}

fn observed_values(observations: &[RecordedSourceObservation], column: &str) -> Vec<String> {
    observations
        .iter()
        .flat_map(|observation| {
            let batch = &observation.batch;
            let index = batch.schema().index_of(column).expect("observed column");
            let values = batch.column(index);
            let formatter = ArrayFormatter::try_new(values.as_ref(), &FormatOptions::default())
                .expect("observed column should render");
            (0..values.len())
                .map(|row| formatter.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn parquet_scan_publishes_observations_for_the_projected_columns() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture(fixture_dir.path());
    let location = file_url_from_directory_path(fixture_dir.path());

    let (rendered, observations) = observed_query(
        parquet_manifest(&location),
        "SELECT metric FROM otel.metrics ORDER BY metric",
    )
    .await;

    assert!(rendered.contains("cpu.usage"));
    assert!(!observations.is_empty(), "the parquet scan should publish");
    for observation in &observations {
        assert_eq!(observation.source_name, "otel");
        assert_eq!(observation.surface_name, "metrics");
        assert_eq!(observation.column_names, vec!["metric".to_string()]);
    }
    let mut observed = observed_values(&observations, "metric");
    observed.sort();
    assert_eq!(observed, vec!["cpu.usage", "memory.usage"]);
}

#[tokio::test]
async fn csv_scan_publishes_observations() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    fs::write(
        fixture_dir.path().join("events.csv"),
        "id|kind\n1|user\n2|assistant\n",
    )
    .expect("csv fixture should be written");
    let location = file_url_from_directory_path(fixture_dir.path());
    let manifest = file_manifest_with_columns(
        "csv_demo",
        "csv",
        &location,
        "**/*.csv",
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "kind", "type": "Utf8" }),
        ],
        Some(json!({ "has_header": true, "delimiter": "|" })),
    );

    let (_, observations) = observed_query(manifest, "SELECT kind FROM csv_demo.events").await;

    assert_eq!(observed_rows(&observations), 2);
    let mut observed = observed_values(&observations, "kind");
    observed.sort();
    assert_eq!(observed, vec!["assistant", "user"]);
}

#[tokio::test]
async fn custom_json_provider_scan_publishes_observations() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    let partition_dir = fixture_dir.path().join("day=2026-03-10");
    fs::create_dir_all(&partition_dir).expect("partition dir should exist");
    fs::write(
        partition_dir.join("events.jsonl"),
        "{\"id\":1,\"kind\":\"user\"}\n{\"id\":2,\"kind\":\"assistant\"}\n",
    )
    .expect("jsonl fixture should be written");
    let location = file_url_from_directory_path(fixture_dir.path());

    let (_, observations) = observed_query(
        partitioned_jsonl_manifest(&location),
        "SELECT kind FROM jsonl_partitioned.events",
    )
    .await;

    assert_eq!(observed_rows(&observations), 2);
    let mut observed = observed_values(&observations, "kind");
    observed.sort();
    assert_eq!(observed, vec!["assistant", "user"]);
}

#[tokio::test]
async fn partition_pruned_scan_publishes_only_the_surviving_files() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture_for_day(
        fixture_dir.path(),
        "2026-03-10",
        &[("cpu.usage", 0.42)],
        "metrics.parquet",
    );
    write_metrics_fixture_for_day(
        fixture_dir.path(),
        "2026-03-11",
        &[("disk.usage", 7.5)],
        "metrics.parquet",
    );
    let location = file_url_from_directory_path(fixture_dir.path());

    let (rendered, observations) = observed_query(
        parquet_manifest(&location),
        "SELECT metric FROM otel.metrics WHERE date = '2026-03-10'",
    )
    .await;

    assert!(rendered.contains("cpu.usage"));
    assert!(!rendered.contains("disk.usage"));
    assert_eq!(
        observed_values(&observations, "metric"),
        vec!["cpu.usage"],
        "the pruned partition's file must not be observed"
    );
}

#[tokio::test]
async fn a_predicate_over_the_table_selects_what_is_observed() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture(fixture_dir.path());
    let location = file_url_from_directory_path(fixture_dir.path());
    let sql = "SELECT metric, value FROM otel.metrics WHERE metric = 'cpu.usage'";

    let (observed_rendered, observations) = observed_query(parquet_manifest(&location), sql).await;

    assert_eq!(
        observed_values(&observations, "metric"),
        vec!["cpu.usage"],
        "only the rows the predicate selects should be observed"
    );

    let ctx = SessionContext::new();
    register_sources_blocking(&ctx, compile_sources(vec![parquet_manifest(&location)]))
        .expect("file source should register");
    let batches = ctx
        .sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    let plain_rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert_eq!(
        observed_rendered, plain_rendered,
        "observation must not change what the query returns"
    );
}

#[tokio::test]
async fn without_observation_publishers_file_providers_are_registered_unwrapped() {
    let fixture_dir = tempdir().expect("tempdir should be created");
    write_metrics_fixture(fixture_dir.path());
    let location = file_url_from_directory_path(fixture_dir.path());

    let plain_ctx = SessionContext::new();
    register_sources_blocking(
        &plain_ctx,
        compile_sources(vec![parquet_manifest(&location)]),
    )
    .expect("file source should register");
    let observed_ctx = SessionContext::new();
    register_sources_blocking(
        &observed_ctx,
        compile_sources_with_observation_publishers(
            vec![parquet_manifest(&location)],
            &[Arc::new(RecordingSourceObservationPublisher::default())],
        ),
    )
    .expect("observed file source should register");

    assert!(
        registered_metrics_provider(&plain_ctx)
            .await
            .downcast_ref::<FileTableProvider>()
            .is_some(),
        "capture off must register the file provider itself, not a wrapper"
    );
    assert!(
        registered_metrics_provider(&observed_ctx)
            .await
            .downcast_ref::<FileTableProvider>()
            .is_none(),
        "capture on must register the observing wrapper"
    );
}

async fn registered_metrics_provider(
    ctx: &SessionContext,
) -> Arc<dyn datafusion::catalog::TableProvider> {
    ctx.catalog("datafusion")
        .expect("catalog should exist")
        .schema("otel")
        .expect("schema should exist")
        .table("metrics")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist")
}

fn partitioned_jsonl_manifest(location: &str) -> ValidatedSourceManifest {
    parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "jsonl_partitioned",
        "version": "0.1.0",
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "events",
            "format": "jsonl",
            "source": {
                "location": location,
                "glob": "**/*.jsonl",
                "partitions": [{ "name": "day", "type": "Utf8" }],
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "kind", "type": "Utf8" },
            ],
        }]
    }))
    .expect("partitioned jsonl manifest should parse")
}

fn parquet_table_spec(location: &str) -> FileTableSpec {
    parquet_table_spec_with_glob(location, "**/*.parquet")
}

fn parquet_table_spec_with_glob(location: &str, glob: &str) -> FileTableSpec {
    let source_manifest = parquet_manifest_with_glob_and_partitions(location, glob, &[]);
    let manifest = source_manifest.as_file().expect("file manifest");
    manifest.tables.first().expect("parquet table").clone()
}

fn parquet_manifest_no_partitions(location: &str) -> ValidatedSourceManifest {
    parquet_manifest_no_partitions_with_glob(location, "**/*.parquet")
}

fn parquet_manifest_no_partitions_with_glob(location: &str, glob: &str) -> ValidatedSourceManifest {
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

    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer should be created");
    writer.write(&batch).expect("batch should be written");
    writer.close().expect("writer should close");
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
