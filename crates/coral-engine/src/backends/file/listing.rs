//! File listing, URL, format, and object-store setup helpers.
//!
//! This backend deliberately omits the `client`, `transport`, `response`, and
//! `fetch` modules used by request/response backends. `DataFusion`'s
//! `ListingTable` plus its `ObjectStore` registry own per-request transport,
//! response decoding, and parallel scan orchestration internally. This module
//! is the setup-and-hand-off layer: it builds an object-store handle and
//! listing-table configuration, then hands both to `DataFusion`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::file_format::FileFormat as DataFusionFileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;

use crate::backends::shared::template::{RenderContext, render_template};
use coral_spec::ParsedTemplate;
use coral_spec::backends::file::{
    FileFormat, FileObjectStoreSpec, FileSourceSpec, FileTableSpec, S3AuthSpec,
};

use super::error::FileBackendError;
use super::partitions::PartitionColumns;

pub(super) struct PreparedListingTable {
    pub(super) table_path: ListingTableUrl,
    pub(super) object_store: Arc<dyn ObjectStore>,
    pub(super) listing_options: ListingOptions,
    pub(super) partition_columns: PartitionColumns,
}

pub(super) async fn prepare_listing_table(
    ctx: &SessionContext,
    source_schema: &str,
    table: &FileTableSpec,
    home_dir: Option<&Path>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<PreparedListingTable> {
    let source = &table.source;
    let format = table.format;
    let (table_path, is_collection, uses_default_glob) = resolve_listing_table_url(
        source_schema,
        table.name(),
        format,
        source,
        home_dir,
        resolved_inputs,
    )?;
    let object_store = build_object_store(source_schema, &table_path, source, resolved_inputs)?;
    ctx.register_object_store(table_path.object_store().as_ref(), object_store.clone());

    let partition_columns = PartitionColumns::try_new(&source.partitions)?;
    let listing_options = ListingOptions::new(datafusion_file_format(format, table))
        .with_session_config_options(ctx.state().config())
        .with_file_extension(listing_file_extension(
            format,
            is_collection,
            uses_default_glob,
        ))
        .with_table_partition_cols(partition_columns.hive_arrow_columns());

    listing_options
        .validate_partitions(&ctx.state(), &table_path)
        .await?;

    Ok(PreparedListingTable {
        table_path,
        object_store,
        listing_options,
        partition_columns,
    })
}

fn datafusion_file_format(
    format: FileFormat,
    table: &FileTableSpec,
) -> Arc<dyn DataFusionFileFormat> {
    match format {
        FileFormat::Parquet => Arc::new(ParquetFormat::default()),
        FileFormat::Jsonl => Arc::new(JsonFormat::default().with_newline_delimited(true)),
        FileFormat::Json => Arc::new(JsonFormat::default().with_newline_delimited(false)),
        FileFormat::Csv => Arc::new(
            CsvFormat::default()
                .with_has_header(table.format_options.csv_has_header())
                .with_delimiter(table.format_options.csv_delimiter()),
        ),
    }
}

fn listing_file_extension(
    format: FileFormat,
    is_collection: bool,
    uses_default_glob: bool,
) -> &'static str {
    if is_collection && uses_default_glob {
        format.default_extension()
    } else {
        ""
    }
}

fn resolve_listing_table_url(
    source_schema: &str,
    table_name: &str,
    format: FileFormat,
    source: &FileSourceSpec,
    home_dir: Option<&Path>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<(ListingTableUrl, bool, bool)> {
    let location = listing_location(&source.location, home_dir, resolved_inputs)?;
    let mut table_path = ListingTableUrl::parse(&location).map_err(|error| {
        FileBackendError::InvalidSourceLocation {
            source_schema: source_schema.to_string(),
            table: table_name.to_string(),
            location: source.location.raw().to_string(),
            detail: error.to_string(),
        }
        .plan()
    })?;

    let is_collection = table_path.is_collection();
    let uses_default_glob = source.glob.is_none();
    if is_collection {
        table_path = table_path.with_glob(source.glob_or_default(format))?;
    }

    validate_local_location_exists(
        source_schema,
        table_name,
        &table_path,
        source.location.raw(),
    )?;

    Ok((table_path, is_collection, uses_default_glob))
}

fn listing_location(
    location: &ParsedTemplate,
    home_dir: Option<&Path>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<String> {
    let context = RenderContext::source_scoped(resolved_inputs);
    let location = render_template(location, &context)?;
    normalize_listing_location(&location, home_dir)
}

fn normalize_listing_location(location: &str, home_dir: Option<&Path>) -> Result<String> {
    if let Some(rest) = location.strip_prefix("file://~/") {
        reject_file_location_components(location)?;
        let home = home_dir.ok_or_else(|| {
            DataFusionError::Plan(
                "source.location uses '~' but home directory is not available".to_string(),
            )
        })?;
        let decoded = urlencoding::decode(rest).map_err(|error| {
            DataFusionError::Plan(format!("source.location has invalid encoding: {error}"))
        })?;
        return file_url_from_path(location, &home.join(decoded.as_ref()), rest.ends_with('/'));
    }

    if location.starts_with("file://") {
        let normalized = if let Some(rest) = location.strip_prefix("file://localhost/") {
            format!("file:///{rest}")
        } else {
            location.to_string()
        };
        let url = url::Url::parse(&normalized).map_err(|error| {
            DataFusionError::Plan(format!("source.location has invalid file URL: {error}"))
        })?;
        reject_file_url_components(location, &url)?;
        let path = url.to_file_path().map_err(|()| {
            DataFusionError::Plan(format!(
                "source.location must be a valid local file URL, got '{location}'"
            ))
        })?;
        return file_url_from_path(location, &path, url.path().ends_with('/'));
    }

    Ok(location.to_string())
}

fn reject_file_location_components(location: &str) -> Result<()> {
    if location.contains('?') || location.contains('#') {
        return Err(DataFusionError::Plan(format!(
            "source.location must not include query or fragment components, got '{location}'"
        )));
    }
    Ok(())
}

fn reject_file_url_components(location: &str, url: &url::Url) -> Result<()> {
    if url.query().is_some() || url.fragment().is_some() {
        return Err(DataFusionError::Plan(format!(
            "source.location must not include query or fragment components, got '{location}'"
        )));
    }
    Ok(())
}

fn file_url_from_path(location: &str, path: &Path, is_collection: bool) -> Result<String> {
    let url = if is_collection {
        url::Url::from_directory_path(path)
    } else {
        url::Url::from_file_path(path)
    }
    .map_err(|()| {
        DataFusionError::Plan(format!(
            "source.location must be a valid local file URL, got '{location}'"
        ))
    })?;
    Ok(url.to_string())
}

fn validate_local_location_exists(
    source_schema: &str,
    table_name: &str,
    table_path: &ListingTableUrl,
    manifest_location: &str,
) -> Result<()> {
    if table_path.scheme() != "file" {
        return Ok(());
    }

    let path = table_path.get_url().to_file_path().map_err(|()| {
        DataFusionError::Plan(format!(
            "{source_schema}.{table_name} source.location '{manifest_location}' is not a valid file path"
        ))
    })?;
    if table_path.is_collection() {
        if !path.is_dir() {
            return Err(DataFusionError::Plan(format!(
                "{source_schema}.{table_name} source.location '{manifest_location}' is not a directory"
            )));
        }
    } else if !path.is_file() {
        return Err(DataFusionError::Plan(format!(
            "{source_schema}.{table_name} source.location '{manifest_location}' is not a file"
        )));
    }

    Ok(())
}

fn build_object_store(
    source_schema: &str,
    table_path: &ListingTableUrl,
    source: &FileSourceSpec,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<Arc<dyn ObjectStore>> {
    match table_path.scheme() {
        "file" => Ok(Arc::new(LocalFileSystem::new())),
        "s3" => {
            let bucket = table_path.get_url().host_str().ok_or_else(|| {
                FileBackendError::MissingS3Bucket {
                    source_schema: source_schema.to_string(),
                    location: table_path.as_str().to_string(),
                }
                .plan()
            })?;

            let Some(FileObjectStoreSpec::S3 { region, auth }) = source.object_store.as_ref()
            else {
                return Err(FileBackendError::MissingS3ObjectStore {
                    source_schema: source_schema.to_string(),
                    location: table_path.as_str().to_string(),
                }
                .plan());
            };

            let context = RenderContext::source_scoped(resolved_inputs);

            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            if let Some(region) = region {
                builder = builder.with_region(render_template(region, &context)?);
            }

            match auth {
                S3AuthSpec::AccessKey {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => {
                    builder = builder
                        .with_access_key_id(render_template(access_key_id, &context)?)
                        .with_secret_access_key(render_template(secret_access_key, &context)?);

                    if let Some(session_token) = session_token {
                        builder = builder.with_token(render_template(session_token, &context)?);
                    }
                }
                S3AuthSpec::InstanceProfile => {}
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
        other => Err(FileBackendError::UnsupportedScheme {
            source_schema: source_schema.to_string(),
            scheme: other.to_string(),
        }
        .plan()),
    }
}

pub(super) fn parse_bool(value: &str) -> Result<bool> {
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
    use super::*;
    use tempfile::tempdir;

    fn file_url_from_directory_path(path: &Path) -> String {
        url::Url::from_directory_path(path)
            .expect("path should convert to file URL")
            .to_string()
    }

    #[test]
    fn listing_location_accepts_native_file_urls() {
        let fixture_dir = tempdir().expect("tempdir");
        let location = ParsedTemplate::parse(file_url_from_directory_path(fixture_dir.path()))
            .expect("location template should parse");

        let resolved =
            listing_location(&location, None, &BTreeMap::new()).expect("file URL should resolve");

        assert_eq!(resolved, file_url_from_directory_path(fixture_dir.path()));
    }

    #[test]
    fn listing_location_accepts_localhost_file_urls() {
        let fixture_dir = tempdir().expect("tempdir");
        let location = file_url_from_directory_path(fixture_dir.path()).replacen(
            "file:///",
            "file://localhost/",
            1,
        );
        let location = ParsedTemplate::parse(&location).expect("location template should parse");

        let resolved = listing_location(&location, None, &BTreeMap::new())
            .expect("localhost file URL should resolve");

        assert_eq!(resolved, file_url_from_directory_path(fixture_dir.path()));
    }

    #[test]
    fn listing_location_expands_home_relative_file_urls() {
        let home = tempdir().expect("home");
        let location = ParsedTemplate::parse("file://~/nested%20dir/")
            .expect("location template should parse");

        let resolved = listing_location(&location, Some(home.path()), &BTreeMap::new())
            .expect("home-relative file URL should resolve");
        let resolved_path = url::Url::parse(&resolved)
            .expect("resolved location should be a URL")
            .to_file_path()
            .expect("resolved location should be a file URL");

        assert_eq!(resolved_path, home.path().join("nested dir"));
        assert!(
            resolved.ends_with('/'),
            "home-relative directory locations must remain collections"
        );
    }

    #[test]
    fn listing_location_rejects_query_components() {
        let fixture_dir = tempdir().expect("tempdir");
        let location = ParsedTemplate::parse(format!(
            "{}?download=1",
            file_url_from_directory_path(fixture_dir.path())
        ))
        .expect("location template should parse");

        let error = listing_location(&location, None, &BTreeMap::new())
            .expect_err("query components should be rejected");

        assert!(
            error
                .to_string()
                .contains("must not include query or fragment components"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn listing_location_rejects_home_relative_query_components() {
        let home = tempdir().expect("home");
        let location = ParsedTemplate::parse("file://~/nested/?download=1")
            .expect("location template should parse");

        let error = listing_location(&location, Some(home.path()), &BTreeMap::new())
            .expect_err("home-relative query components should be rejected");

        assert!(
            error
                .to_string()
                .contains("must not include query or fragment components"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn s3_object_store_uses_typed_source_config() {
        let table_path = ListingTableUrl::parse("s3://example-bucket/events/")
            .expect("s3 table path should parse");
        let source = FileSourceSpec {
            location: ParsedTemplate::parse("s3://example-bucket/events/")
                .expect("location template should parse"),
            glob: None,
            partitions: vec![],
            object_store: Some(FileObjectStoreSpec::S3 {
                region: Some(
                    ParsedTemplate::parse("{{input.AWS_REGION}}")
                        .expect("region template should parse"),
                ),
                auth: S3AuthSpec::InstanceProfile,
            }),
        };
        let mut resolved_inputs = BTreeMap::new();
        resolved_inputs.insert("AWS_REGION".to_string(), "us-east-1".to_string());

        build_object_store("codex", &table_path, &source, &resolved_inputs)
            .expect("S3 object store should use typed object-store settings");
    }

    #[test]
    fn s3_object_store_accepts_access_key_config() {
        let table_path = ListingTableUrl::parse("s3://example-bucket/events/")
            .expect("s3 table path should parse");
        let source = FileSourceSpec {
            location: ParsedTemplate::parse("s3://example-bucket/events/")
                .expect("location template should parse"),
            glob: None,
            partitions: vec![],
            object_store: Some(FileObjectStoreSpec::S3 {
                region: None,
                auth: S3AuthSpec::AccessKey {
                    access_key_id: ParsedTemplate::parse("{{input.AWS_ACCESS_KEY_ID}}")
                        .expect("access key template should parse"),
                    secret_access_key: ParsedTemplate::parse("{{input.AWS_SECRET_ACCESS_KEY}}")
                        .expect("secret key template should parse"),
                    session_token: Some(
                        ParsedTemplate::parse("{{input.AWS_SESSION_TOKEN}}")
                            .expect("session token template should parse"),
                    ),
                },
            }),
        };
        let resolved_inputs = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".to_string(), "access-key".to_string()),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                "secret-key".to_string(),
            ),
            ("AWS_SESSION_TOKEN".to_string(), "session-token".to_string()),
        ]);

        build_object_store("codex", &table_path, &source, &resolved_inputs)
            .expect("S3 object store should accept access-key auth settings");
    }
}
