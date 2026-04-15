#![allow(
    missing_docs,
    reason = "This module exposes many field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for file-backed sources.
//!
//! File-backed manifests cover backends that read from object stores or local
//! filesystems, currently `parquet` and `jsonl`. This module normalizes source
//! locations, file globs, partition metadata, and declared table columns.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use url::Url;

use crate::common::parse_manifest_data_type;
use crate::{
    ColumnSpec, FilterSpec, ManifestDataType, ManifestError, Result, SourceBackend,
    SourceManifestCommon, TableCommon, validate_columns, validate_filters_and_column_exprs,
};

/// Validated top-level manifest for a `Parquet`-backed source.
#[derive(Debug, Clone)]
pub struct ParquetSourceManifest {
    pub common: SourceManifestCommon,
    pub tables: Vec<FileTableSpec>,
}

/// Validated top-level manifest for a `JSONL`-backed source.
#[derive(Debug, Clone)]
pub struct JsonlSourceManifest {
    pub common: SourceManifestCommon,
    pub tables: Vec<FileTableSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFileSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    backend: SourceBackend,
    tables: Vec<RawFileTableSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFileTableSpec {
    name: String,
    description: String,
    #[serde(default)]
    guide: String,
    #[serde(default)]
    filters: Vec<FilterSpec>,
    #[serde(default)]
    fetch_limit_default: Option<usize>,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
    source: FileSourceSpec,
}

/// One validated file-backed table declaration.
#[derive(Debug, Clone)]
pub struct FileTableSpec {
    pub common: TableCommon,
    pub source: FileSourceSpec,
}

impl FileTableSpec {
    #[must_use]
    /// Returns the stable table name.
    pub fn name(&self) -> &str {
        &self.common.name
    }

    #[must_use]
    /// Returns the declared SQL filters for this table.
    pub fn filters(&self) -> &[FilterSpec] {
        &self.common.filters
    }

    #[must_use]
    /// Returns the declared output columns for this table.
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.common.columns
    }

    #[must_use]
    /// Returns whether the manifest explicitly declared output columns.
    ///
    /// When this is `false`, the engine may need to infer a schema from the
    /// underlying files.
    pub fn has_explicit_columns(&self) -> bool {
        !self.columns().is_empty()
    }
}

/// File-backed source configuration shared by `Parquet` and `JSONL` backends.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSourceSpec {
    pub location: String,
    #[serde(default)]
    pub glob: Option<String>,
    #[serde(default)]
    pub partitions: Vec<PartitionColumnSpec>,
}

impl FileSourceSpec {
    #[must_use]
    /// Returns the configured parquet glob or the manifest default.
    pub fn parquet_glob_or_default(&self) -> &str {
        self.glob.as_deref().unwrap_or("**/*.parquet")
    }

    #[must_use]
    /// Returns the configured JSONL glob or the manifest default.
    pub fn jsonl_glob_or_default(&self) -> &str {
        self.glob.as_deref().unwrap_or("**/*.jsonl")
    }

    /// Validates file-backed source settings for a parquet table.
    fn validate_for_parquet(&self, schema: &str, table: &str) -> Result<()> {
        self.validate_common(schema, table)?;
        let location = self.parse_location(schema, table)?;
        if !matches!(location.scheme(), "file" | "s3") {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} source.location scheme '{}' is unsupported (expected file:// or s3://)",
                location.scheme()
            )));
        }
        Ok(())
    }

    /// Validates file-backed source settings for a JSONL table.
    fn validate_for_jsonl(&self, schema: &str, table: &str) -> Result<()> {
        self.validate_common(schema, table)?;
        let location = self.parse_location(schema, table)?;
        if location.scheme() != "file" {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} source.location scheme '{}' is unsupported for jsonl (expected file://)",
                location.scheme()
            )));
        }
        Ok(())
    }

    fn validate_common(&self, schema: &str, table: &str) -> Result<()> {
        let mut seen_partitions = HashSet::new();
        for partition in &self.partitions {
            if !seen_partitions.insert(partition.name.clone()) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} has duplicate partition '{}'",
                    partition.name
                )));
            }
            let _ = partition.manifest_data_type()?;
        }

        Ok(())
    }

    fn parse_location(&self, schema: &str, table: &str) -> Result<Url> {
        let check_location = if self.location.starts_with("file://~/") {
            self.location
                .replacen("file://~/", "file:///placeholder/", 1)
        } else {
            self.location.clone()
        };

        Url::parse(&check_location).map_err(|error| {
            ManifestError::validation(format!(
                "{schema}.{table} has invalid source.location '{}': {error}",
                self.location
            ))
        })
    }
}

/// One declared partition column derived from the file path layout.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl PartitionColumnSpec {
    /// Parses the partition column type into a normalized manifest data type.
    pub fn manifest_data_type(&self) -> Result<ManifestDataType> {
        parse_manifest_data_type(&self.data_type)
    }
}

impl RawFileTableSpec {
    fn into_validated_parquet(self, schema: &str) -> Result<FileTableSpec> {
        self.source.validate_for_parquet(schema, &self.name)?;
        validate_columns(&self.columns, schema, &self.name)?;

        let partition_names = self
            .source
            .partitions
            .iter()
            .map(|partition| partition.name.as_str())
            .collect::<HashSet<_>>();

        for col in &self.columns {
            if partition_names.contains(col.name.as_str()) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{} column '{}' duplicates a partition column",
                    self.name, col.name
                )));
            }
        }

        Ok(FileTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                self.filters,
                self.fetch_limit_default,
                self.columns,
            ),
            source: self.source,
        })
    }

    fn into_validated_jsonl(self, schema: &str) -> Result<FileTableSpec> {
        if self.columns.is_empty() {
            return Err(ManifestError::validation(format!(
                "{schema}.{} uses backend=jsonl and must define columns",
                self.name
            )));
        }
        self.source.validate_for_jsonl(schema, &self.name)?;
        validate_columns(&self.columns, schema, &self.name)?;
        validate_filters_and_column_exprs(&self.filters, &self.columns, schema, &self.name)?;

        Ok(FileTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                self.filters,
                self.fetch_limit_default,
                self.columns,
            ),
            source: self.source,
        })
    }
}

impl ParquetSourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw: RawFileSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawFileSourceManifest {
            dsl_version,
            name,
            version,
            description,
            backend: _backend,
            tables,
        } = raw;
        let common = SourceManifestCommon::new(dsl_version, name, version, description);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated_parquet(&common.name))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { common, tables })
    }
}

impl JsonlSourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw: RawFileSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawFileSourceManifest {
            dsl_version,
            name,
            version,
            description,
            backend: _backend,
            tables,
        } = raw;
        let common = SourceManifestCommon::new(dsl_version, name, version, description);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated_jsonl(&common.name))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { common, tables })
    }
}
