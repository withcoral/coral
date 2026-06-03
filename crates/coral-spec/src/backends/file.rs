#![allow(
    missing_docs,
    reason = "This module exposes many field-heavy declarative source-spec types."
)]

//! Backend-owned manifest model and validation for native file-backed sources.
//!
//! File-backed manifests use `backend: file` plus table-level `format` fields
//! so transport (`file://`, `s3://`, and future object stores) is independent
//! from file format. The engine can then route every supported file format
//! through `DataFusion`'s native listing-table machinery.

use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use url::Url;

use crate::common::parse_manifest_data_type;
use crate::inputs::{
    collect_source_inputs_value, declared_secret_input_names, required_secret_input_names,
};
use crate::{
    ColumnSpec, DeclaredRelation, FilterSpec, ManifestDataType, ManifestError, ManifestInputSpec,
    ParsedTemplate, Result, SourceBackend, SourceManifestCommon, TableCommon, TemplateNamespace,
    TemplatePart, validate_columns, validate_declared_relation_namespace, validate_test_queries,
};

/// Validated top-level manifest for a native file-backed source.
#[derive(Debug, Clone)]
pub struct FileSourceManifest {
    pub common: SourceManifestCommon,
    pub tables: Vec<FileTableSpec>,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

impl FileSourceManifest {
    /// Returns all source secrets declared by this manifest.
    pub fn declared_secret_names(&self) -> BTreeSet<String> {
        declared_secret_input_names(&self.declared_inputs)
    }

    /// Returns the source secrets required by this manifest.
    ///
    /// Required declared inputs with `kind: secret` must be available before a
    /// source can compile or authenticate.
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        required_secret_input_names(&self.declared_inputs)
    }
}

/// Supported native file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    Jsonl,
    Json,
    Csv,
}

impl FileFormat {
    fn parse(value: &str, schema: &str, table: &str) -> Result<Self> {
        match value {
            "parquet" => Ok(Self::Parquet),
            "jsonl" => Ok(Self::Jsonl),
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "arrow" | "avro" => Err(ManifestError::validation(format!(
                "{schema}.{table} uses format='{value}', which is out of scope for backend=file"
            ))),
            other => Err(ManifestError::validation(format!(
                "{schema}.{table} uses unsupported file format '{other}'"
            ))),
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Jsonl => "jsonl",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }

    #[must_use]
    pub fn default_glob(self) -> &'static str {
        match self {
            Self::Parquet => "**/*.parquet",
            Self::Jsonl => "**/*.jsonl",
            Self::Json => "**/*.json",
            Self::Csv => "**/*.csv",
        }
    }

    #[must_use]
    pub fn default_extension(self) -> &'static str {
        match self {
            Self::Parquet => ".parquet",
            Self::Jsonl => ".jsonl",
            Self::Json => ".json",
            Self::Csv => ".csv",
        }
    }

    #[must_use]
    fn requires_declared_columns(self) -> bool {
        !matches!(self, Self::Parquet)
    }

    #[must_use]
    fn supports_segment_partitions(self) -> bool {
        matches!(self, Self::Jsonl | Self::Json)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFileSourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    backend: SourceBackend,
    #[serde(default)]
    inputs: Option<Value>,
    tables: Vec<RawFileTableSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFileTableSpec {
    name: String,
    description: String,
    format: String,
    #[serde(default)]
    guide: String,
    #[serde(default)]
    filters: Vec<FilterSpec>,
    #[serde(default)]
    fetch_limit_default: Option<usize>,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
    #[serde(default)]
    format_options: FileFormatOptions,
    source: FileSourceSpec,
}

/// One validated file-backed table declaration.
#[derive(Debug, Clone)]
pub struct FileTableSpec {
    pub common: TableCommon,
    pub format: FileFormat,
    pub format_options: FileFormatOptions,
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

/// File-backed source configuration shared by all native file formats.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSourceSpec {
    pub location: ParsedTemplate,
    #[serde(default)]
    pub glob: Option<String>,
    #[serde(default)]
    pub partitions: Vec<PartitionColumnSpec>,
    #[serde(default)]
    pub object_store: Option<FileObjectStoreSpec>,
}

impl FileSourceSpec {
    #[must_use]
    /// Returns the configured glob or the format default.
    pub fn glob_or_default(&self, format: FileFormat) -> &str {
        self.glob
            .as_deref()
            .unwrap_or_else(|| format.default_glob())
    }

    /// Validates file-backed source settings.
    fn validate_for_file(&self, schema: &str, table: &str, format: FileFormat) -> Result<()> {
        validate_source_scoped_template(schema, table, "source.location", &self.location)?;
        let mut seen_partitions = HashSet::new();
        for partition in &self.partitions {
            if !seen_partitions.insert(partition.name.clone()) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} has duplicate partition '{}'",
                    partition.name
                )));
            }
            if !partition.path.is_hive() && !format.supports_segment_partitions() {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} partition '{}' uses path.kind={}, which is currently supported only for backend=file formats jsonl and json; parquet and csv use DataFusion hive partitioning",
                    partition.name,
                    partition.path.kind()
                )));
            }
        }

        let location = self.parse_location(schema, table)?;
        match (location.scheme(), &self.object_store) {
            ("file", None) => {}
            ("file", Some(_)) => {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} source.object_store is only supported for s3:// locations"
                )));
            }
            ("s3", Some(FileObjectStoreSpec::S3 { region, auth })) => {
                validate_s3_object_store(schema, table, region.as_ref(), auth)?;
            }
            ("s3", None) => {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} uses s3:// source.location and must declare source.object_store with type=s3"
                )));
            }
            (unsupported_scheme, _) => {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} source.location scheme '{unsupported_scheme}' is unsupported for backend=file (expected file:// or s3://)"
                )));
            }
        }

        Ok(())
    }

    fn parse_location(&self, schema: &str, table: &str) -> Result<Url> {
        let rendered = render_template_with_placeholders(&self.location);
        let check_location = if rendered.starts_with("file://~/") {
            rendered.replacen("file://~/", "file:///placeholder/", 1)
        } else {
            rendered
        };

        Url::parse(&check_location).map_err(|error| {
            ManifestError::validation(format!(
                "{schema}.{table} has invalid source.location '{}': {error}",
                self.location.raw()
            ))
        })
    }
}

/// Object-store configuration for file-backed table locations.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileObjectStoreSpec {
    S3 {
        #[serde(default)]
        region: Option<ParsedTemplate>,
        auth: S3AuthSpec,
    },
}

fn validate_s3_object_store(
    schema: &str,
    table: &str,
    region: Option<&ParsedTemplate>,
    auth: &S3AuthSpec,
) -> Result<()> {
    if let Some(region) = region {
        validate_source_scoped_template(schema, table, "source.object_store.region", region)?;
    }
    auth.validate(schema, table)
}

/// Credential mode for an S3 object store.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3AuthSpec {
    AccessKey {
        access_key_id: ParsedTemplate,
        secret_access_key: ParsedTemplate,
        #[serde(default)]
        session_token: Option<ParsedTemplate>,
    },
    InstanceProfile,
}

impl S3AuthSpec {
    fn validate(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                validate_source_scoped_template(
                    schema,
                    table,
                    "source.object_store.auth.access_key_id",
                    access_key_id,
                )?;
                validate_source_scoped_template(
                    schema,
                    table,
                    "source.object_store.auth.secret_access_key",
                    secret_access_key,
                )?;
                if let Some(session_token) = session_token {
                    validate_source_scoped_template(
                        schema,
                        table,
                        "source.object_store.auth.session_token",
                        session_token,
                    )?;
                }
            }
            Self::InstanceProfile => {}
        }
        Ok(())
    }
}

fn validate_source_scoped_template(
    schema: &str,
    table: &str,
    field: &str,
    template: &ParsedTemplate,
) -> Result<()> {
    for token in template.tokens() {
        if token.namespace() != &TemplateNamespace::Input {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} {field} uses unsupported template token '{}'; backend=file source settings only support input tokens",
                token.raw()
            )));
        }
    }
    Ok(())
}

fn render_template_with_placeholders(template: &ParsedTemplate) -> String {
    let mut rendered = String::new();
    for part in template.parts() {
        match part {
            TemplatePart::Literal(value) => rendered.push_str(value),
            TemplatePart::Token(_) => rendered.push_str("placeholder"),
        }
    }
    rendered
}

/// One declared partition column derived from the file path layout.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: FilePartitionDataType,
    #[serde(default)]
    pub path: PartitionPathSpec,
}

/// Data types supported for file path partition values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePartitionDataType {
    Utf8,
    Int64,
    Boolean,
    Float64,
    Json,
}

impl FilePartitionDataType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Utf8 => "Utf8",
            Self::Int64 => "Int64",
            Self::Boolean => "Boolean",
            Self::Float64 => "Float64",
            Self::Json => "Json",
        }
    }

    fn from_manifest(data_type: ManifestDataType) -> Result<Self> {
        match data_type {
            ManifestDataType::Utf8 => Ok(Self::Utf8),
            ManifestDataType::Int64 => Ok(Self::Int64),
            ManifestDataType::Boolean => Ok(Self::Boolean),
            ManifestDataType::Float64 => Ok(Self::Float64),
            ManifestDataType::Json => Ok(Self::Json),
            ManifestDataType::Timestamp => Err(ManifestError::validation(
                "type=Timestamp is not supported for backend=file path partitions",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for FilePartitionDataType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let data_type = parse_manifest_data_type(&value).map_err(serde::de::Error::custom)?;
        Self::from_manifest(data_type).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for FilePartitionDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a partition column is extracted from the object path.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionPathSpec {
    /// Extract from Hive-style path segments such as `year=2026`.
    #[default]
    Hive,
    /// Extract from a zero-based path segment relative to `source.location`.
    Segment { index: usize },
}

impl PartitionPathSpec {
    #[must_use]
    pub fn is_hive(&self) -> bool {
        matches!(self, Self::Hive)
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Hive => "hive",
            Self::Segment { .. } => "segment",
        }
    }
}

/// Format-specific file reader options.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFormatOptions {
    pub has_header: Option<bool>,
    pub delimiter: Option<String>,
}

impl FileFormatOptions {
    fn validate_for_format(&self, format: FileFormat, schema: &str, table: &str) -> Result<()> {
        if format != FileFormat::Csv {
            if self.has_header.is_some() {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} format_options.has_header is only supported for format=csv"
                )));
            }
            if self.delimiter.is_some() {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} format_options.delimiter is only supported for format=csv"
                )));
            }
            return Ok(());
        }

        if let Some(delimiter) = &self.delimiter
            && delimiter.len() != 1
        {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} format_options.delimiter must be exactly one byte"
            )));
        }

        Ok(())
    }

    #[must_use]
    pub fn csv_has_header(&self) -> bool {
        self.has_header.unwrap_or(true)
    }

    #[must_use]
    pub fn csv_delimiter(&self) -> u8 {
        self.delimiter
            .as_deref()
            .and_then(|value| value.as_bytes().first().copied())
            .unwrap_or(b',')
    }
}

impl RawFileTableSpec {
    fn into_validated(self, schema: &str) -> Result<FileTableSpec> {
        let format = FileFormat::parse(&self.format, schema, &self.name)?;
        if format.requires_declared_columns() && self.columns.is_empty() {
            return Err(ManifestError::validation(format!(
                "{schema}.{} uses format={} and must define columns",
                self.name,
                format.as_str()
            )));
        }

        self.source.validate_for_file(schema, &self.name, format)?;
        validate_columns(&self.columns, schema, &self.name)?;
        validate_native_file_table_features(
            schema,
            &self.name,
            format,
            &self.filters,
            &self.columns,
        )?;
        validate_partition_column_overlap(schema, &self.name, &self.source, &self.columns)?;
        self.format_options
            .validate_for_format(format, schema, &self.name)?;

        Ok(FileTableSpec {
            common: TableCommon::new(
                self.name,
                self.description,
                self.guide,
                self.filters,
                self.fetch_limit_default,
                None,
                Vec::new(),
                self.columns,
            ),
            format,
            format_options: self.format_options,
            source: self.source,
        })
    }
}

fn validate_native_file_table_features(
    schema: &str,
    table: &str,
    _format: FileFormat,
    filters: &[FilterSpec],
    columns: &[ColumnSpec],
) -> Result<()> {
    if !filters.is_empty() {
        return Err(ManifestError::validation(format!(
            "{schema}.{table} uses backend=file and does not support declared filters; use SQL WHERE predicates instead"
        )));
    }

    for column in columns {
        if column.r#virtual {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} column '{}' is virtual, which is not supported for backend=file",
                column.name
            )));
        }
        if column.expr.is_some() {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} column '{}' uses expr, which is not supported for backend=file; use SQL expressions instead",
                column.name
            )));
        }
    }

    Ok(())
}

fn validate_partition_column_overlap(
    schema: &str,
    table: &str,
    source: &FileSourceSpec,
    columns: &[ColumnSpec],
) -> Result<()> {
    let partition_names = source
        .partitions
        .iter()
        .map(|partition| partition.name.as_str())
        .collect::<HashSet<_>>();

    for col in columns {
        if partition_names.contains(col.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} column '{}' duplicates a partition column",
                col.name
            )));
        }
    }

    Ok(())
}

impl FileSourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let declared_inputs = collect_source_inputs_value(&value)?;
        let raw: RawFileSourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawFileSourceManifest {
            dsl_version,
            name,
            version,
            description,
            test_queries,
            backend: _backend,
            inputs: _inputs,
            tables,
        } = raw;
        validate_test_queries(&name, &test_queries)?;
        validate_declared_relation_namespace(
            &name,
            tables
                .iter()
                .map(|table| DeclaredRelation::table(table.name.as_str())),
        )?;
        let common =
            SourceManifestCommon::new(dsl_version, name, version, description, test_queries);
        let tables = tables
            .into_iter()
            .map(|table| table.into_validated(&common.name))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            common,
            tables,
            declared_inputs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FileFormat, FileSourceManifest};
    use crate::ManifestInputKind;
    use serde_json::json;

    #[test]
    fn file_manifest_surfaces_declared_secret_inputs() {
        let manifest = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "warehouse",
            "version": "0.1.0",
            "backend": "file",
            "inputs": {
                "api_token": { "kind": "secret" },
                "signing_key": { "kind": "secret" },
                "optional_token": { "kind": "secret", "required": false },
                "region": { "kind": "variable", "default": "us-east-1" },
            },
            "tables": [{
                "name": "events",
                "description": "Warehouse events",
                "format": "parquet",
                "source": { "location": "file:///tmp/warehouse/" },
                "columns": [{ "name": "id", "type": "Int64" }],
            }],
        }))
        .expect("file manifest with inputs should parse");

        let required = manifest.required_secret_names();
        assert!(required.contains("api_token"));
        assert!(required.contains("signing_key"));
        assert!(!required.contains("optional_token"));
        assert_eq!(required.len(), 2);

        let kinds: Vec<(&str, ManifestInputKind)> = manifest
            .declared_inputs
            .iter()
            .map(|input| (input.key.as_str(), input.kind))
            .collect();
        assert!(kinds.contains(&("api_token", ManifestInputKind::Secret)));
        assert!(kinds.contains(&("region", ManifestInputKind::Variable)));
    }

    #[test]
    fn file_manifest_without_inputs_block_has_no_required_secrets() {
        let manifest = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "local",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Local events",
                "format": "parquet",
                "source": { "location": "file:///tmp/local/" },
                "columns": [],
            }],
        }))
        .expect("file manifest without inputs should parse");

        assert!(manifest.required_secret_names().is_empty());
        assert!(manifest.declared_inputs.is_empty());
    }

    #[test]
    fn file_manifest_allows_per_table_formats() {
        let manifest = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "local",
            "version": "0.1.0",
            "backend": "file",
            "tables": [
                {
                    "name": "events_jsonl",
                    "description": "JSONL events",
                    "format": "jsonl",
                    "source": { "location": "file:///tmp/local/" },
                    "columns": [{ "name": "id", "type": "Int64" }],
                },
                {
                    "name": "events_csv",
                    "description": "CSV events",
                    "format": "csv",
                    "source": { "location": "file:///tmp/local/" },
                    "columns": [{ "name": "id", "type": "Int64" }],
                },
            ],
        }))
        .expect("mixed file formats should parse");

        let formats = manifest
            .tables
            .iter()
            .map(|table| table.format)
            .collect::<Vec<_>>();
        assert_eq!(formats, vec![FileFormat::Jsonl, FileFormat::Csv]);
    }

    #[test]
    fn jsonl_file_manifest_requires_columns() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "source": { "location": "file:///tmp/logs/" },
                "columns": [],
            }],
        }))
        .expect_err("jsonl manifest without columns should fail");

        assert!(
            error
                .to_string()
                .contains("uses format=jsonl and must define columns")
        );
    }

    #[test]
    fn file_manifest_rejects_filters() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "filters": [{ "name": "kind" }],
                "source": { "location": "file:///tmp/logs/" },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect_err("file filters should fail");

        assert!(
            error
                .to_string()
                .contains("does not support declared filters")
        );
    }

    #[test]
    fn file_manifest_rejects_json_column_exprs() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "source": { "location": "file:///tmp/logs/" },
                "columns": [{
                    "name": "kind",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["payload", "kind"] }
                }],
            }],
        }))
        .expect_err("jsonl file expr should fail");

        assert!(error.to_string().contains("uses expr"));

        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSON messages",
                "format": "json",
                "source": { "location": "file:///tmp/logs/" },
                "columns": [{
                    "name": "kind",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["payload", "kind"] }
                }],
            }],
        }))
        .expect_err("json file expr should fail");

        assert!(error.to_string().contains("uses expr"));
    }

    #[test]
    fn native_file_manifest_rejects_column_exprs() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "CSV messages",
                "format": "csv",
                "source": { "location": "file:///tmp/logs/" },
                "columns": [{
                    "name": "kind",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["payload", "kind"] }
                }],
            }],
        }))
        .expect_err("file expr should fail");

        assert!(error.to_string().contains("uses expr"));
    }

    #[test]
    fn file_manifest_defaults_partitions_to_hive_path() {
        let manifest = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "source": {
                    "location": "file:///tmp/logs/",
                    "partitions": [{ "name": "year", "type": "Int64" }]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect("hive partition manifest should parse");

        let partition = manifest
            .tables
            .first()
            .and_then(|table| table.source.partitions.first())
            .expect("partition should exist");
        assert!(partition.path.is_hive());
    }

    #[test]
    fn json_file_manifest_accepts_segment_partitions() {
        FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "source": {
                    "location": "file:///tmp/logs/",
                    "partitions": [{
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    }]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect("jsonl segment partition manifest should parse");
    }

    #[test]
    fn listing_file_manifest_rejects_segment_partitions() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "warehouse",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Warehouse events",
                "format": "parquet",
                "source": {
                    "location": "file:///tmp/warehouse/",
                    "partitions": [{
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    }]
                },
                "columns": [],
            }],
        }))
        .expect_err("parquet segment partitions should fail");

        assert!(error.to_string().contains("DataFusion hive partitioning"));
    }

    #[test]
    fn s3_file_manifest_requires_object_store_config() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "warehouse",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Warehouse events",
                "format": "parquet",
                "source": { "location": "s3://example/warehouse/" },
                "columns": [],
            }],
        }))
        .expect_err("s3 file manifest without object_store should fail");

        assert!(
            error
                .to_string()
                .contains("must declare source.object_store")
        );
    }

    #[test]
    fn s3_file_manifest_accepts_typed_object_store_config() {
        FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "warehouse",
            "version": "0.1.0",
            "backend": "file",
            "inputs": {
                "AWS_REGION": { "kind": "variable", "default": "us-east-1" },
                "AWS_ACCESS_KEY_ID": { "kind": "secret" },
                "AWS_SECRET_ACCESS_KEY": { "kind": "secret" },
            },
            "tables": [{
                "name": "events",
                "description": "Warehouse events",
                "format": "jsonl",
                "source": {
                    "location": "s3://example/warehouse/",
                    "object_store": {
                        "type": "s3",
                        "region": "{{input.AWS_REGION}}",
                        "auth": {
                            "type": "access_key",
                            "access_key_id": "{{input.AWS_ACCESS_KEY_ID}}",
                            "secret_access_key": "{{input.AWS_SECRET_ACCESS_KEY}}"
                        }
                    }
                },
                "columns": [{ "name": "id", "type": "Int64" }],
            }],
        }))
        .expect("typed s3 object-store config should parse");
    }

    #[test]
    fn file_manifest_rejects_timestamp_partitions() {
        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "logs",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "messages",
                "description": "JSONL messages",
                "format": "jsonl",
                "source": {
                    "location": "file:///tmp/logs/",
                    "partitions": [{ "name": "created_at", "type": "Timestamp" }]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect_err("timestamp partitions should fail");

        assert!(error.to_string().contains("type=Timestamp"));
    }

    #[test]
    fn csv_options_validate_per_format() {
        FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "local",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Local events",
                "format": "csv",
                "source": { "location": "file:///tmp/local/" },
                "format_options": { "has_header": false, "delimiter": "|" },
                "columns": [{ "name": "id", "type": "Int64" }],
            }],
        }))
        .expect("csv options should parse");

        let error = FileSourceManifest::parse_manifest_value(json!({
            "dsl_version": 3,
            "name": "local",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Local events",
                "format": "jsonl",
                "source": { "location": "file:///tmp/local/" },
                "format_options": { "has_header": false },
                "columns": [{ "name": "id", "type": "Int64" }],
            }],
        }))
        .expect_err("non-csv option should fail");

        assert!(error.to_string().contains("only supported for format=csv"));
    }
}
