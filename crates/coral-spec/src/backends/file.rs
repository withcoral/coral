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
    pub metadata: Vec<FileMetadataColumnSpec>,
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
        let mut seen_metadata = HashSet::new();
        for metadata in &self.metadata {
            if !seen_metadata.insert(metadata.name.clone()) {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} has duplicate metadata column '{}'",
                    metadata.name
                )));
            }
            if metadata.kind == FileMetadataKind::LineNumber && format != FileFormat::Jsonl {
                return Err(ManifestError::validation(format!(
                    "{schema}.{table} metadata column '{}' uses kind=line_number, which is only supported for backend=file format jsonl",
                    metadata.name
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

/// One declared metadata column derived from the scanned file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMetadataColumnSpec {
    pub name: String,
    pub kind: FileMetadataKind,
}

/// Metadata values the file backend can add to each scanned row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileMetadataKind {
    /// Path to the scanned file, relative to `source.location`.
    RelativePath,
    /// File name of the scanned file, including its final extension.
    FileName,
    /// File stem of the scanned file, without its final extension.
    FileStem,
    /// One-based line number within the scanned JSONL file.
    LineNumber,
}

impl FileMetadataKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelativePath => "relative_path",
            Self::FileName => "file_name",
            Self::FileStem => "file_stem",
            Self::LineNumber => "line_number",
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
        validate_native_file_table_features(schema, &self.name, &self.filters, &self.columns)?;
        validate_derived_column_overlap(schema, &self.name, &self.source, &self.columns)?;
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

fn validate_derived_column_overlap(
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

    let metadata_names = source
        .metadata
        .iter()
        .map(|metadata| metadata.name.as_str())
        .collect::<HashSet<_>>();

    for col in columns {
        if metadata_names.contains(col.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} column '{}' duplicates a metadata column",
                col.name
            )));
        }
    }

    for metadata in &source.metadata {
        if partition_names.contains(metadata.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} metadata column '{}' duplicates a partition column",
                metadata.name
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
    use crate::test_support::{assert_error_contains, insert_field};
    use serde::Serialize;
    use serde_json::{Value, json};

    fn manifest_with_tables(name: &str, tables: impl Serialize) -> Value {
        json!({
            "dsl_version": 3,
            "name": name,
            "version": "0.1.0",
            "backend": "file",
            "tables": tables,
        })
    }

    fn parse_manifest(name: &str, tables: Vec<Value>) -> crate::Result<FileSourceManifest> {
        FileSourceManifest::parse_manifest_value(manifest_with_tables(name, tables))
    }

    fn file_table(name: &str, format: &str, columns: Value) -> Value {
        file_table_with_source(
            name,
            format,
            json!({ "location": "file:///tmp/local/" }),
            columns,
        )
    }

    fn file_table_with_source(
        name: &str,
        format: &str,
        source: impl Serialize,
        columns: impl Serialize,
    ) -> Value {
        json!({
            "name": name,
            "description": "Test table",
            "format": format,
            "source": source,
            "columns": columns,
        })
    }

    fn id_column() -> Value {
        json!({ "name": "id", "type": "Int64" })
    }

    fn kind_column() -> Value {
        json!({ "name": "kind", "type": "Utf8" })
    }

    #[test]
    fn file_manifest_surfaces_declared_secret_inputs() {
        let mut raw = manifest_with_tables(
            "warehouse",
            vec![file_table_with_source(
                "events",
                "parquet",
                json!({ "location": "file:///tmp/warehouse/" }),
                json!([id_column()]),
            )],
        );
        insert_field(
            &mut raw,
            "inputs",
            json!({
                "api_token": { "kind": "secret" },
                "signing_key": { "kind": "secret" },
                "optional_token": { "kind": "secret", "required": false },
                "region": { "kind": "variable", "default": "us-east-1" },
            }),
        );
        let manifest = FileSourceManifest::parse_manifest_value(raw)
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
        let manifest = parse_manifest("local", vec![file_table("events", "parquet", json!([]))])
            .expect("file manifest without inputs should parse");

        assert!(manifest.required_secret_names().is_empty());
        assert!(manifest.declared_inputs.is_empty());
    }

    #[test]
    fn file_manifest_allows_per_table_formats() {
        let manifest = parse_manifest(
            "local",
            vec![
                file_table("events_jsonl", "jsonl", json!([id_column()])),
                file_table("events_csv", "csv", json!([id_column()])),
            ],
        )
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
        let error = parse_manifest("logs", vec![file_table("messages", "jsonl", json!([]))])
            .expect_err("jsonl manifest without columns should fail");

        assert_error_contains(&error, "uses format=jsonl and must define columns");
    }

    #[test]
    fn file_manifest_rejects_filters() {
        let mut table = file_table("messages", "jsonl", json!([kind_column()]));
        insert_field(&mut table, "filters", json!([{ "name": "kind" }]));
        let error = parse_manifest("logs", vec![table]).expect_err("file filters should fail");

        assert_error_contains(&error, "does not support declared filters");
    }

    #[test]
    fn file_manifest_rejects_json_column_exprs() {
        let expr_column = json!({
            "name": "kind",
            "type": "Utf8",
            "expr": { "kind": "path", "path": ["payload", "kind"] }
        });
        for format in ["jsonl", "json"] {
            let error = parse_manifest(
                "logs",
                vec![file_table("messages", format, json!([expr_column.clone()]))],
            )
            .expect_err("json file expr should fail");
            assert_error_contains(&error, "uses expr");
        }
    }

    #[test]
    fn native_file_manifest_rejects_column_exprs() {
        let error = parse_manifest(
            "logs",
            vec![file_table(
                "messages",
                "csv",
                json!([{
                    "name": "kind",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["payload", "kind"] }
                }]),
            )],
        )
        .expect_err("file expr should fail");

        assert_error_contains(&error, "uses expr");
    }

    #[test]
    fn file_manifest_defaults_partitions_to_hive_path() {
        let manifest = parse_manifest(
            "logs",
            vec![file_table_with_source(
                "messages",
                "jsonl",
                json!({
                    "location": "file:///tmp/logs/",
                    "partitions": [{ "name": "year", "type": "Int64" }]
                }),
                json!([kind_column()]),
            )],
        )
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
        parse_manifest(
            "logs",
            vec![file_table_with_source(
                "messages",
                "jsonl",
                json!({
                    "location": "file:///tmp/logs/",
                    "partitions": [{
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    }]
                }),
                json!([kind_column()]),
            )],
        )
        .expect("jsonl segment partition manifest should parse");
    }

    #[test]
    fn jsonl_file_manifest_accepts_metadata_columns() {
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
                    "metadata": [
                        { "name": "session_path", "kind": "relative_path" },
                        { "name": "session_name", "kind": "file_name" },
                        { "name": "session_file", "kind": "file_stem" },
                        { "name": "event_index", "kind": "line_number" }
                    ]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect("jsonl metadata manifest should parse");

        let metadata = &manifest
            .tables
            .first()
            .expect("manifest should include the JSONL table")
            .source
            .metadata;
        let parsed_metadata = metadata
            .iter()
            .map(|metadata| (metadata.name.as_str(), metadata.kind.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            parsed_metadata,
            vec![
                ("session_path", "relative_path"),
                ("session_name", "file_name"),
                ("session_file", "file_stem"),
                ("event_index", "line_number"),
            ]
        );
    }

    #[test]
    fn file_manifest_rejects_duplicate_metadata_column_names() {
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
                    "metadata": [
                        { "name": "session_file", "kind": "file_name" },
                        { "name": "session_file", "kind": "file_stem" }
                    ]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect_err("duplicate metadata column names should fail");

        assert!(
            error
                .to_string()
                .contains("duplicate metadata column 'session_file'"),
            "{error}"
        );
    }

    #[test]
    fn file_manifest_rejects_metadata_payload_column_overlap() {
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
                    "metadata": [{ "name": "kind", "kind": "relative_path" }]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect_err("metadata and payload columns should not overlap");

        assert!(error.to_string().contains("duplicates a metadata column"));
    }

    #[test]
    fn file_manifest_rejects_metadata_partition_column_overlap() {
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
                    "partitions": [{
                        "name": "event_index",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    }],
                    "metadata": [{ "name": "event_index", "kind": "line_number" }]
                },
                "columns": [{ "name": "kind", "type": "Utf8" }],
            }],
        }))
        .expect_err("metadata and partition columns should not overlap");

        assert!(
            error
                .to_string()
                .contains("metadata column 'event_index' duplicates a partition column"),
            "{error}"
        );
    }

    #[test]
    fn file_manifest_accepts_file_scoped_metadata_for_non_jsonl_tables() {
        FileSourceManifest::parse_manifest_value(json!({
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
                    "metadata": [{ "name": "path", "kind": "relative_path" }]
                },
                "columns": [],
            }],
        }))
        .expect("parquet file-scoped metadata columns should parse");
    }

    #[test]
    fn file_manifest_rejects_line_number_metadata_for_non_jsonl_tables() {
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
                    "metadata": [{ "name": "event_index", "kind": "line_number" }]
                },
                "columns": [],
            }],
        }))
        .expect_err("parquet line_number metadata should fail");

        assert!(error.to_string().contains("kind=line_number"));
        assert!(error.to_string().contains("format jsonl"));
    }

    #[test]
    fn listing_file_manifest_rejects_segment_partitions() {
        let error = parse_manifest(
            "warehouse",
            vec![file_table_with_source(
                "events",
                "parquet",
                json!({
                    "location": "file:///tmp/warehouse/",
                    "partitions": [{
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    }]
                }),
                json!([]),
            )],
        )
        .expect_err("parquet segment partitions should fail");

        assert_error_contains(&error, "DataFusion hive partitioning");
    }

    #[test]
    fn s3_file_manifest_requires_object_store_config() {
        let error = parse_manifest(
            "warehouse",
            vec![file_table_with_source(
                "events",
                "parquet",
                json!({ "location": "s3://example/warehouse/" }),
                json!([]),
            )],
        )
        .expect_err("s3 file manifest without object_store should fail");

        assert_error_contains(&error, "must declare source.object_store");
    }

    #[test]
    fn s3_file_manifest_accepts_typed_object_store_config() {
        let mut raw = manifest_with_tables(
            "warehouse",
            vec![file_table_with_source(
                "events",
                "jsonl",
                json!({
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
                }),
                json!([id_column()]),
            )],
        );
        insert_field(
            &mut raw,
            "inputs",
            json!({
                "AWS_REGION": { "kind": "variable", "default": "us-east-1" },
                "AWS_ACCESS_KEY_ID": { "kind": "secret" },
                "AWS_SECRET_ACCESS_KEY": { "kind": "secret" },
            }),
        );
        FileSourceManifest::parse_manifest_value(raw)
            .expect("typed s3 object-store config should parse");
    }

    #[test]
    fn file_manifest_rejects_timestamp_partitions() {
        let error = parse_manifest(
            "logs",
            vec![file_table_with_source(
                "messages",
                "jsonl",
                json!({
                    "location": "file:///tmp/logs/",
                    "partitions": [{ "name": "created_at", "type": "Timestamp" }]
                }),
                json!([kind_column()]),
            )],
        )
        .expect_err("timestamp partitions should fail");

        assert_error_contains(&error, "type=Timestamp");
    }

    #[test]
    fn csv_options_validate_per_format() {
        let mut table = file_table("events", "csv", json!([id_column()]));
        insert_field(
            &mut table,
            "format_options",
            json!({ "has_header": false, "delimiter": "|" }),
        );
        parse_manifest("local", vec![table]).expect("csv options should parse");

        let mut table = file_table("events", "jsonl", json!([id_column()]));
        insert_field(&mut table, "format_options", json!({ "has_header": false }));
        let error = parse_manifest("local", vec![table]).expect_err("non-csv option should fail");

        assert_error_contains(&error, "only supported for format=csv");
    }
}
