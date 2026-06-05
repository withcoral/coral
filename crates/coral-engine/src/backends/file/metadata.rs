//! File metadata columns for file-backed tables.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaBuilder, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion_datasource::table_schema::TableSchema;
use object_store::path::Path as ObjectPath;
use serde_json::Value;

use coral_spec::backends::file::{FileMetadataColumnSpec, FileMetadataKind};

#[derive(Debug, Clone)]
pub(super) struct FileMetadataColumns {
    file_columns: Vec<FileMetadataColumn>,
    row_columns: Vec<FileMetadataColumn>,
}

impl FileMetadataColumns {
    pub(super) fn try_new(specs: &[FileMetadataColumnSpec]) -> Result<Self> {
        let mut seen = HashSet::with_capacity(specs.len());
        let mut file_columns = Vec::new();
        let mut row_columns = Vec::new();

        for spec in specs {
            if !seen.insert(spec.name.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate metadata column '{}'",
                    spec.name
                )));
            }

            let column = FileMetadataColumn {
                name: spec.name.clone(),
                kind: spec.kind,
            };
            if is_file_scoped(column.kind) {
                file_columns.push(column);
            } else {
                row_columns.push(column);
            }
        }

        Ok(Self {
            file_columns,
            row_columns,
        })
    }

    pub(super) fn has_file_columns(&self) -> bool {
        !self.file_columns.is_empty()
    }

    pub(super) fn has_row_columns(&self) -> bool {
        !self.row_columns.is_empty()
    }

    pub(super) fn reject_schema_collisions(
        &self,
        file_schema: &SchemaRef,
        source_schema: &str,
        table_name: &str,
    ) -> Result<()> {
        for field in file_schema.fields() {
            if self.column_named(field.name()).is_some() {
                return Err(DataFusionError::Plan(format!(
                    "{source_schema}.{table_name} metadata column '{}' duplicates a file column",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn row_contains(&self, name: &str) -> bool {
        self.row_columns
            .iter()
            .any(|metadata| metadata.name == name)
    }

    /// Extends a table schema with configured metadata columns, when present.
    ///
    /// Row-scoped metadata is read by the JSON decoder as part of the file
    /// schema. File-scoped metadata is modeled as table partition fields so
    /// `DataFusion` can attach one constant value per file.
    pub(super) fn extend_table_schema_if_present(&self, table_schema: TableSchema) -> TableSchema {
        if !self.has_row_columns() && !self.has_file_columns() {
            return table_schema;
        }

        let file_schema = self.append_row_fields(Arc::clone(table_schema.file_schema()));
        let partition_fields = table_schema
            .table_partition_cols()
            .iter()
            .cloned()
            .chain(self.file_fields())
            .collect();
        TableSchema::new(file_schema, partition_fields)
    }

    pub(super) fn file_values(
        &self,
        table_path: &ListingTableUrl,
        location: &ObjectPath,
    ) -> Result<Vec<ScalarValue>> {
        if self.file_columns.is_empty() {
            return Ok(vec![]);
        }

        let relative_path = relative_file_path(table_path, location);
        self.file_columns
            .iter()
            .map(|column| column.file_value(&relative_path))
            .collect()
    }

    pub(super) fn insert_row_values(
        &self,
        location: &ObjectPath,
        line_number: usize,
        object: &mut serde_json::Map<String, Value>,
    ) -> Result<()> {
        for column in &self.row_columns {
            object.insert(
                column.name.clone(),
                column.row_value(location, line_number)?,
            );
        }
        Ok(())
    }

    fn file_fields(&self) -> Vec<FieldRef> {
        self.file_columns
            .iter()
            .map(FileMetadataColumn::arrow_field)
            .collect()
    }

    fn column_named(&self, name: &str) -> Option<&FileMetadataColumn> {
        self.file_columns
            .iter()
            .chain(self.row_columns.iter())
            .find(|column| column.name == name)
    }

    fn append_row_fields(&self, file_schema: SchemaRef) -> SchemaRef {
        if self.row_columns.is_empty() {
            return file_schema;
        }

        let mut builder = SchemaBuilder::from(file_schema.as_ref());
        builder.extend(self.row_columns.iter().map(FileMetadataColumn::arrow_field));
        Arc::new(builder.finish())
    }
}

#[derive(Debug, Clone)]
struct FileMetadataColumn {
    name: String,
    kind: FileMetadataKind,
}

impl FileMetadataColumn {
    fn arrow_field(&self) -> FieldRef {
        Arc::new(Field::new(
            &self.name,
            metadata_arrow_type(self.kind),
            false,
        ))
    }

    fn file_value(&self, relative_path: &str) -> Result<ScalarValue> {
        match self.kind {
            FileMetadataKind::RelativePath => {
                Ok(ScalarValue::Utf8(Some(relative_path.to_string())))
            }
            FileMetadataKind::FileName => Ok(ScalarValue::Utf8(Some(file_name(relative_path)))),
            FileMetadataKind::FileStem => Ok(ScalarValue::Utf8(Some(file_stem(relative_path)))),
            FileMetadataKind::LineNumber => Err(DataFusionError::Execution(
                "line_number metadata is row-scoped and cannot be computed per file".to_string(),
            )),
        }
    }

    fn row_value(&self, location: &ObjectPath, line_number: usize) -> Result<Value> {
        match self.kind {
            FileMetadataKind::LineNumber => Ok(Value::Number(serde_json::Number::from(
                i64::try_from(line_number).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "{location} line number {line_number} is not Int64: {error}"
                    ))
                })?,
            ))),
            FileMetadataKind::RelativePath
            | FileMetadataKind::FileName
            | FileMetadataKind::FileStem => Err(DataFusionError::Execution(format!(
                "{} metadata is file-scoped and cannot be inserted per row",
                self.kind.as_str()
            ))),
        }
    }
}

fn is_file_scoped(kind: FileMetadataKind) -> bool {
    matches!(
        kind,
        FileMetadataKind::RelativePath | FileMetadataKind::FileName | FileMetadataKind::FileStem
    )
}

fn metadata_arrow_type(kind: FileMetadataKind) -> DataType {
    match kind {
        FileMetadataKind::RelativePath
        | FileMetadataKind::FileName
        | FileMetadataKind::FileStem => DataType::Utf8,
        FileMetadataKind::LineNumber => DataType::Int64,
    }
}

fn relative_file_path(table_path: &ListingTableUrl, location: &ObjectPath) -> String {
    let Some(segments) = table_path.strip_prefix(location) else {
        return location.to_string();
    };

    let relative_path = segments.collect::<Vec<_>>().join("/");
    if relative_path.is_empty() {
        return file_name(location.as_ref());
    }
    relative_path
}

fn file_stem(relative_path: &str) -> String {
    let file_name = file_name(relative_path);
    file_name
        .rsplit_once('.')
        .map_or(file_name.as_str(), |(stem, _extension)| stem)
        .to_string()
}

fn file_name(relative_path: &str) -> String {
    relative_path
        .rsplit('/')
        .next()
        .unwrap_or(relative_path)
        .to_string()
}
