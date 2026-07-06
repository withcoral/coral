//! `Parquet` schema inference helpers for mixed `Arrow` encodings.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt as _;
use object_store::ObjectStoreExt as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{ChunkReader, Length};

// Parquet footer layout (end of file):
//   [thrift-encoded FileMetaData: metadata_len bytes]
//   [4-byte LE int32: metadata_len]
//   [4-byte magic: "PAR1"]
const PARQUET_FOOTER_SIZE: u64 = 8;

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
pub(super) async fn infer_schema_expand_dicts(
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
        return Ok(Arc::new(schema_without_metadata(&expanded)));
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
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "parquet schema read failed for {}: {e}",
                    obj_meta.location
                ))
            })?
            .schema()
            .clone();

        let expanded = expand_dictionary_types(&arrow_schema);
        // Strip schema-level metadata before merging: OTel Arrow embeds
        // per-file values (e.g. _part_id UUIDs) in schema metadata, which
        // causes Schema::try_merge to fail with "conflicting metadata".
        let expanded_no_meta = schema_without_metadata(&expanded);

        merged = Some(match merged {
            None => expanded_no_meta,
            Some(s) => Schema::try_merge(vec![s, expanded_no_meta])?,
        });
    }

    Ok(Arc::new(merged.expect("metadata items were processed")))
}

fn schema_without_metadata(schema: &SchemaRef) -> Schema {
    Schema::new_with_metadata(schema.fields().clone(), HashMap::default())
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
