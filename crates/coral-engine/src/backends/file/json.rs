//! JSON and JSONL file tables backed by `DataFusion` file scan primitives.

use std::any::Any;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::ReaderBuilder;
use datafusion::common::Statistics;
use datafusion::common::runtime::SpawnedTask;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::json::JsonDecoder;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileOpenFuture, FileOpener, FileScanConfig, FileScanConfigBuilder, FileSource,
    JsonSource as DataFusionJsonSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::SessionContext;
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource_json::utils::{ChannelReader, JsonArrayToNdjsonReader};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use object_store::path::Path as ObjectPath;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;

use crate::backends::schema_from_columns;
use crate::backends::shared::mapping::json_value_to_text;
use coral_spec::backends::file::{FileFormat, FileTableSpec};
use coral_spec::{ColumnSpec, ManifestDataType};

use super::listing::{PreparedListingTable, prepare_listing_table};
use super::partitions::{
    PartitionColumns, filter_is_supported_partition_filter, filter_references_partition,
    partition_filter_constraints, partition_values_for_path,
};

const JSON_ARRAY_CHANNEL_BUFFER_SIZE: usize = 128;
const JSON_ARRAY_CONVERTER_BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonReadMode {
    NewlineDelimited,
    Array,
}

impl JsonReadMode {
    fn from_file_format(format: FileFormat) -> Self {
        match format {
            FileFormat::Jsonl => Self::NewlineDelimited,
            FileFormat::Json => Self::Array,
            FileFormat::Parquet | FileFormat::Csv => {
                unreachable!("CoralJsonSource is only constructed for JSON file formats")
            }
        }
    }

    fn is_newline_delimited(self) -> bool {
        matches!(self, Self::NewlineDelimited)
    }

    fn file_type(self) -> &'static str {
        match self {
            Self::NewlineDelimited => "jsonl",
            Self::Array => "json",
        }
    }
}

pub(super) struct JsonFileTableProvider {
    source_schema: String,
    table_name: String,
    table_path: ListingTableUrl,
    object_store: Arc<dyn ObjectStore>,
    file_extension: String,
    format: FileFormat,
    schema: SchemaRef,
    table_schema: TableSchema,
    json_fields: Arc<HashSet<String>>,
    partition_columns: PartitionColumns,
}

impl fmt::Debug for JsonFileTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonFileTableProvider")
            .field("source_schema", &self.source_schema)
            .field("table_name", &self.table_name)
            .field("format", &self.format.as_str())
            .field("table_path", &self.table_path.as_str())
            .finish_non_exhaustive()
    }
}

impl JsonFileTableProvider {
    pub(super) async fn try_new_async(
        ctx: &SessionContext,
        source_schema: &str,
        table: FileTableSpec,
        home_dir: Option<&Path>,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let format = table.format;
        let PreparedListingTable {
            table_path,
            object_store,
            listing_options,
            partition_columns,
        } = prepare_listing_table(ctx, source_schema, &table, home_dir, resolved_inputs).await?;

        let file_schema = schema_from_columns(table.columns(), source_schema, table.name())?;
        let partition_fields = partition_columns.arrow_fields();
        let table_schema = TableSchema::new(file_schema, partition_fields);
        let schema = Arc::clone(table_schema.table_schema());
        let json_fields = Arc::new(json_field_names(table.columns())?);

        Ok(Self {
            source_schema: source_schema.to_string(),
            table_name: table.name().to_string(),
            table_path,
            object_store,
            file_extension: listing_options.file_extension,
            format,
            schema,
            table_schema,
            json_fields,
            partition_columns,
        })
    }
}

pub(super) fn requires_custom_provider(table: &FileTableSpec) -> Result<bool> {
    Ok(!table.source.partitions.is_empty() || !json_field_names(table.columns())?.is_empty())
}

#[async_trait]
impl TableProvider for JsonFileTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_is_supported_partition_filter(filter, &self.partition_columns) {
                    TableProviderFilterPushDown::Exact
                } else if filter_references_partition(filter, &self.partition_columns) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
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
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!("failed to list {}: {error}", self.table_path))
            })?
            .try_collect()
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!("failed to list {}: {error}", self.table_path))
            })?;

        let JsonFileGroups {
            groups,
            grouped_by_partition,
        } = json_file_groups(
            &self.table_path,
            &self.partition_columns,
            files,
            filters,
            state.config().target_partitions(),
            state.config_options().optimizer.preserve_file_partitions,
        )?;

        let file_source = json_file_source(
            self.table_schema.clone(),
            self.format,
            Arc::clone(&self.json_fields),
            projection,
        );

        let config = FileScanConfigBuilder::new(self.table_path.object_store(), file_source)
            .with_file_groups(groups)
            .with_statistics(Statistics::new_unknown(&self.schema))
            .with_limit(limit)
            .with_partitioned_by_file_group(grouped_by_partition)
            .with_projection_indices(projection.cloned())?
            .build();

        Ok(DataSourceExec::from_data_source(config))
    }
}

fn json_file_source(
    table_schema: TableSchema,
    format: FileFormat,
    json_fields: Arc<HashSet<String>>,
    projection: Option<&Vec<usize>>,
) -> Arc<dyn FileSource> {
    let read_mode = JsonReadMode::from_file_format(format);
    if projection_requires_json_normalization(&table_schema, &json_fields, projection) {
        Arc::new(CoralJsonSource::new(table_schema, read_mode, json_fields))
    } else {
        Arc::new(
            DataFusionJsonSource::new(table_schema)
                .with_newline_delimited(read_mode.is_newline_delimited()),
        )
    }
}

fn projection_requires_json_normalization(
    table_schema: &TableSchema,
    json_fields: &HashSet<String>,
    projection: Option<&Vec<usize>>,
) -> bool {
    if json_fields.is_empty() {
        return false;
    }

    let Some(projection) = projection else {
        return true;
    };

    projection.iter().any(|index| {
        table_schema
            .table_schema()
            .fields()
            .get(*index)
            .is_some_and(|field| json_fields.contains(field.name()))
    })
}

#[derive(Clone)]
struct CoralJsonSource {
    table_schema: TableSchema,
    read_mode: JsonReadMode,
    json_fields: Arc<HashSet<String>>,
    projection: SplitProjection,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl CoralJsonSource {
    fn new(
        table_schema: TableSchema,
        read_mode: JsonReadMode,
        json_fields: Arc<HashSet<String>>,
    ) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            table_schema,
            read_mode,
            json_fields,
            projection,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for CoralJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let projected_file_schema = Arc::new(
            self.table_schema
                .file_schema()
                .project(&self.projection.file_indices)?,
        );

        let opener = Arc::new(CoralJsonOpener {
            batch_size: self
                .batch_size
                .expect("batch size is set before creating the file opener"),
            projected_file_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
            read_mode: self.read_mode,
            json_fields: Arc::clone(&self.json_fields),
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        self.read_mode.file_type()
    }

    fn fmt_extra(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, ", json_fields={}", self.json_fields.len())
    }

    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let merged = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &merged);
        Ok(Some(Arc::new(source)))
    }
}

struct CoralJsonOpener {
    batch_size: usize,
    projected_file_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    read_mode: JsonReadMode,
    json_fields: Arc<HashSet<String>>,
}

impl FileOpener for CoralJsonOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        if partitioned_file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "Coral JSON source does not support range-based file scanning".to_string(),
            ));
        }

        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_file_schema);
        let batch_size = self.batch_size;
        let compression = self.file_compression_type;
        let read_mode = self.read_mode;
        let json_fields = Arc::clone(&self.json_fields);
        let location = partitioned_file.object_meta.location;

        Ok(Box::pin(async move {
            let result = store.get(&location).await?;
            match (read_mode, result.payload) {
                (JsonReadMode::NewlineDelimited, GetResultPayload::File(file, _)) => {
                    let reader = compression.convert_read(file)?;
                    let reader = CompatJsonlReader::new(reader, location, json_fields);
                    let arrow_reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(reader)?;
                    Ok(stream::iter(arrow_reader)
                        .map(|result| result.map_err(Into::into))
                        .boxed())
                }
                (JsonReadMode::Array, GetResultPayload::File(file, _)) => {
                    let reader = compression.convert_read(file)?;
                    let reader = JsonArrayToNdjsonReader::with_capacity(
                        reader,
                        JSON_ARRAY_CONVERTER_BUFFER_SIZE,
                    );
                    let reader = CompatJsonlReader::new(reader, location, json_fields);
                    let arrow_reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(reader)?;
                    Ok(stream::iter(arrow_reader)
                        .map(|result| result.map_err(Into::into))
                        .boxed())
                }
                (JsonReadMode::NewlineDelimited, GetResultPayload::Stream(stream)) => {
                    let input = stream.map_err(DataFusionError::from).boxed();
                    let input = compression.convert_stream(input)?;
                    let input = normalize_jsonl_stream(input, location, json_fields);
                    let decoder = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build_decoder()?;
                    let stream = deserialize_stream(
                        input,
                        DecoderDeserializer::new(JsonDecoder::new(decoder)),
                    );
                    Ok(stream.map_err(Into::into).boxed())
                }
                (JsonReadMode::Array, GetResultPayload::Stream(stream)) => {
                    let input = stream.map_err(DataFusionError::from).boxed();
                    let input = compression.convert_stream(input)?;
                    Ok(normalize_json_array_stream(
                        input,
                        location,
                        json_fields,
                        schema,
                        batch_size,
                    ))
                }
            }
        }))
    }
}

struct CoralJsonArrayStream {
    inner: ReceiverStream<std::result::Result<RecordBatch, ArrowError>>,
    _read_task: SpawnedTask<()>,
    _parse_task: SpawnedTask<()>,
}

impl Stream for CoralJsonArrayStream {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

struct CompatJsonlReader<R> {
    reader: BufReader<R>,
    location: ObjectPath,
    json_fields: Arc<HashSet<String>>,
    line: Vec<u8>,
    pending: Vec<u8>,
    pending_offset: usize,
    line_number: usize,
}

impl<R: Read> CompatJsonlReader<R> {
    fn new(reader: R, location: ObjectPath, json_fields: Arc<HashSet<String>>) -> Self {
        Self {
            reader: BufReader::new(reader),
            location,
            json_fields,
            line: Vec::new(),
            pending: Vec::new(),
            pending_offset: 0,
            line_number: 0,
        }
    }

    fn read_next_normalized_line(&mut self) -> io::Result<bool> {
        loop {
            self.line.clear();
            let bytes_read = self.reader.read_until(b'\n', &mut self.line)?;
            if bytes_read == 0 {
                return Ok(false);
            }

            self.line_number += 1;
            let Some(line) = normalize_jsonl_line(
                &self.location,
                self.line_number,
                &self.line,
                &self.json_fields,
            )
            .map_err(|error| datafusion_error_to_io(&error))?
            else {
                continue;
            };

            self.pending = line.to_vec();
            self.pending_offset = 0;
            return Ok(true);
        }
    }

    fn into_inner(self) -> R {
        self.reader.into_inner()
    }
}

impl<R: Read> Read for CompatJsonlReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

        while self.pending_offset >= self.pending.len() {
            if !self.read_next_normalized_line()? {
                return Ok(0);
            }
        }

        let Some(available) = self.pending.get(self.pending_offset..) else {
            return Ok(0);
        };
        let count = available.len().min(output.len());
        let Some(output) = output.get_mut(..count) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output buffer shorter than requested read",
            ));
        };
        let Some(input) = available.get(..count) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pending JSONL buffer shorter than requested read",
            ));
        };
        output.copy_from_slice(input);
        self.pending_offset += count;
        Ok(count)
    }
}

impl<R: Read> BufRead for CompatJsonlReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.pending_offset >= self.pending.len() {
            if !self.read_next_normalized_line()? {
                return Ok(&[]);
            }
        }

        self.pending.get(self.pending_offset..).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid JSONL buffer offset")
        })
    }

    fn consume(&mut self, amount: usize) {
        self.pending_offset = self
            .pending_offset
            .saturating_add(amount)
            .min(self.pending.len());
    }
}

struct JsonlStreamState {
    input: BoxStream<'static, Result<Bytes>>,
    location: ObjectPath,
    json_fields: Arc<HashSet<String>>,
    buffer: BytesMut,
    pending: VecDeque<Bytes>,
    line_number: usize,
    input_done: bool,
}

impl JsonlStreamState {
    fn drain_complete_lines(&mut self) -> Result<()> {
        while let Some(line) = take_jsonl_line(&mut self.buffer) {
            self.line_number += 1;
            if let Some(line) =
                normalize_jsonl_line(&self.location, self.line_number, &line, &self.json_fields)?
            {
                self.pending.push_back(line);
            }
        }
        Ok(())
    }
}

fn normalize_jsonl_stream(
    input: BoxStream<'static, Result<Bytes>>,
    location: ObjectPath,
    json_fields: Arc<HashSet<String>>,
) -> BoxStream<'static, Result<Bytes>> {
    let state = JsonlStreamState {
        input,
        location,
        json_fields,
        buffer: BytesMut::new(),
        pending: VecDeque::new(),
        line_number: 0,
        input_done: false,
    };

    stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(line) = state.pending.pop_front() {
                return Ok(Some((line, state)));
            }
            if state.input_done {
                return Ok(None);
            }

            if let Some(chunk) = state.input.try_next().await? {
                state.buffer.extend_from_slice(&chunk);
                state.drain_complete_lines()?;
            } else {
                state.input_done = true;
                if !state.buffer.is_empty() {
                    state.line_number += 1;
                    if let Some(line) = normalize_jsonl_line(
                        &state.location,
                        state.line_number,
                        &state.buffer,
                        &state.json_fields,
                    )? {
                        state.pending.push_back(line);
                    }
                    state.buffer.clear();
                }
            }
        }
    })
    .boxed()
}

fn normalize_json_array_stream(
    mut input: BoxStream<'static, Result<Bytes>>,
    location: ObjectPath,
    json_fields: Arc<HashSet<String>>,
    schema: SchemaRef,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    let (byte_tx, byte_rx) = tokio::sync::mpsc::channel::<Bytes>(JSON_ARRAY_CHANNEL_BUFFER_SIZE);
    let (result_tx, result_rx) = tokio::sync::mpsc::channel(2);
    let error_tx = result_tx.clone();

    let read_task = SpawnedTask::spawn(async move {
        while let Some(chunk) = input.next().await {
            match chunk {
                Ok(bytes) => {
                    if byte_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    if error_tx
                        .send(Err(datafusion_error_to_arrow(error)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    break;
                }
            }
        }
    });

    let parse_task = SpawnedTask::spawn_blocking(move || {
        let channel_reader = ChannelReader::new(byte_rx);
        let ndjson_reader = JsonArrayToNdjsonReader::with_capacity(
            channel_reader,
            JSON_ARRAY_CONVERTER_BUFFER_SIZE,
        );
        let mut reader = CompatJsonlReader::new(ndjson_reader, location, json_fields);

        match ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build(&mut reader)
        {
            Ok(arrow_reader) => {
                for batch in arrow_reader {
                    if result_tx.blocking_send(batch).is_err() {
                        return;
                    }
                }
            }
            Err(error) => {
                if result_tx.blocking_send(Err(error)).is_err() {
                    return;
                }
                return;
            }
        }

        if let Err(error) = reader.into_inner().validate_complete() {
            drop(result_tx.blocking_send(Err(ArrowError::JsonError(error.to_string()))));
        }
    });

    CoralJsonArrayStream {
        inner: ReceiverStream::new(result_rx),
        _read_task: read_task,
        _parse_task: parse_task,
    }
    .map(|result| result.map_err(Into::into))
    .boxed()
}

fn normalize_jsonl_line(
    location: &ObjectPath,
    line_number: usize,
    line: &[u8],
    json_fields: &HashSet<String>,
) -> Result<Option<Bytes>> {
    let line = jsonl_line_text(location, line_number, line)?;
    if line.is_empty() {
        return Ok(None);
    }

    let value = serde_json::from_str::<Value>(line).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to parse {location} line {line_number} as JSON: {error}"
        ))
    })?;
    let value = normalize_json_record(location, Some(line_number), value, json_fields)?;
    Ok(Some(Bytes::from(json_value_to_line_bytes(&value)?)))
}

fn normalize_json_record(
    location: &ObjectPath,
    line_number: Option<usize>,
    row: Value,
    json_fields: &HashSet<String>,
) -> Result<Value> {
    let Value::Object(mut object) = row else {
        let row_description = line_number
            .map(|line| format!(" line {line}"))
            .unwrap_or_default();
        return Err(DataFusionError::Execution(format!(
            "{location}{row_description} is not a JSON object"
        )));
    };

    for field in json_fields {
        if let Some(value) = object.get_mut(field)
            && !value.is_null()
        {
            let Some(text) = json_value_to_text(value) else {
                continue;
            };
            *value = Value::String(text);
        }
    }

    Ok(Value::Object(object))
}

fn json_value_to_line_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&value).map_err(|error| {
        DataFusionError::Execution(format!("failed to serialize JSON row: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn take_jsonl_line(buffer: &mut BytesMut) -> Option<BytesMut> {
    let newline = buffer.iter().position(|byte| *byte == b'\n')?;
    Some(buffer.split_to(newline + 1))
}

fn jsonl_line_text<'a>(
    location: &ObjectPath,
    line_number: usize,
    line: &'a [u8],
) -> Result<&'a str> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    std::str::from_utf8(line).map(str::trim).map_err(|error| {
        DataFusionError::Execution(format!(
            "{location} line {line_number} is not valid UTF-8: {error}"
        ))
    })
}

fn datafusion_error_to_io(error: &DataFusionError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn datafusion_error_to_arrow(error: DataFusionError) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}

pub(super) struct JsonFileGroups {
    pub(super) groups: Vec<FileGroup>,
    pub(super) grouped_by_partition: bool,
}

pub(super) fn json_file_groups(
    table_path: &ListingTableUrl,
    partition_columns: &PartitionColumns,
    files: Vec<ObjectMeta>,
    filters: &[Expr],
    target_partitions: usize,
    preserve_file_partitions: usize,
) -> Result<JsonFileGroups> {
    let constraints = partition_filter_constraints(filters, partition_columns);
    let mut partitioned_files = Vec::new();

    for meta in files {
        let partition_values =
            partition_values_for_path(table_path, &meta.location, partition_columns)?;
        if !constraints.matches(&partition_values) {
            continue;
        }

        partitioned_files.push(
            PartitionedFile::new_from_meta(meta)
                .with_partition_values(partition_values.into_scalars()),
        );
    }

    if partitioned_files.is_empty() {
        return Ok(JsonFileGroups {
            groups: vec![FileGroup::default()],
            grouped_by_partition: false,
        });
    }

    partitioned_files.sort_by(|left, right| {
        left.object_meta
            .location
            .as_ref()
            .cmp(right.object_meta.location.as_ref())
    });

    let file_group = FileGroup::new(partitioned_files);
    let target_partitions = target_partitions.max(1);
    if partition_columns.is_empty() || preserve_file_partitions == 0 {
        return Ok(JsonFileGroups {
            groups: file_group.split_files(target_partitions),
            grouped_by_partition: false,
        });
    }

    let grouped = file_group.group_by_partition_values(target_partitions);
    if grouped.len() >= preserve_file_partitions {
        Ok(JsonFileGroups {
            groups: grouped,
            grouped_by_partition: true,
        })
    } else {
        let files = grouped
            .into_iter()
            .flat_map(FileGroup::into_inner)
            .collect::<Vec<_>>();
        Ok(JsonFileGroups {
            groups: FileGroup::new(files).split_files(target_partitions),
            grouped_by_partition: false,
        })
    }
}

fn json_field_names(columns: &[ColumnSpec]) -> Result<HashSet<String>> {
    columns
        .iter()
        .filter_map(|column| match column.manifest_data_type() {
            Ok(ManifestDataType::Json) => Some(Ok(column.name.clone())),
            Ok(_) => None,
            Err(error) => Some(Err(DataFusionError::Execution(error.to_string()))),
        })
        .collect()
}
