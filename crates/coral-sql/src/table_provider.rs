use std::any::Any;
use std::path::{Component, Path};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use coral_capabilities::{
    FileArtifactRef, FileFormatDescriptor, RestParameterLocation, UpstreamBinding,
};
use coral_exports::{SqlProjectionV1, SqlRowShape};
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::{
    BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionContext};

use crate::info::{quote_sql_identifier, sql_reference_parts};
use crate::projection::sql_identifier;
use crate::runtime::{SqlProviderInvocation, SqlProviderInvoker, SqlRuntimeBinding};
use crate::{SqlError, SqlResult};

pub(crate) async fn register_runtime_table(
    ctx: &SessionContext,
    binding: &SqlRuntimeBinding,
    provider_invoker: Option<Arc<dyn SqlProviderInvoker>>,
) -> SqlResult<()> {
    let (schema, table) = sql_reference_parts(&binding.binding.sql_reference).ok_or_else(|| {
        SqlError::FailedPrecondition(format!(
            "SQL binding '{}' is not a schema.table reference",
            binding.binding.sql_reference
        ))
    })?;
    let create_schema_sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_sql_identifier(&schema)
    );
    ctx.sql(&create_schema_sql).await?.collect().await?;
    let table_ref = TableReference::partial(schema, table);
    let UpstreamBinding::FileRead(file_binding) = &binding.capability.upstream_binding else {
        let Some(invoker) = provider_invoker else {
            return Err(SqlError::Unimplemented(format!(
                "SQL binding '{}' requires provider execution, but no provider invoker was configured",
                binding.binding.sql_reference
            )));
        };
        ctx.register_table(
            table_ref,
            Arc::new(ProviderBackedTable::new(binding.clone(), invoker)),
        )?;
        return Ok(());
    };
    let files = selected_file_artifact_paths(binding, file_binding)?;
    if files.is_empty() {
        return Err(SqlError::FailedPrecondition(format!(
            "SQL binding '{}' has no file artifacts",
            binding.binding.sql_reference
        )));
    }
    match &file_binding.format {
        FileFormatDescriptor::Json => {
            let dataframe = ctx
                .read_json(
                    files,
                    JsonReadOptions::default()
                        .file_extension("")
                        .newline_delimited(false),
                )
                .await?;
            ctx.register_table(table_ref, dataframe.into_view())?;
        }
        FileFormatDescriptor::Jsonl => {
            let dataframe = ctx
                .read_json(files, JsonReadOptions::default().file_extension(""))
                .await?;
            ctx.register_table(table_ref, dataframe.into_view())?;
        }
        FileFormatDescriptor::Csv => {
            let dataframe = ctx
                .read_csv(
                    files,
                    CsvReadOptions::new().has_header(true).file_extension(""),
                )
                .await?;
            ctx.register_table(table_ref, dataframe.into_view())?;
        }
        FileFormatDescriptor::Parquet => {
            let dataframe = ctx
                .read_parquet(files, ParquetReadOptions::default().file_extension(""))
                .await?;
            ctx.register_table(table_ref, dataframe.into_view())?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct ProviderBackedTable {
    binding: SqlRuntimeBinding,
    invoker: Arc<dyn SqlProviderInvoker>,
    schema: SchemaRef,
}

impl ProviderBackedTable {
    fn new(binding: SqlRuntimeBinding, invoker: Arc<dyn SqlProviderInvoker>) -> Self {
        let schema = arrow_schema_from_projection(&binding.binding.projection);
        Self {
            binding,
            invoker,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ProviderBackedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let args = provider_args_from_filters(&self.binding, filters, limit)
            .map_err(datafusion_external_error)?;
        let value = self
            .invoker
            .invoke_provider(SqlProviderInvocation {
                capability: &self.binding.capability,
                binding: &self.binding.binding,
                source_materialized_dir: &self.binding.source_materialized_dir,
                args: args.provider_args.clone(),
            })
            .await
            .map_err(datafusion_external_error)?;
        let batch = provider_response_batch(
            &self.binding,
            Arc::clone(&self.schema),
            &args.sql_args,
            &value,
        )
        .map_err(datafusion_external_error)?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        table.scan(state, projection, &[], limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_eq_input_literal(filter, &self.binding).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn datafusion_external_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(error))
}

fn arrow_schema_from_projection(projection: &SqlProjectionV1) -> SchemaRef {
    let mut fields = projection
        .columns
        .iter()
        .map(|column| {
            Field::new(
                column.name.clone(),
                arrow_data_type(&column.data_type),
                column.nullable,
            )
        })
        .collect::<Vec<_>>();
    fields.extend(
        projection
            .inputs
            .iter()
            .map(|input| Field::new(input.name.clone(), arrow_data_type(&input.data_type), true)),
    );
    Arc::new(Schema::new(fields))
}

fn arrow_data_type(data_type: &str) -> DataType {
    match data_type {
        "Int64" => DataType::Int64,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProviderFilterArgs {
    pub(crate) provider_args: serde_json::Map<String, serde_json::Value>,
    pub(crate) sql_args: serde_json::Map<String, serde_json::Value>,
}

pub(crate) fn provider_args_from_filters(
    binding: &SqlRuntimeBinding,
    filters: &[Expr],
    limit: Option<usize>,
) -> SqlResult<ProviderFilterArgs> {
    let mut sql_args = serde_json::Map::new();
    for filter in filters {
        if let Some((name, value)) = filter_eq_input_literal(filter, binding) {
            insert_unique_sql_arg(binding, &mut sql_args, name, value)?;
        }
    }
    if let Some(limit) = limit
        && let Some(input_name) = binding
            .binding
            .projection
            .pagination
            .as_ref()
            .and_then(|pagination| pagination.page_size_input.as_deref())
        && !sql_args.contains_key(input_name)
    {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        sql_args.insert(
            input_name.to_string(),
            serde_json::Value::Number(limit.into()),
        );
    }
    for input in binding
        .binding
        .projection
        .inputs
        .iter()
        .filter(|input| input.required)
    {
        if !sql_args.contains_key(&input.name) {
            return Err(SqlError::InvalidInput(format!(
                "{} requires `WHERE {} = <constant>`",
                binding.binding.sql_reference, input.name
            )));
        }
    }
    let provider_args = provider_args_from_sql_args(binding, &sql_args)?;
    Ok(ProviderFilterArgs {
        provider_args,
        sql_args,
    })
}

fn insert_unique_sql_arg(
    binding: &SqlRuntimeBinding,
    args: &mut serde_json::Map<String, serde_json::Value>,
    name: String,
    value: serde_json::Value,
) -> SqlResult<()> {
    if let Some(existing) = args.get(&name) {
        if existing != &value {
            return Err(SqlError::InvalidInput(format!(
                "{} received conflicting filters for `{}`",
                binding.binding.sql_reference, name
            )));
        }
        return Ok(());
    }
    args.insert(name, value);
    Ok(())
}

fn provider_args_from_sql_args(
    binding: &SqlRuntimeBinding,
    sql_args: &serde_json::Map<String, serde_json::Value>,
) -> SqlResult<serde_json::Map<String, serde_json::Value>> {
    let mut provider_args = serde_json::Map::new();
    for (sql_name, value) in sql_args {
        let provider_name = provider_input_name_for_sql_input(binding, sql_name);
        if let Some(existing) = provider_args.get(&provider_name) {
            if existing != value {
                return Err(SqlError::InvalidInput(format!(
                    "{} maps conflicting SQL inputs to provider input `{}`",
                    binding.binding.sql_reference, provider_name
                )));
            }
            continue;
        }
        provider_args.insert(provider_name, value.clone());
    }
    Ok(provider_args)
}

fn provider_input_name_for_sql_input(binding: &SqlRuntimeBinding, sql_name: &str) -> String {
    match &binding.capability.upstream_binding {
        UpstreamBinding::Rest(rest) => rest
            .parameter_bindings
            .iter()
            .filter(|parameter| {
                matches!(
                    parameter.location,
                    RestParameterLocation::Path | RestParameterLocation::Query
                )
            })
            .find(|parameter| sql_identifier(&parameter.name) == sql_name)
            .map(|parameter| parameter.name.clone()),
        UpstreamBinding::Graphql(graphql) => graphql
            .variable_bindings
            .iter()
            .filter_map(|binding| binding.argument_path.first())
            .find(|argument| sql_identifier(argument) == sql_name)
            .cloned(),
        _ => None,
    }
    .unwrap_or_else(|| sql_name.to_string())
}

fn filter_eq_input_literal(
    filter: &Expr,
    binding: &SqlRuntimeBinding,
) -> Option<(String, serde_json::Value)> {
    match filter {
        Expr::Not(inner) | Expr::IsFalse(inner) => {
            return input_boolean_literal(inner, binding, false);
        }
        Expr::IsTrue(inner) => {
            return input_boolean_literal(inner, binding, true);
        }
        Expr::Column(_) => {
            return input_boolean_literal(filter, binding, true);
        }
        _ => {}
    }
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
        return None;
    };
    match op {
        Operator::Eq => input_literal_pair(left, right, binding)
            .or_else(|| input_literal_pair(right, left, binding)),
        Operator::NotEq => input_boolean_literal_pair(left, right, binding)
            .or_else(|| input_boolean_literal_pair(right, left, binding))
            .map(|(name, value)| (name, serde_json::Value::Bool(!value))),
        _ => None,
    }
}

fn input_boolean_literal(
    column_expr: &Expr,
    binding: &SqlRuntimeBinding,
    value: bool,
) -> Option<(String, serde_json::Value)> {
    let Expr::Column(Column { name, .. }) = column_expr else {
        return None;
    };
    let input = binding
        .binding
        .projection
        .inputs
        .iter()
        .find(|input| input.name == *name && input.data_type == "Boolean")?;
    Some((input.name.clone(), serde_json::Value::Bool(value)))
}

fn input_boolean_literal_pair(
    column_expr: &Expr,
    value_expr: &Expr,
    binding: &SqlRuntimeBinding,
) -> Option<(String, bool)> {
    let Expr::Column(Column { name, .. }) = column_expr else {
        return None;
    };
    let input = binding
        .binding
        .projection
        .inputs
        .iter()
        .find(|input| input.name == *name && input.data_type == "Boolean")?;
    let Expr::Literal(ScalarValue::Boolean(Some(value)), _) = value_expr else {
        return None;
    };
    Some((input.name.clone(), *value))
}

fn input_literal_pair(
    column_expr: &Expr,
    value_expr: &Expr,
    binding: &SqlRuntimeBinding,
) -> Option<(String, serde_json::Value)> {
    let Expr::Column(Column { name, .. }) = column_expr else {
        return None;
    };
    let input = binding
        .binding
        .projection
        .inputs
        .iter()
        .find(|input| input.name == *name)?;
    let value = match value_expr {
        Expr::Literal(value, _) => scalar_value_to_json(value)?,
        _ => return None,
    };
    Some((input.name.clone(), value))
}

fn scalar_value_to_json(value: &ScalarValue) -> Option<serde_json::Value> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(serde_json::Value::String(value.clone())),
        ScalarValue::Int8(Some(value)) => Some(serde_json::Value::Number(i64::from(*value).into())),
        ScalarValue::Int16(Some(value)) => {
            Some(serde_json::Value::Number(i64::from(*value).into()))
        }
        ScalarValue::Int32(Some(value)) => {
            Some(serde_json::Value::Number(i64::from(*value).into()))
        }
        ScalarValue::Int64(Some(value)) => Some(serde_json::Value::Number((*value).into())),
        ScalarValue::UInt8(Some(value)) => {
            Some(serde_json::Value::Number(u64::from(*value).into()))
        }
        ScalarValue::UInt16(Some(value)) => {
            Some(serde_json::Value::Number(u64::from(*value).into()))
        }
        ScalarValue::UInt32(Some(value)) => {
            Some(serde_json::Value::Number(u64::from(*value).into()))
        }
        ScalarValue::UInt64(Some(value)) => Some(serde_json::Value::Number((*value).into())),
        ScalarValue::Float32(Some(value)) => {
            serde_json::Number::from_f64(f64::from(*value)).map(serde_json::Value::Number)
        }
        ScalarValue::Float64(Some(value)) => {
            serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
        }
        ScalarValue::Boolean(Some(value)) => Some(serde_json::Value::Bool(*value)),
        _ => None,
    }
}

fn provider_response_batch(
    binding: &SqlRuntimeBinding,
    schema: SchemaRef,
    args: &serde_json::Map<String, serde_json::Value>,
    value: &serde_json::Value,
) -> SqlResult<RecordBatch> {
    let rows = response_rows(binding, value);
    let full_rows = rows
        .iter()
        .map(|row| provider_row_values(binding, args, value, row))
        .collect::<Vec<_>>();
    record_batch_from_rows(schema, &full_rows)
}

fn response_rows<'a>(
    binding: &SqlRuntimeBinding,
    value: &'a serde_json::Value,
) -> Vec<&'a serde_json::Value> {
    let selected: &[String] = binding
        .binding
        .projection
        .response_selection
        .as_ref()
        .map_or(&[], |selection| selection.path.as_slice());
    let mut current = value;
    for segment in selected {
        let Some(next) = current.get(segment) else {
            return Vec::new();
        };
        current = next;
    }
    match (binding.binding.projection.row_shape, current) {
        (_, serde_json::Value::Array(values)) => values.iter().collect(),
        (SqlRowShape::Collection, serde_json::Value::Object(object)) => object
            .get("items")
            .and_then(serde_json::Value::as_array)
            .map_or_else(|| vec![current], |values| values.iter().collect()),
        _ => vec![current],
    }
}

fn provider_row_values(
    binding: &SqlRuntimeBinding,
    args: &serde_json::Map<String, serde_json::Value>,
    root: &serde_json::Value,
    row: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let mut values = Vec::new();
    for column in &binding.binding.projection.columns {
        values.push(project_row_column(row, root, &column.name));
    }
    for input in &binding.binding.projection.inputs {
        values.push(
            args.get(&input.name)
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    values
}

fn project_row_column(
    row: &serde_json::Value,
    root: &serde_json::Value,
    sql_name: &str,
) -> serde_json::Value {
    if sql_name == "json" {
        return serde_json::Value::String(row.to_string());
    }
    project_object_column(row, sql_name)
        .or_else(|| project_object_column(root, sql_name))
        .unwrap_or(serde_json::Value::Null)
}

fn project_object_column(value: &serde_json::Value, sql_name: &str) -> Option<serde_json::Value> {
    value
        .as_object()?
        .iter()
        .find(|(key, _)| sql_identifier(key) == sql_name)
        .map(|(_, value)| value.clone())
}

fn record_batch_from_rows(
    schema: SchemaRef,
    rows: &[Vec<serde_json::Value>],
) -> SqlResult<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| json_column_array(field.data_type(), rows, index))
        .collect::<SqlResult<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(SqlError::from)
}

fn json_column_array(
    data_type: &DataType,
    rows: &[Vec<serde_json::Value>],
    index: usize,
) -> SqlResult<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in rows {
                match row.get(index).and_then(json_i64) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for row in rows {
                match row.get(index).and_then(json_f64) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match row.get(index).and_then(serde_json::Value::as_bool) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match row.get(index).and_then(json_string_value) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(SqlError::Internal(format!(
            "unsupported provider-backed SQL data type {other}"
        ))),
    }
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn json_string_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

fn selected_file_artifact_paths(
    binding: &SqlRuntimeBinding,
    file_binding: &coral_capabilities::FileScanBinding,
) -> SqlResult<Vec<String>> {
    let selected_ids = binding.binding.projection.file_scan.as_ref().map(|scan| {
        scan.file_refs
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    });
    file_binding
        .file_refs
        .iter()
        .filter(|file| {
            selected_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(&file.id))
        })
        .map(|file| trusted_file_artifact_path(&binding.source_materialized_dir, file))
        .collect()
}

fn trusted_file_artifact_path(
    source_materialized_dir: &Path,
    file: &FileArtifactRef,
) -> SqlResult<String> {
    let relative = Path::new(&file.source_local_path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(SqlError::FailedPrecondition(format!(
            "file artifact ref '{}' must be source-local",
            file.id
        )));
    }
    let path = source_materialized_dir.join(relative);
    let base = source_materialized_dir.canonicalize().map_err(|error| {
        SqlError::NotFound(format!(
            "installed source materialization is missing: {error}"
        ))
    })?;
    let canonical = path.canonicalize().map_err(|error| {
        SqlError::NotFound(format!("file artifact '{}' is missing: {error}", file.id))
    })?;
    if !canonical.starts_with(&base) {
        return Err(SqlError::FailedPrecondition(format!(
            "file artifact '{}' escaped the installed source directory",
            file.id
        )));
    }
    Ok(canonical.to_string_lossy().to_string())
}
