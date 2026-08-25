//! UDF SQL signature inference, runtime argument binding, and catalog helpers.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use coral_spec::ManifestDataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;

use crate::runtime::catalog::{
    CatalogTableFunction, CatalogTableFunctionArgument, CatalogTableFunctionResultColumn,
};
use crate::runtime::literal_scalar_value;
use crate::runtime::query::{QueryRuntimeAdapter, query_parameter_scalar_value};
use crate::runtime::scoped_table_functions::{ScopedTableFunctionName, qualified_name};
use crate::types::parameter_binding_is_string_shaped;
use crate::{
    CoralSqlFunctionArgument, CoralSqlFunctionDefinition, CoralSqlFunctionInferenceDefinition,
    CoralSqlFunctionSignature, CoralSqlResultColumn, CoreError, QueryParameterValue,
    QueryParameters,
};

pub(crate) async fn infer_udf_signature(
    query_runtime: &QueryRuntimeAdapter,
    udf: &CoralSqlFunctionInferenceDefinition,
) -> Result<CoralSqlFunctionSignature, CoreError> {
    let signature = query_runtime.infer_sql_signature(&udf.query).await?;
    let mut arguments = Vec::new();
    for (placeholder, field) in signature.parameter_fields {
        let name = placeholder
            .strip_prefix('$')
            .unwrap_or(&placeholder)
            .to_string();
        let Some(field) = field else {
            return Err(CoreError::InvalidInput(format!(
                "udf '{}' SQL parameter '{}' has no inferred type; cast it in SQL, for example CAST({} AS VARCHAR)",
                udf.name, placeholder, placeholder
            )));
        };
        let data_type = match signature.declared_parameter_types.get(&placeholder) {
            Some(declared) => *declared,
            None => {
                crate::types::manifest_data_type_for_arrow(field.data_type()).ok_or_else(|| {
                    CoreError::InvalidInput(format!(
                        "udf '{}' SQL parameter '{}' inferred unsupported type {}",
                        udf.name,
                        placeholder,
                        field.data_type()
                    ))
                })?
            }
        };
        arguments.push(CoralSqlFunctionArgument { name, data_type });
    }
    arguments.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(CoralSqlFunctionSignature {
        arguments,
        result_columns: result_columns(signature.planned_schema.as_ref()),
        source_names: signature.source_names,
    })
}

fn result_columns(schema: &Schema) -> Vec<CoralSqlResultColumn> {
    schema
        .fields()
        .iter()
        .map(|field| CoralSqlResultColumn {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect()
}

pub(crate) fn udf_query_parameters(
    udf: &CoralSqlFunctionDefinition,
    arguments: &QueryParameters,
) -> DataFusionResult<QueryParameters> {
    UdfArgumentBinding::new(udf, arguments).into_query_params()
}

pub(crate) fn published_table_functions(
    udfs: &[CoralSqlFunctionDefinition],
    source_function_names: &HashSet<ScopedTableFunctionName>,
) -> DataFusionResult<Vec<CatalogTableFunction>> {
    PublishedTableFunctions::new(source_function_names).build(udfs)
}

struct PublishedTableFunctions<'a> {
    source_function_names: &'a HashSet<ScopedTableFunctionName>,
    seen_udfs: HashSet<ScopedTableFunctionName>,
    rows: Vec<CatalogTableFunction>,
}

impl<'a> PublishedTableFunctions<'a> {
    fn new(source_function_names: &'a HashSet<ScopedTableFunctionName>) -> Self {
        Self {
            source_function_names,
            seen_udfs: HashSet::new(),
            rows: Vec::new(),
        }
    }

    fn build(
        mut self,
        udfs: &[CoralSqlFunctionDefinition],
    ) -> DataFusionResult<Vec<CatalogTableFunction>> {
        for udf in udfs {
            self.push_udf(udf)?;
        }
        self.rows.sort_by(|left, right| {
            (&left.schema_name, &left.function_name)
                .cmp(&(&right.schema_name, &right.function_name))
        });
        Ok(self.rows)
    }

    fn push_udf(&mut self, udf: &CoralSqlFunctionDefinition) -> DataFusionResult<()> {
        let publish = &udf.publish;
        let key = ScopedTableFunctionName::from_parts(&publish.schema, &publish.name);
        self.reject_duplicate_udf(&key)?;
        self.reject_source_collision(&key)?;
        udf_arrow_schema(udf)?;
        self.rows.push(catalog_table_function(udf, &key));
        Ok(())
    }

    fn reject_duplicate_udf(&mut self, key: &ScopedTableFunctionName) -> DataFusionResult<()> {
        if self.seen_udfs.insert(key.clone()) {
            return Ok(());
        }
        let display_name = qualified_name(&key.schema, &key.function);
        Err(DataFusionError::Plan(format!(
            "duplicate udf table function {display_name}"
        )))
    }

    fn reject_source_collision(&self, key: &ScopedTableFunctionName) -> DataFusionResult<()> {
        if !self.source_function_names.contains(key) {
            return Ok(());
        }
        let display_name = qualified_name(&key.schema, &key.function);
        Err(DataFusionError::Plan(format!(
            "udf table function {display_name} conflicts with existing table function"
        )))
    }
}

fn catalog_table_function(
    udf: &CoralSqlFunctionDefinition,
    key: &ScopedTableFunctionName,
) -> CatalogTableFunction {
    let publish = &udf.publish;
    CatalogTableFunction {
        catalog_name: None,
        schema_name: key.schema.clone(),
        function_name: key.function.clone(),
        kind: coral_spec::SourceTableFunctionKind::Table,
        description: publish_description(&publish.description, &udf.description),
        guide: publish.guide.clone(),
        require_guide_read: false,
        arguments: udf
            .arguments
            .iter()
            .map(|argument| CatalogTableFunctionArgument {
                name: argument.name.clone(),
                data_type: argument.data_type.to_string(),
                required: true,
                values: Vec::new(),
            })
            .collect(),
        result_columns: udf
            .result_columns
            .iter()
            .map(|column| CatalogTableFunctionResultColumn {
                name: column.name.clone(),
                data_type: column.data_type.to_string(),
                nullable: column.nullable,
                description: String::new(),
            })
            .collect(),
        search_limits: None,
    }
}

pub(crate) fn udf_argument_values(
    udf: &CoralSqlFunctionDefinition,
    args: &[Expr],
) -> DataFusionResult<QueryParameters> {
    if args.len() > udf.arguments.len() {
        return Err(DataFusionError::Plan(format!(
            "udf '{}' expected at most {} arguments, got {}",
            udf.name,
            udf.arguments.len(),
            args.len()
        )));
    }

    let mut params = QueryParameters::new();
    for (index, argument) in udf.arguments.iter().enumerate() {
        if let Some(expr) = args.get(index) {
            params.insert(
                argument.name.clone(),
                udf_argument_value(udf, argument, expr)?,
            );
        }
    }
    udf_query_parameters(udf, &params)
}

fn udf_argument_value(
    udf: &CoralSqlFunctionDefinition,
    argument: &CoralSqlFunctionArgument,
    expr: &Expr,
) -> DataFusionResult<QueryParameterValue> {
    let Some(value) = literal_scalar_value(expr)? else {
        return Err(DataFusionError::Plan(format!(
            "udf '{}' argument '{}' must be a literal after parameter binding",
            udf.name, argument.name
        )));
    };
    UdfParameterTypeBinding::new(argument.data_type)
        .literal_value(&value)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "udf '{}' argument '{}' expected {}, got {}",
                udf.name,
                argument.name,
                argument.data_type.as_manifest_str(),
                scalar_literal_kind(&value)
            ))
        })
}

pub(crate) fn udf_param_values(params: &QueryParameters) -> Vec<(String, ScalarValue)> {
    params
        .iter()
        .map(|(name, value)| (name.clone(), query_parameter_scalar_value(value)))
        .collect()
}

pub(crate) fn udf_arrow_schema(udf: &CoralSqlFunctionDefinition) -> DataFusionResult<Arc<Schema>> {
    if udf.result_columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "published udf '{}' requires declared result columns",
            udf.name
        )));
    }

    let fields = udf
        .result_columns
        .iter()
        .map(udf_result_field)
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn udf_result_field(column: &CoralSqlResultColumn) -> Field {
    Field::new(&column.name, column.data_type.clone(), column.nullable)
}

struct UdfArgumentBinding<'a> {
    udf: &'a CoralSqlFunctionDefinition,
    arguments: &'a QueryParameters,
}

impl<'a> UdfArgumentBinding<'a> {
    fn new(udf: &'a CoralSqlFunctionDefinition, arguments: &'a QueryParameters) -> Self {
        Self { udf, arguments }
    }

    fn into_query_params(self) -> DataFusionResult<QueryParameters> {
        self.reject_unknown_arguments()?;

        let mut params = self.arguments.clone();
        for argument in &self.udf.arguments {
            self.bind_argument(argument, &mut params)?;
        }
        Ok(params)
    }

    fn reject_unknown_arguments(&self) -> DataFusionResult<()> {
        if let Some(argument_name) = self.arguments.keys().find(|argument_name| {
            self.udf
                .arguments
                .iter()
                .all(|argument| argument.name != **argument_name)
        }) {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' received unknown argument '{}'",
                self.udf.name, argument_name
            )));
        }
        Ok(())
    }

    fn bind_argument(
        &self,
        argument: &CoralSqlFunctionArgument,
        params: &mut QueryParameters,
    ) -> DataFusionResult<()> {
        let binding = UdfParameterTypeBinding::new(argument.data_type);
        let Some(value) = params.get(&argument.name) else {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' is missing argument '{}'",
                self.udf.name, argument.name
            )));
        };
        let value_kind = query_parameter_value_kind(value);
        let Some(value) = binding.coerce_value(value) else {
            return Err(DataFusionError::Plan(format!(
                "udf '{}' argument '{}' expected {}, got {}",
                self.udf.name,
                argument.name,
                argument.data_type.as_manifest_str(),
                value_kind
            )));
        };
        params.insert(argument.name.clone(), value);
        Ok(())
    }
}

struct UdfParameterTypeBinding {
    data_type: ManifestDataType,
}

impl UdfParameterTypeBinding {
    fn new(data_type: ManifestDataType) -> Self {
        Self { data_type }
    }

    fn literal_value(&self, value: &ScalarValue) -> Option<QueryParameterValue> {
        if value.is_null() {
            return Some(self.typed_null());
        }

        if self.is_string_shaped()
            && let Some(value) = value.try_as_str().flatten()
        {
            return Some(QueryParameterValue::string(value));
        }

        match self.data_type {
            ManifestDataType::Int64 => {
                signed_integer_literal(value).map(QueryParameterValue::integer)
            }
            ManifestDataType::Float64 => float_literal(value)
                .map(QueryParameterValue::float)
                .or_else(|| {
                    signed_integer_literal(value)
                        .and_then(|value| float_parameter_from_integer(Some(value)))
                }),
            ManifestDataType::Boolean => match value {
                ScalarValue::Boolean(Some(value)) => Some(QueryParameterValue::boolean(*value)),
                _ => None,
            },
            ManifestDataType::Timestamp => timestamp_parameter_from_scalar(value),
            ManifestDataType::Utf8 | ManifestDataType::Json => None,
        }
    }

    fn coerce_value(&self, value: &QueryParameterValue) -> Option<QueryParameterValue> {
        match (self.data_type, value) {
            (data_type, QueryParameterValue::String(_))
                if parameter_binding_is_string_shaped(data_type) =>
            {
                Some(value.clone())
            }
            (ManifestDataType::Int64, QueryParameterValue::Integer(_))
            | (ManifestDataType::Float64, QueryParameterValue::Float(_))
            | (ManifestDataType::Boolean, QueryParameterValue::Boolean(_))
            | (ManifestDataType::Timestamp, QueryParameterValue::Timestamp(_)) => {
                Some(value.clone())
            }
            (ManifestDataType::Float64, QueryParameterValue::Integer(value)) => {
                float_parameter_from_integer(*value)
            }
            _ => None,
        }
    }

    fn typed_null(&self) -> QueryParameterValue {
        if self.is_string_shaped() {
            return QueryParameterValue::null_string();
        }

        match self.data_type {
            ManifestDataType::Int64 => QueryParameterValue::null_integer(),
            ManifestDataType::Float64 => QueryParameterValue::null_float(),
            ManifestDataType::Boolean => QueryParameterValue::null_boolean(),
            ManifestDataType::Timestamp => QueryParameterValue::null_timestamp(),
            _ => unreachable!("string-binding manifest types returned above"),
        }
    }

    fn is_string_shaped(&self) -> bool {
        parameter_binding_is_string_shaped(self.data_type)
    }
}

fn signed_integer_literal(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        _ => None,
    }
}

fn float_literal(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => Some(*value),
        _ => None,
    }
}

fn float_parameter_from_integer(value: Option<i64>) -> Option<QueryParameterValue> {
    let Some(value) = value else {
        return Some(QueryParameterValue::null_float());
    };
    let ScalarValue::Float64(Some(value)) = ScalarValue::Int64(Some(value))
        .cast_to(&DataType::Float64)
        .ok()?
    else {
        return None;
    };
    Some(QueryParameterValue::float(value))
}

fn timestamp_parameter_from_scalar(value: &ScalarValue) -> Option<QueryParameterValue> {
    let data_type = crate::types::arrow_data_type(ManifestDataType::Timestamp);
    let ScalarValue::TimestampMicrosecond(Some(value), _) = value.cast_to(&data_type).ok()? else {
        return None;
    };
    Some(QueryParameterValue::timestamp_micros(value))
}

fn query_parameter_value_kind(value: &QueryParameterValue) -> &'static str {
    match value {
        QueryParameterValue::String(_) => "string",
        QueryParameterValue::Integer(_) => "integer",
        QueryParameterValue::Float(_) => "float",
        QueryParameterValue::Boolean(_) => "boolean",
        QueryParameterValue::Timestamp(_) => "timestamp",
    }
}

fn scalar_literal_kind(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_) => "string",
        ScalarValue::Int64(_) => "integer",
        ScalarValue::Float64(_) => "float",
        ScalarValue::Boolean(_) => "boolean",
        ScalarValue::TimestampSecond(_, _)
        | ScalarValue::TimestampMillisecond(_, _)
        | ScalarValue::TimestampMicrosecond(_, _)
        | ScalarValue::TimestampNanosecond(_, _) => "timestamp",
        _ => "unsupported literal",
    }
}

fn publish_description(target_description: &str, udf_description: &str) -> String {
    if target_description.trim().is_empty() {
        udf_description.to_string()
    } else {
        target_description.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use datafusion::logical_expr::expr::{Cast, TryCast};

    fn argument(name: &str, data_type: ManifestDataType) -> CoralSqlFunctionArgument {
        CoralSqlFunctionArgument {
            name: name.to_string(),
            data_type,
        }
    }

    fn udf() -> CoralSqlFunctionDefinition {
        CoralSqlFunctionDefinition {
            name: "open_pull_requests".to_string(),
            description: String::new(),
            arguments: vec![argument("author", ManifestDataType::Utf8)],
            query: "select * from github.pull_requests where author = $author".to_string(),
            publish: crate::CoralSqlTableFunctionPublish {
                schema: "udfs".to_string(),
                name: "open_pull_requests".to_string(),
                description: String::new(),
                guide: String::new(),
            },
            result_columns: Vec::new(),
            source_names: vec!["github".to_string()],
        }
    }

    fn params(
        values: impl IntoIterator<Item = (&'static str, QueryParameterValue)>,
    ) -> QueryParameters {
        values
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect()
    }

    #[test]
    fn udf_query_parameters_accepts_matching_arguments() {
        let udf = udf();
        let arguments = params([("author", QueryParameterValue::string("Bradley-Butcher"))]);

        assert_eq!(
            udf_query_parameters(&udf, &arguments).unwrap(),
            params([("author", QueryParameterValue::string("Bradley-Butcher"))])
        );
    }

    #[test]
    fn udf_query_parameters_coerces_integer_values_for_float_arguments() {
        let mut udf = udf();
        udf.arguments = vec![argument("min_score", ManifestDataType::Float64)];

        assert_eq!(
            udf_query_parameters(
                &udf,
                &params([("min_score", QueryParameterValue::integer(1))])
            )
            .unwrap(),
            params([("min_score", QueryParameterValue::float(1.0))])
        );
        assert_eq!(
            udf_query_parameters(
                &udf,
                &params([("min_score", QueryParameterValue::null_integer())])
            )
            .unwrap(),
            params([("min_score", QueryParameterValue::null_float())])
        );
    }

    #[test]
    fn udf_query_parameters_rejects_invalid_arguments() {
        let udf = udf();
        let cases = [
            (
                params([
                    ("author", QueryParameterValue::string("Bradley-Butcher")),
                    ("repository", QueryParameterValue::string("withcoral/coral")),
                ]),
                "Error during planning: udf 'open_pull_requests' received unknown argument 'repository'",
            ),
            (
                QueryParameters::new(),
                "Error during planning: udf 'open_pull_requests' is missing argument 'author'",
            ),
            (
                params([("author", QueryParameterValue::integer(42))]),
                "Error during planning: udf 'open_pull_requests' argument 'author' expected Utf8, got integer",
            ),
        ];

        for (arguments, expected) in cases {
            assert_eq!(
                udf_query_parameters(&udf, &arguments)
                    .unwrap_err()
                    .strip_backtrace(),
                expected
            );
        }
    }

    #[test]
    fn udf_argument_values_binds_supported_literals() {
        let udf = udf();
        let exprs = [
            Expr::Literal(ScalarValue::Utf8(Some("Bradley-Butcher".into())), None),
            Expr::Literal(ScalarValue::Utf8View(Some("Bradley-Butcher".into())), None),
            Expr::Literal(ScalarValue::Utf8(Some("Bradley-Butcher".into())), None).alias("author"),
        ];

        for expr in exprs {
            assert_eq!(
                udf_argument_values(&udf, &[expr]).unwrap(),
                params([("author", QueryParameterValue::string("Bradley-Butcher"))])
            );
        }
    }

    #[test]
    fn udf_argument_values_coerces_integer_literals_for_float_arguments() {
        let mut udf = udf();
        udf.arguments = vec![argument("min_score", ManifestDataType::Float64)];

        assert_eq!(
            udf_argument_values(&udf, &[Expr::Literal(ScalarValue::Int64(Some(1)), None)]).unwrap(),
            params([("min_score", QueryParameterValue::float(1.0))])
        );
    }

    #[test]
    fn udf_argument_values_widens_compatible_numeric_casts() {
        let integer = Expr::Cast(Cast::new(
            Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
            DataType::Int32,
        ));
        let mut udf = udf();
        udf.arguments = vec![argument("value", ManifestDataType::Int64)];
        assert_eq!(
            udf_argument_values(&udf, &[integer]).unwrap(),
            params([("value", QueryParameterValue::integer(1))])
        );

        let float = Expr::Cast(Cast::new(
            Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
            DataType::Float32,
        ));
        udf.arguments = vec![argument("value", ManifestDataType::Float64)];
        assert_eq!(
            udf_argument_values(&udf, &[float]).unwrap(),
            params([("value", QueryParameterValue::float(1.0))])
        );
    }

    #[test]
    fn udf_argument_values_accepts_negative_numeric_literals() {
        let mut udf = udf();
        udf.arguments = vec![argument("value", ManifestDataType::Int64)];
        let integer = Expr::Negative(Box::new(Expr::Literal(ScalarValue::Int64(Some(3)), None)));
        assert_eq!(
            udf_argument_values(&udf, &[integer]).unwrap(),
            params([("value", QueryParameterValue::integer(-3))])
        );

        udf.arguments = vec![argument("value", ManifestDataType::Float64)];
        let float = Expr::Negative(Box::new(Expr::Literal(
            ScalarValue::Float64(Some(1.5)),
            None,
        )));
        assert_eq!(
            udf_argument_values(&udf, &[float]).unwrap(),
            params([("value", QueryParameterValue::float(-1.5))])
        );
    }

    #[test]
    fn udf_argument_values_evaluates_try_cast_literals() {
        let mut udf = udf();
        udf.arguments = vec![argument("min_score", ManifestDataType::Float64)];

        let try_cast = |value: &str| {
            Expr::TryCast(TryCast::new(
                Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some(value.to_string())),
                    None,
                )),
                DataType::Float64,
            ))
        };

        assert_eq!(
            udf_argument_values(&udf, &[try_cast("1.5")]).unwrap(),
            params([("min_score", QueryParameterValue::float(1.5))])
        );
        assert_eq!(
            udf_argument_values(&udf, &[try_cast("not-a-number")]).unwrap(),
            params([("min_score", QueryParameterValue::null_float())])
        );
    }

    #[test]
    fn udf_argument_values_preserves_native_timestamp_literals() {
        let mut udf = udf();
        udf.arguments = vec![argument("since", ManifestDataType::Timestamp)];
        let timestamp =
            ScalarValue::TimestampMicrosecond(Some(1_704_067_200_000_000), Some("+00:00".into()));

        assert_eq!(
            udf_argument_values(&udf, &[Expr::Literal(timestamp, None)]).unwrap(),
            params([(
                "since",
                QueryParameterValue::timestamp_micros(1_704_067_200_000_000)
            )])
        );
    }

    #[test]
    fn udf_argument_values_evaluates_timestamp_literal_casts() {
        let mut udf = udf();
        udf.arguments = vec![argument("since", ManifestDataType::Timestamp)];
        let timestamp = Expr::Cast(Cast::new(
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("2024-01-01T00:00:00Z".to_string())),
                None,
            )),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        ));

        assert_eq!(
            udf_argument_values(&udf, &[timestamp]).unwrap(),
            params([(
                "since",
                QueryParameterValue::timestamp_micros(1_704_067_200_000_000)
            )])
        );
    }
}
