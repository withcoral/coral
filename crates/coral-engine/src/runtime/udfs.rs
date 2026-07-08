//! UDF SQL signature inference and runtime argument binding helpers.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use coral_spec::ManifestDataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;

use crate::runtime::parameter_inference;
use crate::runtime::query::{QueryRuntimeAdapter, query_parameter_scalar_value};
use crate::types::parameter_binding_is_string_shaped;
use crate::{
    CoreError, QueryParameterValue, QueryParameters, UdfRuntimeArgument, UdfRuntimeDefinition,
    UdfRuntimeImplementation, UdfRuntimeResultColumn, UdfRuntimeSignature, UdfRuntimeSqlDefinition,
};

#[expect(
    dead_code,
    reason = "UDF table-function execution consumes registered UDF SQL in the next stack branch."
)]
pub(crate) fn udf_sql(udf: &UdfRuntimeDefinition) -> &str {
    let UdfRuntimeImplementation::CoralSql { query } = &udf.implementation;
    query
}

pub(crate) async fn infer_udf_signature(
    query_runtime: &QueryRuntimeAdapter,
    udf: &UdfRuntimeSqlDefinition,
) -> Result<UdfRuntimeSignature, CoreError> {
    let planned = query_runtime.plan_sql(udf.sql()).await?;
    let mut arguments = parameter_inference::infer_parameters(&planned.plan)
        .map_err(|error| CoreError::InvalidInput(format!("udf '{}': {error}", udf.name)))?
        .into_iter()
        .map(|parameter| UdfRuntimeArgument {
            name: parameter
                .name
                .strip_prefix('$')
                .unwrap_or(&parameter.name)
                .to_string(),
            data_type: parameter.data_type,
        })
        .collect::<Vec<_>>();
    arguments.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(UdfRuntimeSignature {
        arguments,
        result_columns: result_columns(planned.plan.schema().inner().as_ref()),
    })
}

fn result_columns(schema: &Schema) -> Vec<UdfRuntimeResultColumn> {
    schema
        .fields()
        .iter()
        .map(|field| UdfRuntimeResultColumn {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect()
}

pub(crate) fn udf_query_parameters(
    udf: &UdfRuntimeDefinition,
    arguments: &QueryParameters,
) -> DataFusionResult<QueryParameters> {
    UdfArgumentBinding::new(udf, arguments).into_query_params()
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "UDF table-function execution consumes literal call binding in the next stack branch."
    )
)]
pub(crate) fn udf_argument_values(
    udf: &UdfRuntimeDefinition,
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
    udf: &UdfRuntimeDefinition,
    argument: &UdfRuntimeArgument,
    expr: &Expr,
) -> DataFusionResult<QueryParameterValue> {
    let expr = unalias(expr);
    let Expr::Literal(value, _) = expr else {
        return Err(DataFusionError::Plan(format!(
            "udf '{}' argument '{}' must be a literal after parameter binding",
            udf.name, argument.name
        )));
    };
    UdfParameterTypeBinding::new(argument.data_type)
        .literal_value(value)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "udf '{}' argument '{}' expected {}, got {}",
                udf.name,
                argument.name,
                argument.data_type.as_manifest_str(),
                scalar_literal_kind(value)
            ))
        })
}

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

#[expect(
    dead_code,
    reason = "UDF table-function execution consumes bound parameter values in the next stack branch."
)]
pub(crate) fn udf_param_values(params: &QueryParameters) -> Vec<(String, ScalarValue)> {
    params
        .iter()
        .map(|(name, value)| (name.clone(), query_parameter_scalar_value(value)))
        .collect()
}

#[expect(
    dead_code,
    reason = "UDF table-function execution consumes result schemas in the next stack branch."
)]
pub(crate) fn udf_arrow_schema(udf: &UdfRuntimeDefinition) -> DataFusionResult<Arc<Schema>> {
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

fn udf_result_field(column: &UdfRuntimeResultColumn) -> Field {
    Field::new(&column.name, column.data_type.clone(), column.nullable)
}

struct UdfArgumentBinding<'a> {
    udf: &'a UdfRuntimeDefinition,
    arguments: &'a QueryParameters,
}

impl<'a> UdfArgumentBinding<'a> {
    fn new(udf: &'a UdfRuntimeDefinition, arguments: &'a QueryParameters) -> Self {
        Self { udf, arguments }
    }

    fn into_query_params(self) -> DataFusionResult<QueryParameters> {
        self.reject_duplicate_argument_definitions()?;
        self.reject_unknown_arguments()?;

        let mut params = self.arguments.clone();
        for argument in &self.udf.arguments {
            self.bind_argument(argument, &mut params)?;
        }
        Ok(params)
    }

    fn reject_duplicate_argument_definitions(&self) -> DataFusionResult<()> {
        let mut seen = BTreeSet::new();
        for argument in &self.udf.arguments {
            if !seen.insert(argument.name.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "udf '{}' argument '{}' is declared more than once",
                    self.udf.name, argument.name
                )));
            }
        }
        Ok(())
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
        argument: &UdfRuntimeArgument,
        params: &mut QueryParameters,
    ) -> DataFusionResult<()> {
        let binding = UdfParameterTypeBinding::new(argument.data_type);
        match params.get(&argument.name) {
            Some(value) if !binding.accepts_value(value) => Err(DataFusionError::Plan(format!(
                "udf '{}' argument '{}' expected {}, got {}",
                self.udf.name,
                argument.name,
                argument.data_type.as_manifest_str(),
                query_parameter_value_kind(value)
            ))),
            Some(_) => Ok(()),
            None => Err(DataFusionError::Plan(format!(
                "udf '{}' is missing argument '{}'",
                self.udf.name, argument.name
            ))),
        }
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

        match value {
            ScalarValue::Utf8(Some(value)) if self.is_string_shaped() => {
                Some(QueryParameterValue::string(value.clone()))
            }
            ScalarValue::LargeUtf8(Some(value)) if self.is_string_shaped() => {
                Some(QueryParameterValue::string(value.clone()))
            }
            ScalarValue::Utf8View(Some(value)) if self.is_string_shaped() => {
                Some(QueryParameterValue::string((*value).clone()))
            }
            ScalarValue::Int64(Some(value)) if self.data_type == ManifestDataType::Int64 => {
                Some(QueryParameterValue::integer(*value))
            }
            ScalarValue::Float64(Some(value)) if self.data_type == ManifestDataType::Float64 => {
                Some(QueryParameterValue::float(*value))
            }
            ScalarValue::Boolean(Some(value)) if self.data_type == ManifestDataType::Boolean => {
                Some(QueryParameterValue::boolean(*value))
            }
            _ => None,
        }
    }

    fn accepts_value(&self, value: &QueryParameterValue) -> bool {
        if matches!(value, QueryParameterValue::String(_)) {
            return self.is_string_shaped();
        }

        matches!(
            (self.data_type, value),
            (ManifestDataType::Int64, QueryParameterValue::Integer(_))
                | (ManifestDataType::Float64, QueryParameterValue::Float(_))
                | (ManifestDataType::Boolean, QueryParameterValue::Boolean(_))
        )
    }

    fn typed_null(&self) -> QueryParameterValue {
        if self.is_string_shaped() {
            return QueryParameterValue::null_string();
        }

        match self.data_type {
            ManifestDataType::Int64 => QueryParameterValue::null_integer(),
            ManifestDataType::Float64 => QueryParameterValue::null_float(),
            ManifestDataType::Boolean => QueryParameterValue::null_boolean(),
            _ => unreachable!("string-binding manifest types returned above"),
        }
    }

    fn is_string_shaped(&self) -> bool {
        parameter_binding_is_string_shaped(self.data_type)
    }
}

fn query_parameter_value_kind(value: &QueryParameterValue) -> &'static str {
    match value {
        QueryParameterValue::String(_) => "string",
        QueryParameterValue::Integer(_) => "integer",
        QueryParameterValue::Float(_) => "float",
        QueryParameterValue::Boolean(_) => "boolean",
    }
}

fn scalar_literal_kind(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_) => "string",
        ScalarValue::Int64(_) => "integer",
        ScalarValue::Float64(_) => "float",
        ScalarValue::Boolean(_) => "boolean",
        _ => "unsupported literal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argument(name: &str, data_type: ManifestDataType) -> UdfRuntimeArgument {
        UdfRuntimeArgument {
            name: name.to_string(),
            data_type,
        }
    }

    fn udf() -> UdfRuntimeDefinition {
        UdfRuntimeDefinition {
            name: "open_pull_requests".to_string(),
            description: String::new(),
            arguments: vec![argument("author", ManifestDataType::Utf8)],
            implementation: UdfRuntimeImplementation::CoralSql {
                query: "select * from github.pull_requests where author = $author".to_string(),
            },
            publish: crate::UdfRuntimePublish {
                table_function: crate::UdfRuntimeTableFunctionPublish {
                    schema: "udfs".to_string(),
                    name: "open_pull_requests".to_string(),
                    description: String::new(),
                },
            },
            result_columns: Vec::new(),
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
}
