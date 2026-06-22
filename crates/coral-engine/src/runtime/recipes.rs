//! Recipe SQL parameter binding helpers.

use std::collections::BTreeMap;
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

use crate::runtime::query::QueryRuntimeAdapter;
use crate::runtime::query::parameter_scalar_value;
use crate::{
    CoreError, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
    RecipeRuntimeArgumentType, RecipeRuntimeArgumentValue, RecipeRuntimeDefinition,
    RecipeRuntimeImplementation, RecipeRuntimeResultColumn,
};

pub(crate) fn recipe_sql(recipe: &RecipeRuntimeDefinition) -> &str {
    let RecipeRuntimeImplementation::CoralSql { query } = &recipe.implementation;
    query
}

pub(crate) fn recipe_query_parameters(
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<QueryParameters> {
    reject_unknown_arguments(recipe, arguments)?;

    recipe
        .arguments
        .iter()
        .map(|argument| {
            let value = arguments.get(&argument.name);
            let query_value = recipe_query_parameter(&recipe.name, argument, value)?;
            Ok((argument.name.clone(), query_value))
        })
        .collect()
}

pub(crate) async fn validate_recipe(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<Arc<Schema>, CoreError> {
    let params = recipe_query_parameters(recipe, arguments)
        .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
    query_runtime
        .validate_sql(recipe_sql(recipe), &params)
        .await
}

pub(crate) fn recipe_argument_values(
    recipe: &RecipeRuntimeDefinition,
    args: &[Expr],
) -> Result<BTreeMap<String, RecipeRuntimeArgumentValue>> {
    if args.len() > recipe.arguments.len() {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' expected at most {} arguments, got {}",
            recipe.name,
            recipe.arguments.len(),
            args.len()
        )));
    }

    recipe
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            let value = args
                .get(index)
                .map(|expr| recipe_argument_value(recipe, argument, expr))
                .transpose()?
                .unwrap_or(RecipeRuntimeArgumentValue::Null);
            Ok((argument.name.clone(), value))
        })
        .collect()
}

fn recipe_argument_value(
    recipe: &RecipeRuntimeDefinition,
    argument: &RecipeRuntimeArgument,
    expr: &Expr,
) -> Result<RecipeRuntimeArgumentValue> {
    let Expr::Literal(value, _) = expr else {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' must be a literal after parameter binding",
            recipe.name, argument.name
        )));
    };
    scalar_recipe_argument_value(value).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' expected {}, got {}",
            recipe.name,
            argument.name,
            argument_type_name(argument.data_type),
            scalar_value_name(value)
        ))
    })
}

fn scalar_recipe_argument_value(value: &ScalarValue) -> Option<RecipeRuntimeArgumentValue> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::String(value.clone()))
        }
        ScalarValue::Int64(Some(value)) => Some(RecipeRuntimeArgumentValue::Integer(*value)),
        ScalarValue::Int32(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Int16(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Int8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value)
            .ok()
            .map(RecipeRuntimeArgumentValue::Integer),
        ScalarValue::UInt32(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt16(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Boolean(Some(value)) => Some(RecipeRuntimeArgumentValue::Boolean(*value)),
        value if value.is_null() => Some(RecipeRuntimeArgumentValue::Null),
        _ => None,
    }
}

pub(crate) fn recipe_param_values(params: &QueryParameters) -> Vec<(String, ScalarValue)> {
    params
        .iter()
        .map(|(name, value)| (name.clone(), parameter_scalar_value(value)))
        .collect()
}

pub(crate) fn recipe_arrow_schema(recipe: &RecipeRuntimeDefinition) -> Result<Arc<Schema>> {
    if recipe.result_columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "published recipe '{}' requires inferred result columns",
            recipe.name
        )));
    }

    let fields = recipe
        .result_columns
        .iter()
        .map(recipe_result_field)
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn recipe_result_field(column: &RecipeRuntimeResultColumn) -> Result<Field> {
    let data_type = DataType::from_str(&column.data_type).map_err(|error| {
        DataFusionError::Plan(format!(
            "recipe result column '{}' has unsupported inferred type '{}': {error}",
            column.name, column.data_type
        ))
    })?;
    Ok(Field::new(&column.name, data_type, column.nullable))
}

fn scalar_value_name(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Utf8(_) | ScalarValue::Utf8View(_) | ScalarValue::LargeUtf8(_) => "string",
        ScalarValue::Int64(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int8(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt8(_) => "integer",
        ScalarValue::Boolean(_) => "boolean",
        value if value.is_null() => "null",
        _ => "unsupported literal",
    }
}

fn reject_unknown_arguments(
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<()> {
    if let Some(argument_name) = arguments.keys().find(|argument_name| {
        recipe
            .arguments
            .iter()
            .all(|argument| argument.name != **argument_name)
    }) {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' received unknown argument '{}'",
            recipe.name, argument_name
        )));
    }

    Ok(())
}

fn recipe_query_parameter(
    recipe_name: &str,
    argument: &RecipeRuntimeArgument,
    value: Option<&RecipeRuntimeArgumentValue>,
) -> Result<QueryParameterValue> {
    match value {
        Some(RecipeRuntimeArgumentValue::String(value))
            if argument.data_type == RecipeRuntimeArgumentType::String =>
        {
            Ok(QueryParameterValue::String(value.clone()))
        }
        Some(RecipeRuntimeArgumentValue::Integer(value))
            if argument.data_type == RecipeRuntimeArgumentType::Integer =>
        {
            Ok(QueryParameterValue::Integer(*value))
        }
        Some(RecipeRuntimeArgumentValue::Boolean(value))
            if argument.data_type == RecipeRuntimeArgumentType::Boolean =>
        {
            Ok(QueryParameterValue::Boolean(*value))
        }
        Some(RecipeRuntimeArgumentValue::Null) if argument.required => {
            Err(DataFusionError::Plan(format!(
                "recipe '{}' argument '{}' is required and cannot be null",
                recipe_name, argument.name
            )))
        }
        Some(RecipeRuntimeArgumentValue::Null) | None if !argument.required => {
            Ok(QueryParameterValue::Null)
        }
        None => Err(DataFusionError::Plan(format!(
            "recipe '{}' is missing required argument '{}'",
            recipe_name, argument.name
        ))),
        Some(value) => Err(DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' expected {}, got {}",
            recipe_name,
            argument.name,
            argument_type_name(argument.data_type),
            argument_value_name(value)
        ))),
    }
}

fn argument_type_name(data_type: RecipeRuntimeArgumentType) -> &'static str {
    match data_type {
        RecipeRuntimeArgumentType::String => "string",
        RecipeRuntimeArgumentType::Integer => "integer",
        RecipeRuntimeArgumentType::Boolean => "boolean",
    }
}

fn argument_value_name(value: &RecipeRuntimeArgumentValue) -> &'static str {
    match value {
        RecipeRuntimeArgumentValue::String(_) => "string",
        RecipeRuntimeArgumentValue::Integer(_) => "integer",
        RecipeRuntimeArgumentValue::Boolean(_) => "boolean",
        RecipeRuntimeArgumentValue::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argument(
        name: &str,
        data_type: RecipeRuntimeArgumentType,
        required: bool,
    ) -> RecipeRuntimeArgument {
        RecipeRuntimeArgument {
            name: name.to_string(),
            data_type,
            required,
            description: String::new(),
        }
    }

    fn recipe() -> RecipeRuntimeDefinition {
        RecipeRuntimeDefinition {
            name: "open_pull_requests".to_string(),
            description: String::new(),
            arguments: vec![
                argument("author", RecipeRuntimeArgumentType::String, true),
                argument("limit", RecipeRuntimeArgumentType::Integer, false),
                argument("draft", RecipeRuntimeArgumentType::Boolean, false),
            ],
            implementation: RecipeRuntimeImplementation::CoralSql {
                query: "select * from github.pull_requests where author = $author".to_string(),
            },
            publish: crate::RecipeRuntimePublish {
                table_function: crate::RecipeRuntimeTableFunctionPublish {
                    schema: "recipes".to_string(),
                    name: "open_pull_requests".to_string(),
                    description: String::new(),
                },
            },
            result_columns: Vec::new(),
        }
    }

    fn args(
        values: impl IntoIterator<Item = (&'static str, RecipeRuntimeArgumentValue)>,
    ) -> BTreeMap<String, RecipeRuntimeArgumentValue> {
        values
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect()
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
    fn recipe_query_parameters_binds_declared_arguments() {
        let recipe = recipe();
        let arguments = args([
            (
                "author",
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit", RecipeRuntimeArgumentValue::Integer(25)),
            ("draft", RecipeRuntimeArgumentValue::Boolean(false)),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            params([
                (
                    "author",
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit", QueryParameterValue::Integer(25)),
                ("draft", QueryParameterValue::Boolean(false)),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_missing_optional_arguments_as_nulls() {
        let recipe = recipe();
        let arguments = args([(
            "author",
            RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
        )]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            params([
                (
                    "author",
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit", QueryParameterValue::Null),
                ("draft", QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_explicit_optional_nulls_as_nulls() {
        let recipe = recipe();
        let arguments = args([
            (
                "author",
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit", RecipeRuntimeArgumentValue::Null),
            ("draft", RecipeRuntimeArgumentValue::Null),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            params([
                (
                    "author",
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit", QueryParameterValue::Null),
                ("draft", QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_invalid_arguments() {
        let recipe = recipe();
        let cases = [
            (
                args([
                    (
                        "author",
                        RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
                    ),
                    (
                        "repository",
                        RecipeRuntimeArgumentValue::String("withcoral/coral".to_string()),
                    ),
                ]),
                "Error during planning: recipe 'open_pull_requests' received unknown argument 'repository'",
            ),
            (
                BTreeMap::new(),
                "Error during planning: recipe 'open_pull_requests' is missing required argument 'author'",
            ),
            (
                args([("author", RecipeRuntimeArgumentValue::Null)]),
                "Error during planning: recipe 'open_pull_requests' argument 'author' is required and cannot be null",
            ),
            (
                args([("author", RecipeRuntimeArgumentValue::Integer(42))]),
                "Error during planning: recipe 'open_pull_requests' argument 'author' expected string, got integer",
            ),
        ];

        for (arguments, expected) in cases {
            assert_eq!(
                recipe_query_parameters(&recipe, &arguments)
                    .unwrap_err()
                    .strip_backtrace(),
                expected
            );
        }
    }

    #[test]
    fn recipe_argument_values_accepts_utf8_view_literals() {
        let recipe = recipe();
        let exprs = vec![Expr::Literal(
            ScalarValue::Utf8View(Some("Bradley-Butcher".into())),
            None,
        )];

        assert_eq!(
            recipe_argument_values(&recipe, &exprs).unwrap(),
            args([
                (
                    "author",
                    RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit", RecipeRuntimeArgumentValue::Null),
                ("draft", RecipeRuntimeArgumentValue::Null),
            ])
        );
    }
}
