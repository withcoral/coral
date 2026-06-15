//! Recipe SQL parameter binding and catalog helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

use crate::runtime::catalog::{
    CatalogTableFunction, CatalogTableFunctionArgument, CatalogTableFunctionResultColumn,
};
use crate::runtime::query::QueryRuntimeAdapter;
use crate::runtime::query::parameter_scalar_value;
use crate::{
    CoreError, QueryExecution, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
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

pub(crate) async fn infer_recipe_schema(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let params = recipe_query_parameters(recipe, arguments)
        .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
    query_runtime
        .infer_sql_schema(recipe_sql(recipe), &params)
        .await
}

pub(crate) async fn validate_recipe(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let schema = infer_recipe_schema(query_runtime, recipe, arguments).await?;
    execute_recipe_sql(query_runtime, recipe, arguments).await?;
    Ok(schema)
}

pub(crate) async fn execute_recipe_sql(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<QueryExecution, CoreError> {
    let params = recipe_query_parameters(recipe, arguments)
        .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
    query_runtime.execute_sql(recipe_sql(recipe), &params).await
}

pub(crate) fn published_table_functions(
    recipes: &[RecipeRuntimeDefinition],
) -> Result<Vec<CatalogTableFunction>> {
    let mut functions = Vec::new();
    let mut seen = BTreeSet::new();

    for recipe in recipes {
        let publish = &recipe.publish.table_function;
        let key = (publish.schema.clone(), publish.name.clone());
        if !seen.insert(key.clone()) {
            return Err(DataFusionError::Plan(format!(
                "duplicate recipe table function {}.{}",
                key.0, key.1
            )));
        }

        recipe_arrow_schema(recipe)?;
        functions.push(CatalogTableFunction {
            schema_name: publish.schema.clone(),
            function_name: publish.name.clone(),
            kind: "recipe".to_string(),
            description: publish_description(&publish.description, &recipe.description),
            arguments: recipe
                .arguments
                .iter()
                .map(|argument| CatalogTableFunctionArgument {
                    name: argument.name.clone(),
                    required: argument.required,
                    values: Vec::new(),
                })
                .collect(),
            result_columns: recipe
                .result_columns
                .iter()
                .map(|column| CatalogTableFunctionResultColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    description: column.description.clone(),
                })
                .collect(),
            search_limits_json: None,
        });
    }

    functions.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });
    Ok(functions)
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

fn publish_description(target_description: &str, recipe_description: &str) -> String {
    if target_description.trim().is_empty() {
        recipe_description.to_string()
    } else {
        target_description.to_string()
    }
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
    let declared_arguments = recipe
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<BTreeSet<_>>();

    if let Some(argument_name) = arguments
        .keys()
        .find(|argument_name| !declared_arguments.contains(argument_name.as_str()))
    {
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

    fn recipe() -> RecipeRuntimeDefinition {
        RecipeRuntimeDefinition {
            name: "open_pull_requests".to_string(),
            description: String::new(),
            arguments: vec![
                RecipeRuntimeArgument {
                    name: "author".to_string(),
                    data_type: RecipeRuntimeArgumentType::String,
                    required: true,
                    description: String::new(),
                },
                RecipeRuntimeArgument {
                    name: "limit".to_string(),
                    data_type: RecipeRuntimeArgumentType::Integer,
                    required: false,
                    description: String::new(),
                },
                RecipeRuntimeArgument {
                    name: "draft".to_string(),
                    data_type: RecipeRuntimeArgumentType::Boolean,
                    required: false,
                    description: String::new(),
                },
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

    #[test]
    fn recipe_sql_returns_coral_sql_body() {
        let recipe = recipe();

        assert_eq!(
            recipe_sql(&recipe),
            "select * from github.pull_requests where author = $author"
        );
    }

    #[test]
    fn recipe_query_parameters_binds_declared_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit".to_string(), RecipeRuntimeArgumentValue::Integer(25)),
            (
                "draft".to_string(),
                RecipeRuntimeArgumentValue::Boolean(false),
            ),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Integer(25)),
                ("draft".to_string(), QueryParameterValue::Boolean(false)),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_missing_optional_arguments_as_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([(
            "author".to_string(),
            RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
        )]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Null),
                ("draft".to_string(), QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_explicit_optional_nulls_as_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit".to_string(), RecipeRuntimeArgumentValue::Null),
            ("draft".to_string(), RecipeRuntimeArgumentValue::Null),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Null),
                ("draft".to_string(), QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_unknown_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            (
                "repository".to_string(),
                RecipeRuntimeArgumentValue::String("withcoral/coral".to_string()),
            ),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' received unknown argument 'repository'"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_missing_required_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::new();

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' is missing required argument 'author'"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_required_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([("author".to_string(), RecipeRuntimeArgumentValue::Null)]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' argument 'author' is required and cannot be null"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_type_mismatches() {
        let recipe = recipe();
        let arguments = BTreeMap::from([(
            "author".to_string(),
            RecipeRuntimeArgumentValue::Integer(42),
        )]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' argument 'author' expected string, got integer"
        );
    }

    #[test]
    fn recipe_argument_values_accepts_utf8_view_literals() {
        let recipe = recipe();
        let args = vec![Expr::Literal(
            ScalarValue::Utf8View(Some("Bradley-Butcher".into())),
            None,
        )];

        assert_eq!(
            recipe_argument_values(&recipe, &args).unwrap(),
            BTreeMap::from([
                (
                    "author".to_string(),
                    RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), RecipeRuntimeArgumentValue::Null),
                ("draft".to_string(), RecipeRuntimeArgumentValue::Null),
            ])
        );
    }
}
