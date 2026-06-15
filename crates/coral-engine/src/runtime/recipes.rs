//! Recipe SQL parameter binding helpers.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};

use crate::runtime::query::QueryRuntimeAdapter;
use crate::{
    CoreError, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
    RecipeRuntimeArgumentType, RecipeRuntimeArgumentValue, RecipeRuntimeDefinition,
    RecipeRuntimeImplementation,
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
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let (sample_schema, sample_values) = infer_recipe_sample_schema(query_runtime, recipe).await?;
    let Some(null_values) = recipe_optional_null_values(recipe, &sample_values) else {
        return Ok(sample_schema);
    };
    let null_schema = infer_recipe_schema_with_values(query_runtime, recipe, &null_values).await?;
    merge_recipe_inferred_schemas(
        recipe.name.as_str(),
        sample_schema.as_ref(),
        null_schema.as_ref(),
    )
}

async fn infer_recipe_sample_schema(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
) -> Result<
    (
        std::sync::Arc<Schema>,
        BTreeMap<String, RecipeRuntimeArgumentValue>,
    ),
    CoreError,
> {
    let mut sample_values = recipe_sample_values(recipe);
    let max_attempts = sample_values.len() + 1;
    let mut attempts = 0;
    loop {
        attempts += 1;
        match infer_recipe_schema_with_values(query_runtime, recipe, &sample_values).await {
            Ok(schema) => return Ok((schema, sample_values)),
            Err(error)
                if attempts < max_attempts
                    && update_recipe_sample_from_allowed_value(&error, &mut sample_values) => {}
            Err(error) => return Err(error),
        }
    }
}

async fn infer_recipe_schema_with_values(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    values: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let params = recipe_query_parameters(recipe, values)
        .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
    query_runtime
        .infer_sql_schema(recipe_sql(recipe), &params)
        .await
}

fn recipe_sample_values(
    recipe: &RecipeRuntimeDefinition,
) -> BTreeMap<String, RecipeRuntimeArgumentValue> {
    recipe
        .arguments
        .iter()
        .map(|argument| {
            (
                argument.name.clone(),
                recipe_sample_value(argument.data_type, &argument.name),
            )
        })
        .collect()
}

fn recipe_optional_null_values(
    recipe: &RecipeRuntimeDefinition,
    sample_values: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Option<BTreeMap<String, RecipeRuntimeArgumentValue>> {
    if recipe.arguments.iter().all(|argument| argument.required) {
        return None;
    }
    Some(
        recipe
            .arguments
            .iter()
            .map(|argument| {
                let value = if argument.required {
                    sample_values
                        .get(&argument.name)
                        .cloned()
                        .unwrap_or_else(|| recipe_sample_value(argument.data_type, &argument.name))
                } else {
                    RecipeRuntimeArgumentValue::Null
                };
                (argument.name.clone(), value)
            })
            .collect(),
    )
}

fn recipe_sample_value(
    data_type: RecipeRuntimeArgumentType,
    argument_name: &str,
) -> RecipeRuntimeArgumentValue {
    match data_type {
        RecipeRuntimeArgumentType::String => {
            RecipeRuntimeArgumentValue::String(format!("__coral_recipe_sample_{argument_name}"))
        }
        RecipeRuntimeArgumentType::Integer => RecipeRuntimeArgumentValue::Integer(0),
        RecipeRuntimeArgumentType::Boolean => RecipeRuntimeArgumentValue::Boolean(false),
    }
}

fn update_recipe_sample_from_allowed_value(
    error: &CoreError,
    sample_values: &mut BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> bool {
    let message = error.to_string();
    let Some(allowed_value) = first_allowed_source_function_value(&message) else {
        return false;
    };
    for sample in sample_values.values_mut() {
        if message.contains(sample_error_value(sample).as_str())
            && let Some(value) = parse_allowed_value_for_sample(sample, allowed_value)
        {
            *sample = value;
            return true;
        }
    }
    false
}

fn sample_error_value(sample: &RecipeRuntimeArgumentValue) -> String {
    match sample {
        RecipeRuntimeArgumentValue::String(value) => value.clone(),
        RecipeRuntimeArgumentValue::Integer(value) => value.to_string(),
        RecipeRuntimeArgumentValue::Boolean(value) => value.to_string(),
        RecipeRuntimeArgumentValue::Null => "NULL".to_string(),
    }
}

fn parse_allowed_value_for_sample(
    sample: &RecipeRuntimeArgumentValue,
    allowed_value: &str,
) -> Option<RecipeRuntimeArgumentValue> {
    match sample {
        RecipeRuntimeArgumentValue::String(_) => Some(RecipeRuntimeArgumentValue::String(
            allowed_value.to_string(),
        )),
        RecipeRuntimeArgumentValue::Integer(_) => allowed_value
            .parse()
            .ok()
            .map(RecipeRuntimeArgumentValue::Integer),
        RecipeRuntimeArgumentValue::Boolean(_) => allowed_value
            .parse()
            .ok()
            .map(RecipeRuntimeArgumentValue::Boolean),
        RecipeRuntimeArgumentValue::Null => None,
    }
}

fn first_allowed_source_function_value(message: &str) -> Option<&str> {
    let (_, values) = message.split_once("expected one of: ")?;
    values
        .split([',', '\n'])
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn merge_recipe_inferred_schemas(
    recipe_name: &str,
    sample_schema: &Schema,
    null_schema: &Schema,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    if sample_schema.fields().len() != null_schema.fields().len() {
        return Err(CoreError::FailedPrecondition(format!(
            "recipe '{recipe_name}' inferred different result column counts for sample and omitted optional arguments"
        )));
    }

    let fields = sample_schema
        .fields()
        .iter()
        .zip(null_schema.fields())
        .map(|(sample, null)| {
            if sample.name() != null.name()
                || !compatible_recipe_data_type(sample.data_type(), null.data_type())
            {
                return Err(CoreError::FailedPrecondition(format!(
                    "recipe '{recipe_name}' inferred incompatible result column '{}' for omitted optional arguments",
                    sample.name()
                )));
            }
            Ok(Field::new(
                sample.name(),
                sample.data_type().clone(),
                sample.is_nullable() || null.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

fn compatible_recipe_data_type(left: &DataType, right: &DataType) -> bool {
    left == right || matches!(left, DataType::Null) || matches!(right, DataType::Null)
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
}
