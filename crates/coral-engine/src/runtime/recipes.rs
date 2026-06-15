//! Recipe SQL parameter binding helpers.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::error::{DataFusionError, Result};

use crate::{
    QueryParameterValue, QueryParameters, RecipeRuntimeArgument, RecipeRuntimeArgumentType,
    RecipeRuntimeArgumentValue, RecipeRuntimeDefinition, RecipeRuntimeImplementation,
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
