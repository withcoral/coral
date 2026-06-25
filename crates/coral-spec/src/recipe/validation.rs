use std::collections::{HashMap, HashSet};

use crate::{ManifestError, Result, validate_identifier};

use super::RawRecipeSpec;
use super::model::{
    RecipeArgumentSpec, RecipeArgumentType, RecipeImplementationSpec, RecipePublishSpec,
    RecipeSpec, RecipeValidationSpec, RecipeValidationValue,
};

const RESERVED_TABLE_FUNCTION_SCHEMAS: &[&str] =
    &["coral", "coral_admin", "__coral_saved_functions"];

pub(super) fn validate_raw_recipe(raw: RawRecipeSpec) -> Result<RecipeSpec> {
    if raw.kind != "recipe" {
        return Err(ManifestError::validation(format!(
            "recipe kind must be 'recipe', got '{}'",
            raw.kind
        )));
    }
    validate_lowercase_identifier(&raw.name, "recipe name")?;
    validate_arguments(&raw.name, &raw.inputs.0)?;
    validate_implementation(&raw.name, &raw.implementation)?;
    validate_validation(&raw.name, &raw.inputs.0, &raw.validation)?;
    validate_publish_targets(&raw.name, &raw.publish)?;
    Ok(RecipeSpec {
        name: raw.name,
        description: raw.description,
        arguments: raw.inputs.0,
        implementation: raw.implementation,
        validation: raw.validation,
        publish: raw.publish,
    })
}

fn validate_arguments(recipe: &str, arguments: &[RecipeArgumentSpec]) -> Result<()> {
    let mut seen = HashSet::new();
    for argument in arguments {
        validate_lowercase_identifier(&argument.name, &format!("recipe '{recipe}' input name"))?;
        if !seen.insert(argument.name.as_str()) {
            return Err(ManifestError::validation(format!(
                "recipe '{recipe}' input '{}' is declared more than once",
                argument.name
            )));
        }
    }
    Ok(())
}

fn validate_implementation(recipe: &str, implementation: &RecipeImplementationSpec) -> Result<()> {
    match implementation {
        RecipeImplementationSpec::CoralSql { query } if query.trim().is_empty() => {
            Err(ManifestError::validation(format!(
                "recipe '{recipe}' coral_sql query must not be empty"
            )))
        }
        RecipeImplementationSpec::CoralSql { .. } => Ok(()),
    }
}

fn validate_validation(
    recipe: &str,
    arguments: &[RecipeArgumentSpec],
    validation: &RecipeValidationSpec,
) -> Result<()> {
    let arguments_by_name = arguments
        .iter()
        .map(|argument| (argument.name.as_str(), argument))
        .collect::<HashMap<_, _>>();

    for (name, value) in &validation.args {
        let Some(argument) = arguments_by_name.get(name.as_str()) else {
            return Err(ManifestError::validation(format!(
                "recipe '{recipe}' validation arg '{name}' is not declared as an input"
            )));
        };
        if !validation_value_matches_argument(argument, value) {
            return Err(ManifestError::validation(format!(
                "recipe '{recipe}' validation arg '{name}' expected {}, got {}",
                argument_type_name(argument.data_type),
                validation_value_type_name(value)
            )));
        }
    }

    for argument in arguments {
        if argument.required
            && !matches!(
                validation.args.get(&argument.name),
                Some(value) if !matches!(value, RecipeValidationValue::Null(()))
            )
        {
            return Err(ManifestError::validation(format!(
                "recipe '{recipe}' validation.args must include required input '{}'",
                argument.name
            )));
        }
    }

    Ok(())
}

fn validation_value_matches_argument(
    argument: &SavedFunctionArgumentSpec,
    value: &SavedFunctionValidationValue,
) -> bool {
    matches!(
        (argument.data_type, value),
        (
            SavedFunctionArgumentType::String,
            SavedFunctionValidationValue::String(_)
        ) | (
            SavedFunctionArgumentType::Integer,
            SavedFunctionValidationValue::Integer(_)
        ) | (
            SavedFunctionArgumentType::Boolean,
            SavedFunctionValidationValue::Boolean(_)
        ) | (_, SavedFunctionValidationValue::Null(()))
    )
}

fn argument_type_name(data_type: SavedFunctionArgumentType) -> &'static str {
    match data_type {
        SavedFunctionArgumentType::String => "string",
        SavedFunctionArgumentType::Integer => "integer",
        SavedFunctionArgumentType::Boolean => "boolean",
    }
}

fn validation_value_type_name(value: &SavedFunctionValidationValue) -> &'static str {
    match value {
        SavedFunctionValidationValue::String(_) => "string",
        SavedFunctionValidationValue::Integer(_) => "integer",
        SavedFunctionValidationValue::Boolean(_) => "boolean",
        SavedFunctionValidationValue::Null(()) => "null",
    }
}

fn validate_publish_targets(recipe: &str, publish: &RecipePublishSpec) -> Result<()> {
    let table_function = &publish.table_function;
    validate_lowercase_identifier(
        &table_function.schema,
        &format!("recipe '{recipe}' table_function publish schema"),
    )?;
    validate_lowercase_identifier(
        &table_function.name,
        &format!("recipe '{recipe}' table_function publish name"),
    )?;
    if RESERVED_TABLE_FUNCTION_SCHEMAS
        .iter()
        .any(|reserved| table_function.schema.eq_ignore_ascii_case(reserved))
    {
        return Err(ManifestError::validation(format!(
            "recipe '{recipe}' table_function publish schema '{}' is reserved",
            table_function.schema
        )));
    }
    if let Some(mcp) = &publish.mcp {
        validate_lowercase_identifier(&mcp.name, &format!("recipe '{recipe}' mcp publish name"))?;
    }
    Ok(())
}

fn validate_lowercase_identifier(value: &str, context: &str) -> Result<()> {
    validate_identifier(value, context)?;
    if value != value.to_ascii_lowercase() {
        return Err(ManifestError::validation(format!(
            "{context} '{value}' must be lowercase"
        )));
    }
    Ok(())
}
