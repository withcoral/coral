use std::collections::{HashMap, HashSet};

use crate::{ManifestError, Result, validate_identifier};

use super::RawRecipeSpec;
use super::model::{
    SavedFunctionArgumentSpec, SavedFunctionArgumentType, SavedFunctionImplementationSpec,
    SavedFunctionSpec, SavedFunctionValidationValue,
};

const RESERVED_TABLE_FUNCTION_SCHEMAS: &[&str] =
    &["coral", "coral_admin", "__coral_saved_functions"];

pub(super) fn validate_raw_saved_function(raw: RawSavedFunctionSpec) -> Result<SavedFunctionSpec> {
    SavedFunctionValidator { raw }.validate()
}

struct SavedFunctionValidator {
    raw: RawSavedFunctionSpec,
}

impl SavedFunctionValidator {
    fn validate(self) -> Result<SavedFunctionSpec> {
        self.validate_header()?;
        self.validate_inputs()?;
        self.validate_implementation()?;
        self.validate_validation_call()?;
        self.validate_publish()?;
        Ok(self.finish())
    }

    fn validate_header(&self) -> Result<()> {
        if self.raw.kind != "saved_function" {
            return Err(ManifestError::validation(format!(
                "saved_function kind must be 'saved_function', got '{}'",
                self.raw.kind
            )));
        }
        validate_lowercase_identifier(&self.raw.name, "saved_function name")
    }

    fn validate_inputs(&self) -> Result<()> {
        let mut seen = HashSet::new();
        for argument in &self.raw.inputs.0 {
            validate_lowercase_identifier(
                &argument.name,
                &format!("saved_function '{}' input name", self.raw.name),
            )?;
            if !seen.insert(argument.name.as_str()) {
                return Err(ManifestError::validation(format!(
                    "recipe '{}' input '{}' is declared more than once",
                    self.raw.name, argument.name
                )));
            }
        }
        Ok(())
    }

    fn validate_implementation(&self) -> Result<()> {
        match &self.raw.implementation {
            SavedFunctionImplementationSpec::CoralSql { query } if query.trim().is_empty() => {
                Err(ManifestError::validation(format!(
                    "saved_function '{}' coral_sql query must not be empty",
                    self.raw.name
                )))
            }
            SavedFunctionImplementationSpec::CoralSql { .. } => Ok(()),
        }
    }

    fn validate_validation_call(&self) -> Result<()> {
        let arguments_by_name = self
            .raw
            .inputs
            .0
            .iter()
            .map(|argument| (argument.name.as_str(), argument))
            .collect::<HashMap<_, _>>();

        for (name, value) in &self.raw.validation.args {
            let Some(argument) = arguments_by_name.get(name.as_str()) else {
                return Err(ManifestError::validation(format!(
                    "saved_function '{}' validation arg '{name}' is not declared as an input",
                    self.raw.name
                )));
            };
            if !validation_value_matches_argument(argument, value) {
                return Err(ManifestError::validation(format!(
                    "saved_function '{}' validation arg '{name}' expected {}, got {}",
                    self.raw.name,
                    argument_type_name(argument.data_type),
                    validation_value_type_name(value)
                )));
            }
        }

        for argument in &self.raw.inputs.0 {
            if argument.required
                && !matches!(
                    self.raw.validation.args.get(&argument.name),
                    Some(value) if !matches!(value, SavedFunctionValidationValue::Null(()))
                )
            {
                return Err(ManifestError::validation(format!(
                    "saved_function '{}' validation.args must include required input '{}'",
                    self.raw.name, argument.name
                )));
            }
        }

        Ok(())
    }

    fn validate_publish(&self) -> Result<()> {
        let table_function = &self.raw.publish.table_function;
        validate_lowercase_identifier(
            &table_function.schema,
            &format!(
                "saved_function '{}' table_function publish schema",
                self.raw.name
            ),
        )?;
        validate_lowercase_identifier(
            &table_function.name,
            &format!(
                "saved_function '{}' table_function publish name",
                self.raw.name
            ),
        )?;
        if RESERVED_TABLE_FUNCTION_SCHEMAS
            .iter()
            .any(|reserved| table_function.schema.eq_ignore_ascii_case(reserved))
        {
            return Err(ManifestError::validation(format!(
                "saved_function '{}' table_function publish schema '{}' is reserved",
                self.raw.name, table_function.schema
            )));
        }
        if let Some(mcp) = &self.raw.publish.mcp {
            validate_lowercase_identifier(
                &mcp.name,
                &format!("saved_function '{}' mcp publish name", self.raw.name),
            )?;
        }
        Ok(())
    }

    fn finish(self) -> SavedFunctionSpec {
        SavedFunctionSpec {
            name: self.raw.name,
            description: self.raw.description,
            arguments: self.raw.inputs.0,
            implementation: self.raw.implementation,
            validation: self.raw.validation,
            publish: self.raw.publish,
        }
    }
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

fn validate_lowercase_identifier(value: &str, context: &str) -> Result<()> {
    validate_identifier(value, context)?;
    if value != value.to_ascii_lowercase() {
        return Err(ManifestError::validation(format!(
            "{context} '{value}' must be lowercase"
        )));
    }
    Ok(())
}
