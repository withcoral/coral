use crate::{ManifestError, Result, validate_identifier, validate_reserved_source_schema_name};

use super::model::FunctionSpec;
use super::parser::RawFunctionSpec;

const RESERVED_FUNCTION_SCHEMA_NAMES: &[&str] = &["__coral_udfs"];

pub(super) fn validate_raw_function(raw: RawFunctionSpec) -> Result<FunctionSpec> {
    FunctionValidator { raw }.validate()
}

struct FunctionValidator {
    raw: RawFunctionSpec,
}

impl FunctionValidator {
    fn validate(self) -> Result<FunctionSpec> {
        self.validate_header()?;
        self.validate_implementation()?;
        Ok(self.finish())
    }

    fn validate_header(&self) -> Result<()> {
        let frontmatter = &self.raw.frontmatter;
        validate_lowercase_identifier(&frontmatter.name, "function name")?;
        validate_lowercase_identifier(
            &frontmatter.schema,
            &format!("function '{}' schema", frontmatter.name),
        )?;
        validate_reserved_source_schema_name(
            &frontmatter.schema,
            &format!("function '{}' schema", frontmatter.name),
        )?;
        if RESERVED_FUNCTION_SCHEMA_NAMES
            .iter()
            .any(|reserved| frontmatter.schema.eq_ignore_ascii_case(reserved))
        {
            return Err(ManifestError::validation(format!(
                "function '{}' schema '{}' is reserved",
                frontmatter.name, frontmatter.schema
            )));
        }
        Ok(())
    }

    fn validate_implementation(&self) -> Result<()> {
        if self.raw.implementation.coral_sql.query.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "function '{}' SQL body must not be empty",
                self.raw.frontmatter.name
            )));
        }
        Ok(())
    }

    fn finish(self) -> FunctionSpec {
        let frontmatter = self.raw.frontmatter;
        FunctionSpec {
            name: frontmatter.name,
            schema: frontmatter.schema,
            description: frontmatter.description,
            guide: frontmatter.guide,
            implementation: self.raw.implementation,
        }
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
