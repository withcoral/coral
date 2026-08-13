use std::collections::HashSet;

use crate::{ManifestError, Result, validate_identifier, validate_reserved_source_schema_name};

use super::model::{FunctionDeclaredSignature, FunctionImplementationSpec, FunctionSpec};
use super::parser::{RawFunctionSignature, RawFunctionSpec};

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
        match &self.raw.implementation {
            FunctionImplementationSpec::CoralSql(implementation) => {
                if implementation.query.trim().is_empty() {
                    return Err(ManifestError::validation(format!(
                        "function '{}' SQL body must not be empty",
                        self.raw.frontmatter.name
                    )));
                }
            }
            FunctionImplementationSpec::TypeScript(implementation) => {
                if implementation.source.trim().is_empty() {
                    return Err(ManifestError::validation(format!(
                        "function '{}' TypeScript body must not be empty",
                        self.raw.frontmatter.name
                    )));
                }
                if self.raw.frontmatter.signature.is_none() {
                    return Err(ManifestError::validation(format!(
                        "function '{}' TypeScript implementation requires a declared signature",
                        self.raw.frontmatter.name
                    )));
                }
            }
        }
        if let Some(signature) = &self.raw.frontmatter.signature {
            self.validate_declared_signature(signature)?;
        }
        Ok(())
    }

    fn validate_declared_signature(&self, signature: &RawFunctionSignature) -> Result<()> {
        if signature.result_columns.is_empty() {
            return Err(ManifestError::validation(format!(
                "function '{}' declared signature must declare at least one result column",
                self.raw.frontmatter.name
            )));
        }
        self.validate_signature_names(
            signature
                .arguments
                .iter()
                .map(|argument| argument.name.as_str()),
            "argument",
            "argument",
        )?;
        self.validate_signature_names(
            signature
                .result_columns
                .iter()
                .map(|column| column.name.as_str()),
            "column",
            "result column",
        )
    }

    fn validate_signature_names<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
        kind: &str,
        display_kind: &str,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        for name in names {
            validate_lowercase_identifier(
                name,
                &format!("function '{}' {kind} '{name}'", self.raw.frontmatter.name),
            )?;
            if !seen.insert(name) {
                return Err(ManifestError::validation(format!(
                    "function '{}' declares {display_kind} '{name}' more than once",
                    self.raw.frontmatter.name
                )));
            }
        }
        Ok(())
    }

    fn finish(self) -> FunctionSpec {
        let frontmatter = self.raw.frontmatter;
        let signature = frontmatter
            .signature
            .map(|signature| FunctionDeclaredSignature {
                arguments: signature.arguments,
                result_columns: signature.result_columns,
            });
        FunctionSpec {
            name: frontmatter.name,
            group: frontmatter.schema,
            description: frontmatter.description,
            guide: frontmatter.guide,
            implementation: self.raw.implementation,
            signature,
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
