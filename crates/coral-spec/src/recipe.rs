//! Recipe artifact parsing and static validation.
//!
//! Recipes are source-neutral task capabilities. This module validates the
//! artifact shape only; installed-source references, SQL planning, and publish
//! collisions are checked by the app/runtime layers.

use std::collections::HashSet;
use std::fmt;

use schemars::JsonSchema;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{ManifestError, Result, validate_identifier};

const RESERVED_TABLE_FUNCTION_SCHEMAS: &[&str] = &["coral", "coral_admin", "__coral_recipes"];

/// Validated recipe artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecipeSpec {
    name: String,
    description: String,
    arguments: Vec<RecipeArgumentSpec>,
    implementation: RecipeImplementationSpec,
    publish: Vec<RecipePublishSpec>,
}

/// One typed recipe argument.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecipeArgumentSpec {
    /// Argument name used in recipe SQL placeholders and published call schemas.
    pub name: String,
    /// Scalar argument type.
    #[serde(rename = "type")]
    pub data_type: RecipeArgumentType,
    /// Whether callers must provide this argument.
    #[serde(default)]
    pub required: bool,
    /// Optional human-readable argument description.
    #[serde(default)]
    pub description: String,
}

/// Scalar argument types supported by v1 recipes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecipeArgumentType {
    /// UTF-8 string value.
    String,
    /// Signed 64-bit integer value.
    Integer,
    /// Boolean value.
    Boolean,
}

/// The executable body behind a recipe.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecipeImplementationSpec {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql {
        /// SQL query executed by Coral after typed argument binding.
        query: String,
    },
}

/// One public surface a recipe should publish.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub enum RecipePublishSpec {
    /// Publish the recipe as a public SQL table function.
    TableFunction {
        /// SQL schema where the public table function is exposed.
        schema: String,
        /// Public table-function name within `schema`.
        name: String,
        /// Optional publish-target-specific description.
        description: String,
    },
    /// Publish the recipe as an MCP tool.
    McpTool {
        /// MCP tool name.
        name: String,
        /// Optional publish-target-specific description.
        description: String,
    },
}

/// One recipe input as authored under the `inputs` map.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecipeInputSpec {
    #[serde(rename = "type")]
    data_type: RecipeArgumentType,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    description: String,
}

impl RecipeInputSpec {
    fn into_argument(self, name: String) -> RecipeArgumentSpec {
        RecipeArgumentSpec {
            name,
            data_type: self.data_type,
            required: self.required,
            description: self.description,
        }
    }
}

#[derive(Debug, Default)]
struct RawRecipeInputs(Vec<RecipeArgumentSpec>);

impl<'de> Deserialize<'de> for RawRecipeInputs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawRecipeInputsVisitor;

        impl<'de> Visitor<'de> for RawRecipeInputsVisitor {
            type Value = RawRecipeInputs;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of input names to input specs")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen = HashSet::new();
                let mut arguments = Vec::new();
                while let Some((name, input)) = map.next_entry::<String, RecipeInputSpec>()? {
                    if !seen.insert(name.clone()) {
                        return Err(de::Error::custom(format!(
                            "recipe input '{name}' is declared more than once"
                        )));
                    }
                    arguments.push(input.into_argument(name));
                }
                Ok(RawRecipeInputs(arguments))
            }
        }

        deserializer.deserialize_map(RawRecipeInputsVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecipePublishSpec {
    #[serde(default)]
    table_function: Option<String>,
    #[serde(default)]
    mcp_tool: Option<String>,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecipeSpec {
    kind: String,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    inputs: RawRecipeInputs,
    implementation: RecipeImplementationSpec,
    #[serde(default)]
    publish: Vec<RawRecipePublishSpec>,
}

impl RecipeSpec {
    /// Returns the stable recipe id within one workspace.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the user-facing recipe description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns declared recipe arguments in authored order.
    #[must_use]
    pub fn arguments(&self) -> &[RecipeArgumentSpec] {
        &self.arguments
    }

    /// Returns the executable recipe implementation.
    #[must_use]
    pub fn implementation(&self) -> &RecipeImplementationSpec {
        &self.implementation
    }

    /// Returns public surfaces the recipe asks Coral to publish.
    #[must_use]
    pub fn publish(&self) -> &[RecipePublishSpec] {
        &self.publish
    }
}

/// Parses and statically validates one recipe YAML document.
///
/// # Errors
///
/// Returns [`ManifestError`] when the YAML cannot be parsed, has an unsupported
/// shape, or violates recipe-local invariants.
pub fn parse_recipe_yaml(raw: &str) -> Result<RecipeSpec> {
    let raw: RawRecipeSpec = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    validate_raw_recipe(raw)
}

fn validate_raw_recipe(raw: RawRecipeSpec) -> Result<RecipeSpec> {
    if raw.kind != "recipe" {
        return Err(ManifestError::validation(format!(
            "recipe kind must be 'recipe', got '{}'",
            raw.kind
        )));
    }
    validate_lowercase_identifier(&raw.name, "recipe name")?;
    validate_arguments(&raw.name, &raw.inputs.0)?;
    validate_implementation(&raw.name, &raw.implementation)?;
    let publish = raw_publish_targets(&raw.name, raw.publish)?;
    validate_publish_targets(&raw.name, &publish)?;
    Ok(RecipeSpec {
        name: raw.name,
        description: raw.description,
        arguments: raw.inputs.0,
        implementation: raw.implementation,
        publish,
    })
}

fn raw_publish_targets(
    recipe: &str,
    publish: Vec<RawRecipePublishSpec>,
) -> Result<Vec<RecipePublishSpec>> {
    publish
        .into_iter()
        .map(|target| raw_publish_target(recipe, target))
        .collect()
}

fn raw_publish_target(recipe: &str, target: RawRecipePublishSpec) -> Result<RecipePublishSpec> {
    match (target.table_function, target.mcp_tool) {
        (Some(table_function), None) => {
            let (schema, name) = parse_table_function_target(recipe, &table_function)?;
            Ok(RecipePublishSpec::TableFunction {
                schema,
                name,
                description: target.description,
            })
        }
        (None, Some(name)) => Ok(RecipePublishSpec::McpTool {
            name,
            description: target.description,
        }),
        _ => Err(ManifestError::validation(format!(
            "recipe '{recipe}' publish entry must set exactly one of 'table_function' or 'mcp_tool'"
        ))),
    }
}

fn parse_table_function_target(recipe: &str, target: &str) -> Result<(String, String)> {
    let Some((schema, name)) = target.split_once('.') else {
        return Err(malformed_table_function_target(recipe, target));
    };
    if schema.is_empty() || name.is_empty() || name.contains('.') {
        return Err(malformed_table_function_target(recipe, target));
    }
    Ok((schema.to_string(), name.to_string()))
}

fn malformed_table_function_target(recipe: &str, target: &str) -> ManifestError {
    ManifestError::validation(format!(
        "recipe '{recipe}' table_function publish target '{target}' must be written as schema.name"
    ))
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

fn validate_publish_targets(recipe: &str, publish: &[RecipePublishSpec]) -> Result<()> {
    let mut table_functions = HashSet::new();
    let mut mcp_tools = HashSet::new();
    for target in publish {
        match target {
            RecipePublishSpec::TableFunction { schema, name, .. } => {
                validate_lowercase_identifier(
                    schema,
                    &format!("recipe '{recipe}' table_function publish schema"),
                )?;
                validate_lowercase_identifier(
                    name,
                    &format!("recipe '{recipe}' table_function publish name"),
                )?;
                if RESERVED_TABLE_FUNCTION_SCHEMAS
                    .iter()
                    .any(|reserved| schema.eq_ignore_ascii_case(reserved))
                {
                    return Err(ManifestError::validation(format!(
                        "recipe '{recipe}' table_function publish schema '{schema}' is reserved"
                    )));
                }
                let key = (schema.as_str(), name.as_str());
                if !table_functions.insert(key) {
                    return Err(ManifestError::validation(format!(
                        "recipe '{recipe}' table_function publish target '{schema}.{name}' is declared more than once"
                    )));
                }
            }
            RecipePublishSpec::McpTool { name, .. } => {
                validate_lowercase_identifier(name, &format!("recipe '{recipe}' mcp_tool name"))?;
                if !mcp_tools.insert(name.as_str()) {
                    return Err(ManifestError::validation(format!(
                        "recipe '{recipe}' mcp_tool publish target '{name}' is declared more than once"
                    )));
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{RecipeImplementationSpec, RecipePublishSpec, parse_recipe_yaml};

    fn valid_recipe() -> &'static str {
        r"
kind: recipe
name: github_review_queue
description: GitHub PR review queue
inputs:
  owner:
    type: string
    required: true
  repo:
    type: string
    required: true
implementation:
  kind: coral_sql
  query: |
    select *
    from github.pulls(owner => $owner, repo => $repo)
publish:
  - table_function: recipes.github_review_queue
  - mcp_tool: github_review_queue
"
    }

    #[test]
    fn parse_recipe_yaml_accepts_valid_recipe() {
        let recipe = parse_recipe_yaml(valid_recipe()).expect("recipe should parse");

        assert_eq!(recipe.name(), "github_review_queue");
        assert_eq!(recipe.arguments().len(), 2);
        assert!(matches!(
            recipe.implementation(),
            RecipeImplementationSpec::CoralSql { .. }
        ));
        assert_eq!(recipe.publish().len(), 2);
        assert!(matches!(
            recipe.publish().first(),
            Some(RecipePublishSpec::TableFunction { schema, name, .. })
                if schema == "recipes" && name == "github_review_queue"
        ));
    }
}
