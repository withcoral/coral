//! Recipe artifact parsing and static validation.
//!
//! Recipes are source-neutral task capabilities. This module validates the
//! artifact shape only; installed-source references, SQL planning, and publish
//! collisions against live catalog objects are checked by the app/runtime
//! layers.

use std::collections::{BTreeMap, HashMap, HashSet};
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
    validation: RecipeValidationSpec,
    publish: RecipePublishSpec,
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

/// Runtime validation inputs for one recipe.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecipeValidationSpec {
    /// Concrete argument values Coral uses when validating the recipe at install time.
    #[serde(default)]
    pub args: BTreeMap<String, RecipeValidationValue>,
}

/// One scalar validation argument value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum RecipeValidationValue {
    /// UTF-8 string value.
    String(String),
    /// Signed 64-bit integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
    /// Explicit null value.
    Null(()),
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

/// Public surfaces a recipe should publish.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecipePublishSpec {
    /// Required SQL table-function surface.
    pub table_function: RecipeTableFunctionPublishSpec,
    /// Optional MCP tool surface.
    #[serde(default)]
    pub mcp: Option<RecipeMcpPublishSpec>,
}

/// SQL table-function surface published by a recipe.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecipeTableFunctionPublishSpec {
    /// SQL schema where the public table function is exposed.
    pub schema: String,
    /// Public table-function name within `schema`.
    pub name: String,
    /// Optional publish-target-specific description.
    #[serde(default)]
    pub description: String,
}

/// Optional MCP tool surface published by a recipe.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecipeMcpPublishSpec {
    /// MCP tool name.
    pub name: String,
    /// Optional publish-target-specific description.
    #[serde(default)]
    pub description: String,
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
struct RawRecipeSpec {
    kind: String,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    inputs: RawRecipeInputs,
    implementation: RecipeImplementationSpec,
    #[serde(default)]
    validation: RecipeValidationSpec,
    publish: RecipePublishSpec,
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

    /// Returns the install-time validation invocation.
    #[must_use]
    pub fn validation(&self) -> &RecipeValidationSpec {
        &self.validation
    }

    /// Returns public surfaces the recipe asks Coral to publish.
    #[must_use]
    pub fn publish(&self) -> &RecipePublishSpec {
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
    argument: &RecipeArgumentSpec,
    value: &RecipeValidationValue,
) -> bool {
    matches!(
        (argument.data_type, value),
        (RecipeArgumentType::String, RecipeValidationValue::String(_))
            | (
                RecipeArgumentType::Integer,
                RecipeValidationValue::Integer(_)
            )
            | (
                RecipeArgumentType::Boolean,
                RecipeValidationValue::Boolean(_)
            )
            | (_, RecipeValidationValue::Null(()))
    )
}

fn argument_type_name(data_type: RecipeArgumentType) -> &'static str {
    match data_type {
        RecipeArgumentType::String => "string",
        RecipeArgumentType::Integer => "integer",
        RecipeArgumentType::Boolean => "boolean",
    }
}

fn validation_value_type_name(value: &RecipeValidationValue) -> &'static str {
    match value {
        RecipeValidationValue::String(_) => "string",
        RecipeValidationValue::Integer(_) => "integer",
        RecipeValidationValue::Boolean(_) => "boolean",
        RecipeValidationValue::Null(()) => "null",
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

#[cfg(test)]
mod tests {
    use super::{RecipeImplementationSpec, RecipeValidationValue, parse_recipe_yaml};

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
validation:
  args:
    owner: withcoral
    repo: coral
publish:
  table_function:
    schema: recipes
    name: github_review_queue
  mcp:
    name: github_review_queue
"
    }

    #[test]
    fn parse_recipe_yaml_accepts_valid_recipe() {
        let recipe = parse_recipe_yaml(valid_recipe()).expect("recipe should parse");

        assert_eq!(recipe.name(), "github_review_queue");
        assert_eq!(recipe.arguments().len(), 2);
        assert!(matches!(
            recipe.validation().args.get("owner"),
            Some(RecipeValidationValue::String(owner)) if owner == "withcoral"
        ));
        assert!(matches!(
            recipe.implementation(),
            RecipeImplementationSpec::CoralSql { .. }
        ));
        assert_eq!(recipe.publish().table_function.schema, "recipes");
        assert_eq!(recipe.publish().table_function.name, "github_review_queue");
        assert_eq!(
            recipe.publish().mcp.as_ref().map(|mcp| mcp.name.as_str()),
            Some("github_review_queue")
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_unknown_kind() {
        let error = parse_recipe_yaml(
            r"
kind: source
name: demo
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("kind should fail");

        assert_eq!(
            error.to_string(),
            "recipe kind must be 'recipe', got 'source'"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_duplicate_inputs() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
inputs:
  owner:
    type: string
  owner:
    type: string
implementation:
  kind: coral_sql
  query: select 1
",
        )
        .expect_err("duplicate input should fail");

        assert!(
            error
                .to_string()
                .contains("recipe input 'owner' is declared more than once")
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_mixed_case_identifiers() {
        let recipe_name_error = parse_recipe_yaml(
            r"
kind: recipe
name: Demo
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("mixed-case recipe name should fail");

        assert_eq!(
            recipe_name_error.to_string(),
            "recipe name 'Demo' must be lowercase"
        );

        let input_name_error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
inputs:
  Owner:
    type: string
implementation:
  kind: coral_sql
  query: select $Owner
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("mixed-case input name should fail");

        assert_eq!(
            input_name_error.to_string(),
            "recipe 'demo' input name 'Owner' must be lowercase"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_unknown_fields() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
presentation: {}
implementation:
  kind: coral_sql
  query: select 1
",
        )
        .expect_err("unknown field should fail");

        assert!(error.to_string().contains("unknown field `presentation`"));
    }

    #[test]
    fn parse_recipe_yaml_rejects_empty_coral_sql_query() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
implementation:
  kind: coral_sql
  query: '   '
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("empty query should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' coral_sql query must not be empty"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_missing_required_validation_arg() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
inputs:
  owner:
    type: string
    required: true
implementation:
  kind: coral_sql
  query: select $owner
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("missing validation arg should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' validation.args must include required input 'owner'"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_unknown_validation_arg() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
validation:
  args:
    owner: withcoral
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("unknown validation arg should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' validation arg 'owner' is not declared as an input"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_validation_arg_type_mismatch() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
inputs:
  limit:
    type: integer
    required: true
validation:
  args:
    limit: many
implementation:
  kind: coral_sql
  query: select $limit
publish:
  table_function:
    schema: recipes
    name: demo
",
        )
        .expect_err("validation arg type mismatch should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' validation arg 'limit' expected integer, got string"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_reserved_table_function_schemas() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: __coral_recipes
    name: demo
",
        )
        .expect_err("reserved recipe schema should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' table_function publish schema '__coral_recipes' is reserved"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_missing_publish() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
implementation:
  kind: coral_sql
  query: select 1
",
        )
        .expect_err("missing publish should fail");

        assert!(error.to_string().contains("missing field `publish`"));
    }

    #[test]
    fn parse_recipe_yaml_rejects_malformed_table_function_target() {
        let error = parse_recipe_yaml(
            r"
kind: recipe
name: demo
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: Recipes
    name: demo
",
        )
        .expect_err("mixed-case schema should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' table_function publish schema 'Recipes' must be lowercase"
        );
    }

    #[test]
    fn parse_recipe_yaml_rejects_malformed_mcp_publish_target() {
        let error = parse_recipe_yaml(&recipe_yaml_with_mcp_name("Demo"))
            .expect_err("mixed-case mcp target should fail");

        assert_eq!(
            error.to_string(),
            "recipe 'demo' mcp publish name 'Demo' must be lowercase"
        );
    }

    #[test]
    fn parse_recipe_yaml_allows_builtin_mcp_publish_name() {
        let spec = parse_recipe_yaml(&recipe_yaml_with_mcp_name("sql"))
            .expect("built-in MCP names are prefixed by the MCP adapter");

        assert_eq!(spec.publish().mcp.as_ref().expect("mcp").name, "sql");
    }

    fn recipe_yaml_with_mcp_name(name: &str) -> String {
        format!(
            r"
kind: recipe
name: demo
implementation:
  kind: coral_sql
  query: select 1
publish:
  table_function:
    schema: recipes
    name: demo
  mcp:
    name: {name}
"
        )
    }
}
