//! Recipe artifact parsing and static validation.
//!
//! Recipes are source-neutral task capabilities. This module validates the
//! artifact shape only; installed-source references, SQL planning, and publish
//! collisions against live catalog objects are checked by the app/runtime
//! layers.

use std::collections::HashSet;
use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::{ManifestError, Result};

mod model;
mod validation;

pub use model::{
    RecipeArgumentSpec, RecipeArgumentType, RecipeImplementationSpec, RecipeMcpPublishSpec,
    RecipePublishSpec, RecipeSpec, RecipeTableFunctionPublishSpec, RecipeValidationSpec,
    RecipeValidationValue,
};

use validation::validate_raw_recipe;

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
pub(super) struct RawRecipeInputs(pub(super) Vec<RecipeArgumentSpec>);

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
pub(super) struct RawRecipeSpec {
    pub(super) kind: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) description: String,
    #[serde(default)]
    pub(super) inputs: RawRecipeInputs,
    pub(super) implementation: RecipeImplementationSpec,
    #[serde(default)]
    pub(super) validation: RecipeValidationSpec,
    pub(super) publish: RecipePublishSpec,
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
