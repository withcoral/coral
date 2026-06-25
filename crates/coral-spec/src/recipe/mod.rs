//! Recipe artifact parsing and static validation.
//!
//! Recipes are source-neutral task capabilities. This module validates recipe
//! artifact shape and artifact-local invariants. Installed-source references,
//! SQL planning, result columns, and publish collisions against live catalog
//! objects are checked by the app/runtime layers.

mod model;
mod parser;
mod validation;

pub use model::{
    RecipeArgumentSpec, RecipeArgumentType, RecipeImplementationSpec, RecipeMcpPublishSpec,
    RecipePublishSpec, RecipeSpec, RecipeTableFunctionPublishSpec, RecipeValidationSpec,
    RecipeValidationValue,
};
pub use parser::parse_recipe_yaml;
