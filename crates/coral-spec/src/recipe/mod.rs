//! Recipe artifact parsing and static validation.
//!
//! Recipes are source-neutral task capabilities. This module validates the
//! artifact shape only; installed-source references, SQL planning, and publish
//! collisions against live catalog objects are checked by the app/runtime
//! layers.

mod model;
mod parser;
mod validation;

pub use model::{
    RecipeArgumentSpec, RecipeArgumentType, RecipeImplementationSpec, RecipeMcpPublishSpec,
    RecipePublishSpec, RecipeSpec, RecipeTableFunctionPublishSpec, RecipeValidationSpec,
    RecipeValidationValue,
};
pub use parser::parse_recipe_yaml;
