//! Owns user-installed recipe files and workspace inventory.

#![allow(
    dead_code,
    reason = "recipe manager is exposed through API/CLI surfaces in later stack branches"
)]

use std::io::ErrorKind;

use coral_spec::parse_recipe_yaml;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::recipes::model::{InstalledRecipe, RecipeName, RecipeOrigin};
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct RecipeManager {
    config_store: ConfigStore,
    layout: AppStateLayout,
}

impl RecipeManager {
    pub(crate) fn new(config_store: ConfigStore, layout: AppStateLayout) -> Self {
        Self {
            config_store,
            layout,
        }
    }

    pub(crate) fn install_user_recipe(
        &self,
        workspace_name: &WorkspaceName,
        raw_yaml: &str,
    ) -> Result<InstalledRecipe, AppError> {
        let recipe = parse_recipe_yaml(raw_yaml).map_err(|error| {
            AppError::InvalidInput(format!("recipe validation failed: {error}"))
        })?;
        let recipe_name = RecipeName::parse(recipe.name())?;
        let installed = InstalledRecipe {
            name: recipe_name.clone(),
            origin: RecipeOrigin::User,
            enabled: true,
        };

        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        let recipe_file = self.layout.recipe_file(workspace_name, recipe_name);
        let previous_yaml = match std::fs::read(&recipe_file) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };

        fs::ensure_private_dir(&recipe_dir)?;
        fs::write_atomic(&recipe_file, raw_yaml.as_bytes())?;
        if let Err(error) = self
            .config_store
            .upsert_recipe(workspace_name, installed.clone())
        {
            rollback_recipe_install(&recipe_dir, &recipe_file, previous_yaml.as_deref());
            return Err(error);
        }

        Ok(installed)
    }

    pub(crate) fn list_user_recipes(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledRecipe>, AppError> {
        Ok(self
            .config_store
            .list_workspace_recipes(workspace_name)?
            .into_iter()
            .filter(|recipe| recipe.origin == RecipeOrigin::User)
            .collect())
    }

    pub(crate) fn remove_user_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<(), AppError> {
        let installed = self.config_store.get_recipe(workspace_name, recipe_name)?;
        if installed.origin != RecipeOrigin::User {
            return Err(AppError::InvalidInput(format!(
                "recipe '{recipe_name}' is bundled and cannot be removed from workspace config"
            )));
        }
        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        let recipe_dir_backup =
            recipe_dir.with_file_name(format!("{recipe_name}.delete.rollback.{}", Uuid::new_v4()));
        let had_recipe_dir = recipe_dir.exists();
        if had_recipe_dir {
            if recipe_dir_backup.exists() {
                std::fs::remove_dir_all(&recipe_dir_backup)?;
            }
            std::fs::rename(&recipe_dir, &recipe_dir_backup)?;
        }
        if let Err(error) = self.config_store.remove_recipe(workspace_name, recipe_name) {
            if had_recipe_dir
                && recipe_dir_backup.exists()
                && let Err(restore_error) = std::fs::rename(&recipe_dir_backup, &recipe_dir)
            {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove recipe '{recipe_name}': {error}; failed to restore recipe directory from '{}': {restore_error}",
                    recipe_dir_backup.display()
                )));
            }
            return Err(error);
        }
        if recipe_dir_backup.exists() {
            std::fs::remove_dir_all(&recipe_dir_backup)?;
        }
        Ok(())
    }
}

fn rollback_recipe_install(
    recipe_dir: &std::path::Path,
    recipe_file: &std::path::Path,
    previous_yaml: Option<&[u8]>,
) {
    if let Some(previous_yaml) = previous_yaml {
        if let Err(error) = fs::ensure_private_dir(recipe_dir) {
            tracing::warn!(
                path = %recipe_dir.display(),
                detail = %error,
                "failed to restore previous recipe directory after config update failure"
            );
            return;
        }
        if let Err(error) = fs::write_atomic(recipe_file, previous_yaml) {
            tracing::warn!(
                path = %recipe_file.display(),
                detail = %error,
                "failed to restore previous recipe file after config update failure"
            );
        }
    } else if let Err(error) = std::fs::remove_dir_all(recipe_dir) {
        tracing::warn!(
            path = %recipe_dir.display(),
            detail = %error,
            "failed to remove new recipe directory after config update failure"
        );
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::state::AppStateLayout;

    fn fixture() -> (TempDir, AppStateLayout, RecipeManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let manager = RecipeManager::new(config_store, layout.clone());
        (temp, layout, manager)
    }

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn recipe_yaml(name: &str) -> String {
        format!(
            r"
kind: recipe
name: {name}
implementation:
  kind: coral_sql
  query: select 1 as id
publish:
  - table_function: recipes.{name}
"
        )
    }

    #[test]
    fn install_user_recipe_persists_yaml_and_config() {
        let (_temp, layout, manager) = fixture();
        let workspace = workspace();

        let installed = manager
            .install_user_recipe(&workspace, &recipe_yaml("review_queue"))
            .expect("install recipe");

        assert_eq!(installed.name.as_str(), "review_queue");
        assert_eq!(installed.origin, RecipeOrigin::User);
        assert!(installed.enabled);

        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        let raw = std::fs::read_to_string(layout.recipe_file(&workspace, &recipe_name))
            .expect("recipe file");
        assert!(raw.contains("name: review_queue"));
        assert_eq!(
            manager
                .list_user_recipes(&workspace)
                .expect("list recipes")
                .len(),
            1
        );
    }

    #[test]
    fn remove_user_recipe_removes_yaml_and_config() {
        let (_temp, layout, manager) = fixture();
        let workspace = workspace();
        manager
            .install_user_recipe(&workspace, &recipe_yaml("review_queue"))
            .expect("install recipe");
        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");

        manager
            .remove_user_recipe(&workspace, &recipe_name)
            .expect("remove recipe");

        assert!(!layout.recipe_dir(&workspace, &recipe_name).exists());
        assert!(
            manager
                .list_user_recipes(&workspace)
                .expect("list recipes")
                .is_empty()
        );
    }
}
