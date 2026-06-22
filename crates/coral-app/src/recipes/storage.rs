//! Storage seams for workspace recipe inventory and artifacts.

use std::io::ErrorKind;

use crate::bootstrap::AppError;
use crate::recipes::model::{InstalledRecipe, RecipeName};
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

pub(crate) trait RecipeRegistry: Send + Sync {
    fn list_workspace_recipes(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledRecipe>, AppError>;

    fn get_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<InstalledRecipe, AppError>;

    fn upsert_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe: InstalledRecipe,
    ) -> Result<(), AppError>;

    fn remove_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<(), AppError>;
}

#[derive(Clone)]
pub(crate) struct ConfigRecipeRegistry {
    config_store: ConfigStore,
}

impl ConfigRecipeRegistry {
    pub(crate) fn new(config_store: ConfigStore) -> Self {
        Self { config_store }
    }
}

impl RecipeRegistry for ConfigRecipeRegistry {
    fn list_workspace_recipes(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledRecipe>, AppError> {
        self.config_store.list_workspace_recipes(workspace_name)
    }

    fn get_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<InstalledRecipe, AppError> {
        self.config_store.get_recipe(workspace_name, recipe_name)
    }

    fn upsert_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe: InstalledRecipe,
    ) -> Result<(), AppError> {
        self.config_store.upsert_recipe(workspace_name, recipe)
    }

    fn remove_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<(), AppError> {
        self.config_store.remove_recipe(workspace_name, recipe_name)
    }
}

pub(crate) trait RecipeArtifactStore: Send + Sync {
    fn read_recipe_yaml(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<Option<String>, AppError>;

    fn read_runtime_metadata(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<Option<String>, AppError>;

    fn write_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        runtime_metadata: &[u8],
    ) -> Result<RecipeArtifactSnapshot, AppError>;

    fn remove_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<RecipeArtifactSnapshot, AppError>;

    fn restore_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        snapshot: &RecipeArtifactSnapshot,
    ) -> Result<(), AppError>;
}

#[derive(Clone)]
pub(crate) struct FsRecipeArtifactStore {
    layout: AppStateLayout,
}

impl FsRecipeArtifactStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    fn snapshot(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<RecipeArtifactSnapshot, AppError> {
        Ok(RecipeArtifactSnapshot {
            recipe_yaml: read_optional_bytes(
                &self.layout.recipe_file(workspace_name, recipe_name),
            )?,
            runtime_metadata: read_optional_bytes(
                &self.layout.recipe_runtime_file(workspace_name, recipe_name),
            )?,
        })
    }

    fn write_user_recipe_artifact_inner(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        runtime_metadata: &[u8],
    ) -> Result<(), AppError> {
        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        let recipe_file = self.layout.recipe_file(workspace_name, recipe_name);
        let runtime_file = self.layout.recipe_runtime_file(workspace_name, recipe_name);

        fs::ensure_private_dir(&recipe_dir)?;
        fs::write_atomic(&recipe_file, raw_yaml.as_bytes())?;
        fs::write_atomic(&runtime_file, runtime_metadata)?;
        Ok(())
    }
}

impl RecipeArtifactStore for FsRecipeArtifactStore {
    fn read_recipe_yaml(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<Option<String>, AppError> {
        match std::fs::read_to_string(self.layout.recipe_file(workspace_name, recipe_name)) {
            Ok(raw_yaml) => Ok(Some(raw_yaml)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn read_runtime_metadata(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<Option<String>, AppError> {
        match std::fs::read_to_string(self.layout.recipe_runtime_file(workspace_name, recipe_name))
        {
            Ok(raw_metadata) => Ok(Some(raw_metadata)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        runtime_metadata: &[u8],
    ) -> Result<RecipeArtifactSnapshot, AppError> {
        let previous = self.snapshot(workspace_name, recipe_name)?;
        if let Err(error) = self.write_user_recipe_artifact_inner(
            workspace_name,
            recipe_name,
            raw_yaml,
            runtime_metadata,
        ) {
            if let Err(restore_error) =
                self.restore_user_recipe_artifact(workspace_name, recipe_name, &previous)
            {
                tracing::warn!(
                    recipe = %recipe_name,
                    detail = %restore_error,
                    "failed to restore previous recipe artifact after write failure"
                );
            }
            return Err(error);
        }
        Ok(previous)
    }

    fn remove_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<RecipeArtifactSnapshot, AppError> {
        let previous = self.snapshot(workspace_name, recipe_name)?;
        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        match std::fs::remove_dir_all(recipe_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(previous)
    }

    fn restore_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        snapshot: &RecipeArtifactSnapshot,
    ) -> Result<(), AppError> {
        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        let recipe_file = self.layout.recipe_file(workspace_name, recipe_name);
        let runtime_file = self.layout.recipe_runtime_file(workspace_name, recipe_name);

        if snapshot.recipe_yaml.is_none() && snapshot.runtime_metadata.is_none() {
            match std::fs::remove_dir_all(recipe_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(());
        }

        fs::ensure_private_dir(&recipe_dir)?;
        match &snapshot.recipe_yaml {
            Some(raw_yaml) => fs::write_atomic(&recipe_file, raw_yaml)?,
            None => remove_file_if_present(&recipe_file)?,
        }
        match &snapshot.runtime_metadata {
            Some(raw_metadata) => fs::write_atomic(&runtime_file, raw_metadata)?,
            None => remove_file_if_present(&runtime_file)?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecipeArtifactSnapshot {
    recipe_yaml: Option<Vec<u8>>,
    runtime_metadata: Option<Vec<u8>>,
}

fn read_optional_bytes(path: &std::path::Path) -> Result<Option<Vec<u8>>, AppError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn remove_file_if_present(path: &std::path::Path) -> Result<(), AppError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
