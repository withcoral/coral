//! Owns user-installed recipe files and workspace inventory.

#![allow(
    dead_code,
    reason = "recipe manager is exposed through API/CLI surfaces in later stack branches"
)]

use std::collections::HashSet;
use std::fmt;
use std::io::ErrorKind;

use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, RecipeRuntimeArgument, RecipeRuntimeArgumentType,
    RecipeRuntimeDefinition, RecipeRuntimeImplementation, RecipeRuntimePublish,
    RecipeRuntimeResultColumn,
};
use coral_spec::{
    RecipeArgumentType, RecipeImplementationSpec, RecipePublishSpec, RecipeSpec, parse_recipe_yaml,
};
use serde::{Deserialize, Serialize};
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

struct RecipeArtifact {
    name: RecipeName,
    origin: RecipeOrigin,
    raw_yaml: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecipeRuntimeMetadata {
    version: u32,
    result_columns: Vec<CachedRecipeResultColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedRecipeResultColumn {
    name: String,
    data_type: String,
    nullable: bool,
    #[serde(default)]
    description: String,
}

impl From<&RecipeRuntimeResultColumn> for CachedRecipeResultColumn {
    fn from(column: &RecipeRuntimeResultColumn) -> Self {
        Self {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            description: column.description.clone(),
        }
    }
}

impl From<CachedRecipeResultColumn> for RecipeRuntimeResultColumn {
    fn from(column: CachedRecipeResultColumn) -> Self {
        Self {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
            description: column.description,
        }
    }
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
        self.install_user_recipe_artifact(workspace_name, &recipe_name, raw_yaml, None)
    }

    pub(crate) fn install_validated_user_recipe(
        &self,
        workspace_name: &WorkspaceName,
        raw_yaml: &str,
        runtime_recipe: &RecipeRuntimeDefinition,
    ) -> Result<InstalledRecipe, AppError> {
        let recipe = parse_recipe_yaml(raw_yaml).map_err(|error| {
            AppError::InvalidInput(format!("recipe validation failed: {error}"))
        })?;
        let recipe_name = RecipeName::parse(recipe.name())?;
        if recipe_name.as_str() != runtime_recipe.name {
            return Err(AppError::FailedPrecondition(format!(
                "validated recipe '{}' does not match installed recipe '{}'",
                runtime_recipe.name, recipe_name
            )));
        }
        self.install_user_recipe_artifact(
            workspace_name,
            &recipe_name,
            raw_yaml,
            Some(runtime_recipe),
        )
    }

    fn install_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        runtime_recipe: Option<&RecipeRuntimeDefinition>,
    ) -> Result<InstalledRecipe, AppError> {
        let installed = InstalledRecipe {
            name: recipe_name.clone(),
            origin: RecipeOrigin::User,
            enabled: true,
        };

        let recipe_dir = self.layout.recipe_dir(workspace_name, recipe_name);
        let recipe_file = self.layout.recipe_file(workspace_name, recipe_name);
        let recipe_runtime_file = self.layout.recipe_runtime_file(workspace_name, recipe_name);
        let previous_yaml = match std::fs::read(&recipe_file) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let previous_runtime = match std::fs::read(&recipe_runtime_file) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };

        fs::ensure_private_dir(&recipe_dir)?;
        fs::write_atomic(&recipe_file, raw_yaml.as_bytes())?;
        if let Err(error) = write_recipe_runtime_metadata(&recipe_runtime_file, runtime_recipe) {
            rollback_recipe_install(
                &recipe_dir,
                &recipe_file,
                previous_yaml.as_deref(),
                &recipe_runtime_file,
                previous_runtime.as_deref(),
            );
            return Err(error);
        }
        if let Err(error) = self
            .config_store
            .upsert_recipe(workspace_name, installed.clone())
        {
            rollback_recipe_install(
                &recipe_dir,
                &recipe_file,
                previous_yaml.as_deref(),
                &recipe_runtime_file,
                previous_runtime.as_deref(),
            );
            return Err(error);
        }

        Ok(installed)
    }

    pub(crate) async fn validate_user_recipe_yaml(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
        raw_yaml: &str,
    ) -> Result<RecipeRuntimeDefinition, AppError> {
        let spec = parse_recipe_yaml(raw_yaml).map_err(|error| {
            AppError::InvalidInput(format!("recipe validation failed: {error}"))
        })?;
        let recipe_name = RecipeName::parse(spec.name())?;
        let mut publish_targets =
            source_publish_targets(selected_sources, runtime_config()?).await?;
        self.record_installed_recipe_publish_targets(
            workspace_name,
            &recipe_name,
            &mut publish_targets,
        )?;
        let runtime_recipe =
            infer_runtime_recipe(selected_sources, runtime_config()?, &spec).await?;
        record_publish_targets(&runtime_recipe, &mut publish_targets)?;
        Ok(runtime_recipe)
    }

    pub(crate) fn list_recipes(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<RecipeRuntimeDefinition>, AppError> {
        let artifacts = self.load_recipe_artifacts(workspace_name)?;
        let mut seen_names = HashSet::new();
        let mut recipes = Vec::new();
        for artifact in artifacts {
            if !seen_names.insert(artifact.name.clone()) {
                skip_recipe(
                    &artifact,
                    format_args!("recipe '{}' is installed more than once", artifact.name),
                );
                continue;
            }
            let spec = match parse_recipe_yaml(&artifact.raw_yaml) {
                Ok(spec) => spec,
                Err(error) => {
                    skip_recipe(&artifact, format_args!("recipe is invalid: {error}"));
                    continue;
                }
            };
            let mut recipe = runtime_recipe_without_columns(&spec);
            if artifact.origin == RecipeOrigin::User {
                recipe.result_columns =
                    self.cached_recipe_result_columns(workspace_name, &artifact.name);
            }
            recipes.push(recipe);
        }
        Ok(recipes)
    }

    pub(crate) async fn load_runtime_recipes(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
    ) -> Result<Vec<RecipeRuntimeDefinition>, AppError> {
        let artifacts = self.load_recipe_artifacts(workspace_name)?;
        if artifacts.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen_names = HashSet::new();
        let mut publish_targets =
            source_publish_targets(selected_sources, runtime_config()?).await?;
        let mut runtime_recipes = Vec::new();
        for artifact in artifacts {
            if !seen_names.insert(artifact.name.clone()) {
                skip_recipe(
                    &artifact,
                    format_args!("recipe '{}' is installed more than once", artifact.name),
                );
                continue;
            }
            let spec = match parse_recipe_yaml(&artifact.raw_yaml) {
                Ok(spec) => spec,
                Err(error) => {
                    skip_recipe(&artifact, format_args!("recipe is invalid: {error}"));
                    continue;
                }
            };
            let runtime_recipe =
                match infer_runtime_recipe(selected_sources, runtime_config()?, &spec).await {
                    Ok(runtime_recipe) => runtime_recipe,
                    Err(error) => {
                        skip_recipe(
                            &artifact,
                            format_args!("recipe failed runtime validation: {error}"),
                        );
                        continue;
                    }
                };
            if let Err(error) = record_publish_targets(&runtime_recipe, &mut publish_targets) {
                skip_recipe(&artifact, format_args!("{error}"));
                continue;
            }
            runtime_recipes.push(runtime_recipe);
        }
        Ok(runtime_recipes)
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

    fn load_recipe_artifacts(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<RecipeArtifact>, AppError> {
        let mut artifacts = Vec::new();
        for installed in self.config_store.list_workspace_recipes(workspace_name)? {
            if !installed.enabled {
                continue;
            }
            let recipe_file = self.layout.recipe_file(workspace_name, &installed.name);
            let raw_yaml = match std::fs::read_to_string(&recipe_file) {
                Ok(raw_yaml) => raw_yaml,
                Err(error) => {
                    tracing::warn!(
                        recipe = %installed.name,
                        path = %recipe_file.display(),
                        detail = %error,
                        "skipping installed recipe because its recipe file could not be read"
                    );
                    continue;
                }
            };
            artifacts.push(RecipeArtifact {
                name: installed.name,
                origin: installed.origin,
                raw_yaml,
            });
        }

        artifacts.sort_by(|left, right| {
            (origin_sort_key(left.origin), left.name.as_str())
                .cmp(&(origin_sort_key(right.origin), right.name.as_str()))
        });
        Ok(artifacts)
    }

    fn record_installed_recipe_publish_targets(
        &self,
        workspace_name: &WorkspaceName,
        replacing_recipe: &RecipeName,
        publish_targets: &mut HashSet<PublishTarget>,
    ) -> Result<(), AppError> {
        let mut seen_names = HashSet::new();
        for artifact in self.load_recipe_artifacts(workspace_name)? {
            if artifact.name == *replacing_recipe {
                continue;
            }
            if !seen_names.insert(artifact.name.clone()) {
                return Err(AppError::FailedPrecondition(format!(
                    "recipe '{}' is installed more than once",
                    artifact.name
                )));
            }
            let spec = parse_recipe_yaml(&artifact.raw_yaml).map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "installed recipe '{}' is invalid: {error}",
                    artifact.name
                ))
            })?;
            let runtime_recipe = runtime_recipe_without_columns(&spec);
            record_publish_targets(&runtime_recipe, publish_targets)?;
        }
        Ok(())
    }

    fn cached_recipe_result_columns(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Vec<RecipeRuntimeResultColumn> {
        let metadata_file = self.layout.recipe_runtime_file(workspace_name, recipe_name);
        let raw = match std::fs::read_to_string(&metadata_file) {
            Ok(raw) => raw,
            Err(error) if error.kind() == ErrorKind::NotFound => return Vec::new(),
            Err(error) => {
                tracing::warn!(
                    recipe = %recipe_name,
                    path = %metadata_file.display(),
                    detail = %error,
                    "ignoring recipe runtime metadata because it could not be read"
                );
                return Vec::new();
            }
        };
        match serde_json::from_str::<RecipeRuntimeMetadata>(&raw) {
            Ok(metadata) => metadata
                .result_columns
                .into_iter()
                .map(RecipeRuntimeResultColumn::from)
                .collect(),
            Err(error) => {
                tracing::warn!(
                    recipe = %recipe_name,
                    path = %metadata_file.display(),
                    detail = %error,
                    "ignoring recipe runtime metadata because it is invalid"
                );
                Vec::new()
            }
        }
    }
}

fn skip_recipe(artifact: &RecipeArtifact, detail: fmt::Arguments<'_>) {
    tracing::warn!(
        recipe = %artifact.name,
        origin = ?artifact.origin,
        detail = %detail,
        "skipping recipe during runtime publication"
    );
}

async fn source_publish_targets(
    selected_sources: &[QuerySource],
    runtime_config: QueryRuntimeConfig,
) -> Result<HashSet<PublishTarget>, AppError> {
    let catalog = CoralQuery::list_catalog(selected_sources, runtime_config, None)
        .await
        .map_err(|error| {
            AppError::FailedPrecondition(format!(
                "failed to inspect installed source catalog for recipe publish collisions: {error}"
            ))
        })?;
    let mut targets = HashSet::new();
    targets.extend(
        catalog
            .tables
            .into_iter()
            .map(|table| PublishTarget::sql_relation(&table.schema_name, &table.table_name)),
    );
    targets.extend(catalog.table_functions.into_iter().map(|function| {
        PublishTarget::sql_relation(&function.schema_name, &function.function_name)
    }));
    Ok(targets)
}

fn runtime_recipe_without_columns(spec: &RecipeSpec) -> RecipeRuntimeDefinition {
    runtime_recipe_with_result_columns(spec, Vec::new())
}

async fn infer_runtime_recipe(
    selected_sources: &[QuerySource],
    runtime_config: QueryRuntimeConfig,
    spec: &RecipeSpec,
) -> Result<RecipeRuntimeDefinition, AppError> {
    let runtime_recipe = runtime_recipe_without_columns(spec);
    let schema = CoralQuery::infer_recipe_schema(selected_sources, runtime_config, runtime_recipe)
        .await
        .map_err(|error| {
            AppError::FailedPrecondition(format!("recipe failed runtime validation: {error}"))
        })?;
    Ok(runtime_recipe_with_schema(spec, schema.as_ref()))
}

fn runtime_recipe_with_schema(
    spec: &RecipeSpec,
    schema: &arrow::datatypes::Schema,
) -> RecipeRuntimeDefinition {
    let result_columns = schema
        .fields()
        .iter()
        .map(|field| RecipeRuntimeResultColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
            description: String::new(),
        })
        .collect();
    runtime_recipe_with_result_columns(spec, result_columns)
}

fn runtime_recipe_with_result_columns(
    spec: &RecipeSpec,
    result_columns: Vec<RecipeRuntimeResultColumn>,
) -> RecipeRuntimeDefinition {
    RecipeRuntimeDefinition {
        name: spec.name().to_string(),
        description: spec.description().to_string(),
        arguments: runtime_arguments(spec),
        implementation: runtime_implementation(spec.implementation()),
        publish: runtime_publish(spec.publish()),
        result_columns,
    }
}

fn runtime_arguments(spec: &RecipeSpec) -> Vec<RecipeRuntimeArgument> {
    spec.arguments()
        .iter()
        .map(|argument| RecipeRuntimeArgument {
            name: argument.name.clone(),
            data_type: match argument.data_type {
                RecipeArgumentType::String => RecipeRuntimeArgumentType::String,
                RecipeArgumentType::Integer => RecipeRuntimeArgumentType::Integer,
                RecipeArgumentType::Boolean => RecipeRuntimeArgumentType::Boolean,
            },
            required: argument.required,
            description: argument.description.clone(),
        })
        .collect()
}

fn runtime_implementation(spec: &RecipeImplementationSpec) -> RecipeRuntimeImplementation {
    match spec {
        RecipeImplementationSpec::CoralSql { query } => RecipeRuntimeImplementation::CoralSql {
            query: query.clone(),
        },
    }
}

fn runtime_publish(specs: &[RecipePublishSpec]) -> Vec<RecipeRuntimePublish> {
    specs
        .iter()
        .map(|spec| match spec {
            RecipePublishSpec::TableFunction {
                schema,
                name,
                description,
            } => RecipeRuntimePublish::TableFunction {
                schema: schema.clone(),
                name: name.clone(),
                description: description.clone(),
            },
            RecipePublishSpec::McpTool { name, description } => RecipeRuntimePublish::McpTool {
                name: name.clone(),
                description: description.clone(),
            },
        })
        .collect()
}

fn record_publish_targets(
    recipe: &RecipeRuntimeDefinition,
    publish_targets: &mut HashSet<PublishTarget>,
) -> Result<(), AppError> {
    let mut recipe_targets = HashSet::new();
    for publish in &recipe.publish {
        let target = match publish {
            RecipeRuntimePublish::TableFunction { schema, name, .. } => {
                PublishTarget::sql_relation(schema, name)
            }
            RecipeRuntimePublish::McpTool { .. } => continue,
        };
        if publish_targets.contains(&target) || !recipe_targets.insert(target.clone()) {
            return Err(AppError::FailedPrecondition(format!(
                "recipe publish target '{}' is installed more than once",
                target.display_name()
            )));
        }
    }
    publish_targets.extend(recipe_targets);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PublishTarget {
    SqlRelation { schema: String, name: String },
}

impl PublishTarget {
    fn sql_relation(schema: &str, name: &str) -> Self {
        Self::SqlRelation {
            schema: schema.to_ascii_lowercase(),
            name: name.to_ascii_lowercase(),
        }
    }

    fn display_name(&self) -> String {
        match self {
            Self::SqlRelation { schema, name } => format!("{schema}.{name}"),
        }
    }
}

fn origin_sort_key(origin: RecipeOrigin) -> u8 {
    match origin {
        RecipeOrigin::Bundled => 0,
        RecipeOrigin::User => 1,
    }
}

fn write_recipe_runtime_metadata(
    recipe_runtime_file: &std::path::Path,
    runtime_recipe: Option<&RecipeRuntimeDefinition>,
) -> Result<(), AppError> {
    if let Some(runtime_recipe) = runtime_recipe {
        let metadata = RecipeRuntimeMetadata {
            version: 1,
            result_columns: runtime_recipe
                .result_columns
                .iter()
                .map(CachedRecipeResultColumn::from)
                .collect(),
        };
        let raw = serde_json::to_vec_pretty(&metadata)?;
        fs::write_atomic(recipe_runtime_file, &raw)?;
    } else if recipe_runtime_file.exists() {
        std::fs::remove_file(recipe_runtime_file)?;
    }
    Ok(())
}

fn rollback_recipe_install(
    recipe_dir: &std::path::Path,
    recipe_file: &std::path::Path,
    previous_yaml: Option<&[u8]>,
    recipe_runtime_file: &std::path::Path,
    previous_runtime: Option<&[u8]>,
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
        rollback_recipe_runtime_file(recipe_runtime_file, previous_runtime);
        if let Err(error) = fs::write_atomic(recipe_file, previous_yaml) {
            tracing::warn!(
                path = %recipe_file.display(),
                detail = %error,
                "failed to restore previous recipe file after config update failure"
            );
        }
    } else if previous_runtime.is_some() {
        rollback_recipe_runtime_file(recipe_runtime_file, previous_runtime);
    } else if let Err(error) = std::fs::remove_dir_all(recipe_dir) {
        tracing::warn!(
            path = %recipe_dir.display(),
            detail = %error,
            "failed to remove new recipe directory after config update failure"
        );
    }
}

fn rollback_recipe_runtime_file(
    recipe_runtime_file: &std::path::Path,
    previous_runtime: Option<&[u8]>,
) {
    if let Some(previous_runtime) = previous_runtime {
        if let Err(error) = fs::write_atomic(recipe_runtime_file, previous_runtime) {
            tracing::warn!(
                path = %recipe_runtime_file.display(),
                detail = %error,
                "failed to restore previous recipe runtime metadata after config update failure"
            );
        }
    } else if recipe_runtime_file.exists()
        && let Err(error) = std::fs::remove_file(recipe_runtime_file)
    {
        tracing::warn!(
            path = %recipe_runtime_file.display(),
            detail = %error,
            "failed to remove recipe runtime metadata after config update failure"
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
        recipe_yaml_with_publish(name, &format!("recipes.{name}"))
    }

    fn recipe_yaml_with_publish(name: &str, publish_target: &str) -> String {
        format!(
            r"
kind: recipe
name: {name}
implementation:
  kind: coral_sql
  query: select 1 as id
publish:
  - table_function: {publish_target}
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

    #[tokio::test]
    async fn install_validated_user_recipe_caches_result_columns_for_list() {
        let (_temp, layout, manager) = fixture();
        let workspace = workspace();
        let raw_yaml = recipe_yaml("review_queue");
        let runtime_recipe = manager
            .validate_user_recipe_yaml(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &raw_yaml,
            )
            .await
            .expect("validate recipe");

        manager
            .install_validated_user_recipe(&workspace, &raw_yaml, &runtime_recipe)
            .expect("install validated recipe");

        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        assert!(
            layout
                .recipe_runtime_file(&workspace, &recipe_name)
                .exists()
        );
        let listed = manager.list_recipes(&workspace).expect("list recipes");
        assert_eq!(listed.len(), 1);
        let listed_recipe = listed.first().expect("listed recipe");
        assert_eq!(listed_recipe.result_columns.len(), 1);
        let column = listed_recipe
            .result_columns
            .first()
            .expect("id result column");
        assert_eq!(column.name, "id");
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

    #[tokio::test]
    async fn validate_user_recipe_yaml_infers_result_columns() {
        let (_temp, _layout, manager) = fixture();
        let workspace = workspace();

        let recipe = manager
            .validate_user_recipe_yaml(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &recipe_yaml("review_queue"),
            )
            .await
            .expect("validate recipe");

        assert_eq!(recipe.name, "review_queue");
        assert_eq!(recipe.result_columns.len(), 1);
        let column = recipe.result_columns.first().expect("id result column");
        assert_eq!(column.name, "id");
    }

    #[tokio::test]
    async fn validate_user_recipe_yaml_rejects_installed_publish_collision() {
        let (_temp, _layout, manager) = fixture();
        let workspace = workspace();
        manager
            .install_user_recipe(
                &workspace,
                &recipe_yaml_with_publish("existing_queue", "recipes.shared_queue"),
            )
            .expect("install existing recipe");

        let error = manager
            .validate_user_recipe_yaml(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &recipe_yaml_with_publish("new_queue", "recipes.shared_queue"),
            )
            .await
            .expect_err("collision should fail validation");

        assert!(matches!(
            error,
            AppError::FailedPrecondition(message) if message.contains("recipes.shared_queue")
        ));
    }
}
