//! Owns user-installed recipe files and workspace inventory.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, RecipeRuntimeArgument, RecipeRuntimeArgumentType,
    RecipeRuntimeArgumentValue, RecipeRuntimeDefinition, RecipeRuntimeImplementation,
    RecipeRuntimeMcpToolPublish, RecipeRuntimePublish, RecipeRuntimeResultColumn,
    RecipeRuntimeTableFunctionPublish, RuntimeSourceComponent,
};
use coral_spec::{
    RecipeArgumentType, RecipeImplementationSpec, RecipePublishSpec, RecipeSpec,
    RecipeValidationValue, parse_recipe_yaml,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::bootstrap::AppError;
use crate::recipes::model::{InstalledRecipe, RecipeName};
use crate::recipes::storage::{
    ConfigRecipeRegistry, FsRecipeArtifactStore, RecipeArtifactStore, RecipeRegistry,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

const RECIPE_RUNTIME_METADATA_VERSION: u32 = 2;

#[derive(Clone)]
pub(crate) struct RecipeManager {
    registry: Arc<dyn RecipeRegistry>,
    artifacts: Arc<dyn RecipeArtifactStore>,
}

struct RecipeArtifact {
    name: RecipeName,
    raw_yaml: String,
}

/// One recipe as listed by the app inventory surface.
pub(crate) struct RecipeListing {
    /// Runtime definition for the recipe.
    pub(crate) definition: RecipeRuntimeDefinition,
    /// Optional MCP tool presentation for this recipe.
    pub(crate) mcp: Option<RecipeMcpPresentation>,
}

/// App-owned MCP presentation metadata for one recipe.
#[derive(Debug, Clone)]
pub(crate) struct RecipeMcpPresentation {
    pub(crate) name: String,
    pub(crate) description: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecipeRuntimeMetadata {
    version: u32,
    #[serde(default)]
    recipe_yaml_sha256: String,
    #[serde(default)]
    validation_args: BTreeMap<String, CachedRecipeArgumentValue>,
    result_columns: Vec<CachedRecipeResultColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum CachedRecipeArgumentValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Null(()),
}

impl From<&RecipeRuntimeArgumentValue> for CachedRecipeArgumentValue {
    fn from(value: &RecipeRuntimeArgumentValue) -> Self {
        match value {
            RecipeRuntimeArgumentValue::String(value) => Self::String(value.clone()),
            RecipeRuntimeArgumentValue::Integer(value) => Self::Integer(*value),
            RecipeRuntimeArgumentValue::Boolean(value) => Self::Boolean(*value),
            RecipeRuntimeArgumentValue::Null => Self::Null(()),
        }
    }
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
        Self::with_stores(
            Arc::new(ConfigRecipeRegistry::new(config_store)),
            Arc::new(FsRecipeArtifactStore::new(layout)),
        )
    }

    pub(crate) fn with_stores(
        registry: Arc<dyn RecipeRegistry>,
        artifacts: Arc<dyn RecipeArtifactStore>,
    ) -> Self {
        Self {
            registry,
            artifacts,
        }
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
        let validation_arguments = runtime_validation_arguments(&recipe);
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
            runtime_recipe,
            &validation_arguments,
        )
    }

    fn install_user_recipe_artifact(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        runtime_recipe: &RecipeRuntimeDefinition,
        validation_arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
    ) -> Result<InstalledRecipe, AppError> {
        let installed = InstalledRecipe {
            name: recipe_name.clone(),
        };

        let runtime_metadata =
            encode_recipe_runtime_metadata(runtime_recipe, raw_yaml, validation_arguments)?;
        let previous_artifact = self.artifacts.write_user_recipe_artifact(
            workspace_name,
            recipe_name,
            raw_yaml,
            &runtime_metadata,
        )?;
        if let Err(error) = self
            .registry
            .upsert_recipe(workspace_name, installed.clone())
        {
            if let Err(restore_error) = self.artifacts.restore_user_recipe_artifact(
                workspace_name,
                recipe_name,
                &previous_artifact,
            ) {
                tracing::warn!(
                    recipe = %recipe_name,
                    detail = %restore_error,
                    "failed to restore previous recipe artifact after inventory update failure"
                );
            }
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
        let mut publish_targets = source_publish_targets(selected_sources);
        self.record_installed_recipe_publish_targets(
            workspace_name,
            &recipe_name,
            &mut publish_targets,
        )?;
        let mut mcp_publish_targets = HashSet::new();
        self.record_installed_recipe_mcp_publish_targets(
            workspace_name,
            &recipe_name,
            &mut mcp_publish_targets,
        )?;
        let runtime_recipe =
            validate_runtime_recipe(selected_sources, runtime_config()?, &spec).await?;
        record_publish_targets(&runtime_recipe, &mut publish_targets)?;
        record_mcp_publish_target(spec.publish(), &mut mcp_publish_targets)?;
        Ok(runtime_recipe)
    }

    pub(crate) fn list_recipes(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<RecipeListing>, AppError> {
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
            recipe.result_columns = self.cached_recipe_result_columns(
                workspace_name,
                &artifact.name,
                &artifact.raw_yaml,
                &runtime_validation_arguments(&spec),
            );
            recipes.push(RecipeListing {
                mcp: recipe_mcp_presentation(&spec),
                definition: recipe,
            });
        }
        Ok(recipes)
    }

    pub(crate) fn load_runtime_recipes(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Result<Vec<RecipeRuntimeDefinition>, AppError> {
        Ok(self
            .list_runtime_recipe_listings(workspace_name, selected_sources)?
            .into_iter()
            .map(|listing| listing.definition)
            .collect())
    }

    pub(crate) fn list_recipe_mcp_tools(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Result<Vec<RecipeListing>, AppError> {
        let runtime_recipes =
            self.list_runtime_recipe_listings(workspace_name, selected_sources)?;
        Ok(filter_presentable_recipe_mcp_tools(runtime_recipes))
    }

    fn list_runtime_recipe_listings(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Result<Vec<RecipeListing>, AppError> {
        let artifacts = self.load_recipe_artifacts(workspace_name)?;
        if artifacts.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen_names = HashSet::new();
        let mut publish_targets = source_publish_targets(selected_sources);
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
            let mut runtime_recipe = runtime_recipe_without_columns(&spec);
            runtime_recipe.result_columns = self.cached_recipe_result_columns(
                workspace_name,
                &artifact.name,
                &artifact.raw_yaml,
                &runtime_validation_arguments(&spec),
            );
            if runtime_recipe.result_columns.is_empty() {
                skip_recipe(
                    &artifact,
                    format_args!("recipe is missing cached runtime metadata; re-add the recipe"),
                );
                continue;
            }
            if let Err(error) = record_publish_targets(&runtime_recipe, &mut publish_targets) {
                skip_recipe(&artifact, format_args!("{error}"));
                continue;
            }
            runtime_recipes.push(RecipeListing {
                mcp: recipe_mcp_presentation(&spec),
                definition: runtime_recipe,
            });
        }
        Ok(runtime_recipes)
    }

    pub(crate) fn remove_user_recipe(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
    ) -> Result<(), AppError> {
        self.registry.get_recipe(workspace_name, recipe_name)?;
        let removed_artifact = self
            .artifacts
            .remove_user_recipe_artifact(workspace_name, recipe_name)?;
        if let Err(error) = self.registry.remove_recipe(workspace_name, recipe_name) {
            if let Err(restore_error) = self.artifacts.restore_user_recipe_artifact(
                workspace_name,
                recipe_name,
                &removed_artifact,
            ) {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove recipe '{recipe_name}': {error}; failed to restore recipe artifact: {restore_error}"
                )));
            }
            return Err(error);
        }
        Ok(())
    }

    fn load_recipe_artifacts(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<RecipeArtifact>, AppError> {
        let mut artifacts = Vec::new();
        for installed in self.registry.list_workspace_recipes(workspace_name)? {
            let raw_yaml = match self
                .artifacts
                .read_recipe_yaml(workspace_name, &installed.name)
            {
                Ok(Some(raw_yaml)) => raw_yaml,
                Ok(None) => {
                    tracing::warn!(
                        recipe = %installed.name,
                        "skipping installed recipe because its recipe file is missing"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        recipe = %installed.name,
                        detail = %error,
                        "skipping installed recipe because its recipe file could not be read"
                    );
                    continue;
                }
            };
            artifacts.push(RecipeArtifact {
                name: installed.name,
                raw_yaml,
            });
        }

        artifacts.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
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

    fn record_installed_recipe_mcp_publish_targets(
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
            if let Some(mcp) = &spec.publish().mcp {
                publish_targets.insert(PublishTarget::mcp_tool(&mcp.name));
            }
        }
        Ok(())
    }

    fn cached_recipe_result_columns(
        &self,
        workspace_name: &WorkspaceName,
        recipe_name: &RecipeName,
        raw_yaml: &str,
        validation_arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
    ) -> Vec<RecipeRuntimeResultColumn> {
        let raw = match self
            .artifacts
            .read_runtime_metadata(workspace_name, recipe_name)
        {
            Ok(Some(raw)) => raw,
            Ok(None) => return Vec::new(),
            Err(error) => {
                tracing::warn!(
                    recipe = %recipe_name,
                    detail = %error,
                    "ignoring recipe runtime metadata because it could not be read"
                );
                return Vec::new();
            }
        };
        match serde_json::from_str::<RecipeRuntimeMetadata>(&raw) {
            Ok(metadata) if metadata.version == RECIPE_RUNTIME_METADATA_VERSION => {
                let expected_recipe_yaml_sha256 = sha256_hex(raw_yaml.as_bytes());
                if metadata.recipe_yaml_sha256 != expected_recipe_yaml_sha256 {
                    tracing::warn!(
                        recipe = %recipe_name,
                        "ignoring recipe runtime metadata because recipe YAML changed"
                    );
                    return Vec::new();
                }
                let expected_args = cached_validation_arguments(validation_arguments);
                if metadata.validation_args != expected_args {
                    tracing::warn!(
                        recipe = %recipe_name,
                        "ignoring recipe runtime metadata because validation arguments changed"
                    );
                    return Vec::new();
                }
                metadata
                    .result_columns
                    .into_iter()
                    .map(RecipeRuntimeResultColumn::from)
                    .collect()
            }
            Ok(metadata) => {
                tracing::warn!(
                    recipe = %recipe_name,
                    version = metadata.version,
                    supported_version = RECIPE_RUNTIME_METADATA_VERSION,
                    "ignoring recipe runtime metadata because its version is unsupported"
                );
                Vec::new()
            }
            Err(error) => {
                tracing::warn!(
                    recipe = %recipe_name,
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
        detail = %detail,
        "skipping recipe during runtime publication"
    );
}

fn filter_presentable_recipe_mcp_tools(recipes: Vec<RecipeListing>) -> Vec<RecipeListing> {
    let mut target_counts = HashMap::new();
    for listing in &recipes {
        if let Some(mcp) = &listing.mcp {
            let target = PublishTarget::mcp_tool(&mcp.name);
            *target_counts.entry(target).or_insert(0usize) += 1;
        }
    }

    recipes
        .into_iter()
        .filter_map(|listing| {
            let Some(mcp) = &listing.mcp else {
                return None;
            };
            let target = PublishTarget::mcp_tool(&mcp.name);
            if target_counts.get(&target).copied() == Some(1) {
                return Some(listing);
            }
            tracing::warn!(
                recipe = %listing.definition.name,
                tool = %mcp.name,
                "skipping recipe MCP tool because its publish target is installed more than once"
            );
            None
        })
        .collect()
}

fn source_publish_targets(selected_sources: &[QuerySource]) -> HashSet<PublishTarget> {
    let mut targets = HashSet::new();
    for source in selected_sources {
        for component in source.components() {
            record_source_component_targets(component, &mut targets);
        }
    }
    targets
}

fn record_source_component_targets(
    component: &RuntimeSourceComponent,
    targets: &mut HashSet<PublishTarget>,
) {
    match component {
        RuntimeSourceComponent::Http(manifest) => {
            for table in &manifest.tables {
                targets.insert(PublishTarget::sql_relation(
                    &manifest.common.name,
                    table.name(),
                ));
            }
            for function in &manifest.functions {
                targets.insert(PublishTarget::sql_relation(
                    &manifest.common.name,
                    &function.name,
                ));
            }
        }
        RuntimeSourceComponent::File(manifest) => {
            for table in &manifest.tables {
                targets.insert(PublishTarget::sql_relation(
                    &manifest.common.name,
                    table.name(),
                ));
            }
        }
        RuntimeSourceComponent::Mcp(manifest) => {
            for table in &manifest.tables {
                targets.insert(PublishTarget::sql_relation(
                    &manifest.common.name,
                    &table.common.name,
                ));
            }
            for function in &manifest.functions {
                targets.insert(PublishTarget::sql_relation(
                    &manifest.common.name,
                    &function.common.name,
                ));
            }
        }
    }
}

fn runtime_recipe_without_columns(spec: &RecipeSpec) -> RecipeRuntimeDefinition {
    runtime_recipe_with_result_columns(spec, Vec::new())
}

async fn validate_runtime_recipe(
    selected_sources: &[QuerySource],
    runtime_config: QueryRuntimeConfig,
    spec: &RecipeSpec,
) -> Result<RecipeRuntimeDefinition, AppError> {
    let runtime_recipe = runtime_recipe_without_columns(spec);
    let schema = CoralQuery::validate_recipe(
        selected_sources,
        runtime_config,
        runtime_recipe,
        runtime_validation_arguments(spec),
    )
    .await
    .map_err(|error| {
        AppError::FailedPrecondition(format!("recipe failed runtime validation: {error}"))
    })?;
    Ok(runtime_recipe_with_schema(spec, schema.as_ref()))
}

fn runtime_validation_arguments(spec: &RecipeSpec) -> BTreeMap<String, RecipeRuntimeArgumentValue> {
    spec.validation()
        .args
        .iter()
        .map(|(name, value)| (name.clone(), runtime_validation_argument_value(value)))
        .collect()
}

fn runtime_validation_argument_value(value: &RecipeValidationValue) -> RecipeRuntimeArgumentValue {
    match value {
        RecipeValidationValue::String(value) => RecipeRuntimeArgumentValue::String(value.clone()),
        RecipeValidationValue::Integer(value) => RecipeRuntimeArgumentValue::Integer(*value),
        RecipeValidationValue::Boolean(value) => RecipeRuntimeArgumentValue::Boolean(*value),
        RecipeValidationValue::Null(()) => RecipeRuntimeArgumentValue::Null,
    }
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

fn runtime_publish(spec: &RecipePublishSpec) -> RecipeRuntimePublish {
    RecipeRuntimePublish {
        table_function: RecipeRuntimeTableFunctionPublish {
            schema: spec.table_function.schema.clone(),
            name: spec.table_function.name.clone(),
            description: spec.table_function.description.clone(),
        },
        mcp: spec.mcp.as_ref().map(|mcp| RecipeRuntimeMcpToolPublish {
            name: mcp.name.clone(),
            description: mcp.description.clone(),
        }),
    }
}

fn recipe_mcp_presentation(spec: &RecipeSpec) -> Option<RecipeMcpPresentation> {
    spec.publish()
        .mcp
        .as_ref()
        .map(|mcp| RecipeMcpPresentation {
            name: mcp.name.clone(),
            description: mcp.description.clone(),
        })
}

fn record_publish_targets(
    recipe: &RecipeRuntimeDefinition,
    publish_targets: &mut HashSet<PublishTarget>,
) -> Result<(), AppError> {
    let mut recipe_targets = HashSet::new();
    let target = PublishTarget::sql_relation(
        &recipe.publish.table_function.schema,
        &recipe.publish.table_function.name,
    );
    if publish_targets.contains(&target) || !recipe_targets.insert(target.clone()) {
        return Err(AppError::FailedPrecondition(format!(
            "recipe publish target '{}' is installed more than once",
            target.display_name()
        )));
    }
    publish_targets.extend(recipe_targets);
    Ok(())
}

fn record_mcp_publish_target(
    publish: &RecipePublishSpec,
    publish_targets: &mut HashSet<PublishTarget>,
) -> Result<(), AppError> {
    if let Some(mcp) = &publish.mcp {
        let target = PublishTarget::mcp_tool(&mcp.name);
        if !publish_targets.insert(target.clone()) {
            return Err(AppError::FailedPrecondition(format!(
                "recipe publish target '{}' is installed more than once",
                target.display_name()
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PublishTarget {
    SqlRelation { schema: String, name: String },
    McpTool { name: String },
}

impl PublishTarget {
    fn sql_relation(schema: &str, name: &str) -> Self {
        Self::SqlRelation {
            schema: schema.to_ascii_lowercase(),
            name: name.to_ascii_lowercase(),
        }
    }

    fn mcp_tool(name: &str) -> Self {
        Self::McpTool {
            name: name.to_ascii_lowercase(),
        }
    }

    fn display_name(&self) -> String {
        match self {
            Self::SqlRelation { schema, name } => format!("{schema}.{name}"),
            Self::McpTool { name } => format!("mcp tool {name}"),
        }
    }
}

fn encode_recipe_runtime_metadata(
    runtime_recipe: &RecipeRuntimeDefinition,
    raw_yaml: &str,
    validation_arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<Vec<u8>, AppError> {
    let metadata = RecipeRuntimeMetadata {
        version: RECIPE_RUNTIME_METADATA_VERSION,
        recipe_yaml_sha256: sha256_hex(raw_yaml.as_bytes()),
        validation_args: cached_validation_arguments(validation_arguments),
        result_columns: runtime_recipe
            .result_columns
            .iter()
            .map(CachedRecipeResultColumn::from)
            .collect(),
    };
    serde_json::to_vec_pretty(&metadata).map_err(AppError::from)
}

fn cached_validation_arguments(
    validation_arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> BTreeMap<String, CachedRecipeArgumentValue> {
    validation_arguments
        .iter()
        .map(|(name, value)| (name.clone(), CachedRecipeArgumentValue::from(value)))
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::state::AppStateLayout;
    use crate::storage::fs;

    fn fixture() -> (TempDir, AppStateLayout, ConfigStore, RecipeManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let manager = RecipeManager::new(config_store.clone(), layout.clone());
        (temp, layout, config_store, manager)
    }

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn recipe_yaml(name: &str) -> String {
        recipe_yaml_with_publish(name, &format!("recipes.{name}"))
    }

    fn recipe_yaml_with_validation_owner(name: &str, owner: &str) -> String {
        format!(
            r"
kind: recipe
name: {name}
inputs:
  owner:
    type: string
    required: true
implementation:
  kind: coral_sql
  query: select $owner as owner
validation:
  args:
    owner: {owner}
publish:
  table_function:
    schema: recipes
    name: {name}
"
        )
    }

    fn recipe_yaml_with_publish(name: &str, publish_target: &str) -> String {
        let (schema, function) = publish_target
            .split_once('.')
            .expect("publish target should be schema.name");
        format!(
            r"
kind: recipe
name: {name}
implementation:
  kind: coral_sql
  query: select 1 as id
publish:
  table_function:
    schema: {schema}
    name: {function}
"
        )
    }

    fn recipe_yaml_with_publish_and_mcp(
        name: &str,
        publish_target: &str,
        mcp_tool: &str,
    ) -> String {
        let (schema, function) = publish_target
            .split_once('.')
            .expect("publish target should be schema.name");
        format!(
            r"
kind: recipe
name: {name}
implementation:
  kind: coral_sql
  query: select 1 as id
publish:
  table_function:
    schema: {schema}
    name: {function}
  mcp:
    name: {mcp_tool}
"
        )
    }

    fn validated_recipe(raw_yaml: &str) -> RecipeRuntimeDefinition {
        let spec = parse_recipe_yaml(raw_yaml).expect("recipe spec");
        runtime_recipe_with_result_columns(
            &spec,
            vec![RecipeRuntimeResultColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                description: String::new(),
            }],
        )
    }

    fn install_fixture_recipe(
        manager: &RecipeManager,
        workspace: &WorkspaceName,
        raw_yaml: &str,
    ) -> InstalledRecipe {
        let runtime_recipe = validated_recipe(raw_yaml);
        manager
            .install_validated_user_recipe(workspace, raw_yaml, &runtime_recipe)
            .expect("install recipe")
    }

    #[test]
    fn list_recipes_ignores_cached_columns_when_validation_args_change() {
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_yaml = recipe_yaml_with_validation_owner("review_queue", "withcoral");
        install_fixture_recipe(&manager, &workspace, &raw_yaml);
        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        std::fs::write(
            layout.recipe_file(&workspace, &recipe_name),
            recipe_yaml_with_validation_owner("review_queue", "other_org"),
        )
        .expect("rewrite recipe yaml");

        let listed = manager.list_recipes(&workspace).expect("list recipes");

        assert_eq!(listed.len(), 1);
        let listed_recipe = listed.first().expect("listed recipe");
        assert!(
            listed_recipe.definition.result_columns.is_empty(),
            "cached result columns should not survive validation argument drift"
        );
    }

    #[test]
    fn list_recipes_ignores_cached_columns_when_recipe_yaml_changes() {
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_yaml = recipe_yaml_with_validation_owner("review_queue", "withcoral");
        install_fixture_recipe(&manager, &workspace, &raw_yaml);
        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        let changed_yaml = raw_yaml.replace(
            "query: select $owner as owner",
            "query: select $owner as reviewer",
        );
        std::fs::write(layout.recipe_file(&workspace, &recipe_name), changed_yaml)
            .expect("rewrite recipe yaml");

        let listed = manager.list_recipes(&workspace).expect("list recipes");

        assert_eq!(listed.len(), 1);
        let listed_recipe = listed.first().expect("listed recipe");
        assert!(
            listed_recipe.definition.result_columns.is_empty(),
            "cached result columns should not survive authored recipe drift"
        );
    }

    #[test]
    fn list_recipes_ignores_unsupported_runtime_metadata_version() {
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_yaml = recipe_yaml("review_queue");
        install_fixture_recipe(&manager, &workspace, &raw_yaml);
        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        std::fs::write(
            layout.recipe_runtime_file(&workspace, &recipe_name),
            r#"
{
  "version": 999,
  "result_columns": [
    {"name": "id", "data_type": "Int64", "nullable": false}
  ]
}
"#,
        )
        .expect("write unsupported runtime metadata");

        let listed = manager.list_recipes(&workspace).expect("list recipes");

        assert_eq!(listed.len(), 1);
        let listed_recipe = listed.first().expect("listed recipe");
        assert!(
            listed_recipe.definition.result_columns.is_empty(),
            "unsupported cached runtime metadata should not drive list output"
        );
    }

    #[tokio::test]
    async fn validate_user_recipe_yaml_rejects_installed_publish_collision() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let existing_yaml = recipe_yaml_with_publish("existing_queue", "recipes.shared_queue");
        manager
            .install_validated_user_recipe(
                &workspace,
                &existing_yaml,
                &validated_recipe(&existing_yaml),
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

    #[tokio::test]
    async fn validate_user_recipe_yaml_rejects_installed_mcp_publish_collision() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let existing_yaml = recipe_yaml_with_publish_and_mcp(
            "existing_queue",
            "recipes.existing_queue",
            "shared_queue",
        );
        manager
            .install_validated_user_recipe(
                &workspace,
                &existing_yaml,
                &validated_recipe(&existing_yaml),
            )
            .expect("install existing recipe");

        let error = manager
            .validate_user_recipe_yaml(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &recipe_yaml_with_publish_and_mcp("new_queue", "recipes.new_queue", "shared_queue"),
            )
            .await
            .expect_err("mcp collision should fail validation");

        assert!(matches!(
            error,
            AppError::FailedPrecondition(message) if message.contains("mcp tool shared_queue")
        ));
    }

    #[tokio::test]
    async fn validate_user_recipe_yaml_tolerates_legacy_duplicate_mcp_publish_targets() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let first_yaml =
            recipe_yaml_with_publish_and_mcp("first_queue", "recipes.first_queue", "shared_queue");
        let second_yaml = recipe_yaml_with_publish_and_mcp(
            "second_queue",
            "recipes.second_queue",
            "shared_queue",
        );
        install_fixture_recipe(&manager, &workspace, &first_yaml);
        install_fixture_recipe(&manager, &workspace, &second_yaml);

        let runtime_recipe = manager
            .validate_user_recipe_yaml(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &recipe_yaml_with_publish_and_mcp("new_queue", "recipes.new_queue", "new_queue"),
            )
            .await
            .expect("unrelated MCP target should validate");

        assert_eq!(runtime_recipe.name, "new_queue");
    }

    #[test]
    fn load_runtime_recipes_allows_duplicate_mcp_names_when_sql_targets_are_unique() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let first_yaml =
            recipe_yaml_with_publish_and_mcp("first_queue", "recipes.first_queue", "shared_queue");
        let second_yaml = recipe_yaml_with_publish_and_mcp(
            "second_queue",
            "recipes.second_queue",
            "shared_queue",
        );
        install_fixture_recipe(&manager, &workspace, &first_yaml);
        install_fixture_recipe(&manager, &workspace, &second_yaml);

        let runtime_recipes = manager
            .load_runtime_recipes(&workspace, &[])
            .expect("load runtime recipes");

        assert_eq!(
            runtime_recipes
                .iter()
                .map(|recipe| recipe.name.as_str())
                .collect::<Vec<_>>(),
            vec!["first_queue", "second_queue"]
        );
    }

    #[test]
    fn list_recipe_mcp_tools_drops_duplicate_mcp_names() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let first_yaml =
            recipe_yaml_with_publish_and_mcp("first_queue", "recipes.first_queue", "shared_queue");
        let second_yaml = recipe_yaml_with_publish_and_mcp(
            "second_queue",
            "recipes.second_queue",
            "shared_queue",
        );
        install_fixture_recipe(&manager, &workspace, &first_yaml);
        install_fixture_recipe(&manager, &workspace, &second_yaml);

        let mcp_tools = manager
            .list_recipe_mcp_tools(&workspace, &[])
            .expect("list recipe MCP tools");

        assert!(
            mcp_tools.is_empty(),
            "duplicate MCP tool names should not expose an arbitrary recipe"
        );
    }

    #[test]
    fn artifact_restore_removes_new_yaml_when_only_runtime_metadata_existed() {
        let (_temp, layout, _config_store, _manager) = fixture();
        let workspace = workspace();
        let recipe_name = RecipeName::parse("review_queue").expect("recipe name");
        let recipe_dir = layout.recipe_dir(&workspace, &recipe_name);
        let recipe_file = layout.recipe_file(&workspace, &recipe_name);
        let recipe_runtime_file = layout.recipe_runtime_file(&workspace, &recipe_name);
        fs::ensure_private_dir(&recipe_dir).expect("recipe dir");
        std::fs::write(&recipe_runtime_file, b"previous runtime").expect("previous runtime");
        let store = FsRecipeArtifactStore::new(layout.clone());
        let previous = store
            .write_user_recipe_artifact(&workspace, &recipe_name, "new yaml", b"new runtime")
            .expect("write new artifact");

        store
            .restore_user_recipe_artifact(&workspace, &recipe_name, &previous)
            .expect("restore artifact");

        assert!(
            !recipe_file.exists(),
            "restore should remove newly written recipe yaml"
        );
        assert_eq!(
            std::fs::read(&recipe_runtime_file).expect("restored runtime metadata"),
            b"previous runtime"
        );
    }
}
