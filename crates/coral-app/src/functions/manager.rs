//! Owns user-installed function files and workspace inventory.

use std::sync::Arc;

use coral_engine::UdfRuntimeDefinition;
use coral_spec::parse_function_sql;

use crate::bootstrap::AppError;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::functions::storage::{
    ConfigFunctionRegistry, FsFunctionArtifactStore, FunctionArtifactStore, FunctionRegistry,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct FunctionManager {
    registry: Arc<dyn FunctionRegistry>,
    artifacts: Arc<dyn FunctionArtifactStore>,
}

impl FunctionManager {
    pub(crate) fn new(config_store: ConfigStore, layout: &AppStateLayout) -> Self {
        Self::with_stores(
            Arc::new(ConfigFunctionRegistry::new(config_store)),
            Arc::new(FsFunctionArtifactStore::new(layout.clone())),
        )
    }

    pub(crate) fn with_stores(
        registry: Arc<dyn FunctionRegistry>,
        artifacts: Arc<dyn FunctionArtifactStore>,
    ) -> Self {
        Self {
            registry,
            artifacts,
        }
    }

    pub(crate) fn install_validated_user_function(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
        runtime_function: &UdfRuntimeDefinition,
    ) -> Result<InstalledFunction, AppError> {
        let function = parse_function_sql(raw_sql).map_err(|error| {
            AppError::InvalidInput(format!("function validation failed: {error}"))
        })?;
        let function_name = FunctionName::parse(function.name())?;
        if function_name.as_str() != runtime_function.name {
            return Err(AppError::FailedPrecondition(format!(
                "validated function '{}' does not match installed function '{}'",
                runtime_function.name, function_name
            )));
        }
        self.install_user_function_artifact(workspace_name, &function_name, raw_sql)
    }

    fn install_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<InstalledFunction, AppError> {
        let installed = InstalledFunction {
            name: function_name.clone(),
        };

        let previous_artifact =
            self.artifacts
                .write_user_function_artifact(workspace_name, function_name, raw_sql)?;
        if let Err(error) = self
            .registry
            .upsert_function(workspace_name, installed.clone())
        {
            if let Err(restore_error) = self.artifacts.restore_user_function_artifact(
                workspace_name,
                function_name,
                &previous_artifact,
            ) {
                tracing::warn!(
                    function = %function_name,
                    detail = %restore_error,
                    "failed to restore previous function artifact after inventory update failure"
                );
            }
            return Err(error);
        }

        Ok(installed)
    }

    pub(crate) fn remove_user_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError> {
        self.registry.get_function(workspace_name, function_name)?;
        let removed_artifact = self
            .artifacts
            .remove_user_function_artifact(workspace_name, function_name)?;
        if let Err(error) = self.registry.remove_function(workspace_name, function_name) {
            if let Err(restore_error) = self.artifacts.restore_user_function_artifact(
                workspace_name,
                function_name,
                &removed_artifact,
            ) {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove function '{function_name}': {error}; failed to restore function artifact: {restore_error}"
                )));
            }
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use coral_engine::{
        UdfRuntimeDefinition, UdfRuntimeImplementation, UdfRuntimePublish, UdfRuntimeResultColumn,
        UdfRuntimeTableFunctionPublish,
    };
    use tempfile::TempDir;

    use super::*;

    fn fixture() -> (TempDir, AppStateLayout, ConfigStore, FunctionManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let manager = FunctionManager::new(config_store.clone(), &layout);
        (temp, layout, config_store, manager)
    }

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn function_sql(name: &str) -> String {
        format!(
            r"---
name: {name}
schema: functions
description: Test function {name}
---

select 1 as id
"
        )
    }

    fn runtime_function(name: &str) -> UdfRuntimeDefinition {
        UdfRuntimeDefinition {
            name: name.to_string(),
            description: String::new(),
            arguments: Vec::new(),
            implementation: UdfRuntimeImplementation::CoralSql {
                query: "select 1 as id".to_string(),
            },
            publish: UdfRuntimePublish {
                table_function: UdfRuntimeTableFunctionPublish {
                    schema: "functions".to_string(),
                    name: name.to_string(),
                    description: String::new(),
                },
            },
            result_columns: vec![UdfRuntimeResultColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }],
        }
    }

    #[test]
    fn install_user_function_writes_inventory_and_artifact() {
        let (_temp, layout, config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");

        let installed = manager
            .install_validated_user_function(
                &workspace,
                &raw_sql,
                &runtime_function("review_queue"),
            )
            .expect("install function");

        assert_eq!(installed.name.as_str(), "review_queue");
        assert_eq!(
            config_store
                .list_workspace_functions(&workspace)
                .expect("list function inventory")
                .len(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(layout.function_file(&workspace, &installed.name))
                .expect("read function artifact"),
            raw_sql
        );
    }

    #[test]
    fn remove_user_function_removes_inventory_and_artifacts() {
        let (_temp, layout, config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        let installed = manager
            .install_validated_user_function(
                &workspace,
                &raw_sql,
                &runtime_function("review_queue"),
            )
            .expect("install function");

        manager
            .remove_user_function(&workspace, &installed.name)
            .expect("remove function");

        assert!(
            config_store
                .list_workspace_functions(&workspace)
                .expect("list function inventory")
                .is_empty()
        );
        assert!(
            !layout.function_dir(&workspace, &installed.name).exists(),
            "function artifact directory should be removed"
        );
    }
}
