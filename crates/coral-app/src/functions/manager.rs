//! Owns user-installed function files and workspace inventory.

use std::sync::Arc;

use coral_engine::UdfRuntimeDefinition;
use coral_spec::parse_function_sql;

use crate::bootstrap::AppError;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::functions::store::{FsFunctionArtifactStore, FunctionArtifactStore};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};

#[derive(Clone)]
pub(crate) struct FunctionManager {
    config_store: ConfigStore,
    artifacts: Arc<dyn FunctionArtifactStore>,
    lifecycle_lock: WorkspaceLifecycleLock,
}

impl FunctionManager {
    #[cfg(test)]
    pub(crate) fn new_for_tests(config_store: ConfigStore, layout: &AppStateLayout) -> Self {
        Self::new(config_store, layout, WorkspaceLifecycleLock::default())
    }

    pub(crate) fn new(
        config_store: ConfigStore,
        layout: &AppStateLayout,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        Self {
            config_store,
            artifacts: Arc::new(FsFunctionArtifactStore::new(layout.clone())),
            lifecycle_lock,
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
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let _state_lock = self.config_store.state_lock_exclusive()?;
        let installed = InstalledFunction {
            name: function_name.clone(),
        };

        let previous_artifact =
            self.artifacts
                .write_user_function_artifact(workspace_name, function_name, raw_sql)?;
        if let Err(error) = self
            .config_store
            .upsert_function_unlocked(workspace_name, installed.clone())
        {
            if let Err(restore_error) = self.artifacts.restore_user_function_artifact(
                workspace_name,
                function_name,
                &previous_artifact,
            ) {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to install function '{function_name}': {error}; failed to restore function artifact: {restore_error}"
                )));
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
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let _state_lock = self.config_store.state_lock_exclusive()?;
        self.config_store
            .get_function_unlocked(workspace_name, function_name)?;
        let removed_artifact = self
            .artifacts
            .remove_user_function_artifact(workspace_name, function_name)?;
        if let Err(error) = self
            .config_store
            .remove_function_unlocked(workspace_name, function_name)
        {
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
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

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
        let manager = FunctionManager::new_for_tests(config_store.clone(), &layout);
        (temp, layout, config_store, manager)
    }

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn function_sql(name: &str) -> String {
        format!(
            r"/*
name: {name}
schema: functions
description: Test function {name}
*/

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

    #[derive(Clone)]
    struct RestoreFailingArtifactStore {
        inner: FsFunctionArtifactStore,
    }

    impl FunctionArtifactStore for RestoreFailingArtifactStore {
        fn read_function_sql(
            &self,
            _workspace_name: &WorkspaceName,
            _function_name: &FunctionName,
        ) -> Result<Option<String>, AppError> {
            unreachable!("rollback test does not read function artifacts")
        }

        fn write_user_function_artifact(
            &self,
            workspace_name: &WorkspaceName,
            function_name: &FunctionName,
            raw_sql: &str,
        ) -> Result<crate::functions::store::FunctionArtifactSnapshot, AppError> {
            self.inner
                .write_user_function_artifact(workspace_name, function_name, raw_sql)
        }

        fn remove_user_function_artifact(
            &self,
            workspace_name: &WorkspaceName,
            function_name: &FunctionName,
        ) -> Result<crate::functions::store::FunctionArtifactSnapshot, AppError> {
            self.inner
                .remove_user_function_artifact(workspace_name, function_name)
        }

        fn restore_user_function_artifact(
            &self,
            _workspace_name: &WorkspaceName,
            _function_name: &FunctionName,
            _snapshot: &crate::functions::store::FunctionArtifactSnapshot,
        ) -> Result<(), AppError> {
            Err(AppError::FailedPrecondition(
                "injected restore failure".to_string(),
            ))
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

    #[test]
    fn remove_user_function_reports_typed_missing_function() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let function_name = FunctionName::parse("missing").expect("function name");

        let error = manager
            .remove_user_function(&workspace(), &function_name)
            .expect_err("missing function should fail");

        assert!(matches!(
            error,
            AppError::FunctionNotFound(name) if name == "missing"
        ));
    }

    #[test]
    fn install_user_function_waits_for_workspace_lifecycle_lock() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let lifecycle_lock = WorkspaceLifecycleLock::default();
        let manager = FunctionManager::new(config_store, &layout, lifecycle_lock.clone());
        let lifecycle_guard = lifecycle_lock.lock();
        let workspace = workspace();

        let install_manager = manager.clone();
        let install_workspace = workspace.clone();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("send started");
            let result = install_manager
                .install_validated_user_function(
                    &install_workspace,
                    &function_sql("review_queue"),
                    &runtime_function("review_queue"),
                )
                .map(|function| function.name.to_string())
                .map_err(|error| error.to_string());
            done_tx.send(result).expect("send install result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("install thread should start");
        assert!(
            matches!(
                done_rx.recv_timeout(Duration::from_millis(300)),
                Err(std_mpsc::RecvTimeoutError::Timeout)
            ),
            "function install completed while the workspace lifecycle lock was held"
        );

        drop(lifecycle_guard);
        let installed = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("install should finish after releasing lifecycle lock")
            .expect("install should succeed");
        assert_eq!(installed, "review_queue");
        handle.join().expect("join install thread");
    }

    #[test]
    fn install_user_function_reports_inventory_and_restore_failures() {
        let (_temp, layout, config_store, manager) = fixture();
        let workspace = workspace();
        let original_sql = function_sql("review_queue");
        manager
            .install_validated_user_function(
                &workspace,
                &original_sql,
                &runtime_function("review_queue"),
            )
            .expect("install original function");

        let function_name = FunctionName::parse("review_queue").expect("function name");
        let replacement_sql = format!("{original_sql}\n");
        std::fs::remove_file(layout.config_file()).expect("remove config file");
        std::fs::create_dir(layout.config_file()).expect("replace config file with directory");

        let manager = FunctionManager {
            config_store,
            artifacts: Arc::new(RestoreFailingArtifactStore {
                inner: FsFunctionArtifactStore::new(layout.clone()),
            }),
            lifecycle_lock: WorkspaceLifecycleLock::default(),
        };
        let error = manager
            .install_validated_user_function(
                &workspace,
                &replacement_sql,
                &runtime_function("review_queue"),
            )
            .expect_err("inventory and restore failures should be reported together");

        let AppError::FailedPrecondition(message) = error else {
            panic!("unexpected error: {error}");
        };
        assert!(
            message.contains("failed to install function 'review_queue'"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("failed to restore function artifact"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("injected restore failure"),
            "unexpected error: {message}"
        );
        assert_eq!(
            std::fs::read_to_string(layout.function_file(&workspace, &function_name))
                .expect("read unrestored function artifact"),
            replacement_sql
        );
    }
}
