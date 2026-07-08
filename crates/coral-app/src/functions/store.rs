//! Storage seams for workspace function inventory and artifacts.

use std::io::ErrorKind;

use crate::bootstrap::AppError;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

pub(crate) trait FunctionRegistry: Send + Sync {
    fn list_workspace_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledFunction>, AppError>;

    fn get_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<InstalledFunction, AppError>;

    fn upsert_function(
        &self,
        workspace_name: &WorkspaceName,
        function: InstalledFunction,
    ) -> Result<(), AppError>;

    fn remove_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError>;
}

#[derive(Clone)]
pub(crate) struct ConfigFunctionRegistry {
    config_store: ConfigStore,
}

impl ConfigFunctionRegistry {
    pub(crate) fn new(config_store: ConfigStore) -> Self {
        Self { config_store }
    }
}

impl FunctionRegistry for ConfigFunctionRegistry {
    fn list_workspace_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledFunction>, AppError> {
        self.config_store.list_workspace_functions(workspace_name)
    }

    fn get_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<InstalledFunction, AppError> {
        self.config_store
            .get_function(workspace_name, function_name)
    }

    fn upsert_function(
        &self,
        workspace_name: &WorkspaceName,
        function: InstalledFunction,
    ) -> Result<(), AppError> {
        self.config_store.upsert_function(workspace_name, function)
    }

    fn remove_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError> {
        self.config_store
            .remove_function(workspace_name, function_name)
    }
}

pub(crate) trait FunctionArtifactStore: Send + Sync {
    fn read_function_sql(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<Option<String>, AppError>;

    fn write_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<FunctionArtifactSnapshot, AppError>;

    fn remove_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<FunctionArtifactSnapshot, AppError>;

    fn restore_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        snapshot: &FunctionArtifactSnapshot,
    ) -> Result<(), AppError>;
}

#[derive(Clone)]
pub(crate) struct FsFunctionArtifactStore {
    layout: AppStateLayout,
}

impl FsFunctionArtifactStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    fn snapshot(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<FunctionArtifactSnapshot, AppError> {
        Ok(FunctionArtifactSnapshot {
            function_sql: read_optional_bytes(
                &self.layout.function_file(workspace_name, function_name),
            )?,
        })
    }

    fn write_user_function_artifact_inner(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<(), AppError> {
        let function_dir = self.layout.function_dir(workspace_name, function_name);
        let function_file = self.layout.function_file(workspace_name, function_name);

        fs::ensure_private_dir(&function_dir)?;
        fs::write_atomic(&function_file, raw_sql.as_bytes())?;
        Ok(())
    }
}

impl FunctionArtifactStore for FsFunctionArtifactStore {
    fn read_function_sql(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<Option<String>, AppError> {
        match std::fs::read_to_string(self.layout.function_file(workspace_name, function_name)) {
            Ok(raw_sql) => Ok(Some(raw_sql)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<FunctionArtifactSnapshot, AppError> {
        let previous = self.snapshot(workspace_name, function_name)?;
        if let Err(error) =
            self.write_user_function_artifact_inner(workspace_name, function_name, raw_sql)
        {
            if let Err(restore_error) =
                self.restore_user_function_artifact(workspace_name, function_name, &previous)
            {
                tracing::warn!(
                    function = %function_name,
                    detail = %restore_error,
                    "failed to restore previous function artifact after write failure"
                );
            }
            return Err(error);
        }
        Ok(previous)
    }

    fn remove_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<FunctionArtifactSnapshot, AppError> {
        let previous = self.snapshot(workspace_name, function_name)?;
        let function_dir = self.layout.function_dir(workspace_name, function_name);
        match std::fs::remove_dir_all(function_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(previous)
    }

    fn restore_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        snapshot: &FunctionArtifactSnapshot,
    ) -> Result<(), AppError> {
        let function_dir = self.layout.function_dir(workspace_name, function_name);
        let function_file = self.layout.function_file(workspace_name, function_name);

        if snapshot.function_sql.is_none() {
            match std::fs::remove_dir_all(function_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(());
        }

        fs::ensure_private_dir(&function_dir)?;
        match &snapshot.function_sql {
            Some(raw_sql) => fs::write_atomic(&function_file, raw_sql)?,
            None => remove_file_if_present(&function_file)?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FunctionArtifactSnapshot {
    function_sql: Option<Vec<u8>>,
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
