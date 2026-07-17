//! Storage seams for workspace function inventory and artifacts.

use std::io::ErrorKind;

use crate::bootstrap::AppError;
use crate::functions::model::FunctionName;
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

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

        let Some(raw_sql) = &snapshot.function_sql else {
            match std::fs::remove_dir_all(function_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(());
        };

        fs::ensure_private_dir(&function_dir)?;
        fs::write_atomic(&function_file, raw_sql)?;
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
