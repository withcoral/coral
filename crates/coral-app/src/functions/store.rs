//! Durable storage seam for workspace functions.

use crate::bootstrap::AppError;
use crate::functions::model::FunctionName;
use crate::state::db::{CoralDb, DbRepos};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredFunction {
    pub(crate) name: FunctionName,
    pub(crate) artifact_sql: String,
}

#[tonic::async_trait]
pub(crate) trait FunctionStore: Send + Sync {
    async fn list(&self, workspace_name: &WorkspaceName) -> Result<Vec<StoredFunction>, AppError>;

    async fn upsert(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        artifact_sql: &str,
    ) -> Result<(), AppError>;

    async fn delete(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<bool, AppError>;
}

#[tonic::async_trait]
impl FunctionStore for CoralDb {
    async fn list(&self, workspace_name: &WorkspaceName) -> Result<Vec<StoredFunction>, AppError> {
        let mut session = self;
        let functions = session
            .functions()
            .list(workspace_name.as_str())
            .await?
            .into_iter()
            .map(|record| {
                Ok(StoredFunction {
                    name: FunctionName::parse(&record.name)?,
                    artifact_sql: record.artifact_sql,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()?;
        Ok(functions)
    }

    async fn upsert(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        artifact_sql: &str,
    ) -> Result<(), AppError> {
        let mut session = self;
        session
            .functions()
            .upsert(
                workspace_name.as_str(),
                function_name.as_str(),
                artifact_sql,
            )
            .await?;
        Ok(())
    }

    async fn delete(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<bool, AppError> {
        let mut session = self;
        Ok(session
            .functions()
            .delete(workspace_name.as_str(), function_name.as_str())
            .await?)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::RwLock;

    use super::*;

    #[derive(Default)]
    pub(crate) struct InMemoryFunctionStore {
        functions: RwLock<BTreeMap<(WorkspaceName, FunctionName), String>>,
    }

    #[tonic::async_trait]
    impl FunctionStore for InMemoryFunctionStore {
        async fn list(
            &self,
            workspace_name: &WorkspaceName,
        ) -> Result<Vec<StoredFunction>, AppError> {
            let functions = self
                .functions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(functions
                .iter()
                .filter(|((workspace, _), _)| workspace == workspace_name)
                .map(|((_, name), artifact_sql)| StoredFunction {
                    name: name.clone(),
                    artifact_sql: artifact_sql.clone(),
                })
                .collect())
        }

        async fn upsert(
            &self,
            workspace_name: &WorkspaceName,
            function_name: &FunctionName,
            artifact_sql: &str,
        ) -> Result<(), AppError> {
            self.functions
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(
                    (workspace_name.clone(), function_name.clone()),
                    artifact_sql.to_owned(),
                );
            Ok(())
        }

        async fn delete(
            &self,
            workspace_name: &WorkspaceName,
            function_name: &FunctionName,
        ) -> Result<bool, AppError> {
            Ok(self
                .functions
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&(workspace_name.clone(), function_name.clone()))
                .is_some())
        }
    }
}
