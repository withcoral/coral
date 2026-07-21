//! Owns workspace-scoped function validation and durable storage.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use coral_engine::{PreparedQueryRuntime, QueryRuntimeConfig, QuerySource, UdfRuntimeDefinition};
use coral_spec::{FunctionSpec, parse_function_sql};

use crate::bootstrap::AppError;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::functions::runtime::{
    infer_runtime_function, infer_runtime_functions, infer_runtime_functions_in_prepared_runtime,
    runtime_function_without_signature,
};
use crate::functions::store::FunctionStore;
use crate::functions::validation::{
    SqlPublishTargets, initial_sql_publish_targets, record_sql_publish_target,
    source_sql_publish_targets_for_schemas, unchecked_source_publish_schemas,
};
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceName};

#[derive(Clone)]
pub(crate) struct FunctionManager {
    store: Arc<dyn FunctionStore>,
    lifecycle_lock: WorkspaceLifecycleLock,
}

struct FunctionArtifact {
    name: FunctionName,
    sql: String,
}

/// One function as listed by the app inventory surface.
pub(crate) struct FunctionListing {
    /// Stable installed inventory name.
    pub(crate) name: FunctionName,
    /// Current runtime state for this installed function.
    pub(crate) runtime: FunctionRuntimeStatus,
}

pub(crate) enum FunctionRuntimeStatus {
    Ready(UdfRuntimeDefinition),
    Invalid(String),
}

pub(crate) enum ValidatedFunctionInstall {
    Installed,
    WorkspaceChanged,
}

enum FunctionCandidate {
    Listing(FunctionListing),
    Pending {
        name: FunctionName,
        definition: UdfRuntimeDefinition,
    },
}

impl FunctionManager {
    #[cfg(test)]
    pub(crate) fn new_for_tests(store: Arc<dyn FunctionStore>) -> Self {
        Self::new(store, WorkspaceLifecycleLock::default())
    }

    pub(crate) fn new(
        store: Arc<dyn FunctionStore>,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        Self {
            store,
            lifecycle_lock,
        }
    }

    #[cfg(test)]
    pub(crate) async fn install_validated_user_function(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
        runtime_function: &UdfRuntimeDefinition,
    ) -> Result<InstalledFunction, AppError> {
        let function_name = validated_function_name(raw_sql, runtime_function)?;
        self.install_user_function_artifact(workspace_name, &function_name, raw_sql)
            .await
    }

    pub(crate) async fn install_validated_user_function_if_unchanged(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
        runtime_function: &UdfRuntimeDefinition,
        revision: WorkspaceLifecycleRevision,
    ) -> Result<ValidatedFunctionInstall, AppError> {
        let function_name = validated_function_name(raw_sql, runtime_function)?;
        let Some(_lifecycle_guard) = self
            .lifecycle_lock
            .lock_if_unchanged(workspace_name, revision)
            .await
        else {
            return Ok(ValidatedFunctionInstall::WorkspaceChanged);
        };
        self.install_user_function_artifact_with_lifecycle_lock(
            workspace_name,
            &function_name,
            raw_sql,
        )
        .await?;
        Ok(ValidatedFunctionInstall::Installed)
    }

    #[cfg(test)]
    async fn install_user_function_artifact(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<InstalledFunction, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock(workspace_name).await;
        self.install_user_function_artifact_with_lifecycle_lock(
            workspace_name,
            function_name,
            raw_sql,
        )
        .await
    }

    async fn install_user_function_artifact_with_lifecycle_lock(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
        raw_sql: &str,
    ) -> Result<InstalledFunction, AppError> {
        let installed = InstalledFunction {
            name: function_name.clone(),
        };
        self.store
            .upsert(workspace_name, function_name, raw_sql)
            .await?;
        Ok(installed)
    }

    pub(crate) async fn validate_user_function_sql(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
        raw_sql: &str,
    ) -> Result<UdfRuntimeDefinition, AppError> {
        let function = parse_function_sql(raw_sql).map_err(|error| {
            AppError::InvalidInput(format!("function validation failed: {error}"))
        })?;
        let function_name = FunctionName::parse(function.name())?;
        let mut sql_publish_targets = initial_sql_publish_targets(selected_sources);
        self.record_installed_function_sql_publish_targets(
            workspace_name,
            &function_name,
            &mut sql_publish_targets,
        )
        .await?;
        let runtime_function =
            infer_runtime_function(selected_sources, runtime_config()?, &function).await?;
        record_sql_publish_target(&runtime_function, &mut sql_publish_targets)?;
        Ok(runtime_function)
    }

    pub(crate) async fn list_functions(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
    ) -> Result<Vec<FunctionListing>, AppError> {
        self.evaluate_function_listings(workspace_name, selected_sources, |pending| async move {
            infer_runtime_functions(selected_sources, runtime_config()?, pending).await
        })
        .await
    }

    pub(crate) async fn load_runtime_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        runtime: &PreparedQueryRuntime,
    ) -> Result<Vec<UdfRuntimeDefinition>, AppError> {
        let listings = self
            .evaluate_function_listings(workspace_name, selected_sources, |pending| {
                infer_runtime_functions_in_prepared_runtime(runtime, pending)
            })
            .await?;
        let mut definitions = Vec::new();
        for listing in listings {
            match listing.runtime {
                FunctionRuntimeStatus::Ready(definition) => definitions.push(definition),
                FunctionRuntimeStatus::Invalid(error) => tracing::warn!(
                    function = %listing.name,
                    detail = %error,
                    "skipping function during runtime publication"
                ),
            }
        }
        Ok(definitions)
    }

    async fn evaluate_function_listings<Infer, InferFuture>(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        infer: Infer,
    ) -> Result<Vec<FunctionListing>, AppError>
    where
        Infer: FnOnce(Vec<UdfRuntimeDefinition>) -> InferFuture,
        InferFuture: Future<Output = Result<Vec<Result<UdfRuntimeDefinition, AppError>>, AppError>>,
    {
        let artifacts = self.load_function_artifacts(workspace_name).await?;
        if artifacts.is_empty() {
            return Ok(Vec::new());
        }

        let mut checked_source_schemas = BTreeSet::new();
        let mut sql_publish_targets = SqlPublishTargets::default();
        let mut candidates = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            let spec = match parse_function_artifact(&artifact) {
                Ok(spec) => spec,
                Err(error) => {
                    candidates.push(FunctionCandidate::Listing(invalid_listing(
                        artifact.name,
                        error,
                    )));
                    continue;
                }
            };
            let runtime_function = runtime_function_without_signature(&spec);
            let unchecked_source_schemas =
                unchecked_source_publish_schemas(&runtime_function, &checked_source_schemas);
            if !unchecked_source_schemas.is_empty() {
                sql_publish_targets.extend(source_sql_publish_targets_for_schemas(
                    selected_sources,
                    &unchecked_source_schemas,
                ));
                checked_source_schemas.extend(unchecked_source_schemas);
            }
            candidates.push(FunctionCandidate::Pending {
                name: artifact.name,
                definition: runtime_function,
            });
        }

        let pending = candidates
            .iter()
            .filter_map(|candidate| match candidate {
                FunctionCandidate::Pending { definition, .. } => Some(definition.clone()),
                FunctionCandidate::Listing(_) => None,
            })
            .collect::<Vec<_>>();
        let inferred = if pending.is_empty() {
            Vec::new()
        } else {
            infer(pending).await?
        };
        let mut inferred = inferred.into_iter();

        candidates
            .into_iter()
            .map(|candidate| match candidate {
                FunctionCandidate::Listing(listing) => Ok(listing),
                FunctionCandidate::Pending { name, .. } => match inferred.next() {
                    Some(Ok(definition)) => {
                        match record_sql_publish_target(&definition, &mut sql_publish_targets) {
                            Ok(()) => Ok(ready_listing(name, definition)),
                            Err(error) => Ok(invalid_listing(name, error.to_string())),
                        }
                    }
                    Some(Err(error)) => Ok(invalid_listing(name, error.to_string())),
                    None => Err(AppError::FailedPrecondition(
                        "function runtime validation returned too few results".to_string(),
                    )),
                },
            })
            .collect()
    }

    pub(crate) async fn remove_user_function(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> Result<(), AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock(workspace_name).await;
        if !self.store.delete(workspace_name, function_name).await? {
            return Err(AppError::FunctionNotFound(function_name.to_string()));
        }
        Ok(())
    }

    async fn load_function_artifacts(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<FunctionArtifact>, AppError> {
        Ok(self
            .store
            .list(workspace_name)
            .await?
            .into_iter()
            .map(|stored| FunctionArtifact {
                name: stored.name,
                sql: stored.artifact_sql,
            })
            .collect())
    }

    async fn record_installed_function_sql_publish_targets(
        &self,
        workspace_name: &WorkspaceName,
        replacing_function: &FunctionName,
        publish_targets: &mut SqlPublishTargets,
    ) -> Result<(), AppError> {
        for artifact in self.load_function_artifacts(workspace_name).await? {
            if artifact.name == *replacing_function {
                continue;
            }
            let spec = match parse_function_artifact(&artifact) {
                Ok(spec) => spec,
                Err(error) => {
                    tracing::warn!(
                        function = %artifact.name,
                        detail = %error,
                        "ignoring invalid installed function during publish collision validation"
                    );
                    continue;
                }
            };
            let runtime_function = runtime_function_without_signature(&spec);
            record_sql_publish_target(&runtime_function, publish_targets)?;
        }
        Ok(())
    }
}

fn validated_function_name(
    raw_sql: &str,
    runtime_function: &UdfRuntimeDefinition,
) -> Result<FunctionName, AppError> {
    let function = parse_function_sql(raw_sql)
        .map_err(|error| AppError::InvalidInput(format!("function validation failed: {error}")))?;
    let function_name = FunctionName::parse(function.name())?;
    if function_name.as_str() != runtime_function.name {
        return Err(AppError::FailedPrecondition(format!(
            "validated function '{}' does not match installed function '{}'",
            runtime_function.name, function_name
        )));
    }
    Ok(function_name)
}

fn parse_function_artifact(artifact: &FunctionArtifact) -> Result<FunctionSpec, String> {
    let spec = parse_function_sql(&artifact.sql)
        .map_err(|error| format!("function is invalid: {error}"))?;
    let declared_name = FunctionName::parse(spec.name()).map_err(|error| error.to_string())?;
    if declared_name != artifact.name {
        return Err(format!(
            "stored function SQL declares name '{declared_name}' but its row name is '{}'",
            artifact.name
        ));
    }
    Ok(spec)
}

fn ready_listing(name: FunctionName, definition: UdfRuntimeDefinition) -> FunctionListing {
    FunctionListing {
        name,
        runtime: FunctionRuntimeStatus::Ready(definition),
    }
}

fn invalid_listing(name: FunctionName, error: String) -> FunctionListing {
    FunctionListing {
        name,
        runtime: FunctionRuntimeStatus::Invalid(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::functions::store::tests::InMemoryFunctionStore;

    fn fixture() -> (Arc<InMemoryFunctionStore>, FunctionManager) {
        let store = Arc::new(InMemoryFunctionStore::default());
        let manager = FunctionManager::new_for_tests(store.clone());
        (store, manager)
    }

    fn workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn function_sql(name: &str) -> String {
        function_sql_with_publish(name, &format!("functions.{name}"))
    }

    fn function_sql_with_owner_query(name: &str) -> String {
        format!(
            r"/*
name: {name}
schema: functions
*/

select cast($owner as VARCHAR) as owner
"
        )
    }

    fn function_sql_with_publish(name: &str, publish_target: &str) -> String {
        let (schema, function) = publish_target
            .split_once('.')
            .expect("publish target should be schema.name");
        format!(
            r"/*
name: {function}
schema: {schema}
description: Test function {name}
*/

select 1 as id
"
        )
    }

    fn validated_function(raw_sql: &str) -> UdfRuntimeDefinition {
        let spec = parse_function_sql(raw_sql).expect("function spec");
        runtime_function_without_signature(&spec)
    }

    async fn install_fixture_function(
        manager: &FunctionManager,
        workspace: &WorkspaceName,
        raw_sql: &str,
    ) -> InstalledFunction {
        let runtime_function = validated_function(raw_sql);
        manager
            .install_validated_user_function(workspace, raw_sql, &runtime_function)
            .await
            .expect("install function")
    }

    #[tokio::test]
    async fn list_functions_infers_columns_from_sql_body() {
        let (store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql_with_owner_query("review_queue");
        install_fixture_function(&manager, &workspace, &raw_sql).await;
        let function_name = FunctionName::parse("review_queue").expect("function name");
        store
            .upsert(
                &workspace,
                &function_name,
                &raw_sql.replace(
                    "select cast($owner as VARCHAR) as owner",
                    "select cast($owner as VARCHAR) as reviewer",
                ),
            )
            .await
            .expect("rewrite function sql");

        let listed = manager
            .list_functions(&workspace, &[], || Ok(QueryRuntimeConfig::default()))
            .await
            .expect("list functions");

        assert_eq!(listed.len(), 1);
        let listed_function = listed.first().expect("one listed function");
        let FunctionRuntimeStatus::Ready(definition) = &listed_function.runtime else {
            panic!("function should be runtime-ready");
        };
        assert_eq!(definition.result_columns.len(), 1);
        assert_eq!(
            definition
                .result_columns
                .first()
                .expect("inferred result column")
                .name,
            "reviewer"
        );
    }

    #[tokio::test]
    async fn list_functions_builds_one_inference_runtime_for_all_functions() {
        let (_store, manager) = fixture();
        let workspace = workspace();
        install_fixture_function(&manager, &workspace, &function_sql("first")).await;
        install_fixture_function(&manager, &workspace, &function_sql("second")).await;
        let runtime_builds = std::sync::atomic::AtomicUsize::new(0);

        let listed = manager
            .list_functions(&workspace, &[], || {
                runtime_builds.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(QueryRuntimeConfig::default())
            })
            .await
            .expect("list functions");

        assert_eq!(listed.len(), 2);
        assert_eq!(runtime_builds.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_functions_keeps_runtime_invalid_artifacts_visible() {
        let (store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql_with_owner_query("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql).await;
        store
            .upsert(
                &workspace,
                &installed.name,
                &raw_sql.replace(
                    "select cast($owner as VARCHAR) as owner",
                    "select $owner as owner",
                ),
            )
            .await
            .expect("rewrite invalid function sql");

        let listed = manager
            .list_functions(&workspace, &[], || Ok(QueryRuntimeConfig::default()))
            .await
            .expect("list functions");

        let listing = listed.first().expect("installed function remains visible");
        assert_eq!(listing.name, installed.name);
        let FunctionRuntimeStatus::Invalid(error) = &listing.runtime else {
            panic!("function should be runtime-invalid");
        };
        assert!(error.contains("has no inferred type"));
    }

    #[tokio::test]
    async fn list_functions_rejects_artifact_name_drift_under_inventory_name() {
        let (store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql).await;
        store
            .upsert(
                &workspace,
                &installed.name,
                &raw_sql.replace("name: review_queue", "name: renamed"),
            )
            .await
            .expect("rewrite function name");

        let listed = manager
            .list_functions(&workspace, &[], || Ok(QueryRuntimeConfig::default()))
            .await
            .expect("list functions");

        let listing = listed.first().expect("installed function remains visible");
        assert_eq!(listing.name, installed.name);
        let FunctionRuntimeStatus::Invalid(error) = &listing.runtime else {
            panic!("function should be runtime-invalid");
        };
        assert!(error.contains("declares name 'renamed'"));
        manager
            .remove_user_function(&workspace, &installed.name)
            .await
            .expect("inventory name remains removable");
    }

    #[tokio::test]
    async fn load_runtime_udfs_uses_only_runtime_ready_functions() {
        let (_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        install_fixture_function(&manager, &workspace, &raw_sql).await;
        let runtime = coral_engine::CoralQuery::prepare(&[], QueryRuntimeConfig::default())
            .await
            .expect("prepare runtime");

        let runtime_functions = manager
            .load_runtime_udfs(&workspace, &[], &runtime)
            .await
            .expect("load runtime functions");

        assert_eq!(runtime_functions.len(), 1);
        let runtime_function = runtime_functions.first().expect("one runtime function");
        assert_eq!(runtime_function.name, "review_queue");
        assert_eq!(runtime_function.result_columns.len(), 1);
    }

    #[tokio::test]
    async fn remove_user_function_removes_stored_row() {
        let (store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql).await;

        manager
            .remove_user_function(&workspace, &installed.name)
            .await
            .expect("remove function");

        assert!(
            store
                .list(&workspace)
                .await
                .expect("list functions")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn remove_user_function_reports_typed_missing_function() {
        let (_store, manager) = fixture();
        let function_name = FunctionName::parse("missing").expect("function name");

        let error = manager
            .remove_user_function(&workspace(), &function_name)
            .await
            .expect_err("missing function should fail");

        assert!(matches!(
            error,
            AppError::FunctionNotFound(name) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn install_user_function_waits_for_workspace_lifecycle_lock() {
        let lifecycle_lock = WorkspaceLifecycleLock::default();
        let manager = FunctionManager::new(
            Arc::new(InMemoryFunctionStore::default()),
            lifecycle_lock.clone(),
        );
        let workspace = workspace();
        let lifecycle_guard = lifecycle_lock.lock(&workspace).await;

        let install_manager = manager.clone();
        let install_workspace = workspace.clone();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("send started");
            let raw_sql = function_sql("review_queue");
            let runtime_function = validated_function(&raw_sql);
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime")
                .block_on(install_manager.install_validated_user_function(
                    &install_workspace,
                    &raw_sql,
                    &runtime_function,
                ))
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
}
