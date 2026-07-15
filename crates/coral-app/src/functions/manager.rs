//! Owns user-installed function files and workspace inventory.

use std::collections::BTreeSet;
use std::sync::Arc;

use coral_engine::{QueryRuntimeConfig, QuerySource, UdfRuntimeDefinition};
use coral_spec::{FunctionSpec, parse_function_sql};

use crate::bootstrap::AppError;
use crate::functions::model::{FunctionName, InstalledFunction};
use crate::functions::runtime::{
    infer_runtime_function, infer_runtime_functions, runtime_function_without_signature,
};
use crate::functions::store::{FsFunctionArtifactStore, FunctionArtifactStore};
use crate::functions::validation::{
    SqlPublishTargets, initial_sql_publish_targets, record_sql_publish_target,
    source_sql_publish_targets_for_schemas, unchecked_source_publish_schemas,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};

#[derive(Clone)]
pub(crate) struct FunctionManager {
    config_store: ConfigStore,
    artifacts: Arc<dyn FunctionArtifactStore>,
    lifecycle_lock: WorkspaceLifecycleLock,
}

struct FunctionArtifact {
    name: FunctionName,
    content: FunctionArtifactContent,
}

enum FunctionArtifactContent {
    Sql(String),
    Unavailable(String),
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

enum FunctionCandidate {
    Listing(FunctionListing),
    Pending {
        name: FunctionName,
        definition: UdfRuntimeDefinition,
    },
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

    pub(crate) async fn validate_user_function_sql(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
        raw_sql: &str,
    ) -> Result<UdfRuntimeDefinition, AppError> {
        let spec = parse_function_sql(raw_sql).map_err(|error| {
            AppError::InvalidInput(format!("function validation failed: {error}"))
        })?;
        let function_name = FunctionName::parse(spec.name())?;
        let mut sql_publish_targets = initial_sql_publish_targets(selected_sources);
        self.record_installed_function_sql_publish_targets(
            workspace_name,
            &function_name,
            &mut sql_publish_targets,
        )?;
        let runtime_function =
            infer_runtime_function(selected_sources, runtime_config()?, &spec).await?;
        record_sql_publish_target(&runtime_function, &mut sql_publish_targets)?;
        Ok(runtime_function)
    }

    pub(crate) async fn list_functions(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
    ) -> Result<Vec<FunctionListing>, AppError> {
        self.evaluate_function_listings(workspace_name, selected_sources, runtime_config)
            .await
    }

    pub(crate) fn list_functions_with_preparation_failure(
        &self,
        workspace_name: &WorkspaceName,
        preparation_error: &AppError,
    ) -> Result<Vec<FunctionListing>, AppError> {
        let preparation_error = format!("function runtime unavailable: {preparation_error}");
        Ok(self
            .load_function_artifacts(workspace_name)?
            .into_iter()
            .map(|artifact| {
                let error = parse_function_artifact(&artifact)
                    .err()
                    .unwrap_or_else(|| preparation_error.clone());
                invalid_listing(artifact.name, error)
            })
            .collect())
    }

    pub(crate) async fn load_runtime_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
    ) -> Result<Vec<UdfRuntimeDefinition>, AppError> {
        let listings = self
            .evaluate_function_listings(workspace_name, selected_sources, runtime_config)
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

    async fn evaluate_function_listings(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
        mut runtime_config: impl FnMut() -> Result<QueryRuntimeConfig, AppError>,
    ) -> Result<Vec<FunctionListing>, AppError> {
        let artifacts = self.load_function_artifacts(workspace_name)?;
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
            infer_runtime_functions(selected_sources, runtime_config()?, pending).await?
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

    fn load_function_artifacts(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<FunctionArtifact>, AppError> {
        let _state_lock = self.config_store.state_lock_shared()?;
        let mut artifacts = Vec::new();
        for installed in self
            .config_store
            .list_workspace_functions_unlocked(workspace_name)?
        {
            let content = match self
                .artifacts
                .read_function_sql(workspace_name, &installed.name)
            {
                Ok(Some(raw_sql)) => FunctionArtifactContent::Sql(raw_sql),
                Ok(None) => FunctionArtifactContent::Unavailable(
                    "installed function file is missing".to_string(),
                ),
                Err(error) => FunctionArtifactContent::Unavailable(format!(
                    "installed function file could not be read: {error}"
                )),
            };
            let function_name = installed.name;
            artifacts.push(FunctionArtifact {
                name: function_name,
                content,
            });
        }

        artifacts.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
        Ok(artifacts)
    }

    fn record_installed_function_sql_publish_targets(
        &self,
        workspace_name: &WorkspaceName,
        replacing_function: &FunctionName,
        publish_targets: &mut SqlPublishTargets,
    ) -> Result<(), AppError> {
        for artifact in self.load_function_artifacts(workspace_name)? {
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

fn parse_function_artifact(artifact: &FunctionArtifact) -> Result<FunctionSpec, String> {
    let raw_sql = match &artifact.content {
        FunctionArtifactContent::Sql(raw_sql) => raw_sql,
        FunctionArtifactContent::Unavailable(error) => return Err(error.clone()),
    };
    let spec =
        parse_function_sql(raw_sql).map_err(|error| format!("function is invalid: {error}"))?;
    let declared_name = FunctionName::parse(spec.name()).map_err(|error| error.to_string())?;
    if declared_name != artifact.name {
        return Err(format!(
            "function file declares name '{declared_name}' but its inventory name is '{}'",
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

    use tempfile::TempDir;

    use super::*;
    use crate::state::AppStateLayout;

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

    fn install_fixture_function(
        manager: &FunctionManager,
        workspace: &WorkspaceName,
        raw_sql: &str,
    ) -> InstalledFunction {
        let runtime_function = validated_function(raw_sql);
        manager
            .install_validated_user_function(workspace, raw_sql, &runtime_function)
            .expect("install function")
    }

    #[tokio::test]
    async fn list_functions_infers_columns_from_sql_body() {
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql_with_owner_query("review_queue");
        install_fixture_function(&manager, &workspace, &raw_sql);
        let function_name = FunctionName::parse("review_queue").expect("function name");
        std::fs::write(
            layout.function_file(&workspace, &function_name),
            raw_sql.replace(
                "select cast($owner as VARCHAR) as owner",
                "select cast($owner as VARCHAR) as reviewer",
            ),
        )
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
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        install_fixture_function(&manager, &workspace, &function_sql("first"));
        install_fixture_function(&manager, &workspace, &function_sql("second"));
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
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql_with_owner_query("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql);
        std::fs::write(
            layout.function_file(&workspace, &installed.name),
            raw_sql.replace(
                "select cast($owner as VARCHAR) as owner",
                "select $owner as owner",
            ),
        )
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
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql);
        std::fs::write(
            layout.function_file(&workspace, &installed.name),
            raw_sql.replace("name: review_queue", "name: renamed"),
        )
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
            .expect("inventory name remains removable");
    }

    #[tokio::test]
    async fn validate_function_ignores_unrelated_missing_artifact() {
        let (_temp, layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let installed =
            install_fixture_function(&manager, &workspace, &function_sql("missing_artifact"));
        std::fs::remove_file(layout.function_file(&workspace, &installed.name))
            .expect("remove existing artifact");

        let validated = manager
            .validate_user_function_sql(
                &workspace,
                &[],
                || Ok(QueryRuntimeConfig::default()),
                &function_sql("new_function"),
            )
            .await
            .expect("unrelated broken artifact should not block validation");

        assert_eq!(validated.name, "new_function");
    }

    #[tokio::test]
    async fn load_runtime_udfs_uses_only_runtime_ready_functions() {
        let (_temp, _layout, _config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        install_fixture_function(&manager, &workspace, &raw_sql);

        let runtime_functions = manager
            .load_runtime_udfs(&workspace, &[], || Ok(QueryRuntimeConfig::default()))
            .await
            .expect("load runtime functions");

        assert_eq!(runtime_functions.len(), 1);
        let runtime_function = runtime_functions.first().expect("one runtime function");
        assert_eq!(runtime_function.name, "review_queue");
        assert_eq!(runtime_function.result_columns.len(), 1);
    }

    #[test]
    fn remove_user_function_removes_inventory_and_artifacts() {
        let (_temp, layout, config_store, manager) = fixture();
        let workspace = workspace();
        let raw_sql = function_sql("review_queue");
        let installed = install_fixture_function(&manager, &workspace, &raw_sql);

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
            let raw_sql = function_sql("review_queue");
            let runtime_function = validated_function(&raw_sql);
            let result = install_manager
                .install_validated_user_function(&install_workspace, &raw_sql, &runtime_function)
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
        install_fixture_function(&manager, &workspace, &original_sql);

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
        let runtime_function = validated_function(&replacement_sql);
        let error = manager
            .install_validated_user_function(&workspace, &replacement_sql, &runtime_function)
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
