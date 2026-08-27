//! Live observed-value source-scope loading.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::search::observed::source_scope::{SourceScopeSeed, source_surface_scopes};
use crate::search::observed::{ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure};
use crate::sources::catalog::{load_bundled_source, resolve_installed_manifest_from_yaml};
use crate::sources::materialization::{
    LoadedV4Materialization, SourceDiagnosticReporter, incompatible_materialization_error,
    load_v4_materialization_from_record,
};
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::sources::runtime_package::query_source_from_installed_manifest;
use crate::state::db::{CoralDb, DbRepos};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct ObservedValuesLiveScopeLoader {
    config_store: ConfigStore,
    db: Arc<CoralDb>,
    diagnostic_reporter: SourceDiagnosticReporter,
    layout: AppStateLayout,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesLiveScopeLoad {
    pub(crate) live_scopes: Vec<ObservedValuesLiveScope>,
    pub(crate) failed_sources: Vec<ObservedValuesLiveScopeLoadFailure>,
}

impl ObservedValuesLiveScopeLoader {
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: ConfigStore,
        db: Arc<CoralDb>,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        Self {
            config_store,
            db,
            diagnostic_reporter,
            layout,
        }
    }

    pub(crate) async fn load(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesLiveScopeLoad, AppError> {
        // Rebuild from the canonical runtime package on every search. Its
        // contract includes materialized artifacts and per-surface overrides;
        // a separate cache key would duplicate that dependency graph and risk
        // admitting observations under a stale scope.
        let _state_lock = self.config_store.state_lock_shared()?;
        let sources = {
            let mut session = self.db.as_ref();
            session
                .sources()
                .list_workspace_sources(workspace_name)
                .await?
        };
        self.load_sources(workspace_name, sources).await
    }

    async fn load_sources(
        &self,
        workspace_name: &WorkspaceName,
        sources: Vec<InstalledSource>,
    ) -> Result<ObservedValuesLiveScopeLoad, AppError> {
        let mut live_scopes = Vec::new();
        let mut failed_sources = Vec::new();
        for source in sources {
            let source_name = source.name.as_str().to_string();
            match self.load_source_scopes(workspace_name, &source).await {
                Ok(source_scopes) => live_scopes.extend(source_scopes),
                Err(error @ AppError::Database(_)) => return Err(error),
                Err(error) => {
                    tracing::debug!(
                        workspace = %workspace_name,
                        source = %source_name,
                        error = %error,
                        "skipping observed-value live scope for source"
                    );
                    failed_sources.push(ObservedValuesLiveScopeLoadFailure {
                        source_name,
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(ObservedValuesLiveScopeLoad {
            live_scopes,
            failed_sources,
        })
    }

    async fn load_source_scopes(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<Vec<ObservedValuesLiveScope>, AppError> {
        let manifest_yaml = match source.origin {
            SourceOrigin::Bundled => load_bundled_source(&source.name)?.manifest_yaml,
            SourceOrigin::Imported => {
                let mut session = self.db.as_ref();
                session
                    .source_manifests()
                    .get(workspace_name, &source.name)
                    .await?
                    .map(|record| record.manifest_yaml)
                    .ok_or_else(|| {
                        AppError::SourceNotFound(format!(
                            "manifest for imported source '{workspace_name}:{}'",
                            source.name
                        ))
                    })?
            }
        };
        let installed = resolve_installed_manifest_from_yaml(source, &manifest_yaml)?;
        let loaded_v4_materialization = if let Some(v4) = installed.source_spec.as_v4() {
            let record = {
                let mut session = self.db.as_ref();
                session
                    .materializations()
                    .get(workspace_name, &source.name)
                    .await?
                    .ok_or_else(|| {
                        incompatible_materialization_error(
                            &source.name,
                            "required artifact is missing",
                        )
                    })?
            };
            Some(load_v4_materialization_from_record(
                &self.layout,
                workspace_name,
                &source.name,
                &installed.manifest_yaml,
                v4,
                &record,
                &self.diagnostic_reporter,
            )?)
        } else {
            None::<LoadedV4Materialization>
        };
        let loaded_runtime = query_source_from_installed_manifest(
            workspace_name,
            source,
            &installed,
            loaded_v4_materialization.as_ref(),
            &self.diagnostic_reporter,
            BTreeMap::new(),
        )?;
        let seed = SourceScopeSeed::new(
            loaded_runtime.runtime_contract_fingerprint.as_str(),
            source.credential_revision,
        );
        // A component whose name diverges from its package fails this source
        // closed through the existing partial-result channel.
        let scopes = source_surface_scopes(&loaded_runtime.query_source, seed)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        Ok(scopes.into_iter().map(|scope| scope.live_scope()).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::ObservedValuesLiveScopeLoader;
    use crate::search::observed::source_scope::{SourceScopeSeed, source_surface_scopes};
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;
    use crate::sources::SourceName;
    use crate::sources::catalog::resolve_installed_manifest_from_yaml;
    use crate::sources::materialization::SourceDiagnosticReporter;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::sources::runtime_package::query_source_from_installed_manifest;
    use crate::state::db::{CoralDb, DbRepos, open_test_database, run_state_migrations};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn database_membership_does_not_require_legacy_config_source() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("db-only").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        install_source(
            &layout,
            &config_store,
            &workspace,
            &source,
            "/search/issues",
        );
        let db = test_db(&layout, &config_store).await;
        std::fs::remove_file(layout.manifest_file(&workspace, &source))
            .expect("remove legacy manifest");
        config_store
            .remove_source(&workspace, &source)
            .expect("remove legacy config source");
        let loader = ObservedValuesLiveScopeLoader::new(
            layout,
            config_store,
            db,
            SourceDiagnosticReporter::default(),
        );

        let load = loader.load(&workspace).await.expect("live scope load");

        assert_eq!(load.live_scopes.len(), 1);
        assert!(load.failed_sources.is_empty());
    }

    #[tokio::test]
    async fn live_scope_changes_when_http_request_shape_changes() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        let installed =
            install_source(&layout, &config_store, &workspace, &source, "/repos/issues");
        let db = test_db(&layout, &config_store).await;
        std::fs::remove_file(layout.manifest_file(&workspace, &source))
            .expect("remove legacy manifest");
        config_store
            .remove_source(&workspace, &source)
            .expect("remove legacy config source");
        let loader = ObservedValuesLiveScopeLoader::new(
            layout.clone(),
            config_store,
            Arc::clone(&db),
            SourceDiagnosticReporter::default(),
        );

        let first = loader.load(&workspace).await.expect("first live scope");
        upsert_test_source_with_manifest(
            &db,
            &workspace,
            &installed,
            &source_manifest_yaml(&source, "/search/issues"),
        )
        .await;
        let second = loader.load(&workspace).await.expect("second live scope");

        assert!(first.failed_sources.is_empty());
        assert!(second.failed_sources.is_empty());
        assert_eq!(first.live_scopes.len(), 1);
        assert_eq!(second.live_scopes.len(), 1);
        let first_scope = first.live_scopes.first().expect("first live scope");
        let second_scope = second.live_scopes.first().expect("second live scope");
        assert_eq!(first_scope.source_name, "github");
        assert_eq!(second_scope.source_name, "github");
        assert_eq!(first_scope.surface_kind, ObservedValuesSurfaceKind::Table);
        assert_eq!(first_scope.surface_name, "issues");
        assert_ne!(first_scope.source_scope_id, second_scope.source_scope_id);
    }

    #[tokio::test]
    async fn credential_revision_change_changes_live_scope() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        let mut installed = install_source(
            &layout,
            &config_store,
            &workspace,
            &source,
            "/search/issues",
        );
        let db = test_db(&layout, &config_store).await;
        config_store
            .remove_source(&workspace, &source)
            .expect("remove legacy config source");
        let loader = ObservedValuesLiveScopeLoader::new(
            layout,
            config_store,
            Arc::clone(&db),
            SourceDiagnosticReporter::default(),
        );

        let first = loader.load(&workspace).await.expect("first live scope");
        installed.credential_revision = Uuid::from_u128(1);
        upsert_test_source(&db, &workspace, &installed).await;
        let second = loader.load(&workspace).await.expect("second live scope");

        assert!(first.failed_sources.is_empty());
        assert!(second.failed_sources.is_empty());
        assert_eq!(first.live_scopes.len(), 1);
        assert_eq!(second.live_scopes.len(), 1);
        let first_scope = first.live_scopes.first().expect("first live scope");
        let second_scope = second.live_scopes.first().expect("second live scope");
        assert_ne!(first_scope.source_scope_id, second_scope.source_scope_id);
    }

    #[tokio::test]
    async fn live_loader_matches_publisher_scope_with_resolved_secret_material() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_revision = Uuid::from_u128(42);
        let installed_source = install_secured_source(
            &layout,
            &config_store,
            &workspace,
            &source_name,
            credential_revision,
        );
        let db = test_db(&layout, &config_store).await;
        let manifest_yaml = {
            let mut session = db.as_ref();
            session
                .source_manifests()
                .get(&workspace, &source_name)
                .await
                .expect("load canonical manifest")
                .expect("canonical manifest")
                .manifest_yaml
        };
        let installed_manifest =
            resolve_installed_manifest_from_yaml(&installed_source, &manifest_yaml)
                .expect("resolve installed manifest");
        let writer_runtime = query_source_from_installed_manifest(
            &workspace,
            &installed_source,
            &installed_manifest,
            None,
            &SourceDiagnosticReporter::default(),
            BTreeMap::from([(
                "API_TOKEN".to_string(),
                "literal-secret-material".to_string(),
            )]),
        )
        .expect("load writer runtime");
        assert_eq!(
            writer_runtime
                .query_source
                .secrets()
                .get("API_TOKEN")
                .map(String::as_str),
            Some("literal-secret-material")
        );
        let publisher_scope = source_surface_scopes(
            &writer_runtime.query_source,
            SourceScopeSeed::new(
                writer_runtime.runtime_contract_fingerprint.as_str(),
                credential_revision,
            ),
        )
        .expect("coherent source identity")
        .into_iter()
        .next()
        .expect("publisher scope")
        .live_scope();

        config_store
            .remove_source(&workspace, &source_name)
            .expect("remove legacy config source");
        let loader = ObservedValuesLiveScopeLoader::new(
            layout,
            config_store,
            db,
            SourceDiagnosticReporter::default(),
        );
        let live_load = loader.load(&workspace).await.expect("live scope");

        assert!(live_load.failed_sources.is_empty());
        assert_eq!(live_load.live_scopes, vec![publisher_scope]);
    }

    #[tokio::test]
    async fn one_broken_source_does_not_block_other_live_scopes() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let github = SourceName::parse("github").expect("source");
        let broken = SourceName::parse("broken").expect("source");
        install_source(
            &layout,
            &config_store,
            &workspace,
            &github,
            "/search/issues",
        );
        let db = test_db(&layout, &config_store).await;
        let broken_source = InstalledSource {
            name: broken.clone(),
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            credential_revision: Uuid::default(),
            origin: SourceOrigin::Imported,
        };
        upsert_test_source_with_manifest(&db, &workspace, &broken_source, "name: [").await;
        config_store
            .remove_source(&workspace, &github)
            .expect("remove healthy legacy config source");
        let loader = ObservedValuesLiveScopeLoader::new(
            layout,
            config_store,
            db,
            SourceDiagnosticReporter::default(),
        );

        let load = loader.load(&workspace).await.expect("live scope load");

        assert_eq!(load.live_scopes.len(), 1);
        let live_scope = load.live_scopes.first().expect("live scope");
        assert_eq!(live_scope.source_name, "github");
        assert_eq!(load.failed_sources.len(), 1);
        let failed_source = load.failed_sources.first().expect("failed source");
        assert_eq!(failed_source.source_name, "broken");
    }

    fn install_source(
        layout: &AppStateLayout,
        config_store: &ConfigStore,
        workspace: &WorkspaceName,
        source: &SourceName,
        path: &str,
    ) -> InstalledSource {
        write_source_manifest(layout, workspace, source, path);
        let installed = InstalledSource {
            name: source.clone(),
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            credential_revision: Uuid::default(),
            origin: SourceOrigin::Imported,
        };
        config_store
            .upsert_source(workspace, installed.clone())
            .expect("upsert source");
        installed
    }

    fn write_source_manifest(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source: &SourceName,
        path: &str,
    ) {
        std::fs::create_dir_all(layout.source_dir(workspace, source)).expect("source dir");
        std::fs::write(
            layout.manifest_file(workspace, source),
            source_manifest_yaml(source, path),
        )
        .expect("write manifest");
    }

    fn source_manifest_yaml(source: &SourceName, path: &str) -> String {
        format!(
            r"
name: {source}
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: issues
    description: Issues
    request:
      method: GET
      path: {path}
    columns:
      - name: title
        type: Utf8
"
        )
    }

    async fn test_db(layout: &AppStateLayout, config_store: &ConfigStore) -> Arc<CoralDb> {
        let db = open_test_database(layout)
            .await
            .expect("open test database");
        run_state_migrations(&db, config_store, layout)
            .await
            .expect("run state migrations");
        db
    }

    async fn upsert_test_source(
        db: &Arc<CoralDb>,
        workspace: &WorkspaceName,
        source: &InstalledSource,
    ) {
        let mut tx = db.begin().await.expect("begin test source tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 11)
            .await
            .expect("ensure test workspace");
        tx.sources()
            .upsert_source(workspace, source, 11)
            .await
            .expect("upsert test source");
        tx.commit().await.expect("commit test source");
    }

    async fn upsert_test_source_with_manifest(
        db: &Arc<CoralDb>,
        workspace: &WorkspaceName,
        source: &InstalledSource,
        manifest_yaml: &str,
    ) {
        let mut tx = db.begin().await.expect("begin test source tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 11)
            .await
            .expect("ensure test workspace");
        tx.sources()
            .upsert_source(workspace, source, 11)
            .await
            .expect("upsert test source");
        tx.source_manifests()
            .upsert(workspace, &source.name, manifest_yaml, 11)
            .await
            .expect("upsert test source manifest");
        tx.commit().await.expect("commit test source");
    }

    fn install_secured_source(
        layout: &AppStateLayout,
        config_store: &ConfigStore,
        workspace: &WorkspaceName,
        source: &SourceName,
        credential_revision: Uuid,
    ) -> InstalledSource {
        std::fs::create_dir_all(layout.source_dir(workspace, source)).expect("source dir");
        std::fs::write(
            layout.manifest_file(workspace, source),
            format!(
                r"
name: {source}
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_TOKEN:
    kind: secret
base_url: https://example.com
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{{{input.API_TOKEN}}}}
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
"
            ),
        )
        .expect("write manifest");
        let installed = InstalledSource {
            name: source.clone(),
            version: None,
            variables: BTreeMap::new(),
            secrets: vec!["API_TOKEN".to_string()],
            credential_storage: None,
            credential_revision,
            origin: SourceOrigin::Imported,
        };
        config_store
            .upsert_source(workspace, installed.clone())
            .expect("upsert source");
        installed
    }
}
